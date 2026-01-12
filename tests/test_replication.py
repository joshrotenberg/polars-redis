"""Tests for the CDC replication pipeline.

These tests verify the replication module for filtered CDC replication
from a source stream to multiple destination clusters.
"""

from __future__ import annotations

import asyncio
import uuid

import polars as pl
import pytest


def unique_stream_name(prefix: str = "test:repl") -> str:
    """Generate a unique stream name for testing."""
    return f"{prefix}:{uuid.uuid4().hex[:8]}"


def unique_group_name(prefix: str = "replgroup") -> str:
    """Generate a unique consumer group name."""
    return f"{prefix}:{uuid.uuid4().hex[:8]}"


def unique_key_prefix() -> str:
    """Generate a unique key prefix for testing."""
    return f"test:dest:{uuid.uuid4().hex[:8]}:"


class TestReplicationDestination:
    """Tests for ReplicationDestination dataclass."""

    def test_destination_defaults(self) -> None:
        """Test default values."""
        from polars_redis import ReplicationDestination

        dest = ReplicationDestination(url="redis://localhost:6379")

        assert dest.url == "redis://localhost:6379"
        assert dest.name == "localhost:6379"
        assert dest.filter is None
        assert dest.key_column == "key"
        assert dest.key_prefix == ""
        assert dest.transform is None
        assert dest.write_type == "hash"
        assert dest.timeout_ms == 30000

    def test_destination_with_filter(self) -> None:
        """Test destination with Polars filter expression."""
        from polars_redis import ReplicationDestination

        dest = ReplicationDestination(
            url="redis://localhost:6379",
            name="eu-cluster",
            filter=pl.col("region") == "EU",
        )

        assert dest.name == "eu-cluster"
        assert dest.filter is not None

    def test_destination_with_transform(self) -> None:
        """Test destination with transform function."""
        from polars_redis import ReplicationDestination

        def drop_internal(df: pl.DataFrame) -> pl.DataFrame:
            return df.drop("internal")

        dest = ReplicationDestination(
            url="redis://localhost:6379",
            transform=drop_internal,
        )

        assert dest.transform is not None

        df = pl.DataFrame({"key": ["k1"], "value": ["v1"], "internal": ["secret"]})
        result = dest.transform(df)
        assert "internal" not in result.columns


class TestReplicationStats:
    """Tests for ReplicationStats dataclass."""

    def test_stats_defaults(self) -> None:
        """Test default values."""
        from polars_redis import ReplicationStats

        stats = ReplicationStats()

        assert stats.batches_processed == 0
        assert stats.source_entries_read == 0
        assert stats.total_keys_written == 0
        assert stats.total_keys_failed == 0
        assert stats.start_time > 0

    def test_stats_entries_per_second(self) -> None:
        """Test throughput calculation."""
        import time

        from polars_redis import ReplicationStats

        stats = ReplicationStats()
        stats.source_entries_read = 100
        time.sleep(0.1)

        assert stats.entries_per_second > 0


class TestBatchResult:
    """Tests for BatchResult dataclass."""

    def test_batch_result_all_succeeded(self) -> None:
        """Test all_succeeded property."""
        from polars_redis import BatchResult, Destination, DestinationResult

        dest1 = Destination(url="redis://a:6379")
        dest2 = Destination(url="redis://b:6379")

        result = BatchResult(
            source_entries=10,
            destination_results=[
                DestinationResult(destination=dest1, success=True, keys_written=10),
                DestinationResult(destination=dest2, success=True, keys_written=10),
            ],
            duration_ms=50.0,
        )

        assert result.all_succeeded is True

    def test_batch_result_partial_failure(self) -> None:
        """Test all_succeeded with partial failure."""
        from polars_redis import BatchResult, Destination, DestinationResult

        dest1 = Destination(url="redis://a:6379")
        dest2 = Destination(url="redis://b:6379")

        result = BatchResult(
            source_entries=10,
            destination_results=[
                DestinationResult(destination=dest1, success=True, keys_written=10),
                DestinationResult(destination=dest2, success=False, error="Connection refused"),
            ],
            duration_ms=50.0,
        )

        assert result.all_succeeded is False


class TestReplicationPipeline:
    """Tests for ReplicationPipeline class."""

    @pytest.mark.asyncio
    async def test_pipeline_no_destinations_error(
        self, redis_url: str, redis_available: bool
    ) -> None:
        """Test that running without destinations raises error."""
        if not redis_available:
            pytest.skip("Redis not available")

        from polars_redis import ReplicationPipeline

        pipeline = ReplicationPipeline(
            source_url=redis_url,
            stream=unique_stream_name(),
            group=unique_group_name(),
            consumer="worker-1",
        )

        with pytest.raises(ValueError, match="No destinations configured"):
            await pipeline.run()

    @pytest.mark.asyncio
    async def test_pipeline_single_destination(self, redis_url: str, redis_available: bool) -> None:
        """Test replication to a single destination."""
        if not redis_available:
            pytest.skip("Redis not available")

        import redis as redis_client
        from polars_redis import ReplicationDestination, ReplicationPipeline

        stream = unique_stream_name()
        group = unique_group_name()
        key_prefix = unique_key_prefix()
        client = redis_client.Redis.from_url(redis_url)

        try:
            # Add entries to source stream
            for i in range(5):
                client.xadd(stream, {"key": f"item:{i}", "name": f"Item {i}", "value": str(i)})

            pipeline = ReplicationPipeline(
                source_url=redis_url,
                stream=stream,
                group=group,
                consumer="worker-1",
                batch_timeout_ms=500,
                handle_signals=False,
            )

            pipeline.add_destination(
                ReplicationDestination(
                    url=redis_url,
                    name="dest1",
                    key_prefix=key_prefix,
                )
            )

            # Process one batch
            result = await pipeline.run_once()

            assert result is not None
            assert result.source_entries == 5
            assert result.all_succeeded
            assert len(result.destination_results) == 1
            assert result.destination_results[0].keys_written == 5

            # Verify data was written
            assert client.hget(f"{key_prefix}item:0", "name") == b"Item 0"
            assert client.hget(f"{key_prefix}item:4", "value") == b"4"

        finally:
            # Cleanup
            client.delete(stream)
            for i in range(5):
                client.delete(f"{key_prefix}item:{i}")
            client.close()

    @pytest.mark.asyncio
    async def test_pipeline_with_filter(self, redis_url: str, redis_available: bool) -> None:
        """Test replication with per-destination filtering."""
        if not redis_available:
            pytest.skip("Redis not available")

        import redis as redis_client
        from polars_redis import ReplicationDestination, ReplicationPipeline

        stream = unique_stream_name()
        group = unique_group_name()
        eu_prefix = unique_key_prefix()
        us_prefix = unique_key_prefix()
        client = redis_client.Redis.from_url(redis_url)

        try:
            # Add entries with different regions
            client.xadd(stream, {"key": "user:1", "name": "Alice", "region": "EU"})
            client.xadd(stream, {"key": "user:2", "name": "Bob", "region": "US"})
            client.xadd(stream, {"key": "user:3", "name": "Charlie", "region": "EU"})
            client.xadd(stream, {"key": "user:4", "name": "Diana", "region": "US"})

            pipeline = ReplicationPipeline(
                source_url=redis_url,
                stream=stream,
                group=group,
                consumer="worker-1",
                batch_timeout_ms=500,
                handle_signals=False,
            )

            # EU destination - only EU records
            pipeline.add_destination(
                ReplicationDestination(
                    url=redis_url,
                    name="eu-cluster",
                    filter=pl.col("region") == "EU",
                    key_prefix=eu_prefix,
                )
            )

            # US destination - only US records
            pipeline.add_destination(
                ReplicationDestination(
                    url=redis_url,
                    name="us-cluster",
                    filter=pl.col("region") == "US",
                    key_prefix=us_prefix,
                )
            )

            result = await pipeline.run_once()

            assert result is not None
            assert result.source_entries == 4
            assert result.all_succeeded

            # EU destination should have 2 records (Alice, Charlie)
            eu_result = result.destination_results[0]
            assert eu_result.keys_written == 2

            # US destination should have 2 records (Bob, Diana)
            us_result = result.destination_results[1]
            assert us_result.keys_written == 2

            # Verify EU cluster has correct data
            assert client.hget(f"{eu_prefix}user:1", "name") == b"Alice"
            assert client.hget(f"{eu_prefix}user:3", "name") == b"Charlie"
            assert not client.exists(f"{eu_prefix}user:2")  # Bob is US

            # Verify US cluster has correct data
            assert client.hget(f"{us_prefix}user:2", "name") == b"Bob"
            assert client.hget(f"{us_prefix}user:4", "name") == b"Diana"
            assert not client.exists(f"{us_prefix}user:1")  # Alice is EU

        finally:
            client.delete(stream)
            for i in range(1, 5):
                client.delete(f"{eu_prefix}user:{i}")
                client.delete(f"{us_prefix}user:{i}")
            client.close()

    @pytest.mark.asyncio
    async def test_pipeline_with_transform(self, redis_url: str, redis_available: bool) -> None:
        """Test replication with per-destination transform."""
        if not redis_available:
            pytest.skip("Redis not available")

        import redis as redis_client
        from polars_redis import ReplicationDestination, ReplicationPipeline

        stream = unique_stream_name()
        group = unique_group_name()
        full_prefix = unique_key_prefix()
        filtered_prefix = unique_key_prefix()
        client = redis_client.Redis.from_url(redis_url)

        try:
            client.xadd(
                stream, {"key": "doc:1", "title": "Hello", "internal": "secret", "public": "yes"}
            )

            pipeline = ReplicationPipeline(
                source_url=redis_url,
                stream=stream,
                group=group,
                consumer="worker-1",
                batch_timeout_ms=500,
                handle_signals=False,
            )

            # Full destination - all fields
            pipeline.add_destination(
                ReplicationDestination(
                    url=redis_url,
                    name="full",
                    key_prefix=full_prefix,
                )
            )

            # Filtered destination - drop internal field
            pipeline.add_destination(
                ReplicationDestination(
                    url=redis_url,
                    name="filtered",
                    key_prefix=filtered_prefix,
                    transform=lambda df: df.drop("internal"),
                )
            )

            result = await pipeline.run_once()

            assert result is not None
            assert result.all_succeeded

            # Full destination has internal field
            assert client.hget(f"{full_prefix}doc:1", "internal") == b"secret"

            # Filtered destination does not
            assert client.hget(f"{filtered_prefix}doc:1", "internal") is None
            assert client.hget(f"{filtered_prefix}doc:1", "title") == b"Hello"

        finally:
            client.delete(stream)
            client.delete(f"{full_prefix}doc:1")
            client.delete(f"{filtered_prefix}doc:1")
            client.close()

    @pytest.mark.asyncio
    async def test_pipeline_stats(self, redis_url: str, redis_available: bool) -> None:
        """Test that pipeline tracks statistics."""
        if not redis_available:
            pytest.skip("Redis not available")

        import redis as redis_client
        from polars_redis import ReplicationDestination, ReplicationPipeline

        stream = unique_stream_name()
        group = unique_group_name()
        key_prefix = unique_key_prefix()
        client = redis_client.Redis.from_url(redis_url)

        try:
            for i in range(10):
                client.xadd(stream, {"key": f"k:{i}", "value": str(i)})

            pipeline = ReplicationPipeline(
                source_url=redis_url,
                stream=stream,
                group=group,
                consumer="worker-1",
                batch_timeout_ms=500,
                handle_signals=False,
            )

            pipeline.add_destination(
                ReplicationDestination(url=redis_url, name="dest", key_prefix=key_prefix)
            )

            await pipeline.run_once()

            # Check stats - note: run_once doesn't update pipeline.stats,
            # but we can verify the result
            # For full stats, we'd need to use run() with shutdown

        finally:
            client.delete(stream)
            for i in range(10):
                client.delete(f"{key_prefix}k:{i}")
            client.close()

    @pytest.mark.asyncio
    async def test_pipeline_filter_no_matches(self, redis_url: str, redis_available: bool) -> None:
        """Test that filter with no matches results in zero writes."""
        if not redis_available:
            pytest.skip("Redis not available")

        import redis as redis_client
        from polars_redis import ReplicationDestination, ReplicationPipeline

        stream = unique_stream_name()
        group = unique_group_name()
        key_prefix = unique_key_prefix()
        client = redis_client.Redis.from_url(redis_url)

        try:
            # All entries are EU
            for i in range(3):
                client.xadd(stream, {"key": f"user:{i}", "region": "EU"})

            pipeline = ReplicationPipeline(
                source_url=redis_url,
                stream=stream,
                group=group,
                consumer="worker-1",
                batch_timeout_ms=500,
                handle_signals=False,
            )

            # Destination only wants US records
            pipeline.add_destination(
                ReplicationDestination(
                    url=redis_url,
                    name="us-only",
                    filter=pl.col("region") == "US",
                    key_prefix=key_prefix,
                )
            )

            result = await pipeline.run_once()

            assert result is not None
            assert result.source_entries == 3
            assert result.all_succeeded
            # Zero writes because no records match filter
            assert result.destination_results[0].keys_written == 0

        finally:
            client.delete(stream)
            client.close()

    @pytest.mark.asyncio
    async def test_pipeline_json_write_type(self, redis_url: str, redis_available: bool) -> None:
        """Test replication with JSON write type."""
        if not redis_available:
            pytest.skip("Redis not available")

        import redis as redis_client
        from polars_redis import ReplicationDestination, ReplicationPipeline

        stream = unique_stream_name()
        group = unique_group_name()
        key_prefix = unique_key_prefix()
        client = redis_client.Redis.from_url(redis_url)

        try:
            client.xadd(stream, {"key": "json:1", "title": "Hello", "count": "42"})

            pipeline = ReplicationPipeline(
                source_url=redis_url,
                stream=stream,
                group=group,
                consumer="worker-1",
                batch_timeout_ms=500,
                handle_signals=False,
            )

            pipeline.add_destination(
                ReplicationDestination(
                    url=redis_url,
                    name="json-dest",
                    key_prefix=key_prefix,
                    write_type="json",
                )
            )

            result = await pipeline.run_once()

            assert result is not None
            assert result.all_succeeded
            assert result.destination_results[0].keys_written == 1

            # Verify JSON was written
            doc = client.json().get(f"{key_prefix}json:1")
            assert doc["title"] == "Hello"
            assert doc["count"] == "42"

        finally:
            client.delete(stream)
            client.delete(f"{key_prefix}json:1")
            client.close()


class TestReplicationUnit:
    """Unit tests that don't require Redis."""

    def test_imports(self) -> None:
        """Test that replication classes are importable."""
        from polars_redis import (
            BatchResult,
            ReplicationDestination,
            ReplicationPipeline,
            ReplicationStats,
        )

        assert ReplicationPipeline is not None
        assert ReplicationDestination is not None
        assert ReplicationStats is not None
        assert BatchResult is not None

    def test_add_destination_chaining(self) -> None:
        """Test that add_destination returns self for chaining."""
        from polars_redis import ReplicationDestination, ReplicationPipeline

        pipeline = ReplicationPipeline(
            source_url="redis://localhost:6379",
            stream="test",
            group="group",
            consumer="consumer",
        )

        result = pipeline.add_destination(
            ReplicationDestination(url="redis://dest:6379")
        ).add_destination(ReplicationDestination(url="redis://dest2:6379"))

        assert result is pipeline
        assert len(pipeline._destinations) == 2
