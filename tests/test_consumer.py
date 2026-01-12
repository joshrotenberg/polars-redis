"""Tests for StreamConsumer with checkpointing.

These tests verify the consumer module for high-level stream consumption
with automatic checkpointing and graceful shutdown.
"""

from __future__ import annotations

import asyncio
import tempfile
import uuid
from pathlib import Path

import polars as pl
import pytest


def unique_stream_name(prefix: str = "test:consumer") -> str:
    """Generate a unique stream name for testing."""
    return f"{prefix}:{uuid.uuid4().hex[:8]}"


def unique_group_name(prefix: str = "testgroup") -> str:
    """Generate a unique consumer group name."""
    return f"{prefix}:{uuid.uuid4().hex[:8]}"


class TestCheckpointStores:
    """Tests for checkpoint store implementations."""

    @pytest.mark.asyncio
    async def test_memory_checkpoint_store(self) -> None:
        """Test in-memory checkpoint store."""
        from polars_redis import MemoryCheckpointStore

        store = MemoryCheckpointStore()

        # Initially empty
        result = await store.get("stream", "group", "consumer")
        assert result is None

        # Set and get
        await store.set("stream", "group", "consumer", "1234567890-0")
        result = await store.get("stream", "group", "consumer")
        assert result == "1234567890-0"

        # Update
        await store.set("stream", "group", "consumer", "1234567890-1")
        result = await store.get("stream", "group", "consumer")
        assert result == "1234567890-1"

        # Delete
        await store.delete("stream", "group", "consumer")
        result = await store.get("stream", "group", "consumer")
        assert result is None

    @pytest.mark.asyncio
    async def test_memory_checkpoint_store_isolation(self) -> None:
        """Test that different consumers have isolated checkpoints."""
        from polars_redis import MemoryCheckpointStore

        store = MemoryCheckpointStore()

        await store.set("stream1", "group", "consumer", "id-1")
        await store.set("stream2", "group", "consumer", "id-2")
        await store.set("stream1", "group", "other-consumer", "id-3")

        assert await store.get("stream1", "group", "consumer") == "id-1"
        assert await store.get("stream2", "group", "consumer") == "id-2"
        assert await store.get("stream1", "group", "other-consumer") == "id-3"

    @pytest.mark.asyncio
    async def test_file_checkpoint_store(self) -> None:
        """Test file-based checkpoint store."""
        from polars_redis import FileCheckpointStore

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "checkpoints.json"
            store = FileCheckpointStore(path)

            # Set and get
            await store.set("stream", "group", "consumer", "1234567890-0")
            result = await store.get("stream", "group", "consumer")
            assert result == "1234567890-0"

            # Verify file was created
            assert path.exists()

            # Create new store instance to verify persistence
            store2 = FileCheckpointStore(path)
            result = await store2.get("stream", "group", "consumer")
            assert result == "1234567890-0"

            # Delete
            await store2.delete("stream", "group", "consumer")
            result = await store2.get("stream", "group", "consumer")
            assert result is None

    @pytest.mark.asyncio
    async def test_file_checkpoint_store_creates_parent_dirs(self) -> None:
        """Test that file store creates parent directories."""
        from polars_redis import FileCheckpointStore

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "nested" / "dirs" / "checkpoints.json"
            store = FileCheckpointStore(path)

            await store.set("stream", "group", "consumer", "id-1")
            assert path.exists()

    @pytest.mark.asyncio
    async def test_redis_checkpoint_store(self, redis_url: str, redis_available: bool) -> None:
        """Test Redis-based checkpoint store."""
        if not redis_available:
            pytest.skip("Redis not available")

        from polars_redis import RedisCheckpointStore

        store = RedisCheckpointStore(redis_url, key_prefix="test:checkpoint:")

        try:
            # Set and get
            await store.set("stream", "group", "consumer", "1234567890-0")
            result = await store.get("stream", "group", "consumer")
            assert result == "1234567890-0"

            # Update
            await store.set("stream", "group", "consumer", "1234567890-1")
            result = await store.get("stream", "group", "consumer")
            assert result == "1234567890-1"

            # Delete
            await store.delete("stream", "group", "consumer")
            result = await store.get("stream", "group", "consumer")
            assert result is None

        finally:
            await store.close()


class TestConsumerStats:
    """Tests for ConsumerStats dataclass."""

    def test_consumer_stats_defaults(self) -> None:
        """Test default values."""
        from polars_redis import ConsumerStats

        stats = ConsumerStats()

        assert stats.batches_processed == 0
        assert stats.entries_processed == 0
        assert stats.last_entry_id is None
        assert stats.start_time > 0
        assert stats.last_batch_time is None

    def test_consumer_stats_uptime(self) -> None:
        """Test uptime calculation."""
        import time

        from polars_redis import ConsumerStats

        stats = ConsumerStats()
        time.sleep(0.1)

        assert stats.uptime_seconds >= 0.1

    def test_consumer_stats_entries_per_second(self) -> None:
        """Test entries per second calculation."""
        import time

        from polars_redis import ConsumerStats

        stats = ConsumerStats()
        stats.entries_processed = 100
        time.sleep(0.1)

        # Should be roughly 1000 entries/second (100 entries in 0.1s)
        assert stats.entries_per_second > 0


class TestStreamConsumer:
    """Tests for StreamConsumer class."""

    @pytest.mark.asyncio
    async def test_stream_consumer_basic(self, redis_url: str, redis_available: bool) -> None:
        """Test basic stream consumption."""
        if not redis_available:
            pytest.skip("Redis not available")

        import redis as redis_client
        from polars_redis import MemoryCheckpointStore, StreamConsumer

        stream = unique_stream_name()
        group = unique_group_name()
        client = redis_client.Redis.from_url(redis_url)

        try:
            # Add entries before starting consumer
            for i in range(5):
                client.xadd(stream, {"seq": str(i), "value": str(i * 10)})

            consumer = StreamConsumer(
                url=redis_url,
                stream=stream,
                group=group,
                consumer="worker-1",
                checkpoint_store=MemoryCheckpointStore(),
                batch_size=10,
                batch_timeout_ms=500,
                handle_signals=False,
            )

            batches = []
            async with consumer:
                async for batch_df in consumer:
                    batches.append(batch_df)
                    if consumer.stats.entries_processed >= 5:
                        break

            assert len(batches) >= 1
            total_entries = sum(len(b) for b in batches)
            assert total_entries == 5

            # Check DataFrame structure
            df = batches[0]
            assert "_id" in df.columns
            assert "_ts" in df.columns
            assert "seq" in df.columns
            assert "value" in df.columns

        finally:
            client.delete(stream)
            client.close()

    @pytest.mark.asyncio
    async def test_stream_consumer_with_schema(self, redis_url: str, redis_available: bool) -> None:
        """Test stream consumption with schema casting."""
        if not redis_available:
            pytest.skip("Redis not available")

        import redis as redis_client
        from polars_redis import StreamConsumer

        stream = unique_stream_name()
        group = unique_group_name()
        client = redis_client.Redis.from_url(redis_url)

        try:
            client.xadd(stream, {"count": "42", "ratio": "3.14"})

            consumer = StreamConsumer(
                url=redis_url,
                stream=stream,
                group=group,
                consumer="worker-1",
                schema={"count": pl.Int64, "ratio": pl.Float64},
                batch_timeout_ms=500,
                handle_signals=False,
            )

            async with consumer:
                async for batch_df in consumer:
                    assert batch_df["count"].dtype == pl.Int64
                    assert batch_df["ratio"].dtype == pl.Float64
                    assert batch_df["count"][0] == 42
                    assert abs(batch_df["ratio"][0] - 3.14) < 0.01
                    break

        finally:
            client.delete(stream)
            client.close()

    @pytest.mark.asyncio
    async def test_stream_consumer_include_sequence(
        self, redis_url: str, redis_available: bool
    ) -> None:
        """Test including sequence number column."""
        if not redis_available:
            pytest.skip("Redis not available")

        import redis as redis_client
        from polars_redis import StreamConsumer

        stream = unique_stream_name()
        group = unique_group_name()
        client = redis_client.Redis.from_url(redis_url)

        try:
            client.xadd(stream, {"data": "test"})

            consumer = StreamConsumer(
                url=redis_url,
                stream=stream,
                group=group,
                consumer="worker-1",
                include_sequence=True,
                batch_timeout_ms=500,
                handle_signals=False,
            )

            async with consumer:
                async for batch_df in consumer:
                    assert "_seq" in batch_df.columns
                    assert batch_df["_seq"][0] == 0
                    break

        finally:
            client.delete(stream)
            client.close()

    @pytest.mark.asyncio
    async def test_stream_consumer_custom_columns(
        self, redis_url: str, redis_available: bool
    ) -> None:
        """Test custom column names."""
        if not redis_available:
            pytest.skip("Redis not available")

        import redis as redis_client
        from polars_redis import StreamConsumer

        stream = unique_stream_name()
        group = unique_group_name()
        client = redis_client.Redis.from_url(redis_url)

        try:
            client.xadd(stream, {"data": "test"})

            consumer = StreamConsumer(
                url=redis_url,
                stream=stream,
                group=group,
                consumer="worker-1",
                id_column="entry_id",
                timestamp_column="entry_ts",
                include_sequence=True,
                sequence_column="entry_seq",
                batch_timeout_ms=500,
                handle_signals=False,
            )

            async with consumer:
                async for batch_df in consumer:
                    assert "entry_id" in batch_df.columns
                    assert "entry_ts" in batch_df.columns
                    assert "entry_seq" in batch_df.columns
                    assert "_id" not in batch_df.columns
                    break

        finally:
            client.delete(stream)
            client.close()

    @pytest.mark.asyncio
    async def test_stream_consumer_checkpointing(
        self, redis_url: str, redis_available: bool
    ) -> None:
        """Test that checkpoints are saved correctly."""
        if not redis_available:
            pytest.skip("Redis not available")

        import redis as redis_client
        from polars_redis import MemoryCheckpointStore, StreamConsumer

        stream = unique_stream_name()
        group = unique_group_name()
        client = redis_client.Redis.from_url(redis_url)
        checkpoint_store = MemoryCheckpointStore()

        try:
            # Add entries
            for i in range(3):
                client.xadd(stream, {"seq": str(i)})

            consumer = StreamConsumer(
                url=redis_url,
                stream=stream,
                group=group,
                consumer="worker-1",
                checkpoint_store=checkpoint_store,
                auto_checkpoint=True,
                batch_timeout_ms=500,
                handle_signals=False,
            )

            last_id = None
            async with consumer:
                async for batch_df in consumer:
                    last_id = batch_df["_id"][-1]
                    if consumer.stats.entries_processed >= 3:
                        break

            # Verify checkpoint was saved
            saved_checkpoint = await checkpoint_store.get(stream, group, "worker-1")
            assert saved_checkpoint == last_id

        finally:
            client.delete(stream)
            client.close()

    @pytest.mark.asyncio
    async def test_stream_consumer_manual_checkpoint(
        self, redis_url: str, redis_available: bool
    ) -> None:
        """Test manual checkpointing."""
        if not redis_available:
            pytest.skip("Redis not available")

        import redis as redis_client
        from polars_redis import MemoryCheckpointStore, StreamConsumer

        stream = unique_stream_name()
        group = unique_group_name()
        client = redis_client.Redis.from_url(redis_url)
        checkpoint_store = MemoryCheckpointStore()

        try:
            client.xadd(stream, {"data": "test"})

            consumer = StreamConsumer(
                url=redis_url,
                stream=stream,
                group=group,
                consumer="worker-1",
                checkpoint_store=checkpoint_store,
                auto_checkpoint=False,
                batch_timeout_ms=500,
                handle_signals=False,
            )

            async with consumer:
                async for batch_df in consumer:
                    # Checkpoint not saved yet
                    saved = await checkpoint_store.get(stream, group, "worker-1")
                    assert saved is None

                    # Manual commit
                    await consumer.commit()

                    # Now it should be saved
                    saved = await checkpoint_store.get(stream, group, "worker-1")
                    assert saved is not None
                    break

        finally:
            client.delete(stream)
            client.close()

    @pytest.mark.asyncio
    async def test_stream_consumer_stats(self, redis_url: str, redis_available: bool) -> None:
        """Test consumer statistics tracking."""
        if not redis_available:
            pytest.skip("Redis not available")

        import redis as redis_client
        from polars_redis import StreamConsumer

        stream = unique_stream_name()
        group = unique_group_name()
        client = redis_client.Redis.from_url(redis_url)

        try:
            for i in range(10):
                client.xadd(stream, {"seq": str(i)})

            consumer = StreamConsumer(
                url=redis_url,
                stream=stream,
                group=group,
                consumer="worker-1",
                batch_size=5,
                batch_timeout_ms=500,
                handle_signals=False,
            )

            async with consumer:
                async for _ in consumer:
                    if consumer.stats.entries_processed >= 10:
                        break

            assert consumer.stats.batches_processed >= 1
            assert consumer.stats.entries_processed == 10
            assert consumer.stats.last_entry_id is not None
            assert consumer.stats.last_batch_time is not None
            assert consumer.stats.uptime_seconds > 0

        finally:
            client.delete(stream)
            client.close()

    @pytest.mark.asyncio
    async def test_stream_consumer_request_shutdown(
        self, redis_url: str, redis_available: bool
    ) -> None:
        """Test graceful shutdown request."""
        if not redis_available:
            pytest.skip("Redis not available")

        import redis as redis_client
        from polars_redis import StreamConsumer

        stream = unique_stream_name()
        group = unique_group_name()
        client = redis_client.Redis.from_url(redis_url)

        try:
            # Add some entries
            for i in range(5):
                client.xadd(stream, {"seq": str(i)})

            consumer = StreamConsumer(
                url=redis_url,
                stream=stream,
                group=group,
                consumer="worker-1",
                batch_timeout_ms=500,
                handle_signals=False,
            )

            batch_count = 0
            async with consumer:
                async for _ in consumer:
                    batch_count += 1
                    consumer.request_shutdown()

            # Should exit after first batch due to shutdown request
            assert batch_count == 1

        finally:
            client.delete(stream)
            client.close()

    @pytest.mark.asyncio
    async def test_stream_consumer_rollback(self, redis_url: str, redis_available: bool) -> None:
        """Test rollback does not advance checkpoint."""
        if not redis_available:
            pytest.skip("Redis not available")

        import redis as redis_client
        from polars_redis import MemoryCheckpointStore, StreamConsumer

        stream = unique_stream_name()
        group = unique_group_name()
        client = redis_client.Redis.from_url(redis_url)
        checkpoint_store = MemoryCheckpointStore()

        try:
            # Add entries
            for i in range(3):
                client.xadd(stream, {"seq": str(i)})

            consumer = StreamConsumer(
                url=redis_url,
                stream=stream,
                group=group,
                consumer="worker-1",
                checkpoint_store=checkpoint_store,
                auto_checkpoint=False,
                batch_timeout_ms=500,
                handle_signals=False,
            )

            async with consumer:
                async for batch_df in consumer:
                    # Simulate processing failure - rollback instead of commit
                    await consumer.rollback()
                    break

            # Checkpoint should not have been saved
            saved = await checkpoint_store.get(stream, group, "worker-1")
            assert saved is None

        finally:
            client.delete(stream)
            client.close()


class TestConsumerUnit:
    """Unit tests that don't require Redis."""

    def test_imports(self) -> None:
        """Test that consumer classes are importable."""
        from polars_redis import (
            CheckpointStore,
            ConsumerStats,
            FileCheckpointStore,
            MemoryCheckpointStore,
            RedisCheckpointStore,
            StreamConsumer,
        )

        assert CheckpointStore is not None
        assert MemoryCheckpointStore is not None
        assert FileCheckpointStore is not None
        assert RedisCheckpointStore is not None
        assert StreamConsumer is not None
        assert ConsumerStats is not None

    def test_stream_consumer_signature(self) -> None:
        """Test StreamConsumer constructor signature."""
        import inspect

        from polars_redis import StreamConsumer

        sig = inspect.signature(StreamConsumer.__init__)
        params = set(sig.parameters.keys())

        expected = {
            "self",
            "url",
            "stream",
            "group",
            "consumer",
            "checkpoint_store",
            "batch_size",
            "batch_timeout_ms",
            "auto_ack",
            "auto_checkpoint",
            "schema",
            "include_id",
            "id_column",
            "include_timestamp",
            "timestamp_column",
            "include_sequence",
            "sequence_column",
            "handle_signals",
        }

        assert expected.issubset(params)
