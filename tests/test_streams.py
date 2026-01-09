"""Tests for Redis Streams streaming functionality.

These tests verify the streams module for single-stream consumption
with consumer groups and continuous batch iteration.
"""

from __future__ import annotations

import asyncio
import time
import uuid

import polars as pl
import pytest


def unique_stream_name(prefix: str = "test:stream") -> str:
    """Generate a unique stream name for testing."""
    return f"{prefix}:{uuid.uuid4().hex[:8]}"


def unique_group_name(prefix: str = "testgroup") -> str:
    """Generate a unique consumer group name."""
    return f"{prefix}:{uuid.uuid4().hex[:8]}"


class TestReadStream:
    """Tests for read_stream function."""

    def test_read_stream_basic(self, redis_url: str, redis_available: bool) -> None:
        """Test basic stream reading with XRANGE."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        stream = unique_stream_name()
        client = redis_client.Redis.from_url(redis_url)

        try:
            # Add entries to stream
            for i in range(5):
                client.xadd(
                    stream, {"user_id": f"user_{i}", "action": "click", "value": str(i * 10)}
                )

            # Read the stream
            df = pr.read_stream(
                redis_url,
                stream=stream,
                schema={"user_id": pl.Utf8, "action": pl.Utf8, "value": pl.Int64},
            )

            assert len(df) == 5
            assert "user_id" in df.columns
            assert "action" in df.columns
            assert "value" in df.columns
            assert "_id" in df.columns
            assert "_ts" in df.columns

            # Check values
            assert df["action"].to_list() == ["click"] * 5
            assert df["value"].to_list() == [0, 10, 20, 30, 40]

        finally:
            client.delete(stream)
            client.close()

    def test_read_stream_with_count(self, redis_url: str, redis_available: bool) -> None:
        """Test reading limited number of entries."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        stream = unique_stream_name()
        client = redis_client.Redis.from_url(redis_url)

        try:
            # Add 10 entries
            for i in range(10):
                client.xadd(stream, {"seq": str(i)})

            # Read only 3
            df = pr.read_stream(redis_url, stream=stream, count=3)

            assert len(df) == 3

        finally:
            client.delete(stream)
            client.close()

    def test_read_stream_id_range(self, redis_url: str, redis_available: bool) -> None:
        """Test reading with ID range."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        stream = unique_stream_name()
        client = redis_client.Redis.from_url(redis_url)

        try:
            # Add entries and capture IDs
            ids = []
            for i in range(5):
                entry_id = client.xadd(stream, {"seq": str(i)})
                ids.append(entry_id.decode() if isinstance(entry_id, bytes) else entry_id)

            # Read only middle entries (index 1-3)
            df = pr.read_stream(
                redis_url,
                stream=stream,
                start_id=ids[1],
                end_id=ids[3],
            )

            assert len(df) == 3

        finally:
            client.delete(stream)
            client.close()

    def test_read_stream_include_sequence(self, redis_url: str, redis_available: bool) -> None:
        """Test including sequence number column."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        stream = unique_stream_name()
        client = redis_client.Redis.from_url(redis_url)

        try:
            client.xadd(stream, {"data": "test"})

            df = pr.read_stream(
                redis_url,
                stream=stream,
                include_sequence=True,
            )

            assert "_seq" in df.columns
            assert df["_seq"][0] == 0  # First entry in ms has sequence 0

        finally:
            client.delete(stream)
            client.close()

    def test_read_stream_custom_columns(self, redis_url: str, redis_available: bool) -> None:
        """Test custom column names."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        stream = unique_stream_name()
        client = redis_client.Redis.from_url(redis_url)

        try:
            client.xadd(stream, {"data": "test"})

            df = pr.read_stream(
                redis_url,
                stream=stream,
                id_column="entry_id",
                timestamp_column="entry_ts",
                include_sequence=True,
                sequence_column="entry_seq",
            )

            assert "entry_id" in df.columns
            assert "entry_ts" in df.columns
            assert "entry_seq" in df.columns
            assert "_id" not in df.columns
            assert "_ts" not in df.columns

        finally:
            client.delete(stream)
            client.close()

    def test_read_stream_empty(self, redis_url: str, redis_available: bool) -> None:
        """Test reading from empty/non-existent stream."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr

        stream = unique_stream_name()

        df = pr.read_stream(
            redis_url,
            stream=stream,
            schema={"field": pl.Utf8},
        )

        assert len(df) == 0
        assert "_id" in df.columns
        assert "field" in df.columns


class TestReadStreamConsumerGroup:
    """Tests for consumer group functionality."""

    def test_consumer_group_basic(self, redis_url: str, redis_available: bool) -> None:
        """Test basic consumer group reading."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        stream = unique_stream_name()
        group = unique_group_name()
        client = redis_client.Redis.from_url(redis_url)

        try:
            # Add entries
            for i in range(5):
                client.xadd(stream, {"seq": str(i)})

            # Read with consumer group
            df = pr.read_stream(
                redis_url,
                stream=stream,
                group=group,
                consumer="worker-1",
                auto_ack=True,
            )

            assert len(df) == 5

            # Reading again should return nothing (all consumed)
            df2 = pr.read_stream(
                redis_url,
                stream=stream,
                group=group,
                consumer="worker-1",
                block_ms=100,  # Short block
            )

            assert len(df2) == 0

        finally:
            client.delete(stream)
            client.close()

    def test_consumer_group_manual_ack(self, redis_url: str, redis_available: bool) -> None:
        """Test manual acknowledgment of entries."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        stream = unique_stream_name()
        group = unique_group_name()
        client = redis_client.Redis.from_url(redis_url)

        try:
            # Add entries
            for i in range(3):
                client.xadd(stream, {"seq": str(i)})

            # Read without auto-ack
            df = pr.read_stream(
                redis_url,
                stream=stream,
                group=group,
                consumer="worker-1",
                auto_ack=False,
            )

            assert len(df) == 3

            # Acknowledge entries
            entry_ids = df["_id"].to_list()
            acked = pr.ack_entries(redis_url, stream, group, entry_ids)

            assert acked == 3

        finally:
            client.delete(stream)
            client.close()

    def test_multiple_consumers(self, redis_url: str, redis_available: bool) -> None:
        """Test multiple consumers in same group."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        stream = unique_stream_name()
        group = unique_group_name()
        client = redis_client.Redis.from_url(redis_url)

        try:
            # Add entries
            for i in range(6):
                client.xadd(stream, {"seq": str(i)})

            # Consumer 1 reads 3
            df1 = pr.read_stream(
                redis_url,
                stream=stream,
                group=group,
                consumer="worker-1",
                count=3,
                auto_ack=True,
            )

            # Consumer 2 reads remaining
            df2 = pr.read_stream(
                redis_url,
                stream=stream,
                group=group,
                consumer="worker-2",
                auto_ack=True,
            )

            assert len(df1) == 3
            assert len(df2) == 3

            # No overlap in entries
            ids1 = set(df1["_id"].to_list())
            ids2 = set(df2["_id"].to_list())
            assert len(ids1 & ids2) == 0

        finally:
            client.delete(stream)
            client.close()


class TestScanStream:
    """Tests for scan_stream (lazy) function."""

    def test_scan_stream_lazy(self, redis_url: str, redis_available: bool) -> None:
        """Test lazy stream scanning."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        stream = unique_stream_name()
        client = redis_client.Redis.from_url(redis_url)

        try:
            for i in range(5):
                client.xadd(stream, {"value": str(i * 10)})

            lf = pr.scan_stream(
                redis_url,
                stream=stream,
                schema={"value": pl.Int64},
            )

            # Should be a LazyFrame
            assert isinstance(lf, pl.LazyFrame)

            # Apply lazy operations
            result = lf.filter(pl.col("value") > 10).collect()

            assert len(result) == 3  # 20, 30, 40

        finally:
            client.delete(stream)
            client.close()


class TestIterStream:
    """Tests for iter_stream batch iterator."""

    def test_iter_stream_basic(self, redis_url: str, redis_available: bool) -> None:
        """Test basic batch iteration."""
        if not redis_available:
            pytest.skip("Redis not available")

        import threading

        import polars_redis as pr
        import redis as redis_client

        stream = unique_stream_name()
        client = redis_client.Redis.from_url(redis_url)
        stop_flag = threading.Event()

        def add_entries() -> None:
            time.sleep(0.1)
            for i in range(15):
                if stop_flag.is_set():
                    break
                client.xadd(stream, {"seq": str(i)})
                time.sleep(0.05)

        thread = threading.Thread(target=add_entries)
        thread.start()

        try:
            batches = []
            for batch in pr.iter_stream(
                redis_url,
                stream=stream,
                batch_size=5,
                block_ms=2000,
            ):
                batches.append(batch)
                if len(batches) >= 2:
                    stop_flag.set()
                    break

            thread.join()

            assert len(batches) >= 1

        finally:
            stop_flag.set()
            thread.join()
            client.delete(stream)
            client.close()

    def test_iter_stream_consumer_group(self, redis_url: str, redis_available: bool) -> None:
        """Test batch iteration with consumer group."""
        if not redis_available:
            pytest.skip("Redis not available")

        import threading

        import polars_redis as pr
        import redis as redis_client

        stream = unique_stream_name()
        group = unique_group_name()
        client = redis_client.Redis.from_url(redis_url)
        stop_flag = threading.Event()

        def add_entries() -> None:
            time.sleep(0.1)
            for i in range(10):
                if stop_flag.is_set():
                    break
                client.xadd(stream, {"seq": str(i)})
                time.sleep(0.05)

        thread = threading.Thread(target=add_entries)
        thread.start()

        try:
            batches = []
            for batch in pr.iter_stream(
                redis_url,
                stream=stream,
                group=group,
                consumer="worker-1",
                batch_size=5,
                block_ms=2000,
                auto_ack=True,
            ):
                batches.append(batch)
                if len(batches) >= 2:
                    stop_flag.set()
                    break

            thread.join()

            assert len(batches) >= 1

        finally:
            stop_flag.set()
            thread.join()
            client.delete(stream)
            client.close()


class TestIterStreamAsync:
    """Tests for iter_stream_async async iterator."""

    @pytest.mark.asyncio
    async def test_iter_stream_async_basic(self, redis_url: str, redis_available: bool) -> None:
        """Test basic async batch iteration."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        stream = unique_stream_name()
        client = redis_client.Redis.from_url(redis_url)

        async def add_entries() -> None:
            await asyncio.sleep(0.1)
            for i in range(10):
                client.xadd(stream, {"seq": str(i)})
                await asyncio.sleep(0.05)

        add_task = asyncio.create_task(add_entries())

        try:
            batches = []
            async for batch in pr.iter_stream_async(
                redis_url,
                stream=stream,
                batch_size=5,
                batch_timeout_ms=2000,
            ):
                batches.append(batch)
                if len(batches) >= 2:
                    break

            await add_task

            assert len(batches) >= 1

        finally:
            client.delete(stream)
            client.close()


class TestStreamsUnit:
    """Unit tests that don't require extensive Redis setup."""

    def test_imports(self) -> None:
        """Test that stream functions are importable."""
        from polars_redis import (
            ack_entries,
            iter_stream,
            iter_stream_async,
            read_stream,
            scan_stream,
        )

        assert callable(read_stream)
        assert callable(scan_stream)
        assert callable(iter_stream)
        assert callable(iter_stream_async)
        assert callable(ack_entries)

    def test_read_stream_signature(self) -> None:
        """Test function signature includes expected parameters."""
        import inspect

        from polars_redis import read_stream

        sig = inspect.signature(read_stream)
        params = set(sig.parameters.keys())

        expected = {
            "url",
            "stream",
            "schema",
            "start_id",
            "end_id",
            "count",
            "block_ms",
            "group",
            "consumer",
            "auto_ack",
            "create_group",
            "include_id",
            "id_column",
            "include_timestamp",
            "timestamp_column",
            "include_sequence",
            "sequence_column",
        }

        assert expected.issubset(params)

    def test_empty_stream_with_schema(self, redis_url: str, redis_available: bool) -> None:
        """Test empty stream returns DataFrame with correct schema."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr

        stream = unique_stream_name()

        df = pr.read_stream(
            redis_url,
            stream=stream,
            schema={"field1": pl.Int64, "field2": pl.Utf8},
            include_id=True,
            include_timestamp=True,
            include_sequence=True,
        )

        assert len(df) == 0
        assert "_id" in df.columns
        assert "_ts" in df.columns
        assert "_seq" in df.columns
        assert "field1" in df.columns
        assert "field2" in df.columns
