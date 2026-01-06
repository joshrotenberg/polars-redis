"""Tests for Redis Pub/Sub DataFrame streaming.

These tests verify the pubsub module functionality for collecting
Redis Pub/Sub messages into DataFrames.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from typing import Any

import polars as pl
import pytest


class TestCollectPubsub:
    """Tests for collect_pubsub function."""

    def test_collect_with_count_limit(self, redis_url: str, redis_available: bool) -> None:
        """Test collecting a specific number of messages."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        channel = "test:pubsub:count"
        client = redis_client.Redis.from_url(redis_url)

        # Publish messages in a background thread
        def publish_messages() -> None:
            time.sleep(0.1)  # Give subscriber time to connect
            for i in range(5):
                client.publish(channel, f"message_{i}")
                time.sleep(0.01)

        thread = threading.Thread(target=publish_messages)
        thread.start()

        # Collect exactly 3 messages
        df = pr.collect_pubsub(
            redis_url,
            channels=[channel],
            count=3,
            timeout_ms=5000,
        )

        thread.join()
        client.close()

        assert len(df) == 3
        assert "message" in df.columns

    def test_collect_with_timeout(self, redis_url: str, redis_available: bool) -> None:
        """Test collecting messages with timeout."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        channel = "test:pubsub:timeout"
        client = redis_client.Redis.from_url(redis_url)

        # Publish a few messages
        def publish_messages() -> None:
            time.sleep(0.1)
            for i in range(2):
                client.publish(channel, f"msg_{i}")
                time.sleep(0.05)

        thread = threading.Thread(target=publish_messages)
        thread.start()

        # Collect with short timeout
        start = time.time()
        df = pr.collect_pubsub(
            redis_url,
            channels=[channel],
            timeout_ms=500,
        )
        elapsed = time.time() - start

        thread.join()
        client.close()

        # Should timeout around 500ms
        assert elapsed < 1.0
        assert len(df) >= 0  # May have received some messages

    def test_collect_with_window_seconds(self, redis_url: str, redis_available: bool) -> None:
        """Test collecting messages with time window."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        channel = "test:pubsub:window"
        client = redis_client.Redis.from_url(redis_url)

        # Publish messages continuously
        stop_publishing = threading.Event()

        def publish_messages() -> None:
            time.sleep(0.1)
            i = 0
            while not stop_publishing.is_set():
                client.publish(channel, f"msg_{i}")
                i += 1
                time.sleep(0.05)

        thread = threading.Thread(target=publish_messages)
        thread.start()

        # Collect for 0.3 seconds
        df = pr.collect_pubsub(
            redis_url,
            channels=[channel],
            window_seconds=0.3,
            timeout_ms=5000,
        )

        stop_publishing.set()
        thread.join()
        client.close()

        # Should have received several messages in 0.3 seconds
        assert len(df) > 0

    def test_collect_json_messages(self, redis_url: str, redis_available: bool) -> None:
        """Test collecting JSON formatted messages."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        channel = "test:pubsub:json"
        client = redis_client.Redis.from_url(redis_url)

        # Publish JSON messages
        def publish_messages() -> None:
            time.sleep(0.1)
            for i in range(3):
                msg = json.dumps({"id": i, "value": f"item_{i}", "score": i * 1.5})
                client.publish(channel, msg)
                time.sleep(0.01)

        thread = threading.Thread(target=publish_messages)
        thread.start()

        df = pr.collect_pubsub(
            redis_url,
            channels=[channel],
            count=3,
            timeout_ms=5000,
            message_format="json",
            schema={"id": pl.Int64, "value": pl.Utf8, "score": pl.Float64},
        )

        thread.join()
        client.close()

        assert len(df) == 3
        assert "id" in df.columns
        assert "value" in df.columns
        assert "score" in df.columns

    def test_collect_with_channel_column(self, redis_url: str, redis_available: bool) -> None:
        """Test including channel name as column."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        channel = "test:pubsub:channelcol"
        client = redis_client.Redis.from_url(redis_url)

        def publish_messages() -> None:
            time.sleep(0.1)
            client.publish(channel, "test_message")

        thread = threading.Thread(target=publish_messages)
        thread.start()

        df = pr.collect_pubsub(
            redis_url,
            channels=[channel],
            count=1,
            timeout_ms=5000,
            include_channel=True,
        )

        thread.join()
        client.close()

        assert "_channel" in df.columns
        assert df["_channel"][0] == channel

    def test_collect_with_timestamp_column(self, redis_url: str, redis_available: bool) -> None:
        """Test including receive timestamp as column."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        channel = "test:pubsub:timestamp"
        client = redis_client.Redis.from_url(redis_url)

        def publish_messages() -> None:
            time.sleep(0.1)
            client.publish(channel, "test_message")

        thread = threading.Thread(target=publish_messages)
        thread.start()

        before = time.time()
        df = pr.collect_pubsub(
            redis_url,
            channels=[channel],
            count=1,
            timeout_ms=5000,
            include_timestamp=True,
        )
        after = time.time()

        thread.join()
        client.close()

        assert "_received_at" in df.columns
        ts = df["_received_at"][0]
        assert before <= ts <= after

    def test_collect_pattern_subscribe(self, redis_url: str, redis_available: bool) -> None:
        """Test pattern subscription with PSUBSCRIBE."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        client = redis_client.Redis.from_url(redis_url)

        def publish_messages() -> None:
            time.sleep(0.1)
            client.publish("test:pattern:a", "msg_a")
            client.publish("test:pattern:b", "msg_b")
            client.publish("test:pattern:c", "msg_c")

        thread = threading.Thread(target=publish_messages)
        thread.start()

        df = pr.collect_pubsub(
            redis_url,
            channels=["test:pattern:*"],
            count=3,
            timeout_ms=5000,
            pattern=True,
            include_channel=True,
        )

        thread.join()
        client.close()

        assert len(df) == 3
        channels = set(df["_channel"].to_list())
        assert "test:pattern:a" in channels
        assert "test:pattern:b" in channels
        assert "test:pattern:c" in channels

    def test_collect_custom_parser(self, redis_url: str, redis_available: bool) -> None:
        """Test using a custom message parser."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        channel = "test:pubsub:parser"
        client = redis_client.Redis.from_url(redis_url)

        def custom_parser(channel: str, payload: bytes) -> dict[str, Any]:
            # Parse "key=value" format
            text = payload.decode("utf-8")
            parts = text.split("=")
            return {"key": parts[0], "value": parts[1] if len(parts) > 1 else ""}

        def publish_messages() -> None:
            time.sleep(0.1)
            client.publish(channel, "foo=bar")
            client.publish(channel, "hello=world")

        thread = threading.Thread(target=publish_messages)
        thread.start()

        df = pr.collect_pubsub(
            redis_url,
            channels=[channel],
            count=2,
            timeout_ms=5000,
            parser=custom_parser,
            schema={"key": pl.Utf8, "value": pl.Utf8},
        )

        thread.join()
        client.close()

        assert len(df) == 2
        assert "key" in df.columns
        assert "value" in df.columns

    def test_collect_empty_no_messages(self, redis_url: str, redis_available: bool) -> None:
        """Test collecting when no messages arrive."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr

        # Short timeout, no messages published
        df = pr.collect_pubsub(
            redis_url,
            channels=["test:pubsub:empty"],
            timeout_ms=100,
        )

        assert len(df) == 0
        assert "message" in df.columns

    def test_collect_custom_column_names(self, redis_url: str, redis_available: bool) -> None:
        """Test custom column names for channel and timestamp."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        channel = "test:pubsub:customcols"
        client = redis_client.Redis.from_url(redis_url)

        def publish_messages() -> None:
            time.sleep(0.1)
            client.publish(channel, "test")

        thread = threading.Thread(target=publish_messages)
        thread.start()

        df = pr.collect_pubsub(
            redis_url,
            channels=[channel],
            count=1,
            timeout_ms=5000,
            include_channel=True,
            include_timestamp=True,
            channel_column="source",
            message_column="payload",
            timestamp_column="ts",
        )

        thread.join()
        client.close()

        assert "source" in df.columns
        assert "payload" in df.columns
        assert "ts" in df.columns


class TestIterBatches:
    """Tests for iter_batches synchronous iterator."""

    def test_iter_batches_by_size(self, redis_url: str, redis_available: bool) -> None:
        """Test yielding batches by size."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        channel = "test:iter:size"
        client = redis_client.Redis.from_url(redis_url)
        stop_publishing = threading.Event()

        def publish_messages() -> None:
            time.sleep(0.1)
            for i in range(15):
                if stop_publishing.is_set():
                    break
                client.publish(channel, f"msg_{i}")
                time.sleep(0.02)

        thread = threading.Thread(target=publish_messages)
        thread.start()

        batches = []
        for batch in pr.iter_batches(
            redis_url,
            channels=[channel],
            batch_size=5,
            batch_timeout_ms=2000,
        ):
            batches.append(batch)
            if len(batches) >= 2:
                stop_publishing.set()
                break

        thread.join()
        client.close()

        assert len(batches) >= 1
        # First batch should have up to 5 messages
        assert len(batches[0]) <= 5

    def test_iter_batches_by_timeout(self, redis_url: str, redis_available: bool) -> None:
        """Test yielding batches by timeout."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        channel = "test:iter:timeout"
        client = redis_client.Redis.from_url(redis_url)

        def publish_messages() -> None:
            time.sleep(0.1)
            # Publish slowly so batch times out before filling
            for i in range(3):
                client.publish(channel, f"msg_{i}")
                time.sleep(0.2)

        thread = threading.Thread(target=publish_messages)
        thread.start()

        batches = []
        for batch in pr.iter_batches(
            redis_url,
            channels=[channel],
            batch_size=100,  # Large batch size
            batch_timeout_ms=150,  # Short timeout
        ):
            batches.append(batch)
            if len(batches) >= 2:
                break

        thread.join()
        client.close()

        assert len(batches) >= 1
        # Batches should be partial due to timeout
        for batch in batches:
            assert len(batch) < 100


class TestSubscribeBatches:
    """Tests for subscribe_batches async iterator."""

    @pytest.mark.asyncio
    async def test_subscribe_batches_basic(self, redis_url: str, redis_available: bool) -> None:
        """Test basic async batch iteration."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        channel = "test:async:basic"
        client = redis_client.Redis.from_url(redis_url)

        async def publish_messages() -> None:
            await asyncio.sleep(0.1)
            for i in range(10):
                client.publish(channel, f"async_msg_{i}")
                await asyncio.sleep(0.02)

        publish_task = asyncio.create_task(publish_messages())

        batches = []
        async for batch in pr.subscribe_batches(
            redis_url,
            channels=[channel],
            batch_size=5,
            batch_timeout_ms=2000,
        ):
            batches.append(batch)
            if len(batches) >= 2:
                break

        await publish_task
        client.close()

        assert len(batches) >= 1

    @pytest.mark.asyncio
    async def test_subscribe_batches_json(self, redis_url: str, redis_available: bool) -> None:
        """Test async iteration with JSON messages."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        channel = "test:async:json"
        client = redis_client.Redis.from_url(redis_url)

        async def publish_messages() -> None:
            await asyncio.sleep(0.1)
            for i in range(5):
                msg = json.dumps({"seq": i, "data": f"item_{i}"})
                client.publish(channel, msg)
                await asyncio.sleep(0.02)

        publish_task = asyncio.create_task(publish_messages())

        batches = []
        async for batch in pr.subscribe_batches(
            redis_url,
            channels=[channel],
            batch_size=5,
            batch_timeout_ms=2000,
            message_format="json",
            schema={"seq": pl.Int64, "data": pl.Utf8},
        ):
            batches.append(batch)
            if sum(len(b) for b in batches) >= 5:
                break

        await publish_task
        client.close()

        assert len(batches) >= 1
        total_msgs = sum(len(b) for b in batches)
        assert total_msgs >= 5


class TestPubsubUnit:
    """Unit tests that don't require Redis."""

    def test_empty_dataframe_with_schema(self, redis_url: str, redis_available: bool) -> None:
        """Test that empty DataFrame has correct schema."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr

        # Collect with very short timeout - should return empty DataFrame
        df = pr.collect_pubsub(
            redis_url,
            channels=["test:unit:empty_schema"],
            timeout_ms=50,
            schema={"id": pl.Int64, "name": pl.Utf8},
            include_channel=True,
            include_timestamp=True,
        )

        assert len(df) == 0
        assert "_channel" in df.columns
        assert "_received_at" in df.columns
        assert "id" in df.columns
        assert "name" in df.columns

    def test_imports(self) -> None:
        """Test that pubsub functions are importable."""
        from polars_redis import collect_pubsub, iter_batches, subscribe_batches

        assert callable(collect_pubsub)
        assert callable(iter_batches)
        assert callable(subscribe_batches)

    def test_collect_pubsub_signature(self) -> None:
        """Test function signature includes expected parameters."""
        import inspect

        from polars_redis import collect_pubsub

        sig = inspect.signature(collect_pubsub)
        params = set(sig.parameters.keys())

        expected = {
            "url",
            "channels",
            "count",
            "timeout_ms",
            "window_seconds",
            "pattern",
            "message_format",
            "parser",
            "schema",
            "include_channel",
            "include_timestamp",
            "channel_column",
            "message_column",
            "timestamp_column",
        }

        assert expected.issubset(params)
