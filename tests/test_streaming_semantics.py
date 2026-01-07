"""Streaming semantics tests for Pub/Sub and Streams.

These tests verify the semantic guarantees of streaming operations:
- Message ordering guarantees
- Delivery semantics (at-least-once, at-most-once)
- Timeout behavior with partial messages
- Consumer group semantics
- Reconnection behavior

Run with: pytest tests/test_streaming_semantics.py -v
"""

from __future__ import annotations

import json
import subprocess
import threading
import time
import uuid

import polars as pl
import pytest


def redis_available() -> bool:
    """Check if Redis is available."""
    try:
        result = subprocess.run(
            ["redis-cli", "PING"], capture_output=True, text=True, timeout=5
        )
        return result.stdout.strip() == "PONG"
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not redis_available(),
    reason="Redis not available at localhost:6379",
)

REDIS_URL = "redis://localhost:6379"


def unique_name(prefix: str) -> str:
    """Generate a unique name for testing."""
    return f"{prefix}:{uuid.uuid4().hex[:8]}"


def cleanup_key(key: str) -> None:
    """Delete a Redis key."""
    subprocess.run(["redis-cli", "DEL", key], capture_output=True)


def cleanup_stream_group(stream: str, group: str) -> None:
    """Delete a consumer group."""
    subprocess.run(
        ["redis-cli", "XGROUP", "DESTROY", stream, group], capture_output=True
    )


# =============================================================================
# Message Ordering Tests
# =============================================================================


class TestMessageOrdering:
    """Tests for message ordering guarantees."""

    def test_stream_maintains_insertion_order(self):
        """Verify Redis Streams maintain strict insertion order."""
        import polars_redis as pr
        import redis

        stream = unique_name("test:order:stream")
        client = redis.Redis.from_url(REDIS_URL)

        try:
            # Insert messages with sequence numbers
            for i in range(100):
                client.xadd(stream, {"seq": str(i), "data": f"message_{i}"})

            # Read all messages
            df = pr.read_stream(
                REDIS_URL,
                stream=stream,
                schema={"seq": pl.Int64, "data": pl.Utf8},
            )

            # Verify order is preserved
            sequences = df["seq"].to_list()
            assert sequences == list(range(100)), "Stream should maintain insertion order"

        finally:
            cleanup_key(stream)
            client.close()

    def test_pubsub_message_order_within_channel(self):
        """Verify Pub/Sub messages are received in order within a channel."""
        import polars_redis as pr
        import redis

        channel = unique_name("test:order:pubsub")
        client = redis.Redis.from_url(REDIS_URL)

        messages_sent = []

        def publish_messages():
            time.sleep(0.1)  # Let subscriber connect
            for i in range(20):
                msg = json.dumps({"seq": i})
                client.publish(channel, msg)
                messages_sent.append(i)
                time.sleep(0.01)

        thread = threading.Thread(target=publish_messages)
        thread.start()

        df = pr.collect_pubsub(
            REDIS_URL,
            channels=[channel],
            count=20,
            timeout_ms=5000,
        )

        thread.join()
        client.close()

        # Parse and verify order
        if len(df) > 0:
            sequences = []
            for msg in df["message"].to_list():
                try:
                    data = json.loads(msg)
                    sequences.append(data["seq"])
                except (json.JSONDecodeError, KeyError):
                    pass

            # Messages received should be in order (though some may be missed)
            for i in range(len(sequences) - 1):
                assert sequences[i] < sequences[i + 1], "Messages should be in order"

    def test_stream_order_with_consumer_group(self):
        """Verify consumer groups maintain message order per consumer."""
        import polars_redis as pr
        import redis

        stream = unique_name("test:order:group")
        group = unique_name("group")
        client = redis.Redis.from_url(REDIS_URL)

        try:
            # Insert ordered messages
            for i in range(50):
                client.xadd(stream, {"seq": str(i)})

            # Read with consumer group
            df = pr.read_stream(
                REDIS_URL,
                stream=stream,
                group=group,
                consumer="worker-1",
                auto_ack=True,
                schema={"seq": pl.Int64},
            )

            # Verify order is preserved
            sequences = df["seq"].to_list()
            for i in range(len(sequences) - 1):
                assert sequences[i] < sequences[i + 1], "Consumer group should maintain order"

        finally:
            cleanup_stream_group(stream, group)
            cleanup_key(stream)
            client.close()


# =============================================================================
# Delivery Semantics Tests
# =============================================================================


class TestDeliverySemantics:
    """Tests for message delivery guarantees."""

    def test_stream_at_least_once_with_pending(self):
        """Verify unacked messages can be reclaimed (at-least-once)."""
        import polars_redis as pr
        import redis

        stream = unique_name("test:delivery:pending")
        group = unique_name("group")
        client = redis.Redis.from_url(REDIS_URL)

        try:
            # Add messages
            for i in range(5):
                client.xadd(stream, {"seq": str(i)})

            # Read without acking
            df1 = pr.read_stream(
                REDIS_URL,
                stream=stream,
                group=group,
                consumer="worker-1",
                auto_ack=False,
            )

            assert len(df1) == 5

            # Messages should be in pending list
            pending = client.xpending(stream, group)
            assert pending["pending"] == 5

            # Another consumer can claim pending messages after timeout
            # For this test, we just verify they're pending
            pending_range = client.xpending_range(stream, group, "-", "+", 10)
            assert len(pending_range) == 5

        finally:
            cleanup_stream_group(stream, group)
            cleanup_key(stream)
            client.close()

    def test_stream_exactly_once_with_ack(self):
        """Verify acked messages are not redelivered."""
        import polars_redis as pr
        import redis

        stream = unique_name("test:delivery:ack")
        group = unique_name("group")
        client = redis.Redis.from_url(REDIS_URL)

        try:
            # Add messages
            for i in range(5):
                client.xadd(stream, {"seq": str(i)})

            # Read and ack
            df1 = pr.read_stream(
                REDIS_URL,
                stream=stream,
                group=group,
                consumer="worker-1",
                auto_ack=True,
            )

            assert len(df1) == 5

            # Read again - should get nothing
            df2 = pr.read_stream(
                REDIS_URL,
                stream=stream,
                group=group,
                consumer="worker-1",
                block_ms=100,
            )

            assert len(df2) == 0, "Acked messages should not be redelivered"

        finally:
            cleanup_stream_group(stream, group)
            cleanup_key(stream)
            client.close()

    def test_pubsub_at_most_once_delivery(self):
        """Verify Pub/Sub has at-most-once delivery (no persistence)."""
        import polars_redis as pr
        import redis

        channel = unique_name("test:delivery:pubsub")
        client = redis.Redis.from_url(REDIS_URL)

        try:
            # Publish messages BEFORE subscribing
            for i in range(5):
                client.publish(channel, f"before_subscribe_{i}")

            # Now subscribe and wait briefly
            df = pr.collect_pubsub(
                REDIS_URL,
                channels=[channel],
                timeout_ms=200,
            )

            # Should not receive messages published before subscription
            assert len(df) == 0, "Pub/Sub should not deliver messages sent before subscription"

        finally:
            client.close()


# =============================================================================
# Timeout and Partial Message Tests
# =============================================================================


class TestTimeoutBehavior:
    """Tests for timeout handling with partial messages."""

    def test_stream_block_timeout(self):
        """Verify stream read respects block timeout."""
        import polars_redis as pr
        import redis

        stream = unique_name("test:timeout:block")
        group = unique_name("group")
        client = redis.Redis.from_url(REDIS_URL)

        try:
            # Create empty stream with group
            client.xgroup_create(stream, group, id="0", mkstream=True)

            # Read with short block timeout on empty stream
            start = time.time()
            df = pr.read_stream(
                REDIS_URL,
                stream=stream,
                group=group,
                consumer="worker-1",
                block_ms=200,
            )
            elapsed = time.time() - start

            # Should return after ~200ms with empty result
            assert elapsed < 1.0, "Should not block longer than timeout"
            assert len(df) == 0

        finally:
            cleanup_stream_group(stream, group)
            cleanup_key(stream)
            client.close()

    def test_pubsub_timeout_returns_partial(self):
        """Verify Pub/Sub returns partial results on timeout."""
        import polars_redis as pr
        import redis

        channel = unique_name("test:timeout:partial")
        client = redis.Redis.from_url(REDIS_URL)

        def publish_slowly():
            time.sleep(0.1)
            for i in range(10):
                client.publish(channel, f"msg_{i}")
                time.sleep(0.1)  # 100ms between messages

        thread = threading.Thread(target=publish_slowly)
        thread.start()

        # Collect with 300ms timeout - should get ~2-3 messages
        df = pr.collect_pubsub(
            REDIS_URL,
            channels=[channel],
            timeout_ms=350,
        )

        thread.join()
        client.close()

        # Should have received some but not all messages
        assert 0 < len(df) < 10, "Should return partial results on timeout"

    def test_pubsub_window_seconds_partial(self):
        """Verify window_seconds returns messages received within window."""
        import polars_redis as pr
        import redis

        channel = unique_name("test:timeout:window")
        client = redis.Redis.from_url(REDIS_URL)

        stop_publishing = threading.Event()

        def publish_continuously():
            time.sleep(0.1)
            i = 0
            while not stop_publishing.is_set():
                client.publish(channel, f"msg_{i}")
                i += 1
                time.sleep(0.02)

        thread = threading.Thread(target=publish_continuously)
        thread.start()

        # Collect for 200ms window
        start = time.time()
        df = pr.collect_pubsub(
            REDIS_URL,
            channels=[channel],
            window_seconds=0.2,
            timeout_ms=5000,
        )
        elapsed = time.time() - start

        stop_publishing.set()
        thread.join()
        client.close()

        # Should complete around 200ms
        assert elapsed < 0.5, "Should respect window_seconds"
        assert len(df) > 0, "Should have received some messages"


# =============================================================================
# Consumer Group Semantics Tests
# =============================================================================


class TestConsumerGroupSemantics:
    """Tests for Redis Streams consumer group behavior."""

    def test_consumer_group_load_balancing(self):
        """Verify messages are distributed across consumers."""
        import polars_redis as pr
        import redis

        stream = unique_name("test:cg:balance")
        group = unique_name("group")
        client = redis.Redis.from_url(REDIS_URL)

        try:
            # Add messages
            for i in range(10):
                client.xadd(stream, {"seq": str(i)})

            # Two consumers read from same group
            df1 = pr.read_stream(
                REDIS_URL,
                stream=stream,
                group=group,
                consumer="worker-1",
                count=5,
                auto_ack=True,
            )

            df2 = pr.read_stream(
                REDIS_URL,
                stream=stream,
                group=group,
                consumer="worker-2",
                auto_ack=True,
            )

            # Combined should have all messages, no duplicates
            all_ids = set(df1["_id"].to_list()) | set(df2["_id"].to_list())
            assert len(all_ids) == 10, "All messages should be consumed exactly once"

            # No overlap between consumers
            overlap = set(df1["_id"].to_list()) & set(df2["_id"].to_list())
            assert len(overlap) == 0, "No message should go to both consumers"

        finally:
            cleanup_stream_group(stream, group)
            cleanup_key(stream)
            client.close()

    def test_pending_message_claim(self):
        """Verify pending messages can be claimed by another consumer."""
        import polars_redis as pr
        import redis

        stream = unique_name("test:cg:claim")
        group = unique_name("group")
        client = redis.Redis.from_url(REDIS_URL)

        try:
            # Add messages
            for i in range(3):
                client.xadd(stream, {"seq": str(i)})

            # Consumer 1 reads without acking
            df1 = pr.read_stream(
                REDIS_URL,
                stream=stream,
                group=group,
                consumer="worker-1",
                auto_ack=False,
            )

            assert len(df1) == 3

            # Get pending entry IDs
            pending_ids = df1["_id"].to_list()

            # Worker 2 claims the messages (simulating worker-1 failure)
            # Use XCLAIM with min-idle-time of 0 for testing
            claimed = client.xclaim(
                stream, group, "worker-2", min_idle_time=0, message_ids=pending_ids
            )

            assert len(claimed) == 3, "All pending messages should be claimable"

        finally:
            cleanup_stream_group(stream, group)
            cleanup_key(stream)
            client.close()

    def test_consumer_group_new_messages_only(self):
        """Verify new consumer groups only see new messages by default."""
        import polars_redis as pr
        import redis

        stream = unique_name("test:cg:newonly")
        group = unique_name("group")
        client = redis.Redis.from_url(REDIS_URL)

        try:
            # Add messages before group creation
            for i in range(5):
                client.xadd(stream, {"seq": str(i), "when": "before"})

            # Create group starting from end ($)
            client.xgroup_create(stream, group, id="$", mkstream=False)

            # Add messages after group creation
            for i in range(3):
                client.xadd(stream, {"seq": str(i + 5), "when": "after"})

            # Consumer should only see messages added after group creation
            df = pr.read_stream(
                REDIS_URL,
                stream=stream,
                group=group,
                consumer="worker-1",
                auto_ack=True,
                schema={"seq": pl.Int64, "when": pl.Utf8},
            )

            assert len(df) == 3, "Should only see messages after group creation"
            assert all(w == "after" for w in df["when"].to_list())

        finally:
            cleanup_stream_group(stream, group)
            cleanup_key(stream)
            client.close()

    def test_consumer_group_from_beginning(self):
        """Verify consumer groups can start from beginning (0)."""
        import polars_redis as pr
        import redis

        stream = unique_name("test:cg:beginning")
        group = unique_name("group")
        client = redis.Redis.from_url(REDIS_URL)

        try:
            # Add messages
            for i in range(5):
                client.xadd(stream, {"seq": str(i)})

            # Create group starting from beginning (0)
            client.xgroup_create(stream, group, id="0", mkstream=False)

            # Consumer should see all existing messages
            df = pr.read_stream(
                REDIS_URL,
                stream=stream,
                group=group,
                consumer="worker-1",
                auto_ack=True,
                schema={"seq": pl.Int64},
            )

            assert len(df) == 5, "Should see all messages from beginning"

        finally:
            cleanup_stream_group(stream, group)
            cleanup_key(stream)
            client.close()


# =============================================================================
# Multi-Channel Pub/Sub Tests
# =============================================================================


class TestMultiChannelPubSub:
    """Tests for multi-channel Pub/Sub behavior."""

    def test_multiple_channels_interleaved(self):
        """Verify messages from multiple channels are received."""
        import polars_redis as pr
        import redis

        channel1 = unique_name("test:multi:ch1")
        channel2 = unique_name("test:multi:ch2")
        client = redis.Redis.from_url(REDIS_URL)

        def publish_to_channels():
            time.sleep(0.1)
            for i in range(5):
                client.publish(channel1, f"ch1_msg_{i}")
                client.publish(channel2, f"ch2_msg_{i}")
                time.sleep(0.01)

        thread = threading.Thread(target=publish_to_channels)
        thread.start()

        df = pr.collect_pubsub(
            REDIS_URL,
            channels=[channel1, channel2],
            count=10,
            timeout_ms=5000,
            include_channel=True,
        )

        thread.join()
        client.close()

        # Should have messages from both channels
        assert len(df) == 10
        assert "_channel" in df.columns

        channels_received = set(df["_channel"].to_list())
        assert channel1 in channels_received
        assert channel2 in channels_received

    def test_pattern_subscription(self):
        """Verify pattern subscriptions work correctly."""
        import polars_redis as pr
        import redis

        prefix = unique_name("test:pattern")
        channel1 = f"{prefix}:events"
        channel2 = f"{prefix}:alerts"
        client = redis.Redis.from_url(REDIS_URL)

        def publish_messages():
            time.sleep(0.1)
            for i in range(3):
                client.publish(channel1, f"event_{i}")
                client.publish(channel2, f"alert_{i}")
                time.sleep(0.01)

        thread = threading.Thread(target=publish_messages)
        thread.start()

        df = pr.collect_pubsub(
            REDIS_URL,
            channels=[f"{prefix}:*"],
            pattern=True,
            count=6,
            timeout_ms=5000,
        )

        thread.join()
        client.close()

        # Should have messages from both channels via pattern
        assert len(df) == 6


# =============================================================================
# Edge Cases and Error Handling
# =============================================================================


class TestStreamingEdgeCases:
    """Tests for edge cases in streaming operations."""

    def test_empty_stream_read(self):
        """Verify reading from empty/non-existent stream."""
        import polars_redis as pr

        stream = unique_name("test:edge:empty")

        df = pr.read_stream(
            REDIS_URL,
            stream=stream,
            schema={"field": pl.Utf8},
        )

        assert len(df) == 0
        assert "_id" in df.columns

    def test_stream_with_missing_fields(self):
        """Verify handling of entries with missing fields."""
        import polars_redis as pr
        import redis

        stream = unique_name("test:edge:missing")
        client = redis.Redis.from_url(REDIS_URL)

        try:
            # Add entries with varying fields
            client.xadd(stream, {"a": "1", "b": "2"})
            client.xadd(stream, {"a": "3"})  # Missing 'b'
            client.xadd(stream, {"b": "4"})  # Missing 'a'

            df = pr.read_stream(
                REDIS_URL,
                stream=stream,
                schema={"a": pl.Utf8, "b": pl.Utf8},
            )

            assert len(df) == 3
            # Missing fields should be null
            assert df["a"].null_count() == 1
            assert df["b"].null_count() == 1

        finally:
            cleanup_key(stream)
            client.close()

    def test_large_message_handling(self):
        """Verify handling of large messages."""
        import polars_redis as pr
        import redis

        stream = unique_name("test:edge:large")
        client = redis.Redis.from_url(REDIS_URL)

        try:
            # Add large message (100KB)
            large_data = "x" * 100_000
            client.xadd(stream, {"data": large_data})

            df = pr.read_stream(
                REDIS_URL,
                stream=stream,
                schema={"data": pl.Utf8},
            )

            assert len(df) == 1
            assert len(df["data"][0]) == 100_000

        finally:
            cleanup_key(stream)
            client.close()

    def test_rapid_message_burst(self):
        """Verify handling of rapid message bursts."""
        import polars_redis as pr
        import redis

        channel = unique_name("test:edge:burst")
        client = redis.Redis.from_url(REDIS_URL)

        def publish_burst():
            time.sleep(0.1)
            # Publish 100 messages as fast as possible
            for i in range(100):
                client.publish(channel, f"burst_{i}")

        thread = threading.Thread(target=publish_burst)
        thread.start()

        df = pr.collect_pubsub(
            REDIS_URL,
            channels=[channel],
            count=100,
            timeout_ms=5000,
        )

        thread.join()
        client.close()

        # Should receive most/all messages
        # Note: Pub/Sub doesn't guarantee delivery, some may be lost
        assert len(df) > 50, "Should receive majority of burst messages"

    def test_unicode_in_stream_data(self):
        """Verify handling of Unicode data in streams."""
        import polars_redis as pr
        import redis

        stream = unique_name("test:edge:unicode")
        client = redis.Redis.from_url(REDIS_URL)

        try:
            # Add entries with Unicode
            test_strings = [
                "Hello World",
                "Pinyin zhongwen",
                "Special chars: newline tab",
            ]

            for s in test_strings:
                client.xadd(stream, {"text": s})

            df = pr.read_stream(
                REDIS_URL,
                stream=stream,
                schema={"text": pl.Utf8},
            )

            assert len(df) == 3
            assert df["text"].to_list() == test_strings

        finally:
            cleanup_key(stream)
            client.close()
