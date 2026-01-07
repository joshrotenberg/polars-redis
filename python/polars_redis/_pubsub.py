"""Redis Pub/Sub support for collecting messages into DataFrames.

This module provides functions for subscribing to Redis Pub/Sub channels
and accumulating messages into DataFrames for real-time analytics.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from collections.abc import AsyncIterator, Iterator
from typing import Any, Callable, Literal

import polars as pl


def collect_pubsub(
    url: str,
    channels: list[str],
    *,
    count: int | None = None,
    timeout_ms: int | None = None,
    window_seconds: float | None = None,
    pattern: bool = False,
    message_format: Literal["raw", "json"] = "raw",
    parser: Callable[[str, bytes], dict] | None = None,
    schema: dict[str, pl.DataType] | None = None,
    include_channel: bool = False,
    include_timestamp: bool = False,
    channel_column: str = "_channel",
    message_column: str = "message",
    timestamp_column: str = "_received_at",
) -> pl.DataFrame:
    """Collect messages from Redis Pub/Sub channels into a DataFrame.

    Subscribes to the specified channels and collects messages until one of
    the termination conditions is met.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        channels: List of channel names or patterns to subscribe to.
        count: Maximum number of messages to collect.
        timeout_ms: Timeout in milliseconds.
        window_seconds: Time window for collection in seconds.
        pattern: If True, use pattern subscription (PSUBSCRIBE).
        message_format: Format of messages ("raw" or "json").
        parser: Custom function to parse messages. Receives (channel, payload)
            and returns a dict of column values.
        schema: Schema for parsed message fields. Required if using parser or
            message_format="json".
        include_channel: Include the channel name as a column.
        include_timestamp: Include the receive timestamp as a column.
        channel_column: Name of the channel column.
        message_column: Name of the message column (for raw format).
        timestamp_column: Name of the timestamp column.

    Returns:
        DataFrame containing the collected messages.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> # Collect 100 messages
        >>> df = redis.collect_pubsub(
        ...     "redis://localhost",
        ...     channels=["events"],
        ...     count=100,
        ...     timeout_ms=5000,
        ... )
        >>>
        >>> # Collect JSON messages with schema
        >>> df = redis.collect_pubsub(
        ...     "redis://localhost",
        ...     channels=["json_events"],
        ...     message_format="json",
        ...     schema={"user_id": pl.Int64, "action": pl.Utf8},
        ...     count=50,
        ... )
    """
    import redis as redis_client

    client = redis_client.Redis.from_url(url)
    pubsub = client.pubsub()

    try:
        # Subscribe to channels
        if pattern:
            pubsub.psubscribe(*channels)
        else:
            pubsub.subscribe(*channels)

        messages: list[dict[str, Any]] = []
        start_time = time.time()

        # Calculate end conditions
        max_count = count if count is not None else float("inf")
        deadline = None
        if timeout_ms is not None:
            deadline = start_time + timeout_ms / 1000
        if window_seconds is not None:
            window_deadline = start_time + window_seconds
            if deadline is None or window_deadline < deadline:
                deadline = window_deadline

        while len(messages) < max_count:
            # Check deadline
            if deadline is not None and time.time() >= deadline:
                break

            # Calculate timeout for get_message
            remaining = None
            if deadline is not None:
                remaining = max(0, deadline - time.time())

            # Get next message (with timeout in seconds)
            msg = pubsub.get_message(timeout=remaining if remaining else 1.0)

            if msg is None:
                if deadline is not None and time.time() >= deadline:
                    break
                continue

            # Skip subscribe confirmations
            if msg["type"] not in ("message", "pmessage"):
                continue

            channel = msg["channel"]
            if isinstance(channel, bytes):
                channel = channel.decode("utf-8")

            payload = msg["data"]
            if isinstance(payload, bytes):
                payload_str = payload.decode("utf-8")
            else:
                payload_str = str(payload)

            received_at = time.time()

            # Parse the message
            if parser is not None:
                row = parser(channel, msg["data"])
            elif message_format == "json":
                try:
                    row = json.loads(payload_str)
                except json.JSONDecodeError:
                    row = {message_column: payload_str}
            else:
                row = {message_column: payload_str}

            # Add metadata columns
            if include_channel:
                row[channel_column] = channel
            if include_timestamp:
                row[timestamp_column] = received_at

            messages.append(row)

    finally:
        pubsub.close()
        client.close()

    # Build DataFrame
    if not messages:
        # Return empty DataFrame with appropriate schema
        if schema is not None:
            columns = {}
            if include_channel:
                columns[channel_column] = pl.Series([], dtype=pl.Utf8)
            if include_timestamp:
                columns[timestamp_column] = pl.Series([], dtype=pl.Float64)
            for name, dtype in schema.items():
                columns[name] = pl.Series([], dtype=dtype)
            return pl.DataFrame(columns)
        else:
            columns = {}
            if include_channel:
                columns[channel_column] = pl.Series([], dtype=pl.Utf8)
            if include_timestamp:
                columns[timestamp_column] = pl.Series([], dtype=pl.Float64)
            columns[message_column] = pl.Series([], dtype=pl.Utf8)
            return pl.DataFrame(columns)

    df = pl.DataFrame(messages)

    # Cast to schema if provided
    if schema is not None:
        for name, dtype in schema.items():
            if name in df.columns:
                df = df.with_columns(pl.col(name).cast(dtype))

    return df


async def subscribe_batches(
    url: str,
    channels: list[str],
    *,
    batch_size: int = 100,
    batch_timeout_ms: int = 1000,
    pattern: bool = False,
    message_format: Literal["raw", "json"] = "raw",
    parser: Callable[[str, bytes], dict] | None = None,
    schema: dict[str, pl.DataType] | None = None,
    include_channel: bool = False,
    include_timestamp: bool = False,
    channel_column: str = "_channel",
    message_column: str = "message",
    timestamp_column: str = "_received_at",
) -> AsyncIterator[pl.DataFrame]:
    """Async iterator that yields batches of messages as DataFrames.

    Subscribes to the specified channels and yields DataFrames containing
    batches of messages. Each batch contains up to `batch_size` messages
    or is yielded after `batch_timeout_ms` milliseconds, whichever comes first.

    Args:
        url: Redis connection URL.
        channels: List of channel names or patterns.
        batch_size: Maximum messages per batch.
        batch_timeout_ms: Timeout to yield partial batch (milliseconds).
        pattern: If True, use pattern subscription.
        message_format: Format of messages ("raw" or "json").
        parser: Custom message parser function.
        schema: Schema for parsed message fields.
        include_channel: Include channel name column.
        include_timestamp: Include receive timestamp column.
        channel_column: Name of channel column.
        message_column: Name of message column.
        timestamp_column: Name of timestamp column.

    Yields:
        DataFrames containing batches of messages.

    Example:
        >>> import asyncio
        >>> import polars_redis as redis
        >>>
        >>> async def process_events():
        ...     async for batch_df in redis.subscribe_batches(
        ...         "redis://localhost",
        ...         channels=["events"],
        ...         batch_size=100,
        ...         batch_timeout_ms=1000,
        ...     ):
        ...         summary = batch_df.group_by("type").len()
        ...         print(summary)
        >>>
        >>> asyncio.run(process_events())
    """
    import redis.asyncio as redis_async

    client = redis_async.Redis.from_url(url)
    pubsub = client.pubsub()

    try:
        # Subscribe to channels
        if pattern:
            await pubsub.psubscribe(*channels)
        else:
            await pubsub.subscribe(*channels)

        messages: list[dict[str, Any]] = []
        batch_start = time.time()
        batch_timeout_sec = batch_timeout_ms / 1000

        while True:
            # Calculate remaining time for this batch
            elapsed = time.time() - batch_start
            remaining = max(0, batch_timeout_sec - elapsed)

            try:
                # Wait for message with timeout
                msg = await asyncio.wait_for(
                    pubsub.get_message(ignore_subscribe_messages=True),
                    timeout=remaining if remaining > 0 else 0.1,
                )
            except asyncio.TimeoutError:
                msg = None

            if msg is not None and msg["type"] in ("message", "pmessage"):
                channel = msg["channel"]
                if isinstance(channel, bytes):
                    channel = channel.decode("utf-8")

                payload = msg["data"]
                if isinstance(payload, bytes):
                    payload_str = payload.decode("utf-8")
                else:
                    payload_str = str(payload)

                received_at = time.time()

                # Parse the message
                if parser is not None:
                    row = parser(channel, msg["data"])
                elif message_format == "json":
                    try:
                        row = json.loads(payload_str)
                    except json.JSONDecodeError:
                        row = {message_column: payload_str}
                else:
                    row = {message_column: payload_str}

                # Add metadata columns
                if include_channel:
                    row[channel_column] = channel
                if include_timestamp:
                    row[timestamp_column] = received_at

                messages.append(row)

            # Check if we should yield a batch
            should_yield = len(messages) >= batch_size or (
                len(messages) > 0 and time.time() - batch_start >= batch_timeout_sec
            )

            if should_yield:
                df = pl.DataFrame(messages)

                # Cast to schema if provided
                if schema is not None:
                    for name, dtype in schema.items():
                        if name in df.columns:
                            df = df.with_columns(pl.col(name).cast(dtype))

                yield df

                # Reset for next batch
                messages = []
                batch_start = time.time()

    finally:
        await pubsub.close()
        await client.close()


def iter_batches(
    url: str,
    channels: list[str],
    *,
    batch_size: int = 100,
    batch_timeout_ms: int = 1000,
    pattern: bool = False,
    message_format: Literal["raw", "json"] = "raw",
    parser: Callable[[str, bytes], dict] | None = None,
    schema: dict[str, pl.DataType] | None = None,
    include_channel: bool = False,
    include_timestamp: bool = False,
    channel_column: str = "_channel",
    message_column: str = "message",
    timestamp_column: str = "_received_at",
) -> Iterator[pl.DataFrame]:
    """Synchronous iterator that yields batches of messages as DataFrames.

    This is a synchronous wrapper around subscribe_batches for use in
    non-async contexts.

    Args:
        url: Redis connection URL.
        channels: List of channel names or patterns.
        batch_size: Maximum messages per batch.
        batch_timeout_ms: Timeout to yield partial batch (milliseconds).
        pattern: If True, use pattern subscription.
        message_format: Format of messages ("raw" or "json").
        parser: Custom message parser function.
        schema: Schema for parsed message fields.
        include_channel: Include channel name column.
        include_timestamp: Include receive timestamp column.
        channel_column: Name of channel column.
        message_column: Name of message column.
        timestamp_column: Name of timestamp column.

    Yields:
        DataFrames containing batches of messages.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> for batch_df in redis.iter_batches(
        ...     "redis://localhost",
        ...     channels=["events"],
        ...     batch_size=100,
        ... ):
        ...     print(f"Got {len(batch_df)} messages")
        ...     if should_stop():
        ...         break
    """
    import redis as redis_client

    client = redis_client.Redis.from_url(url)
    pubsub = client.pubsub()

    try:
        # Subscribe to channels
        if pattern:
            pubsub.psubscribe(*channels)
        else:
            pubsub.subscribe(*channels)

        messages: list[dict[str, Any]] = []
        batch_start = time.time()
        batch_timeout_sec = batch_timeout_ms / 1000

        while True:
            # Calculate remaining time for this batch
            elapsed = time.time() - batch_start
            remaining = max(0.01, batch_timeout_sec - elapsed)

            # Get message with timeout
            msg = pubsub.get_message(timeout=remaining)

            if msg is not None and msg["type"] in ("message", "pmessage"):
                channel = msg["channel"]
                if isinstance(channel, bytes):
                    channel = channel.decode("utf-8")

                payload = msg["data"]
                if isinstance(payload, bytes):
                    payload_str = payload.decode("utf-8")
                else:
                    payload_str = str(payload)

                received_at = time.time()

                # Parse the message
                if parser is not None:
                    row = parser(channel, msg["data"])
                elif message_format == "json":
                    try:
                        row = json.loads(payload_str)
                    except json.JSONDecodeError:
                        row = {message_column: payload_str}
                else:
                    row = {message_column: payload_str}

                # Add metadata columns
                if include_channel:
                    row[channel_column] = channel
                if include_timestamp:
                    row[timestamp_column] = received_at

                messages.append(row)

            # Check if we should yield a batch
            should_yield = len(messages) >= batch_size or (
                len(messages) > 0 and time.time() - batch_start >= batch_timeout_sec
            )

            if should_yield:
                df = pl.DataFrame(messages)

                # Cast to schema if provided
                if schema is not None:
                    for name, dtype in schema.items():
                        if name in df.columns:
                            df = df.with_columns(pl.col(name).cast(dtype))

                yield df

                # Reset for next batch
                messages = []
                batch_start = time.time()

    finally:
        pubsub.close()
        client.close()
