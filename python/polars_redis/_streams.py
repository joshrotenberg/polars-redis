"""Redis Streams streaming support for real-time DataFrame processing.

This module provides functions for consuming Redis Streams as streaming data sources,
with support for consumer groups, blocking reads, and continuous batch iteration.

Note: This module differs from _scan.py's scan_streams/read_streams which scan
multiple streams by pattern. This module focuses on single-stream consumption
with consumer group support and real-time tailing.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Iterator
from typing import Any, Literal

import polars as pl


def read_stream(
    url: str,
    stream: str,
    *,
    schema: dict[str, pl.DataType] | None = None,
    start_id: str = "-",
    end_id: str = "+",
    count: int | None = None,
    block_ms: int | None = None,
    group: str | None = None,
    consumer: str | None = None,
    auto_ack: bool = False,
    create_group: bool = True,
    include_id: bool = True,
    id_column: str = "_id",
    include_timestamp: bool = True,
    timestamp_column: str = "_ts",
    include_sequence: bool = False,
    sequence_column: str = "_seq",
) -> pl.DataFrame:
    """Read entries from a Redis Stream into a DataFrame.

    This function reads entries from a single Redis Stream with support for
    consumer groups and blocking reads.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        stream: Name of the stream to read from.
        schema: Dictionary mapping field names to Polars dtypes.
        start_id: Start entry ID ("-" = oldest, "$" = only new entries).
        end_id: End entry ID ("+" = newest). Only used with XRANGE.
        count: Maximum number of entries to read.
        block_ms: Block for new entries (milliseconds). Only works with XREAD/XREADGROUP.
        group: Consumer group name. If provided, uses XREADGROUP.
        consumer: Consumer name within the group. Required if group is set.
        auto_ack: Automatically acknowledge messages after reading.
        create_group: Create the consumer group if it doesn't exist.
        include_id: Include the entry ID as a column.
        id_column: Name of the ID column.
        include_timestamp: Include the timestamp (from ID) as a column.
        timestamp_column: Name of the timestamp column.
        include_sequence: Include the sequence number (from ID) as a column.
        sequence_column: Name of the sequence column.

    Returns:
        DataFrame containing the stream entries.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> # Read all entries from a stream
        >>> df = redis.read_stream(
        ...     "redis://localhost",
        ...     stream="events",
        ...     schema={"user_id": pl.Utf8, "action": pl.Utf8},
        ... )
        >>>
        >>> # Read with consumer group
        >>> df = redis.read_stream(
        ...     "redis://localhost",
        ...     stream="events",
        ...     group="analytics",
        ...     consumer="worker-1",
        ...     schema={"user_id": pl.Utf8, "action": pl.Utf8},
        ...     auto_ack=True,
        ... )
        >>>
        >>> # Block for new entries
        >>> df = redis.read_stream(
        ...     "redis://localhost",
        ...     stream="events",
        ...     start_id="$",  # Only new entries
        ...     block_ms=5000,  # Wait up to 5 seconds
        ...     count=100,
        ... )
    """
    import redis as redis_client

    client = redis_client.Redis.from_url(url)

    try:
        entries = _read_stream_entries(
            client=client,
            stream=stream,
            start_id=start_id,
            end_id=end_id,
            count=count,
            block_ms=block_ms,
            group=group,
            consumer=consumer,
            auto_ack=auto_ack,
            create_group=create_group,
        )

        return _entries_to_dataframe(
            entries=entries,
            schema=schema,
            include_id=include_id,
            id_column=id_column,
            include_timestamp=include_timestamp,
            timestamp_column=timestamp_column,
            include_sequence=include_sequence,
            sequence_column=sequence_column,
        )
    finally:
        client.close()


def scan_stream(
    url: str,
    stream: str,
    *,
    schema: dict[str, pl.DataType] | None = None,
    start_id: str = "-",
    end_id: str = "+",
    count: int | None = None,
    include_id: bool = True,
    id_column: str = "_id",
    include_timestamp: bool = True,
    timestamp_column: str = "_ts",
    include_sequence: bool = False,
    sequence_column: str = "_seq",
) -> pl.LazyFrame:
    """Scan a Redis Stream and return a LazyFrame.

    This is the lazy version of read_stream(). Note that consumer groups
    and blocking reads are not supported in lazy mode as they require
    stateful operations.

    Args:
        url: Redis connection URL.
        stream: Name of the stream to read from.
        schema: Dictionary mapping field names to Polars dtypes.
        start_id: Start entry ID ("-" = oldest).
        end_id: End entry ID ("+" = newest).
        count: Maximum number of entries to read.
        include_id: Include the entry ID as a column.
        id_column: Name of the ID column.
        include_timestamp: Include the timestamp (from ID) as a column.
        timestamp_column: Name of the timestamp column.
        include_sequence: Include the sequence number (from ID) as a column.
        sequence_column: Name of the sequence column.

    Returns:
        LazyFrame for deferred execution.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> lf = redis.scan_stream(
        ...     "redis://localhost",
        ...     stream="events",
        ...     schema={"action": pl.Utf8, "value": pl.Float64},
        ... )
        >>> result = lf.group_by("action").agg(pl.col("value").mean()).collect()
    """
    df = read_stream(
        url=url,
        stream=stream,
        schema=schema,
        start_id=start_id,
        end_id=end_id,
        count=count,
        include_id=include_id,
        id_column=id_column,
        include_timestamp=include_timestamp,
        timestamp_column=timestamp_column,
        include_sequence=include_sequence,
        sequence_column=sequence_column,
    )
    return df.lazy()


def iter_stream(
    url: str,
    stream: str,
    *,
    schema: dict[str, pl.DataType] | None = None,
    batch_size: int = 100,
    block_ms: int = 1000,
    group: str | None = None,
    consumer: str | None = None,
    auto_ack: bool = False,
    create_group: bool = True,
    start_id: str | None = None,
    include_id: bool = True,
    id_column: str = "_id",
    include_timestamp: bool = True,
    timestamp_column: str = "_ts",
    include_sequence: bool = False,
    sequence_column: str = "_seq",
) -> Iterator[pl.DataFrame]:
    """Iterate over stream entries as DataFrame batches.

    Yields DataFrames containing batches of stream entries. This is useful
    for continuous processing of stream data.

    Args:
        url: Redis connection URL.
        stream: Name of the stream to read from.
        schema: Dictionary mapping field names to Polars dtypes.
        batch_size: Maximum entries per batch.
        block_ms: Block time for new entries (milliseconds).
        group: Consumer group name. If provided, uses XREADGROUP.
        consumer: Consumer name within the group.
        auto_ack: Automatically acknowledge messages.
        create_group: Create the consumer group if it doesn't exist.
        start_id: Start ID for reading. Defaults to "$" (new only) for
            non-group reads, ">" (pending) for group reads.
        include_id: Include the entry ID as a column.
        id_column: Name of the ID column.
        include_timestamp: Include the timestamp (from ID) as a column.
        timestamp_column: Name of the timestamp column.
        include_sequence: Include the sequence number (from ID) as a column.
        sequence_column: Name of the sequence column.

    Yields:
        DataFrames containing batches of stream entries.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> for batch_df in redis.iter_stream(
        ...     "redis://localhost",
        ...     stream="events",
        ...     batch_size=100,
        ...     block_ms=1000,
        ... ):
        ...     print(f"Got {len(batch_df)} entries")
        ...     process(batch_df)
        ...     if should_stop():
        ...         break
    """
    import redis as redis_client

    client = redis_client.Redis.from_url(url)

    # Default start_id based on whether using consumer group
    if start_id is None:
        start_id = ">" if group else "$"

    # Ensure consumer group exists if specified
    if group and create_group:
        _ensure_consumer_group(client, stream, group)

    last_id = start_id

    try:
        while True:
            entries = _read_stream_entries(
                client=client,
                stream=stream,
                start_id=last_id,
                count=batch_size,
                block_ms=block_ms,
                group=group,
                consumer=consumer,
                auto_ack=auto_ack,
                create_group=False,  # Already created above
            )

            if entries:
                # Update last_id for next iteration (only for non-group reads)
                if not group:
                    last_id = entries[-1][0]

                df = _entries_to_dataframe(
                    entries=entries,
                    schema=schema,
                    include_id=include_id,
                    id_column=id_column,
                    include_timestamp=include_timestamp,
                    timestamp_column=timestamp_column,
                    include_sequence=include_sequence,
                    sequence_column=sequence_column,
                )

                yield df
    finally:
        client.close()


async def stream_batches(
    url: str,
    stream: str,
    *,
    schema: dict[str, pl.DataType] | None = None,
    batch_size: int = 100,
    batch_timeout_ms: int = 1000,
    group: str | None = None,
    consumer: str | None = None,
    auto_ack: bool = False,
    create_group: bool = True,
    start_id: str | None = None,
    include_id: bool = True,
    id_column: str = "_id",
    include_timestamp: bool = True,
    timestamp_column: str = "_ts",
    include_sequence: bool = False,
    sequence_column: str = "_seq",
) -> AsyncIterator[pl.DataFrame]:
    """Async iterator for stream entries as DataFrame batches.

    Yields DataFrames containing batches of stream entries asynchronously.

    Args:
        url: Redis connection URL.
        stream: Name of the stream to read from.
        schema: Dictionary mapping field names to Polars dtypes.
        batch_size: Maximum entries per batch.
        batch_timeout_ms: Timeout for batch collection (milliseconds).
        group: Consumer group name. If provided, uses XREADGROUP.
        consumer: Consumer name within the group.
        auto_ack: Automatically acknowledge messages.
        create_group: Create the consumer group if it doesn't exist.
        start_id: Start ID for reading.
        include_id: Include the entry ID as a column.
        id_column: Name of the ID column.
        include_timestamp: Include the timestamp (from ID) as a column.
        timestamp_column: Name of the timestamp column.
        include_sequence: Include the sequence number (from ID) as a column.
        sequence_column: Name of the sequence column.

    Yields:
        DataFrames containing batches of stream entries.

    Example:
        >>> import asyncio
        >>> import polars_redis as redis
        >>>
        >>> async def process_events():
        ...     async for batch_df in redis.stream_batches(
        ...         "redis://localhost",
        ...         stream="events",
        ...         batch_size=100,
        ...     ):
        ...         await process_async(batch_df)
        >>>
        >>> asyncio.run(process_events())
    """
    import redis.asyncio as redis_async

    client = redis_async.Redis.from_url(url)

    # Default start_id based on whether using consumer group
    if start_id is None:
        start_id = ">" if group else "$"

    # Ensure consumer group exists if specified
    if group and create_group:
        await _ensure_consumer_group_async(client, stream, group)

    last_id = start_id

    try:
        while True:
            entries = await _read_stream_entries_async(
                client=client,
                stream=stream,
                start_id=last_id,
                count=batch_size,
                block_ms=batch_timeout_ms,
                group=group,
                consumer=consumer,
                auto_ack=auto_ack,
            )

            if entries:
                # Update last_id for next iteration (only for non-group reads)
                if not group:
                    last_id = entries[-1][0]

                df = _entries_to_dataframe(
                    entries=entries,
                    schema=schema,
                    include_id=include_id,
                    id_column=id_column,
                    include_timestamp=include_timestamp,
                    timestamp_column=timestamp_column,
                    include_sequence=include_sequence,
                    sequence_column=sequence_column,
                )

                yield df
    finally:
        await client.aclose()


def ack_entries(
    url: str,
    stream: str,
    group: str,
    entry_ids: list[str],
) -> int:
    """Acknowledge stream entries in a consumer group.

    Args:
        url: Redis connection URL.
        stream: Name of the stream.
        group: Consumer group name.
        entry_ids: List of entry IDs to acknowledge.

    Returns:
        Number of entries acknowledged.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> # Read without auto-ack
        >>> df = redis.read_stream(
        ...     "redis://localhost",
        ...     stream="events",
        ...     group="analytics",
        ...     consumer="worker-1",
        ...     auto_ack=False,
        ... )
        >>>
        >>> # Process entries...
        >>> process(df)
        >>>
        >>> # Acknowledge after successful processing
        >>> ids = df["_id"].to_list()
        >>> redis.ack_entries("redis://localhost", "events", "analytics", ids)
    """
    import redis as redis_client

    client = redis_client.Redis.from_url(url)
    try:
        return client.xack(stream, group, *entry_ids)
    finally:
        client.close()


# =============================================================================
# Internal helper functions
# =============================================================================


def _read_stream_entries(
    client: Any,
    stream: str,
    start_id: str = "-",
    end_id: str = "+",
    count: int | None = None,
    block_ms: int | None = None,
    group: str | None = None,
    consumer: str | None = None,
    auto_ack: bool = False,
    create_group: bool = True,
) -> list[tuple[str, dict[str, str]]]:
    """Read entries from a stream using appropriate Redis command."""
    entries: list[tuple[str, dict[str, str]]] = []

    if group:
        # Consumer group mode - use XREADGROUP
        if not consumer:
            raise ValueError("consumer is required when using a consumer group")

        # Create group if needed
        if create_group:
            _ensure_consumer_group(client, stream, group)

        # Use ">" for new entries in group, or specific ID for pending
        read_id = start_id if start_id != "-" else ">"

        result = client.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={stream: read_id},
            count=count,
            block=block_ms,
            noack=auto_ack,
        )

        if result:
            for stream_name, stream_entries in result:
                for entry_id, fields in stream_entries:
                    entry_id_str = entry_id.decode() if isinstance(entry_id, bytes) else entry_id
                    decoded_fields = _decode_fields(fields)
                    entries.append((entry_id_str, decoded_fields))

    elif block_ms is not None:
        # Blocking read without group - use XREAD
        result = client.xread(
            streams={stream: start_id},
            count=count,
            block=block_ms,
        )

        if result:
            for stream_name, stream_entries in result:
                for entry_id, fields in stream_entries:
                    entry_id_str = entry_id.decode() if isinstance(entry_id, bytes) else entry_id
                    decoded_fields = _decode_fields(fields)
                    entries.append((entry_id_str, decoded_fields))

    else:
        # Non-blocking range read - use XRANGE
        result = client.xrange(
            name=stream,
            min=start_id,
            max=end_id,
            count=count,
        )

        for entry_id, fields in result:
            entry_id_str = entry_id.decode() if isinstance(entry_id, bytes) else entry_id
            decoded_fields = _decode_fields(fields)
            entries.append((entry_id_str, decoded_fields))

    return entries


async def _read_stream_entries_async(
    client: Any,
    stream: str,
    start_id: str = "$",
    count: int | None = None,
    block_ms: int | None = None,
    group: str | None = None,
    consumer: str | None = None,
    auto_ack: bool = False,
) -> list[tuple[str, dict[str, str]]]:
    """Async version of _read_stream_entries."""
    entries: list[tuple[str, dict[str, str]]] = []

    if group:
        if not consumer:
            raise ValueError("consumer is required when using a consumer group")

        read_id = start_id if start_id != ">" else ">"

        result = await client.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={stream: read_id},
            count=count,
            block=block_ms,
            noack=auto_ack,
        )

        if result:
            for stream_name, stream_entries in result:
                for entry_id, fields in stream_entries:
                    entry_id_str = entry_id.decode() if isinstance(entry_id, bytes) else entry_id
                    decoded_fields = _decode_fields(fields)
                    entries.append((entry_id_str, decoded_fields))

    else:
        result = await client.xread(
            streams={stream: start_id},
            count=count,
            block=block_ms,
        )

        if result:
            for stream_name, stream_entries in result:
                for entry_id, fields in stream_entries:
                    entry_id_str = entry_id.decode() if isinstance(entry_id, bytes) else entry_id
                    decoded_fields = _decode_fields(fields)
                    entries.append((entry_id_str, decoded_fields))

    return entries


def _decode_fields(fields: dict) -> dict[str, str]:
    """Decode bytes fields to strings."""
    decoded = {}
    for k, v in fields.items():
        key = k.decode() if isinstance(k, bytes) else k
        value = v.decode() if isinstance(v, bytes) else v
        decoded[key] = value
    return decoded


def _ensure_consumer_group(client: Any, stream: str, group: str) -> None:
    """Create consumer group if it doesn't exist."""
    try:
        # Try to create the group, starting from ID 0 (all messages)
        client.xgroup_create(name=stream, groupname=group, id="0", mkstream=True)
    except Exception as e:
        # Group already exists - that's fine
        if "BUSYGROUP" not in str(e):
            raise


async def _ensure_consumer_group_async(client: Any, stream: str, group: str) -> None:
    """Async version of _ensure_consumer_group."""
    try:
        await client.xgroup_create(name=stream, groupname=group, id="0", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            raise


def _parse_entry_id(entry_id: str) -> tuple[int, int]:
    """Parse entry ID into timestamp (ms) and sequence number."""
    parts = entry_id.split("-")
    timestamp_ms = int(parts[0])
    sequence = int(parts[1]) if len(parts) > 1 else 0
    return timestamp_ms, sequence


def _entries_to_dataframe(
    entries: list[tuple[str, dict[str, str]]],
    schema: dict[str, pl.DataType] | None = None,
    include_id: bool = True,
    id_column: str = "_id",
    include_timestamp: bool = True,
    timestamp_column: str = "_ts",
    include_sequence: bool = False,
    sequence_column: str = "_seq",
) -> pl.DataFrame:
    """Convert stream entries to a DataFrame."""
    if not entries:
        # Return empty DataFrame with correct schema
        columns: dict[str, list] = {}
        if include_id:
            columns[id_column] = []
        if include_timestamp:
            columns[timestamp_column] = []
        if include_sequence:
            columns[sequence_column] = []
        if schema:
            for name in schema:
                columns[name] = []
        return pl.DataFrame(columns)

    # Build rows
    rows: list[dict[str, Any]] = []
    for entry_id, fields in entries:
        row: dict[str, Any] = {}

        if include_id:
            row[id_column] = entry_id

        if include_timestamp or include_sequence:
            ts_ms, seq = _parse_entry_id(entry_id)
            if include_timestamp:
                row[timestamp_column] = ts_ms
            if include_sequence:
                row[sequence_column] = seq

        # Add field values
        row.update(fields)
        rows.append(row)

    df = pl.DataFrame(rows)

    # Cast to schema if provided
    if schema:
        for name, dtype in schema.items():
            if name in df.columns:
                df = df.with_columns(pl.col(name).cast(dtype))

    return df
