"""Eager read functions for polars-redis.

This module contains the eager read functions that immediately execute
and return DataFrames. These are convenience wrappers around the lazy
scan functions.
"""

from __future__ import annotations

import polars as pl

from polars_redis._scan import (
    scan_hashes,
    scan_json,
    scan_lists,
    scan_sets,
    scan_streams,
    scan_strings,
    scan_timeseries,
    scan_zsets,
)


def read_strings(
    url: str,
    pattern: str = "*",
    *,
    value_type: type[pl.DataType] = pl.Utf8,
    include_key: bool = True,
    key_column_name: str = "_key",
    value_column_name: str = "value",
    include_ttl: bool = False,
    ttl_column_name: str = "_ttl",
    batch_size: int = 1000,
    count_hint: int = 100,
) -> pl.DataFrame:
    """Read Redis string values matching a pattern and return a DataFrame.

    This is the eager version of scan_strings(). It immediately executes
    the scan and returns a DataFrame with all results.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        pattern: Key pattern to match (e.g., "cache:*").
        value_type: Polars dtype for the value column (default: pl.Utf8).
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column (default: "_key").
        value_column_name: Name of the value column (default: "value").
        include_ttl: Whether to include the TTL as a column.
        ttl_column_name: Name of the TTL column (default: "_ttl").
        batch_size: Number of keys to process per batch.
        count_hint: SCAN COUNT hint for Redis.

    Returns:
        A Polars DataFrame containing all matching string values.

    Example:
        >>> df = read_strings(
        ...     "redis://localhost:6379",
        ...     pattern="cache:*"
        ... )
        >>> print(df)
    """
    return scan_strings(
        url=url,
        pattern=pattern,
        value_type=value_type,
        include_key=include_key,
        key_column_name=key_column_name,
        value_column_name=value_column_name,
        include_ttl=include_ttl,
        ttl_column_name=ttl_column_name,
        batch_size=batch_size,
        count_hint=count_hint,
    ).collect()


def read_hashes(
    url: str,
    pattern: str = "*",
    schema: dict | None = None,
    *,
    include_key: bool = True,
    key_column_name: str = "_key",
    include_ttl: bool = False,
    ttl_column_name: str = "_ttl",
    include_row_index: bool = False,
    row_index_column_name: str = "_index",
    batch_size: int = 1000,
    count_hint: int = 100,
) -> pl.DataFrame:
    """Read Redis hashes matching a pattern and return a DataFrame.

    This is the eager version of scan_hashes(). It immediately executes
    the scan and returns a DataFrame with all results.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        pattern: Key pattern to match (e.g., "user:*").
        schema: Dictionary mapping field names to Polars dtypes.
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column (default: "_key").
        include_ttl: Whether to include the TTL as a column.
        ttl_column_name: Name of the TTL column (default: "_ttl").
        include_row_index: Whether to include the row index as a column.
        row_index_column_name: Name of the row index column (default: "_index").
        batch_size: Number of keys to process per batch.
        count_hint: SCAN COUNT hint for Redis.

    Returns:
        A Polars DataFrame containing all matching hashes.

    Example:
        >>> df = read_hashes(
        ...     "redis://localhost:6379",
        ...     pattern="user:*",
        ...     schema={"name": pl.Utf8, "age": pl.Int64}
        ... )
        >>> print(df)
    """
    return scan_hashes(
        url=url,
        pattern=pattern,
        schema=schema,
        include_key=include_key,
        key_column_name=key_column_name,
        include_ttl=include_ttl,
        ttl_column_name=ttl_column_name,
        include_row_index=include_row_index,
        row_index_column_name=row_index_column_name,
        batch_size=batch_size,
        count_hint=count_hint,
    ).collect()


def read_json(
    url: str,
    pattern: str = "*",
    schema: dict | None = None,
    *,
    include_key: bool = True,
    key_column_name: str = "_key",
    include_ttl: bool = False,
    ttl_column_name: str = "_ttl",
    include_row_index: bool = False,
    row_index_column_name: str = "_index",
    batch_size: int = 1000,
    count_hint: int = 100,
) -> pl.DataFrame:
    """Read RedisJSON documents matching a pattern and return a DataFrame.

    This is the eager version of scan_json(). It immediately executes
    the scan and returns a DataFrame with all results.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        pattern: Key pattern to match (e.g., "doc:*").
        schema: Dictionary mapping field names to Polars dtypes.
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column (default: "_key").
        include_ttl: Whether to include the TTL as a column.
        ttl_column_name: Name of the TTL column (default: "_ttl").
        include_row_index: Whether to include the row index as a column.
        row_index_column_name: Name of the row index column (default: "_index").
        batch_size: Number of keys to process per batch.
        count_hint: SCAN COUNT hint for Redis.

    Returns:
        A Polars DataFrame containing all matching JSON documents.

    Example:
        >>> df = read_json(
        ...     "redis://localhost:6379",
        ...     pattern="doc:*",
        ...     schema={"title": pl.Utf8, "author": pl.Utf8}
        ... )
        >>> print(df)
    """
    return scan_json(
        url=url,
        pattern=pattern,
        schema=schema,
        include_key=include_key,
        key_column_name=key_column_name,
        include_ttl=include_ttl,
        ttl_column_name=ttl_column_name,
        include_row_index=include_row_index,
        row_index_column_name=row_index_column_name,
        batch_size=batch_size,
        count_hint=count_hint,
    ).collect()


def read_sets(
    url: str,
    pattern: str = "*",
    *,
    include_key: bool = True,
    key_column_name: str = "_key",
    member_column_name: str = "member",
    include_row_index: bool = False,
    row_index_column_name: str = "_index",
    batch_size: int = 1000,
    count_hint: int = 100,
) -> pl.DataFrame:
    """Read Redis sets matching a pattern and return a DataFrame.

    This is the eager version of scan_sets(). It immediately executes
    the scan and returns a DataFrame with all results.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        pattern: Key pattern to match (e.g., "tags:*").
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column (default: "_key").
        member_column_name: Name of the member column (default: "member").
        include_row_index: Whether to include the row index as a column.
        row_index_column_name: Name of the row index column (default: "_index").
        batch_size: Number of keys to process per batch.
        count_hint: SCAN COUNT hint for Redis.

    Returns:
        A Polars DataFrame containing all set members.

    Example:
        >>> df = read_sets(
        ...     "redis://localhost:6379",
        ...     pattern="tags:*"
        ... )
        >>> print(df)
    """
    return scan_sets(
        url=url,
        pattern=pattern,
        include_key=include_key,
        key_column_name=key_column_name,
        member_column_name=member_column_name,
        include_row_index=include_row_index,
        row_index_column_name=row_index_column_name,
        batch_size=batch_size,
        count_hint=count_hint,
    ).collect()


def read_lists(
    url: str,
    pattern: str = "*",
    *,
    include_key: bool = True,
    key_column_name: str = "_key",
    element_column_name: str = "element",
    include_position: bool = False,
    position_column_name: str = "position",
    include_row_index: bool = False,
    row_index_column_name: str = "_index",
    batch_size: int = 1000,
    count_hint: int = 100,
) -> pl.DataFrame:
    """Read Redis lists matching a pattern and return a DataFrame.

    This is the eager version of scan_lists(). It immediately executes
    the scan and returns a DataFrame with all results.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        pattern: Key pattern to match (e.g., "queue:*").
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column (default: "_key").
        element_column_name: Name of the element column (default: "element").
        include_position: Whether to include the position index.
        position_column_name: Name of the position column (default: "position").
        include_row_index: Whether to include the row index as a column.
        row_index_column_name: Name of the row index column (default: "_index").
        batch_size: Number of keys to process per batch.
        count_hint: SCAN COUNT hint for Redis.

    Returns:
        A Polars DataFrame containing all list elements.

    Example:
        >>> df = read_lists(
        ...     "redis://localhost:6379",
        ...     pattern="queue:*",
        ...     include_position=True
        ... )
        >>> print(df)
    """
    return scan_lists(
        url=url,
        pattern=pattern,
        include_key=include_key,
        key_column_name=key_column_name,
        element_column_name=element_column_name,
        include_position=include_position,
        position_column_name=position_column_name,
        include_row_index=include_row_index,
        row_index_column_name=row_index_column_name,
        batch_size=batch_size,
        count_hint=count_hint,
    ).collect()


def read_zsets(
    url: str,
    pattern: str = "*",
    *,
    include_key: bool = True,
    key_column_name: str = "_key",
    member_column_name: str = "member",
    score_column_name: str = "score",
    include_rank: bool = False,
    rank_column_name: str = "rank",
    include_row_index: bool = False,
    row_index_column_name: str = "_index",
    batch_size: int = 1000,
    count_hint: int = 100,
) -> pl.DataFrame:
    """Read Redis sorted sets matching a pattern and return a DataFrame.

    This is the eager version of scan_zsets(). It immediately executes
    the scan and returns a DataFrame with all results.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        pattern: Key pattern to match (e.g., "leaderboard:*").
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column (default: "_key").
        member_column_name: Name of the member column (default: "member").
        score_column_name: Name of the score column (default: "score").
        include_rank: Whether to include the rank index.
        rank_column_name: Name of the rank column (default: "rank").
        include_row_index: Whether to include the row index as a column.
        row_index_column_name: Name of the row index column (default: "_index").
        batch_size: Number of keys to process per batch.
        count_hint: SCAN COUNT hint for Redis.

    Returns:
        A Polars DataFrame containing all sorted set members with scores.

    Example:
        >>> df = read_zsets(
        ...     "redis://localhost:6379",
        ...     pattern="leaderboard:*",
        ...     include_rank=True
        ... )
        >>> print(df)
    """
    return scan_zsets(
        url=url,
        pattern=pattern,
        include_key=include_key,
        key_column_name=key_column_name,
        member_column_name=member_column_name,
        score_column_name=score_column_name,
        include_rank=include_rank,
        rank_column_name=rank_column_name,
        include_row_index=include_row_index,
        row_index_column_name=row_index_column_name,
        batch_size=batch_size,
        count_hint=count_hint,
    ).collect()


def read_streams(
    url: str,
    pattern: str = "*",
    *,
    schema: dict | None = None,
    include_key: bool = True,
    key_column_name: str = "_key",
    include_id: bool = True,
    id_column_name: str = "_id",
    include_timestamp: bool = True,
    timestamp_column_name: str = "_ts",
    include_sequence: bool = False,
    sequence_column_name: str = "_seq",
    include_row_index: bool = False,
    row_index_column_name: str = "_index",
    start_id: str = "-",
    end_id: str = "+",
    count_per_stream: int | None = None,
    batch_size: int = 1000,
    count_hint: int = 100,
) -> pl.DataFrame:
    """Read Redis streams matching a pattern and return a DataFrame.

    This is the eager version of scan_streams(). It immediately executes
    the scan and returns a DataFrame with all results.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        pattern: Key pattern to match (e.g., "events:*").
        schema: Dictionary mapping field names to Polars dtypes for stream fields.
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column (default: "_key").
        include_id: Whether to include the entry ID as a column.
        id_column_name: Name of the entry ID column (default: "_id").
        include_timestamp: Whether to include the timestamp as a column.
        timestamp_column_name: Name of the timestamp column (default: "_ts").
        include_sequence: Whether to include the sequence number.
        sequence_column_name: Name of the sequence column (default: "_seq").
        include_row_index: Whether to include the row index as a column.
        row_index_column_name: Name of the row index column (default: "_index").
        start_id: Start ID for XRANGE (default: "-" for oldest).
        end_id: End ID for XRANGE (default: "+" for newest).
        count_per_stream: Maximum entries per stream (optional).
        batch_size: Number of keys to process per batch.
        count_hint: SCAN COUNT hint for Redis.

    Returns:
        A Polars DataFrame containing all stream entries.

    Example:
        >>> df = read_streams(
        ...     "redis://localhost:6379",
        ...     pattern="events:*",
        ...     schema={"event_type": pl.Utf8, "data": pl.Utf8}
        ... )
        >>> print(df)
    """
    return scan_streams(
        url=url,
        pattern=pattern,
        schema=schema,
        include_key=include_key,
        key_column_name=key_column_name,
        include_id=include_id,
        id_column_name=id_column_name,
        include_timestamp=include_timestamp,
        timestamp_column_name=timestamp_column_name,
        include_sequence=include_sequence,
        sequence_column_name=sequence_column_name,
        include_row_index=include_row_index,
        row_index_column_name=row_index_column_name,
        start_id=start_id,
        end_id=end_id,
        count_per_stream=count_per_stream,
        batch_size=batch_size,
        count_hint=count_hint,
    ).collect()


def read_timeseries(
    url: str,
    pattern: str = "*",
    *,
    include_key: bool = True,
    key_column_name: str = "_key",
    timestamp_column_name: str = "_ts",
    value_column_name: str = "value",
    include_row_index: bool = False,
    row_index_column_name: str = "_index",
    start: str = "-",
    end: str = "+",
    count_per_series: int | None = None,
    aggregation: str | None = None,
    bucket_size_ms: int | None = None,
    batch_size: int = 1000,
    count_hint: int = 100,
) -> pl.DataFrame:
    """Read Redis time series matching a pattern and return a DataFrame.

    This is the eager version of scan_timeseries(). It immediately executes
    the scan and returns a DataFrame with all results.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        pattern: Key pattern to match (e.g., "sensor:*").
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column (default: "_key").
        timestamp_column_name: Name of the timestamp column (default: "_ts").
        value_column_name: Name of the value column (default: "value").
        include_row_index: Whether to include the row index as a column.
        row_index_column_name: Name of the row index column (default: "_index").
        start: Start timestamp for TS.RANGE (default: "-" for oldest).
        end: End timestamp for TS.RANGE (default: "+" for newest).
        count_per_series: Maximum samples per time series (optional).
        aggregation: Aggregation type (avg, sum, min, max, etc.).
        bucket_size_ms: Bucket size in milliseconds for aggregation.
        batch_size: Number of keys to process per batch.
        count_hint: SCAN COUNT hint for Redis.

    Returns:
        A Polars DataFrame containing all time series samples.

    Example:
        >>> df = read_timeseries(
        ...     "redis://localhost:6379",
        ...     pattern="sensor:*",
        ...     aggregation="avg",
        ...     bucket_size_ms=60000
        ... )
        >>> print(df)
    """
    return scan_timeseries(
        url=url,
        pattern=pattern,
        include_key=include_key,
        key_column_name=key_column_name,
        timestamp_column_name=timestamp_column_name,
        value_column_name=value_column_name,
        include_row_index=include_row_index,
        row_index_column_name=row_index_column_name,
        start=start,
        end=end,
        count_per_series=count_per_series,
        aggregation=aggregation,
        bucket_size_ms=bucket_size_ms,
        batch_size=batch_size,
        count_hint=count_hint,
    ).collect()
