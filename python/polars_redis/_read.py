"""Eager read functions for polars-redis.

This module contains the eager read functions that immediately execute
and return DataFrames. These are convenience wrappers around the lazy
scan functions.
"""

from __future__ import annotations

import polars as pl

from polars_redis._scan import scan_hashes, scan_json, scan_strings


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
