"""DataFrame caching functions for polars-redis.

This module provides functions for caching DataFrames in Redis using
Arrow IPC or Parquet formats. This enables using Redis as a high-performance
distributed cache for intermediate computation results.
"""

from __future__ import annotations

import io
from typing import Literal

import polars as pl

from polars_redis._internal import (
    py_cache_get,
    py_cache_set,
)

# Type alias for compression options
IpcCompression = Literal["uncompressed", "lz4", "zstd"]
ParquetCompression = Literal["uncompressed", "snappy", "gzip", "lz4", "zstd"]


def cache_dataframe(
    df: pl.DataFrame,
    url: str,
    key: str,
    *,
    format: Literal["ipc", "parquet"] = "ipc",
    compression: str | None = None,
    compression_level: int | None = None,
    ttl: int | None = None,
) -> int:
    """Cache a DataFrame in Redis.

    Serializes the DataFrame using Arrow IPC or Parquet format and stores
    it in Redis as a binary blob.

    Args:
        df: The DataFrame to cache.
        url: Redis connection URL (e.g., "redis://localhost:6379").
        key: Redis key for storage.
        format: Serialization format, either "ipc" (faster) or "parquet" (smaller).
            Default is "ipc".
        compression: Compression codec.
            For IPC: "uncompressed", "lz4", "zstd" (default: "uncompressed").
            For Parquet: "uncompressed", "snappy", "gzip", "lz4", "zstd"
            (default: "zstd").
        compression_level: Compression level (codec-specific). Only used for
            zstd and gzip.
        ttl: Time-to-live in seconds. If None, the key never expires.

    Returns:
        Number of bytes written to Redis.

    Raises:
        ValueError: If an invalid format or compression is specified.

    Example:
        >>> import polars as pl
        >>> import polars_redis as redis
        >>>
        >>> df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        >>>
        >>> # Cache with Arrow IPC (fast)
        >>> redis.cache_dataframe(df, "redis://localhost", "my_result")
        >>>
        >>> # Cache with Parquet (compact)
        >>> redis.cache_dataframe(
        ...     df, "redis://localhost", "my_result",
        ...     format="parquet", compression="zstd"
        ... )
        >>>
        >>> # With TTL (expires in 1 hour)
        >>> redis.cache_dataframe(df, "redis://localhost", "temp", ttl=3600)
    """
    if format == "ipc":
        data = _serialize_ipc(df, compression, compression_level)
    elif format == "parquet":
        data = _serialize_parquet(df, compression, compression_level)
    else:
        raise ValueError(f"Invalid format: {format}. Must be 'ipc' or 'parquet'.")

    return py_cache_set(url, key, data, ttl)


def get_cached_dataframe(
    url: str,
    key: str,
    *,
    format: Literal["ipc", "parquet"] = "ipc",
    columns: list[str] | None = None,
    n_rows: int | None = None,
) -> pl.DataFrame | None:
    """Retrieve a cached DataFrame from Redis.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        key: Redis key to retrieve.
        format: Serialization format used when caching. Must match the format
            used in cache_dataframe(). Default is "ipc".
        columns: Columns to read (projection pushdown). Only applies to Parquet.
            If None, all columns are read.
        n_rows: Maximum number of rows to read. Only applies to Parquet.
            If None, all rows are read.

    Returns:
        The cached DataFrame, or None if the key doesn't exist.

    Raises:
        ValueError: If an invalid format is specified.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> # Retrieve cached DataFrame
        >>> df = redis.get_cached_dataframe("redis://localhost", "my_result")
        >>> if df is not None:
        ...     print(df)
        >>>
        >>> # With projection (Parquet only)
        >>> df = redis.get_cached_dataframe(
        ...     "redis://localhost", "my_result",
        ...     format="parquet",
        ...     columns=["a", "b"],
        ... )
    """
    data = py_cache_get(url, key)
    if data is None:
        return None

    if format == "ipc":
        return _deserialize_ipc(data)
    elif format == "parquet":
        return _deserialize_parquet(data, columns, n_rows)
    else:
        raise ValueError(f"Invalid format: {format}. Must be 'ipc' or 'parquet'.")


def scan_cached(
    url: str,
    key: str,
    *,
    format: Literal["ipc", "parquet"] = "ipc",
) -> pl.LazyFrame | None:
    """Retrieve a cached DataFrame as a LazyFrame.

    This is useful when you want to apply lazy operations on the cached data.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        key: Redis key to retrieve.
        format: Serialization format used when caching. Default is "ipc".

    Returns:
        A LazyFrame wrapping the cached data, or None if the key doesn't exist.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> lf = redis.scan_cached("redis://localhost", "my_result")
        >>> if lf is not None:
        ...     result = lf.filter(pl.col("a") > 1).collect()
    """
    df = get_cached_dataframe(url, key, format=format)
    if df is None:
        return None
    return df.lazy()


def delete_cached(url: str, key: str) -> bool:
    """Delete a cached DataFrame from Redis.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        key: Redis key to delete.

    Returns:
        True if the key was deleted, False if it didn't exist.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> redis.delete_cached("redis://localhost", "my_result")
    """
    from polars_redis._internal import py_cache_delete

    return py_cache_delete(url, key)


def cache_exists(url: str, key: str) -> bool:
    """Check if a cached DataFrame exists in Redis.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        key: Redis key to check.

    Returns:
        True if the key exists, False otherwise.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> if redis.cache_exists("redis://localhost", "my_result"):
        ...     df = redis.get_cached_dataframe("redis://localhost", "my_result")
    """
    from polars_redis._internal import py_cache_exists

    return py_cache_exists(url, key)


def cache_ttl(url: str, key: str) -> int | None:
    """Get the remaining TTL of a cached DataFrame.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        key: Redis key to check.

    Returns:
        Remaining TTL in seconds, or None if the key doesn't exist or has no TTL.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> ttl = redis.cache_ttl("redis://localhost", "my_result")
        >>> if ttl is not None:
        ...     print(f"Expires in {ttl} seconds")
    """
    from polars_redis._internal import py_cache_ttl

    return py_cache_ttl(url, key)


# =============================================================================
# Internal serialization functions
# =============================================================================


def _serialize_ipc(
    df: pl.DataFrame,
    compression: str | None,
    compression_level: int | None,
) -> bytes:
    """Serialize DataFrame to Arrow IPC format."""
    if compression is None:
        compression = "uncompressed"

    # Validate compression
    valid = ("uncompressed", "lz4", "zstd")
    if compression not in valid:
        raise ValueError(f"Invalid IPC compression: {compression}. Must be one of {valid}.")

    buffer = io.BytesIO()
    df.write_ipc(buffer, compression=compression)  # type: ignore[arg-type]
    return buffer.getvalue()


def _deserialize_ipc(data: bytes) -> pl.DataFrame:
    """Deserialize DataFrame from Arrow IPC format."""
    buffer = io.BytesIO(data)
    return pl.read_ipc(buffer)


def _serialize_parquet(
    df: pl.DataFrame,
    compression: str | None,
    compression_level: int | None,
) -> bytes:
    """Serialize DataFrame to Parquet format."""
    if compression is None:
        compression = "zstd"

    # Validate compression
    valid = ("uncompressed", "snappy", "gzip", "lz4", "zstd")
    if compression not in valid:
        raise ValueError(f"Invalid Parquet compression: {compression}. Must be one of {valid}.")

    buffer = io.BytesIO()
    df.write_parquet(
        buffer,
        compression=compression,  # type: ignore[arg-type]
        compression_level=compression_level,
    )
    return buffer.getvalue()


def _deserialize_parquet(
    data: bytes,
    columns: list[str] | None,
    n_rows: int | None,
) -> pl.DataFrame:
    """Deserialize DataFrame from Parquet format."""
    buffer = io.BytesIO(data)
    return pl.read_parquet(buffer, columns=columns, n_rows=n_rows)
