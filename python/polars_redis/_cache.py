"""DataFrame caching functions for polars-redis.

This module provides functions for caching DataFrames in Redis using
Arrow IPC or Parquet formats. This enables using Redis as a high-performance
distributed cache for intermediate computation results.

For large DataFrames, data is automatically chunked across multiple Redis keys
to avoid memory issues and Redis' 512MB value limit.
"""

from __future__ import annotations

import io
import json
from typing import Literal

import polars as pl

from polars_redis._internal import (
    py_cache_delete,
    py_cache_exists,
    py_cache_get,
    py_cache_set,
    py_cache_ttl,
)

# Type alias for compression options
IpcCompression = Literal["uncompressed", "lz4", "zstd"]
ParquetCompression = Literal["uncompressed", "snappy", "gzip", "lz4", "zstd"]

# Default chunk size: 100MB
DEFAULT_CHUNK_SIZE_MB = 100
BYTES_PER_MB = 1024 * 1024


def cache_dataframe(
    df: pl.DataFrame,
    url: str,
    key: str,
    *,
    format: Literal["ipc", "parquet"] = "ipc",
    compression: str | None = None,
    compression_level: int | None = None,
    ttl: int | None = None,
    chunk_size_mb: int | None = None,
) -> int:
    """Cache a DataFrame in Redis.

    Serializes the DataFrame using Arrow IPC or Parquet format and stores
    it in Redis. For large DataFrames, data is automatically chunked across
    multiple keys.

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
        chunk_size_mb: Maximum size per chunk in MB. If None, uses default (100MB).
            Set to 0 to disable chunking.

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
        >>>
        >>> # Large DataFrame with custom chunk size
        >>> redis.cache_dataframe(large_df, "redis://localhost", "big", chunk_size_mb=50)
    """
    if format == "ipc":
        data = _serialize_ipc(df, compression, compression_level)
    elif format == "parquet":
        data = _serialize_parquet(df, compression, compression_level)
    else:
        raise ValueError(f"Invalid format: {format}. Must be 'ipc' or 'parquet'.")

    # Determine chunk size
    if chunk_size_mb is None:
        chunk_size_mb = DEFAULT_CHUNK_SIZE_MB
    chunk_size_bytes = int(chunk_size_mb * BYTES_PER_MB)

    # If chunking is disabled or data fits in one chunk, store directly
    if chunk_size_bytes == 0 or len(data) <= chunk_size_bytes:
        return py_cache_set(url, key, data, ttl)

    # Chunked storage
    return _cache_chunked(url, key, data, format, chunk_size_bytes, ttl)


def _cache_chunked(
    url: str,
    key: str,
    data: bytes,
    format: str,
    chunk_size_bytes: int,
    ttl: int | None,
) -> int:
    """Store data in chunks across multiple Redis keys."""
    total_size = len(data)
    num_chunks = (total_size + chunk_size_bytes - 1) // chunk_size_bytes
    total_written = 0

    # Store metadata
    metadata = {
        "format": format,
        "total_size": total_size,
        "num_chunks": num_chunks,
        "chunk_size": chunk_size_bytes,
    }
    meta_key = f"{key}:meta"
    meta_bytes = json.dumps(metadata).encode("utf-8")
    py_cache_set(url, meta_key, meta_bytes, ttl)
    total_written += len(meta_bytes)

    # Store chunks
    for i in range(num_chunks):
        start = i * chunk_size_bytes
        end = min(start + chunk_size_bytes, total_size)
        chunk_data = data[start:end]
        chunk_key = f"{key}:chunk:{i}"
        py_cache_set(url, chunk_key, chunk_data, ttl)
        total_written += len(chunk_data)

    return total_written


def get_cached_dataframe(
    url: str,
    key: str,
    *,
    format: Literal["ipc", "parquet"] | None = None,
    columns: list[str] | None = None,
    n_rows: int | None = None,
) -> pl.DataFrame | None:
    """Retrieve a cached DataFrame from Redis.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        key: Redis key to retrieve.
        format: Serialization format used when caching. If None, auto-detects
            from metadata (for chunked) or defaults to "ipc".
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
    # Check for chunked storage first
    meta_key = f"{key}:meta"
    meta_data = py_cache_get(url, meta_key)

    if meta_data is not None:
        # Chunked storage - read metadata and reassemble
        return _get_chunked(url, key, meta_data, columns, n_rows)

    # Single key storage
    data = py_cache_get(url, key)
    if data is None:
        return None

    # Use provided format or default to ipc
    if format is None:
        format = "ipc"

    if format == "ipc":
        return _deserialize_ipc(data)
    elif format == "parquet":
        return _deserialize_parquet(data, columns, n_rows)
    else:
        raise ValueError(f"Invalid format: {format}. Must be 'ipc' or 'parquet'.")


def _get_chunked(
    url: str,
    key: str,
    meta_data: bytes,
    columns: list[str] | None,
    n_rows: int | None,
) -> pl.DataFrame | None:
    """Retrieve chunked data and reassemble."""
    metadata = json.loads(meta_data.decode("utf-8"))
    format = metadata["format"]
    num_chunks = metadata["num_chunks"]

    # Reassemble chunks
    chunks = []
    for i in range(num_chunks):
        chunk_key = f"{key}:chunk:{i}"
        chunk_data = py_cache_get(url, chunk_key)
        if chunk_data is None:
            # Missing chunk - data is corrupted or expired
            return None
        chunks.append(chunk_data)

    data = b"".join(chunks)

    if format == "ipc":
        return _deserialize_ipc(data)
    elif format == "parquet":
        return _deserialize_parquet(data, columns, n_rows)
    else:
        raise ValueError(f"Invalid format in metadata: {format}")


def scan_cached(
    url: str,
    key: str,
    *,
    format: Literal["ipc", "parquet"] | None = None,
) -> pl.LazyFrame | None:
    """Retrieve a cached DataFrame as a LazyFrame.

    This is useful when you want to apply lazy operations on the cached data.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        key: Redis key to retrieve.
        format: Serialization format used when caching. If None, auto-detects.

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

    Handles both single-key and chunked storage.

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
    deleted = False

    # Check for chunked storage
    meta_key = f"{key}:meta"
    meta_data = py_cache_get(url, meta_key)

    if meta_data is not None:
        # Delete all chunks
        metadata = json.loads(meta_data.decode("utf-8"))
        num_chunks = metadata["num_chunks"]

        for i in range(num_chunks):
            chunk_key = f"{key}:chunk:{i}"
            py_cache_delete(url, chunk_key)

        py_cache_delete(url, meta_key)
        deleted = True
    else:
        # Single key storage
        deleted = py_cache_delete(url, key)

    return deleted


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
    # Check for chunked storage first
    meta_key = f"{key}:meta"
    if py_cache_exists(url, meta_key):
        return True

    # Check single key
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
    # Check for chunked storage first
    meta_key = f"{key}:meta"
    if py_cache_exists(url, meta_key):
        return py_cache_ttl(url, meta_key)

    # Check single key
    return py_cache_ttl(url, key)


def cache_info(url: str, key: str) -> dict | None:
    """Get information about a cached DataFrame.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        key: Redis key to check.

    Returns:
        Dictionary with cache info, or None if key doesn't exist.
        Keys include: format, size_bytes, is_chunked, num_chunks, ttl

    Example:
        >>> import polars_redis as redis
        >>>
        >>> info = redis.cache_info("redis://localhost", "my_result")
        >>> if info:
        ...     print(f"Size: {info['size_bytes']} bytes")
        ...     print(f"Chunked: {info['is_chunked']}")
    """
    # Check for chunked storage first
    meta_key = f"{key}:meta"
    meta_data = py_cache_get(url, meta_key)

    if meta_data is not None:
        metadata = json.loads(meta_data.decode("utf-8"))
        return {
            "format": metadata["format"],
            "size_bytes": metadata["total_size"],
            "is_chunked": True,
            "num_chunks": metadata["num_chunks"],
            "chunk_size": metadata["chunk_size"],
            "ttl": py_cache_ttl(url, meta_key),
        }

    # Check single key
    data = py_cache_get(url, key)
    if data is None:
        return None

    return {
        "format": "unknown",  # Can't determine without parsing
        "size_bytes": len(data),
        "is_chunked": False,
        "num_chunks": 1,
        "chunk_size": len(data),
        "ttl": py_cache_ttl(url, key),
    }


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
