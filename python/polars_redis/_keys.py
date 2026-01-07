"""Key management utilities for polars-redis.

This module provides functions for managing Redis keys in a DataFrame-friendly way,
including TTL management, bulk delete, rename, and key info retrieval.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import polars as pl

from polars_redis._internal import (
    py_delete_keys,
    py_delete_keys_pattern,
    py_exists_keys,
    py_get_ttl,
    py_key_info,
    py_persist_keys,
    py_rename_keys,
    py_set_ttl,
    py_set_ttl_individual,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass
class TtlResult:
    """Result of a bulk TTL operation."""

    succeeded: int
    """Number of keys that had TTL set successfully."""

    failed: int
    """Number of keys that failed (e.g., key doesn't exist)."""

    errors: list[tuple[str, str]]
    """Keys that failed with their error messages."""


@dataclass
class DeleteResult:
    """Result of a bulk delete operation."""

    deleted: int
    """Number of keys deleted."""

    not_found: int
    """Number of keys that didn't exist."""


@dataclass
class RenameResult:
    """Result of a bulk rename operation."""

    succeeded: int
    """Number of keys renamed successfully."""

    failed: int
    """Number of keys that failed to rename."""

    errors: list[tuple[str, str]]
    """Keys that failed with their error messages."""


def key_info(
    url: str,
    pattern: str,
    *,
    include_memory: bool = False,
) -> pl.DataFrame:
    """Get information about keys matching a pattern.

    Uses SCAN to iterate keys safely, then retrieves TYPE, TTL, and optionally
    MEMORY USAGE and OBJECT ENCODING for each key.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        pattern: Key pattern to match (e.g., "user:*").
        include_memory: Whether to include memory usage (slower).

    Returns:
        A DataFrame with columns: key, key_type, ttl, memory_usage, encoding.
        Memory usage and encoding are None if include_memory is False.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> # Get info for all user keys
        >>> df = redis.key_info("redis://localhost", "user:*")
        >>> print(df)
        shape: (3, 5)
        +----------+----------+-----+--------------+----------+
        | key      | key_type | ttl | memory_usage | encoding |
        | ---      | ---      | --- | ---          | ---      |
        | str      | str      | i64 | i64          | str      |
        +----------+----------+-----+--------------+----------+
        | user:1   | hash     | -1  | null         | null     |
        | user:2   | hash     | 3600| null         | null     |
        | user:3   | hash     | -1  | null         | null     |
        +----------+----------+-----+--------------+----------+
        >>>
        >>> # With memory usage
        >>> df = redis.key_info("redis://localhost", "user:*", include_memory=True)
    """
    info = py_key_info(url, pattern, include_memory)

    if not info:
        return pl.DataFrame(
            schema={
                "key": pl.Utf8,
                "key_type": pl.Utf8,
                "ttl": pl.Int64,
                "memory_usage": pl.Int64,
                "encoding": pl.Utf8,
            }
        )

    return pl.DataFrame(info)


def set_ttl(
    url: str,
    keys: Sequence[str],
    ttl_seconds: int,
) -> TtlResult:
    """Set TTL for multiple keys.

    Args:
        url: Redis connection URL.
        keys: Keys to set TTL for.
        ttl_seconds: TTL in seconds.

    Returns:
        TtlResult with success/failure counts.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> # Set TTL for all matching keys
        >>> result = redis.set_ttl("redis://localhost", ["user:1", "user:2"], 3600)
        >>> print(f"Set TTL for {result.succeeded} keys")
    """
    result = py_set_ttl(url, list(keys), ttl_seconds)
    return TtlResult(
        succeeded=result["succeeded"],
        failed=result["failed"],
        errors=result["errors"],
    )


def set_ttl_from_column(
    url: str,
    df: pl.DataFrame,
    key_column: str,
    ttl_column: str,
) -> TtlResult:
    """Set TTL for keys using values from DataFrame columns.

    This is useful when you have a DataFrame with keys and their corresponding
    TTL values, for example from a join operation.

    Args:
        url: Redis connection URL.
        df: DataFrame with key and TTL columns.
        key_column: Name of the column containing Redis keys.
        ttl_column: Name of the column containing TTL values in seconds.

    Returns:
        TtlResult with success/failure counts.

    Example:
        >>> import polars as pl
        >>> import polars_redis as redis
        >>>
        >>> # Create DataFrame with keys and TTLs
        >>> df = pl.DataFrame({
        ...     "key": ["user:1", "user:2", "user:3"],
        ...     "ttl": [3600, 7200, 1800],
        ... })
        >>>
        >>> # Set TTL for each key
        >>> result = redis.set_ttl_from_column(
        ...     "redis://localhost", df, "key", "ttl"
        ... )
    """
    keys_and_ttls = list(
        zip(
            df[key_column].to_list(),
            df[ttl_column].to_list(),
            strict=True,
        )
    )
    result = py_set_ttl_individual(url, keys_and_ttls)
    return TtlResult(
        succeeded=result["succeeded"],
        failed=result["failed"],
        errors=result["errors"],
    )


def delete_keys(
    url: str,
    keys: Sequence[str],
) -> DeleteResult:
    """Delete multiple keys.

    Args:
        url: Redis connection URL.
        keys: Keys to delete.

    Returns:
        DeleteResult with deleted and not_found counts.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> result = redis.delete_keys("redis://localhost", ["temp:1", "temp:2"])
        >>> print(f"Deleted {result.deleted} keys")
    """
    result = py_delete_keys(url, list(keys))
    return DeleteResult(
        deleted=result["deleted"],
        not_found=result["not_found"],
    )


def delete_keys_pattern(
    url: str,
    pattern: str,
) -> DeleteResult:
    """Delete keys matching a pattern.

    Uses SCAN to find keys, then deletes them in batches. Safe to use in
    production as it doesn't block the Redis server like KEYS.

    Args:
        url: Redis connection URL.
        pattern: Key pattern to match (e.g., "temp:*").

    Returns:
        DeleteResult with deleted and not_found counts.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> # Delete all temporary keys
        >>> result = redis.delete_keys_pattern("redis://localhost", "temp:*")
        >>> print(f"Deleted {result.deleted} keys")
    """
    result = py_delete_keys_pattern(url, pattern)
    return DeleteResult(
        deleted=result["deleted"],
        not_found=result["not_found"],
    )


def rename_keys(
    url: str,
    renames: Sequence[tuple[str, str]],
) -> RenameResult:
    """Rename multiple keys.

    Args:
        url: Redis connection URL.
        renames: List of (old_key, new_key) tuples.

    Returns:
        RenameResult with success/failure counts.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> # Rename keys to add a prefix
        >>> renames = [("user:1", "v2:user:1"), ("user:2", "v2:user:2")]
        >>> result = redis.rename_keys("redis://localhost", renames)
        >>> print(f"Renamed {result.succeeded} keys")
    """
    result = py_rename_keys(url, list(renames))
    return RenameResult(
        succeeded=result["succeeded"],
        failed=result["failed"],
        errors=result["errors"],
    )


def rename_keys_from_columns(
    url: str,
    df: pl.DataFrame,
    old_key_column: str,
    new_key_column: str,
) -> RenameResult:
    """Rename keys using values from DataFrame columns.

    This is useful when you have a DataFrame with old and new key names,
    for example from a transformation operation.

    Args:
        url: Redis connection URL.
        df: DataFrame with old and new key columns.
        old_key_column: Name of the column containing current Redis keys.
        new_key_column: Name of the column containing new Redis keys.

    Returns:
        RenameResult with success/failure counts.

    Example:
        >>> import polars as pl
        >>> import polars_redis as redis
        >>>
        >>> # Create DataFrame with old and new keys
        >>> df = pl.DataFrame({
        ...     "old_key": ["user:1", "user:2"],
        ...     "new_key": ["v2:user:1", "v2:user:2"],
        ... })
        >>>
        >>> # Rename keys
        >>> result = redis.rename_keys_from_columns(
        ...     "redis://localhost", df, "old_key", "new_key"
        ... )
    """
    renames = list(
        zip(
            df[old_key_column].to_list(),
            df[new_key_column].to_list(),
            strict=True,
        )
    )
    result = py_rename_keys(url, renames)
    return RenameResult(
        succeeded=result["succeeded"],
        failed=result["failed"],
        errors=result["errors"],
    )


def persist_keys(
    url: str,
    keys: Sequence[str],
) -> TtlResult:
    """Remove TTL from keys (make them persistent).

    Args:
        url: Redis connection URL.
        keys: Keys to persist.

    Returns:
        TtlResult with success/failure counts.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> # Make keys persistent
        >>> result = redis.persist_keys("redis://localhost", ["user:1", "user:2"])
        >>> print(f"Persisted {result.succeeded} keys")
    """
    result = py_persist_keys(url, list(keys))
    return TtlResult(
        succeeded=result["succeeded"],
        failed=result["failed"],
        errors=result["errors"],
    )


def exists_keys(
    url: str,
    keys: Sequence[str],
) -> pl.DataFrame:
    """Check if keys exist.

    Args:
        url: Redis connection URL.
        keys: Keys to check.

    Returns:
        DataFrame with columns: key, exists.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> df = redis.exists_keys("redis://localhost", ["user:1", "user:2", "user:999"])
        >>> print(df)
        shape: (3, 2)
        +----------+--------+
        | key      | exists |
        | ---      | ---    |
        | str      | bool   |
        +----------+--------+
        | user:1   | true   |
        | user:2   | true   |
        | user:999 | false  |
        +----------+--------+
    """
    result = py_exists_keys(url, list(keys))
    return pl.DataFrame({"key": [r[0] for r in result], "exists": [r[1] for r in result]})


def get_ttl(
    url: str,
    keys: Sequence[str],
) -> pl.DataFrame:
    """Get TTL for multiple keys.

    Args:
        url: Redis connection URL.
        keys: Keys to get TTL for.

    Returns:
        DataFrame with columns: key, ttl.
        TTL is -1 if no expiry, -2 if key doesn't exist.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> df = redis.get_ttl("redis://localhost", ["user:1", "user:2"])
        >>> print(df)
        shape: (2, 2)
        +--------+------+
        | key    | ttl  |
        | ---    | ---  |
        | str    | i64  |
        +--------+------+
        | user:1 | 3600 |
        | user:2 | -1   |
        +--------+------+
    """
    result = py_get_ttl(url, list(keys))
    return pl.DataFrame({"key": [r[0] for r in result], "ttl": [r[1] for r in result]})


def delete_keys_from_column(
    url: str,
    df: pl.DataFrame,
    key_column: str,
) -> DeleteResult:
    """Delete keys from a DataFrame column.

    This is useful when you have a DataFrame with keys to delete, for example
    from a filter operation.

    Args:
        url: Redis connection URL.
        df: DataFrame with key column.
        key_column: Name of the column containing Redis keys.

    Returns:
        DeleteResult with deleted and not_found counts.

    Example:
        >>> import polars as pl
        >>> import polars_redis as redis
        >>>
        >>> # Read keys and filter for ones to delete
        >>> df = redis.key_info("redis://localhost", "temp:*")
        >>> to_delete = df.filter(pl.col("ttl") == -1)  # No expiry
        >>>
        >>> # Delete those keys
        >>> result = redis.delete_keys_from_column(
        ...     "redis://localhost", to_delete, "key"
        ... )
    """
    keys = df[key_column].to_list()
    result = py_delete_keys(url, keys)
    return DeleteResult(
        deleted=result["deleted"],
        not_found=result["not_found"],
    )
