"""Write functions for polars-redis.

This module contains functions for writing DataFrames to Redis as
hashes, JSON documents, or string values.
"""

from __future__ import annotations

import json

import polars as pl

from polars_redis._internal import (
    py_write_hashes,
    py_write_hashes_detailed,
    py_write_json,
    py_write_lists,
    py_write_sets,
    py_write_strings,
    py_write_zsets,
)


class WriteResult:
    """Detailed result of a write operation with per-key error information.

    This class provides granular error reporting for production workflows where
    partial success is acceptable and retry logic is needed.

    Attributes:
        keys_written: Number of keys successfully written.
        keys_failed: Number of keys that failed to write.
        keys_skipped: Number of keys skipped (when if_exists="fail" and key exists).
        succeeded_keys: List of keys that were successfully written.
        failed_keys: List of keys that failed to write.
        errors: Dictionary mapping failed keys to their error messages.
    """

    def __init__(self, result_dict: dict):
        """Initialize from the Rust result dictionary."""
        self.keys_written: int = result_dict["keys_written"]
        self.keys_failed: int = result_dict["keys_failed"]
        self.keys_skipped: int = result_dict["keys_skipped"]
        self.succeeded_keys: list[str] = result_dict["succeeded_keys"]
        self.failed_keys: list[str] = result_dict["failed_keys"]
        self.errors: dict[str, str] = result_dict["errors"]

    def is_complete_success(self) -> bool:
        """Check if all keys were written successfully."""
        return self.keys_failed == 0

    def __repr__(self) -> str:
        return (
            f"WriteResult(keys_written={self.keys_written}, "
            f"keys_failed={self.keys_failed}, keys_skipped={self.keys_skipped})"
        )


def write_hashes(
    df: pl.DataFrame,
    url: str,
    key_column: str | None = "_key",
    ttl: int | None = None,
    key_prefix: str = "",
    if_exists: str = "replace",
) -> int:
    """Write a DataFrame to Redis as hashes.

    Each row in the DataFrame becomes a Redis hash. The key column specifies
    the Redis key for each hash, and the remaining columns become hash fields.

    Args:
        df: The DataFrame to write.
        url: Redis connection URL (e.g., "redis://localhost:6379").
        key_column: Column containing Redis keys (default: "_key").
            If None, keys are auto-generated from row indices as "{key_prefix}{index}".
        ttl: Optional TTL in seconds for each key (default: None, no expiration).
        key_prefix: Prefix to prepend to all keys (default: "").
            When key_column is None, this becomes required for meaningful keys.
        if_exists: How to handle existing keys (default: "replace").
            - "fail": Skip keys that already exist.
            - "replace": Delete existing keys before writing (clean replacement).
            - "append": Merge new fields into existing hashes.

    Returns:
        Number of keys successfully written.

    Raises:
        ValueError: If the key column is not in the DataFrame or if_exists is invalid.

    Example:
        >>> df = pl.DataFrame({
        ...     "_key": ["user:1", "user:2"],
        ...     "name": ["Alice", "Bob"],
        ...     "age": [30, 25]
        ... })
        >>> count = write_hashes(df, "redis://localhost:6379")
        >>> print(f"Wrote {count} hashes")
        >>> # With TTL (expires in 1 hour)
        >>> count = write_hashes(df, "redis://localhost:6379", ttl=3600)
        >>> # With key prefix (keys become "prod:user:1", "prod:user:2")
        >>> count = write_hashes(df, "redis://localhost:6379", key_prefix="prod:")
        >>> # Skip existing keys
        >>> count = write_hashes(df, "redis://localhost:6379", if_exists="fail")
        >>> # Auto-generate keys from row index
        >>> df = pl.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        >>> count = write_hashes(df, "redis://localhost:6379", key_column=None, key_prefix="user:")
        >>> # Keys will be "user:0", "user:1"
    """
    if key_column is None:
        # Auto-generate keys from row indices
        keys = [f"{key_prefix}{i}" for i in range(len(df))]
        field_columns = list(df.columns)
    else:
        if key_column not in df.columns:
            raise ValueError(f"Key column '{key_column}' not found in DataFrame")
        # Extract keys and apply prefix
        keys = [f"{key_prefix}{k}" for k in df[key_column].to_list()]
        # Get field columns (all columns except the key column)
        field_columns = [c for c in df.columns if c != key_column]

    # Convert all values to strings (Redis stores everything as strings)
    values = []
    for i in range(len(df)):
        row_values = []
        for col in field_columns:
            val = df[col][i]
            if val is None:
                row_values.append(None)
            else:
                row_values.append(str(val))
        values.append(row_values)

    # Call the Rust implementation
    keys_written, _, _ = py_write_hashes(url, keys, field_columns, values, ttl, if_exists)
    return keys_written


def write_hashes_detailed(
    df: pl.DataFrame,
    url: str,
    key_column: str | None = "_key",
    ttl: int | None = None,
    key_prefix: str = "",
    if_exists: str = "replace",
) -> WriteResult:
    """Write a DataFrame to Redis as hashes with detailed error reporting.

    This is similar to write_hashes() but returns detailed information about
    which specific keys succeeded or failed, enabling retry logic and better
    error handling in production workflows.

    Args:
        df: The DataFrame to write.
        url: Redis connection URL (e.g., "redis://localhost:6379").
        key_column: Column containing Redis keys (default: "_key").
            If None, keys are auto-generated from row indices as "{key_prefix}{index}".
        ttl: Optional TTL in seconds for each key (default: None, no expiration).
        key_prefix: Prefix to prepend to all keys (default: "").
        if_exists: How to handle existing keys (default: "replace").
            - "fail": Skip keys that already exist.
            - "replace": Delete existing keys before writing (clean replacement).
            - "append": Merge new fields into existing hashes.

    Returns:
        WriteResult object with detailed success/failure information.

    Raises:
        ValueError: If the key column is not in the DataFrame or if_exists is invalid.

    Example:
        >>> df = pl.DataFrame({
        ...     "_key": ["user:1", "user:2", "user:3"],
        ...     "name": ["Alice", "Bob", "Charlie"],
        ...     "age": [30, 25, 35]
        ... })
        >>> result = write_hashes_detailed(df, "redis://localhost:6379")
        >>> print(f"Wrote {result.keys_written}, failed {result.keys_failed}")
        >>> if not result.is_complete_success():
        ...     for key, error in result.errors.items():
        ...         print(f"  {key}: {error}")
        ...     # Retry failed keys
        ...     failed_df = df.filter(pl.col("_key").is_in(result.failed_keys))
    """
    if key_column is None:
        # Auto-generate keys from row indices
        keys = [f"{key_prefix}{i}" for i in range(len(df))]
        field_columns = list(df.columns)
    else:
        if key_column not in df.columns:
            raise ValueError(f"Key column '{key_column}' not found in DataFrame")
        # Extract keys and apply prefix
        keys = [f"{key_prefix}{k}" for k in df[key_column].to_list()]
        # Get field columns (all columns except the key column)
        field_columns = [c for c in df.columns if c != key_column]

    # Convert all values to strings (Redis stores everything as strings)
    values = []
    for i in range(len(df)):
        row_values = []
        for col in field_columns:
            val = df[col][i]
            if val is None:
                row_values.append(None)
            else:
                row_values.append(str(val))
        values.append(row_values)

    # Call the Rust implementation
    result_dict = py_write_hashes_detailed(url, keys, field_columns, values, ttl, if_exists)
    return WriteResult(result_dict)


def write_json(
    df: pl.DataFrame,
    url: str,
    key_column: str | None = "_key",
    ttl: int | None = None,
    key_prefix: str = "",
    if_exists: str = "replace",
) -> int:
    """Write a DataFrame to Redis as JSON documents.

    Each row in the DataFrame becomes a RedisJSON document. The key column
    specifies the Redis key for each document, and the remaining columns
    become JSON fields.

    Args:
        df: The DataFrame to write.
        url: Redis connection URL (e.g., "redis://localhost:6379").
        key_column: Column containing Redis keys (default: "_key").
            If None, keys are auto-generated from row indices as "{key_prefix}{index}".
        ttl: Optional TTL in seconds for each key (default: None, no expiration).
        key_prefix: Prefix to prepend to all keys (default: "").
            When key_column is None, this becomes required for meaningful keys.
        if_exists: How to handle existing keys (default: "replace").
            - "fail": Skip keys that already exist.
            - "replace": Overwrite existing documents.
            - "append": Same as replace (JSON documents are replaced entirely).

    Returns:
        Number of keys successfully written.

    Raises:
        ValueError: If the key column is not in the DataFrame or if_exists is invalid.

    Example:
        >>> df = pl.DataFrame({
        ...     "_key": ["doc:1", "doc:2"],
        ...     "title": ["Hello", "World"],
        ...     "views": [100, 200]
        ... })
        >>> count = write_json(df, "redis://localhost:6379")
        >>> print(f"Wrote {count} JSON documents")
        >>> # With TTL (expires in 1 hour)
        >>> count = write_json(df, "redis://localhost:6379", ttl=3600)
        >>> # With key prefix (keys become "prod:doc:1", "prod:doc:2")
        >>> count = write_json(df, "redis://localhost:6379", key_prefix="prod:")
        >>> # Skip existing keys
        >>> count = write_json(df, "redis://localhost:6379", if_exists="fail")
        >>> # Auto-generate keys from row index
        >>> df = pl.DataFrame({"title": ["Hello", "World"], "views": [100, 200]})
        >>> count = write_json(df, "redis://localhost:6379", key_column=None, key_prefix="doc:")
        >>> # Keys will be "doc:0", "doc:1"
    """
    if key_column is None:
        # Auto-generate keys from row indices
        keys = [f"{key_prefix}{i}" for i in range(len(df))]
        field_columns = list(df.columns)
    else:
        if key_column not in df.columns:
            raise ValueError(f"Key column '{key_column}' not found in DataFrame")
        # Extract keys and apply prefix
        keys = [f"{key_prefix}{k}" for k in df[key_column].to_list()]
        # Get field columns (all columns except the key column)
        field_columns = [c for c in df.columns if c != key_column]

    # Build JSON strings for each row
    json_strings = []
    for i in range(len(df)):
        doc = {}
        for col in field_columns:
            val = df[col][i]
            if val is not None:
                # Preserve native types for JSON
                doc[col] = val
        json_strings.append(json.dumps(doc))

    # Call the Rust implementation
    keys_written, _, _ = py_write_json(url, keys, json_strings, ttl, if_exists)
    return keys_written


def write_strings(
    df: pl.DataFrame,
    url: str,
    key_column: str | None = "_key",
    value_column: str = "value",
    ttl: int | None = None,
    key_prefix: str = "",
    if_exists: str = "replace",
) -> int:
    """Write a DataFrame to Redis as string values.

    Each row in the DataFrame becomes a Redis string. The key column specifies
    the Redis key, and the value column specifies the string value to store.

    Args:
        df: The DataFrame to write.
        url: Redis connection URL (e.g., "redis://localhost:6379").
        key_column: Column containing Redis keys (default: "_key").
            If None, keys are auto-generated from row indices as "{key_prefix}{index}".
        value_column: Column containing values to write (default: "value").
        ttl: Optional TTL in seconds for each key (default: None, no expiration).
        key_prefix: Prefix to prepend to all keys (default: "").
            When key_column is None, this becomes required for meaningful keys.
        if_exists: How to handle existing keys (default: "replace").
            - "fail": Skip keys that already exist.
            - "replace": Overwrite existing values.
            - "append": Same as replace (strings are replaced entirely).

    Returns:
        Number of keys successfully written.

    Raises:
        ValueError: If the key column or value column is not in the DataFrame.

    Example:
        >>> df = pl.DataFrame({
        ...     "_key": ["counter:1", "counter:2"],
        ...     "value": ["100", "200"]
        ... })
        >>> count = write_strings(df, "redis://localhost:6379")
        >>> print(f"Wrote {count} strings")
        >>> # With TTL (expires in 1 hour)
        >>> count = write_strings(df, "redis://localhost:6379", ttl=3600)
        >>> # With key prefix (keys become "prod:counter:1", "prod:counter:2")
        >>> count = write_strings(df, "redis://localhost:6379", key_prefix="prod:")
        >>> # Skip existing keys
        >>> count = write_strings(df, "redis://localhost:6379", if_exists="fail")
        >>> # Auto-generate keys from row index
        >>> df = pl.DataFrame({"value": ["100", "200", "300"]})
        >>> count = write_strings(df, "redis://localhost:6379", key_column=None, key_prefix="counter:")
        >>> # Keys will be "counter:0", "counter:1", "counter:2"
    """
    if key_column is None:
        # Auto-generate keys from row indices
        keys = [f"{key_prefix}{i}" for i in range(len(df))]
    else:
        if key_column not in df.columns:
            raise ValueError(f"Key column '{key_column}' not found in DataFrame")
        # Extract keys and apply prefix
        keys = [f"{key_prefix}{k}" for k in df[key_column].to_list()]

    if value_column not in df.columns:
        raise ValueError(f"Value column '{value_column}' not found in DataFrame")

    # Extract values, converting to strings
    values = []
    for val in df[value_column].to_list():
        if val is None:
            values.append(None)
        else:
            values.append(str(val))

    # Call the Rust implementation
    keys_written, _, _ = py_write_strings(url, keys, values, ttl, if_exists)
    return keys_written


def write_sets(
    df: pl.DataFrame,
    url: str,
    key_column: str | None = "_key",
    member_column: str = "member",
    ttl: int | None = None,
    key_prefix: str = "",
    if_exists: str = "replace",
) -> int:
    """Write a DataFrame to Redis as sets.

    Rows with the same key are grouped together, and their member values
    become the set members for that key.

    Args:
        df: The DataFrame to write.
        url: Redis connection URL (e.g., "redis://localhost:6379").
        key_column: Column containing Redis keys (default: "_key").
            If None, all members go into a single set with key "{key_prefix}0".
        member_column: Column containing set members (default: "member").
        ttl: Optional TTL in seconds for each key (default: None, no expiration).
        key_prefix: Prefix to prepend to all keys (default: "").
        if_exists: How to handle existing keys (default: "replace").
            - "fail": Skip keys that already exist.
            - "replace": Delete existing sets before writing.
            - "append": Add members to existing sets.

    Returns:
        Number of keys successfully written.

    Raises:
        ValueError: If the key column or member column is not in the DataFrame.

    Example:
        >>> df = pl.DataFrame({
        ...     "_key": ["tags:1", "tags:1", "tags:2"],
        ...     "member": ["python", "redis", "rust"]
        ... })
        >>> count = write_sets(df, "redis://localhost:6379")
        >>> # Creates: tags:1 = {python, redis}, tags:2 = {rust}
        >>> print(f"Wrote {count} sets")
        >>> # With TTL (expires in 1 hour)
        >>> count = write_sets(df, "redis://localhost:6379", ttl=3600)
        >>> # Append to existing sets instead of replacing
        >>> count = write_sets(df, "redis://localhost:6379", if_exists="append")
    """
    if member_column not in df.columns:
        raise ValueError(f"Member column '{member_column}' not found in DataFrame")

    if key_column is None:
        # All members go into a single set
        members = [str(m) for m in df[member_column].to_list() if m is not None]
        keys = [f"{key_prefix}0"]
        members_by_key = [members]
    else:
        if key_column not in df.columns:
            raise ValueError(f"Key column '{key_column}' not found in DataFrame")

        # Group members by key
        grouped: dict[str, list[str]] = {}
        for key, member in zip(df[key_column].to_list(), df[member_column].to_list()):
            if member is None:
                continue
            full_key = f"{key_prefix}{key}"
            if full_key not in grouped:
                grouped[full_key] = []
            grouped[full_key].append(str(member))

        keys = list(grouped.keys())
        members_by_key = list(grouped.values())

    # Call the Rust implementation
    keys_written, _, _ = py_write_sets(url, keys, members_by_key, ttl, if_exists)
    return keys_written


def write_lists(
    df: pl.DataFrame,
    url: str,
    key_column: str | None = "_key",
    element_column: str = "element",
    index_column: str | None = None,
    ttl: int | None = None,
    key_prefix: str = "",
    if_exists: str = "replace",
) -> int:
    """Write a DataFrame to Redis as lists.

    Rows with the same key are grouped together, and their element values
    become the list elements for that key. Elements are added using RPUSH
    (appended to the end).

    Args:
        df: The DataFrame to write.
        url: Redis connection URL (e.g., "redis://localhost:6379").
        key_column: Column containing Redis keys (default: "_key").
            If None, all elements go into a single list with key "{key_prefix}0".
        element_column: Column containing list elements (default: "element").
        index_column: Optional column for ordering elements within each key.
            If provided, elements are sorted by this column before being added.
        ttl: Optional TTL in seconds for each key (default: None, no expiration).
        key_prefix: Prefix to prepend to all keys (default: "").
        if_exists: How to handle existing keys (default: "replace").
            - "fail": Skip keys that already exist.
            - "replace": Delete existing lists before writing.
            - "append": Add elements to existing lists.

    Returns:
        Number of keys successfully written.

    Raises:
        ValueError: If the key column or element column is not in the DataFrame.

    Example:
        >>> df = pl.DataFrame({
        ...     "_key": ["queue:1", "queue:1", "queue:2"],
        ...     "element": ["a", "b", "c"]
        ... })
        >>> count = write_lists(df, "redis://localhost:6379")
        >>> # Creates: queue:1 = [a, b], queue:2 = [c]
        >>> print(f"Wrote {count} lists")
        >>> # With ordering
        >>> df = pl.DataFrame({
        ...     "_key": ["queue:1", "queue:1", "queue:1"],
        ...     "_index": [2, 0, 1],
        ...     "element": ["c", "a", "b"]
        ... })
        >>> count = write_lists(df, "redis://localhost:6379", index_column="_index")
        >>> # Creates: queue:1 = [a, b, c] (sorted by index)
        >>> # Append to existing lists instead of replacing
        >>> count = write_lists(df, "redis://localhost:6379", if_exists="append")
    """
    if element_column not in df.columns:
        raise ValueError(f"Element column '{element_column}' not found in DataFrame")

    if index_column is not None and index_column not in df.columns:
        raise ValueError(f"Index column '{index_column}' not found in DataFrame")

    if key_column is None:
        # All elements go into a single list
        if index_column is not None:
            # Sort by index
            sorted_df = df.sort(index_column)
            elements = [str(e) for e in sorted_df[element_column].to_list() if e is not None]
        else:
            elements = [str(e) for e in df[element_column].to_list() if e is not None]
        keys = [f"{key_prefix}0"]
        elements_by_key = [elements]
    else:
        if key_column not in df.columns:
            raise ValueError(f"Key column '{key_column}' not found in DataFrame")

        # Group elements by key, with optional ordering
        if index_column is not None:
            # Sort by key and index, then group
            sorted_df = df.sort([key_column, index_column])
            grouped: dict[str, list[str]] = {}
            for key, element in zip(
                sorted_df[key_column].to_list(), sorted_df[element_column].to_list()
            ):
                if element is None:
                    continue
                full_key = f"{key_prefix}{key}"
                if full_key not in grouped:
                    grouped[full_key] = []
                grouped[full_key].append(str(element))
        else:
            # Preserve DataFrame order within each key
            grouped = {}
            for key, element in zip(df[key_column].to_list(), df[element_column].to_list()):
                if element is None:
                    continue
                full_key = f"{key_prefix}{key}"
                if full_key not in grouped:
                    grouped[full_key] = []
                grouped[full_key].append(str(element))

        keys = list(grouped.keys())
        elements_by_key = list(grouped.values())

    # Call the Rust implementation
    keys_written, _, _ = py_write_lists(url, keys, elements_by_key, ttl, if_exists)
    return keys_written


def write_zsets(
    df: pl.DataFrame,
    url: str,
    key_column: str | None = "_key",
    member_column: str = "member",
    score_column: str = "score",
    ttl: int | None = None,
    key_prefix: str = "",
    if_exists: str = "replace",
) -> int:
    """Write a DataFrame to Redis as sorted sets.

    Rows with the same key are grouped together, and their member/score pairs
    become the sorted set members for that key.

    Args:
        df: The DataFrame to write.
        url: Redis connection URL (e.g., "redis://localhost:6379").
        key_column: Column containing Redis keys (default: "_key").
            If None, all members go into a single sorted set with key "{key_prefix}0".
        member_column: Column containing sorted set members (default: "member").
        score_column: Column containing scores (default: "score").
        ttl: Optional TTL in seconds for each key (default: None, no expiration).
        key_prefix: Prefix to prepend to all keys (default: "").
        if_exists: How to handle existing keys (default: "replace").
            - "fail": Skip keys that already exist.
            - "replace": Delete existing sorted sets before writing.
            - "append": Add/update members in existing sorted sets.

    Returns:
        Number of keys successfully written.

    Raises:
        ValueError: If any required column is not in the DataFrame.

    Example:
        >>> df = pl.DataFrame({
        ...     "_key": ["leaderboard:1", "leaderboard:1"],
        ...     "member": ["alice", "bob"],
        ...     "score": [1500.0, 1200.0]
        ... })
        >>> count = write_zsets(df, "redis://localhost:6379")
        >>> # Creates: leaderboard:1 = {alice: 1500, bob: 1200}
        >>> print(f"Wrote {count} sorted sets")
        >>> # With TTL (expires in 1 hour)
        >>> count = write_zsets(df, "redis://localhost:6379", ttl=3600)
        >>> # Append/update scores in existing sorted sets
        >>> count = write_zsets(df, "redis://localhost:6379", if_exists="append")
    """
    if member_column not in df.columns:
        raise ValueError(f"Member column '{member_column}' not found in DataFrame")

    if score_column not in df.columns:
        raise ValueError(f"Score column '{score_column}' not found in DataFrame")

    if key_column is None:
        # All members go into a single sorted set
        members_scores = []
        for member, score in zip(df[member_column].to_list(), df[score_column].to_list()):
            if member is None or score is None:
                continue
            members_scores.append((str(member), float(score)))
        keys = [f"{key_prefix}0"]
        members_by_key = [members_scores]
    else:
        if key_column not in df.columns:
            raise ValueError(f"Key column '{key_column}' not found in DataFrame")

        # Group members by key
        grouped: dict[str, list[tuple[str, float]]] = {}
        for key, member, score in zip(
            df[key_column].to_list(), df[member_column].to_list(), df[score_column].to_list()
        ):
            if member is None or score is None:
                continue
            full_key = f"{key_prefix}{key}"
            if full_key not in grouped:
                grouped[full_key] = []
            grouped[full_key].append((str(member), float(score)))

        keys = list(grouped.keys())
        members_by_key = list(grouped.values())

    # Call the Rust implementation
    keys_written, _, _ = py_write_zsets(url, keys, members_by_key, ttl, if_exists)
    return keys_written
