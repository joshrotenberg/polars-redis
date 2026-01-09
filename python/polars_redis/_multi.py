"""Multi-cluster write support for polars-redis.

This module provides functions for writing DataFrames to multiple Redis
clusters in parallel, enabling fan-out patterns for replication and
geographic distribution.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from typing import Any, Callable

import polars as pl


@dataclass
class Destination:
    """Configuration for a write destination.

    Attributes:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        name: Optional name for this destination (for logging/metrics).
        key_prefix: Prefix to prepend to all keys (default: "").
        transform: Optional function to transform the DataFrame before writing.
        timeout_ms: Timeout for this destination in milliseconds (default: 30000).
    """

    url: str
    name: str | None = None
    key_prefix: str = ""
    transform: Callable[[pl.DataFrame], pl.DataFrame] | None = None
    timeout_ms: int = 30000

    def __post_init__(self) -> None:
        if self.name is None:
            # Extract host from URL for default name
            self.name = self.url.split("://")[-1].split("/")[0]


@dataclass
class DestinationResult:
    """Result of a write operation to a single destination.

    Attributes:
        destination: The destination configuration.
        keys_written: Number of keys successfully written.
        keys_failed: Number of keys that failed to write.
        success: Whether the write completed without errors.
        error: Error message if the write failed entirely.
        duration_ms: Time taken for the write in milliseconds.
    """

    destination: Destination
    keys_written: int = 0
    keys_failed: int = 0
    success: bool = True
    error: str | None = None
    duration_ms: float = 0.0

    def __repr__(self) -> str:
        status = "ok" if self.success else f"error: {self.error}"
        return (
            f"DestinationResult({self.destination.name}: "
            f"{self.keys_written} written, {self.keys_failed} failed, {status})"
        )


@dataclass
class MultiWriteResult:
    """Aggregated result of writing to multiple destinations.

    Attributes:
        results: List of per-destination results.
        total_keys_written: Total keys written across all destinations.
        total_keys_failed: Total keys failed across all destinations.
        all_succeeded: Whether all destinations completed successfully.
        failed_destinations: List of destination names that failed.
    """

    results: list[DestinationResult] = field(default_factory=list)

    @property
    def total_keys_written(self) -> int:
        return sum(r.keys_written for r in self.results)

    @property
    def total_keys_failed(self) -> int:
        return sum(r.keys_failed for r in self.results)

    @property
    def all_succeeded(self) -> bool:
        return all(r.success for r in self.results)

    @property
    def failed_destinations(self) -> list[str]:
        return [r.destination.name or r.destination.url for r in self.results if not r.success]

    def __repr__(self) -> str:
        succeeded = sum(1 for r in self.results if r.success)
        failed = len(self.results) - succeeded
        return (
            f"MultiWriteResult({succeeded}/{len(self.results)} destinations succeeded, "
            f"{self.total_keys_written} total keys written)"
        )


async def _write_to_destination_hashes(
    df: pl.DataFrame,
    dest: Destination,
    key_column: str | None,
    ttl: int | None,
    if_exists: str,
) -> DestinationResult:
    """Write DataFrame to a single destination as hashes."""
    import time

    from polars_redis._internal import py_write_hashes

    start = time.perf_counter()
    result = DestinationResult(destination=dest)

    try:
        # Apply transform if provided
        write_df = dest.transform(df) if dest.transform else df

        # Prepare keys and values
        if key_column is None:
            keys = [f"{dest.key_prefix}{i}" for i in range(len(write_df))]
            field_columns = list(write_df.columns)
        else:
            if key_column not in write_df.columns:
                raise ValueError(f"Key column '{key_column}' not found in DataFrame")
            keys = [f"{dest.key_prefix}{k}" for k in write_df[key_column].to_list()]
            field_columns = [c for c in write_df.columns if c != key_column]

        # Convert values to strings
        values = []
        for i in range(len(write_df)):
            row_values = []
            for col in field_columns:
                val = write_df[col][i]
                row_values.append(None if val is None else str(val))
            values.append(row_values)

        # Run the sync write in a thread pool to not block the event loop
        loop = asyncio.get_event_loop()
        keys_written, keys_failed, _ = await asyncio.wait_for(
            loop.run_in_executor(
                None,
                lambda: py_write_hashes(dest.url, keys, field_columns, values, ttl, if_exists),
            ),
            timeout=dest.timeout_ms / 1000,
        )

        result.keys_written = keys_written
        result.keys_failed = keys_failed
        result.success = True

    except asyncio.TimeoutError:
        result.success = False
        result.error = f"Timeout after {dest.timeout_ms}ms"
    except Exception as e:
        result.success = False
        result.error = str(e)

    result.duration_ms = (time.perf_counter() - start) * 1000
    return result


async def _write_to_destination_json(
    df: pl.DataFrame,
    dest: Destination,
    key_column: str | None,
    ttl: int | None,
    if_exists: str,
) -> DestinationResult:
    """Write DataFrame to a single destination as JSON."""
    import time

    from polars_redis._internal import py_write_json

    start = time.perf_counter()
    result = DestinationResult(destination=dest)

    try:
        # Apply transform if provided
        write_df = dest.transform(df) if dest.transform else df

        # Prepare keys and JSON strings
        if key_column is None:
            keys = [f"{dest.key_prefix}{i}" for i in range(len(write_df))]
            field_columns = list(write_df.columns)
        else:
            if key_column not in write_df.columns:
                raise ValueError(f"Key column '{key_column}' not found in DataFrame")
            keys = [f"{dest.key_prefix}{k}" for k in write_df[key_column].to_list()]
            field_columns = [c for c in write_df.columns if c != key_column]

        # Build JSON strings
        json_strings = []
        for i in range(len(write_df)):
            doc = {}
            for col in field_columns:
                val = write_df[col][i]
                if val is not None:
                    doc[col] = val
            json_strings.append(json.dumps(doc))

        # Run the sync write in a thread pool
        loop = asyncio.get_event_loop()
        keys_written, keys_failed, _ = await asyncio.wait_for(
            loop.run_in_executor(
                None,
                lambda: py_write_json(dest.url, keys, json_strings, ttl, if_exists),
            ),
            timeout=dest.timeout_ms / 1000,
        )

        result.keys_written = keys_written
        result.keys_failed = keys_failed
        result.success = True

    except asyncio.TimeoutError:
        result.success = False
        result.error = f"Timeout after {dest.timeout_ms}ms"
    except Exception as e:
        result.success = False
        result.error = str(e)

    result.duration_ms = (time.perf_counter() - start) * 1000
    return result


async def write_hashes_multi(
    df: pl.DataFrame,
    destinations: list[Destination | dict[str, Any]],
    key_column: str | None = "_key",
    ttl: int | None = None,
    if_exists: str = "replace",
) -> MultiWriteResult:
    """Write a DataFrame to multiple Redis clusters as hashes in parallel.

    Each destination receives the same DataFrame (optionally transformed),
    written as Redis hashes. Writes execute concurrently, and failures at
    one destination don't affect others.

    Args:
        df: The DataFrame to write.
        destinations: List of destinations. Each can be a Destination object
            or a dict with keys: url (required), name, key_prefix, transform, timeout_ms.
        key_column: Column containing Redis keys (default: "_key").
            If None, keys are auto-generated from row indices.
        ttl: Optional TTL in seconds for each key.
        if_exists: How to handle existing keys ("fail", "replace", "append").

    Returns:
        MultiWriteResult with per-destination results.

    Example:
        >>> import polars as pl
        >>> import polars_redis as pr
        >>> from polars_redis import Destination
        >>>
        >>> df = pl.DataFrame({
        ...     "id": ["user:1", "user:2"],
        ...     "name": ["Alice", "Bob"],
        ...     "region": ["US", "EU"],
        ... })
        >>>
        >>> result = await pr.write_hashes_multi(
        ...     df,
        ...     destinations=[
        ...         Destination(url="redis://cluster-a:6379", key_prefix="primary:"),
        ...         Destination(url="redis://cluster-b:6379", key_prefix="backup:"),
        ...     ],
        ...     key_column="id",
        ... )
        >>>
        >>> print(f"Wrote to {len(result.results)} destinations")
        >>> for r in result.results:
        ...     print(f"  {r.destination.name}: {r.keys_written} keys")
    """
    # Normalize destinations to Destination objects
    normalized: list[Destination] = []
    for dest in destinations:
        if isinstance(dest, dict):
            normalized.append(Destination(**dest))
        else:
            normalized.append(dest)

    # Write to all destinations in parallel
    tasks = [
        _write_to_destination_hashes(df, dest, key_column, ttl, if_exists) for dest in normalized
    ]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    return MultiWriteResult(results=list(results))


async def write_json_multi(
    df: pl.DataFrame,
    destinations: list[Destination | dict[str, Any]],
    key_column: str | None = "_key",
    ttl: int | None = None,
    if_exists: str = "replace",
) -> MultiWriteResult:
    """Write a DataFrame to multiple Redis clusters as JSON documents in parallel.

    Each destination receives the same DataFrame (optionally transformed),
    written as RedisJSON documents. Writes execute concurrently, and failures
    at one destination don't affect others.

    Args:
        df: The DataFrame to write.
        destinations: List of destinations. Each can be a Destination object
            or a dict with keys: url (required), name, key_prefix, transform, timeout_ms.
        key_column: Column containing Redis keys (default: "_key").
            If None, keys are auto-generated from row indices.
        ttl: Optional TTL in seconds for each key.
        if_exists: How to handle existing keys ("fail", "replace", "append").

    Returns:
        MultiWriteResult with per-destination results.

    Example:
        >>> import polars as pl
        >>> import polars_redis as pr
        >>> from polars_redis import Destination
        >>>
        >>> df = pl.DataFrame({
        ...     "id": ["doc:1", "doc:2"],
        ...     "title": ["Hello", "World"],
        ...     "views": [100, 200],
        ... })
        >>>
        >>> result = await pr.write_json_multi(
        ...     df,
        ...     destinations=[
        ...         Destination(url="redis://primary:6379"),
        ...         Destination(url="redis://replica:6379"),
        ...     ],
        ...     key_column="id",
        ... )
        >>>
        >>> if not result.all_succeeded:
        ...     print(f"Failed destinations: {result.failed_destinations}")
    """
    # Normalize destinations to Destination objects
    normalized: list[Destination] = []
    for dest in destinations:
        if isinstance(dest, dict):
            normalized.append(Destination(**dest))
        else:
            normalized.append(dest)

    # Write to all destinations in parallel
    tasks = [
        _write_to_destination_json(df, dest, key_column, ttl, if_exists) for dest in normalized
    ]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    return MultiWriteResult(results=list(results))
