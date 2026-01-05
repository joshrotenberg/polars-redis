"""Scan functions for polars-redis.

This module contains the lazy scan functions that return LazyFrames
for streaming data from Redis using Polars IO plugin infrastructure.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

import polars as pl
from polars.io.plugins import register_io_source

from polars_redis._internal import (
    PyHashBatchIterator,
    PyJsonBatchIterator,
    PyStringBatchIterator,
)
from polars_redis._utils import _polars_dtype_to_internal
from polars_redis.options import HashScanOptions, JsonScanOptions, StringScanOptions

if TYPE_CHECKING:
    from polars import DataFrame, Expr
    from polars.type_aliases import SchemaDict


def scan_hashes(
    url: str,
    pattern: str = "*",
    schema: dict | None = None,
    *,
    options: HashScanOptions | None = None,
    include_key: bool = True,
    key_column_name: str = "_key",
    include_ttl: bool = False,
    ttl_column_name: str = "_ttl",
    include_row_index: bool = False,
    row_index_column_name: str = "_index",
    batch_size: int = 1000,
    count_hint: int = 100,
    parallel: int | None = None,
) -> pl.LazyFrame:
    """Scan Redis hashes matching a pattern and return a LazyFrame.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        pattern: Key pattern to match (e.g., "user:*").
        schema: Dictionary mapping field names to Polars dtypes.
        options: HashScanOptions object for configuration. If provided,
            individual keyword arguments are ignored.
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column (default: "_key").
        include_ttl: Whether to include the TTL as a column.
        ttl_column_name: Name of the TTL column (default: "_ttl").
        include_row_index: Whether to include the row index as a column.
        row_index_column_name: Name of the row index column (default: "_index").
        batch_size: Number of keys to process per batch.
        count_hint: SCAN COUNT hint for Redis.
        parallel: Number of parallel workers for fetching data (default: None).
            When set, splits each batch across multiple workers for faster fetching.

    Returns:
        A Polars LazyFrame that will scan Redis when collected.

    Example:
        >>> # Using keyword arguments
        >>> lf = scan_hashes(
        ...     "redis://localhost:6379",
        ...     pattern="user:*",
        ...     schema={"name": pl.Utf8, "age": pl.Int64}
        ... )
        >>> df = lf.collect()

        >>> # Using options object
        >>> opts = HashScanOptions(
        ...     pattern="user:*",
        ...     batch_size=500,
        ...     include_ttl=True,
        ... )
        >>> lf = scan_hashes(
        ...     "redis://localhost:6379",
        ...     schema={"name": pl.Utf8, "age": pl.Int64},
        ...     options=opts,
        ... )
        >>> df = lf.collect()
    """
    # If options object provided, use its values
    if options is not None:
        pattern = options.pattern
        include_key = options.include_key
        key_column_name = options.key_column_name
        include_ttl = options.include_ttl
        ttl_column_name = options.ttl_column_name
        include_row_index = options.include_row_index
        row_index_column_name = options.row_index_column_name
        batch_size = options.batch_size
        count_hint = options.count_hint
    if schema is None:
        raise ValueError("schema is required for scan_hashes")

    # Convert schema to internal format: list of (name, type_str) tuples
    internal_schema = [(name, _polars_dtype_to_internal(dtype)) for name, dtype in schema.items()]

    # Build the full Polars schema (for register_io_source)
    polars_schema: SchemaDict = {}
    if include_row_index:
        polars_schema[row_index_column_name] = pl.UInt64
    if include_key:
        polars_schema[key_column_name] = pl.Utf8
    if include_ttl:
        polars_schema[ttl_column_name] = pl.Int64
    for name, dtype in schema.items():
        polars_schema[name] = dtype

    def _hash_source(
        with_columns: list[str] | None,
        predicate: Expr | None,
        n_rows: int | None,
        batch_size_hint: int | None,
    ) -> Iterator[DataFrame]:
        """Generator that yields DataFrames from Redis hashes."""
        # Determine projection
        # We need to tell Rust which columns are requested, including the key column,
        # TTL column, and row index column if in with_columns.
        projection = None
        if with_columns is not None:
            # Include data columns, key column, TTL column, and row index column (if requested)
            projection = [
                c
                for c in with_columns
                if c in schema
                or c == key_column_name
                or c == ttl_column_name
                or c == row_index_column_name
            ]

        # Use batch_size_hint if provided, otherwise use configured batch_size
        effective_batch_size = batch_size_hint if batch_size_hint is not None else batch_size

        # Create the iterator
        iterator = PyHashBatchIterator(
            url=url,
            pattern=pattern,
            schema=internal_schema,
            batch_size=effective_batch_size,
            count_hint=count_hint,
            projection=projection,
            include_key=include_key,
            key_column_name=key_column_name,
            include_ttl=include_ttl,
            ttl_column_name=ttl_column_name,
            include_row_index=include_row_index,
            row_index_column_name=row_index_column_name,
            max_rows=n_rows,
            parallel=parallel,
        )

        # Yield batches
        while not iterator.is_done():
            ipc_bytes = iterator.next_batch_ipc()
            if ipc_bytes is None:
                break

            df = pl.read_ipc(ipc_bytes)

            # Apply predicate filter if provided (client-side filtering)
            # We don't push predicates down to Redis (would need RediSearch),
            # so we must apply them here
            if predicate is not None:
                df = df.filter(predicate)

            # Apply column selection if needed (for key column filtering)
            if with_columns is not None:
                # Only select columns that exist and were requested
                available = [c for c in with_columns if c in df.columns]
                if available:
                    df = df.select(available)

            # Don't yield empty batches
            if len(df) > 0:
                yield df

    return register_io_source(
        io_source=_hash_source,
        schema=polars_schema,
    )


def scan_json(
    url: str,
    pattern: str = "*",
    schema: dict | None = None,
    *,
    options: JsonScanOptions | None = None,
    include_key: bool = True,
    key_column_name: str = "_key",
    include_ttl: bool = False,
    ttl_column_name: str = "_ttl",
    include_row_index: bool = False,
    row_index_column_name: str = "_index",
    batch_size: int = 1000,
    count_hint: int = 100,
    parallel: int | None = None,
) -> pl.LazyFrame:
    """Scan RedisJSON documents matching a pattern and return a LazyFrame.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        pattern: Key pattern to match (e.g., "doc:*").
        schema: Dictionary mapping field names to Polars dtypes.
        options: JsonScanOptions object for configuration. If provided,
            individual keyword arguments are ignored.
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column (default: "_key").
        include_ttl: Whether to include the TTL as a column.
        ttl_column_name: Name of the TTL column (default: "_ttl").
        include_row_index: Whether to include the row index as a column.
        row_index_column_name: Name of the row index column (default: "_index").
        batch_size: Number of keys to process per batch.
        count_hint: SCAN COUNT hint for Redis.
        parallel: Number of parallel workers for fetching data (default: None).
            When set, splits each batch across multiple workers for faster fetching.

    Returns:
        A Polars LazyFrame that will scan Redis when collected.

    Example:
        >>> # Using keyword arguments
        >>> lf = scan_json(
        ...     "redis://localhost:6379",
        ...     pattern="doc:*",
        ...     schema={"title": pl.Utf8, "author": pl.Utf8}
        ... )
        >>> df = lf.collect()

        >>> # Using options object
        >>> opts = JsonScanOptions(
        ...     pattern="doc:*",
        ...     batch_size=500,
        ...     include_ttl=True,
        ... )
        >>> lf = scan_json(
        ...     "redis://localhost:6379",
        ...     schema={"title": pl.Utf8, "author": pl.Utf8},
        ...     options=opts,
        ... )
        >>> df = lf.collect()
    """
    # If options object provided, use its values
    if options is not None:
        pattern = options.pattern
        include_key = options.include_key
        key_column_name = options.key_column_name
        include_ttl = options.include_ttl
        ttl_column_name = options.ttl_column_name
        include_row_index = options.include_row_index
        row_index_column_name = options.row_index_column_name
        batch_size = options.batch_size
        count_hint = options.count_hint
    if schema is None:
        raise ValueError("schema is required for scan_json")

    # Convert schema to internal format: list of (name, type_str) tuples
    internal_schema = [(name, _polars_dtype_to_internal(dtype)) for name, dtype in schema.items()]

    # Build the full Polars schema (for register_io_source)
    polars_schema: SchemaDict = {}
    if include_row_index:
        polars_schema[row_index_column_name] = pl.UInt64
    if include_key:
        polars_schema[key_column_name] = pl.Utf8
    if include_ttl:
        polars_schema[ttl_column_name] = pl.Int64
    for name, dtype in schema.items():
        polars_schema[name] = dtype

    def _json_source(
        with_columns: list[str] | None,
        predicate: Expr | None,
        n_rows: int | None,
        batch_size_hint: int | None,
    ) -> Iterator[DataFrame]:
        """Generator that yields DataFrames from Redis JSON documents."""
        # Determine projection
        # Include data columns, key column, TTL column, and row index column (if requested)
        projection = None
        if with_columns is not None:
            projection = [
                c
                for c in with_columns
                if c in schema
                or c == key_column_name
                or c == ttl_column_name
                or c == row_index_column_name
            ]

        # Use batch_size_hint if provided, otherwise use configured batch_size
        effective_batch_size = batch_size_hint if batch_size_hint is not None else batch_size

        # Create the iterator
        iterator = PyJsonBatchIterator(
            url=url,
            pattern=pattern,
            schema=internal_schema,
            batch_size=effective_batch_size,
            count_hint=count_hint,
            projection=projection,
            include_key=include_key,
            key_column_name=key_column_name,
            include_ttl=include_ttl,
            ttl_column_name=ttl_column_name,
            include_row_index=include_row_index,
            row_index_column_name=row_index_column_name,
            max_rows=n_rows,
            parallel=parallel,
        )

        # Yield batches
        while not iterator.is_done():
            ipc_bytes = iterator.next_batch_ipc()
            if ipc_bytes is None:
                break

            df = pl.read_ipc(ipc_bytes)

            # Apply predicate filter if provided (client-side filtering)
            if predicate is not None:
                df = df.filter(predicate)

            # Apply column selection if needed (for key column filtering)
            if with_columns is not None:
                # Only select columns that exist and were requested
                available = [c for c in with_columns if c in df.columns]
                if available:
                    df = df.select(available)

            # Don't yield empty batches
            if len(df) > 0:
                yield df

    return register_io_source(
        io_source=_json_source,
        schema=polars_schema,
    )


def scan_strings(
    url: str,
    pattern: str = "*",
    *,
    options: StringScanOptions | None = None,
    value_type: type[pl.DataType] = pl.Utf8,
    include_key: bool = True,
    key_column_name: str = "_key",
    value_column_name: str = "value",
    batch_size: int = 1000,
    count_hint: int = 100,
    parallel: int | None = None,
) -> pl.LazyFrame:
    """Scan Redis string values matching a pattern and return a LazyFrame.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        pattern: Key pattern to match (e.g., "cache:*").
        options: StringScanOptions object for configuration. If provided,
            individual keyword arguments are ignored.
        value_type: Polars dtype for the value column (default: pl.Utf8).
            Supported: pl.Utf8, pl.Int64, pl.Float64, pl.Boolean.
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column (default: "_key").
        value_column_name: Name of the value column (default: "value").
        batch_size: Number of keys to process per batch.
        count_hint: SCAN COUNT hint for Redis.
        parallel: Number of parallel workers for fetching data (default: None).
            When set, splits each batch across multiple workers for faster fetching.

    Returns:
        A Polars LazyFrame with key and value columns.

    Example:
        >>> # Scan string values as UTF-8
        >>> lf = scan_strings(
        ...     "redis://localhost:6379",
        ...     pattern="cache:*"
        ... )
        >>> df = lf.collect()

        >>> # Scan counters as integers
        >>> lf = scan_strings(
        ...     "redis://localhost:6379",
        ...     pattern="counter:*",
        ...     value_type=pl.Int64
        ... )
        >>> total = lf.select(pl.col("value").sum()).collect()

        >>> # Using options object
        >>> opts = StringScanOptions(
        ...     pattern="counter:*",
        ...     batch_size=500,
        ...     value_column_name="count",
        ... )
        >>> lf = scan_strings(
        ...     "redis://localhost:6379",
        ...     value_type=pl.Int64,
        ...     options=opts,
        ... )
        >>> df = lf.collect()
    """
    # If options object provided, use its values
    if options is not None:
        pattern = options.pattern
        include_key = options.include_key
        key_column_name = options.key_column_name
        value_column_name = options.value_column_name
        batch_size = options.batch_size
        count_hint = options.count_hint
    # Convert value_type to internal string
    value_type_str = _polars_dtype_to_internal(value_type)

    # Build the full Polars schema (for register_io_source)
    polars_schema: SchemaDict = {}
    if include_key:
        polars_schema[key_column_name] = pl.Utf8
    polars_schema[value_column_name] = value_type

    def _string_source(
        with_columns: list[str] | None,
        predicate: Expr | None,
        n_rows: int | None,
        batch_size_hint: int | None,
    ) -> Iterator[DataFrame]:
        """Generator that yields DataFrames from Redis string values."""
        # Use batch_size_hint if provided, otherwise use configured batch_size
        effective_batch_size = batch_size_hint if batch_size_hint is not None else batch_size

        # Create the iterator
        iterator = PyStringBatchIterator(
            url=url,
            pattern=pattern,
            value_type=value_type_str,
            batch_size=effective_batch_size,
            count_hint=count_hint,
            include_key=include_key,
            key_column_name=key_column_name,
            value_column_name=value_column_name,
            max_rows=n_rows,
            parallel=parallel,
        )

        # Yield batches
        while not iterator.is_done():
            ipc_bytes = iterator.next_batch_ipc()
            if ipc_bytes is None:
                break

            df = pl.read_ipc(ipc_bytes)

            # Apply predicate filter if provided (client-side filtering)
            if predicate is not None:
                df = df.filter(predicate)

            # Apply column selection if needed
            if with_columns is not None:
                available = [c for c in with_columns if c in df.columns]
                if available:
                    df = df.select(available)

            # Don't yield empty batches
            if len(df) > 0:
                yield df

    return register_io_source(
        io_source=_string_source,
        schema=polars_schema,
    )
