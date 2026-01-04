"""polars-redis: Redis IO plugin for Polars.

This package provides a Polars IO plugin that enables scanning Redis data structures
(hashes, JSON documents, strings) as LazyFrames, with support for projection pushdown,
predicate pushdown, and batched iteration.

Example:
    >>> import polars as pl
    >>> import polars_redis as redis
    >>>
    >>> # Scan Redis hashes matching a pattern
    >>> lf = redis.scan_hashes(
    ...     "redis://localhost:6379",
    ...     pattern="user:*",
    ...     schema={"name": pl.Utf8, "age": pl.Int64, "email": pl.Utf8}
    ... )
    >>>
    >>> # LazyFrame - nothing executed yet
    >>> result = (
    ...     lf
    ...     .filter(pl.col("age") > 30)
    ...     .select(["name", "email"])
    ...     .collect()
    ... )
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
    RedisScanner,
    py_infer_hash_schema,
    py_infer_hash_schema_with_overwrite,
    py_infer_json_schema,
    py_infer_json_schema_with_overwrite,
    py_write_hashes,
    py_write_json,
    py_write_strings,
    scan_keys,
)
from polars_redis.options import (
    HashScanOptions,
    JsonScanOptions,
    ScanOptions,
    SearchOptions,
    StreamScanOptions,
    StringScanOptions,
    TimeSeriesScanOptions,
    get_default_batch_size,
    get_default_count_hint,
    get_default_timeout_ms,
)

# RediSearch support (optional - requires search feature)
try:
    from polars_redis._internal import PyHashSearchIterator, py_aggregate

    _HAS_SEARCH = True
except ImportError:
    _HAS_SEARCH = False
    PyHashSearchIterator = None  # type: ignore[misc, assignment]
    py_aggregate = None  # type: ignore[misc, assignment]

if TYPE_CHECKING:
    from polars import DataFrame, Expr
    from polars.type_aliases import SchemaDict

__all__ = [
    # Iterators
    "RedisScanner",
    "PyHashBatchIterator",
    "PyJsonBatchIterator",
    "PyStringBatchIterator",
    # Scan functions
    "scan_hashes",
    "scan_json",
    "scan_strings",
    "search_hashes",
    "aggregate_hashes",
    # Read functions (eager)
    "read_hashes",
    "read_json",
    "read_strings",
    # Write functions
    "write_hashes",
    "write_json",
    "write_strings",
    # Utilities
    "scan_keys",
    "infer_hash_schema",
    "infer_json_schema",
    "infer_hash_schema_with_overwrite",
    "infer_json_schema_with_overwrite",
    # Option classes
    "ScanOptions",
    "HashScanOptions",
    "JsonScanOptions",
    "StringScanOptions",
    "StreamScanOptions",
    "TimeSeriesScanOptions",
    "SearchOptions",
    # Environment defaults
    "get_default_batch_size",
    "get_default_count_hint",
    "get_default_timeout_ms",
    # Version
    "__version__",
]

__version__ = "0.1.0"

# Mapping from Polars dtype to our internal type names
_DTYPE_MAP = {
    pl.Utf8: "utf8",
    pl.String: "utf8",
    pl.Int64: "int64",
    pl.Float64: "float64",
    pl.Boolean: "bool",
    pl.Date: "date",
    pl.Datetime: "datetime",
}


def _polars_dtype_to_internal(dtype: pl.DataType) -> str:
    """Convert a Polars dtype to our internal type string."""
    # Handle both class and instance
    dtype_key = dtype if isinstance(dtype, type) else type(dtype)
    if dtype_key in _DTYPE_MAP:
        return _DTYPE_MAP[dtype_key]
    # Try matching by name for robustness
    dtype_name = str(dtype).lower()
    if "utf8" in dtype_name or "string" in dtype_name:
        return "utf8"
    if "int64" in dtype_name:
        return "int64"
    if "float64" in dtype_name:
        return "float64"
    if "bool" in dtype_name:
        return "bool"
    if "datetime" in dtype_name:
        return "datetime"
    if "date" in dtype_name:
        return "date"
    raise ValueError(f"Unsupported dtype: {dtype}")


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


def search_hashes(
    url: str,
    index: str = "",
    query: str = "*",
    schema: dict | None = None,
    *,
    options: SearchOptions | None = None,
    include_key: bool = True,
    key_column_name: str = "_key",
    include_ttl: bool = False,
    ttl_column_name: str = "_ttl",
    include_row_index: bool = False,
    row_index_column_name: str = "_index",
    batch_size: int = 1000,
    sort_by: str | None = None,
    sort_ascending: bool = True,
) -> pl.LazyFrame:
    """Search Redis hashes using RediSearch and return a LazyFrame.

    This function uses RediSearch's FT.SEARCH command to perform server-side
    filtering (predicate pushdown), which is much more efficient than scanning
    all keys and filtering client-side.

    Requires:
        - RediSearch module installed on Redis server
        - An existing RediSearch index on the hash data

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        index: RediSearch index name (e.g., "users_idx").
        query: RediSearch query string (e.g., "@age:[30 +inf]", "*" for all).
        schema: Dictionary mapping field names to Polars dtypes.
        options: SearchOptions object for configuration. If provided,
            individual keyword arguments are ignored.
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column (default: "_key").
        include_ttl: Whether to include the TTL as a column.
        ttl_column_name: Name of the TTL column (default: "_ttl").
        include_row_index: Whether to include the row index as a column.
        row_index_column_name: Name of the row index column (default: "_index").
        batch_size: Number of documents to fetch per batch.
        sort_by: Optional field name to sort results by.
        sort_ascending: Sort direction (default: True for ascending).

    Returns:
        A Polars LazyFrame that will search Redis when collected.

    Example:
        >>> # Search for users over 30 years old
        >>> lf = search_hashes(
        ...     "redis://localhost:6379",
        ...     index="users_idx",
        ...     query="@age:[30 +inf]",
        ...     schema={"name": pl.Utf8, "age": pl.Int64}
        ... )
        >>> df = lf.collect()

        >>> # Search with sorting
        >>> lf = search_hashes(
        ...     "redis://localhost:6379",
        ...     index="users_idx",
        ...     query="@status:active",
        ...     schema={"name": pl.Utf8, "score": pl.Float64},
        ...     sort_by="score",
        ...     sort_ascending=False
        ... )
        >>> top_users = lf.head(10).collect()

        >>> # Using options object
        >>> opts = SearchOptions(
        ...     index="users_idx",
        ...     query="@age:[30 +inf]",
        ...     sort_by="name",
        ...     sort_ascending=True,
        ... )
        >>> lf = search_hashes(
        ...     "redis://localhost:6379",
        ...     schema={"name": pl.Utf8, "age": pl.Int64},
        ...     options=opts,
        ... )
        >>> df = lf.collect()

    Raises:
        RuntimeError: If RediSearch support is not available.
    """
    # If options object provided, use its values
    if options is not None:
        index = options.index
        query = options.query
        include_key = options.include_key
        key_column_name = options.key_column_name
        include_ttl = options.include_ttl
        ttl_column_name = options.ttl_column_name
        include_row_index = options.include_row_index
        row_index_column_name = options.row_index_column_name
        batch_size = options.batch_size
        sort_by = options.sort_by
        sort_ascending = options.sort_ascending
    if not _HAS_SEARCH:
        raise RuntimeError(
            "RediSearch support is not available. "
            "Ensure the 'search' feature is enabled when building polars-redis."
        )

    if schema is None:
        raise ValueError("schema is required for search_hashes")

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

    def _search_source(
        with_columns: list[str] | None,
        predicate: Expr | None,
        n_rows: int | None,
        batch_size_hint: int | None,
    ) -> Iterator[DataFrame]:
        """Generator that yields DataFrames from RediSearch results."""
        # Determine projection
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
        iterator = PyHashSearchIterator(
            url=url,
            index=index,
            query=query,
            schema=internal_schema,
            batch_size=effective_batch_size,
            projection=projection,
            include_key=include_key,
            key_column_name=key_column_name,
            include_ttl=include_ttl,
            ttl_column_name=ttl_column_name,
            include_row_index=include_row_index,
            row_index_column_name=row_index_column_name,
            max_rows=n_rows,
            sort_by=sort_by,
            sort_ascending=sort_ascending,
        )

        # Yield batches
        while not iterator.is_done():
            ipc_bytes = iterator.next_batch_ipc()
            if ipc_bytes is None:
                break

            df = pl.read_ipc(ipc_bytes)

            # Apply additional predicate filter if provided
            # Note: The query parameter already does server-side filtering,
            # but additional predicates can be applied client-side
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
        io_source=_search_source,
        schema=polars_schema,
    )


def read_strings(
    url: str,
    pattern: str = "*",
    *,
    value_type: type[pl.DataType] = pl.Utf8,
    include_key: bool = True,
    key_column_name: str = "_key",
    value_column_name: str = "value",
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


def infer_hash_schema(
    url: str,
    pattern: str = "*",
    *,
    sample_size: int = 100,
    type_inference: bool = True,
) -> dict[str, type[pl.DataType]]:
    """Infer schema from Redis hashes by sampling keys.

    Samples keys matching the pattern and infers field names and types
    from the hash values. This is useful for discovering the schema
    when you don't know it ahead of time.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        pattern: Key pattern to match (e.g., "user:*").
        sample_size: Maximum number of keys to sample (default: 100).
        type_inference: Whether to infer types (default: True).
            If False, all fields will be Utf8.

    Returns:
        A dictionary mapping field names to Polars dtypes, suitable
        for passing to scan_hashes() or read_hashes().

    Example:
        >>> schema = infer_hash_schema(
        ...     "redis://localhost:6379",
        ...     pattern="user:*",
        ...     sample_size=50
        ... )
        >>> print(schema)
        {'name': Utf8, 'age': Int64, 'email': Utf8}
        >>> df = read_hashes(
        ...     "redis://localhost:6379",
        ...     pattern="user:*",
        ...     schema=schema
        ... )
    """
    fields, _ = py_infer_hash_schema(url, pattern, sample_size, type_inference)
    return _fields_to_schema(fields)


def infer_json_schema(
    url: str,
    pattern: str = "*",
    *,
    sample_size: int = 100,
) -> dict[str, type[pl.DataType]]:
    """Infer schema from RedisJSON documents by sampling keys.

    Samples keys matching the pattern and infers field names and types
    from the JSON document structure. This is useful for discovering
    the schema when you don't know it ahead of time.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        pattern: Key pattern to match (e.g., "doc:*").
        sample_size: Maximum number of keys to sample (default: 100).

    Returns:
        A dictionary mapping field names to Polars dtypes, suitable
        for passing to scan_json() or read_json().

    Example:
        >>> schema = infer_json_schema(
        ...     "redis://localhost:6379",
        ...     pattern="doc:*",
        ...     sample_size=50
        ... )
        >>> print(schema)
        {'title': Utf8, 'views': Int64, 'rating': Float64}
        >>> df = read_json(
        ...     "redis://localhost:6379",
        ...     pattern="doc:*",
        ...     schema=schema
        ... )
    """
    fields, _ = py_infer_json_schema(url, pattern, sample_size)
    return _fields_to_schema(fields)


def _fields_to_schema(fields: list[tuple[str, str]]) -> dict[str, type[pl.DataType]]:
    """Convert internal field list to Polars schema dict."""
    type_map = {
        "utf8": pl.Utf8,
        "int64": pl.Int64,
        "float64": pl.Float64,
        "bool": pl.Boolean,
        "date": pl.Date,
        "datetime": pl.Datetime,
    }
    return {name: type_map.get(type_str, pl.Utf8) for name, type_str in fields}


def _schema_to_overwrite(schema: dict) -> list[tuple[str, str]]:
    """Convert Polars schema dict to overwrite format."""
    result = []
    for name, dtype in schema.items():
        type_str = _polars_dtype_to_internal(dtype)
        result.append((name, type_str))
    return result


def infer_hash_schema_with_overwrite(
    url: str,
    pattern: str = "*",
    *,
    schema_overwrite: dict | None = None,
    sample_size: int = 100,
    type_inference: bool = True,
) -> dict[str, type[pl.DataType]]:
    """Infer schema from Redis hashes with optional type overrides.

    This function infers a schema by sampling Redis hashes, then applies
    user-specified type overrides. This is useful when you want to infer
    most fields but explicitly set types for specific ones.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        pattern: Key pattern to match (e.g., "user:*").
        schema_overwrite: Dictionary mapping field names to Polars dtypes
            that override inferred types. Fields not in inferred schema
            will be added.
        sample_size: Maximum number of keys to sample (default: 100).
        type_inference: Whether to infer types (default: True).
            If False, all fields will be Utf8 before overwrites are applied.

    Returns:
        A dictionary mapping field names to Polars dtypes, suitable
        for passing to scan_hashes() or read_hashes().

    Example:
        >>> # Infer schema but force 'age' to Int64 and 'created_at' to Datetime
        >>> schema = infer_hash_schema_with_overwrite(
        ...     "redis://localhost:6379",
        ...     pattern="user:*",
        ...     schema_overwrite={"age": pl.Int64, "created_at": pl.Datetime}
        ... )
        >>> print(schema)
        {'age': Int64, 'created_at': Datetime, 'email': Utf8, 'name': Utf8}
        >>> df = read_hashes(
        ...     "redis://localhost:6379",
        ...     pattern="user:*",
        ...     schema=schema
        ... )
    """
    overwrite_list = None
    if schema_overwrite is not None:
        overwrite_list = _schema_to_overwrite(schema_overwrite)

    fields, _ = py_infer_hash_schema_with_overwrite(
        url, pattern, overwrite_list, sample_size, type_inference
    )
    return _fields_to_schema(fields)


def infer_json_schema_with_overwrite(
    url: str,
    pattern: str = "*",
    *,
    schema_overwrite: dict | None = None,
    sample_size: int = 100,
) -> dict[str, type[pl.DataType]]:
    """Infer schema from RedisJSON documents with optional type overrides.

    This function infers a schema by sampling RedisJSON documents, then applies
    user-specified type overrides. This is useful when you want to infer
    most fields but explicitly set types for specific ones.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        pattern: Key pattern to match (e.g., "doc:*").
        schema_overwrite: Dictionary mapping field names to Polars dtypes
            that override inferred types. Fields not in inferred schema
            will be added.
        sample_size: Maximum number of keys to sample (default: 100).

    Returns:
        A dictionary mapping field names to Polars dtypes, suitable
        for passing to scan_json() or read_json().

    Example:
        >>> # Infer schema but force 'timestamp' to Datetime
        >>> schema = infer_json_schema_with_overwrite(
        ...     "redis://localhost:6379",
        ...     pattern="doc:*",
        ...     schema_overwrite={"timestamp": pl.Datetime}
        ... )
        >>> df = read_json(
        ...     "redis://localhost:6379",
        ...     pattern="doc:*",
        ...     schema=schema
        ... )
    """
    overwrite_list = None
    if schema_overwrite is not None:
        overwrite_list = _schema_to_overwrite(schema_overwrite)

    fields, _ = py_infer_json_schema_with_overwrite(url, pattern, overwrite_list, sample_size)
    return _fields_to_schema(fields)


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
    import json

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


def aggregate_hashes(
    url: str,
    index: str,
    query: str = "*",
    *,
    group_by: list[str] | None = None,
    reduce: list[tuple[str, list[str], str]] | None = None,
    apply: list[tuple[str, str]] | None = None,
    filter_expr: str | None = None,
    sort_by: list[tuple[str, bool]] | None = None,
    limit: int | None = None,
    offset: int = 0,
    load: list[str] | None = None,
) -> pl.DataFrame:
    """Execute a RediSearch FT.AGGREGATE query and return results as a DataFrame.

    FT.AGGREGATE performs aggregations on indexed data, supporting grouping,
    reduce functions, computed fields, filtering, and sorting - all server-side.

    Requires:
        - RediSearch module installed on Redis server
        - An existing RediSearch index on the hash data

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        index: RediSearch index name (e.g., "users_idx").
        query: RediSearch query string (e.g., "@status:active", "*" for all).
        group_by: Fields to group by (e.g., ["@department", "@role"]).
        reduce: Reduce operations as (function, args, alias) tuples.
            Examples:
            - ("COUNT", [], "total")
            - ("SUM", ["@salary"], "total_salary")
            - ("AVG", ["@age"], "avg_age")
            - ("MIN", ["@score"], "min_score")
            - ("MAX", ["@score"], "max_score")
            - ("FIRST_VALUE", ["@name"], "first_name")
            - ("TOLIST", ["@name"], "names")
            - ("QUANTILE", ["@salary", "0.5"], "median_salary")
            - ("STDDEV", ["@score"], "score_stddev")
        apply: Computed expressions as (expression, alias) tuples.
            Example: ("@total_salary / @total", "avg_salary")
        filter_expr: Filter expression after aggregation.
            Example: "@total > 10"
        sort_by: Sort specifications as (field, ascending) tuples.
            Example: [("@total", False)] for descending by total.
        limit: Maximum number of results to return.
        offset: Number of results to skip (for pagination).
        load: Fields to load from the source documents (before aggregation).

    Returns:
        A Polars DataFrame containing the aggregation results.

    Example:
        >>> # Count users by department
        >>> df = aggregate_hashes(
        ...     "redis://localhost:6379",
        ...     index="users_idx",
        ...     query="*",
        ...     group_by=["@department"],
        ...     reduce=[("COUNT", [], "count")],
        ... )
        >>> print(df)

        >>> # Average salary by department, sorted by average
        >>> df = aggregate_hashes(
        ...     "redis://localhost:6379",
        ...     index="users_idx",
        ...     query="@status:active",
        ...     group_by=["@department"],
        ...     reduce=[
        ...         ("COUNT", [], "employee_count"),
        ...         ("AVG", ["@salary"], "avg_salary"),
        ...     ],
        ...     sort_by=[("@avg_salary", False)],
        ...     limit=10,
        ... )

        >>> # Calculate computed fields with APPLY
        >>> df = aggregate_hashes(
        ...     "redis://localhost:6379",
        ...     index="orders_idx",
        ...     query="*",
        ...     group_by=["@customer_id"],
        ...     reduce=[
        ...         ("SUM", ["@amount"], "total"),
        ...         ("COUNT", [], "order_count"),
        ...     ],
        ...     apply=[("@total / @order_count", "avg_order")],
        ...     filter_expr="@order_count > 5",
        ... )

    Raises:
        RuntimeError: If RediSearch support is not available.
    """
    if not _HAS_SEARCH:
        raise RuntimeError(
            "RediSearch support is not available. "
            "Ensure the 'search' feature is enabled when building polars-redis."
        )

    # Call the Rust implementation
    results = py_aggregate(
        url=url,
        index=index,
        query=query,
        group_by=group_by or [],
        reduce=reduce or [],
        apply=apply,
        filter=filter_expr,
        sort_by=sort_by,
        limit=limit,
        offset=offset,
        load=load,
    )

    # Convert list of dicts to DataFrame
    if not results:
        return pl.DataFrame()

    return pl.DataFrame(results)


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
