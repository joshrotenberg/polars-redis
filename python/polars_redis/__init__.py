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

# Re-export from submodules
from polars_redis._infer import (
    SchemaConfidence,
    infer_hash_schema,
    infer_hash_schema_with_confidence,
    infer_hash_schema_with_overwrite,
    infer_json_schema,
    infer_json_schema_with_overwrite,
)

# Re-export from internal Rust module
from polars_redis._internal import (
    PyHashBatchIterator,
    PyJsonBatchIterator,
    PyStringBatchIterator,
    RedisScanner,
    scan_keys,
)
from polars_redis._read import (
    read_hashes,
    read_json,
    read_strings,
)
from polars_redis._scan import (
    scan_hashes,
    scan_json,
    scan_strings,
)
from polars_redis._search import (
    aggregate_hashes,
    search_hashes,
)
from polars_redis._write import (
    WriteResult,
    write_hashes,
    write_hashes_detailed,
    write_json,
    write_strings,
)

# Re-export from options module
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

# Re-export query builder
from polars_redis.query import col, raw

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
    "write_hashes_detailed",
    "write_json",
    "write_strings",
    "WriteResult",
    # Utilities
    "scan_keys",
    "infer_hash_schema",
    "infer_json_schema",
    "infer_hash_schema_with_overwrite",
    "infer_json_schema_with_overwrite",
    "infer_hash_schema_with_confidence",
    "SchemaConfidence",
    # Option classes
    "ScanOptions",
    "HashScanOptions",
    "JsonScanOptions",
    "StringScanOptions",
    "StreamScanOptions",
    "TimeSeriesScanOptions",
    "SearchOptions",
    # Query builder (predicate pushdown)
    "col",
    "raw",
    # Environment defaults
    "get_default_batch_size",
    "get_default_count_hint",
    "get_default_timeout_ms",
    # Version
    "__version__",
]

__version__ = "0.1.0"
