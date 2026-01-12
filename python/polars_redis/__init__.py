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

from polars_redis._cache import (
    cache_dataframe,
    cache_exists,
    cache_info,
    cache_ttl,
    delete_cached,
    get_cached_dataframe,
    scan_cached,
)
from polars_redis._consumer import (
    CheckpointStore,
    ConsumerStats,
    FileCheckpointStore,
    MemoryCheckpointStore,
    RedisCheckpointStore,
    StreamConsumer,
)
from polars_redis._decorator import (
    cache,
    cache_lazy,
)
from polars_redis._geo import (
    GeoAddResult,
    geo_add,
    geo_add_from_dataframe,
    geo_dist,
    geo_dist_matrix,
    geo_hash,
    geo_pos,
    geo_radius,
    geo_radius_by_member,
)
from polars_redis._index import (
    Field,
    GeoField,
    GeoShapeField,
    Index,
    IndexDiff,
    IndexInfo,
    NumericField,
    TagField,
    TextField,
    VectorField,
)

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
from polars_redis._keys import (
    DeleteResult,
    RenameResult,
    TtlResult,
    delete_keys,
    delete_keys_from_column,
    delete_keys_pattern,
    exists_keys,
    get_ttl,
    key_info,
    persist_keys,
    rename_keys,
    rename_keys_from_columns,
    set_ttl,
    set_ttl_from_column,
)
from polars_redis._multi import (
    Destination,
    DestinationResult,
    MultiWriteResult,
    write_hashes_multi,
    write_json_multi,
)
from polars_redis._pipeline import (
    CommandResult,
    Pipeline,
    PipelineResult,
    Transaction,
)
from polars_redis._pubsub import (
    collect_pubsub,
    iter_pubsub,
    iter_pubsub_async,
)
from polars_redis._read import (
    read_hashes,
    read_json,
    read_lists,
    read_sets,
    read_streams,
    read_strings,
    read_timeseries,
    read_zsets,
)
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
from polars_redis._search import (
    aggregate_hashes,
    aggregate_json,
    search_hashes,
    search_json,
)
from polars_redis._smart import (
    DetectedIndex,
    ExecutionStrategy,
    QueryPlan,
    explain_scan,
    find_index_for_pattern,
    list_indexes,
    smart_scan,
)
from polars_redis._streams import (
    ack_entries,
    iter_stream,
    iter_stream_async,
    read_stream,
    scan_stream,
)
from polars_redis._write import (
    WriteResult,
    write_hashes,
    write_hashes_detailed,
    write_json,
    write_lists,
    write_sets,
    write_strings,
    write_zsets,
)

# Re-export from options module
from polars_redis.options import (
    HashScanOptions,
    JsonScanOptions,
    ListScanOptions,
    ScanOptions,
    SearchOptions,
    SetScanOptions,
    StreamScanOptions,
    StringScanOptions,
    TimeSeriesScanOptions,
    ZSetScanOptions,
    get_default_batch_size,
    get_default_count_hint,
    get_default_timeout_ms,
)

# Re-export query builder
from polars_redis.query import col, cols, raw

__all__ = [
    # Iterators
    "RedisScanner",
    "PyHashBatchIterator",
    "PyJsonBatchIterator",
    "PyStringBatchIterator",
    # Scan functions (lazy)
    "scan_hashes",
    "scan_json",
    "scan_lists",
    "scan_sets",
    "scan_streams",
    "scan_strings",
    "scan_timeseries",
    "scan_zsets",
    "search_hashes",
    "search_json",
    "aggregate_hashes",
    "aggregate_json",
    # Read functions (eager)
    "read_hashes",
    "read_json",
    "read_lists",
    "read_sets",
    "read_streams",
    "read_strings",
    "read_timeseries",
    "read_zsets",
    # Write functions
    "write_hashes",
    "write_hashes_detailed",
    "write_json",
    "write_lists",
    "write_sets",
    "write_strings",
    "write_zsets",
    "WriteResult",
    # Multi-cluster write
    "write_hashes_multi",
    "write_json_multi",
    "Destination",
    "DestinationResult",
    "MultiWriteResult",
    # Pipeline and Transaction
    "Pipeline",
    "Transaction",
    "PipelineResult",
    "CommandResult",
    # Utilities
    "scan_keys",
    "infer_hash_schema",
    "infer_json_schema",
    "infer_hash_schema_with_overwrite",
    "infer_json_schema_with_overwrite",
    "infer_hash_schema_with_confidence",
    "SchemaConfidence",
    # Index management (RediSearch)
    "Index",
    "IndexInfo",
    "IndexDiff",
    "Field",
    "TextField",
    "NumericField",
    "TagField",
    "GeoField",
    "GeoShapeField",
    "VectorField",
    # Option classes
    "ScanOptions",
    "HashScanOptions",
    "JsonScanOptions",
    "ListScanOptions",
    "SetScanOptions",
    "StringScanOptions",
    "StreamScanOptions",
    "TimeSeriesScanOptions",
    "ZSetScanOptions",
    "SearchOptions",
    # Query builder (predicate pushdown)
    "col",
    "cols",
    "raw",
    # DataFrame caching
    "cache_dataframe",
    "get_cached_dataframe",
    "scan_cached",
    "delete_cached",
    "cache_exists",
    "cache_ttl",
    "cache_info",
    # Caching decorators
    "cache",
    "cache_lazy",
    # Key management
    "key_info",
    "set_ttl",
    "set_ttl_from_column",
    "delete_keys",
    "delete_keys_pattern",
    "delete_keys_from_column",
    "rename_keys",
    "rename_keys_from_columns",
    "persist_keys",
    "exists_keys",
    "get_ttl",
    "TtlResult",
    "DeleteResult",
    "RenameResult",
    # Pub/Sub streaming
    "collect_pubsub",
    "iter_pubsub",
    "iter_pubsub_async",
    # Stream consumption (single stream with consumer groups)
    "read_stream",
    "scan_stream",
    "iter_stream",
    "iter_stream_async",
    "ack_entries",
    # Stream consumer with checkpointing
    "StreamConsumer",
    "CheckpointStore",
    "MemoryCheckpointStore",
    "FileCheckpointStore",
    "RedisCheckpointStore",
    "ConsumerStats",
    # Environment defaults
    "get_default_batch_size",
    "get_default_count_hint",
    "get_default_timeout_ms",
    # Smart scan (auto-detection)
    "smart_scan",
    "explain_scan",
    "list_indexes",
    "find_index_for_pattern",
    "ExecutionStrategy",
    "QueryPlan",
    "DetectedIndex",
    # Geospatial
    "geo_add",
    "geo_add_from_dataframe",
    "geo_radius",
    "geo_radius_by_member",
    "geo_dist",
    "geo_pos",
    "geo_dist_matrix",
    "geo_hash",
    "GeoAddResult",
    # Version
    "__version__",
]

__version__ = "0.1.0"
