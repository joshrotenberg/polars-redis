"""Configuration options for Redis operations.

This module provides option classes following the polars-io pattern:
- Dataclasses with sensible defaults
- Builder-style methods for configuration
- Environment variable overrides for common settings

Example:
    >>> from polars_redis.options import HashScanOptions
    >>>
    >>> opts = HashScanOptions(
    ...     pattern="user:*",
    ...     batch_size=500,
    ...     include_ttl=True,
    ... )
    >>> # Or use builder pattern
    >>> opts = HashScanOptions(pattern="user:*").with_batch_size(500).with_ttl(True)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

__all__ = [
    "ScanOptions",
    "HashScanOptions",
    "JsonScanOptions",
    "StringScanOptions",
    "StreamScanOptions",
    "TimeSeriesScanOptions",
    "SearchOptions",
    "get_default_batch_size",
    "get_default_count_hint",
    "get_default_timeout_ms",
]


def get_default_batch_size() -> int:
    """Get the default batch size from environment or fallback."""
    return int(os.environ.get("POLARS_REDIS_BATCH_SIZE", "1000"))


def get_default_count_hint() -> int:
    """Get the default count hint from environment or fallback."""
    return int(os.environ.get("POLARS_REDIS_COUNT_HINT", "100"))


def get_default_timeout_ms() -> int:
    """Get the default timeout in milliseconds from environment or fallback."""
    return int(os.environ.get("POLARS_REDIS_TIMEOUT_MS", "30000"))


@dataclass
class ScanOptions:
    """Base scan options shared across all Redis data types.

    Attributes:
        pattern: Key pattern to match (e.g., "user:*").
        batch_size: Number of keys to process per batch.
        count_hint: SCAN COUNT hint for Redis.
        n_rows: Maximum total rows to return (None for unlimited).
    """

    pattern: str = "*"
    batch_size: int = field(default_factory=get_default_batch_size)
    count_hint: int = field(default_factory=get_default_count_hint)
    n_rows: int | None = None

    def with_pattern(self, pattern: str) -> ScanOptions:
        """Set the key pattern."""
        self.pattern = pattern
        return self

    def with_batch_size(self, size: int) -> ScanOptions:
        """Set the batch size."""
        self.batch_size = size
        return self

    def with_count_hint(self, count: int) -> ScanOptions:
        """Set the COUNT hint for SCAN."""
        self.count_hint = count
        return self

    def with_n_rows(self, n: int) -> ScanOptions:
        """Set the maximum number of rows to return."""
        self.n_rows = n
        return self


@dataclass
class HashScanOptions:
    """Options for scanning Redis hashes.

    Example:
        >>> opts = HashScanOptions(
        ...     pattern="user:*",
        ...     batch_size=500,
        ...     include_ttl=True,
        ...     projection=["name", "email"],
        ... )

    Attributes:
        pattern: Key pattern to match (e.g., "user:*").
        batch_size: Number of keys to process per batch.
        count_hint: SCAN COUNT hint for Redis.
        n_rows: Maximum total rows to return (None for unlimited).
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column.
        include_ttl: Whether to include the TTL as a column.
        ttl_column_name: Name of the TTL column.
        include_row_index: Whether to include the row index as a column.
        row_index_column_name: Name of the row index column.
        row_index_offset: Starting offset for the row index.
        projection: Fields to fetch (None = all via HGETALL).
    """

    pattern: str = "*"
    batch_size: int = field(default_factory=get_default_batch_size)
    count_hint: int = field(default_factory=get_default_count_hint)
    n_rows: int | None = None
    include_key: bool = True
    key_column_name: str = "_key"
    include_ttl: bool = False
    ttl_column_name: str = "_ttl"
    include_row_index: bool = False
    row_index_column_name: str = "_index"
    row_index_offset: int = 0
    projection: list[str] | None = None

    def with_pattern(self, pattern: str) -> HashScanOptions:
        """Set the key pattern."""
        self.pattern = pattern
        return self

    def with_batch_size(self, size: int) -> HashScanOptions:
        """Set the batch size."""
        self.batch_size = size
        return self

    def with_count_hint(self, count: int) -> HashScanOptions:
        """Set the COUNT hint for SCAN."""
        self.count_hint = count
        return self

    def with_n_rows(self, n: int) -> HashScanOptions:
        """Set the maximum number of rows to return."""
        self.n_rows = n
        return self

    def with_key(self, include: bool = True, name: str | None = None) -> HashScanOptions:
        """Configure the key column."""
        self.include_key = include
        if name is not None:
            self.key_column_name = name
        return self

    def with_ttl(self, include: bool = True, name: str | None = None) -> HashScanOptions:
        """Configure the TTL column."""
        self.include_ttl = include
        if name is not None:
            self.ttl_column_name = name
        return self

    def with_row_index(
        self, include: bool = True, name: str | None = None, offset: int | None = None
    ) -> HashScanOptions:
        """Configure the row index column."""
        self.include_row_index = include
        if name is not None:
            self.row_index_column_name = name
        if offset is not None:
            self.row_index_offset = offset
        return self

    def with_projection(self, fields: list[str]) -> HashScanOptions:
        """Set the fields to fetch (projection)."""
        self.projection = fields
        return self


@dataclass
class JsonScanOptions:
    """Options for scanning Redis JSON documents.

    Example:
        >>> opts = JsonScanOptions(
        ...     pattern="doc:*",
        ...     batch_size=500,
        ...     path="$.user",
        ... )

    Attributes:
        pattern: Key pattern to match (e.g., "doc:*").
        batch_size: Number of keys to process per batch.
        count_hint: SCAN COUNT hint for Redis.
        n_rows: Maximum total rows to return (None for unlimited).
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column.
        include_ttl: Whether to include the TTL as a column.
        ttl_column_name: Name of the TTL column.
        include_row_index: Whether to include the row index as a column.
        row_index_column_name: Name of the row index column.
        row_index_offset: Starting offset for the row index.
        path: JSON path to extract (None = root "$").
        projection: Fields to fetch from the JSON document.
    """

    pattern: str = "*"
    batch_size: int = field(default_factory=get_default_batch_size)
    count_hint: int = field(default_factory=get_default_count_hint)
    n_rows: int | None = None
    include_key: bool = True
    key_column_name: str = "_key"
    include_ttl: bool = False
    ttl_column_name: str = "_ttl"
    include_row_index: bool = False
    row_index_column_name: str = "_index"
    row_index_offset: int = 0
    path: str | None = None
    projection: list[str] | None = None

    def with_pattern(self, pattern: str) -> JsonScanOptions:
        """Set the key pattern."""
        self.pattern = pattern
        return self

    def with_batch_size(self, size: int) -> JsonScanOptions:
        """Set the batch size."""
        self.batch_size = size
        return self

    def with_count_hint(self, count: int) -> JsonScanOptions:
        """Set the COUNT hint for SCAN."""
        self.count_hint = count
        return self

    def with_n_rows(self, n: int) -> JsonScanOptions:
        """Set the maximum number of rows to return."""
        self.n_rows = n
        return self

    def with_key(self, include: bool = True, name: str | None = None) -> JsonScanOptions:
        """Configure the key column."""
        self.include_key = include
        if name is not None:
            self.key_column_name = name
        return self

    def with_ttl(self, include: bool = True, name: str | None = None) -> JsonScanOptions:
        """Configure the TTL column."""
        self.include_ttl = include
        if name is not None:
            self.ttl_column_name = name
        return self

    def with_row_index(
        self, include: bool = True, name: str | None = None, offset: int | None = None
    ) -> JsonScanOptions:
        """Configure the row index column."""
        self.include_row_index = include
        if name is not None:
            self.row_index_column_name = name
        if offset is not None:
            self.row_index_offset = offset
        return self

    def with_path(self, path: str) -> JsonScanOptions:
        """Set the JSON path to extract."""
        self.path = path
        return self

    def with_projection(self, fields: list[str]) -> JsonScanOptions:
        """Set the fields to fetch (projection)."""
        self.projection = fields
        return self


@dataclass
class StringScanOptions:
    """Options for scanning Redis strings.

    Example:
        >>> opts = StringScanOptions(
        ...     pattern="counter:*",
        ...     value_column_name="count",
        ...     include_ttl=True,
        ... )

    Attributes:
        pattern: Key pattern to match (e.g., "counter:*").
        batch_size: Number of keys to process per batch.
        count_hint: SCAN COUNT hint for Redis.
        n_rows: Maximum total rows to return (None for unlimited).
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column.
        value_column_name: Name of the value column.
        include_ttl: Whether to include the TTL as a column.
        ttl_column_name: Name of the TTL column.
        include_row_index: Whether to include the row index as a column.
        row_index_column_name: Name of the row index column.
        row_index_offset: Starting offset for the row index.
    """

    pattern: str = "*"
    batch_size: int = field(default_factory=get_default_batch_size)
    count_hint: int = field(default_factory=get_default_count_hint)
    n_rows: int | None = None
    include_key: bool = True
    key_column_name: str = "_key"
    value_column_name: str = "value"
    include_ttl: bool = False
    ttl_column_name: str = "_ttl"
    include_row_index: bool = False
    row_index_column_name: str = "_index"
    row_index_offset: int = 0

    def with_pattern(self, pattern: str) -> StringScanOptions:
        """Set the key pattern."""
        self.pattern = pattern
        return self

    def with_batch_size(self, size: int) -> StringScanOptions:
        """Set the batch size."""
        self.batch_size = size
        return self

    def with_count_hint(self, count: int) -> StringScanOptions:
        """Set the COUNT hint for SCAN."""
        self.count_hint = count
        return self

    def with_n_rows(self, n: int) -> StringScanOptions:
        """Set the maximum number of rows to return."""
        self.n_rows = n
        return self

    def with_key(self, include: bool = True, name: str | None = None) -> StringScanOptions:
        """Configure the key column."""
        self.include_key = include
        if name is not None:
            self.key_column_name = name
        return self

    def with_value_column_name(self, name: str) -> StringScanOptions:
        """Set the value column name."""
        self.value_column_name = name
        return self

    def with_ttl(self, include: bool = True, name: str | None = None) -> StringScanOptions:
        """Configure the TTL column."""
        self.include_ttl = include
        if name is not None:
            self.ttl_column_name = name
        return self

    def with_row_index(
        self, include: bool = True, name: str | None = None, offset: int | None = None
    ) -> StringScanOptions:
        """Configure the row index column."""
        self.include_row_index = include
        if name is not None:
            self.row_index_column_name = name
        if offset is not None:
            self.row_index_offset = offset
        return self


@dataclass
class StreamScanOptions:
    """Options for scanning Redis streams.

    Example:
        >>> opts = StreamScanOptions(
        ...     pattern="events:*",
        ...     start_id="-",
        ...     end_id="+",
        ...     count_per_stream=1000,
        ... )

    Attributes:
        pattern: Key pattern to match (e.g., "events:*").
        batch_size: Number of keys to process per batch.
        count_hint: SCAN COUNT hint for Redis.
        n_rows: Maximum total rows to return (None for unlimited).
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column.
        include_id: Whether to include the entry ID column.
        include_timestamp: Whether to include the timestamp column.
        include_sequence: Whether to include the sequence number column.
        include_row_index: Whether to include the row index as a column.
        row_index_column_name: Name of the row index column.
        start_id: Start ID for XRANGE (default: "-").
        end_id: End ID for XRANGE (default: "+").
        count_per_stream: Maximum entries per stream.
        fields: Fields to extract from stream entries.
    """

    pattern: str = "*"
    batch_size: int = field(default_factory=get_default_batch_size)
    count_hint: int = field(default_factory=get_default_count_hint)
    n_rows: int | None = None
    include_key: bool = True
    key_column_name: str = "_key"
    include_id: bool = True
    include_timestamp: bool = True
    include_sequence: bool = False
    include_row_index: bool = False
    row_index_column_name: str = "_index"
    start_id: str = "-"
    end_id: str = "+"
    count_per_stream: int | None = None
    fields: list[str] | None = None

    def with_pattern(self, pattern: str) -> StreamScanOptions:
        """Set the key pattern."""
        self.pattern = pattern
        return self

    def with_batch_size(self, size: int) -> StreamScanOptions:
        """Set the batch size."""
        self.batch_size = size
        return self

    def with_count_hint(self, count: int) -> StreamScanOptions:
        """Set the COUNT hint for SCAN."""
        self.count_hint = count
        return self

    def with_n_rows(self, n: int) -> StreamScanOptions:
        """Set the maximum number of rows to return."""
        self.n_rows = n
        return self

    def with_key(self, include: bool = True, name: str | None = None) -> StreamScanOptions:
        """Configure the key column."""
        self.include_key = include
        if name is not None:
            self.key_column_name = name
        return self

    def with_id(self, include: bool = True) -> StreamScanOptions:
        """Configure the entry ID column."""
        self.include_id = include
        return self

    def with_timestamp(self, include: bool = True) -> StreamScanOptions:
        """Configure the timestamp column."""
        self.include_timestamp = include
        return self

    def with_sequence(self, include: bool = True) -> StreamScanOptions:
        """Configure the sequence number column."""
        self.include_sequence = include
        return self

    def with_row_index(self, include: bool = True, name: str | None = None) -> StreamScanOptions:
        """Configure the row index column."""
        self.include_row_index = include
        if name is not None:
            self.row_index_column_name = name
        return self

    def with_range(self, start_id: str = "-", end_id: str = "+") -> StreamScanOptions:
        """Set the range for XRANGE."""
        self.start_id = start_id
        self.end_id = end_id
        return self

    def with_count_per_stream(self, count: int) -> StreamScanOptions:
        """Set the maximum entries to fetch per stream."""
        self.count_per_stream = count
        return self

    def with_fields(self, fields: list[str]) -> StreamScanOptions:
        """Set the fields to extract from stream entries."""
        self.fields = fields
        return self


@dataclass
class TimeSeriesScanOptions:
    """Options for scanning Redis time series.

    Example:
        >>> opts = TimeSeriesScanOptions(
        ...     pattern="sensor:*",
        ...     start="-",
        ...     end="+",
        ...     aggregation="avg",
        ...     bucket_size_ms=60000,
        ... )

    Attributes:
        pattern: Key pattern to match (e.g., "sensor:*").
        batch_size: Number of keys to process per batch.
        count_hint: SCAN COUNT hint for Redis.
        n_rows: Maximum total rows to return (None for unlimited).
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column.
        timestamp_column_name: Name of the timestamp column.
        value_column_name: Name of the value column.
        include_row_index: Whether to include the row index as a column.
        row_index_column_name: Name of the row index column.
        start: Start timestamp for TS.RANGE (default: "-").
        end: End timestamp for TS.RANGE (default: "+").
        count_per_series: Maximum samples per time series.
        aggregation: Aggregation type (avg, sum, min, max, etc.).
        bucket_size_ms: Bucket size in milliseconds for aggregation.
    """

    pattern: str = "*"
    batch_size: int = field(default_factory=get_default_batch_size)
    count_hint: int = field(default_factory=get_default_count_hint)
    n_rows: int | None = None
    include_key: bool = True
    key_column_name: str = "_key"
    timestamp_column_name: str = "_ts"
    value_column_name: str = "value"
    include_row_index: bool = False
    row_index_column_name: str = "_index"
    start: str = "-"
    end: str = "+"
    count_per_series: int | None = None
    aggregation: str | None = None
    bucket_size_ms: int | None = None

    def with_pattern(self, pattern: str) -> TimeSeriesScanOptions:
        """Set the key pattern."""
        self.pattern = pattern
        return self

    def with_batch_size(self, size: int) -> TimeSeriesScanOptions:
        """Set the batch size."""
        self.batch_size = size
        return self

    def with_count_hint(self, count: int) -> TimeSeriesScanOptions:
        """Set the COUNT hint for SCAN."""
        self.count_hint = count
        return self

    def with_n_rows(self, n: int) -> TimeSeriesScanOptions:
        """Set the maximum number of rows to return."""
        self.n_rows = n
        return self

    def with_key(self, include: bool = True, name: str | None = None) -> TimeSeriesScanOptions:
        """Configure the key column."""
        self.include_key = include
        if name is not None:
            self.key_column_name = name
        return self

    def with_timestamp_column_name(self, name: str) -> TimeSeriesScanOptions:
        """Set the timestamp column name."""
        self.timestamp_column_name = name
        return self

    def with_value_column_name(self, name: str) -> TimeSeriesScanOptions:
        """Set the value column name."""
        self.value_column_name = name
        return self

    def with_row_index(
        self, include: bool = True, name: str | None = None
    ) -> TimeSeriesScanOptions:
        """Configure the row index column."""
        self.include_row_index = include
        if name is not None:
            self.row_index_column_name = name
        return self

    def with_range(self, start: str = "-", end: str = "+") -> TimeSeriesScanOptions:
        """Set the range for TS.RANGE."""
        self.start = start
        self.end = end
        return self

    def with_count_per_series(self, count: int) -> TimeSeriesScanOptions:
        """Set the maximum samples to fetch per time series."""
        self.count_per_series = count
        return self

    def with_aggregation(self, agg_type: str, bucket_size_ms: int) -> TimeSeriesScanOptions:
        """Set aggregation type and bucket size."""
        self.aggregation = agg_type
        self.bucket_size_ms = bucket_size_ms
        return self


@dataclass
class SearchOptions:
    """Options for RediSearch FT.SEARCH queries.

    Example:
        >>> opts = SearchOptions(
        ...     index="users_idx",
        ...     query="@age:[30 +inf]",
        ...     sort_by="score",
        ...     sort_ascending=False,
        ... )

    Attributes:
        index: RediSearch index name.
        query: Search query string (e.g., "@age:[30 +inf]").
        batch_size: Number of documents to fetch per batch.
        n_rows: Maximum total rows to return (None for unlimited).
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column.
        include_ttl: Whether to include the TTL as a column.
        ttl_column_name: Name of the TTL column.
        include_row_index: Whether to include the row index as a column.
        row_index_column_name: Name of the row index column.
        sort_by: Optional field name to sort results by.
        sort_ascending: Sort direction (default: True for ascending).
        projection: Fields to return (None = all indexed fields).
    """

    index: str = ""
    query: str = "*"
    batch_size: int = field(default_factory=get_default_batch_size)
    n_rows: int | None = None
    include_key: bool = True
    key_column_name: str = "_key"
    include_ttl: bool = False
    ttl_column_name: str = "_ttl"
    include_row_index: bool = False
    row_index_column_name: str = "_index"
    sort_by: str | None = None
    sort_ascending: bool = True
    projection: list[str] | None = None

    def with_index(self, index: str) -> SearchOptions:
        """Set the RediSearch index name."""
        self.index = index
        return self

    def with_query(self, query: str) -> SearchOptions:
        """Set the search query."""
        self.query = query
        return self

    def with_batch_size(self, size: int) -> SearchOptions:
        """Set the batch size."""
        self.batch_size = size
        return self

    def with_n_rows(self, n: int) -> SearchOptions:
        """Set the maximum number of rows to return."""
        self.n_rows = n
        return self

    def with_key(self, include: bool = True, name: str | None = None) -> SearchOptions:
        """Configure the key column."""
        self.include_key = include
        if name is not None:
            self.key_column_name = name
        return self

    def with_ttl(self, include: bool = True, name: str | None = None) -> SearchOptions:
        """Configure the TTL column."""
        self.include_ttl = include
        if name is not None:
            self.ttl_column_name = name
        return self

    def with_row_index(self, include: bool = True, name: str | None = None) -> SearchOptions:
        """Configure the row index column."""
        self.include_row_index = include
        if name is not None:
            self.row_index_column_name = name
        return self

    def with_sort(self, field: str, ascending: bool = True) -> SearchOptions:
        """Set the sort field and direction."""
        self.sort_by = field
        self.sort_ascending = ascending
        return self

    def with_projection(self, fields: list[str]) -> SearchOptions:
        """Set the fields to return (projection)."""
        self.projection = fields
        return self
