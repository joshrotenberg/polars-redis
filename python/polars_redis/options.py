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
    "SetScanOptions",
    "ListScanOptions",
    "ZSetScanOptions",
    "StreamScanOptions",
    "TimeSeriesScanOptions",
    "SearchOptions",
    "SummarizeOptions",
    "HighlightOptions",
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
class SetScanOptions:
    """Options for scanning Redis sets.

    Example:
        >>> opts = SetScanOptions(
        ...     pattern="tags:*",
        ...     member_column_name="tag",
        ... )

    Attributes:
        pattern: Key pattern to match (e.g., "tags:*").
        batch_size: Number of keys to process per batch.
        count_hint: SCAN COUNT hint for Redis.
        n_rows: Maximum total rows to return (None for unlimited).
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column.
        member_column_name: Name of the member column.
        include_row_index: Whether to include the row index as a column.
        row_index_column_name: Name of the row index column.
    """

    pattern: str = "*"
    batch_size: int = field(default_factory=get_default_batch_size)
    count_hint: int = field(default_factory=get_default_count_hint)
    n_rows: int | None = None
    include_key: bool = True
    key_column_name: str = "_key"
    member_column_name: str = "member"
    include_row_index: bool = False
    row_index_column_name: str = "_index"

    def with_pattern(self, pattern: str) -> SetScanOptions:
        """Set the key pattern."""
        self.pattern = pattern
        return self

    def with_batch_size(self, size: int) -> SetScanOptions:
        """Set the batch size."""
        self.batch_size = size
        return self

    def with_count_hint(self, count: int) -> SetScanOptions:
        """Set the COUNT hint for SCAN."""
        self.count_hint = count
        return self

    def with_n_rows(self, n: int) -> SetScanOptions:
        """Set the maximum number of rows to return."""
        self.n_rows = n
        return self

    def with_key(self, include: bool = True, name: str | None = None) -> SetScanOptions:
        """Configure the key column."""
        self.include_key = include
        if name is not None:
            self.key_column_name = name
        return self

    def with_member_column_name(self, name: str) -> SetScanOptions:
        """Set the member column name."""
        self.member_column_name = name
        return self

    def with_row_index(self, include: bool = True, name: str | None = None) -> SetScanOptions:
        """Configure the row index column."""
        self.include_row_index = include
        if name is not None:
            self.row_index_column_name = name
        return self


@dataclass
class ListScanOptions:
    """Options for scanning Redis lists.

    Example:
        >>> opts = ListScanOptions(
        ...     pattern="queue:*",
        ...     element_column_name="item",
        ...     include_position=True,
        ... )

    Attributes:
        pattern: Key pattern to match (e.g., "queue:*").
        batch_size: Number of keys to process per batch.
        count_hint: SCAN COUNT hint for Redis.
        n_rows: Maximum total rows to return (None for unlimited).
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column.
        element_column_name: Name of the element column.
        include_position: Whether to include the position index.
        position_column_name: Name of the position column.
        include_row_index: Whether to include the row index as a column.
        row_index_column_name: Name of the row index column.
    """

    pattern: str = "*"
    batch_size: int = field(default_factory=get_default_batch_size)
    count_hint: int = field(default_factory=get_default_count_hint)
    n_rows: int | None = None
    include_key: bool = True
    key_column_name: str = "_key"
    element_column_name: str = "element"
    include_position: bool = False
    position_column_name: str = "position"
    include_row_index: bool = False
    row_index_column_name: str = "_index"

    def with_pattern(self, pattern: str) -> ListScanOptions:
        """Set the key pattern."""
        self.pattern = pattern
        return self

    def with_batch_size(self, size: int) -> ListScanOptions:
        """Set the batch size."""
        self.batch_size = size
        return self

    def with_count_hint(self, count: int) -> ListScanOptions:
        """Set the COUNT hint for SCAN."""
        self.count_hint = count
        return self

    def with_n_rows(self, n: int) -> ListScanOptions:
        """Set the maximum number of rows to return."""
        self.n_rows = n
        return self

    def with_key(self, include: bool = True, name: str | None = None) -> ListScanOptions:
        """Configure the key column."""
        self.include_key = include
        if name is not None:
            self.key_column_name = name
        return self

    def with_element_column_name(self, name: str) -> ListScanOptions:
        """Set the element column name."""
        self.element_column_name = name
        return self

    def with_position(self, include: bool = True, name: str | None = None) -> ListScanOptions:
        """Configure the position column."""
        self.include_position = include
        if name is not None:
            self.position_column_name = name
        return self

    def with_row_index(self, include: bool = True, name: str | None = None) -> ListScanOptions:
        """Configure the row index column."""
        self.include_row_index = include
        if name is not None:
            self.row_index_column_name = name
        return self


@dataclass
class ZSetScanOptions:
    """Options for scanning Redis sorted sets.

    Example:
        >>> opts = ZSetScanOptions(
        ...     pattern="leaderboard:*",
        ...     member_column_name="player",
        ...     include_rank=True,
        ... )

    Attributes:
        pattern: Key pattern to match (e.g., "leaderboard:*").
        batch_size: Number of keys to process per batch.
        count_hint: SCAN COUNT hint for Redis.
        n_rows: Maximum total rows to return (None for unlimited).
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column.
        member_column_name: Name of the member column.
        score_column_name: Name of the score column.
        include_rank: Whether to include the rank index.
        rank_column_name: Name of the rank column.
        include_row_index: Whether to include the row index as a column.
        row_index_column_name: Name of the row index column.
    """

    pattern: str = "*"
    batch_size: int = field(default_factory=get_default_batch_size)
    count_hint: int = field(default_factory=get_default_count_hint)
    n_rows: int | None = None
    include_key: bool = True
    key_column_name: str = "_key"
    member_column_name: str = "member"
    score_column_name: str = "score"
    include_rank: bool = False
    rank_column_name: str = "rank"
    include_row_index: bool = False
    row_index_column_name: str = "_index"

    def with_pattern(self, pattern: str) -> ZSetScanOptions:
        """Set the key pattern."""
        self.pattern = pattern
        return self

    def with_batch_size(self, size: int) -> ZSetScanOptions:
        """Set the batch size."""
        self.batch_size = size
        return self

    def with_count_hint(self, count: int) -> ZSetScanOptions:
        """Set the COUNT hint for SCAN."""
        self.count_hint = count
        return self

    def with_n_rows(self, n: int) -> ZSetScanOptions:
        """Set the maximum number of rows to return."""
        self.n_rows = n
        return self

    def with_key(self, include: bool = True, name: str | None = None) -> ZSetScanOptions:
        """Configure the key column."""
        self.include_key = include
        if name is not None:
            self.key_column_name = name
        return self

    def with_member_column_name(self, name: str) -> ZSetScanOptions:
        """Set the member column name."""
        self.member_column_name = name
        return self

    def with_score_column_name(self, name: str) -> ZSetScanOptions:
        """Set the score column name."""
        self.score_column_name = name
        return self

    def with_rank(self, include: bool = True, name: str | None = None) -> ZSetScanOptions:
        """Configure the rank column."""
        self.include_rank = include
        if name is not None:
            self.rank_column_name = name
        return self

    def with_row_index(self, include: bool = True, name: str | None = None) -> ZSetScanOptions:
        """Configure the row index column."""
        self.include_row_index = include
        if name is not None:
            self.row_index_column_name = name
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
class SummarizeOptions:
    """Options for SUMMARIZE in FT.SEARCH results.

    SUMMARIZE returns a snippet of the matching text with the search terms
    highlighted, useful for search result previews.

    Attributes:
        fields: Fields to summarize (None = all TEXT fields).
        frags: Number of fragments to return per field.
        len: Length of each fragment in words.
        separator: Separator between fragments (default: "...").
    """

    fields: list[str] | None = None
    frags: int = 3
    len: int = 20
    separator: str = "..."


@dataclass
class HighlightOptions:
    """Options for HIGHLIGHT in FT.SEARCH results.

    HIGHLIGHT wraps matching terms with open/close tags for display.

    Attributes:
        fields: Fields to highlight (None = all TEXT fields).
        open_tag: Opening tag for highlighted terms (default: "<b>").
        close_tag: Closing tag for highlighted terms (default: "</b>").
    """

    fields: list[str] | None = None
    open_tag: str = "<b>"
    close_tag: str = "</b>"


@dataclass
class SearchOptions:
    """Options for RediSearch FT.SEARCH queries.

    This class provides comprehensive configuration for FT.SEARCH including
    query modifiers, result formatting, and advanced options.

    Example:
        >>> opts = SearchOptions(
        ...     index="users_idx",
        ...     query="@age:[30 +inf]",
        ...     sort_by="score",
        ...     sort_ascending=False,
        ... )
        >>>
        >>> # With advanced options
        >>> opts = SearchOptions(
        ...     index="articles_idx",
        ...     query="python programming",
        ...     verbatim=True,  # Exact match, no stemming
        ...     language="english",
        ...     highlight=HighlightOptions(open_tag="<em>", close_tag="</em>"),
        ...     summarize=SummarizeOptions(len=50),
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
        include_score: Whether to include the relevance score.
        score_column_name: Name of the score column.
        sort_by: Optional field name to sort results by.
        sort_ascending: Sort direction (default: True for ascending).
        projection: Fields to return (None = all indexed fields).
        verbatim: If True, disable stemming (exact term matching).
        no_stopwords: If True, include stop words in the query.
        language: Language for stemming (e.g., "english", "spanish").
        scorer: Custom scorer function name (e.g., "BM25", "TFIDF").
        in_keys: Limit search to specific document keys.
        in_fields: Limit search to specific fields.
        timeout_ms: Query timeout in milliseconds.
        dialect: RediSearch dialect version (1, 2, 3, or 4).
        params: Additional PARAMS for parameterized queries.
        summarize: Options for generating text snippets.
        highlight: Options for highlighting matching terms.
        slop: Default slop for phrase queries.
        in_order: Whether phrase terms must appear in order.
        expander: Query expander to use.
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
    include_score: bool = False
    score_column_name: str = "_score"
    sort_by: str | None = None
    sort_ascending: bool = True
    projection: list[str] | None = None
    # Query modifiers
    verbatim: bool = False
    no_stopwords: bool = False
    language: str | None = None
    scorer: str | None = None
    # Filtering
    in_keys: list[str] | None = None
    in_fields: list[str] | None = None
    # Performance
    timeout_ms: int | None = None
    # Dialect and params
    dialect: int | None = None
    params: dict[str, str | bytes] | None = None
    # Result enrichment
    summarize: SummarizeOptions | None = None
    highlight: HighlightOptions | None = None
    # Phrase options
    slop: int | None = None
    in_order: bool | None = None
    expander: str | None = None

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

    def with_score(self, include: bool = True, name: str | None = None) -> SearchOptions:
        """Configure the relevance score column."""
        self.include_score = include
        if name is not None:
            self.score_column_name = name
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

    def with_verbatim(self, verbatim: bool = True) -> SearchOptions:
        """Disable stemming for exact term matching."""
        self.verbatim = verbatim
        return self

    def with_no_stopwords(self, no_stopwords: bool = True) -> SearchOptions:
        """Include stop words in the query."""
        self.no_stopwords = no_stopwords
        return self

    def with_language(self, language: str) -> SearchOptions:
        """Set the language for stemming."""
        self.language = language
        return self

    def with_scorer(self, scorer: str) -> SearchOptions:
        """Set the scoring function (e.g., 'BM25', 'TFIDF', 'DISMAX')."""
        self.scorer = scorer
        return self

    def with_in_keys(self, keys: list[str]) -> SearchOptions:
        """Limit search to specific document keys."""
        self.in_keys = keys
        return self

    def with_in_fields(self, fields: list[str]) -> SearchOptions:
        """Limit search to specific fields."""
        self.in_fields = fields
        return self

    def with_timeout(self, timeout_ms: int) -> SearchOptions:
        """Set query timeout in milliseconds."""
        self.timeout_ms = timeout_ms
        return self

    def with_dialect(self, dialect: int) -> SearchOptions:
        """Set RediSearch dialect version (1, 2, 3, or 4)."""
        self.dialect = dialect
        return self

    def with_params(self, params: dict[str, str | bytes]) -> SearchOptions:
        """Set additional PARAMS for parameterized queries."""
        self.params = params
        return self

    def with_summarize(
        self,
        fields: list[str] | None = None,
        frags: int = 3,
        len: int = 20,
        separator: str = "...",
    ) -> SearchOptions:
        """Configure text summarization for search results."""
        self.summarize = SummarizeOptions(fields=fields, frags=frags, len=len, separator=separator)
        return self

    def with_highlight(
        self,
        fields: list[str] | None = None,
        open_tag: str = "<b>",
        close_tag: str = "</b>",
    ) -> SearchOptions:
        """Configure term highlighting for search results."""
        self.highlight = HighlightOptions(fields=fields, open_tag=open_tag, close_tag=close_tag)
        return self

    def with_slop(self, slop: int) -> SearchOptions:
        """Set default slop for phrase queries."""
        self.slop = slop
        return self

    def with_in_order(self, in_order: bool = True) -> SearchOptions:
        """Set whether phrase terms must appear in order."""
        self.in_order = in_order
        return self

    def with_expander(self, expander: str) -> SearchOptions:
        """Set the query expander to use."""
        self.expander = expander
        return self
