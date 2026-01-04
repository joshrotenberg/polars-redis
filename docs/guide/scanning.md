# Scanning Data

polars-redis provides scan functions for eight Redis data types: hashes, JSON, strings, sets, lists, sorted sets, streams, and time series.

## Scanning Hashes

`scan_hashes` scans Redis hashes matching a pattern:

```python
import polars as pl
import polars_redis as redis

lf = redis.scan_hashes(
    "redis://localhost:6379",
    pattern="user:*",
    schema={"name": pl.Utf8, "age": pl.Int64, "score": pl.Float64},
)
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | str | required | Redis connection URL |
| `pattern` | str | `"*"` | Key pattern to match |
| `schema` | dict | required | Field names to Polars dtypes |
| `include_key` | bool | `True` | Include Redis key as column |
| `key_column_name` | str | `"_key"` | Name of key column |
| `include_ttl` | bool | `False` | Include TTL as column |
| `ttl_column_name` | str | `"_ttl"` | Name of TTL column |
| `include_row_index` | bool | `False` | Include row index column |
| `row_index_column_name` | str | `"_index"` | Name of index column |
| `batch_size` | int | `1000` | Keys per batch |
| `count_hint` | int | `100` | Redis SCAN COUNT hint |
| `parallel` | int | `None` | Number of parallel workers for fetching |

### Supported Types

| Polars Type | Redis Value |
|-------------|-------------|
| `pl.Utf8` / `pl.String` | Any string |
| `pl.Int64` | Integer string |
| `pl.Float64` | Float string |
| `pl.Boolean` | `"true"`, `"false"`, `"1"`, `"0"` |
| `pl.Date` | ISO date or epoch days |
| `pl.Datetime` | ISO datetime or Unix timestamp |

## Scanning JSON

`scan_json` scans RedisJSON documents:

```python
lf = redis.scan_json(
    "redis://localhost:6379",
    pattern="doc:*",
    schema={"title": pl.Utf8, "views": pl.Int64, "rating": pl.Float64},
)
```

Parameters are identical to `scan_hashes`. Requires the RedisJSON module.

## Scanning Strings

`scan_strings` scans Redis string values:

```python
# Scan as UTF-8 strings
lf = redis.scan_strings(
    "redis://localhost:6379",
    pattern="cache:*",
)

# Scan as integers (e.g., counters)
lf = redis.scan_strings(
    "redis://localhost:6379",
    pattern="counter:*",
    value_type=pl.Int64,
)
```

### String-specific Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `value_type` | type | `pl.Utf8` | Type for value column |
| `value_column_name` | str | `"value"` | Name of value column |

## Eager Reading

Each scan function has an eager counterpart that returns a DataFrame:

```python
# Lazy (returns LazyFrame)
lf = redis.scan_hashes(...)
df = lf.collect()

# Eager (returns DataFrame directly)
df = redis.read_hashes(...)
```

## Projection Pushdown

When you select specific columns, polars-redis optimizes the Redis query:

```python
lf = redis.scan_hashes(
    "redis://localhost:6379",
    pattern="user:*",
    schema={"name": pl.Utf8, "age": pl.Int64, "email": pl.Utf8, "phone": pl.Utf8},
)

# Only 'name' and 'age' are fetched from Redis
df = lf.select(["name", "age"]).collect()
```

For hashes, this uses `HMGET` instead of `HGETALL`, reducing network transfer.

## Metadata Columns

### Key Column

Include the Redis key:

```python
lf = redis.scan_hashes(
    ...,
    include_key=True,
    key_column_name="_key",  # default
)
```

### TTL Column

Include time-to-live (seconds until expiration, -1 if no expiry):

```python
lf = redis.scan_hashes(
    ...,
    include_ttl=True,
    ttl_column_name="_ttl",
)
```

### Row Index Column

Include a monotonic row index:

```python
lf = redis.scan_hashes(
    ...,
    include_row_index=True,
    row_index_column_name="_index",
)
```

## Scanning Sets

`scan_sets` scans Redis sets, returning one row per member:

```python
lf = redis.scan_sets(
    "redis://localhost:6379",
    pattern="tags:*",
)
```

### Set-specific Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `member_column_name` | str | `"member"` | Name of member column |

### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `_key` | Utf8 | Redis key (if `include_key=True`) |
| `member` | Utf8 | Set member value |
| `_index` | UInt64 | Row index (if `include_row_index=True`) |

## Scanning Lists

`scan_lists` scans Redis lists, returning one row per element:

```python
lf = redis.scan_lists(
    "redis://localhost:6379",
    pattern="queue:*",
)

# Include position index
lf = redis.scan_lists(
    "redis://localhost:6379",
    pattern="queue:*",
    include_position=True,
)
```

### List-specific Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `element_column_name` | str | `"element"` | Name of element column |
| `include_position` | bool | `False` | Include position index |
| `position_column_name` | str | `"position"` | Name of position column |

### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `_key` | Utf8 | Redis key (if `include_key=True`) |
| `element` | Utf8 | List element value |
| `position` | Int64 | 0-based position (if `include_position=True`) |
| `_index` | UInt64 | Row index (if `include_row_index=True`) |

## Scanning Sorted Sets

`scan_zsets` scans Redis sorted sets, returning one row per member with its score:

```python
lf = redis.scan_zsets(
    "redis://localhost:6379",
    pattern="leaderboard:*",
)

# Include rank (0-based position by score)
lf = redis.scan_zsets(
    "redis://localhost:6379",
    pattern="leaderboard:*",
    include_rank=True,
)
```

### Sorted Set-specific Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `member_column_name` | str | `"member"` | Name of member column |
| `score_column_name` | str | `"score"` | Name of score column |
| `include_rank` | bool | `False` | Include rank index |
| `rank_column_name` | str | `"rank"` | Name of rank column |

### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `_key` | Utf8 | Redis key (if `include_key=True`) |
| `member` | Utf8 | Sorted set member |
| `score` | Float64 | Member's score |
| `rank` | Int64 | 0-based rank by score (if `include_rank=True`) |
| `_index` | UInt64 | Row index (if `include_row_index=True`) |

## Scanning Streams

`scan_streams` scans Redis Streams, returning one row per entry:

```python
lf = redis.scan_streams(
    "redis://localhost:6379",
    pattern="events:*",
    fields=["action", "user_id", "timestamp"],
)

# Filter by entry ID range
lf = redis.scan_streams(
    "redis://localhost:6379",
    pattern="events:*",
    fields=["action", "user_id"],
    start_id="-",      # oldest
    end_id="+",        # newest
)
```

### Stream-specific Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `fields` | list | `[]` | Field names to extract from entries |
| `start_id` | str | `"-"` | Start entry ID (or "-" for oldest) |
| `end_id` | str | `"+"` | End entry ID (or "+" for newest) |
| `count_per_stream` | int | None | Max entries per stream |
| `include_id` | bool | `True` | Include entry ID as column |
| `id_column_name` | str | `"_id"` | Name of entry ID column |
| `include_timestamp` | bool | `True` | Include timestamp as column |
| `timestamp_column_name` | str | `"_ts"` | Name of timestamp column |
| `include_sequence` | bool | `False` | Include sequence number |
| `sequence_column_name` | str | `"_seq"` | Name of sequence column |

### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `_key` | Utf8 | Redis key (if `include_key=True`) |
| `_id` | Utf8 | Entry ID e.g. "1234567890123-0" (if `include_id=True`) |
| `_ts` | Timestamp | Timestamp from entry ID (if `include_timestamp=True`) |
| `_seq` | UInt64 | Sequence number (if `include_sequence=True`) |
| `<field>` | Utf8 | User-defined fields (nullable) |
| `_index` | UInt64 | Row index (if `include_row_index=True`) |

## Scanning Time Series

`scan_timeseries` scans RedisTimeSeries data, returning one row per sample:

```python
lf = redis.scan_timeseries(
    "redis://localhost:6379",
    pattern="sensor:*",
)

# With time range
lf = redis.scan_timeseries(
    "redis://localhost:6379",
    pattern="sensor:*",
    start="-",         # oldest
    end="+",           # newest
)

# With aggregation (server-side downsampling)
lf = redis.scan_timeseries(
    "redis://localhost:6379",
    pattern="sensor:*",
    aggregation="avg",
    bucket_size_ms=60000,  # 1 minute buckets
)
```

### TimeSeries-specific Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `start` | str | `"-"` | Start timestamp (or "-" for oldest) |
| `end` | str | `"+"` | End timestamp (or "+" for newest) |
| `count_per_series` | int | None | Max samples per time series |
| `aggregation` | str | None | Aggregation type (see below) |
| `bucket_size_ms` | int | None | Bucket size in ms (required with aggregation) |
| `value_column_name` | str | `"value"` | Name of value column |
| `label_columns` | list | `[]` | Label names to include as columns |

### Aggregation Types

| Type | Description |
|------|-------------|
| `avg` | Average value |
| `sum` | Sum of values |
| `min` | Minimum value |
| `max` | Maximum value |
| `range` | Max - Min |
| `count` | Number of samples |
| `first` | First value |
| `last` | Last value |
| `std.p` | Population standard deviation |
| `std.s` | Sample standard deviation |
| `var.p` | Population variance |
| `var.s` | Sample variance |

### Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `_key` | Utf8 | Redis key (if `include_key=True`) |
| `_ts` | Timestamp | Sample timestamp (if `include_timestamp=True`) |
| `value` | Float64 | Sample value |
| `<label>` | Utf8 | Label values (if `label_columns` specified) |
| `_index` | UInt64 | Row index (if `include_row_index=True`) |

## Batching

Data is fetched in batches for memory efficiency:

```python
lf = redis.scan_hashes(
    ...,
    batch_size=500,    # Keys per Arrow batch
    count_hint=100,    # Redis SCAN COUNT hint
)
```

- `batch_size`: Controls memory usage and Arrow batch size
- `count_hint`: Hint to Redis for keys per SCAN iteration

## Parallel Fetching

Speed up large scans by fetching data with multiple parallel workers:

```python
lf = redis.scan_hashes(
    "redis://localhost:6379",
    pattern="user:*",
    schema={"name": pl.Utf8, "age": pl.Int64},
    parallel=4,  # Use 4 parallel workers
)
```

Each batch is split across workers, with results collected in order. This is most effective for:

- Large datasets (thousands of keys)
- High-latency connections
- Batch sizes larger than the worker count

!!! note
    Parallel fetching uses multiple Redis connections. Ensure your Redis server can handle the additional concurrent connections.

## RediSearch: Server-Side Filtering

For even better performance, use RediSearch to filter data in Redis before transfer:

```python
from polars_redis import col

# Only matching documents are transferred
df = redis.search_hashes(
    "redis://localhost:6379",
    index="users_idx",
    query=(col("age") > 30) & (col("status") == "active"),
    schema={"name": pl.Utf8, "age": pl.Int64, "status": pl.Utf8},
).collect()
```

See the [RediSearch Guide](redisearch.md) for details on `search_hashes()`, `aggregate_hashes()`, and the query builder.
