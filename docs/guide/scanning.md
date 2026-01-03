# Scanning Data

polars-redis provides three scan functions for different Redis data types.

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
