# polars-redis

Query Redis like a database. Transform with [Polars](https://pola.rs/). Write back without ETL.

!!! tip "New to Polars?"
    [Polars](https://pola.rs/) is a lightning-fast DataFrame library for Python and Rust, designed for performance and ease of use. Check out the [Polars User Guide](https://docs.pola.rs/) to get started.

polars-redis brings Redis into your Polars analytics workflows as a first-class data source - scan hashes, JSON, strings, sets, lists, and sorted sets directly into LazyFrames with projection pushdown and batched iteration.

## What is polars-redis?

polars-redis makes Redis just another connector alongside Parquet, CSV, and databases:

```python
import polars as pl
import polars_redis as redis

url = "redis://localhost:6379"

# Enrich Redis data with external source, write back
users = redis.read_hashes(url, "user:*", {"user_id": pl.Utf8, "region": pl.Utf8})
purchases = pl.read_parquet("purchases.parquet")

high_value = (
    users.join(purchases, on="user_id")
    .group_by("user_id")
    .agg(pl.col("amount").sum().alias("lifetime_value"))
    .filter(pl.col("lifetime_value") > 1000)
)

redis.write_hashes(high_value, url, key_prefix="whale:")
```

## When to Use What

| Your data | Use | Why |
|-----------|-----|-----|
| User profiles, configs | `scan_hashes()` | Field-level access, projection pushdown |
| Nested documents | `scan_json()` | Full document structure |
| Counters, flags, caches | `scan_strings()` | Simple key-value |
| Tags, memberships | `scan_sets()` | Unique members |
| Queues, recent items | `scan_lists()` | Ordered elements |
| Leaderboards, rankings | `scan_zsets()` | Score-based ordering |
| Event logs | `scan_streams()` | Timestamped entries |
| Metrics | `scan_timeseries()` | Server-side aggregation |

## Features

- **Scan Redis data** as Polars LazyFrames
    - Hashes, JSON documents, strings, sets, lists, sorted sets, streams, and time series
    - Projection pushdown (only fetch requested fields)
    - Batched iteration for memory efficiency
    - Parallel fetching for improved throughput
- **DataFrame caching** with automatic chunking
    - Cache DataFrames in Redis with Arrow IPC or Parquet format
    - Built-in compression (lz4, zstd, gzip, snappy)
    - Auto-chunking for large DataFrames (no 512MB limit)
    - `@cache` decorator for function memoization
    - TTL support for automatic expiration
- **Real-time streaming** from Pub/Sub and Streams
    - `collect_pubsub()` - Collect messages into DataFrames
    - `read_stream()` - Consumer group support with acknowledgment
    - Batch iterators for continuous processing
- **RediSearch integration** for predicate pushdown
    - `search_hashes()` - Server-side filtering with FT.SEARCH
    - `aggregate_hashes()` - Server-side aggregation with FT.AGGREGATE
    - Polars-like query builder: `col("age") > 30`
- **Write DataFrames** to Redis
    - Hashes, JSON documents, strings, sets, lists, and sorted sets
    - Write modes: fail, replace, append
    - Optional TTL support
- **Schema inference** from existing Redis data
- **Metadata columns** for keys, TTL, and row indices

## Supported Redis Types

| Redis Type | Scan | Write | Notes |
|------------|------|-------|-------|
| Hash | Yes | Yes | Field-level projection pushdown |
| JSON | Yes | Yes | Requires RedisJSON module |
| String | Yes | Yes | Configurable value type |
| Set | Yes | Yes | One row per member |
| List | Yes | Yes | One row per element, optional position |
| Sorted Set | Yes | Yes | One row per member with score |
| Stream | Yes | No | One row per entry with timestamp |
| TimeSeries | Yes | No | Server-side aggregation support |

## Quick Start

```python
import polars as pl
import polars_redis as redis

url = "redis://localhost:6379"

# Scan with schema
lf = redis.scan_hashes(
    url,
    pattern="user:*",
    schema={"name": pl.Utf8, "age": pl.Int64, "active": pl.Boolean},
)

# Filter and collect (projection pushdown applies)
active_users = lf.filter(pl.col("active")).select(["name", "age"]).collect()

# Write with TTL (1 hour)
redis.write_hashes(active_users, url, key_prefix="cache:user:", ttl=3600)
```

## DataFrame Caching

Cache expensive computations with a single decorator - no manual serialization needed:

```python
import polars_redis as redis

@redis.cache(url="redis://localhost:6379", ttl=3600)
def expensive_query(start_date: str, end_date: str) -> pl.DataFrame:
    # Complex computation...
    return df

# First call: computes and caches
result = expensive_query("2024-01-01", "2024-12-31")

# Second call: instant cache hit
result = expensive_query("2024-01-01", "2024-12-31")
```

Or cache directly with full control:

```python
# Cache with compression and TTL
redis.cache_dataframe(df, url, "result", compression="zstd", ttl=3600)

# Retrieve later
df = redis.get_cached_dataframe(url, "result")
```

**Why polars-redis for caching?**

- **No boilerplate** - One line vs 10+ lines of manual PyArrow/pickle serialization
- **Auto-chunking** - Handles DataFrames of any size (bypasses Redis 512MB limit)
- **Built-in compression** - lz4, zstd, gzip, snappy with configurable levels
- **Cache metadata** - `cache_info()`, `cache_ttl()`, `cache_exists()`
- **Native Polars** - No pandas conversion overhead

## Requirements

- Python 3.9+
- Redis 7.0+ (RedisJSON module for JSON support)

## License

MIT or Apache-2.0
