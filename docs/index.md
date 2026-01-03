# polars-redis

Query Redis like a database. Transform with Polars. Write back without ETL.

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

## Requirements

- Python 3.9+
- Redis 7.0+ (RedisJSON module for JSON support)

## License

MIT or Apache-2.0
