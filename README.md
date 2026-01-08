# polars-redis

**Redis + Polars. No ETL required.**

DataFrames from Redis without N+1 queries. Server-side filtering with RediSearch. Write results back.

[![CI](https://github.com/joshrotenberg/polars-redis/actions/workflows/ci.yml/badge.svg)](https://github.com/joshrotenberg/polars-redis/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/polars-redis.svg)](https://pypi.org/project/polars-redis/)
[![Crates.io](https://img.shields.io/crates/v/polars-redis.svg)](https://crates.io/crates/polars-redis)
[![Downloads](https://img.shields.io/pypi/dm/polars-redis.svg)](https://pypi.org/project/polars-redis/)
[![Python](https://img.shields.io/pypi/pyversions/polars-redis.svg)](https://pypi.org/project/polars-redis/)
[![Docs](https://img.shields.io/badge/docs-mkdocs-blue.svg)](https://joshrotenberg.github.io/polars-redis/)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)

**Best for:** 10K-1M documents where your data already lives in Redis and you want to query it without copying it elsewhere.

**What this is NOT:** A Redis client replacement. Use polars-redis alongside redis-py when you need DataFrame operations on your Redis data.

## Why polars-redis?

**The old way:**
```python
# Fetch everything, process in Python
keys = redis.scan_iter("user:*")
users = []
for key in keys:                        # N+1 queries
    data = redis.hgetall(key)
    users.append(data)

df = pd.DataFrame(users)
df["age"] = df["age"].astype(int)       # Manual type coercion
active = df[df["status"] == "active"]   # Filter client-side
```

**With polars-redis:**
```python
import polars_redis as redis
from polars_redis import col

# One call, proper types, server-side filtering (with RediSearch)
df = redis.search_hashes(
    url,
    index="users_idx",
    query=col("status") == "active",
    schema={"name": pl.Utf8, "age": pl.Int64},
).collect()
```

- **No N+1 queries** - Batched async operations
- **Automatic types** - Schema or inference
- **Predicate pushdown** - Filter in Redis with RediSearch (optional)
- **Native Polars** - LazyFrames, Arrow, zero-copy

## Quick Start

```bash
pip install polars-redis
docker run -d -p 6379:6379 redis/redis-stack:latest
```

```python
import polars as pl
import polars_redis as redis

url = "redis://localhost:6379"

# Write a DataFrame to Redis
df = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Carol"],
    "score": [95.5, 87.3, 92.1],
})
redis.write_hashes(df, url, key_column="id", key_prefix="user:")

# Scan it back
result = redis.scan_hashes(
    url,
    pattern="user:*",
    schema={"name": pl.Utf8, "score": pl.Float64},
).collect()

print(result)
# shape: (3, 3)
# +---------+-------+-------+
# | _key    | name  | score |
# +---------+-------+-------+
# | user:1  | Alice | 95.5  |
# | user:2  | Bob   | 87.3  |
# | user:3  | Carol | 92.1  |
# +---------+-------+-------+
```

## The Golden Path

Redis is just another data source alongside Parquet, CSV, and databases:

```python
import polars as pl
import polars_redis as redis
from polars_redis import col

url = "redis://localhost:6379"

# Join Redis data with external sources
users = redis.scan_hashes(url, "user:*", {"user_id": pl.Utf8, "region": pl.Utf8})
orders = pl.read_parquet("s3://bucket/orders.parquet")

# Full Polars transformation power
high_value = (
    users.join(orders, on="user_id")
    .group_by("region")
    .agg(pl.col("amount").sum().alias("revenue"))
    .filter(pl.col("revenue") > 10000)
)

# Write results back to Redis
redis.write_hashes(high_value.collect(), url, key_prefix="region_stats:")
```

## RediSearch: Server-Side Filtering (Optional)

RediSearch enables filtering and aggregation in Redis - only results are transferred. **This is optional** - `scan_hashes()` works without any index.

```python
from polars_redis import col, Index, TextField, NumericField, TagField

# Create an index (one-time setup)
Index(
    name="users_idx",
    prefix="user:",
    schema=[
        TextField("name"),
        NumericField("age", sortable=True),
        TagField("status"),
    ]
).create(url)

# Query with Polars-like syntax
df = redis.search_hashes(
    url,
    index="users_idx",
    query=(col("age") > 30) & (col("status") == "active"),
    schema={"name": pl.Utf8, "age": pl.Int64},
).collect()

# Server-side aggregation
stats = redis.aggregate_hashes(
    url,
    index="users_idx",
    query="*",
    group_by=["@status"],
    reduce=[("COUNT", [], "count"), ("AVG", ["@age"], "avg_age")],
)
```

## DataFrame Caching

Cache expensive computations with one decorator:

```python
@redis.cache(url=url, ttl=3600, compression="zstd")
def expensive_query(start_date, end_date):
    return (
        pl.scan_parquet("huge_dataset.parquet")
        .filter(pl.col("date").is_between(start_date, end_date))
        .group_by("category")
        .agg(pl.sum("revenue"))
        .collect()
    )

# First call: computes and caches
result = expensive_query("2024-01-01", "2024-12-31")

# Second call: instant cache hit
result = expensive_query("2024-01-01", "2024-12-31")
```

## Features

**DataFrame I/O:**
- Scan hashes, JSON, strings, sets, lists, sorted sets, streams, time series
- Write DataFrames back to Redis
- Schema inference
- Projection pushdown
- Parallel fetching

**RediSearch (optional):**
- Server-side filtering with `search_hashes()`
- Server-side aggregation with `aggregate_hashes()`
- Polars-like query builder
- Smart scan (auto-detects indexes)

**Caching:**
- `@cache` decorator for function memoization
- Auto-chunking (no 512MB limit)
- Compression (lz4, zstd, gzip)

**Client Operations:**
- Geospatial queries (`geo_radius`, `geo_dist`)
- Key management (`set_ttl`, `delete_keys`)
- Pipelines and transactions
- Pub/Sub message collection

## Supported Types

| Your Data | Use | Notes |
|-----------|-----|-------|
| User profiles, configs | `scan_hashes()` | Field-level projection pushdown |
| Nested documents | `scan_json()` | JSONPath extraction |
| Key-value pairs | `scan_strings()` | Simple values |
| Tags, memberships | `scan_sets()` | Unique members |
| Queues, recent items | `scan_lists()` | Ordered elements |
| Leaderboards | `scan_zsets()` | Score-based ordering |
| Event logs | `scan_streams()` | Consumer groups |
| Metrics | `scan_timeseries()` | Server-side aggregation |

## Requirements

- Python 3.9+ (or Rust)
- Redis 7.0+
- Redis Stack for RediSearch/JSON features (optional for basic scanning)

## Documentation

- [Full Documentation](https://joshrotenberg.github.io/polars-redis/)
- [Product Catalog Example](https://joshrotenberg.github.io/polars-redis/examples/product-catalog/)
- [API Reference](https://joshrotenberg.github.io/polars-redis/api/python/)

> **New to Polars?** [Polars](https://pola.rs/) is a lightning-fast DataFrame library for Python and Rust. Check out the [Polars User Guide](https://docs.pola.rs/) to get started.

## License

MIT or Apache-2.0
