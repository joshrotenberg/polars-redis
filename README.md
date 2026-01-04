# polars-redis

Query Redis like a database. Transform with Polars. Write back without ETL.

[![CI](https://github.com/joshrotenberg/polars-redis/actions/workflows/ci.yml/badge.svg)](https://github.com/joshrotenberg/polars-redis/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/polars-redis.svg)](https://pypi.org/project/polars-redis/)
[![Crates.io](https://img.shields.io/crates/v/polars-redis.svg)](https://crates.io/crates/polars-redis)
[![Downloads](https://img.shields.io/pypi/dm/polars-redis.svg)](https://pypi.org/project/polars-redis/)
[![Python](https://img.shields.io/pypi/pyversions/polars-redis.svg)](https://pypi.org/project/polars-redis/)
[![Docs](https://img.shields.io/badge/docs-mkdocs-blue.svg)](https://joshrotenberg.github.io/polars-redis/)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)

## What is polars-redis?

polars-redis makes Redis a first-class data source in your Polars workflows. Query, transform, and write back - Redis becomes just another connector alongside Parquet, CSV, and databases.

```python
import polars as pl
import polars_redis as redis

# Redis is just another source
users = redis.read_hashes(url, "user:*", schema)
orders = pl.read_parquet("s3://bucket/orders.parquet")

# Full Polars transformation power
result = (
    users.join(orders, on="user_id")
    .group_by("region")
    .agg(pl.col("amount").sum())
)

# Write back to Redis
redis.write_hashes(result, url, key_prefix="region_stats:")
```

## Installation

```bash
pip install polars-redis
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

# Write with TTL
redis.write_hashes(active_users, url, key_prefix="cache:user:", ttl=3600)
```

## Features

**Read:**
- `scan_hashes()` / `read_hashes()` - Redis hashes
- `scan_json()` / `read_json()` - RedisJSON documents  
- `scan_strings()` / `read_strings()` - Redis strings
- `scan_sets()` / `read_sets()` - Redis sets
- `scan_lists()` / `read_lists()` - Redis lists
- `scan_zsets()` / `read_zsets()` - Redis sorted sets
- Projection pushdown (fetch only requested fields)
- Schema inference (`infer_hash_schema()`, `infer_json_schema()`)
- Metadata columns (key, TTL, row index)

**Write:**
- `write_hashes()`, `write_json()`, `write_strings()`
- `write_sets()`, `write_lists()`, `write_zsets()`
- TTL support
- Key prefix
- Write modes: fail, replace, append

## Supported Types

| Polars | Redis |
|--------|-------|
| `Utf8` | string |
| `Int64` | parsed int |
| `Float64` | parsed float |
| `Boolean` | true/false, 1/0, yes/no |
| `Date` | YYYY-MM-DD or epoch days |
| `Datetime` | ISO 8601 or Unix timestamp |

## Requirements

- Python 3.9+
- Redis 7.0+ (RedisJSON module for JSON support)

## Documentation

Full documentation at [joshrotenberg.github.io/polars-redis](https://joshrotenberg.github.io/polars-redis/)

## License

MIT or Apache-2.0
