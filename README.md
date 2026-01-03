# polars-redis

A [Polars](https://pola.rs/) IO plugin for Redis. Scan Redis data structures as LazyFrames with projection pushdown, or write DataFrames back to Redis.

[![CI](https://github.com/joshrotenberg/polars-redis/actions/workflows/ci.yml/badge.svg)](https://github.com/joshrotenberg/polars-redis/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)

## Installation

```bash
pip install polars-redis
```

## Quick Start

```python
import polars as pl
import polars_redis as redis

# Scan hashes
lf = redis.scan_hashes(
    "redis://localhost:6379",
    pattern="user:*",
    schema={"name": pl.Utf8, "age": pl.Int64},
)
df = lf.filter(pl.col("age") > 30).collect()

# Write back
redis.write_hashes(df, "redis://localhost:6379", key_prefix="user:")
```

## Features

**Read:**
- `scan_hashes()` / `read_hashes()` - Redis hashes
- `scan_json()` / `read_json()` - RedisJSON documents
- `scan_strings()` / `read_strings()` - Redis strings
- Projection pushdown (HMGET vs HGETALL)
- Schema inference (`infer_hash_schema()`, `infer_json_schema()`)
- TTL and row index columns

**Write:**
- `write_hashes()`, `write_json()`, `write_strings()`
- TTL support
- Key prefix
- Write modes: fail, replace, append
- Auto-generate keys from row index

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

## License

MIT or Apache-2.0
