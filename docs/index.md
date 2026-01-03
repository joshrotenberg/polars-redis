# polars-redis

A [Polars](https://pola.rs/) IO plugin for Redis.

Scan Redis data structures as LazyFrames with projection pushdown, or write DataFrames back to Redis.

## Features

- **Scan Redis data** as Polars LazyFrames
    - Hashes, JSON documents, strings, sets, lists, and sorted sets
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

## Quick Example

```python
import polars as pl
import polars_redis as redis

# Scan hashes matching a pattern
lf = redis.scan_hashes(
    "redis://localhost:6379",
    pattern="user:*",
    schema={"name": pl.Utf8, "age": pl.Int64},
)

# Filter and collect (lazy evaluation)
df = lf.filter(pl.col("age") > 30).collect()

# Write back to Redis
redis.write_hashes(df, "redis://localhost:6379", key_prefix="user:")
```

## Requirements

- Python 3.9+
- Redis 7.0+ (RedisJSON module for JSON support)

## License

MIT or Apache-2.0
