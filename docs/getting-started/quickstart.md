# Quick Start

This guide walks through the basic operations with polars-redis.

## Setup Sample Data

First, let's create some sample data in Redis:

```python
import redis

r = redis.Redis()

# Create some user hashes
r.hset("user:1", mapping={"name": "Alice", "age": "30", "city": "NYC"})
r.hset("user:2", mapping={"name": "Bob", "age": "25", "city": "LA"})
r.hset("user:3", mapping={"name": "Carol", "age": "35", "city": "NYC"})
```

## Scanning Hashes

Scan Redis hashes into a Polars LazyFrame:

```python
import polars as pl
import polars_redis as redis

# Define schema
schema = {"name": pl.Utf8, "age": pl.Int64, "city": pl.Utf8}

# Scan hashes (returns LazyFrame)
lf = redis.scan_hashes(
    "redis://localhost:6379",
    pattern="user:*",
    schema=schema,
)

# Collect all data
df = lf.collect()
print(df)
```

Output:
```
shape: (3, 4)
┌─────────┬───────┬─────┬──────┐
│ _key    ┆ name  ┆ age ┆ city │
│ ---     ┆ ---   ┆ --- ┆ ---  │
│ str     ┆ str   ┆ i64 ┆ str  │
╞═════════╪═══════╪═════╪══════╡
│ user:1  ┆ Alice ┆ 30  ┆ NYC  │
│ user:2  ┆ Bob   ┆ 25  ┆ LA   │
│ user:3  ┆ Carol ┆ 35  ┆ NYC  │
└─────────┴───────┴─────┴──────┘
```

## Filtering with LazyFrame

Since `scan_hashes` returns a LazyFrame, you can chain Polars operations:

```python
# Filter and select (lazy - nothing executed yet)
result = (
    lf
    .filter(pl.col("city") == "NYC")
    .select(["name", "age"])
    .collect()  # Execute
)
print(result)
```

Output:
```
shape: (2, 2)
┌───────┬─────┐
│ name  ┆ age │
│ ---   ┆ --- │
│ str   ┆ i64 │
╞═══════╪═════╡
│ Alice ┆ 30  │
│ Carol ┆ 35  │
└───────┴─────┘
```

## Writing Data Back

Write a DataFrame to Redis:

```python
# Create new data
df = pl.DataFrame({
    "name": ["Dave", "Eve"],
    "age": [28, 32],
    "city": ["Chicago", "Boston"],
})

# Write as hashes with auto-generated keys
count = redis.write_hashes(
    df,
    "redis://localhost:6379",
    key_column=None,  # Auto-generate keys
    key_prefix="user:",
)
print(f"Wrote {count} hashes")
```

## Schema Inference

Don't know the schema? Infer it from existing data:

```python
# Infer schema from Redis
schema = redis.infer_hash_schema(
    "redis://localhost:6379",
    pattern="user:*",
    sample_size=100,
)
print(schema)
# {'name': Utf8, 'age': Int64, 'city': Utf8}

# Use inferred schema
lf = redis.scan_hashes(
    "redis://localhost:6379",
    pattern="user:*",
    schema=schema,
)
```

## Next Steps

- [Scanning Data](../guide/scanning.md) - Deep dive into scan operations
- [Writing Data](../guide/writing.md) - Write modes and options
- [Schema Inference](../guide/schema-inference.md) - Automatic type detection
- [Configuration](../guide/configuration.md) - Tuning batch sizes and performance
