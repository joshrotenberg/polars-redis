# Configuration

This guide covers performance tuning and configuration options.

## Connection URL

polars-redis uses standard Redis URLs:

```python
# Local Redis
url = "redis://localhost:6379"

# With password
url = "redis://:password@localhost:6379"

# With username and password
url = "redis://user:password@localhost:6379"

# Specific database
url = "redis://localhost:6379/1"

# TLS
url = "rediss://localhost:6379"
```

## Batch Size

The `batch_size` parameter controls how many keys are processed per Arrow batch:

```python
lf = redis.scan_hashes(
    url,
    pattern="user:*",
    schema=schema,
    batch_size=1000,  # default
)
```

### Tuning Guidelines

| Batch Size | Memory | Latency | Use Case |
|------------|--------|---------|----------|
| 100-500 | Low | Higher | Memory-constrained, streaming |
| 1000 | Medium | Balanced | General purpose (default) |
| 5000-10000 | Higher | Lower | Large datasets, fast networks |

## Count Hint

The `count_hint` parameter hints to Redis how many keys to return per SCAN iteration:

```python
lf = redis.scan_hashes(
    url,
    pattern="user:*",
    schema=schema,
    count_hint=100,  # default
)
```

!!! note
    This is a hint, not a guarantee. Redis may return more or fewer keys.

### Tuning Guidelines

- **Low values (10-50)**: More SCAN iterations, lower memory per iteration
- **High values (500-1000)**: Fewer iterations, higher throughput

## Write Pipelining

Write operations automatically use Redis pipelining with batches of 1000 keys:

```python
# Writes are pipelined automatically
redis.write_hashes(df, url)
```

This reduces network round-trips significantly for large writes.

## Memory Considerations

### Scanning Large Datasets

For very large datasets, use streaming with smaller batches:

```python
lf = redis.scan_hashes(
    url,
    pattern="user:*",
    schema=schema,
    batch_size=500,  # Smaller batches
)

# Process in chunks
for batch_df in lf.collect_iter():
    process(batch_df)
```

### Projection Pushdown

Always select only needed columns to reduce memory:

```python
# Good: Only fetches 'name' and 'age' from Redis
df = lf.select(["name", "age"]).collect()

# Less efficient: Fetches all fields, then discards
df = lf.collect().select(["name", "age"])
```

## Error Handling

### Connection Errors

```python
try:
    lf = redis.scan_hashes(url, pattern="*", schema=schema)
    df = lf.collect()
except Exception as e:
    print(f"Redis error: {e}")
```

### Missing Fields

Missing hash fields become `null`:

```python
schema = {"name": pl.Utf8, "optional_field": pl.Utf8}
df = redis.read_hashes(url, pattern="user:*", schema=schema)
# optional_field will be null for hashes that don't have it
```

### Type Conversion Errors

Invalid values for a type become `null`:

```python
schema = {"age": pl.Int64}
# If a hash has age="not a number", it becomes null
```

## Environment Variables

For Rust examples, the connection URL can be overridden:

```bash
export REDIS_URL="redis://custom-host:6379"
cargo run --example scan_hashes
```
