# Writing Data

polars-redis provides three write functions for different Redis data types.

## Writing Hashes

`write_hashes` writes DataFrame rows as Redis hashes:

```python
import polars as pl
import polars_redis as redis

df = pl.DataFrame({
    "_key": ["user:1", "user:2"],
    "name": ["Alice", "Bob"],
    "age": [30, 25],
})

count = redis.write_hashes(df, "redis://localhost:6379")
print(f"Wrote {count} hashes")
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | DataFrame | required | Data to write |
| `url` | str | required | Redis connection URL |
| `key_column` | str \| None | `"_key"` | Column with Redis keys |
| `ttl` | int \| None | `None` | TTL in seconds |
| `key_prefix` | str | `""` | Prefix for all keys |
| `if_exists` | str | `"replace"` | How to handle existing keys |

## Writing JSON

`write_json` writes DataFrame rows as RedisJSON documents:

```python
df = pl.DataFrame({
    "_key": ["doc:1", "doc:2"],
    "title": ["Hello", "World"],
    "views": [100, 200],
})

count = redis.write_json(df, "redis://localhost:6379")
```

Parameters are identical to `write_hashes`.

## Writing Strings

`write_strings` writes DataFrame rows as Redis strings:

```python
df = pl.DataFrame({
    "_key": ["counter:1", "counter:2"],
    "value": ["100", "200"],
})

count = redis.write_strings(
    df,
    "redis://localhost:6379",
    value_column="value",
)
```

### String-specific Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `value_column` | str | `"value"` | Column with values to write |

## Key Generation

### From Column

Use an existing column as keys:

```python
df = pl.DataFrame({
    "id": ["user:1", "user:2"],
    "name": ["Alice", "Bob"],
})

redis.write_hashes(df, url, key_column="id")
```

### Auto-generated

Generate keys from row indices:

```python
df = pl.DataFrame({
    "name": ["Alice", "Bob", "Carol"],
    "age": [30, 25, 35],
})

# Keys will be "user:0", "user:1", "user:2"
redis.write_hashes(
    df,
    url,
    key_column=None,
    key_prefix="user:",
)
```

### With Prefix

Add a prefix to existing keys:

```python
df = pl.DataFrame({
    "_key": ["1", "2"],
    "name": ["Alice", "Bob"],
})

# Keys will be "user:1", "user:2"
redis.write_hashes(df, url, key_prefix="user:")
```

## Write Modes

The `if_exists` parameter controls behavior for existing keys:

### Replace (default)

Delete existing keys before writing:

```python
redis.write_hashes(df, url, if_exists="replace")
```

This ensures a clean write - existing hash fields not in the DataFrame are removed.

### Fail

Skip keys that already exist:

```python
redis.write_hashes(df, url, if_exists="fail")
```

Only new keys are written. Existing keys are left unchanged.

### Append

Merge new fields into existing hashes:

```python
redis.write_hashes(df, url, if_exists="append")
```

Existing fields are overwritten, but fields not in the DataFrame are preserved.

!!! note
    For JSON and strings, `append` behaves the same as `replace` since these types don't have partial updates.

## TTL (Time-to-Live)

Set expiration on written keys:

```python
# Expire in 1 hour (3600 seconds)
redis.write_hashes(df, url, ttl=3600)

# Expire in 1 day
redis.write_hashes(df, url, ttl=86400)

# No expiration (default)
redis.write_hashes(df, url, ttl=None)
```

## Batch Pipelining

Write operations are automatically pipelined in batches of 1000 keys for performance. This reduces round-trips to Redis.

## Return Value

All write functions return the number of keys successfully written:

```python
count = redis.write_hashes(df, url)
print(f"Wrote {count} of {len(df)} rows")
```
