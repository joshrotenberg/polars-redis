# Writing Data

polars-redis provides write functions for six Redis data types: hashes, JSON, strings, sets, lists, and sorted sets.

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

## Writing Sets

`write_sets` writes DataFrame rows as Redis sets:

```python
df = pl.DataFrame({
    "_key": ["tags:post:1", "tags:post:2"],
    "member": ["python", "redis"],
})

count = redis.write_sets(df, "redis://localhost:6379")
```

Multiple members per key can be written by having multiple rows with the same key:

```python
df = pl.DataFrame({
    "_key": ["tags:post:1", "tags:post:1", "tags:post:1"],
    "member": ["python", "redis", "polars"],
})

# Creates one set with 3 members
count = redis.write_sets(df, url)
```

### Set-specific Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `member_column` | str | `"member"` | Column with member values |

## Writing Lists

`write_lists` writes DataFrame rows as Redis lists:

```python
df = pl.DataFrame({
    "_key": ["queue:tasks", "queue:tasks", "queue:tasks"],
    "element": ["task1", "task2", "task3"],
})

count = redis.write_lists(df, "redis://localhost:6379")
```

Elements are written in DataFrame row order using `RPUSH`.

### List-specific Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `element_column` | str | `"element"` | Column with element values |

## Writing Sorted Sets

`write_zsets` writes DataFrame rows as Redis sorted sets:

```python
df = pl.DataFrame({
    "_key": ["leaderboard:game1", "leaderboard:game1", "leaderboard:game1"],
    "member": ["alice", "bob", "carol"],
    "score": [100.0, 85.5, 92.0],
})

count = redis.write_zsets(df, "redis://localhost:6379")
```

### Sorted Set-specific Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `member_column` | str | `"member"` | Column with member values |
| `score_column` | str | `"score"` | Column with score values |

## Batch Pipelining

Write operations are automatically pipelined in batches of 1000 keys for performance. This reduces round-trips to Redis.

## Return Value

All write functions return the number of keys successfully written:

```python
count = redis.write_hashes(df, url)
print(f"Wrote {count} of {len(df)} rows")
```

## Detailed Error Handling

For production workflows where you need granular error reporting, use the `_detailed` variants of write functions. These return a `WriteResult` object with per-key success/failure information.

### WriteResult Class

```python
result = redis.write_hashes_detailed(df, "redis://localhost:6379")

# Check counts
result.keys_written   # Number of keys successfully written
result.keys_failed    # Number of keys that failed
result.keys_skipped   # Number of keys skipped (when if_exists="fail")

# Access key lists
result.succeeded_keys  # List of successfully written keys
result.failed_keys     # List of keys that failed

# Get error details
result.errors          # Dict mapping failed keys to error messages

# Check for complete success
if result.is_complete_success():
    print("All keys written successfully")
```

### Basic Usage

```python
import polars as pl
import polars_redis as redis

df = pl.DataFrame({
    "_key": ["user:1", "user:2", "user:3"],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [30, 25, 35],
})

result = redis.write_hashes_detailed(df, "redis://localhost:6379")

print(f"Wrote {result.keys_written} keys")
print(f"Failed: {result.keys_failed}")
print(f"Skipped: {result.keys_skipped}")

if not result.is_complete_success():
    for key, error in result.errors.items():
        print(f"  {key}: {error}")
```

### Retry Pattern

The detailed result enables retry logic for failed keys:

```python
import polars as pl
import polars_redis as redis

def write_with_retry(df, url, max_retries=3):
    """Write with automatic retry for failed keys."""
    remaining = df
    total_written = 0
    
    for attempt in range(max_retries):
        result = redis.write_hashes_detailed(remaining, url)
        total_written += result.keys_written
        
        if result.is_complete_success():
            break
            
        if result.keys_failed > 0:
            print(f"Attempt {attempt + 1}: {result.keys_failed} failures")
            # Filter to only failed keys for retry
            remaining = remaining.filter(
                pl.col("_key").is_in(result.failed_keys)
            )
    
    return total_written

# Usage
df = pl.DataFrame({
    "_key": ["user:1", "user:2", "user:3"],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [30, 25, 35],
})

written = write_with_retry(df, "redis://localhost:6379")
print(f"Successfully wrote {written} keys")
```

### When to Use Detailed Functions

Use `write_hashes_detailed()` when you need to:

- **Implement retry logic**: Retry only the keys that failed
- **Log specific failures**: Record which keys failed and why
- **Partial success handling**: Accept partial writes in non-critical workflows
- **Debugging**: Identify problematic keys during development

For simple scripts where you just need a count, the regular `write_hashes()` is sufficient.
