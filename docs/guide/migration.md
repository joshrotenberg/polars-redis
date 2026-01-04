# Migration Guide

This guide covers breaking changes and new features when upgrading polars-redis.

## v0.2.0 - Options-Based API

Version 0.2.0 introduces a new options-based API following the polars-io patterns. The existing keyword argument API remains fully supported for backward compatibility.

### New Features

#### Options Classes

New dataclass-based configuration options are available:

```python
from polars_redis import (
    HashScanOptions,
    JsonScanOptions,
    StringScanOptions,
    SearchOptions,
)

# Configure with constructor
opts = HashScanOptions(
    pattern="user:*",
    batch_size=500,
    include_ttl=True,
)

# Or use builder pattern
opts = (
    HashScanOptions()
    .with_pattern("user:*")
    .with_batch_size(500)
    .with_ttl(True)
)
```

#### Using Options with Scan Functions

All scan functions now accept an optional `options` parameter:

=== "New (Options)"

    ```python
    opts = HashScanOptions(
        pattern="user:*",
        batch_size=500,
        include_ttl=True,
    )
    
    lf = scan_hashes(
        "redis://localhost:6379",
        schema={"name": pl.Utf8, "age": pl.Int64},
        options=opts,
    )
    ```

=== "Original (Kwargs)"

    ```python
    lf = scan_hashes(
        "redis://localhost:6379",
        pattern="user:*",
        schema={"name": pl.Utf8, "age": pl.Int64},
        batch_size=500,
        include_ttl=True,
    )
    ```

Both approaches work identically. Use whichever style you prefer.

#### Environment Variable Defaults

Configure defaults via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `POLARS_REDIS_BATCH_SIZE` | 1000 | Keys per batch |
| `POLARS_REDIS_COUNT_HINT` | 100 | Redis SCAN COUNT hint |
| `POLARS_REDIS_TIMEOUT_MS` | 30000 | Operation timeout (ms) |

```python
from polars_redis import (
    get_default_batch_size,
    get_default_count_hint,
    get_default_timeout_ms,
)

# Check current defaults
print(f"Batch size: {get_default_batch_size()}")
print(f"Count hint: {get_default_count_hint()}")
print(f"Timeout: {get_default_timeout_ms()}ms")
```

#### Schema Overwrite

New functions allow inferring schema with type overrides:

```python
from polars_redis import infer_hash_schema_with_overwrite
import polars as pl

# Infer most fields, but force specific types
schema = infer_hash_schema_with_overwrite(
    "redis://localhost:6379",
    pattern="user:*",
    schema_overwrite={
        "age": pl.Int64,           # Force to Int64 (might infer as Utf8)
        "created_at": pl.Datetime,  # Force timestamp field
    },
)

# Use the merged schema
df = read_hashes(
    "redis://localhost:6379",
    pattern="user:*",
    schema=schema,
)
```

### Available Options Classes

#### HashScanOptions

For scanning Redis hashes:

```python
opts = HashScanOptions(
    pattern="user:*",           # Key pattern
    batch_size=1000,            # Keys per batch
    count_hint=100,             # Redis SCAN COUNT
    n_rows=None,                # Max rows (None = unlimited)
    include_key=True,           # Include _key column
    key_column_name="_key",     # Key column name
    include_ttl=False,          # Include _ttl column
    ttl_column_name="_ttl",     # TTL column name
    include_row_index=False,    # Include row index
    row_index_column_name="_index",
    projection=None,            # Fields to fetch (None = all)
)
```

#### JsonScanOptions

For scanning RedisJSON documents:

```python
opts = JsonScanOptions(
    pattern="doc:*",
    batch_size=1000,
    count_hint=100,
    include_key=True,
    include_ttl=False,
    path=None,                  # JSON path (None = root "$")
    projection=None,
)
```

#### StringScanOptions

For scanning Redis strings:

```python
opts = StringScanOptions(
    pattern="cache:*",
    batch_size=1000,
    count_hint=100,
    include_key=True,
    key_column_name="_key",
    value_column_name="value",  # Value column name
)
```

#### SearchOptions

For RediSearch queries:

```python
opts = SearchOptions(
    index="users_idx",          # RediSearch index name
    query="@age:[30 +inf]",     # RediSearch query
    batch_size=1000,
    include_key=True,
    include_ttl=False,
    sort_by=None,               # Sort field
    sort_ascending=True,        # Sort direction
    projection=None,
)
```

### Builder Methods

All options classes support builder-style configuration:

```python
opts = (
    HashScanOptions()
    .with_pattern("user:*")
    .with_batch_size(500)
    .with_count_hint(200)
    .with_n_rows(10000)
    .with_key(include=True, name="redis_key")
    .with_ttl(include=True, name="expires_in")
    .with_row_index(include=True, name="row_num", offset=1)
    .with_projection(["name", "email"])
)
```

### Backward Compatibility

The original keyword argument API is fully supported and will continue to work:

```python
# This still works exactly as before
lf = scan_hashes(
    "redis://localhost:6379",
    pattern="user:*",
    schema={"name": pl.Utf8, "age": pl.Int64},
    batch_size=500,
    include_ttl=True,
)
```

When both `options` and keyword arguments are provided, the options object takes precedence.

### Deprecations

None. All existing APIs remain supported.

### Breaking Changes

None. This release is fully backward compatible.
