# polars-redis Write API Design Proposal

## Overview

This document proposes the write API for polars-redis, enabling DataFrame-to-Redis operations for hashes and JSON documents.

## Core Concepts

### Key Generation Strategies

The primary challenge with writing DataFrames to Redis is determining the key for each row. We propose three strategies:

1. **Column-based keys**: Use a DataFrame column as the Redis key
2. **Pattern-based keys**: Generate keys using a pattern with row index (e.g., `user:{idx}`)
3. **UUID keys**: Generate unique keys with a prefix (e.g., `user:{uuid}`)

### Write Modes

- **insert**: Only write if key doesn't exist (NX semantics)
- **upsert**: Overwrite if key exists (default)
- **update**: Only write if key exists (XX semantics)

## Python API

### write_hashes

```python
def write_hashes(
    df: pl.DataFrame,
    url: str,
    *,
    # Key generation (one of these is required)
    key_column: str | None = None,        # Use this column as the key
    key_pattern: str | None = None,       # Pattern with {idx} placeholder
    key_prefix: str | None = None,        # Generate UUID keys with prefix

    # Field mapping
    columns: list[str] | None = None,     # Columns to write (None = all except key_column)

    # Write behavior
    mode: Literal["insert", "upsert", "update"] = "upsert",

    # Performance
    batch_size: int = 1000,               # Keys per pipeline
    
    # Options
    ttl: int | None = None,               # TTL in seconds (None = no expiry)
) -> WriteResult:
    """Write a DataFrame to Redis hashes.
    
    Args:
        df: The DataFrame to write.
        url: Redis connection URL.
        key_column: Column to use as Redis keys (removes from fields).
        key_pattern: Pattern for key generation, e.g., "user:{idx}".
        key_prefix: Prefix for UUID key generation, e.g., "user:".
        columns: Columns to write as hash fields (default: all).
        mode: Write mode - insert, upsert, or update.
        batch_size: Number of keys per Redis pipeline.
        ttl: Optional TTL in seconds.
    
    Returns:
        WriteResult with statistics.
    
    Examples:
        # Use existing column as key
        >>> write_hashes(df, url, key_column="_key")
        
        # Generate keys from pattern
        >>> write_hashes(df, url, key_pattern="user:{idx}")
        
        # Only write specific columns
        >>> write_hashes(df, url, key_column="id", columns=["name", "email"])
        
        # Insert only (skip existing keys)
        >>> write_hashes(df, url, key_column="id", mode="insert")
        
        # With TTL
        >>> write_hashes(df, url, key_column="id", ttl=3600)
    """
```

### write_json

```python
def write_json(
    df: pl.DataFrame,
    url: str,
    *,
    # Key generation (same as write_hashes)
    key_column: str | None = None,
    key_pattern: str | None = None,
    key_prefix: str | None = None,

    # Field mapping
    columns: list[str] | None = None,

    # JSON options
    path: str = "$",                      # JSONPath to write to

    # Write behavior
    mode: Literal["insert", "upsert", "update"] = "upsert",

    # Performance
    batch_size: int = 1000,
    
    # Options
    ttl: int | None = None,
) -> WriteResult:
    """Write a DataFrame to Redis JSON documents.
    
    Args:
        df: The DataFrame to write.
        url: Redis connection URL.
        key_column: Column to use as Redis keys.
        key_pattern: Pattern for key generation.
        key_prefix: Prefix for UUID key generation.
        columns: Columns to include in JSON (default: all).
        path: JSONPath to write to (default: "$" for root).
        mode: Write mode.
        batch_size: Number of keys per Redis pipeline.
        ttl: Optional TTL in seconds.
    
    Examples:
        # Write full documents
        >>> write_json(df, url, key_column="id")
        
        # Update nested path
        >>> write_json(df, url, key_column="id", path="$.metadata")
    """
```

### WriteResult

```python
@dataclass
class WriteResult:
    """Result of a write operation."""
    rows_written: int           # Number of rows successfully written
    rows_skipped: int           # Rows skipped (mode=insert and key exists)
    rows_failed: int            # Rows that failed (mode=update and key missing)
    elapsed_ms: float           # Total time in milliseconds
    keys: list[str] | None      # Generated keys (if key_pattern/key_prefix used)
```

## Rust API

### HashWriter

```rust
pub struct HashWriter {
    connection: RedisConnection,
    config: WriteConfig,
}

pub struct WriteConfig {
    pub batch_size: usize,
    pub mode: WriteMode,
    pub ttl: Option<Duration>,
}

pub enum WriteMode {
    Insert,   // NX - only if not exists
    Upsert,   // Default - overwrite
    Update,   // XX - only if exists
}

pub enum KeyStrategy {
    Column(String),           // Use column value
    Pattern(String),          // Pattern with {idx}
    Prefix(String),           // UUID with prefix
}

impl HashWriter {
    pub fn new(url: &str, config: WriteConfig) -> Result<Self>;
    
    /// Write a RecordBatch to Redis hashes.
    pub fn write_batch(
        &mut self,
        batch: &RecordBatch,
        key_strategy: &KeyStrategy,
        columns: Option<&[String]>,
    ) -> Result<BatchWriteResult>;
}

pub struct BatchWriteResult {
    pub written: usize,
    pub skipped: usize,
    pub failed: usize,
}
```

### JsonWriter

```rust
pub struct JsonWriter {
    connection: RedisConnection,
    config: WriteConfig,
}

impl JsonWriter {
    pub fn new(url: &str, config: WriteConfig) -> Result<Self>;
    
    pub fn write_batch(
        &mut self,
        batch: &RecordBatch,
        key_strategy: &KeyStrategy,
        columns: Option<&[String]>,
        path: &str,
    ) -> Result<BatchWriteResult>;
}
```

## Implementation Plan

### Phase 1: Core Write Infrastructure
1. Add `WriteConfig` and `WriteMode` types
2. Implement `KeyStrategy` with column, pattern, and UUID support
3. Add pipelining utilities for batch writes

### Phase 2: Hash Writing
1. Implement `HashWriter` in Rust
2. Add Arrow-to-Redis-hash conversion
3. Create `PyHashWriter` Python bindings
4. Implement `write_hashes()` Python function

### Phase 3: JSON Writing
1. Implement `JsonWriter` in Rust
2. Add Arrow-to-JSON conversion
3. Create `PyJsonWriter` Python bindings
4. Implement `write_json()` Python function

### Phase 4: Advanced Features
1. TTL support via EXPIRE pipelining
2. Transaction support (MULTI/EXEC) for atomicity
3. Streaming write for large DataFrames

## Redis Commands Used

### Hashes
- `HSET key field value [field value ...]` - Set multiple fields
- `HSETNX key field value` - Set if not exists (for insert mode)
- `EXPIRE key seconds` - Set TTL

### JSON
- `JSON.SET key path value [NX|XX]` - Set JSON value
- `EXPIRE key seconds` - Set TTL

## Error Handling

- Connection errors: Retry with exponential backoff
- Type conversion errors: Collect and report, continue with other rows
- Mode violations: Track as skipped/failed in WriteResult

## Performance Considerations

1. **Pipelining**: Batch multiple commands to reduce round trips
2. **Parallel connections**: Consider connection pooling for high throughput
3. **Streaming**: For large DataFrames, stream in chunks to avoid memory issues
4. **Serialization**: Pre-serialize JSON in Rust for efficiency

## Open Questions

1. Should we support atomic transactions across multiple keys?
2. Should we allow custom serialization for complex types (dates, lists)?
3. Should write operations return the generated keys by default?
4. Should we support conditional writes based on field values?
