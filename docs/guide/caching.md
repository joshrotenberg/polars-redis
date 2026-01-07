# DataFrame Caching

polars-redis provides functions for caching entire DataFrames in Redis using Arrow IPC or Parquet format. This enables using Redis as a high-performance distributed cache for intermediate computation results.

## Caching Decorator

The easiest way to cache function results is with the `@cache` decorator:

```python
import polars as pl
import polars_redis as redis

@redis.cache(url="redis://localhost:6379", ttl=3600)
def expensive_transform(start_date: str, end_date: str) -> pl.DataFrame:
    # Complex computation...
    return df

# First call: computes and caches
result = expensive_transform("2024-01-01", "2024-12-31")

# Second call: returns from cache instantly
result = expensive_transform("2024-01-01", "2024-12-31")
```

### Cache Control

```python
# Force recomputation (updates cache)
result = expensive_transform("2024-01-01", "2024-12-31", _cache_refresh=True)

# Skip cache entirely (no read or write)
result = expensive_transform("2024-01-01", "2024-12-31", _cache_skip=True)

# Invalidate specific cached result
expensive_transform.invalidate("2024-01-01", "2024-12-31")

# Check if result is cached
if expensive_transform.is_cached("2024-01-01", "2024-12-31"):
    print("Cache hit!")

# Get the cache key (for debugging)
key = expensive_transform.cache_key_for("2024-01-01", "2024-12-31")
```

### Custom Key Generation

```python
@redis.cache(
    url="redis://localhost",
    ttl=3600,
    key_prefix="transforms",
    key_fn=lambda start, end: f"{start}_{end}",
)
def transform(start_date: str, end_date: str) -> pl.DataFrame:
    ...
```

### LazyFrame Support

For functions returning LazyFrames, use `@cache_lazy`:

```python
@redis.cache_lazy(url="redis://localhost", ttl=3600)
def build_pipeline(config: dict) -> pl.LazyFrame:
    return pl.scan_parquet("data.parquet").filter(...)

# First call: collects and caches the result
lf = build_pipeline({"filter": "active"})
df = lf.collect()  # Already collected internally

# Second call: returns cached result as LazyFrame
lf = build_pipeline({"filter": "active"})
```

### Decorator Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `url` | `str` | required | Redis connection URL |
| `ttl` | `int` | `None` | Cache TTL in seconds |
| `key_prefix` | `str` | `"polars_redis:cache"` | Prefix for cache keys |
| `key_fn` | `Callable` | `None` | Custom key generation function |
| `format` | `str` | `"ipc"` | Serialization format |
| `compression` | `str` | `None` | Compression codec |
| `chunk_size_mb` | `int` | `None` | Chunk size for large DataFrames |

## Manual Caching Functions

For more control, use the caching functions directly:

## Quick Start

```python
import polars as pl
import polars_redis as redis

df = pl.DataFrame({
    "id": range(1000),
    "name": [f"item_{i}" for i in range(1000)],
    "value": [i * 0.5 for i in range(1000)],
})

# Cache the DataFrame
redis.cache_dataframe(df, "redis://localhost:6379", key="my_result")

# Retrieve it later
df2 = redis.get_cached_dataframe("redis://localhost:6379", key="my_result")
```

## Format Options

### Arrow IPC (Default)

Arrow IPC is the default format, optimized for speed:

```python
# Fast serialization/deserialization
redis.cache_dataframe(df, url, key="fast_cache", format="ipc")

# With compression
redis.cache_dataframe(df, url, key="compressed", format="ipc", compression="zstd")
```

**Compression options for IPC:** `uncompressed` (default), `lz4`, `zstd`

### Parquet

Parquet provides better compression ratios for storage efficiency:

```python
# Compact storage
redis.cache_dataframe(df, url, key="compact_cache", format="parquet")

# With specific compression
redis.cache_dataframe(
    df, url, key="result",
    format="parquet",
    compression="zstd",
    compression_level=3,
)
```

**Compression options for Parquet:** `uncompressed`, `snappy`, `gzip`, `lz4`, `zstd` (default)

## Chunked Storage for Large DataFrames

For large DataFrames that exceed Redis's 512MB value limit or cause memory pressure, polars-redis automatically chunks the data across multiple keys:

```python
# Automatic chunking (default: 100MB chunks)
redis.cache_dataframe(large_df, url, key="big_result")

# Custom chunk size (50MB chunks)
redis.cache_dataframe(large_df, url, key="big_result", chunk_size_mb=50)

# Disable chunking (store as single key)
redis.cache_dataframe(df, url, key="small_result", chunk_size_mb=0)
```

Chunked data is stored using the pattern:
- `{key}:meta` - JSON metadata (format, size, chunk count)
- `{key}:chunk:0`, `{key}:chunk:1`, ... - Data chunks

Retrieval, deletion, and other operations handle chunked data automatically.

### Inspecting Cached Data

Use `cache_info()` to get details about cached DataFrames:

```python
info = redis.cache_info(url, key="my_result")
if info:
    print(f"Size: {info['size_bytes']} bytes")
    print(f"Chunked: {info['is_chunked']}")
    print(f"Chunks: {info['num_chunks']}")
    print(f"TTL: {info['ttl']}")
```

## Time-to-Live (TTL)

Set expiration for cached DataFrames:

```python
# Expire in 1 hour
redis.cache_dataframe(df, url, key="temp_result", ttl=3600)

# Expire in 1 day
redis.cache_dataframe(df, url, key="daily_cache", ttl=86400)

# Check remaining TTL
remaining = redis.cache_ttl(url, key="temp_result")
print(f"Expires in {remaining} seconds")
```

## Cache Management

### Check Existence

```python
if redis.cache_exists(url, key="my_result"):
    df = redis.get_cached_dataframe(url, key="my_result")
else:
    df = compute_expensive_result()
    redis.cache_dataframe(df, url, key="my_result")
```

### Delete Cache

```python
redis.delete_cached(url, key="my_result")
```

### Lazy Loading

Load cached data as a LazyFrame for further processing:

```python
lf = redis.scan_cached(url, key="my_result")
if lf is not None:
    result = lf.filter(pl.col("value") > 100).collect()
```

## Use Cases

### Caching Pipeline Results

```python
def expensive_pipeline(start_date: str, end_date: str) -> pl.DataFrame:
    cache_key = f"pipeline:{start_date}:{end_date}"
    
    # Check cache first
    cached = redis.get_cached_dataframe(url, key=cache_key)
    if cached is not None:
        return cached
    
    # Compute and cache
    result = (
        pl.scan_parquet("large_dataset.parquet")
        .filter(pl.col("date").is_between(start_date, end_date))
        .group_by("category")
        .agg(pl.sum("amount"))
        .collect()
    )
    
    redis.cache_dataframe(result, url, key=cache_key, ttl=3600)
    return result
```

### Sharing Results Across Workers

```python
# Worker 1: Compute and cache
result = heavy_computation()
redis.cache_dataframe(result, url, key="shared:result")

# Worker 2: Retrieve cached result
df = redis.get_cached_dataframe(url, key="shared:result")
```

### Intermediate Results in ETL

```python
# Stage 1: Extract and transform
raw_data = extract_from_source()
redis.cache_dataframe(raw_data, url, key="etl:stage1", ttl=7200)

# Stage 2: Further processing (can be run separately)
stage1 = redis.get_cached_dataframe(url, key="etl:stage1")
processed = transform(stage1)
redis.cache_dataframe(processed, url, key="etl:stage2", ttl=7200)

# Stage 3: Final load
final = redis.get_cached_dataframe(url, key="etl:stage2")
load_to_destination(final)
```

## Format Comparison

| Aspect | Arrow IPC | Parquet |
|--------|-----------|---------|
| Serialization speed | Faster | Slower |
| Deserialization speed | Faster | Slower |
| Compression ratio | Good | Better |
| Zero-copy potential | Yes | No |
| Best for | Hot data, short-term cache | Large datasets, long-term cache |

**Recommendations:**

- Use **IPC** for frequently accessed data where speed matters
- Use **Parquet** for large datasets where storage efficiency matters
- Use **zstd** compression for best balance of speed and size

## API Reference

### cache_dataframe

```python
def cache_dataframe(
    df: pl.DataFrame,
    url: str,
    key: str,
    *,
    format: Literal["ipc", "parquet"] = "ipc",
    compression: str | None = None,
    compression_level: int | None = None,
    ttl: int | None = None,
    chunk_size_mb: int | None = None,
) -> int
```

Cache a DataFrame in Redis. Returns bytes written. Set `chunk_size_mb=0` to disable chunking.

### get_cached_dataframe

```python
def get_cached_dataframe(
    url: str,
    key: str,
    *,
    format: Literal["ipc", "parquet"] = "ipc",
    columns: list[str] | None = None,
    n_rows: int | None = None,
) -> pl.DataFrame | None
```

Retrieve a cached DataFrame. Returns None if key doesn't exist.

### scan_cached

```python
def scan_cached(
    url: str,
    key: str,
    *,
    format: Literal["ipc", "parquet"] = "ipc",
) -> pl.LazyFrame | None
```

Retrieve cached data as a LazyFrame.

### delete_cached

```python
def delete_cached(url: str, key: str) -> bool
```

Delete a cached DataFrame. Returns True if deleted.

### cache_exists

```python
def cache_exists(url: str, key: str) -> bool
```

Check if a cached DataFrame exists.

### cache_ttl

```python
def cache_ttl(url: str, key: str) -> int | None
```

Get remaining TTL in seconds, or None if no TTL set.

### cache_info

```python
def cache_info(url: str, key: str) -> dict | None
```

Get information about a cached DataFrame. Returns a dict with keys:
- `format`: Serialization format used
- `size_bytes`: Total size in bytes
- `is_chunked`: Whether data is stored in chunks
- `num_chunks`: Number of chunks (1 if not chunked)
- `chunk_size`: Size of each chunk in bytes
- `ttl`: Remaining TTL in seconds, or None

## Rust API

The caching functionality is also available in Rust for pure Rust applications.

### Quick Start (Rust)

```rust
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use polars_redis::cache::{
    cache_record_batch, get_cached_record_batch, delete_cached,
    cache_exists, cache_ttl, cache_info,
    CacheConfig, CacheFormat, IpcCompression, ParquetCompressionType,
};
use std::sync::Arc;

// Create a RecordBatch
let schema = Arc::new(Schema::new(vec![
    Field::new("id", DataType::Int64, false),
    Field::new("name", DataType::Utf8, false),
]));

let batch = RecordBatch::try_new(
    schema,
    vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        Arc::new(StringArray::from(vec!["a", "b", "c"])),
    ],
).unwrap();

// Cache with default config (IPC format)
let config = CacheConfig::default();
cache_record_batch("redis://localhost:6379", "my_result", &batch, &config).unwrap();

// Retrieve later
let cached = get_cached_record_batch("redis://localhost:6379", "my_result").unwrap();
assert!(cached.is_some());

// Delete when done
delete_cached("redis://localhost:6379", "my_result").unwrap();
```

### Configuration (Rust)

```rust
use polars_redis::cache::{CacheConfig, IpcCompression, ParquetCompressionType};

// IPC with compression
let config = CacheConfig::ipc()
    .with_ipc_compression(IpcCompression::Zstd)
    .with_ttl(3600);

// Parquet with compression
let config = CacheConfig::parquet()
    .with_parquet_compression(ParquetCompressionType::Zstd)
    .with_compression_level(3)
    .with_ttl(86400);

// Custom chunk size (50MB)
let config = CacheConfig::default()
    .with_chunk_size(50 * 1024 * 1024);

// Disable chunking
let config = CacheConfig::default()
    .without_chunking();
```

### Rust API Reference

| Function | Description |
|----------|-------------|
| `cache_record_batch(url, key, batch, config)` | Cache a RecordBatch in Redis |
| `get_cached_record_batch(url, key)` | Retrieve a cached RecordBatch |
| `delete_cached(url, key)` | Delete cached data |
| `cache_exists(url, key)` | Check if cached data exists |
| `cache_ttl(url, key)` | Get remaining TTL |
| `cache_info(url, key)` | Get cache metadata |

### CacheConfig Options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `format` | `CacheFormat` | `Ipc` | Serialization format |
| `ipc_compression` | `IpcCompression` | `Uncompressed` | IPC compression codec |
| `parquet_compression` | `ParquetCompressionType` | `Zstd` | Parquet compression codec |
| `compression_level` | `Option<i32>` | `None` | Compression level |
| `ttl` | `Option<i64>` | `None` | TTL in seconds |
| `chunk_size` | `usize` | `100MB` | Chunk size in bytes |
