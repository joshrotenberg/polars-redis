# DataFrame Caching

polars-redis provides functions for caching entire DataFrames in Redis using Arrow IPC or Parquet format. This enables using Redis as a high-performance distributed cache for intermediate computation results.

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
) -> int
```

Cache a DataFrame in Redis. Returns bytes written.

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
