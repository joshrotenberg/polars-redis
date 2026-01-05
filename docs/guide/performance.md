# Performance Guide

This guide covers performance optimization strategies for polars-redis, including
batch tuning, parallel fetching, memory management, and when to use different
approaches.

## Key Performance Factors

| Factor | Impact | Tunable |
|--------|--------|---------|
| Batch size | Throughput vs memory | `batch_size` parameter |
| Parallel workers | Fetch speed | `parallel` parameter |
| Projection pushdown | Data transfer | Column selection |
| Predicate pushdown | Data transfer | RediSearch queries |
| Network latency | Overall speed | Connection pooling |

## Batch Size Tuning

The `batch_size` parameter controls how many keys are processed in each Redis
pipeline operation.

### Impact on Performance

```
Batch Size    Throughput       Memory Usage    Network Round-trips
-----------   ------------     ------------    -------------------
100           Low              Low             High (many batches)
1,000         Good             Moderate        Moderate
10,000        High             High            Low (fewer batches)
100,000       Diminishing      Very High       Minimal
```

### Recommendations

```python
import polars_redis as pr

# Small datasets (<10K keys): larger batches are fine
df = pr.scan_hashes(url, "small:*", schema, batch_size=5000).collect()

# Medium datasets (10K-1M keys): balance throughput and memory
df = pr.scan_hashes(url, "medium:*", schema, batch_size=1000).collect()

# Large datasets (>1M keys): use streaming with moderate batches
lf = pr.scan_hashes(url, "large:*", schema, batch_size=1000)
for batch in lf.collect_batches():
    process(batch)
```

### Finding Your Optimal Batch Size

```python
import time
import polars_redis as pr

def benchmark_batch_size(url, pattern, schema, batch_sizes):
    results = []
    for size in batch_sizes:
        start = time.perf_counter()
        df = pr.scan_hashes(url, pattern, schema, batch_size=size).collect()
        elapsed = time.perf_counter() - start
        results.append({
            "batch_size": size,
            "rows": len(df),
            "time_sec": elapsed,
            "rows_per_sec": len(df) / elapsed
        })
    return pl.DataFrame(results)

# Test different batch sizes
results = benchmark_batch_size(
    "redis://localhost",
    "user:*",
    {"name": pl.Utf8, "age": pl.Int64},
    [100, 500, 1000, 2000, 5000]
)
print(results)
```

## Parallel Fetching

The `parallel` parameter enables concurrent data fetching across multiple workers.

### When to Use Parallel Fetching

- Large datasets with many keys
- High-latency connections (remote Redis)
- Multi-core systems with available CPU

### Configuration

```python
import polars_redis as pr

# Single-threaded (default)
df = pr.scan_hashes(url, "user:*", schema).collect()

# 4 parallel workers
df = pr.scan_hashes(url, "user:*", schema, parallel=4).collect()

# Match CPU cores
import os
df = pr.scan_hashes(url, "user:*", schema, parallel=os.cpu_count()).collect()
```

### Scaling Characteristics

```
Workers    Relative Throughput    Notes
-------    -------------------    -----
1          1.0x (baseline)        Simple, predictable
2          ~1.8x                  Good improvement
4          ~3.2x                  Diminishing returns start
8          ~4.5x                  Network often becomes bottleneck
16         ~5.0x                  Rarely beneficial beyond this
```

### Parallel + Batch Size Interaction

```python
# Optimal: moderate batch size with parallel workers
df = pr.scan_hashes(
    url, "user:*", schema,
    batch_size=1000,  # Each worker handles 1000 keys at a time
    parallel=4        # 4 workers = 4000 keys in flight
).collect()
```

## Projection Pushdown

Request only the columns you need to reduce data transfer.

### How It Works

```python
# Full scan: transfers ALL fields from Redis
df = pr.scan_hashes(url, "user:*", schema).collect()

# Projection pushdown: only requested columns transferred
df = (
    pr.scan_hashes(url, "user:*", schema)
    .select(["name", "email"])  # Polars pushes this down
    .collect()
)
```

### Memory Savings

```
Fields in Hash    Columns Selected    Data Transfer
--------------    ----------------    -------------
10                10 (all)            100%
10                5                   ~50%
10                2                   ~20%
10                1                   ~10%
```

### Best Practice

```python
# Define full schema once
full_schema = {
    "name": pl.Utf8,
    "email": pl.Utf8,
    "age": pl.Int64,
    "department": pl.Utf8,
    "salary": pl.Float64,
    "hire_date": pl.Utf8,
    "manager_id": pl.Utf8,
    "location": pl.Utf8,
}

# Select only what you need
summary = (
    pr.scan_hashes(url, "employee:*", full_schema)
    .select(["department", "salary"])
    .group_by("department")
    .agg(pl.col("salary").mean())
    .collect()
)
```

## Predicate Pushdown with RediSearch

The biggest performance gain comes from filtering data in Redis rather than
transferring everything and filtering in Python.

### SCAN vs RediSearch

```python
# BAD: Scan ALL keys, filter in Python
df = (
    pr.scan_hashes(url, "user:*", schema)
    .filter(pl.col("age") > 30)
    .filter(pl.col("status") == "active")
    .collect()
)
# Transfers: ALL users
# Filters: In Python after transfer

# GOOD: Filter in Redis with RediSearch
df = pr.search_hashes(
    url,
    index="users_idx",
    query="@age:[30 +inf] @status:{active}",
    schema=schema
).collect()
# Transfers: Only matching users
# Filters: In Redis before transfer
```

### Performance Comparison

```
Dataset Size    Matching %    SCAN Time    RediSearch Time    Speedup
------------    ----------    ---------    ---------------    -------
10,000          100%          0.5s         0.6s               0.8x (overhead)
10,000          10%           0.5s         0.1s               5x
100,000         10%           5.0s         0.2s               25x
1,000,000       1%            50s          0.3s               166x
```

### Server-Side Aggregation

```python
# BAD: Transfer all data, aggregate in Polars
df = pr.scan_hashes(url, "sale:*", schema).collect()
result = df.group_by("region").agg(pl.col("amount").sum())

# GOOD: Aggregate in Redis
result = pr.aggregate_hashes(
    url,
    index="sales_idx",
    query="*",
    group_by=["region"],
    reduce=[("SUM", ["@amount"], "total")]
)
# Only aggregated results transferred
```

## Memory Management

### Streaming Large Datasets

```python
import polars_redis as pr

# DON'T: Load everything into memory
df = pr.scan_hashes(url, "huge:*", schema).collect()  # May OOM

# DO: Process in batches
lf = pr.scan_hashes(url, "huge:*", schema, batch_size=1000)
for batch in lf.collect_batches():
    process_and_save(batch)

# Or use sink operations
lf = pr.scan_hashes(url, "huge:*", schema)
lf.sink_parquet("output.parquet")  # Streams to disk
```

### Memory Estimation

```python
# Rough estimation: ~100 bytes per field per row (varies by data)
keys = 1_000_000
fields = 10
estimated_bytes = keys * fields * 100
print(f"Estimated memory: {estimated_bytes / 1e9:.1f} GB")
```

### Monitoring Memory

```python
import psutil
import polars_redis as pr

process = psutil.Process()
before = process.memory_info().rss

df = pr.scan_hashes(url, pattern, schema).collect()

after = process.memory_info().rss
print(f"Memory used: {(after - before) / 1e6:.1f} MB")
print(f"Rows: {len(df)}, Bytes per row: {(after - before) / len(df):.0f}")
```

## Network Considerations

### High-Latency Connections

For remote Redis instances with high latency:

```python
# Increase batch size to reduce round-trips
df = pr.scan_hashes(url, pattern, schema, batch_size=5000).collect()

# Use parallel fetching to hide latency
df = pr.scan_hashes(url, pattern, schema, parallel=8).collect()
```

### Connection String Tuning

```python
# Basic connection
url = "redis://localhost:6379"

# With connection timeout
url = "redis://localhost:6379?connect_timeout=5"

# With TLS
url = "rediss://redis.example.com:6380"
```

## Anti-Patterns to Avoid

### 1. Scanning When You Should Search

```python
# BAD: Scan 1M keys to find 100 matches
df = pr.scan_hashes(url, "user:*", schema).filter(pl.col("vip") == True).collect()

# GOOD: Use RediSearch index
df = pr.search_hashes(url, "users_idx", "@vip:{true}", schema).collect()
```

### 2. Tiny Batch Sizes

```python
# BAD: Too many round-trips
df = pr.scan_hashes(url, pattern, schema, batch_size=10).collect()

# GOOD: Reasonable batch size
df = pr.scan_hashes(url, pattern, schema, batch_size=1000).collect()
```

### 3. Fetching Unused Columns

```python
# BAD: Fetch all fields, use one
df = pr.scan_hashes(url, pattern, large_schema).collect()
names = df["name"].to_list()

# GOOD: Project to only needed column
names = pr.scan_hashes(url, pattern, large_schema).select("name").collect()["name"].to_list()
```

### 4. Repeated Full Scans

```python
# BAD: Scan same data multiple times
users = pr.scan_hashes(url, "user:*", schema).collect()
active = pr.scan_hashes(url, "user:*", schema).filter(...).collect()
admins = pr.scan_hashes(url, "user:*", schema).filter(...).collect()

# GOOD: Scan once, filter in memory
users = pr.scan_hashes(url, "user:*", schema).collect()
active = users.filter(...)
admins = users.filter(...)

# BETTER: Use RediSearch if filtering is selective
active = pr.search_hashes(url, "users_idx", "@status:{active}", schema).collect()
```

## Benchmarking Your Setup

Use this script to benchmark your specific configuration:

```python
import time
import polars as pl
import polars_redis as pr

def benchmark(name, func):
    start = time.perf_counter()
    result = func()
    elapsed = time.perf_counter() - start
    rows = len(result) if hasattr(result, '__len__') else result
    print(f"{name}: {elapsed:.3f}s ({rows} rows)")
    return elapsed

url = "redis://localhost:6379"
pattern = "bench:*"
schema = {"field1": pl.Utf8, "field2": pl.Int64, "field3": pl.Float64}

# Baseline
benchmark("Default", lambda: pr.scan_hashes(url, pattern, schema).collect())

# Batch size variations
for bs in [100, 500, 1000, 2000, 5000]:
    benchmark(f"batch_size={bs}", 
              lambda bs=bs: pr.scan_hashes(url, pattern, schema, batch_size=bs).collect())

# Parallel variations
for p in [1, 2, 4, 8]:
    benchmark(f"parallel={p}",
              lambda p=p: pr.scan_hashes(url, pattern, schema, parallel=p).collect())

# Projection
benchmark("All columns", lambda: pr.scan_hashes(url, pattern, schema).collect())
benchmark("1 column", lambda: pr.scan_hashes(url, pattern, schema).select("field1").collect())
```

## Quick Reference

| Scenario | Recommendation |
|----------|----------------|
| < 10K keys | Default settings, batch_size=1000-5000 |
| 10K-100K keys | batch_size=1000, parallel=2-4 |
| 100K-1M keys | batch_size=1000, parallel=4-8, consider RediSearch |
| > 1M keys | RediSearch for filtering, streaming for full scans |
| High latency | Larger batch_size, more parallel workers |
| Memory constrained | Smaller batch_size, streaming |
| Selective queries (<10% match) | Always use RediSearch |
