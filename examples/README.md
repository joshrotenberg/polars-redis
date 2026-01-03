# polars-redis Examples

This directory contains examples demonstrating how to use polars-redis to scan Redis data into Polars DataFrames.

## Prerequisites

1. **Redis running locally** (or set `REDIS_URL` environment variable)
   ```bash
   # Using Docker
   docker run -d --name redis -p 6379:6379 redis:latest
   
   # Or using redis-server directly
   redis-server
   ```

2. **Sample data loaded**
   ```bash
   pip install redis
   python setup_sample_data.py
   ```

## Examples

### Python Examples

| File | Description |
|------|-------------|
| `python/setup_sample_data.py` | Creates sample user hashes and JSON docs in Redis |
| `python/scan_hashes.py` | Comprehensive demo of hash scanning features |
| `python/scan_json.py` | Demo of JSON document scanning |

Run Python examples:
```bash
cd examples/python
python setup_sample_data.py
python scan_hashes.py
python scan_json.py
```

### Rust Examples

| File | Description |
|------|-------------|
| `rust/scan_hashes.rs` | Direct Rust API usage for hash scanning |
| `rust/scan_json.rs` | Direct Rust API for JSON document scanning |

Run Rust examples:
```bash
cargo run --example scan_hashes
cargo run --example scan_json
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:6379` | Redis connection URL |
| `NUM_USERS` | `100` | Number of sample users to create |

## Quick Start

```bash
# Terminal 1: Start Redis
docker run -d --name redis -p 6379:6379 redis:latest

# Terminal 2: Run examples
pip install redis polars
python setup_sample_data.py
python scan_hashes.py
```

## Example Output

```
=== Example 1: Basic Hash Scanning ===

LazyFrame schema: {'_key': String, 'name': String, 'email': String, 'age': Int64, 'score': Float64, 'active': Boolean}
Type: <class 'polars.LazyFrame'>

Collected 100 rows
shape: (5, 6)
┌─────────┬───────────────┬─────────────────────────────┬─────┬───────┬────────┐
│ _key    ┆ name          ┆ email                       ┆ age ┆ score ┆ active │
│ ---     ┆ ---           ┆ ---                         ┆ --- ┆ ---   ┆ ---    │
│ str     ┆ str           ┆ str                         ┆ i64 ┆ f64   ┆ bool   │
╞═════════╪═══════════════╪═════════════════════════════╪═════╪═══════╪════════╡
│ user:1  ┆ Alice Smith   ┆ alice.smith1@gmail.com      ┆ 34  ┆ 87.5  ┆ true   │
│ user:2  ┆ Bob Johnson   ┆ bob.johnson2@yahoo.com      ┆ 28  ┆ 62.3  ┆ true   │
│ user:3  ┆ Charlie Brown ┆ charlie.brown3@outlook.com  ┆ 45  ┆ 91.2  ┆ false  │
│ user:4  ┆ Diana Garcia  ┆ diana.garcia4@example.com   ┆ 31  ┆ 78.9  ┆ true   │
│ user:5  ┆ Eve Wilson    ┆ eve.wilson5@test.org        ┆ 52  ┆ 45.6  ┆ true   │
└─────────┴───────────────┴─────────────────────────────┴─────┴───────┴────────┘
```

## API Quick Reference

### Python

```python
import polars as pl
import polars_redis as redis

# Basic scan - returns LazyFrame
lf = redis.scan_hashes(
    "redis://localhost:6379",
    pattern="user:*",
    schema={
        "name": pl.Utf8,
        "age": pl.Int64,
    },
    include_key=True,
)

# Use Polars operations
result = (
    lf
    .filter(pl.col("age") > 30)
    .select(["_key", "name", "age"])
    .collect()
)
```

### Rust

```rust
use polars_redis::{BatchConfig, HashBatchIterator, HashSchema, RedisType};

let schema = HashSchema::new(vec![
    ("name".to_string(), RedisType::Utf8),
    ("age".to_string(), RedisType::Int64),
])
.with_key(true);

let config = BatchConfig::new("user:*")
    .with_batch_size(100);

let mut iterator = HashBatchIterator::new(url, schema, config, None)?;

while let Some(batch) = iterator.next_batch()? {
    println!("Got {} rows", batch.num_rows());
}
```

## Features Demonstrated

- [x] Basic hash scanning
- [x] Projection pushdown (HMGET optimization)
- [x] Row limiting (n_rows / .head())
- [x] Filtering and aggregation
- [x] Joining Redis with local data
- [x] Batch configuration
- [x] Query plan inspection
- [x] Error handling
