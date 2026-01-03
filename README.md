# polars-redis

A [Polars](https://pola.rs/) IO plugin for scanning Redis data structures as LazyFrames with projection pushdown.

[![PyPI](https://img.shields.io/pypi/v/polars-redis.svg)](https://pypi.org/project/polars-redis/)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)

## Features

- **Lazy scanning** - Scan Redis hashes and JSON documents as Polars LazyFrames
- **Projection pushdown** - Only fetch the fields you need (HMGET vs HGETALL)
- **Batched iteration** - Efficiently handle large keyspaces with configurable batch sizes
- **Type conversion** - Automatic schema-based parsing (strings, integers, floats, booleans)
- **Null handling** - Missing fields become null values

## Installation

```bash
pip install polars-redis
```

## Quick Start

```python
import polars as pl
import polars_redis as redis

# Scan Redis hashes matching a pattern
lf = redis.scan_hashes(
    "redis://localhost:6379",
    pattern="user:*",
    schema={
        "name": pl.Utf8,
        "age": pl.Int64,
        "score": pl.Float64,
        "active": pl.Boolean,
    }
)

# LazyFrame - nothing executed yet
# Projection pushdown: only "name" and "age" are fetched from Redis
result = (
    lf
    .filter(pl.col("age") > 30)
    .select(["name", "age"])
    .collect()
)
```

## API Reference

### Lazy Functions (Recommended)

#### `scan_hashes()`

Scan Redis hashes and return a LazyFrame.

```python
lf = redis.scan_hashes(
    url="redis://localhost:6379",
    pattern="user:*",                    # Key pattern (supports * and ?)
    schema={"name": pl.Utf8, "age": pl.Int64},
    include_key=True,                    # Include Redis key as column
    key_column_name="_key",              # Name of key column
    batch_size=1000,                     # Keys per batch
    count_hint=100,                      # SCAN COUNT hint
)
```

#### `scan_json()`

Scan RedisJSON documents and return a LazyFrame.

```python
lf = redis.scan_json(
    url="redis://localhost:6379",
    pattern="doc:*",
    schema={"title": pl.Utf8, "price": pl.Float64},
)
```

### Eager Functions

For convenience when you want a DataFrame immediately:

```python
# Returns DataFrame directly (calls .collect() internally)
df = redis.read_hashes(url, pattern="user:*", schema=schema)
df = redis.read_json(url, pattern="doc:*", schema=schema)
```

## Examples

### Basic Hash Scanning

```python
import polars as pl
import polars_redis as redis

# Define schema for type conversion
schema = {
    "name": pl.Utf8,
    "email": pl.Utf8,
    "age": pl.Int64,
    "score": pl.Float64,
    "active": pl.Boolean,
}

# Scan all user hashes
lf = redis.scan_hashes(
    "redis://localhost:6379",
    pattern="user:*",
    schema=schema,
)

# Collect all data
df = lf.collect()
print(df)
```

### Projection Pushdown

When you select specific columns, polars-redis uses `HMGET` instead of `HGETALL`, fetching only the fields you need:

```python
# Only fetches "name" and "age" fields from Redis
df = (
    redis.scan_hashes(url, pattern="user:*", schema=schema)
    .select(["name", "age"])
    .collect()
)
```

### Filtering

Filters are applied client-side after fetching:

```python
# Get active users over 30 with high scores
df = (
    redis.scan_hashes(url, pattern="user:*", schema=schema)
    .filter(
        (pl.col("age") > 30) &
        (pl.col("active") == True) &
        (pl.col("score") > 80)
    )
    .collect()
)
```

### Aggregation

```python
# Average score by active status
df = (
    redis.scan_hashes(url, pattern="user:*", schema=schema)
    .group_by("active")
    .agg([
        pl.len().alias("count"),
        pl.col("age").mean().alias("avg_age"),
        pl.col("score").mean().alias("avg_score"),
    ])
    .collect()
)
```

### Limiting Results

The `head()` operation stops iteration early:

```python
# Only scans until 10 rows are collected
df = redis.scan_hashes(url, pattern="user:*", schema=schema).head(10).collect()
```

### Joining with Local Data

```python
# Scan users from Redis
users = redis.scan_hashes(
    url,
    pattern="user:*",
    schema={"name": pl.Utf8},
    include_key=True,
)

# Local orders data
orders = pl.DataFrame({
    "user_key": ["user:1", "user:2", "user:3"],
    "amount": [99.99, 149.50, 299.00],
}).lazy()

# Join on Redis key
result = (
    users
    .join(orders, left_on="_key", right_on="user_key")
    .collect()
)
```

### JSON Documents

```python
# Scan RedisJSON documents
lf = redis.scan_json(
    "redis://localhost:6379",
    pattern="product:*",
    schema={
        "name": pl.Utf8,
        "category": pl.Utf8,
        "price": pl.Float64,
        "in_stock": pl.Boolean,
    },
)

# Aggregate by category
df = (
    lf
    .group_by("category")
    .agg([
        pl.col("price").mean().alias("avg_price"),
        pl.len().alias("count"),
    ])
    .sort("avg_price", descending=True)
    .collect()
)
```

## Supported Types

| Polars Type | Redis String Parsing |
|-------------|---------------------|
| `pl.Utf8` / `pl.String` | Pass-through |
| `pl.Int64` | Parsed as integer |
| `pl.Float64` | Parsed as float |
| `pl.Boolean` | `true/false`, `1/0`, `yes/no`, `t/f`, `y/n` |

## Performance Tips

1. **Use projection pushdown** - Only include fields you need in your schema or use `.select()` to limit columns
2. **Adjust batch_size** - Larger batches reduce round trips but use more memory
3. **Use count_hint** - Higher values make SCAN return more keys per iteration
4. **Use head()** - If you only need N rows, use `.head(N)` to stop early

## Roadmap

- [x] `scan_hashes()` - Lazy hash scanning
- [x] `scan_json()` - Lazy JSON scanning  
- [x] `read_hashes()` / `read_json()` - Eager variants
- [x] Projection pushdown
- [x] n_rows pushdown (head/limit)
- [ ] Schema inference
- [ ] Write support (`write_hashes()`, `write_json()`)
- [ ] RediSearch predicate pushdown
- [ ] Redis Streams support
- [ ] RedisTimeSeries support

## Development

### Prerequisites

- Python 3.9+
- Rust 1.70+
- Redis 7.0+ (with RedisJSON module for JSON support)

### Setup

```bash
# Clone the repository
git clone https://github.com/joshrotenberg/polars-redis.git
cd polars-redis

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install maturin polars pytest

# Build and install in development mode
maturin develop

# Run tests (requires Redis on localhost:6379)
cargo test --lib --all-features
pytest tests/ -v
```

### Running Lints

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
