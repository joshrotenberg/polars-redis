# polars-redis

A Polars IO plugin for scanning Redis data structures as LazyFrames.

## Features

- Scan Redis hashes, JSON documents, and strings as Polars LazyFrames
- Projection pushdown (only fetch needed fields)
- Batched iteration (handles large keyspaces efficiently)
- Predicate pushdown via RediSearch (planned)

## Installation

```bash
pip install polars-redis
```

## Usage

```python
import polars as pl
import polars_redis as redis

# Scan Redis hashes matching a pattern
lf = redis.scan_hashes(
    "redis://localhost:6379",
    pattern="user:*",
    schema={"name": pl.Utf8, "age": pl.Int64, "email": pl.Utf8}
)

# LazyFrame - nothing executed yet
result = (
    lf
    .filter(pl.col("age") > 30)
    .select(["name", "email"])
    .collect()
)
```

## Development

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dev dependencies
pip install maturin polars pytest

# Build and install in development mode
maturin develop

# Run tests
cargo test
pytest
```

## License

MIT OR Apache-2.0
