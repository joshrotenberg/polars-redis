# Examples

Examples for polars-redis in both Python and Rust.

## Prerequisites

- Redis running on localhost:6379
- RedisJSON module for JSON examples

## Python Examples

See [python/README.md](python/README.md) for Python-specific examples.

```bash
pip install polars-redis
python examples/python/basic_hashes.py
```

## Rust Examples

See [rust/](rust/) for Rust examples.

```bash
cargo run --example scan_hashes
cargo run --example scan_json
```

## Sample Data

Use `python/setup_sample_data.py` to populate Redis with sample data for testing:

```bash
python examples/python/setup_sample_data.py
```
