# Rust Examples

These examples demonstrate the polars-redis Rust API directly.

## Prerequisites

- Redis running on localhost:6379
- RedisJSON module for JSON examples
- Sample data loaded

## Examples

| File | Description |
|------|-------------|
| `scan_hashes.rs` | Scanning Redis hashes with projection and batching |
| `scan_json.rs` | Working with RedisJSON documents |
| `scan_strings.rs` | Redis strings with different value types |
| `schema_inference.rs` | Automatic schema detection |

## Running

```bash
# Load sample data first
python examples/python/setup_sample_data.py

# Run examples
cargo run --example scan_hashes
cargo run --example scan_json
cargo run --example scan_strings
cargo run --example schema_inference
```

## Environment Variables

- `REDIS_URL`: Override the default Redis connection URL (default: `redis://localhost:6379`)
