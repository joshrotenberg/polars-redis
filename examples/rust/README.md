# Rust Examples

These examples demonstrate the polars-redis Rust API directly.

## Prerequisites

- Redis running on localhost:6379
- RedisJSON module for JSON examples
- Sample data loaded

## Examples

### Scanning

| File | Description |
|------|-------------|
| `scan_hashes.rs` | Scanning Redis hashes with projection and batching |
| `scan_json.rs` | Working with RedisJSON documents |
| `scan_strings.rs` | Redis strings with different value types |
| `schema_inference.rs` | Automatic schema detection |

### Writing

| File | Description |
|------|-------------|
| `write_hashes.rs` | Writing data as Redis hashes |
| `write_json.rs` | Writing data as RedisJSON documents |
| `write_strings.rs` | Writing data as Redis strings |

## Running

```bash
# Load sample data for scan examples
python examples/python/setup_sample_data.py

# Run scan examples
cargo run --example scan_hashes
cargo run --example scan_json
cargo run --example scan_strings
cargo run --example schema_inference

# Run write examples (self-contained, cleanup after themselves)
cargo run --example write_hashes
cargo run --example write_json
cargo run --example write_strings
```

## Environment Variables

- `REDIS_URL`: Override the default Redis connection URL (default: `redis://localhost:6379`)
