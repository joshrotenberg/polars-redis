# Rust Examples

Example code is in [`examples/rust/`](https://github.com/joshrotenberg/polars-redis/tree/master/examples/rust).

Run examples with:

```bash
cargo run --example scan_hashes
cargo run --example scan_json
cargo run --example scan_strings
cargo run --example schema_inference
```

## Scan Hashes

Comprehensive example covering projection, batching, and row limits:

```rust
--8<-- "examples/rust/scan_hashes.rs"
```

## Scan JSON

Working with RedisJSON documents:

```rust
--8<-- "examples/rust/scan_json.rs"
```

## Scan Strings

Redis strings with different value types:

```rust
--8<-- "examples/rust/scan_strings.rs"
```

## Schema Inference

Automatic schema detection from existing data:

```rust
--8<-- "examples/rust/schema_inference.rs"
```
