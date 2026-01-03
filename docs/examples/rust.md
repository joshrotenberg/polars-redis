# Rust Examples

Example code is in [`examples/rust/`](https://github.com/joshrotenberg/polars-redis/tree/master/examples/rust).

Run examples with:

```bash
# Scan examples
cargo run --example scan_hashes
cargo run --example scan_json
cargo run --example scan_strings
cargo run --example schema_inference

# Write examples
cargo run --example write_hashes
cargo run --example write_json
cargo run --example write_strings
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

## Write Hashes

Writing data to Redis as hashes:

```rust
--8<-- "examples/rust/write_hashes.rs"
```

## Write JSON

Writing data to Redis as JSON documents:

```rust
--8<-- "examples/rust/write_json.rs"
```

## Write Strings

Writing data to Redis as strings:

```rust
--8<-- "examples/rust/write_strings.rs"
```
