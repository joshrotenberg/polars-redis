# Rust API Reference

The Rust API provides direct access to the scanning and conversion primitives.

## Core Types

### RedisType

Enum representing supported Redis value types:

```rust
pub enum RedisType {
    Utf8,
    Int64,
    Float64,
    Boolean,
    Date,
    Datetime,
}
```

### HashSchema

Schema definition for Redis hashes:

```rust
use polars_redis::{HashSchema, RedisType};

let schema = HashSchema::new(vec![
    ("name".to_string(), RedisType::Utf8),
    ("age".to_string(), RedisType::Int64),
])
.with_key(true)
.with_key_column_name("_key")
.with_ttl(true)
.with_ttl_column_name("_ttl")
.with_row_index(true)
.with_row_index_column_name("_index");
```

### JsonSchema

Schema definition for RedisJSON documents:

```rust
use polars_redis::JsonSchema;
use arrow::datatypes::DataType;

let schema = JsonSchema::new(vec![
    ("title".to_string(), DataType::Utf8),
    ("views".to_string(), DataType::Int64),
])
.with_key(true);
```

### StringSchema

Schema definition for Redis strings:

```rust
use polars_redis::{StringSchema, RedisType};

let schema = StringSchema::new(RedisType::Int64)
    .with_key(true)
    .with_key_column_name("_key")
    .with_value_column_name("count");
```

### BatchConfig

Configuration for batch iteration:

```rust
use polars_redis::BatchConfig;

let config = BatchConfig::new("user:*")
    .with_batch_size(1000)
    .with_count_hint(100)
    .with_max_rows(10000);
```

## Batch Iterators

### HashBatchIterator

Iterate over Redis hashes in Arrow batches:

```rust
use polars_redis::{HashBatchIterator, HashSchema, BatchConfig, RedisType};

let schema = HashSchema::new(vec![
    ("name".to_string(), RedisType::Utf8),
    ("age".to_string(), RedisType::Int64),
]);

let config = BatchConfig::new("user:*").with_batch_size(1000);

let mut iterator = HashBatchIterator::new(
    "redis://localhost:6379",
    schema,
    config,
    None,  // projection
)?;

while let Some(batch) = iterator.next_batch()? {
    println!("Got {} rows", batch.num_rows());
}
```

### JsonBatchIterator

Iterate over RedisJSON documents:

```rust
use polars_redis::{JsonBatchIterator, JsonSchema, BatchConfig};
use arrow::datatypes::DataType;

let schema = JsonSchema::new(vec![
    ("title".to_string(), DataType::Utf8),
]);

let config = BatchConfig::new("doc:*");

let mut iterator = JsonBatchIterator::new(
    "redis://localhost:6379",
    schema,
    config,
    None,
)?;
```

### StringBatchIterator

Iterate over Redis strings:

```rust
use polars_redis::{StringBatchIterator, StringSchema, BatchConfig, RedisType};

let schema = StringSchema::new(RedisType::Int64);
let config = BatchConfig::new("counter:*");

let mut iterator = StringBatchIterator::new(
    "redis://localhost:6379",
    schema,
    config,
)?;
```

## Schema Inference

### infer_hash_schema

```rust
use polars_redis::io::infer::infer_hash_schema;

let (fields, keys_sampled) = infer_hash_schema(
    "redis://localhost:6379",
    "user:*",
    100,   // sample_size
    true,  // type_inference
)?;

for (name, type_str) in &fields {
    println!("{}: {}", name, type_str);
}
```

### infer_json_schema

```rust
use polars_redis::io::infer::infer_json_schema;

let (fields, keys_sampled) = infer_json_schema(
    "redis://localhost:6379",
    "doc:*",
    100,
)?;
```

## Arrow Conversion

### batch_to_ipc

Serialize an Arrow RecordBatch to IPC format:

```rust
use polars_redis::batch_to_ipc;

let batch = iterator.next_batch()?.unwrap();
let ipc_bytes = batch_to_ipc(&batch)?;
// ipc_bytes can be read by Polars: pl.read_ipc(io.BytesIO(ipc_bytes))
```

## Write Operations

Write operations are exposed through the Python bindings. The Rust API provides the underlying implementation via:

- `write::write_hashes_sync`
- `write::write_json_sync`
- `write::write_strings_sync`

## Error Handling

All operations return `Result<T, PolarsRedisError>`:

```rust
use polars_redis::PolarsRedisError;

match iterator.next_batch() {
    Ok(Some(batch)) => { /* process */ }
    Ok(None) => { /* done */ }
    Err(PolarsRedisError::Connection(e)) => { /* connection error */ }
    Err(PolarsRedisError::Redis(e)) => { /* redis error */ }
    Err(e) => { /* other error */ }
}
```

## Re-exports

The crate re-exports commonly used types:

```rust
pub use arrow;
pub use redis;
```
