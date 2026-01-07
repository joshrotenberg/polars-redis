//! Example: Schema inference with polars-redis.
//!
//! This example demonstrates how to use schema inference to automatically
//! detect field names and types from existing Redis data.
//!
//! Run with: cargo run --example schema_inference
//!
//! Prerequisites:
//!     - Redis running on localhost:6379
//!     - Sample data loaded (run setup_sample_data.py first)

use polars_redis::io::infer::{infer_hash_schema, infer_json_schema};
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());

    println!("Connecting to: {}", url);
    println!();

    // =========================================================================
    // Example 1: Infer schema from Redis hashes
    // =========================================================================
    println!("=== Example 1: Infer Hash Schema ===\n");

    let (fields, keys_sampled) = infer_hash_schema(&url, "user:*", 100, true)?;

    println!("Sampled {} keys", keys_sampled);
    println!("Inferred fields:");
    for (name, type_str) in &fields {
        println!("  {:<15} : {}", name, type_str);
    }
    println!();

    // =========================================================================
    // Example 2: Infer schema without type inference (all Utf8)
    // =========================================================================
    println!("=== Example 2: Infer Hash Schema (no type inference) ===\n");

    let (fields, _) = infer_hash_schema(&url, "user:*", 50, false)?;

    println!("Fields (all Utf8):");
    for (name, type_str) in &fields {
        println!("  {:<15} : {}", name, type_str);
    }
    println!();

    // =========================================================================
    // Example 3: Infer schema from RedisJSON documents
    // =========================================================================
    println!("=== Example 3: Infer JSON Schema ===\n");

    let (fields, keys_sampled) = infer_json_schema(&url, "product:*", 100)?;

    println!("Sampled {} keys", keys_sampled);
    println!("Inferred fields:");
    for (name, type_str) in &fields {
        println!("  {:<15} : {}", name, type_str);
    }
    println!();

    // =========================================================================
    // Example 4: Use inferred schema to scan data
    // =========================================================================
    println!("=== Example 4: Scan Using Inferred Schema ===\n");

    use polars_redis::{BatchConfig, HashBatchIterator, HashSchema, RedisType};

    // First infer
    let (fields, _) = infer_hash_schema(&url, "user:*", 50, true)?;

    // Convert to HashSchema
    let schema_fields: Vec<(String, RedisType)> = fields
        .into_iter()
        .map(|(name, type_str)| {
            let redis_type = match type_str.as_str() {
                "int64" => RedisType::Int64,
                "float64" => RedisType::Float64,
                "bool" => RedisType::Boolean,
                "date" => RedisType::Date,
                "datetime" => RedisType::Datetime,
                _ => RedisType::Utf8,
            };
            (name, redis_type)
        })
        .collect();

    let schema = HashSchema::new(schema_fields).with_key(true);

    let config = BatchConfig::new("user:*")
        .with_batch_size(100)
        .with_max_rows(5);

    let mut iterator = HashBatchIterator::new(&url, schema, config, None)?;

    if let Some(batch) = iterator.next_batch()? {
        println!("Scanned {} rows with inferred schema", batch.num_rows());
        println!("Columns:");
        for field in batch.schema().fields() {
            println!("  {:<15} : {:?}", field.name(), field.data_type());
        }
    }

    println!("\n=== All examples complete ===");
    Ok(())
}
