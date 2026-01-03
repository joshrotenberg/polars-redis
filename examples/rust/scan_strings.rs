//! Example: Scanning Redis strings with polars-redis.
//!
//! This example demonstrates how to use the Rust API to scan
//! Redis string values and convert them to Arrow RecordBatches.
//!
//! Run with: cargo run --example scan_strings
//!
//! Prerequisites:
//!     - Redis running on localhost:6379
//!     - Sample data loaded (run setup_sample_data.py first)

use polars_redis::{BatchConfig, RedisType, StringBatchIterator, StringSchema};
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());

    println!("Connecting to: {}", url);
    println!();

    // =========================================================================
    // Example 1: Scan string values as UTF-8
    // =========================================================================
    println!("=== Example 1: Scan Strings as UTF-8 ===\n");

    let schema = StringSchema::new(RedisType::Utf8)
        .with_key(true)
        .with_key_column_name("_key")
        .with_value_column_name("value");

    let config = BatchConfig::new("cache:*").with_batch_size(100);

    let mut iterator = StringBatchIterator::new(&url, schema, config)?;

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch()? {
        total_rows += batch.num_rows();

        if total_rows <= batch.num_rows() {
            println!("Schema:");
            for field in batch.schema().fields() {
                println!("  {:<12} : {:?}", field.name(), field.data_type());
            }
            println!();
        }
    }
    println!("Total cache entries: {}\n", total_rows);

    // =========================================================================
    // Example 2: Scan counters as integers
    // =========================================================================
    println!("=== Example 2: Scan Counters as Int64 ===\n");

    let schema = StringSchema::new(RedisType::Int64)
        .with_key(true)
        .with_value_column_name("count");

    let config = BatchConfig::new("counter:*").with_batch_size(100);

    let mut iterator = StringBatchIterator::new(&url, schema, config)?;

    let mut total: i64 = 0;
    let mut count = 0;

    while let Some(batch) = iterator.next_batch()? {
        count += batch.num_rows();

        // Sum the counter values
        let value_col = batch
            .column_by_name("count")
            .expect("count column should exist");

        let values = value_col
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("should be Int64Array");

        total += values.iter().flatten().sum::<i64>();
    }

    println!("Counters found: {}", count);
    println!("Sum of all counters: {}\n", total);

    // =========================================================================
    // Example 3: Scan float values
    // =========================================================================
    println!("=== Example 3: Scan Float Values ===\n");

    let schema = StringSchema::new(RedisType::Float64)
        .with_key(true)
        .with_value_column_name("score");

    let config = BatchConfig::new("score:*").with_batch_size(100);

    let mut iterator = StringBatchIterator::new(&url, schema, config)?;

    let mut sum: f64 = 0.0;
    let mut count = 0;

    while let Some(batch) = iterator.next_batch()? {
        count += batch.num_rows();

        let value_col = batch
            .column_by_name("score")
            .expect("score column should exist");

        let values = value_col
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("should be Float64Array");

        sum += values.iter().flatten().sum::<f64>();
    }

    if count > 0 {
        println!("Scores found: {}", count);
        println!("Average score: {:.2}\n", sum / count as f64);
    } else {
        println!("No score keys found\n");
    }

    // =========================================================================
    // Example 4: Without key column
    // =========================================================================
    println!("=== Example 4: Values Only (no key column) ===\n");

    let schema = StringSchema::new(RedisType::Utf8)
        .with_key(false)
        .with_value_column_name("data");

    let config = BatchConfig::new("cache:*")
        .with_batch_size(10)
        .with_max_rows(5);

    let mut iterator = StringBatchIterator::new(&url, schema, config)?;

    if let Some(batch) = iterator.next_batch()? {
        println!("Columns (key excluded):");
        for field in batch.schema().fields() {
            println!("  {}", field.name());
        }
        println!("\nRows: {}", batch.num_rows());
    }

    println!("\n=== All examples complete ===");
    Ok(())
}
