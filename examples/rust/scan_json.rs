//! Example: Scanning Redis JSON documents with polars-redis.
//!
//! This example demonstrates how to use the Rust API directly to scan
//! RedisJSON documents and convert them to Arrow RecordBatches.
//!
//! Run with: cargo run --example scan_json
//!
//! Prerequisites:
//!     - Redis 8+ running on localhost:6379 (has native JSON support)
//!     - Sample data loaded (run setup_sample_data.py first)

use arrow::datatypes::DataType;
use polars_redis::{BatchConfig, JsonBatchIterator, JsonSchema};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = "redis://localhost:6379";

    // Define the schema for product JSON documents
    let schema = JsonSchema::new(vec![
        ("name".to_string(), DataType::Utf8),
        ("category".to_string(), DataType::Utf8),
        ("price".to_string(), DataType::Float64),
        ("quantity".to_string(), DataType::Int64),
        ("in_stock".to_string(), DataType::Boolean),
    ])
    .with_key(true)
    .with_key_column_name("_key");

    // Configure the batch iterator
    let config = BatchConfig::new("product:*")
        .with_batch_size(10)
        .with_count_hint(50);

    // Create the iterator
    let mut iterator = JsonBatchIterator::new(url, schema, config, None)?;

    println!("=== Scanning Redis JSON Documents ===\n");

    let mut total_rows = 0;
    let mut batch_count = 0;

    // Iterate over batches
    while let Some(batch) = iterator.next_batch()? {
        batch_count += 1;
        let num_rows = batch.num_rows();
        total_rows += num_rows;

        println!("Batch {}: {} rows", batch_count, num_rows);

        // Print schema on first batch
        if batch_count == 1 {
            println!("\nSchema:");
            for field in batch.schema().fields() {
                println!("  {} : {:?}", field.name(), field.data_type());
            }
            println!();
        }
    }

    println!("\nTotal: {} rows in {} batches", total_rows, batch_count);

    // Example with projection
    println!("\n=== With Projection (name, price only) ===\n");

    let schema = JsonSchema::new(vec![
        ("name".to_string(), DataType::Utf8),
        ("category".to_string(), DataType::Utf8),
        ("price".to_string(), DataType::Float64),
        ("quantity".to_string(), DataType::Int64),
        ("in_stock".to_string(), DataType::Boolean),
    ])
    .with_key(false);

    let config = BatchConfig::new("product:*").with_batch_size(50);

    let projection = Some(vec!["name".to_string(), "price".to_string()]);
    let mut iterator = JsonBatchIterator::new(url, schema, config, projection)?;

    if let Some(batch) = iterator.next_batch()? {
        println!("Projected columns:");
        for field in batch.schema().fields() {
            println!("  {}", field.name());
        }
        println!("\nFirst batch: {} rows", batch.num_rows());
    }

    // Example: Access column data
    println!("\n=== Accessing Column Data ===\n");

    let schema = JsonSchema::new(vec![
        ("name".to_string(), DataType::Utf8),
        ("price".to_string(), DataType::Float64),
    ])
    .with_key(true);

    let config = BatchConfig::new("product:*").with_batch_size(100);

    let mut iterator = JsonBatchIterator::new(url, schema, config, None)?;

    if let Some(batch) = iterator.next_batch()? {
        // Access the price column
        let price_col = batch
            .column_by_name("price")
            .expect("price column should exist");

        let prices = price_col
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("should be Float64Array");

        let total: f64 = prices.iter().flatten().sum();
        let count = prices.len();

        println!("Products: {}", count);
        println!("Total price sum: ${:.2}", total);
        println!("Average price: ${:.2}", total / count as f64);
    }

    Ok(())
}
