//! Example: Scanning Redis hashes with polars-redis.
//!
//! This example demonstrates how to use the Rust API directly to scan
//! Redis hashes and convert them to Arrow RecordBatches.
//!
//! Run with: cargo run --example scan_hashes
//!
//! Prerequisites:
//!     - Redis running on localhost:6379
//!     - Sample data loaded (run setup_sample_data.py first)

use polars_redis::{BatchConfig, HashBatchIterator, HashSchema, RedisType};
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Allow override via environment variable
    let url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());

    println!("Connecting to: {}", url);
    println!();

    // =========================================================================
    // Example 1: Basic scanning with full schema
    // =========================================================================
    println!("=== Example 1: Basic Hash Scanning ===\n");

    // Define the schema for user hashes
    // This tells the iterator what fields to expect and how to convert them
    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("email".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
        ("score".to_string(), RedisType::Float64),
        ("active".to_string(), RedisType::Boolean),
    ])
    .with_key(true) // Include the Redis key as a column
    .with_key_column_name("_key"); // Name of the key column

    // Configure the batch iterator
    let config = BatchConfig::new("user:*") // Pattern to match keys
        .with_batch_size(50) // Rows per Arrow batch
        .with_count_hint(100); // SCAN COUNT hint to Redis

    // Create the iterator
    let mut iterator = HashBatchIterator::new(&url, schema, config, None)?;

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
                println!("  {:<12} : {:?}", field.name(), field.data_type());
            }
            println!();
        }
    }

    println!("Total: {} rows in {} batches\n", total_rows, batch_count);

    // =========================================================================
    // Example 2: Projection pushdown - only fetch specific fields
    // =========================================================================
    println!("=== Example 2: Projection Pushdown (name, age only) ===\n");

    // Even though we define the full schema, we only request specific columns.
    // The iterator will use HMGET instead of HGETALL, reducing data transfer.
    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("email".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
        ("score".to_string(), RedisType::Float64),
        ("active".to_string(), RedisType::Boolean),
    ])
    .with_key(false); // Don't include key this time

    let config = BatchConfig::new("user:*").with_batch_size(100);

    // Request only these columns - this triggers HMGET optimization
    let projection = Some(vec!["name".to_string(), "age".to_string()]);

    let mut iterator = HashBatchIterator::new(&url, schema, config, projection)?;

    if let Some(batch) = iterator.next_batch()? {
        println!("Projected columns (only these were fetched from Redis):");
        for field in batch.schema().fields() {
            println!("  {}", field.name());
        }
        println!("\nFirst batch: {} rows", batch.num_rows());
    }
    println!();

    // =========================================================================
    // Example 3: Row limit - stop early
    // =========================================================================
    println!("=== Example 3: Row Limit (max 10 rows) ===\n");

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(true);

    let config = BatchConfig::new("user:*")
        .with_batch_size(100)
        .with_max_rows(10); // Stop after 10 rows total

    let mut iterator = HashBatchIterator::new(&url, schema, config, None)?;

    let mut total = 0;
    while let Some(batch) = iterator.next_batch()? {
        total += batch.num_rows();
        println!("Batch: {} rows (cumulative: {})", batch.num_rows(), total);
    }
    println!("\nTotal rows with limit: {} (requested max: 10)\n", total);

    // =========================================================================
    // Example 4: Different key patterns
    // =========================================================================
    println!("=== Example 4: Different Key Patterns ===\n");

    let patterns = vec![
        "user:*",    // All users
        "user:1*",   // Users starting with 1
        "session:*", // All sessions (may not exist)
    ];

    for pattern in patterns {
        let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]);
        let config = BatchConfig::new(pattern).with_batch_size(1000);

        let mut iterator = HashBatchIterator::new(&url, schema.clone(), config, None)?;

        let mut count = 0;
        while let Some(batch) = iterator.next_batch()? {
            count += batch.num_rows();
        }

        println!("Pattern '{}': {} keys found", pattern, count);
    }
    println!();

    // =========================================================================
    // Example 5: Serialize batch to Arrow IPC (for Python interop)
    // =========================================================================
    println!("=== Example 5: Arrow IPC Serialization ===\n");

    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
    ])
    .with_key(true);

    let config = BatchConfig::new("user:*")
        .with_batch_size(10)
        .with_max_rows(10);

    let mut iterator = HashBatchIterator::new(&url, schema, config, None)?;

    if let Some(batch) = iterator.next_batch()? {
        // Serialize to Arrow IPC format (what we'd send to Python)
        let ipc_bytes = polars_redis::batch_to_ipc(&batch)?;
        println!(
            "Serialized {} rows to {} bytes of Arrow IPC",
            batch.num_rows(),
            ipc_bytes.len()
        );

        // This is what Python would do:
        // df = pl.read_ipc(io.BytesIO(ipc_bytes))
    }

    println!("\n=== All examples complete ===");
    Ok(())
}
