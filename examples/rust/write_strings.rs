//! Example: Writing strings to Redis with polars-redis.
//!
//! This example demonstrates how to use the Rust API to write
//! data to Redis as string values.
//!
//! Run with: cargo run --example write_strings
//!
//! Prerequisites:
//!     - Redis running on localhost:6379

use polars_redis::write::{WriteMode, write_strings};
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());

    println!("Connecting to: {}", url);
    println!();

    // =========================================================================
    // Example 1: Basic string writing
    // =========================================================================
    println!("=== Example 1: Basic String Writing ===\n");

    let keys = vec![
        "example:counter:1".to_string(),
        "example:counter:2".to_string(),
        "example:counter:3".to_string(),
    ];

    let values = vec![
        Some("100".to_string()),
        Some("200".to_string()),
        Some("300".to_string()),
    ];

    let result = write_strings(&url, keys, values, None, WriteMode::Replace)?;

    println!("Keys written: {}", result.keys_written);
    println!("Keys failed: {}", result.keys_failed);
    println!("Keys skipped: {}", result.keys_skipped);
    println!();

    // =========================================================================
    // Example 2: Writing with TTL
    // =========================================================================
    println!("=== Example 2: Writing with TTL ===\n");

    let keys = vec!["example:temp:1".to_string(), "example:temp:2".to_string()];

    let values = vec![
        Some("temporary value 1".to_string()),
        Some("temporary value 2".to_string()),
    ];

    // TTL of 120 seconds (2 minutes)
    let result = write_strings(&url, keys, values, Some(120), WriteMode::Replace)?;

    println!("Keys written with 2min TTL: {}", result.keys_written);
    println!();

    // =========================================================================
    // Example 3: Fail mode (skip existing keys)
    // =========================================================================
    println!("=== Example 3: Fail Mode (Skip Existing) ===\n");

    let keys = vec![
        "example:counter:1".to_string(), // Already exists from Example 1
        "example:counter:4".to_string(), // New key
    ];

    let values = vec![Some("999".to_string()), Some("400".to_string())];

    let result = write_strings(&url, keys, values, None, WriteMode::Fail)?;

    println!("Keys written: {}", result.keys_written);
    println!("Keys skipped (already existed): {}", result.keys_skipped);
    println!();

    // =========================================================================
    // Example 4: Handling null values
    // =========================================================================
    println!("=== Example 4: Handling Null Values ===\n");

    let keys = vec![
        "example:value:1".to_string(),
        "example:value:2".to_string(),
        "example:value:3".to_string(),
    ];

    // Some values are None - these keys will not be written
    let values = vec![
        Some("present".to_string()),
        None, // This key will be skipped
        Some("also present".to_string()),
    ];

    let result = write_strings(&url, keys, values, None, WriteMode::Replace)?;

    println!("Keys written: {}", result.keys_written);
    println!("(null values are skipped)");
    println!();

    // =========================================================================
    // Example 5: Writing various data types as strings
    // =========================================================================
    println!("=== Example 5: Various Data Types as Strings ===\n");

    let keys = vec![
        "example:int".to_string(),
        "example:float".to_string(),
        "example:bool".to_string(),
        "example:json".to_string(),
    ];

    let values = vec![
        Some("42".to_string()),
        Some("3.14159".to_string()),
        Some("true".to_string()),
        Some(r#"{"nested": "data"}"#.to_string()),
    ];

    let result = write_strings(&url, keys, values, None, WriteMode::Replace)?;

    println!("Keys written: {}", result.keys_written);
    println!("(all values stored as strings, can be parsed on read)");
    println!();

    // =========================================================================
    // Example 6: Batch writing
    // =========================================================================
    println!("=== Example 6: Batch Writing ===\n");

    let count = 100;
    let keys: Vec<String> = (0..count).map(|i| format!("example:batch:{}", i)).collect();
    let values: Vec<Option<String>> = (0..count).map(|i| Some(format!("value_{}", i))).collect();

    let result = write_strings(&url, keys, values, None, WriteMode::Replace)?;

    println!("Batch size: {}", count);
    println!("Keys written: {}", result.keys_written);
    println!("(writes are automatically pipelined for performance)");
    println!();

    // Cleanup
    println!("=== Cleanup ===\n");
    cleanup_example_keys(&url)?;
    println!("Example keys deleted");

    println!("\n=== All examples complete ===");
    Ok(())
}

fn cleanup_example_keys(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = redis::Client::open(url)?;
    let mut conn = client.get_connection()?;

    let keys: Vec<String> = redis::cmd("KEYS")
        .arg("example:*")
        .query(&mut conn)
        .unwrap_or_default();

    if !keys.is_empty() {
        redis::cmd("DEL").arg(&keys).query::<()>(&mut conn).ok();
    }

    Ok(())
}
