//! Example: Writing hashes to Redis with polars-redis.
//!
//! This example demonstrates how to use the Rust API to write
//! data to Redis as hashes.
//!
//! Run with: cargo run --example write_hashes
//!
//! Prerequisites:
//!     - Redis running on localhost:6379

use polars_redis::write::{WriteMode, write_hashes};
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());

    println!("Connecting to: {}", url);
    println!();

    // =========================================================================
    // Example 1: Basic hash writing
    // =========================================================================
    println!("=== Example 1: Basic Hash Writing ===\n");

    let keys = vec![
        "example:user:1".to_string(),
        "example:user:2".to_string(),
        "example:user:3".to_string(),
    ];

    let fields = vec!["name".to_string(), "age".to_string(), "city".to_string()];

    let values = vec![
        vec![
            Some("Alice".to_string()),
            Some("30".to_string()),
            Some("NYC".to_string()),
        ],
        vec![
            Some("Bob".to_string()),
            Some("25".to_string()),
            Some("LA".to_string()),
        ],
        vec![
            Some("Carol".to_string()),
            Some("35".to_string()),
            Some("Chicago".to_string()),
        ],
    ];

    let result = write_hashes(&url, keys, fields, values, None, WriteMode::Replace)?;

    println!("Keys written: {}", result.keys_written);
    println!("Keys failed: {}", result.keys_failed);
    println!("Keys skipped: {}", result.keys_skipped);
    println!();

    // =========================================================================
    // Example 2: Writing with TTL
    // =========================================================================
    println!("=== Example 2: Writing with TTL ===\n");

    let keys = vec![
        "example:session:1".to_string(),
        "example:session:2".to_string(),
    ];

    let fields = vec!["user_id".to_string(), "token".to_string()];

    let values = vec![
        vec![Some("user:1".to_string()), Some("abc123".to_string())],
        vec![Some("user:2".to_string()), Some("def456".to_string())],
    ];

    // TTL of 60 seconds
    let result = write_hashes(&url, keys, fields, values, Some(60), WriteMode::Replace)?;

    println!("Keys written with 60s TTL: {}", result.keys_written);
    println!();

    // =========================================================================
    // Example 3: Fail mode (skip existing keys)
    // =========================================================================
    println!("=== Example 3: Fail Mode (Skip Existing) ===\n");

    let keys = vec![
        "example:user:1".to_string(), // Already exists from Example 1
        "example:user:4".to_string(), // New key
    ];

    let fields = vec!["name".to_string(), "age".to_string()];

    let values = vec![
        vec![Some("Alice Updated".to_string()), Some("31".to_string())],
        vec![Some("Dave".to_string()), Some("28".to_string())],
    ];

    let result = write_hashes(&url, keys, fields, values, None, WriteMode::Fail)?;

    println!("Keys written: {}", result.keys_written);
    println!("Keys skipped (already existed): {}", result.keys_skipped);
    println!();

    // =========================================================================
    // Example 4: Append mode (merge fields)
    // =========================================================================
    println!("=== Example 4: Append Mode (Merge Fields) ===\n");

    let keys = vec!["example:user:1".to_string()];

    // Add new field to existing hash
    let fields = vec!["email".to_string()];
    let values = vec![vec![Some("alice@example.com".to_string())]];

    let result = write_hashes(&url, keys, fields, values, None, WriteMode::Append)?;

    println!("Keys updated with new field: {}", result.keys_written);
    println!();

    // =========================================================================
    // Example 5: Handling null values
    // =========================================================================
    println!("=== Example 5: Handling Null Values ===\n");

    let keys = vec!["example:partial:1".to_string()];

    let fields = vec![
        "required".to_string(),
        "optional".to_string(),
        "another".to_string(),
    ];

    // Some values are None - these fields will not be written
    let values = vec![vec![
        Some("present".to_string()),
        None, // This field will be skipped
        Some("also present".to_string()),
    ]];

    let result = write_hashes(&url, keys, fields, values, None, WriteMode::Replace)?;

    println!("Keys written: {}", result.keys_written);
    println!("(null values are skipped, only non-null fields written)");
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
