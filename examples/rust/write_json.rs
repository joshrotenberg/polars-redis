//! Example: Writing JSON documents to Redis with polars-redis.
//!
//! This example demonstrates how to use the Rust API to write
//! data to Redis as JSON documents.
//!
//! Run with: cargo run --example write_json
//!
//! Prerequisites:
//!     - Redis running on localhost:6379 with RedisJSON module

use polars_redis::write::{WriteMode, write_json};
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());

    println!("Connecting to: {}", url);
    println!();

    // =========================================================================
    // Example 1: Basic JSON writing
    // =========================================================================
    println!("=== Example 1: Basic JSON Writing ===\n");

    let keys = vec![
        "example:doc:1".to_string(),
        "example:doc:2".to_string(),
        "example:doc:3".to_string(),
    ];

    let json_strings = vec![
        r#"{"title": "Hello World", "views": 100, "published": true}"#.to_string(),
        r#"{"title": "Getting Started", "views": 250, "published": true}"#.to_string(),
        r#"{"title": "Draft Post", "views": 0, "published": false}"#.to_string(),
    ];

    let result = write_json(&url, keys, json_strings, None, WriteMode::Replace)?;

    println!("Keys written: {}", result.keys_written);
    println!("Keys failed: {}", result.keys_failed);
    println!("Keys skipped: {}", result.keys_skipped);
    println!();

    // =========================================================================
    // Example 2: Writing with TTL
    // =========================================================================
    println!("=== Example 2: Writing with TTL ===\n");

    let keys = vec!["example:cache:1".to_string(), "example:cache:2".to_string()];

    let json_strings = vec![
        r#"{"data": "cached value 1", "timestamp": 1704067200}"#.to_string(),
        r#"{"data": "cached value 2", "timestamp": 1704067200}"#.to_string(),
    ];

    // TTL of 300 seconds (5 minutes)
    let result = write_json(&url, keys, json_strings, Some(300), WriteMode::Replace)?;

    println!("Keys written with 5min TTL: {}", result.keys_written);
    println!();

    // =========================================================================
    // Example 3: Fail mode (skip existing keys)
    // =========================================================================
    println!("=== Example 3: Fail Mode (Skip Existing) ===\n");

    let keys = vec![
        "example:doc:1".to_string(), // Already exists from Example 1
        "example:doc:4".to_string(), // New key
    ];

    let json_strings = vec![
        r#"{"title": "Updated Title", "views": 999}"#.to_string(),
        r#"{"title": "New Document", "views": 50}"#.to_string(),
    ];

    let result = write_json(&url, keys, json_strings, None, WriteMode::Fail)?;

    println!("Keys written: {}", result.keys_written);
    println!("Keys skipped (already existed): {}", result.keys_skipped);
    println!();

    // =========================================================================
    // Example 4: Nested JSON structures
    // =========================================================================
    println!("=== Example 4: Nested JSON Structures ===\n");

    let keys = vec!["example:complex:1".to_string()];

    let json_strings = vec![
        r#"{
        "user": {
            "name": "Alice",
            "email": "alice@example.com"
        },
        "tags": ["rust", "redis", "polars"],
        "metadata": {
            "created": "2024-01-15",
            "version": 1
        }
    }"#
        .to_string(),
    ];

    let result = write_json(&url, keys, json_strings, None, WriteMode::Replace)?;

    println!("Keys written: {}", result.keys_written);
    println!("(nested structures are stored as-is)");
    println!();

    // =========================================================================
    // Example 5: Programmatic JSON construction
    // =========================================================================
    println!("=== Example 5: Programmatic JSON Construction ===\n");

    use std::collections::HashMap;

    let mut data: Vec<(String, String)> = Vec::new();

    for i in 1..=5 {
        let key = format!("example:product:{}", i);
        let mut product: HashMap<&str, serde_json::Value> = HashMap::new();
        product.insert("id", serde_json::json!(i));
        product.insert("name", serde_json::json!(format!("Product {}", i)));
        product.insert("price", serde_json::json!(i as f64 * 9.99));
        product.insert("in_stock", serde_json::json!(i % 2 == 0));

        let json_str = serde_json::to_string(&product)?;
        data.push((key, json_str));
    }

    let (keys, json_strings): (Vec<_>, Vec<_>) = data.into_iter().unzip();

    let result = write_json(&url, keys, json_strings, None, WriteMode::Replace)?;

    println!("Keys written: {}", result.keys_written);
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
