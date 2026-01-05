//! Integration tests for Redis write operations.
//!
//! These tests require a running Redis instance.
//! Run with: `cargo test --test integration_write --all-features`

use polars_redis::{WriteMode, write_hashes, write_strings};

mod common;
use common::{cleanup_keys, redis_available, redis_cli_output, redis_url};

/// Test basic hash writing.
#[test]
#[ignore] // Requires Redis
fn test_write_hashes_basic() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:write:*");

    let keys = vec![
        "rust:write:1".to_string(),
        "rust:write:2".to_string(),
        "rust:write:3".to_string(),
    ];
    let fields = vec!["name".to_string(), "age".to_string()];
    let values = vec![
        vec![Some("Alice".to_string()), Some("30".to_string())],
        vec![Some("Bob".to_string()), Some("25".to_string())],
        vec![Some("Charlie".to_string()), Some("35".to_string())],
    ];

    let result = write_hashes(&redis_url(), keys, fields, values, None, WriteMode::Replace)
        .expect("Failed to write hashes");

    assert_eq!(result.keys_written, 3);
    assert_eq!(result.keys_failed, 0);
    assert_eq!(result.keys_skipped, 0);

    // Verify data was written
    let name = redis_cli_output(&["HGET", "rust:write:1", "name"]);
    assert_eq!(name, Some("Alice".to_string()));

    let age = redis_cli_output(&["HGET", "rust:write:2", "age"]);
    assert_eq!(age, Some("25".to_string()));

    cleanup_keys("rust:write:*");
}

/// Test hash writing with TTL.
#[test]
#[ignore] // Requires Redis
fn test_write_hashes_with_ttl() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:ttlwrite:*");

    let keys = vec!["rust:ttlwrite:1".to_string()];
    let fields = vec!["name".to_string()];
    let values = vec![vec![Some("Test".to_string())]];

    let result = write_hashes(
        &redis_url(),
        keys,
        fields,
        values,
        Some(3600), // 1 hour TTL
        WriteMode::Replace,
    )
    .expect("Failed to write hashes");

    assert_eq!(result.keys_written, 1);

    // Verify TTL was set
    let ttl = redis_cli_output(&["TTL", "rust:ttlwrite:1"]);
    assert!(ttl.is_some());
    let ttl_value: i64 = ttl.unwrap().parse().unwrap_or(0);
    assert!(ttl_value > 0 && ttl_value <= 3600);

    cleanup_keys("rust:ttlwrite:*");
}

/// Test hash writing with null values.
#[test]
#[ignore] // Requires Redis
fn test_write_hashes_with_nulls() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:nullwrite:*");

    let keys = vec!["rust:nullwrite:1".to_string()];
    let fields = vec!["name".to_string(), "age".to_string()];
    let values = vec![vec![Some("Alice".to_string()), None]]; // age is null

    let result = write_hashes(&redis_url(), keys, fields, values, None, WriteMode::Replace)
        .expect("Failed to write hashes");

    assert_eq!(result.keys_written, 1);

    // Verify name was written but age was not
    let name = redis_cli_output(&["HGET", "rust:nullwrite:1", "name"]);
    assert_eq!(name, Some("Alice".to_string()));

    let age = redis_cli_output(&["HEXISTS", "rust:nullwrite:1", "age"]);
    assert_eq!(age, Some("0".to_string())); // Field doesn't exist

    cleanup_keys("rust:nullwrite:*");
}

/// Test string writing.
#[test]
#[ignore] // Requires Redis
fn test_write_strings_basic() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:strwrite:*");

    let keys = vec!["rust:strwrite:1".to_string(), "rust:strwrite:2".to_string()];
    let values = vec![Some("value1".to_string()), Some("value2".to_string())];

    let result = write_strings(&redis_url(), keys, values, None, WriteMode::Replace)
        .expect("Failed to write strings");

    assert_eq!(result.keys_written, 2);

    // Verify data was written
    let val1 = redis_cli_output(&["GET", "rust:strwrite:1"]);
    assert_eq!(val1, Some("value1".to_string()));

    let val2 = redis_cli_output(&["GET", "rust:strwrite:2"]);
    assert_eq!(val2, Some("value2".to_string()));

    cleanup_keys("rust:strwrite:*");
}

/// Test write mode: Replace.
#[test]
#[ignore] // Requires Redis
fn test_write_mode_replace() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:mode:*");

    // Write initial data
    let keys = vec!["rust:mode:1".to_string()];
    let fields = vec!["name".to_string(), "old_field".to_string()];
    let values = vec![vec![Some("Old".to_string()), Some("data".to_string())]];

    write_hashes(
        &redis_url(),
        keys.clone(),
        fields,
        values,
        None,
        WriteMode::Replace,
    )
    .expect("Failed to write initial data");

    // Replace with new data
    let fields = vec!["name".to_string(), "new_field".to_string()];
    let values = vec![vec![Some("New".to_string()), Some("data".to_string())]];

    let result = write_hashes(&redis_url(), keys, fields, values, None, WriteMode::Replace)
        .expect("Failed to replace data");

    assert_eq!(result.keys_written, 1);

    // Verify old field is gone and new data is there
    let name = redis_cli_output(&["HGET", "rust:mode:1", "name"]);
    assert_eq!(name, Some("New".to_string()));

    let old = redis_cli_output(&["HEXISTS", "rust:mode:1", "old_field"]);
    assert_eq!(old, Some("0".to_string())); // Old field should be gone

    cleanup_keys("rust:mode:*");
}

/// Test write mode: Append.
#[test]
#[ignore] // Requires Redis
fn test_write_mode_append() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:append:*");

    // Write initial data
    let keys = vec!["rust:append:1".to_string()];
    let fields = vec!["name".to_string()];
    let values = vec![vec![Some("Original".to_string())]];

    write_hashes(
        &redis_url(),
        keys.clone(),
        fields,
        values,
        None,
        WriteMode::Replace,
    )
    .expect("Failed to write initial data");

    // Append new field
    let fields = vec!["age".to_string()];
    let values = vec![vec![Some("30".to_string())]];

    let result = write_hashes(&redis_url(), keys, fields, values, None, WriteMode::Append)
        .expect("Failed to append data");

    assert_eq!(result.keys_written, 1);

    // Verify both fields exist
    let name = redis_cli_output(&["HGET", "rust:append:1", "name"]);
    assert_eq!(name, Some("Original".to_string()));

    let age = redis_cli_output(&["HGET", "rust:append:1", "age"]);
    assert_eq!(age, Some("30".to_string()));

    cleanup_keys("rust:append:*");
}

/// Test batch_to_ipc function.
#[test]
#[ignore] // Requires Redis
fn test_batch_to_ipc() {
    use polars_redis::{BatchConfig, HashBatchIterator, HashSchema, RedisType, batch_to_ipc};

    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:ipc:*");

    // Create test data
    common::setup_test_hashes("rust:ipc:", 3);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(true);

    let config = BatchConfig::new("rust:ipc:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected a batch");

    // Convert to IPC
    let ipc_bytes = batch_to_ipc(&batch).expect("Failed to convert to IPC");

    // IPC bytes should be non-empty and start with Arrow magic bytes
    assert!(!ipc_bytes.is_empty());
    assert!(ipc_bytes.len() > 8);

    cleanup_keys("rust:ipc:*");
}
