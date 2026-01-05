//! Integration tests for Redis hash operations.
//!
//! These tests require a running Redis instance.
//! By default, tests connect to localhost:16379 (started via docker-wrapper).
//!
//! Run with: `cargo test --test integration_hash --all-features`
//! Run ignored tests: `cargo test --test integration_hash --all-features -- --ignored`

use polars_redis::{BatchConfig, HashBatchIterator, HashSchema, RedisType};

mod common;
use common::{cleanup_keys, redis_available, redis_cli, redis_url, setup_test_hashes};

/// Test basic hash scanning with explicit schema.
#[test]
#[ignore] // Requires Redis
fn test_scan_hashes_basic() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    // Setup test data
    cleanup_keys("rust:hash:*");
    setup_test_hashes("rust:hash:", 10);

    // Create schema
    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
        ("active".to_string(), RedisType::Boolean),
    ])
    .with_key(true)
    .with_key_column_name("_key".to_string());

    // Create config
    let config = BatchConfig::new("rust:hash:*".to_string())
        .with_batch_size(100)
        .with_count_hint(50);

    // Create iterator
    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    // Collect all batches
    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
        assert!(batch.num_columns() >= 3); // name, age, active (+ _key)
    }

    assert_eq!(total_rows, 10);
    assert!(iterator.is_done());

    // Cleanup
    cleanup_keys("rust:hash:*");
}

/// Test hash scanning with projection (subset of fields).
#[test]
#[ignore] // Requires Redis
fn test_scan_hashes_with_projection() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:proj:*");
    setup_test_hashes("rust:proj:", 5);

    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
        ("active".to_string(), RedisType::Boolean),
    ])
    .with_key(false); // Don't include key

    let config = BatchConfig::new("rust:proj:*".to_string()).with_batch_size(100);

    // Only request 'name' field
    let projection = Some(vec!["name".to_string()]);
    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, projection)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected a batch");

    // Should only have the projected column
    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.num_rows(), 5);

    cleanup_keys("rust:proj:*");
}

/// Test hash scanning with no matching keys.
#[test]
#[ignore] // Requires Redis
fn test_scan_hashes_no_matches() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let schema = HashSchema::new(vec![("field".to_string(), RedisType::Utf8)]).with_key(false);

    let config = BatchConfig::new("nonexistent:pattern:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    // Should return None immediately
    let batch = iterator.next_batch().expect("Failed to get batch");
    assert!(batch.is_none() || batch.unwrap().num_rows() == 0);
    assert!(iterator.is_done());
}

/// Test hash scanning with max_rows limit.
#[test]
#[ignore] // Requires Redis
fn test_scan_hashes_max_rows() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:maxrows:*");
    setup_test_hashes("rust:maxrows:", 20);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(true);

    let config = BatchConfig::new("rust:maxrows:*".to_string())
        .with_batch_size(100)
        .with_max_rows(5); // Only get 5 rows

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 5);

    cleanup_keys("rust:maxrows:*");
}

/// Test hash scanning with small batch size.
#[test]
#[ignore] // Requires Redis
fn test_scan_hashes_small_batches() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:batch:*");
    setup_test_hashes("rust:batch:", 15);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(false);

    let config = BatchConfig::new("rust:batch:*".to_string())
        .with_batch_size(3) // Very small batch
        .with_count_hint(3);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let mut batch_count = 0;
    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        batch_count += 1;
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 15);
    assert!(batch_count >= 5); // Should have multiple batches

    cleanup_keys("rust:batch:*");
}

/// Test hash scanning with TTL column.
#[test]
#[ignore] // Requires Redis
fn test_scan_hashes_with_ttl() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:ttl:*");

    // Create hash with TTL
    redis_cli(&["HSET", "rust:ttl:1", "name", "test"]);
    redis_cli(&["EXPIRE", "rust:ttl:1", "3600"]);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)])
        .with_key(true)
        .with_ttl(true)
        .with_ttl_column_name("_ttl".to_string());

    let config = BatchConfig::new("rust:ttl:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected a batch");

    // Should have name, _key, _ttl columns
    assert!(batch.num_columns() >= 3);
    assert_eq!(batch.num_rows(), 1);

    cleanup_keys("rust:ttl:*");
}

/// Test hash scanning with row index.
#[test]
#[ignore] // Requires Redis
fn test_scan_hashes_with_row_index() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:idx:*");
    setup_test_hashes("rust:idx:", 5);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)])
        .with_key(false)
        .with_row_index(true)
        .with_row_index_column_name("_index".to_string());

    let config = BatchConfig::new("rust:idx:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected a batch");

    // Should have name and _index columns
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.num_rows(), 5);

    cleanup_keys("rust:idx:*");
}

/// Test type conversion for different Redis types.
#[test]
#[ignore] // Requires Redis
fn test_scan_hashes_type_conversion() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:types:*");

    // Create hash with various types
    redis_cli(&[
        "HSET",
        "rust:types:1",
        "str_field",
        "hello",
        "int_field",
        "42",
        "float_field",
        "3.14",
        "bool_field",
        "true",
    ]);

    let schema = HashSchema::new(vec![
        ("str_field".to_string(), RedisType::Utf8),
        ("int_field".to_string(), RedisType::Int64),
        ("float_field".to_string(), RedisType::Float64),
        ("bool_field".to_string(), RedisType::Boolean),
    ])
    .with_key(false);

    let config = BatchConfig::new("rust:types:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected a batch");

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 4);

    cleanup_keys("rust:types:*");
}

/// Test handling of missing fields (should be null).
#[test]
#[ignore] // Requires Redis
fn test_scan_hashes_missing_fields() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:missing:*");

    // Create hash with only some fields
    redis_cli(&["HSET", "rust:missing:1", "name", "Alice"]);
    // Missing 'age' field

    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64), // This field doesn't exist
    ])
    .with_key(false);

    let config = BatchConfig::new("rust:missing:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected a batch");

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 2);
    // The 'age' column should exist but have null value

    cleanup_keys("rust:missing:*");
}

/// Test rows_yielded tracking.
#[test]
#[ignore] // Requires Redis
fn test_scan_hashes_rows_yielded() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:yielded:*");
    setup_test_hashes("rust:yielded:", 10);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(false);

    let config = BatchConfig::new("rust:yielded:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    assert_eq!(iterator.rows_yielded(), 0);

    while iterator
        .next_batch()
        .expect("Failed to get batch")
        .is_some()
    {}

    assert_eq!(iterator.rows_yielded(), 10);

    cleanup_keys("rust:yielded:*");
}
