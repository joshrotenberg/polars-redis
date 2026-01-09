//! Integration tests for Redis hash operations.
//!
//! These tests automatically detect and use Redis:
//! - Uses REDIS_URL env var if set (CI)
//! - Uses existing Redis on port 6379 (docker-compose)
//! - Auto-starts container via docker-wrapper if needed
//!
//! Run with: `cargo test --test integration_hash --features "json,search"`

use polars_redis::{BatchConfig, HashBatchIterator, HashSchema, RedisType};

mod common;
use common::{cleanup_keys, ensure_redis, redis_cli, setup_test_hashes};

/// Test basic hash scanning with explicit schema.
#[tokio::test]
async fn test_scan_hashes_basic() {
    let url = ensure_redis().await.to_string();

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

    let (total_rows, is_done) = tokio::task::spawn_blocking(move || {
        let mut iterator =
            HashBatchIterator::new(&url, schema, config, None).expect("Failed to create iterator");

        let mut total_rows = 0;
        while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
            total_rows += batch.num_rows();
            assert!(batch.num_columns() >= 3); // name, age, active (+ _key)
        }

        (total_rows, iterator.is_done())
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(total_rows, 10);
    assert!(is_done);

    // Cleanup
    cleanup_keys("rust:hash:*");
}

/// Test hash scanning with projection (subset of fields).
#[tokio::test]
async fn test_scan_hashes_with_projection() {
    let url = ensure_redis().await.to_string();

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

    let (num_columns, num_rows) = tokio::task::spawn_blocking(move || {
        let mut iterator = HashBatchIterator::new(&url, schema, config, projection)
            .expect("Failed to create iterator");

        let batch = iterator
            .next_batch()
            .expect("Failed to get batch")
            .expect("Expected a batch");

        (batch.num_columns(), batch.num_rows())
    })
    .await
    .expect("spawn_blocking failed");

    // Should only have the projected column
    assert_eq!(num_columns, 1);
    assert_eq!(num_rows, 5);

    cleanup_keys("rust:proj:*");
}

/// Test hash scanning with no matching keys.
#[tokio::test]
async fn test_scan_hashes_no_matches() {
    let url = ensure_redis().await.to_string();

    let schema = HashSchema::new(vec![("field".to_string(), RedisType::Utf8)]).with_key(false);

    let config = BatchConfig::new("nonexistent:pattern:*".to_string()).with_batch_size(100);

    let (is_empty, is_done) = tokio::task::spawn_blocking(move || {
        let mut iterator =
            HashBatchIterator::new(&url, schema, config, None).expect("Failed to create iterator");

        let batch = iterator.next_batch().expect("Failed to get batch");
        let is_empty = batch.is_none() || batch.unwrap().num_rows() == 0;

        (is_empty, iterator.is_done())
    })
    .await
    .expect("spawn_blocking failed");

    assert!(is_empty);
    assert!(is_done);
}

/// Test hash scanning with max_rows limit.
#[tokio::test]
async fn test_scan_hashes_max_rows() {
    let url = ensure_redis().await.to_string();

    cleanup_keys("rust:maxrows:*");
    setup_test_hashes("rust:maxrows:", 20);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(true);

    let config = BatchConfig::new("rust:maxrows:*".to_string())
        .with_batch_size(100)
        .with_max_rows(5); // Only get 5 rows

    let total_rows = tokio::task::spawn_blocking(move || {
        let mut iterator =
            HashBatchIterator::new(&url, schema, config, None).expect("Failed to create iterator");

        let mut total_rows = 0;
        while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
            total_rows += batch.num_rows();
        }
        total_rows
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(total_rows, 5);

    cleanup_keys("rust:maxrows:*");
}

/// Test hash scanning with small batch size.
#[tokio::test]
async fn test_scan_hashes_small_batches() {
    let url = ensure_redis().await.to_string();

    cleanup_keys("rust:batch:*");
    setup_test_hashes("rust:batch:", 15);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(false);

    let config = BatchConfig::new("rust:batch:*".to_string())
        .with_batch_size(3) // Very small batch
        .with_count_hint(3);

    let (batch_count, total_rows) = tokio::task::spawn_blocking(move || {
        let mut iterator =
            HashBatchIterator::new(&url, schema, config, None).expect("Failed to create iterator");

        let mut batch_count = 0;
        let mut total_rows = 0;
        while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
            batch_count += 1;
            total_rows += batch.num_rows();
        }
        (batch_count, total_rows)
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(total_rows, 15);
    assert!(batch_count >= 5); // Should have multiple batches

    cleanup_keys("rust:batch:*");
}

/// Test hash scanning with TTL column.
#[tokio::test]
async fn test_scan_hashes_with_ttl() {
    let url = ensure_redis().await.to_string();

    cleanup_keys("rust:ttl:*");

    // Create hash with TTL
    redis_cli(&["HSET", "rust:ttl:1", "name", "test"]);
    redis_cli(&["EXPIRE", "rust:ttl:1", "3600"]);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)])
        .with_key(true)
        .with_ttl(true)
        .with_ttl_column_name("_ttl".to_string());

    let config = BatchConfig::new("rust:ttl:*".to_string()).with_batch_size(100);

    let (num_columns, num_rows) = tokio::task::spawn_blocking(move || {
        let mut iterator =
            HashBatchIterator::new(&url, schema, config, None).expect("Failed to create iterator");

        let batch = iterator
            .next_batch()
            .expect("Failed to get batch")
            .expect("Expected a batch");

        (batch.num_columns(), batch.num_rows())
    })
    .await
    .expect("spawn_blocking failed");

    // Should have name, _key, _ttl columns
    assert!(num_columns >= 3);
    assert_eq!(num_rows, 1);

    cleanup_keys("rust:ttl:*");
}

/// Test hash scanning with row index.
#[tokio::test]
async fn test_scan_hashes_with_row_index() {
    let url = ensure_redis().await.to_string();

    cleanup_keys("rust:idx:*");
    setup_test_hashes("rust:idx:", 5);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)])
        .with_key(false)
        .with_row_index(true)
        .with_row_index_column_name("_index".to_string());

    let config = BatchConfig::new("rust:idx:*".to_string()).with_batch_size(100);

    let (num_columns, num_rows) = tokio::task::spawn_blocking(move || {
        let mut iterator =
            HashBatchIterator::new(&url, schema, config, None).expect("Failed to create iterator");

        let batch = iterator
            .next_batch()
            .expect("Failed to get batch")
            .expect("Expected a batch");

        (batch.num_columns(), batch.num_rows())
    })
    .await
    .expect("spawn_blocking failed");

    // Should have name and _index columns
    assert_eq!(num_columns, 2);
    assert_eq!(num_rows, 5);

    cleanup_keys("rust:idx:*");
}

/// Test type conversion for different Redis types.
#[tokio::test]
async fn test_scan_hashes_type_conversion() {
    let url = ensure_redis().await.to_string();

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

    let (num_rows, num_columns) = tokio::task::spawn_blocking(move || {
        let mut iterator =
            HashBatchIterator::new(&url, schema, config, None).expect("Failed to create iterator");

        let batch = iterator
            .next_batch()
            .expect("Failed to get batch")
            .expect("Expected a batch");

        (batch.num_rows(), batch.num_columns())
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(num_rows, 1);
    assert_eq!(num_columns, 4);

    cleanup_keys("rust:types:*");
}

/// Test handling of missing fields (should be null).
#[tokio::test]
async fn test_scan_hashes_missing_fields() {
    let url = ensure_redis().await.to_string();

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

    let (num_rows, num_columns) = tokio::task::spawn_blocking(move || {
        let mut iterator =
            HashBatchIterator::new(&url, schema, config, None).expect("Failed to create iterator");

        let batch = iterator
            .next_batch()
            .expect("Failed to get batch")
            .expect("Expected a batch");

        (batch.num_rows(), batch.num_columns())
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(num_rows, 1);
    assert_eq!(num_columns, 2);
    // The 'age' column should exist but have null value

    cleanup_keys("rust:missing:*");
}

/// Test rows_yielded tracking.
#[tokio::test]
async fn test_scan_hashes_rows_yielded() {
    let url = ensure_redis().await.to_string();

    cleanup_keys("rust:yielded:*");
    setup_test_hashes("rust:yielded:", 10);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(false);

    let config = BatchConfig::new("rust:yielded:*".to_string()).with_batch_size(100);

    let rows_yielded = tokio::task::spawn_blocking(move || {
        let mut iterator =
            HashBatchIterator::new(&url, schema, config, None).expect("Failed to create iterator");

        assert_eq!(iterator.rows_yielded(), 0);

        while iterator
            .next_batch()
            .expect("Failed to get batch")
            .is_some()
        {}

        iterator.rows_yielded()
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(rows_yielded, 10);

    cleanup_keys("rust:yielded:*");
}
