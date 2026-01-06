//! Integration tests that automatically manage Redis containers.
//!
//! These tests use docker-wrapper's ContainerGuard for automatic container
//! lifecycle management. No `#[ignore]` attribute needed - they will
//! automatically start Redis if not available.
//!
//! Run with: `cargo test --test integration_with_container --all-features`

use polars_redis::{
    BatchConfig, HashBatchIterator, HashSchema, RedisType, WriteMode, write_hashes,
};

mod common;
use common::{
    cleanup_keys, redis_cli, redis_cli_output, redis_guard, redis_url_from_guard, setup_test_hashes,
};

// =============================================================================
// Hash Read Tests (Auto-managed Container)
// =============================================================================

/// Test basic hash scanning with automatic container management.
#[tokio::test]
async fn test_hash_scan_basic() {
    let guard = redis_guard().await;
    let url = redis_url_from_guard(guard);

    cleanup_keys("rust:auto:hash:*");
    setup_test_hashes("rust:auto:hash:", 5);

    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
    ])
    .with_key(true);

    let config = BatchConfig::new("rust:auto:hash:*".to_string()).with_batch_size(100);

    // Use spawn_blocking to run sync code that creates its own runtime
    let total_rows = tokio::task::spawn_blocking(move || {
        let mut iterator =
            HashBatchIterator::new(&url, schema, config, None).expect("Failed to create iterator");

        let mut rows = 0;
        while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
            rows += batch.num_rows();
        }
        rows
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(total_rows, 5);

    cleanup_keys("rust:auto:hash:*");
}

/// Test hash scanning with projection.
#[tokio::test]
async fn test_hash_scan_with_projection() {
    let guard = redis_guard().await;
    let url = redis_url_from_guard(guard);

    cleanup_keys("rust:auto:proj:*");
    setup_test_hashes("rust:auto:proj:", 3);

    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
        ("active".to_string(), RedisType::Boolean),
    ])
    .with_key(false); // Don't include key

    let config = BatchConfig::new("rust:auto:proj:*".to_string()).with_batch_size(100);

    // Only project 'name' column
    let projection = Some(vec!["name".to_string()]);

    // Use spawn_blocking to run sync code that creates its own runtime
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

    // Should have only the projected 'name' column
    assert_eq!(num_columns, 1);
    assert_eq!(num_rows, 3);

    cleanup_keys("rust:auto:proj:*");
}

// =============================================================================
// Hash Write Tests (Auto-managed Container)
// =============================================================================

/// Test basic hash writing.
#[tokio::test]
async fn test_hash_write_basic() {
    let guard = redis_guard().await;
    let url = redis_url_from_guard(guard);

    cleanup_keys("rust:auto:write:*");

    let keys = vec![
        "rust:auto:write:1".to_string(),
        "rust:auto:write:2".to_string(),
        "rust:auto:write:3".to_string(),
    ];
    let fields = vec!["name".to_string(), "score".to_string()];
    let values = vec![
        vec![Some("Alice".to_string()), Some("100".to_string())],
        vec![Some("Bob".to_string()), Some("95".to_string())],
        vec![Some("Charlie".to_string()), Some("88".to_string())],
    ];

    // Use spawn_blocking for sync write operation
    let result = tokio::task::spawn_blocking(move || {
        write_hashes(&url, keys, fields, values, None, WriteMode::Replace)
            .expect("Failed to write hashes")
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(result.keys_written, 3);
    assert_eq!(result.keys_failed, 0);

    // Verify data
    let name = redis_cli_output(&["HGET", "rust:auto:write:1", "name"]);
    assert_eq!(name, Some("Alice".to_string()));

    cleanup_keys("rust:auto:write:*");
}

/// Test hash writing with TTL.
#[tokio::test]
async fn test_hash_write_with_ttl() {
    let guard = redis_guard().await;
    let url = redis_url_from_guard(guard);

    cleanup_keys("rust:auto:ttl:*");

    let keys = vec!["rust:auto:ttl:1".to_string()];
    let fields = vec!["data".to_string()];
    let values = vec![vec![Some("test".to_string())]];

    // Use spawn_blocking for sync write operation
    let result = tokio::task::spawn_blocking(move || {
        write_hashes(&url, keys, fields, values, Some(3600), WriteMode::Replace)
            .expect("Failed to write hashes")
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(result.keys_written, 1);

    // Verify TTL was set
    let ttl = redis_cli_output(&["TTL", "rust:auto:ttl:1"]);
    assert!(ttl.is_some());
    let ttl_value: i64 = ttl.unwrap().parse().unwrap_or(0);
    assert!(ttl_value > 0 && ttl_value <= 3600);

    cleanup_keys("rust:auto:ttl:*");
}

/// Test WriteMode::Append.
#[tokio::test]
async fn test_hash_write_append_mode() {
    let guard = redis_guard().await;
    let url = redis_url_from_guard(guard);

    cleanup_keys("rust:auto:append:*");

    // Create initial data
    redis_cli(&["HSET", "rust:auto:append:1", "existing", "data"]);

    let keys = vec!["rust:auto:append:1".to_string()];
    let fields = vec!["new_field".to_string()];
    let values = vec![vec![Some("new_value".to_string())]];

    // Use spawn_blocking for sync write operation
    let result = tokio::task::spawn_blocking(move || {
        write_hashes(&url, keys, fields, values, None, WriteMode::Append)
            .expect("Failed to append hashes")
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(result.keys_written, 1);

    // Both fields should exist
    let existing = redis_cli_output(&["HGET", "rust:auto:append:1", "existing"]);
    assert_eq!(existing, Some("data".to_string()));

    let new_field = redis_cli_output(&["HGET", "rust:auto:append:1", "new_field"]);
    assert_eq!(new_field, Some("new_value".to_string()));

    cleanup_keys("rust:auto:append:*");
}

/// Test WriteMode::Fail skips existing keys.
#[tokio::test]
async fn test_hash_write_fail_mode() {
    let guard = redis_guard().await;
    let url = redis_url_from_guard(guard);

    cleanup_keys("rust:auto:fail:*");

    // Create existing key
    redis_cli(&["HSET", "rust:auto:fail:1", "existing", "data"]);

    let keys = vec![
        "rust:auto:fail:1".to_string(), // exists
        "rust:auto:fail:2".to_string(), // doesn't exist
    ];
    let fields = vec!["name".to_string()];
    let values = vec![
        vec![Some("Attempt1".to_string())],
        vec![Some("Attempt2".to_string())],
    ];

    // Use spawn_blocking for sync write operation
    let result = tokio::task::spawn_blocking(move || {
        write_hashes(&url, keys, fields, values, None, WriteMode::Fail)
            .expect("Failed to write hashes")
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(result.keys_written, 1); // Only key2 written
    assert_eq!(result.keys_skipped, 1); // Key1 skipped

    // Verify key1 wasn't overwritten
    let existing = redis_cli_output(&["HGET", "rust:auto:fail:1", "existing"]);
    assert_eq!(existing, Some("data".to_string()));

    cleanup_keys("rust:auto:fail:*");
}

// =============================================================================
// Edge Case Tests (Auto-managed Container)
// =============================================================================

/// Test scanning with no matching keys.
#[tokio::test]
async fn test_scan_empty_result() {
    let guard = redis_guard().await;
    let url = redis_url_from_guard(guard);

    cleanup_keys("rust:auto:empty:*");

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(true);
    let config = BatchConfig::new("rust:auto:empty:*".to_string()).with_batch_size(100);

    // Use spawn_blocking for sync iterator operation
    let is_empty = tokio::task::spawn_blocking(move || {
        let mut iterator =
            HashBatchIterator::new(&url, schema, config, None).expect("Failed to create iterator");

        let batch = iterator.next_batch().expect("Failed to get batch");
        batch.is_none() || batch.unwrap().num_rows() == 0
    })
    .await
    .expect("spawn_blocking failed");

    assert!(is_empty);
}

/// Test hash with missing fields (sparse data).
#[tokio::test]
async fn test_hash_sparse_data() {
    let guard = redis_guard().await;
    let url = redis_url_from_guard(guard);

    cleanup_keys("rust:auto:sparse:*");

    // Create hashes with different fields
    redis_cli(&["HSET", "rust:auto:sparse:1", "name", "Alice", "age", "30"]);
    redis_cli(&["HSET", "rust:auto:sparse:2", "name", "Bob"]); // missing age
    redis_cli(&["HSET", "rust:auto:sparse:3", "age", "25"]); // missing name

    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
    ])
    .with_key(true);

    let config = BatchConfig::new("rust:auto:sparse:*".to_string()).with_batch_size(100);

    // Use spawn_blocking for sync iterator operation
    let num_rows = tokio::task::spawn_blocking(move || {
        let mut iterator =
            HashBatchIterator::new(&url, schema, config, None).expect("Failed to create iterator");

        let batch = iterator
            .next_batch()
            .expect("Failed to get batch")
            .expect("Expected batch");

        batch.num_rows()
    })
    .await
    .expect("spawn_blocking failed");

    // Should have 3 rows with nulls for missing fields
    assert_eq!(num_rows, 3);

    cleanup_keys("rust:auto:sparse:*");
}

/// Test batch iteration across multiple batches.
#[tokio::test]
async fn test_batch_iteration() {
    let guard = redis_guard().await;
    let url = redis_url_from_guard(guard);

    cleanup_keys("rust:auto:batch:*");
    setup_test_hashes("rust:auto:batch:", 25);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(true);

    // Use small batch size to force multiple batches
    let config = BatchConfig::new("rust:auto:batch:*".to_string()).with_batch_size(10);

    // Use spawn_blocking for sync iterator operation
    let (total_rows, batch_count) = tokio::task::spawn_blocking(move || {
        let mut iterator =
            HashBatchIterator::new(&url, schema, config, None).expect("Failed to create iterator");

        let mut rows = 0;
        let mut batches = 0;
        while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
            rows += batch.num_rows();
            batches += 1;
        }
        (rows, batches)
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(total_rows, 25);
    assert!(batch_count >= 1);

    cleanup_keys("rust:auto:batch:*");
}

// =============================================================================
// Additional Hash Read Tests
// =============================================================================

/// Test hash scanning with max_rows limit.
#[tokio::test]
async fn test_hash_scan_max_rows() {
    let guard = redis_guard().await;
    let url = redis_url_from_guard(guard);

    cleanup_keys("rust:auto:maxrows:*");
    setup_test_hashes("rust:auto:maxrows:", 20);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(true);

    let config = BatchConfig::new("rust:auto:maxrows:*".to_string())
        .with_batch_size(100)
        .with_max_rows(5); // Only get 5 rows

    let total_rows = tokio::task::spawn_blocking(move || {
        let mut iterator =
            HashBatchIterator::new(&url, schema, config, None).expect("Failed to create iterator");

        let mut rows = 0;
        while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
            rows += batch.num_rows();
        }
        rows
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(total_rows, 5);

    cleanup_keys("rust:auto:maxrows:*");
}

/// Test hash scanning with TTL column.
#[tokio::test]
async fn test_hash_scan_with_ttl() {
    let guard = redis_guard().await;
    let url = redis_url_from_guard(guard);

    cleanup_keys("rust:auto:scanttl:*");

    // Create hash with TTL
    redis_cli(&["HSET", "rust:auto:scanttl:1", "name", "test"]);
    redis_cli(&["EXPIRE", "rust:auto:scanttl:1", "3600"]);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)])
        .with_key(true)
        .with_ttl(true)
        .with_ttl_column_name("_ttl".to_string());

    let config = BatchConfig::new("rust:auto:scanttl:*".to_string()).with_batch_size(100);

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

    cleanup_keys("rust:auto:scanttl:*");
}

/// Test hash scanning with row index.
#[tokio::test]
async fn test_hash_scan_with_row_index() {
    let guard = redis_guard().await;
    let url = redis_url_from_guard(guard);

    cleanup_keys("rust:auto:idx:*");
    setup_test_hashes("rust:auto:idx:", 5);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)])
        .with_key(false)
        .with_row_index(true)
        .with_row_index_column_name("_index".to_string());

    let config = BatchConfig::new("rust:auto:idx:*".to_string()).with_batch_size(100);

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

    cleanup_keys("rust:auto:idx:*");
}

/// Test type conversion for different Redis types.
#[tokio::test]
async fn test_hash_type_conversion() {
    let guard = redis_guard().await;
    let url = redis_url_from_guard(guard);

    cleanup_keys("rust:auto:types:*");

    // Create hash with various types
    redis_cli(&[
        "HSET",
        "rust:auto:types:1",
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

    let config = BatchConfig::new("rust:auto:types:*".to_string()).with_batch_size(100);

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

    assert_eq!(num_rows, 1);
    assert_eq!(num_columns, 4);

    cleanup_keys("rust:auto:types:*");
}

/// Test rows_yielded tracking.
#[tokio::test]
async fn test_rows_yielded_tracking() {
    let guard = redis_guard().await;
    let url = redis_url_from_guard(guard);

    cleanup_keys("rust:auto:yielded:*");
    setup_test_hashes("rust:auto:yielded:", 10);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(false);
    let config = BatchConfig::new("rust:auto:yielded:*".to_string()).with_batch_size(100);

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

    cleanup_keys("rust:auto:yielded:*");
}
