//! Failure mode and error recovery tests.
//!
//! These tests verify graceful error handling, resource cleanup, and recovery:
//! - Connection drop simulation
//! - Resource cleanup verification
//! - Partial failure recovery
//! - Error message quality
//! - Index-not-found handling
//!
//! Run with: `cargo test --test failure_mode_tests --all-features`

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use polars_redis::{
    BatchConfig, HashBatchIterator, HashSchema, RedisType, WriteMode, write_hashes,
};

mod common;
use common::{cleanup_keys, redis_available, redis_cli, redis_cli_output, redis_url};

// =============================================================================
// Error Message Quality Tests
// =============================================================================

/// Test that connection errors have helpful messages.
#[test]
fn test_connection_error_message_quality() {
    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]);
    let config = BatchConfig::new("test:*".to_string());

    let result = HashBatchIterator::new("redis://invalid-host-12345:6379", schema, config, None);

    match result {
        Ok(_) => panic!("Expected connection error"),
        Err(err) => {
            let err_msg = err.to_string().to_lowercase();
            // Error should mention connection or host
            assert!(
                err_msg.contains("connection")
                    || err_msg.contains("redis")
                    || err_msg.contains("error"),
                "Error message should be informative: {}",
                err_msg
            );
        }
    }
}

/// Test that invalid URL errors are clear.
#[test]
fn test_invalid_url_error_message() {
    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]);
    let config = BatchConfig::new("test:*".to_string());

    let result = HashBatchIterator::new("completely-invalid-url-format", schema, config, None);

    match result {
        Ok(_) => panic!("Expected URL error"),
        Err(err) => {
            let err_msg = err.to_string();
            // Error should indicate URL problem
            assert!(
                err_msg.contains("URL") || err_msg.contains("url") || err_msg.contains("format"),
                "Error message should mention URL issue: {}",
                err_msg
            );
        }
    }
}

/// Test type conversion error messages.
#[test]
#[ignore] // Requires Redis
fn test_type_conversion_error_message() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:failmsg:typeconv:*");

    // Set up hash with non-numeric value
    redis_cli(&["HSET", "rust:failmsg:typeconv:1", "age", "not_a_number"]);

    // Try to read as Int64
    let schema = HashSchema::new(vec![("age".to_string(), RedisType::Int64)]);
    let config = BatchConfig::new("rust:failmsg:typeconv:*".to_string());

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let result = iterator.next_batch();
    assert!(result.is_err(), "Should fail on type conversion");

    let err = result.unwrap_err();
    let err_msg = err.to_string();

    // Error should mention type or conversion
    assert!(
        err_msg.contains("type") || err_msg.contains("conversion") || err_msg.contains("parse"),
        "Error should mention type conversion: {}",
        err_msg
    );

    cleanup_keys("rust:failmsg:typeconv:*");
}

// =============================================================================
// Resource Cleanup Tests
// =============================================================================

/// Test that iterator properly drops resources.
#[test]
#[ignore] // Requires Redis
fn test_iterator_drop_cleanup() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:cleanup:iter:*");

    // Set up test data
    for i in 1..=10 {
        redis_cli(&[
            "HSET",
            &format!("rust:cleanup:iter:{}", i),
            "name",
            &format!("User{}", i),
        ]);
    }

    // Create and drop iterator multiple times
    for _ in 0..5 {
        let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]);
        let config = BatchConfig::new("rust:cleanup:iter:*".to_string());

        let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
            .expect("Failed to create iterator");

        // Read only one batch (partial iteration)
        let _ = iterator.next_batch();

        // Iterator drops here
    }

    // If resources weren't cleaned up, we'd have issues creating new iterators
    // This test passes if we can create iterators without issues
    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]);
    let config = BatchConfig::new("rust:cleanup:iter:*".to_string());

    let iterator = HashBatchIterator::new(&redis_url(), schema, config, None);
    assert!(
        iterator.is_ok(),
        "Should be able to create new iterator after cleanup"
    );

    cleanup_keys("rust:cleanup:iter:*");
}

/// Test cleanup after panic-like early returns.
#[test]
#[ignore] // Requires Redis
fn test_cleanup_on_early_return() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:cleanup:early:*");

    // Set up test data
    for i in 1..=50 {
        redis_cli(&[
            "HSET",
            &format!("rust:cleanup:early:{}", i),
            "name",
            &format!("User{}", i),
        ]);
    }

    // Simulate early return during iteration
    let result = std::panic::catch_unwind(|| {
        let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]);
        let config = BatchConfig::new("rust:cleanup:early:*".to_string()).with_batch_size(10);

        let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
            .expect("Failed to create iterator");

        // Read some batches then "panic"
        let _ = iterator.next_batch();
        let _ = iterator.next_batch();

        // Force a panic to test cleanup
        panic!("Simulated panic during iteration");
    });

    assert!(result.is_err(), "Should have caught panic");

    // Verify we can still work with Redis
    assert!(redis_available(), "Redis should still be available");

    cleanup_keys("rust:cleanup:early:*");
}

/// Test memory is freed after processing large batches.
#[test]
#[ignore] // Requires Redis
fn test_memory_cleanup_large_batches() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:cleanup:large:*");

    // Create moderately large dataset
    for i in 1..=100 {
        let large_value = "x".repeat(1000); // 1KB per field
        redis_cli(&[
            "HSET",
            &format!("rust:cleanup:large:{}", i),
            "data",
            &large_value,
        ]);
    }

    // Process multiple times - if memory wasn't freed, we'd see growth
    for round in 0..3 {
        let schema = HashSchema::new(vec![("data".to_string(), RedisType::Utf8)]);
        let config = BatchConfig::new("rust:cleanup:large:*".to_string()).with_batch_size(50);

        let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
            .expect("Failed to create iterator");

        let mut total_rows = 0;
        while let Ok(Some(batch)) = iterator.next_batch() {
            total_rows += batch.num_rows();
        }

        assert_eq!(total_rows, 100, "Round {} should read all rows", round);
    }

    cleanup_keys("rust:cleanup:large:*");
}

// =============================================================================
// Partial Failure Recovery Tests
// =============================================================================

/// Test behavior when some keys are deleted mid-scan.
#[test]
#[ignore] // Requires Redis
fn test_keys_deleted_during_scan() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:partial:del:*");

    // Set up test data
    for i in 1..=20 {
        redis_cli(&[
            "HSET",
            &format!("rust:partial:del:{}", i),
            "name",
            &format!("User{}", i),
        ]);
    }

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(true);
    let config = BatchConfig::new("rust:partial:del:*".to_string()).with_batch_size(5);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    // Read first batch
    let batch1 = iterator.next_batch().expect("First batch should work");
    assert!(batch1.is_some());

    // Delete some keys that haven't been scanned yet
    for i in 15..=20 {
        redis_cli(&["DEL", &format!("rust:partial:del:{}", i)]);
    }

    // Continue iteration - should gracefully handle missing keys
    let mut total_rows = batch1.unwrap().num_rows();
    while let Ok(Some(batch)) = iterator.next_batch() {
        total_rows += batch.num_rows();
    }

    // Should have fewer rows due to deletion, but no errors
    assert!(
        total_rows < 20,
        "Should have fewer rows after deletion: got {}",
        total_rows
    );

    cleanup_keys("rust:partial:del:*");
}

/// Test behavior when key types change mid-scan.
#[test]
#[ignore] // Requires Redis
fn test_key_type_changed_during_scan() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:partial:type:*");

    // Set up hash keys
    for i in 1..=10 {
        redis_cli(&[
            "HSET",
            &format!("rust:partial:type:{}", i),
            "name",
            &format!("User{}", i),
        ]);
    }

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(true);
    let config = BatchConfig::new("rust:partial:type:*".to_string()).with_batch_size(3);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    // Read first batch
    let batch1 = iterator.next_batch().expect("First batch should work");
    assert!(batch1.is_some());

    // Change type of some keys (delete hash and create string)
    for i in 7..=10 {
        let key = format!("rust:partial:type:{}", i);
        redis_cli(&["DEL", &key]);
        redis_cli(&["SET", &key, "now_a_string"]);
    }

    // Continue iteration - wrong type keys should be skipped
    let mut error_count = 0;
    loop {
        match iterator.next_batch() {
            Ok(Some(_)) => {}
            Ok(None) => break,
            Err(_) => {
                error_count += 1;
                // If we get too many errors, break to avoid infinite loop
                if error_count > 5 {
                    break;
                }
            }
        }
    }

    cleanup_keys("rust:partial:type:*");
}

/// Test TTL expiration during iteration.
#[test]
#[ignore] // Requires Redis
fn test_ttl_expiration_during_scan() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:partial:ttl:*");

    // Set up keys with very short TTL
    for i in 1..=10 {
        redis_cli(&[
            "HSET",
            &format!("rust:partial:ttl:{}", i),
            "name",
            &format!("User{}", i),
        ]);
        // Set 1 second TTL on half the keys
        if i > 5 {
            redis_cli(&["EXPIRE", &format!("rust:partial:ttl:{}", i), "1"]);
        }
    }

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(true);
    let config = BatchConfig::new("rust:partial:ttl:*".to_string()).with_batch_size(3);

    // Wait for some keys to expire
    thread::sleep(Duration::from_secs(2));

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let mut total_rows = 0;
    while let Ok(Some(batch)) = iterator.next_batch() {
        total_rows += batch.num_rows();
    }

    // Should only have non-expired keys (approximately 5)
    assert!(
        total_rows <= 5,
        "Should only have non-expired keys: got {}",
        total_rows
    );

    cleanup_keys("rust:partial:ttl:*");
}

// =============================================================================
// Write Operation Failure Tests
// =============================================================================

/// Test write with empty vectors.
#[test]
#[ignore] // Requires Redis
fn test_write_empty_data() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:fail:write:empty:*");

    // Create empty vectors
    let keys: Vec<String> = vec![];
    let fields = vec!["name".to_string(), "age".to_string()];
    let values: Vec<Vec<Option<String>>> = vec![];

    let result = write_hashes(&redis_url(), keys, fields, values, None, WriteMode::Replace);

    // Writing empty data should succeed (no-op)
    assert!(result.is_ok());

    // Verify no keys were created
    let keys_output = redis_cli_output(&["KEYS", "rust:fail:write:empty:*"]);
    assert!(
        keys_output.is_none() || keys_output.as_ref().unwrap().is_empty(),
        "No keys should be created for empty data"
    );

    cleanup_keys("rust:fail:write:empty:*");
}

/// Test write mode Fail behavior (skip existing keys).
#[test]
#[ignore] // Requires Redis
fn test_write_mode_fail() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:fail:write:exists:*");

    // Create initial data
    let keys1 = vec!["rust:fail:write:exists:1".to_string()];
    let fields1 = vec!["name".to_string()];
    let values1 = vec![vec![Some("Alice".to_string())]];

    // Write first time
    let result1 = write_hashes(
        &redis_url(),
        keys1,
        fields1,
        values1,
        None,
        WriteMode::Replace,
    );
    assert!(result1.is_ok(), "First write should succeed");

    // Try to write again with Fail mode (same key)
    let keys2 = vec!["rust:fail:write:exists:1".to_string()];
    let fields2 = vec!["name".to_string()];
    let values2 = vec![vec![Some("Bob".to_string())]];

    let result2 = write_hashes(&redis_url(), keys2, fields2, values2, None, WriteMode::Fail);

    // With Fail mode, existing keys are skipped (not overwritten)
    assert!(result2.is_ok(), "Write with Fail mode should succeed");
    let result2 = result2.unwrap();
    assert_eq!(result2.keys_skipped, 1, "Existing key should be skipped");
    assert_eq!(result2.keys_written, 0, "No new keys should be written");

    // Verify original data wasn't overwritten
    let name = redis_cli_output(&["HGET", "rust:fail:write:exists:1", "name"]);
    assert_eq!(
        name,
        Some("Alice".to_string()),
        "Original value should remain"
    );

    cleanup_keys("rust:fail:write:exists:*");
}

/// Test write with null values.
#[test]
#[ignore] // Requires Redis
fn test_write_with_nulls() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:fail:write:null:*");

    let keys = vec!["rust:fail:write:null:1".to_string()];
    let fields = vec!["name".to_string(), "optional".to_string()];
    let values = vec![vec![Some("Alice".to_string()), None]];

    let result = write_hashes(&redis_url(), keys, fields, values, None, WriteMode::Replace);

    assert!(result.is_ok(), "Write with nulls should succeed");

    // Verify name was written but optional was not
    let name = redis_cli_output(&["HGET", "rust:fail:write:null:1", "name"]);
    assert_eq!(name, Some("Alice".to_string()));

    let optional = redis_cli_output(&["HGET", "rust:fail:write:null:1", "optional"]);
    // Null fields should not create the field or should be empty
    assert!(
        optional.is_none() || optional.as_ref().unwrap().is_empty(),
        "Null field should not be written"
    );

    cleanup_keys("rust:fail:write:null:*");
}

// =============================================================================
// Search Index Not Found Tests
// =============================================================================

/// Test search on non-existent index.
#[test]
#[ignore] // Requires Redis
fn test_search_nonexistent_index() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    // Try to drop any existing index with this name
    let _ = redis_cli(&["FT.DROPINDEX", "idx:nonexistent:test"]);

    // Search module functionality test - search on non-existent index
    let result = redis_cli_output(&["FT.SEARCH", "idx:nonexistent:test", "*"]);

    // The command should fail or return an error
    // redis_cli_output returns None on failure
    if let Some(output) = result {
        // If it returns something, it should indicate an error
        assert!(
            output.to_lowercase().contains("unknown index name")
                || output.to_lowercase().contains("no such index"),
            "Should indicate index not found: {}",
            output
        );
    }
    // None means the command failed, which is expected
}

/// Test behavior when index is dropped during iteration.
#[test]
#[ignore] // Requires Redis
fn test_index_dropped_during_search() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:fail:idxdrop:*");

    // Create test data
    for i in 1..=10 {
        redis_cli(&[
            "HSET",
            &format!("rust:fail:idxdrop:{}", i),
            "name",
            &format!("User{}", i),
            "age",
            &i.to_string(),
        ]);
    }

    // Create index
    let _ = redis_cli(&["FT.DROPINDEX", "idx:fail:idxdrop"]);
    assert!(
        redis_cli(&[
            "FT.CREATE",
            "idx:fail:idxdrop",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "rust:fail:idxdrop:",
            "SCHEMA",
            "name",
            "TEXT",
            "age",
            "NUMERIC",
        ]),
        "Index creation should succeed"
    );

    // Wait for indexing
    thread::sleep(Duration::from_millis(200));

    // Verify index works
    let search_result =
        redis_cli_output(&["FT.SEARCH", "idx:fail:idxdrop", "*", "LIMIT", "0", "5"]);
    assert!(search_result.is_some(), "Initial search should work");

    // Drop the index
    assert!(
        redis_cli(&["FT.DROPINDEX", "idx:fail:idxdrop"]),
        "Index drop should succeed"
    );

    // Search again - should fail gracefully
    let search_result2 = redis_cli_output(&["FT.SEARCH", "idx:fail:idxdrop", "*"]);

    // Either returns None (command failed) or error message
    if let Some(output) = search_result2 {
        let lower = output.to_lowercase();
        assert!(
            lower.contains("unknown index") || lower.contains("no such index"),
            "Should indicate index not found: {}",
            output
        );
    }

    cleanup_keys("rust:fail:idxdrop:*");
}

// =============================================================================
// Concurrent Failure Handling Tests
// =============================================================================

/// Test multiple threads encountering failures.
#[test]
#[ignore] // Requires Redis
fn test_concurrent_failure_handling() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let error_count = Arc::new(AtomicUsize::new(0));
    let success_count = Arc::new(AtomicUsize::new(0));

    let handles: Vec<_> = (0..4)
        .map(|i| {
            let error_count = Arc::clone(&error_count);
            let success_count = Arc::clone(&success_count);

            thread::spawn(move || {
                // Even threads try valid connection, odd try invalid
                let url = if i % 2 == 0 {
                    redis_url()
                } else {
                    "redis://invalid-host-67890:6379".to_string()
                };

                let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]);
                let config = BatchConfig::new("nonexistent:*".to_string());

                match HashBatchIterator::new(&url, schema, config, None) {
                    Ok(_) => {
                        success_count.fetch_add(1, Ordering::SeqCst);
                    }
                    Err(_) => {
                        error_count.fetch_add(1, Ordering::SeqCst);
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread should not panic");
    }

    // Should have both successes (even threads) and errors (odd threads)
    let errors = error_count.load(Ordering::SeqCst);
    let successes = success_count.load(Ordering::SeqCst);

    assert!(errors > 0, "Should have some errors from invalid URLs");
    assert!(successes > 0, "Should have some successes from valid URLs");
    assert_eq!(errors + successes, 4, "All threads should complete");
}

/// Test that errors in one thread don't affect others.
#[test]
#[ignore] // Requires Redis
fn test_error_isolation_between_threads() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:fail:isolation:*");

    // Set up valid test data
    for i in 1..=10 {
        redis_cli(&[
            "HSET",
            &format!("rust:fail:isolation:{}", i),
            "name",
            &format!("User{}", i),
        ]);
    }

    let results = Arc::new(std::sync::Mutex::new(Vec::new()));

    let handles: Vec<_> = (0..4)
        .map(|i| {
            let results = Arc::clone(&results);
            let url = redis_url();

            thread::spawn(move || {
                // Thread 0 uses invalid schema, others use valid
                let schema = if i == 0 {
                    // Try to read non-existent field as Int64
                    HashSchema::new(vec![("nonexistent".to_string(), RedisType::Int64)])
                } else {
                    HashSchema::new(vec![("name".to_string(), RedisType::Utf8)])
                };

                let config =
                    BatchConfig::new("rust:fail:isolation:*".to_string()).with_batch_size(5);

                let mut iterator = match HashBatchIterator::new(&url, schema, config, None) {
                    Ok(it) => it,
                    Err(e) => {
                        results.lock().unwrap().push((i, Err(e.to_string())));
                        return;
                    }
                };

                let mut row_count = 0;
                loop {
                    match iterator.next_batch() {
                        Ok(Some(batch)) => row_count += batch.num_rows(),
                        Ok(None) => break,
                        Err(e) => {
                            results.lock().unwrap().push((i, Err(e.to_string())));
                            return;
                        }
                    }
                }

                results.lock().unwrap().push((i, Ok(row_count)));
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread should not panic");
    }

    let results = results.lock().unwrap();

    // Thread 0 should have error or 0 rows (non-existent field)
    // Other threads should succeed with 10 rows each
    let mut success_threads = 0;
    for (thread_id, result) in results.iter() {
        match result {
            Ok(count) if *thread_id != 0 => {
                assert_eq!(*count, 10, "Thread {} should read all rows", thread_id);
                success_threads += 1;
            }
            Ok(_) => {}  // Thread 0 might succeed with nulls
            Err(_) => {} // Thread 0 might error
        }
    }

    // At least threads 1, 2, 3 should succeed
    assert!(
        success_threads >= 3,
        "Most threads should succeed independently"
    );

    cleanup_keys("rust:fail:isolation:*");
}

// =============================================================================
// Connection Recovery Tests
// =============================================================================

/// Test that new connections work after failure.
#[test]
#[ignore] // Requires Redis
fn test_new_connection_after_failure() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    // First, try to connect to invalid host
    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]);
    let config = BatchConfig::new("test:*".to_string());

    let bad_result = HashBatchIterator::new(
        "redis://definitely-not-a-real-host:6379",
        schema.clone(),
        config.clone(),
        None,
    );
    assert!(bad_result.is_err(), "Bad connection should fail");

    // Now connect to valid host - should work independently
    let good_result = HashBatchIterator::new(&redis_url(), schema, config, None);
    assert!(
        good_result.is_ok(),
        "Good connection should succeed after bad one"
    );
}

/// Test rapid connection attempts.
#[test]
#[ignore] // Requires Redis
fn test_rapid_connection_attempts() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:fail:rapid:*");

    // Set up test data
    redis_cli(&["HSET", "rust:fail:rapid:1", "name", "Test"]);

    // Rapidly create and drop connections
    for _ in 0..10 {
        let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]);
        let config = BatchConfig::new("rust:fail:rapid:*".to_string());

        let mut iterator =
            HashBatchIterator::new(&redis_url(), schema, config, None).expect("Connection failed");

        // Read one batch
        let _ = iterator.next_batch();

        // Iterator dropped here
    }

    // Final connection should still work
    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]);
    let config = BatchConfig::new("rust:fail:rapid:*".to_string());

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Final connection failed");

    let batch = iterator.next_batch().expect("Final read failed");
    assert!(batch.is_some());

    cleanup_keys("rust:fail:rapid:*");
}

// =============================================================================
// Batch Size Edge Cases
// =============================================================================

/// Test very small batch size.
#[test]
#[ignore] // Requires Redis
fn test_batch_size_one() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:fail:batch1:*");

    // Set up test data
    for i in 1..=5 {
        redis_cli(&[
            "HSET",
            &format!("rust:fail:batch1:{}", i),
            "name",
            &format!("User{}", i),
        ]);
    }

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]);
    let config = BatchConfig::new("rust:fail:batch1:*".to_string()).with_batch_size(1);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let mut batch_count = 0;
    let mut total_rows = 0;

    while let Ok(Some(batch)) = iterator.next_batch() {
        batch_count += 1;
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 5, "Should read all rows");
    // With batch size 1, we should have approximately 5 batches (one per key)
    assert!(
        batch_count >= 5,
        "Should have at least 5 batches with batch size 1"
    );

    cleanup_keys("rust:fail:batch1:*");
}

/// Test batch size larger than data.
#[test]
#[ignore] // Requires Redis
fn test_batch_size_larger_than_data() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:fail:batchlarge:*");

    // Set up small dataset
    for i in 1..=3 {
        redis_cli(&[
            "HSET",
            &format!("rust:fail:batchlarge:{}", i),
            "name",
            &format!("User{}", i),
        ]);
    }

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]);
    let config = BatchConfig::new("rust:fail:batchlarge:*".to_string()).with_batch_size(10000);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let mut total_rows = 0;
    while let Ok(Some(batch)) = iterator.next_batch() {
        total_rows += batch.num_rows();
    }

    assert_eq!(
        total_rows, 3,
        "Should read all rows even with large batch size"
    );

    cleanup_keys("rust:fail:batchlarge:*");
}
