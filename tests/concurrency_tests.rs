//! Concurrency and race condition tests for polars-redis.
//!
//! These tests verify thread-safety and correct behavior under concurrent access:
//! - Parallel batch read operations
//! - Concurrent writes to different keys
//! - Cache key contention (simultaneous read/write)
//! - Multiple smart_scan/search operations on same index
//! - Write conflicts and ordering
//!
//! Run with: `cargo test --test concurrency_tests -- --ignored --test-threads=1`
//!
//! Note: These tests use --test-threads=1 to ensure consistent test isolation,
//! but the tests themselves spawn multiple threads internally.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use arrow::array::Array;
use polars_redis::{BatchConfig, HashBatchIterator, HashSchema, RedisType, WriteMode};

mod common;
use common::{cleanup_keys, redis_available, redis_cli, redis_cli_output, redis_url};

// =============================================================================
// Parallel Batch Read Operations
// =============================================================================

/// Test multiple threads reading from the same key pattern simultaneously.
#[test]
#[ignore] // Requires Redis
fn test_parallel_batch_reads_same_pattern() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:parallel:read:*");

    // Setup test data
    for i in 0..100 {
        redis_cli(&[
            "HSET",
            &format!("rust:parallel:read:{}", i),
            "name",
            &format!("User{}", i),
            "value",
            &i.to_string(),
        ]);
    }

    let num_threads = 8;
    let barrier = Arc::new(Barrier::new(num_threads));
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let barrier = Arc::clone(&barrier);
        let success = Arc::clone(&success_count);

        let handle = thread::spawn(move || {
            barrier.wait(); // Sync start for maximum contention

            let schema = HashSchema::new(vec![
                ("name".to_string(), RedisType::Utf8),
                ("value".to_string(), RedisType::Int64),
            ])
            .with_key(true);
            let config = BatchConfig::new("rust:parallel:read:*".to_string()).with_batch_size(25);

            match HashBatchIterator::new(&redis_url(), schema, config, None) {
                Ok(mut iterator) => {
                    let mut count = 0;
                    while let Ok(Some(batch)) = iterator.next_batch() {
                        count += batch.num_rows();
                    }
                    if count == 100 {
                        success.fetch_add(1, Ordering::SeqCst);
                    } else {
                        eprintln!("Thread {} got {} rows instead of 100", thread_id, count);
                    }
                },
                Err(e) => {
                    eprintln!("Thread {} failed: {}", thread_id, e);
                },
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    let successes = success_count.load(Ordering::SeqCst);
    assert_eq!(
        successes, num_threads,
        "All {} threads should succeed, but only {} did",
        num_threads, successes
    );

    cleanup_keys("rust:parallel:read:*");
}

/// Test parallel reads with different patterns (no overlap).
#[test]
#[ignore] // Requires Redis
fn test_parallel_reads_different_patterns() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let num_patterns = 4;
    let keys_per_pattern = 50;

    // Setup data for each pattern
    for p in 0..num_patterns {
        cleanup_keys(&format!("rust:pattern{}:*", p));
        for i in 0..keys_per_pattern {
            redis_cli(&[
                "HSET",
                &format!("rust:pattern{}:{}", p, i),
                "data",
                &format!("value_{}_{}", p, i),
            ]);
        }
    }

    let barrier = Arc::new(Barrier::new(num_patterns));
    let results = Arc::new(std::sync::Mutex::new(vec![0usize; num_patterns]));
    let mut handles = vec![];

    for pattern_id in 0..num_patterns {
        let barrier = Arc::clone(&barrier);
        let results = Arc::clone(&results);

        let handle = thread::spawn(move || {
            barrier.wait();

            let schema =
                HashSchema::new(vec![("data".to_string(), RedisType::Utf8)]).with_key(true);
            let config =
                BatchConfig::new(format!("rust:pattern{}:*", pattern_id)).with_batch_size(20);

            let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
                .expect("Failed to create iterator");

            let mut count = 0;
            while let Some(batch) = iterator.next_batch().expect("Batch failed") {
                count += batch.num_rows();
            }

            let mut r = results.lock().unwrap();
            r[pattern_id] = count;
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    let final_results = results.lock().unwrap();
    for (i, count) in final_results.iter().enumerate() {
        assert_eq!(
            *count, keys_per_pattern,
            "Pattern {} should have {} keys, got {}",
            i, keys_per_pattern, count
        );
    }

    // Cleanup
    for p in 0..num_patterns {
        cleanup_keys(&format!("rust:pattern{}:*", p));
    }
}

// =============================================================================
// Concurrent Write Operations
// =============================================================================

/// Test concurrent writes to completely different keys (no contention).
#[test]
#[ignore] // Requires Redis
fn test_concurrent_writes_no_overlap() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:concwrite2:*");

    let num_threads = 8;
    let writes_per_thread = 100;
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let barrier = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier.wait();

            let keys: Vec<String> = (0..writes_per_thread)
                .map(|i| format!("rust:concwrite2:t{}:k{}", thread_id, i))
                .collect();
            let fields = vec!["data".to_string(), "thread".to_string()];
            let values: Vec<Vec<Option<String>>> = (0..writes_per_thread)
                .map(|i| vec![Some(format!("value_{}", i)), Some(thread_id.to_string())])
                .collect();

            polars_redis::write_hashes(&redis_url(), keys, fields, values, None, WriteMode::Replace)
                .expect("Write failed")
        });

        handles.push(handle);
    }

    let results: Vec<_> = handles
        .into_iter()
        .map(|h| h.join().expect("Thread panicked"))
        .collect();

    // Verify all writes succeeded
    let total_written: usize = results.iter().map(|r| r.keys_written).sum();
    assert_eq!(total_written, num_threads * writes_per_thread);

    // Verify data integrity
    let schema = HashSchema::new(vec![
        ("data".to_string(), RedisType::Utf8),
        ("thread".to_string(), RedisType::Int64),
    ])
    .with_key(true);
    let config = BatchConfig::new("rust:concwrite2:*".to_string()).with_batch_size(500);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let mut total_read = 0;
    while let Some(batch) = iterator.next_batch().expect("Read failed") {
        total_read += batch.num_rows();
    }

    assert_eq!(total_read, num_threads * writes_per_thread);

    cleanup_keys("rust:concwrite2:*");
}

/// Test concurrent writes to the SAME keys (write contention).
#[test]
#[ignore] // Requires Redis
fn test_concurrent_writes_same_keys() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:samekey:*");

    let num_threads = 4;
    let num_keys = 10; // Small number of keys = high contention
    let writes_per_thread = 20;
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let barrier = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier.wait();

            for iteration in 0..writes_per_thread {
                let keys: Vec<String> = (0..num_keys)
                    .map(|i| format!("rust:samekey:{}", i))
                    .collect();
                let fields = vec!["writer".to_string(), "iteration".to_string()];
                let values: Vec<Vec<Option<String>>> = (0..num_keys)
                    .map(|_| vec![Some(thread_id.to_string()), Some(iteration.to_string())])
                    .collect();

                let _ = polars_redis::write_hashes(
                    &redis_url(),
                    keys,
                    fields,
                    values,
                    None,
                    WriteMode::Replace,
                );
            }
            thread_id
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify final state - each key should have valid data from SOME thread
    for i in 0..num_keys {
        let key = format!("rust:samekey:{}", i);
        let writer = redis_cli_output(&["HGET", &key, "writer"]);
        assert!(writer.is_some(), "Key {} should exist", key);

        let writer_id: usize = writer.unwrap().parse().expect("writer should be a number");
        assert!(
            writer_id < num_threads,
            "writer_id {} should be < {}",
            writer_id,
            num_threads
        );
    }

    cleanup_keys("rust:samekey:*");
}

// =============================================================================
// Cache Key Contention
// =============================================================================

/// Test simultaneous read and write to overlapping key sets.
#[test]
#[ignore] // Requires Redis
fn test_read_write_contention() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:rwcont:*");

    // Pre-populate some data
    for i in 0..50 {
        redis_cli(&[
            "HSET",
            &format!("rust:rwcont:{}", i),
            "value",
            &i.to_string(),
        ]);
    }

    let barrier = Arc::new(Barrier::new(3)); // 1 reader + 2 writers
    let read_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    // Reader thread - continuously reads
    {
        let barrier = Arc::clone(&barrier);
        let read_count = Arc::clone(&read_count);

        let handle = thread::spawn(move || {
            barrier.wait();

            let start = Instant::now();
            while start.elapsed() < Duration::from_secs(2) {
                let schema =
                    HashSchema::new(vec![("value".to_string(), RedisType::Int64)]).with_key(true);
                let config = BatchConfig::new("rust:rwcont:*".to_string()).with_batch_size(100);

                if let Ok(mut iterator) = HashBatchIterator::new(&redis_url(), schema, config, None)
                {
                    while let Ok(Some(_batch)) = iterator.next_batch() {
                        read_count.fetch_add(1, Ordering::SeqCst);
                    }
                }
                thread::sleep(Duration::from_millis(10));
            }
        });
        handles.push(handle);
    }

    // Writer threads - add new keys
    for writer_id in 0..2 {
        let barrier = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier.wait();

            for i in 0..50 {
                let key = format!("rust:rwcont:new_{}_{}", writer_id, i);
                redis_cli(&["HSET", &key, "value", &(i + 100).to_string()]);
                thread::sleep(Duration::from_millis(20));
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    let reads = read_count.load(Ordering::SeqCst);
    assert!(reads > 0, "Should have completed some reads");

    // Verify final state
    let schema = HashSchema::new(vec![("value".to_string(), RedisType::Int64)]).with_key(true);
    let config = BatchConfig::new("rust:rwcont:*".to_string()).with_batch_size(200);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let mut total = 0;
    while let Some(batch) = iterator.next_batch().expect("Read failed") {
        total += batch.num_rows();
    }

    // Should have original 50 + 2 writers * 50 new keys = 150
    assert_eq!(total, 150);

    cleanup_keys("rust:rwcont:*");
}

// =============================================================================
// Index Query Concurrency
// =============================================================================

/// Test multiple search operations on the same index concurrently.
#[test]
#[ignore] // Requires Redis with RediSearch
fn test_concurrent_search_same_index() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:searchconc:*");

    // Drop and create index
    let _ = redis_cli(&["FT.DROPINDEX", "idx:searchconc"]);

    // Create test data
    for i in 0..100 {
        redis_cli(&[
            "HSET",
            &format!("rust:searchconc:{}", i),
            "name",
            &format!("User{}", i),
            "age",
            &(20 + i % 50).to_string(),
            "category",
            if i % 2 == 0 { "even" } else { "odd" },
        ]);
    }

    // Create index
    redis_cli(&[
        "FT.CREATE",
        "idx:searchconc",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "rust:searchconc:",
        "SCHEMA",
        "name",
        "TEXT",
        "age",
        "NUMERIC",
        "SORTABLE",
        "category",
        "TAG",
    ]);

    // Wait for index
    thread::sleep(Duration::from_millis(500));

    let num_threads = 6;
    let barrier = Arc::new(Barrier::new(num_threads));
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    // Different query types
    let queries = vec![
        "*",                // All
        "@age:[30 50]",     // Numeric range
        "@category:{even}", // Tag
        "@name:User*",      // Text prefix
        "@age:[20 30]",     // Another range
        "@category:{odd}",  // Another tag
    ];

    for (thread_id, query) in queries.into_iter().enumerate() {
        let barrier = Arc::clone(&barrier);
        let success = Arc::clone(&success_count);
        let query = query.to_string();

        let handle = thread::spawn(move || {
            barrier.wait();

            // Run multiple iterations
            for _ in 0..10 {
                let output = std::process::Command::new("redis-cli")
                    .args([
                        "-p",
                        &common::redis_port().to_string(),
                        "FT.SEARCH",
                        "idx:searchconc",
                        &query,
                        "LIMIT",
                        "0",
                        "100",
                    ])
                    .output();

                if let Ok(o) = output
                    && o.status.success()
                {
                    success.fetch_add(1, Ordering::SeqCst);
                }
                thread::sleep(Duration::from_millis(10));
            }
            thread_id
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    let successes = success_count.load(Ordering::SeqCst);
    // Each thread runs 10 iterations
    assert!(
        successes >= num_threads * 8,
        "Most queries should succeed: {} of {}",
        successes,
        num_threads * 10
    );

    cleanup_keys("rust:searchconc:*");
    let _ = redis_cli(&["FT.DROPINDEX", "idx:searchconc"]);
}

// =============================================================================
// Stress Tests with High Concurrency
// =============================================================================

/// Stress test with many concurrent readers and writers.
#[test]
#[ignore] // Requires Redis
fn test_high_concurrency_stress() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:stress:*");

    // Pre-populate
    for i in 0..100 {
        redis_cli(&[
            "HSET",
            &format!("rust:stress:{}", i),
            "data",
            &format!("initial_{}", i),
        ]);
    }

    let num_readers = 4;
    let num_writers = 4;
    let duration = Duration::from_secs(3);

    let barrier = Arc::new(Barrier::new(num_readers + num_writers));
    let read_ops = Arc::new(AtomicUsize::new(0));
    let write_ops = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    // Reader threads
    for _ in 0..num_readers {
        let barrier = Arc::clone(&barrier);
        let read_ops = Arc::clone(&read_ops);
        let errors = Arc::clone(&errors);

        let handle = thread::spawn(move || {
            barrier.wait();
            let start = Instant::now();

            while start.elapsed() < duration {
                let schema =
                    HashSchema::new(vec![("data".to_string(), RedisType::Utf8)]).with_key(true);
                let config = BatchConfig::new("rust:stress:*".to_string()).with_batch_size(50);

                match HashBatchIterator::new(&redis_url(), schema, config, None) {
                    Ok(mut iterator) => {
                        while let Ok(Some(_)) = iterator.next_batch() {
                            read_ops.fetch_add(1, Ordering::SeqCst);
                        }
                    },
                    Err(_) => {
                        errors.fetch_add(1, Ordering::SeqCst);
                    },
                }
            }
        });
        handles.push(handle);
    }

    // Writer threads
    for writer_id in 0..num_writers {
        let barrier = Arc::clone(&barrier);
        let write_ops = Arc::clone(&write_ops);
        let errors = Arc::clone(&errors);

        let handle = thread::spawn(move || {
            barrier.wait();
            let start = Instant::now();
            let mut counter = 0;

            while start.elapsed() < duration {
                let key = format!("rust:stress:w{}_{}", writer_id, counter % 100);
                if redis_cli(&["HSET", &key, "data", &format!("updated_{}", counter)]) {
                    write_ops.fetch_add(1, Ordering::SeqCst);
                } else {
                    errors.fetch_add(1, Ordering::SeqCst);
                }
                counter += 1;
                thread::sleep(Duration::from_millis(5));
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    let reads = read_ops.load(Ordering::SeqCst);
    let writes = write_ops.load(Ordering::SeqCst);
    let errs = errors.load(Ordering::SeqCst);

    eprintln!(
        "Stress test results: {} read batches, {} writes, {} errors",
        reads, writes, errs
    );

    assert!(reads > 0, "Should complete some reads");
    assert!(writes > 0, "Should complete some writes");
    assert_eq!(errs, 0, "Should have no errors");

    cleanup_keys("rust:stress:*");
}

// =============================================================================
// Iterator Ordering Under Concurrency
// =============================================================================

/// Test that batch iteration produces consistent results under concurrent modifications.
#[test]
#[ignore] // Requires Redis
fn test_iteration_consistency_during_writes() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:itercons:*");

    // Setup initial data
    for i in 0..50 {
        redis_cli(&[
            "HSET",
            &format!("rust:itercons:{}", i),
            "seq",
            &i.to_string(),
        ]);
    }

    let barrier = Arc::new(Barrier::new(2));
    let seen_keys = Arc::new(std::sync::Mutex::new(HashSet::new()));

    // Reader thread
    let barrier_r = Arc::clone(&barrier);
    let seen = Arc::clone(&seen_keys);

    let reader = thread::spawn(move || {
        barrier_r.wait();

        let schema = HashSchema::new(vec![("seq".to_string(), RedisType::Int64)]).with_key(true);
        let config = BatchConfig::new("rust:itercons:*".to_string()).with_batch_size(10);

        let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
            .expect("Failed to create iterator");

        while let Some(batch) = iterator.next_batch().expect("Batch failed") {
            let key_col = batch.column_by_name("_key").unwrap();
            let key_array = key_col
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();

            let mut keys = seen.lock().unwrap();
            for i in 0..key_array.len() {
                keys.insert(key_array.value(i).to_string());
            }

            // Slow down iteration to increase chance of concurrent modification
            thread::sleep(Duration::from_millis(50));
        }
    });

    // Writer thread - adds new keys during iteration
    let barrier_w = Arc::clone(&barrier);

    let writer = thread::spawn(move || {
        barrier_w.wait();

        for i in 50..75 {
            redis_cli(&[
                "HSET",
                &format!("rust:itercons:{}", i),
                "seq",
                &i.to_string(),
            ]);
            thread::sleep(Duration::from_millis(20));
        }
    });

    reader.join().expect("Reader panicked");
    writer.join().expect("Writer panicked");

    let keys = seen_keys.lock().unwrap();

    // Original 50 keys should definitely be seen
    // Some of the new keys (50-74) may or may not be seen depending on timing
    assert!(
        keys.len() >= 50,
        "Should see at least the original 50 keys, got {}",
        keys.len()
    );

    // Verify no duplicate processing would occur (SCAN cursor consistency)
    // This is inherently handled by Redis SCAN guarantees

    cleanup_keys("rust:itercons:*");
}

// =============================================================================
// Connection Pool Behavior
// =============================================================================

/// Test that connection pooling works correctly under load.
#[test]
#[ignore] // Requires Redis
fn test_connection_pool_under_load() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:connpool:*");

    // Setup data
    for i in 0..20 {
        redis_cli(&[
            "HSET",
            &format!("rust:connpool:{}", i),
            "data",
            &format!("value_{}", i),
        ]);
    }

    let num_threads = 16; // More threads than typical pool size
    let iterations_per_thread = 10;
    let barrier = Arc::new(Barrier::new(num_threads));
    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];

    for _ in 0..num_threads {
        let barrier = Arc::clone(&barrier);
        let success = Arc::clone(&success_count);

        let handle = thread::spawn(move || {
            barrier.wait();

            for _ in 0..iterations_per_thread {
                let schema =
                    HashSchema::new(vec![("data".to_string(), RedisType::Utf8)]).with_key(true);
                let config = BatchConfig::new("rust:connpool:*".to_string()).with_batch_size(10);

                if let Ok(mut iterator) = HashBatchIterator::new(&redis_url(), schema, config, None)
                {
                    let mut count = 0;
                    while let Ok(Some(batch)) = iterator.next_batch() {
                        count += batch.num_rows();
                    }
                    if count == 20 {
                        success.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    let successes = success_count.load(Ordering::SeqCst);
    let expected = num_threads * iterations_per_thread;

    assert!(
        successes >= expected * 95 / 100,
        "At least 95% should succeed: {} of {}",
        successes,
        expected
    );

    cleanup_keys("rust:connpool:*");
}
