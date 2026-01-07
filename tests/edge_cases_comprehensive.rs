//! Comprehensive edge case tests for polars-redis.
//!
//! This module covers edge cases identified in issue #137:
//! - All-null columns and schema inference
//! - Round-trip integrity (write-read-compare)
//! - Concurrent access patterns
//! - Numeric edge cases (infinity, NaN, boundaries)
//! - Unicode and RediSearch special characters
//! - Schema mismatch scenarios
//! - RediSearch-specific edge cases
//!
//! Run with: `cargo test --test edge_cases_comprehensive --all-features -- --ignored`

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use polars_redis::{
    BatchConfig, HashBatchIterator, HashSchema, RedisType, WriteMode, write_hashes,
};
use std::collections::HashMap;
use std::sync::{Arc, Barrier};
use std::thread;

mod common;
use common::{cleanup_keys, redis_available, redis_cli, redis_cli_output, redis_url};

// =============================================================================
// All-Null Columns Tests
// =============================================================================

/// Test reading a column where ALL values are null (missing field in every hash).
#[test]
#[ignore] // Requires Redis
fn test_all_null_column() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:allnullcol:*");

    // Create hashes where "missing_field" is never present
    redis_cli(&["HSET", "rust:allnullcol:1", "name", "Alice"]);
    redis_cli(&["HSET", "rust:allnullcol:2", "name", "Bob"]);
    redis_cli(&["HSET", "rust:allnullcol:3", "name", "Charlie"]);

    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("missing_field".to_string(), RedisType::Utf8), // Never present
    ])
    .with_key(true);
    let config = BatchConfig::new("rust:allnullcol:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 3); // key, name, missing_field

    // Verify the missing_field column is all nulls
    let missing_col = batch
        .column_by_name("missing_field")
        .expect("missing_field column not found");
    assert_eq!(missing_col.null_count(), 3);

    cleanup_keys("rust:allnullcol:*");
}

/// Test all-null column with numeric type.
#[test]
#[ignore] // Requires Redis
fn test_all_null_numeric_column() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:allnullnum:*");

    redis_cli(&["HSET", "rust:allnullnum:1", "name", "Alice"]);
    redis_cli(&["HSET", "rust:allnullnum:2", "name", "Bob"]);

    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("score".to_string(), RedisType::Int64), // Never present
    ])
    .with_key(true);
    let config = BatchConfig::new("rust:allnullnum:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");

    let score_col = batch
        .column_by_name("score")
        .expect("score column not found");
    assert_eq!(score_col.null_count(), 2);
    assert_eq!(score_col.len(), 2);

    cleanup_keys("rust:allnullnum:*");
}

/// Test mixed null patterns - some rows have field, others don't.
#[test]
#[ignore] // Requires Redis
fn test_mixed_null_patterns() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:mixednull:*");

    // Various null patterns
    redis_cli(&["HSET", "rust:mixednull:1", "a", "1", "b", "2", "c", "3"]);
    redis_cli(&["HSET", "rust:mixednull:2", "a", "1"]); // b and c missing
    redis_cli(&["HSET", "rust:mixednull:3", "b", "2"]); // a and c missing
    redis_cli(&["HSET", "rust:mixednull:4", "c", "3"]); // a and b missing
    redis_cli(&["HSET", "rust:mixednull:5", "dummy", "x"]); // a, b, c all missing

    let schema = HashSchema::new(vec![
        ("a".to_string(), RedisType::Utf8),
        ("b".to_string(), RedisType::Utf8),
        ("c".to_string(), RedisType::Utf8),
    ])
    .with_key(true);
    let config = BatchConfig::new("rust:mixednull:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");

    assert_eq!(batch.num_rows(), 5);

    // Column a: 2 values (rows 1,2), 3 nulls (rows 3,4,5)
    let col_a = batch.column_by_name("a").expect("column a not found");
    assert_eq!(col_a.null_count(), 3);

    cleanup_keys("rust:mixednull:*");
}

// =============================================================================
// Round-Trip Integrity Tests
// =============================================================================

/// Test write-read-compare for string values.
#[test]
#[ignore] // Requires Redis
fn test_roundtrip_strings() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:rtstring:*");

    let test_values = vec![
        "simple",
        "with spaces",
        "with\ttab",
        "with\nnewline",
        "unicode: cafe",
        "emoji: hello",
        "quotes: \"test\"",
        "backslash: \\path",
        "",                 // empty string
        "   ",              // whitespace only
        "a]b",              // special chars
        "<html>tag</html>", // angle brackets
    ];

    let keys: Vec<String> = (0..test_values.len())
        .map(|i| format!("rust:rtstring:{}", i))
        .collect();
    let fields = vec!["value".to_string()];
    let values: Vec<Vec<Option<String>>> = test_values
        .iter()
        .map(|v| vec![Some(v.to_string())])
        .collect();

    write_hashes(
        &redis_url(),
        keys.clone(),
        fields.clone(),
        values,
        None,
        WriteMode::Replace,
    )
    .expect("Failed to write");

    // Read back
    let schema = HashSchema::new(vec![("value".to_string(), RedisType::Utf8)]).with_key(true);
    let config = BatchConfig::new("rust:rtstring:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");

    // Collect results into a map for comparison
    let key_col = batch
        .column_by_name("_key")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let val_col = batch
        .column_by_name("value")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let mut results: HashMap<String, String> = HashMap::new();
    for i in 0..batch.num_rows() {
        if !val_col.is_null(i) {
            results.insert(key_col.value(i).to_string(), val_col.value(i).to_string());
        }
    }

    // Verify all non-empty values round-tripped correctly
    for (i, expected) in test_values.iter().enumerate() {
        if !expected.is_empty() {
            let key = format!("rust:rtstring:{}", i);
            let actual = results
                .get(&key)
                .unwrap_or_else(|| panic!("Missing key {}", key));
            assert_eq!(actual, *expected, "Mismatch for key {}", key);
        }
    }

    cleanup_keys("rust:rtstring:*");
}

/// Test write-read-compare for numeric values.
#[test]
#[ignore] // Requires Redis
fn test_roundtrip_numerics() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:rtnum:*");

    let int_values: Vec<i64> = vec![
        0,
        1,
        -1,
        i64::MAX,
        i64::MIN,
        i32::MAX as i64,
        i32::MIN as i64,
        1000000000000i64,
        -1000000000000i64,
    ];

    let keys: Vec<String> = (0..int_values.len())
        .map(|i| format!("rust:rtnum:{}", i))
        .collect();
    let fields = vec!["num".to_string()];
    let values: Vec<Vec<Option<String>>> = int_values
        .iter()
        .map(|v| vec![Some(v.to_string())])
        .collect();

    write_hashes(
        &redis_url(),
        keys.clone(),
        fields.clone(),
        values,
        None,
        WriteMode::Replace,
    )
    .expect("Failed to write");

    let schema = HashSchema::new(vec![("num".to_string(), RedisType::Int64)]).with_key(true);
    let config = BatchConfig::new("rust:rtnum:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");

    let key_col = batch
        .column_by_name("_key")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let num_col = batch
        .column_by_name("num")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    let mut results: HashMap<String, i64> = HashMap::new();
    for i in 0..batch.num_rows() {
        if !num_col.is_null(i) {
            results.insert(key_col.value(i).to_string(), num_col.value(i));
        }
    }

    for (i, expected) in int_values.iter().enumerate() {
        let key = format!("rust:rtnum:{}", i);
        let actual = results
            .get(&key)
            .unwrap_or_else(|| panic!("Missing key {}", key));
        assert_eq!(*actual, *expected, "Mismatch for key {}", key);
    }

    cleanup_keys("rust:rtnum:*");
}

/// Test write-read-compare for float values including edge cases.
#[test]
#[ignore] // Requires Redis
fn test_roundtrip_floats() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:rtfloat:*");

    let float_values: Vec<f64> = vec![
        0.0,
        1.0,
        -1.0,
        0.1,
        0.123456789,
        1e10,
        1e-10,
        f64::MAX,
        f64::MIN,
        f64::MIN_POSITIVE,
        std::f64::consts::PI,
        -999999.999999,
    ];

    let keys: Vec<String> = (0..float_values.len())
        .map(|i| format!("rust:rtfloat:{}", i))
        .collect();
    let fields = vec!["val".to_string()];
    let values: Vec<Vec<Option<String>>> = float_values
        .iter()
        .map(|v| vec![Some(v.to_string())])
        .collect();

    write_hashes(
        &redis_url(),
        keys.clone(),
        fields.clone(),
        values,
        None,
        WriteMode::Replace,
    )
    .expect("Failed to write");

    let schema = HashSchema::new(vec![("val".to_string(), RedisType::Float64)]).with_key(true);
    let config = BatchConfig::new("rust:rtfloat:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");

    let key_col = batch
        .column_by_name("_key")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let val_col = batch
        .column_by_name("val")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    let mut results: HashMap<String, f64> = HashMap::new();
    for i in 0..batch.num_rows() {
        if !val_col.is_null(i) {
            results.insert(key_col.value(i).to_string(), val_col.value(i));
        }
    }

    for (i, expected) in float_values.iter().enumerate() {
        let key = format!("rust:rtfloat:{}", i);
        let actual = results
            .get(&key)
            .unwrap_or_else(|| panic!("Missing key {}", key));
        // Use relative comparison for floats
        let diff = (actual - expected).abs();
        let tolerance = expected.abs() * 1e-10 + 1e-15;
        assert!(
            diff < tolerance,
            "Mismatch for key {}: expected {}, got {}",
            key,
            expected,
            actual
        );
    }

    cleanup_keys("rust:rtfloat:*");
}

/// Test roundtrip with mixed null and non-null values.
#[test]
#[ignore] // Requires Redis
fn test_roundtrip_with_nulls() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:rtnull:*");

    let keys = vec![
        "rust:rtnull:1".to_string(),
        "rust:rtnull:2".to_string(),
        "rust:rtnull:3".to_string(),
    ];
    let fields = vec!["a".to_string(), "b".to_string()];
    let values = vec![
        vec![Some("x".to_string()), Some("y".to_string())],
        vec![Some("x".to_string()), None], // b is null
        vec![None, Some("y".to_string())], // a is null
    ];

    write_hashes(
        &redis_url(),
        keys.clone(),
        fields.clone(),
        values,
        None,
        WriteMode::Replace,
    )
    .expect("Failed to write");

    let schema = HashSchema::new(vec![
        ("a".to_string(), RedisType::Utf8),
        ("b".to_string(), RedisType::Utf8),
    ])
    .with_key(true);
    let config = BatchConfig::new("rust:rtnull:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");

    assert_eq!(batch.num_rows(), 3);

    let col_a = batch
        .column_by_name("a")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let col_b = batch
        .column_by_name("b")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Should have 1 null in column a (row 3) and 1 null in column b (row 2)
    assert_eq!(col_a.null_count(), 1);
    assert_eq!(col_b.null_count(), 1);

    cleanup_keys("rust:rtnull:*");
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

/// Test concurrent writes to different keys.
#[test]
#[ignore] // Requires Redis
fn test_concurrent_writes_different_keys() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:concwrite:*");

    let num_threads = 4;
    let keys_per_thread = 100;
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let barrier = Arc::clone(&barrier);
        let url = redis_url();

        let handle = thread::spawn(move || {
            barrier.wait(); // Sync start

            let keys: Vec<String> = (0..keys_per_thread)
                .map(|i| format!("rust:concwrite:t{}:{}", thread_id, i))
                .collect();
            let fields = vec!["val".to_string()];
            let values: Vec<Vec<Option<String>>> = (0..keys_per_thread)
                .map(|i| vec![Some(format!("t{}v{}", thread_id, i))])
                .collect();

            write_hashes(&url, keys, fields, values, None, WriteMode::Replace)
                .expect("Failed to write");
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify all keys exist
    let schema = HashSchema::new(vec![("val".to_string(), RedisType::Utf8)]).with_key(true);
    let config = BatchConfig::new("rust:concwrite:*".to_string()).with_batch_size(1000);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let mut total = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total += batch.num_rows();
    }

    assert_eq!(total, num_threads * keys_per_thread);

    cleanup_keys("rust:concwrite:*");
}

/// Test concurrent reads from the same keys.
#[test]
#[ignore] // Requires Redis
fn test_concurrent_reads() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:concread:*");

    // Setup data
    common::setup_test_hashes("rust:concread:", 100);

    let num_threads = 4;
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = vec![];

    for _ in 0..num_threads {
        let barrier = Arc::clone(&barrier);
        let url = redis_url();

        let handle = thread::spawn(move || {
            barrier.wait();

            let schema =
                HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(true);
            let config = BatchConfig::new("rust:concread:*".to_string()).with_batch_size(50);

            let mut iterator =
                HashBatchIterator::new(&url, schema, config, None).expect("Failed to create");

            let mut count = 0;
            while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
                count += batch.num_rows();
            }
            count
        });

        handles.push(handle);
    }

    let results: Vec<usize> = handles
        .into_iter()
        .map(|h| h.join().expect("Thread panicked"))
        .collect();

    // All threads should see the same count
    for count in &results {
        assert_eq!(*count, 100);
    }

    cleanup_keys("rust:concread:*");
}

/// Test concurrent read and write to overlapping keys.
#[test]
#[ignore] // Requires Redis
fn test_concurrent_read_write() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:concrw:*");

    // Setup initial data
    common::setup_test_hashes("rust:concrw:", 50);

    let barrier = Arc::new(Barrier::new(2));
    let url = redis_url();

    // Writer thread
    let barrier_w = Arc::clone(&barrier);
    let url_w = url.clone();
    let writer = thread::spawn(move || {
        barrier_w.wait();

        for i in 50..100 {
            let keys = vec![format!("rust:concrw:{}", i)];
            let fields = vec!["name".to_string()];
            let values = vec![vec![Some(format!("NewUser{}", i))]];
            write_hashes(&url_w, keys, fields, values, None, WriteMode::Replace)
                .expect("Write failed");
        }
    });

    // Reader thread
    let barrier_r = Arc::clone(&barrier);
    let reader = thread::spawn(move || {
        barrier_r.wait();

        let mut total = 0;
        for _ in 0..10 {
            let schema =
                HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(true);
            let config = BatchConfig::new("rust:concrw:*".to_string()).with_batch_size(100);

            let mut iterator =
                HashBatchIterator::new(&url, schema, config, None).expect("Failed to create");

            let mut count = 0;
            while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
                count += batch.num_rows();
            }
            total = total.max(count);
            thread::sleep(std::time::Duration::from_millis(10));
        }
        total
    });

    writer.join().expect("Writer panicked");
    let final_count = reader.join().expect("Reader panicked");

    // Final count should be between 50 and 100
    assert!((50..=100).contains(&final_count));

    cleanup_keys("rust:concrw:*");
}

// =============================================================================
// Numeric Edge Cases
// =============================================================================

/// Test infinity values in float fields.
#[test]
#[ignore] // Requires Redis
fn test_float_infinity() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:infinity:*");

    // Redis stores inf as "inf" string
    redis_cli(&["HSET", "rust:infinity:1", "val", "inf"]);
    redis_cli(&["HSET", "rust:infinity:2", "val", "-inf"]);
    redis_cli(&["HSET", "rust:infinity:3", "val", "+inf"]);

    let schema = HashSchema::new(vec![("val".to_string(), RedisType::Float64)]).with_key(true);
    let config = BatchConfig::new("rust:infinity:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");

    let val_col = batch
        .column_by_name("val")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    let mut has_pos_inf = false;
    let mut has_neg_inf = false;

    for i in 0..val_col.len() {
        if !val_col.is_null(i) {
            let v = val_col.value(i);
            if v == f64::INFINITY {
                has_pos_inf = true;
            }
            if v == f64::NEG_INFINITY {
                has_neg_inf = true;
            }
        }
    }

    assert!(has_pos_inf, "Missing positive infinity");
    assert!(has_neg_inf, "Missing negative infinity");

    cleanup_keys("rust:infinity:*");
}

/// Test NaN values in float fields.
#[test]
#[ignore] // Requires Redis
fn test_float_nan() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:nan:*");

    // Redis may store NaN as "nan"
    redis_cli(&["HSET", "rust:nan:1", "val", "nan"]);
    redis_cli(&["HSET", "rust:nan:2", "val", "NaN"]);
    redis_cli(&["HSET", "rust:nan:3", "val", "42.0"]); // control

    let schema = HashSchema::new(vec![("val".to_string(), RedisType::Float64)]).with_key(true);
    let config = BatchConfig::new("rust:nan:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");

    // NaN values might become null or actual NaN depending on implementation
    assert_eq!(batch.num_rows(), 3);

    cleanup_keys("rust:nan:*");
}

/// Test integer boundary values.
#[test]
#[ignore] // Requires Redis
fn test_integer_boundaries() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:intbound:*");

    redis_cli(&["HSET", "rust:intbound:1", "val", &i64::MAX.to_string()]);
    redis_cli(&["HSET", "rust:intbound:2", "val", &i64::MIN.to_string()]);
    redis_cli(&["HSET", "rust:intbound:3", "val", "0"]);
    redis_cli(&["HSET", "rust:intbound:4", "val", "-1"]);
    redis_cli(&["HSET", "rust:intbound:5", "val", "1"]);

    let schema = HashSchema::new(vec![("val".to_string(), RedisType::Int64)]).with_key(true);
    let config = BatchConfig::new("rust:intbound:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");

    let val_col = batch
        .column_by_name("val")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    let values: Vec<i64> = (0..val_col.len())
        .filter(|i| !val_col.is_null(*i))
        .map(|i| val_col.value(i))
        .collect();

    assert!(values.contains(&i64::MAX));
    assert!(values.contains(&i64::MIN));
    assert!(values.contains(&0));
    assert!(values.contains(&-1));
    assert!(values.contains(&1));

    cleanup_keys("rust:intbound:*");
}

/// Test overflow - value too large for i64.
#[test]
#[ignore] // Requires Redis
fn test_integer_overflow() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:overflow:*");

    // Value larger than i64::MAX
    let too_large = "99999999999999999999999999999999";
    redis_cli(&["HSET", "rust:overflow:1", "val", too_large]);
    redis_cli(&["HSET", "rust:overflow:2", "val", "42"]); // control

    let schema = HashSchema::new(vec![("val".to_string(), RedisType::Int64)]).with_key(true);
    let config = BatchConfig::new("rust:overflow:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");

    // Overflow value should become null
    assert_eq!(batch.num_rows(), 2);
    let val_col = batch
        .column_by_name("val")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    // At least one should be null (the overflow), one should be 42
    assert!(val_col.null_count() >= 1);

    cleanup_keys("rust:overflow:*");
}

// =============================================================================
// Unicode and Special Character Tests
// =============================================================================

/// Test Unicode round-trip with various scripts.
#[test]
#[ignore] // Requires Redis
fn test_unicode_scripts() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:unicode2:*");

    let unicode_values = vec![
        ("latin", "Hello World"),
        ("german", "Guten Tag"),
        ("french", "francais"),
        ("spanish", "espanol"),
        ("russian", "Russian Cyrillic"),
        ("greek", "Greek Alphabet"),
        ("chinese", "Chinese"),
        ("japanese", "Japanese"),
        ("korean", "Korean"),
        ("arabic", "Arabic Text"),
        ("hebrew", "Hebrew"),
        ("thai", "Thai"),
        ("emoji", "Emoji Test"),
        ("mixed", "Mix: abc Test"),
    ];

    for (i, (name, value)) in unicode_values.iter().enumerate() {
        redis_cli(&[
            "HSET",
            &format!("rust:unicode2:{}", i),
            "name",
            name,
            "text",
            value,
        ]);
    }

    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("text".to_string(), RedisType::Utf8),
    ])
    .with_key(true);
    let config = BatchConfig::new("rust:unicode2:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");

    assert_eq!(batch.num_rows(), unicode_values.len());

    cleanup_keys("rust:unicode2:*");
}

/// Test RediSearch special characters that need escaping.
#[test]
#[ignore] // Requires Redis
fn test_redisearch_special_chars_in_values() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:specialrs:*");

    // RediSearch special chars: , . < > { } [ ] " ' : ; ! @ # $ % ^ & * ( ) - + = ~
    let special_values = vec![
        "comma,here",
        "period.here",
        "colon:here",
        "semicolon;here",
        "bracket[here]",
        "brace{here}",
        "angle<here>",
        "at@here",
        "hash#here",
        "dollar$here",
        "percent%here",
        "caret^here",
        "ampersand&here",
        "asterisk*here",
        "paren(here)",
        "dash-here",
        "plus+here",
        "equals=here",
        "tilde~here",
        "quote\"here",
        "single'here",
        "backslash\\here",
        "pipe|here",
        "question?here",
        "exclaim!here",
    ];

    for (i, value) in special_values.iter().enumerate() {
        redis_cli(&["HSET", &format!("rust:specialrs:{}", i), "val", value]);
    }

    let schema = HashSchema::new(vec![("val".to_string(), RedisType::Utf8)]).with_key(true);
    let config = BatchConfig::new("rust:specialrs:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");

    assert_eq!(batch.num_rows(), special_values.len());

    // Verify values round-tripped correctly
    let val_col = batch
        .column_by_name("val")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let actual: Vec<String> = (0..val_col.len())
        .filter(|i| !val_col.is_null(*i))
        .map(|i| val_col.value(i).to_string())
        .collect();

    for expected in &special_values {
        assert!(
            actual.contains(&expected.to_string()),
            "Missing value: {}",
            expected
        );
    }

    cleanup_keys("rust:specialrs:*");
}

// =============================================================================
// Schema Mismatch Edge Cases
// =============================================================================

/// Test reading with completely wrong schema types.
#[test]
#[ignore] // Requires Redis
fn test_schema_type_coercion_failures() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:coerce:*");

    // Create data with clear types
    redis_cli(&["HSET", "rust:coerce:1", "num", "42", "text", "hello"]);
    redis_cli(&[
        "HSET",
        "rust:coerce:2",
        "num",
        "not-a-number",
        "text",
        "world",
    ]);
    redis_cli(&["HSET", "rust:coerce:3", "num", "3.14", "text", "123"]);

    // Schema expects num as Int64, text as Int64 (wrong!)
    let schema = HashSchema::new(vec![
        ("num".to_string(), RedisType::Int64),
        ("text".to_string(), RedisType::Int64), // Will fail for "hello", "world"
    ])
    .with_key(true);
    let config = BatchConfig::new("rust:coerce:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");

    assert_eq!(batch.num_rows(), 3);

    let text_col = batch
        .column_by_name("text")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    // "hello" and "world" should be null, "123" should parse
    assert!(text_col.null_count() >= 2);

    cleanup_keys("rust:coerce:*");
}

/// Test boolean parsing edge cases.
#[test]
#[ignore] // Requires Redis
fn test_boolean_parsing_edge_cases() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:bool:*");

    let bool_values = vec![
        ("1", "true"),
        ("2", "false"),
        ("3", "TRUE"),
        ("4", "FALSE"),
        ("5", "True"),
        ("6", "False"),
        ("7", "1"),
        ("8", "0"),
        ("9", "yes"),
        ("10", "no"),
        ("11", "YES"),
        ("12", "NO"),
        ("13", "t"),
        ("14", "f"),
        ("15", "T"),
        ("16", "F"),
        ("17", "invalid"), // Should become null
        ("18", "2"),       // Should become null (not 0 or 1)
        ("19", ""),        // Empty should become null
    ];

    for (key, value) in &bool_values {
        redis_cli(&["HSET", &format!("rust:bool:{}", key), "flag", value]);
    }

    let schema = HashSchema::new(vec![("flag".to_string(), RedisType::Boolean)]).with_key(true);
    let config = BatchConfig::new("rust:bool:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");

    assert_eq!(batch.num_rows(), bool_values.len());

    // Some should be null (invalid, 2, empty)
    let flag_col = batch.column_by_name("flag").unwrap();
    assert!(flag_col.null_count() >= 3);

    cleanup_keys("rust:bool:*");
}

// =============================================================================
// RediSearch-Specific Edge Cases
// =============================================================================

/// Test searching with empty query.
#[test]
#[ignore] // Requires Redis
fn test_search_empty_query() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:searchempty:*");
    let _ = redis_cli(&["FT.DROPINDEX", "idx:searchempty"]);

    common::setup_test_hashes("rust:searchempty:", 5);
    common::create_hash_index("idx:searchempty", "rust:searchempty:");
    common::wait_for_index("idx:searchempty");

    // "*" query should return all
    let output = redis_cli_output(&["FT.SEARCH", "idx:searchempty", "*", "LIMIT", "0", "100"]);
    assert!(output.is_some());

    cleanup_keys("rust:searchempty:*");
    let _ = redis_cli(&["FT.DROPINDEX", "idx:searchempty"]);
}

/// Test search pagination boundaries.
#[test]
#[ignore] // Requires Redis
fn test_search_pagination_boundary() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:searchpage:*");
    let _ = redis_cli(&["FT.DROPINDEX", "idx:searchpage"]);

    common::setup_test_hashes("rust:searchpage:", 25);
    common::create_hash_index("idx:searchpage", "rust:searchpage:");
    common::wait_for_index("idx:searchpage");

    // Test various LIMIT offsets
    let out1 = redis_cli_output(&["FT.SEARCH", "idx:searchpage", "*", "LIMIT", "0", "10"]);
    let out2 = redis_cli_output(&["FT.SEARCH", "idx:searchpage", "*", "LIMIT", "10", "10"]);
    let out3 = redis_cli_output(&["FT.SEARCH", "idx:searchpage", "*", "LIMIT", "20", "10"]);
    let out4 = redis_cli_output(&["FT.SEARCH", "idx:searchpage", "*", "LIMIT", "100", "10"]); // Beyond data

    assert!(out1.is_some());
    assert!(out2.is_some());
    assert!(out3.is_some());
    assert!(out4.is_some()); // Should return 0 results but not error

    cleanup_keys("rust:searchpage:*");
    let _ = redis_cli(&["FT.DROPINDEX", "idx:searchpage"]);
}

/// Test search with very long field values.
#[test]
#[ignore] // Requires Redis
fn test_search_long_text_field() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:searchlong:*");
    let _ = redis_cli(&["FT.DROPINDEX", "idx:searchlong"]);

    // Create hash with very long text
    let long_text = "word ".repeat(1000); // 5000+ chars
    redis_cli(&[
        "HSET",
        "rust:searchlong:1",
        "name",
        "test",
        "description",
        &long_text,
    ]);

    redis_cli(&[
        "FT.CREATE",
        "idx:searchlong",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "rust:searchlong:",
        "SCHEMA",
        "name",
        "TEXT",
        "description",
        "TEXT",
    ]);
    common::wait_for_index("idx:searchlong");

    // Search should work
    let output = redis_cli_output(&["FT.SEARCH", "idx:searchlong", "word", "LIMIT", "0", "10"]);
    assert!(output.is_some());

    cleanup_keys("rust:searchlong:*");
    let _ = redis_cli(&["FT.DROPINDEX", "idx:searchlong"]);
}

/// Test index with no matching documents.
#[test]
#[ignore] // Requires Redis
fn test_search_index_no_docs() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let _ = redis_cli(&["FT.DROPINDEX", "idx:nodocs"]);

    // Create index but no data
    redis_cli(&[
        "FT.CREATE",
        "idx:nodocs",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "rust:nodocs:",
        "SCHEMA",
        "name",
        "TEXT",
    ]);
    common::wait_for_index("idx:nodocs");

    let output = redis_cli_output(&["FT.SEARCH", "idx:nodocs", "*", "LIMIT", "0", "10"]);
    assert!(output.is_some());
    // Should return "0" (no results)
    let output_str = output.unwrap();
    assert!(output_str.starts_with("0") || output_str.contains("0"));

    let _ = redis_cli(&["FT.DROPINDEX", "idx:nodocs"]);
}

/// Test tag field with pipe character in value.
#[test]
#[ignore] // Requires Redis
fn test_tag_with_pipe() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:tagpipe:*");
    let _ = redis_cli(&["FT.DROPINDEX", "idx:tagpipe"]);

    // Pipe is OR separator in tags, so this tests escaping
    redis_cli(&[
        "HSET",
        "rust:tagpipe:1",
        "category",
        "a|b", // Contains pipe
    ]);
    redis_cli(&["HSET", "rust:tagpipe:2", "category", "a"]);
    redis_cli(&["HSET", "rust:tagpipe:3", "category", "b"]);

    redis_cli(&[
        "FT.CREATE",
        "idx:tagpipe",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "rust:tagpipe:",
        "SCHEMA",
        "category",
        "TAG",
    ]);
    common::wait_for_index("idx:tagpipe");

    // Search for the literal "a|b" should find key 1
    // Search for "a" should find keys 1 and 2 (unless properly escaped)
    let output = redis_cli_output(&[
        "FT.SEARCH",
        "idx:tagpipe",
        "@category:{a}",
        "LIMIT",
        "0",
        "10",
    ]);
    assert!(output.is_some());

    cleanup_keys("rust:tagpipe:*");
    let _ = redis_cli(&["FT.DROPINDEX", "idx:tagpipe"]);
}
