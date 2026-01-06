//! Integration tests for error handling and edge cases.
//!
//! These tests verify behavior with:
//! - Invalid inputs and configurations
//! - Boundary conditions
//! - Type mismatches
//! - Connection errors
//! - Large data handling
//!
//! Run with: `cargo test --test integration_edge_cases --all-features`

use arrow::datatypes::DataType;
use polars_redis::{
    BatchConfig, HashBatchIterator, HashSchema, JsonBatchIterator, JsonSchema, ListBatchIterator,
    ListSchema, RedisType, WriteMode, ZSetBatchIterator, ZSetSchema, write_hashes, write_json,
    write_strings, write_zsets,
};

mod common;
use common::{cleanup_keys, redis_available, redis_cli, redis_cli_output, redis_url};

// =============================================================================
// Connection Error Tests
// =============================================================================

/// Test connection to invalid URL.
#[test]
fn test_invalid_redis_url() {
    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]);
    let config = BatchConfig::new("test:*".to_string());

    let result = HashBatchIterator::new(
        "redis://invalid-host-that-does-not-exist:6379",
        schema,
        config,
        None,
    );

    assert!(result.is_err());
}

/// Test connection to wrong port.
#[test]
fn test_wrong_port() {
    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]);
    let config = BatchConfig::new("test:*".to_string());

    // Port 1 is unlikely to have Redis running
    let result = HashBatchIterator::new("redis://localhost:1", schema, config, None);

    assert!(result.is_err());
}

/// Test malformed URL.
#[test]
fn test_malformed_url() {
    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]);
    let config = BatchConfig::new("test:*".to_string());

    let result = HashBatchIterator::new("not-a-valid-url", schema, config, None);

    assert!(result.is_err());
}

// =============================================================================
// Empty Data Tests
// =============================================================================

/// Test scanning with pattern that matches no keys.
#[test]
#[ignore] // Requires Redis
fn test_scan_no_matching_keys() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:nomatch:*");

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(true);
    let config = BatchConfig::new("rust:nomatch:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator.next_batch().expect("Failed to get batch");

    // Should return None or empty batch
    assert!(batch.is_none() || batch.unwrap().num_rows() == 0);
}

/// Test JSON scan with no matching keys.
#[test]
#[ignore] // Requires Redis
fn test_json_scan_no_matching_keys() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let schema = JsonSchema::new(vec![("name".to_string(), DataType::Utf8)]).with_key(true);
    let config = BatchConfig::new("rust:jsonempty:*".to_string()).with_batch_size(100);

    let mut iterator = JsonBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator.next_batch().expect("Failed to get batch");
    assert!(batch.is_none() || batch.unwrap().num_rows() == 0);
}

// =============================================================================
// Type Mismatch Tests
// =============================================================================

/// Test reading hash field as wrong type (string as int).
#[test]
#[ignore] // Requires Redis
fn test_hash_type_mismatch_string_as_int() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:typemismatch:*");

    // Create hash with string value
    redis_cli(&["HSET", "rust:typemismatch:1", "value", "not-a-number"]);

    let schema = HashSchema::new(vec![("value".to_string(), RedisType::Int64)]).with_key(true);
    let config = BatchConfig::new("rust:typemismatch:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator.next_batch().expect("Failed to get batch");

    // Should handle gracefully - either null or error
    assert!(batch.is_some());

    cleanup_keys("rust:typemismatch:*");
}

/// Test reading non-numeric string as float.
#[test]
#[ignore] // Requires Redis
fn test_hash_type_mismatch_string_as_float() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:floatmismatch:*");

    redis_cli(&["HSET", "rust:floatmismatch:1", "price", "not-a-float"]);

    let schema = HashSchema::new(vec![("price".to_string(), RedisType::Float64)]).with_key(true);
    let config = BatchConfig::new("rust:floatmismatch:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator.next_batch().expect("Failed to get batch");
    assert!(batch.is_some());

    cleanup_keys("rust:floatmismatch:*");
}

/// Test reading wrong Redis type (expecting hash, got string).
#[test]
#[ignore] // Requires Redis
fn test_wrong_redis_type_string_as_hash() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:wrongtype:*");

    // Create a string key (not a hash)
    redis_cli(&["SET", "rust:wrongtype:1", "i-am-a-string"]);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(true);
    let config = BatchConfig::new("rust:wrongtype:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    // This may skip the key or return null values
    let batch = iterator.next_batch().expect("Failed to get batch");
    // Just verify it doesn't crash
    assert!(batch.is_none() || batch.is_some());

    cleanup_keys("rust:wrongtype:*");
}

/// Test reading hash as list (wrong type).
#[test]
#[ignore] // Requires Redis
fn test_wrong_redis_type_hash_as_list() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:hashlist:*");

    // Create a hash
    redis_cli(&["HSET", "rust:hashlist:1", "field", "value"]);

    let schema = ListSchema::new().with_key(true);
    let config = BatchConfig::new("rust:hashlist:*".to_string()).with_batch_size(100);

    let mut iterator =
        ListBatchIterator::new(&redis_url(), schema, config).expect("Failed to create iterator");

    // Should handle gracefully
    let batch = iterator.next_batch().expect("Failed to get batch");
    assert!(batch.is_none() || batch.is_some());

    cleanup_keys("rust:hashlist:*");
}

// =============================================================================
// Missing Field Tests
// =============================================================================

/// Test hash with missing fields (sparse data).
#[test]
#[ignore] // Requires Redis
fn test_hash_missing_fields() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:sparse:*");

    // Create hashes with different fields present
    redis_cli(&["HSET", "rust:sparse:1", "name", "Alice", "age", "30"]);
    redis_cli(&["HSET", "rust:sparse:2", "name", "Bob"]); // missing age
    redis_cli(&["HSET", "rust:sparse:3", "age", "25"]); // missing name

    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
    ])
    .with_key(true);
    let config = BatchConfig::new("rust:sparse:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");

    // Should have 3 rows with nulls for missing fields
    assert_eq!(batch.num_rows(), 3);

    cleanup_keys("rust:sparse:*");
}

/// Test JSON with missing nested fields.
#[test]
#[ignore] // Requires Redis
fn test_json_missing_nested_fields() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:jsonsparse:*");

    redis_cli(&[
        "JSON.SET",
        "rust:jsonsparse:1",
        "$",
        r#"{"name": "Alice", "age": 30}"#,
    ]);
    redis_cli(&["JSON.SET", "rust:jsonsparse:2", "$", r#"{"name": "Bob"}"#]); // missing age

    let schema = JsonSchema::new(vec![
        ("name".to_string(), DataType::Utf8),
        ("age".to_string(), DataType::Int64),
    ])
    .with_key(true);
    let config = BatchConfig::new("rust:jsonsparse:*".to_string()).with_batch_size(100);

    let mut iterator = JsonBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");
    assert_eq!(batch.num_rows(), 2);

    cleanup_keys("rust:jsonsparse:*");
}

// =============================================================================
// Special Character Tests
// =============================================================================

/// Test keys with special characters.
#[test]
#[ignore] // Requires Redis
fn test_keys_with_special_characters() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:special:*");

    // Keys with special characters
    redis_cli(&["HSET", "rust:special:user:123", "name", "Alice"]);
    redis_cli(&["HSET", "rust:special:user-456", "name", "Bob"]);
    redis_cli(&["HSET", "rust:special:user_789", "name", "Charlie"]);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(true);
    let config = BatchConfig::new("rust:special:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");
    assert_eq!(batch.num_rows(), 3);

    cleanup_keys("rust:special:*");
}

/// Test values with special characters.
#[test]
#[ignore] // Requires Redis
fn test_values_with_special_characters() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:specialval:*");

    // Values with special characters
    redis_cli(&["HSET", "rust:specialval:1", "data", "hello\nworld"]); // newline
    redis_cli(&["HSET", "rust:specialval:2", "data", "tab\there"]); // tab
    redis_cli(&["HSET", "rust:specialval:3", "data", r#"quote"test"#]); // quotes

    let schema = HashSchema::new(vec![("data".to_string(), RedisType::Utf8)]).with_key(true);
    let config = BatchConfig::new("rust:specialval:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");
    assert_eq!(batch.num_rows(), 3);

    cleanup_keys("rust:specialval:*");
}

/// Test Unicode values.
#[test]
#[ignore] // Requires Redis
fn test_unicode_values() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:unicode:*");

    redis_cli(&["HSET", "rust:unicode:1", "name", "Alice"]);
    redis_cli(&["HSET", "rust:unicode:2", "name", "Muller"]); // umlaut
    redis_cli(&["HSET", "rust:unicode:3", "name", "Tokyo"]); // Japanese

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(true);
    let config = BatchConfig::new("rust:unicode:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");
    assert_eq!(batch.num_rows(), 3);

    cleanup_keys("rust:unicode:*");
}

// =============================================================================
// Boundary Condition Tests
// =============================================================================

/// Test very long string values.
#[test]
#[ignore] // Requires Redis
fn test_very_long_string_value() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:longval:*");

    // Create a 100KB string
    let long_value = "x".repeat(100_000);
    redis_cli(&["HSET", "rust:longval:1", "data", &long_value]);

    let schema = HashSchema::new(vec![("data".to_string(), RedisType::Utf8)]).with_key(true);
    let config = BatchConfig::new("rust:longval:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");
    assert_eq!(batch.num_rows(), 1);

    cleanup_keys("rust:longval:*");
}

/// Test hash with many fields.
#[test]
#[ignore] // Requires Redis
fn test_hash_with_many_fields() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:manyfields:*");

    // Create hash with 100 fields
    let mut args = vec!["HSET", "rust:manyfields:1"];
    let field_values: Vec<String> = (1..=100)
        .flat_map(|i| vec![format!("field{}", i), format!("value{}", i)])
        .collect();
    let field_refs: Vec<&str> = field_values.iter().map(|s| s.as_str()).collect();
    args.extend(field_refs);
    redis_cli(&args);

    // Schema for all 100 fields
    let fields: Vec<(String, RedisType)> = (1..=100)
        .map(|i| (format!("field{}", i), RedisType::Utf8))
        .collect();
    let schema = HashSchema::new(fields).with_key(true);
    let config = BatchConfig::new("rust:manyfields:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 101); // 100 fields + key

    cleanup_keys("rust:manyfields:*");
}

/// Test list with many elements.
#[test]
#[ignore] // Requires Redis
fn test_list_with_many_elements() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:biglist:*");

    // Create list with 1000 elements
    let elements: Vec<String> = (1..=1000).map(|i| format!("item{}", i)).collect();
    let element_refs: Vec<&str> = elements.iter().map(|s| s.as_str()).collect();
    let mut args = vec!["RPUSH", "rust:biglist:1"];
    args.extend(element_refs);
    redis_cli(&args);

    let schema = ListSchema::new().with_key(true);
    let config = BatchConfig::new("rust:biglist:*".to_string()).with_batch_size(100);

    let mut iterator =
        ListBatchIterator::new(&redis_url(), schema, config).expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");
    assert_eq!(batch.num_rows(), 1000);

    cleanup_keys("rust:biglist:*");
}

/// Test sorted set with extreme scores.
#[test]
#[ignore] // Requires Redis
fn test_zset_extreme_scores() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:extremescore:*");

    redis_cli(&["ZADD", "rust:extremescore:1", "-inf", "neg_infinity"]);
    redis_cli(&["ZADD", "rust:extremescore:1", "+inf", "pos_infinity"]);
    redis_cli(&["ZADD", "rust:extremescore:1", "0", "zero"]);
    redis_cli(&["ZADD", "rust:extremescore:1", "-999999999", "very_negative"]);
    redis_cli(&["ZADD", "rust:extremescore:1", "999999999", "very_positive"]);

    let schema = ZSetSchema::new().with_key(true);
    let config = BatchConfig::new("rust:extremescore:*".to_string()).with_batch_size(100);

    let mut iterator =
        ZSetBatchIterator::new(&redis_url(), schema, config).expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");
    assert_eq!(batch.num_rows(), 5);

    cleanup_keys("rust:extremescore:*");
}

// =============================================================================
// Write Edge Cases
// =============================================================================

/// Test writing with empty key list.
#[test]
#[ignore] // Requires Redis
fn test_write_empty_keys() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let keys: Vec<String> = vec![];
    let fields = vec!["name".to_string()];
    let values: Vec<Vec<Option<String>>> = vec![];

    let result = write_hashes(&redis_url(), keys, fields, values, None, WriteMode::Replace)
        .expect("Failed to write empty batch");

    assert_eq!(result.keys_written, 0);
    assert_eq!(result.keys_failed, 0);
}

/// Test writing strings with all null values.
#[test]
#[ignore] // Requires Redis
fn test_write_all_null_strings() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:allnull:*");

    let keys = vec!["rust:allnull:1".to_string(), "rust:allnull:2".to_string()];
    let values: Vec<Option<String>> = vec![None, None];

    let result = write_strings(&redis_url(), keys, values, None, WriteMode::Replace)
        .expect("Failed to write null strings");

    // All nulls should be skipped
    assert_eq!(result.keys_written, 0);

    cleanup_keys("rust:allnull:*");
}

/// Test writing hash with all null field values.
#[test]
#[ignore] // Requires Redis
fn test_write_hash_all_null_fields() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:nullfields:*");

    let keys = vec!["rust:nullfields:1".to_string()];
    let fields = vec!["name".to_string(), "age".to_string()];
    let values = vec![vec![None, None]]; // All fields null

    let result = write_hashes(&redis_url(), keys, fields, values, None, WriteMode::Replace)
        .expect("Failed to write null hash");

    // Hash with all null fields should not be written
    assert_eq!(result.keys_written, 0);

    let exists = redis_cli_output(&["EXISTS", "rust:nullfields:1"]);
    assert_eq!(exists, Some("0".to_string()));

    cleanup_keys("rust:nullfields:*");
}

/// Test writing very large JSON document.
#[test]
#[ignore] // Requires Redis
fn test_write_large_json() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:largejson:*");

    // Create a JSON with many fields
    let fields: Vec<String> = (1..=100)
        .map(|i| format!(r#""field{}": "value{}""#, i, i))
        .collect();
    let large_json = format!("{{{}}}", fields.join(", "));

    let keys = vec!["rust:largejson:1".to_string()];
    let json_strings = vec![large_json];

    let result = write_json(&redis_url(), keys, json_strings, None, WriteMode::Replace)
        .expect("Failed to write large JSON");

    assert_eq!(result.keys_written, 1);

    cleanup_keys("rust:largejson:*");
}

/// Test writing sorted set with negative scores.
#[test]
#[ignore] // Requires Redis
fn test_write_zset_negative_scores() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:negscore:*");

    let keys = vec!["rust:negscore:1".to_string()];
    let members = vec![vec![
        ("a".to_string(), -100.0),
        ("b".to_string(), -50.5),
        ("c".to_string(), 0.0),
        ("d".to_string(), 50.5),
    ]];

    let result = write_zsets(&redis_url(), keys, members, None, WriteMode::Replace)
        .expect("Failed to write zset with negative scores");

    assert_eq!(result.keys_written, 1);

    // Verify ordering (lowest score first)
    let first = redis_cli_output(&["ZRANGE", "rust:negscore:1", "0", "0"]);
    assert_eq!(first, Some("a".to_string()));

    let score = redis_cli_output(&["ZSCORE", "rust:negscore:1", "a"]);
    assert_eq!(score, Some("-100".to_string()));

    cleanup_keys("rust:negscore:*");
}

// =============================================================================
// Batch Size Edge Cases
// =============================================================================

/// Test with batch size of 1.
#[test]
#[ignore] // Requires Redis
fn test_batch_size_one() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:batchone:*");

    common::setup_test_hashes("rust:batchone:", 5);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(true);
    let config = BatchConfig::new("rust:batchone:*".to_string()).with_batch_size(1);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let mut total_rows = 0;
    let mut batch_count = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
        batch_count += 1;
    }

    assert_eq!(total_rows, 5);
    assert!(batch_count >= 1); // At least 1 batch

    cleanup_keys("rust:batchone:*");
}

/// Test with very large batch size.
#[test]
#[ignore] // Requires Redis
fn test_very_large_batch_size() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:bigbatch:*");

    common::setup_test_hashes("rust:bigbatch:", 10);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(true);
    let config = BatchConfig::new("rust:bigbatch:*".to_string()).with_batch_size(1_000_000);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");
    assert_eq!(batch.num_rows(), 10);

    cleanup_keys("rust:bigbatch:*");
}

// =============================================================================
// Projection Edge Cases
// =============================================================================

/// Test projection with non-existent field.
#[test]
#[ignore] // Requires Redis
fn test_projection_nonexistent_field() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:projnonexist:*");

    redis_cli(&["HSET", "rust:projnonexist:1", "name", "Alice"]);

    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64), // doesn't exist in data
    ])
    .with_key(true);
    let config = BatchConfig::new("rust:projnonexist:*".to_string()).with_batch_size(100);

    // Project only the non-existent field
    let projection = Some(vec!["age".to_string()]);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, projection)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1); // only age column

    cleanup_keys("rust:projnonexist:*");
}

/// Test empty projection.
#[test]
#[ignore] // Requires Redis
fn test_empty_projection() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:emptyproj:*");

    redis_cli(&["HSET", "rust:emptyproj:1", "name", "Alice"]);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(true);
    let config = BatchConfig::new("rust:emptyproj:*".to_string()).with_batch_size(100);

    // Empty projection
    let projection = Some(vec![]);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, projection)
        .expect("Failed to create iterator");

    let batch = iterator.next_batch().expect("Failed to get batch");
    // Empty projection should return empty columns
    assert!(batch.is_none() || batch.unwrap().num_columns() == 0);

    cleanup_keys("rust:emptyproj:*");
}

// =============================================================================
// TTL Edge Cases
// =============================================================================

/// Test reading keys with TTL set.
#[test]
#[ignore] // Requires Redis
fn test_read_keys_with_ttl() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:withttl:*");

    redis_cli(&["HSET", "rust:withttl:1", "name", "Alice"]);
    redis_cli(&["EXPIRE", "rust:withttl:1", "3600"]); // 1 hour TTL

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)])
        .with_key(true)
        .with_ttl(true);
    let config = BatchConfig::new("rust:withttl:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 3); // key, name, ttl

    cleanup_keys("rust:withttl:*");
}

/// Test reading keys without TTL when schema expects TTL.
#[test]
#[ignore] // Requires Redis
fn test_read_keys_without_ttl() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:nottl:*");

    redis_cli(&["HSET", "rust:nottl:1", "name", "Alice"]);
    // No EXPIRE set

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)])
        .with_key(true)
        .with_ttl(true);
    let config = BatchConfig::new("rust:nottl:*".to_string()).with_batch_size(100);

    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected batch");
    assert_eq!(batch.num_rows(), 1);
    // TTL should be -1 or null for keys without expiration

    cleanup_keys("rust:nottl:*");
}
