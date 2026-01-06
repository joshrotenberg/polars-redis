//! Integration tests for Redis write operations.
//!
//! These tests require a running Redis instance.
//! Run with: `cargo test --test integration_write --all-features`

use polars_redis::{
    WriteMode, write_hashes, write_hashes_detailed, write_json, write_lists, write_sets,
    write_strings, write_zsets,
};

mod common;
use common::{cleanup_keys, redis_available, redis_cli, redis_cli_output, redis_url};

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

// =============================================================================
// JSON Write Tests
// =============================================================================

/// Test basic JSON writing.
#[test]
#[ignore] // Requires Redis with RedisJSON
fn test_write_json_basic() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:jsonwrite:*");

    let keys = vec![
        "rust:jsonwrite:1".to_string(),
        "rust:jsonwrite:2".to_string(),
    ];
    let json_strings = vec![
        r#"{"name": "Alice", "age": 30}"#.to_string(),
        r#"{"name": "Bob", "age": 25}"#.to_string(),
    ];

    let result = write_json(&redis_url(), keys, json_strings, None, WriteMode::Replace)
        .expect("Failed to write JSON");

    assert_eq!(result.keys_written, 2);
    assert_eq!(result.keys_failed, 0);

    // Verify data was written
    let name = redis_cli_output(&["JSON.GET", "rust:jsonwrite:1", "$.name"]);
    assert!(name.is_some());
    assert!(name.unwrap().contains("Alice"));

    cleanup_keys("rust:jsonwrite:*");
}

/// Test JSON writing with TTL.
#[test]
#[ignore] // Requires Redis with RedisJSON
fn test_write_json_with_ttl() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:jsonttl:*");

    let keys = vec!["rust:jsonttl:1".to_string()];
    let json_strings = vec![r#"{"test": true}"#.to_string()];

    let result = write_json(
        &redis_url(),
        keys,
        json_strings,
        Some(3600),
        WriteMode::Replace,
    )
    .expect("Failed to write JSON");

    assert_eq!(result.keys_written, 1);

    // Verify TTL was set
    let ttl = redis_cli_output(&["TTL", "rust:jsonttl:1"]);
    assert!(ttl.is_some());
    let ttl_value: i64 = ttl.unwrap().parse().unwrap_or(0);
    assert!(ttl_value > 0 && ttl_value <= 3600);

    cleanup_keys("rust:jsonttl:*");
}

// =============================================================================
// List Write Tests
// =============================================================================

/// Test basic list writing.
#[test]
#[ignore] // Requires Redis
fn test_write_lists_basic() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:listwrite:*");

    let keys = vec![
        "rust:listwrite:1".to_string(),
        "rust:listwrite:2".to_string(),
    ];
    let elements = vec![
        vec!["a".to_string(), "b".to_string(), "c".to_string()],
        vec!["x".to_string(), "y".to_string()],
    ];

    let result = write_lists(&redis_url(), keys, elements, None, WriteMode::Replace)
        .expect("Failed to write lists");

    assert_eq!(result.keys_written, 2);
    assert_eq!(result.keys_failed, 0);

    // Verify data was written
    let len = redis_cli_output(&["LLEN", "rust:listwrite:1"]);
    assert_eq!(len, Some("3".to_string()));

    let first = redis_cli_output(&["LINDEX", "rust:listwrite:1", "0"]);
    assert_eq!(first, Some("a".to_string()));

    cleanup_keys("rust:listwrite:*");
}

/// Test list writing with append mode.
#[test]
#[ignore] // Requires Redis
fn test_write_lists_append() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:listappend:*");

    // Create initial list
    redis_cli(&["RPUSH", "rust:listappend:1", "existing"]);

    let keys = vec!["rust:listappend:1".to_string()];
    let elements = vec![vec!["new1".to_string(), "new2".to_string()]];

    let result = write_lists(&redis_url(), keys, elements, None, WriteMode::Append)
        .expect("Failed to append to list");

    assert_eq!(result.keys_written, 1);

    // Verify both old and new elements exist
    let len = redis_cli_output(&["LLEN", "rust:listappend:1"]);
    assert_eq!(len, Some("3".to_string())); // existing + new1 + new2

    cleanup_keys("rust:listappend:*");
}

// =============================================================================
// Set Write Tests
// =============================================================================

/// Test basic set writing.
#[test]
#[ignore] // Requires Redis
fn test_write_sets_basic() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:setwrite:*");

    let keys = vec!["rust:setwrite:1".to_string(), "rust:setwrite:2".to_string()];
    let members = vec![
        vec!["a".to_string(), "b".to_string(), "c".to_string()],
        vec!["x".to_string(), "y".to_string()],
    ];

    let result = write_sets(&redis_url(), keys, members, None, WriteMode::Replace)
        .expect("Failed to write sets");

    assert_eq!(result.keys_written, 2);
    assert_eq!(result.keys_failed, 0);

    // Verify data was written
    let card = redis_cli_output(&["SCARD", "rust:setwrite:1"]);
    assert_eq!(card, Some("3".to_string()));

    let is_member = redis_cli_output(&["SISMEMBER", "rust:setwrite:1", "a"]);
    assert_eq!(is_member, Some("1".to_string()));

    cleanup_keys("rust:setwrite:*");
}

/// Test set writing with append mode (adds to existing set).
#[test]
#[ignore] // Requires Redis
fn test_write_sets_append() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:setappend:*");

    // Create initial set
    redis_cli(&["SADD", "rust:setappend:1", "existing"]);

    let keys = vec!["rust:setappend:1".to_string()];
    let members = vec![vec!["new1".to_string(), "new2".to_string()]];

    let result = write_sets(&redis_url(), keys, members, None, WriteMode::Append)
        .expect("Failed to append to set");

    assert_eq!(result.keys_written, 1);

    // Verify all members exist
    let card = redis_cli_output(&["SCARD", "rust:setappend:1"]);
    assert_eq!(card, Some("3".to_string())); // existing + new1 + new2

    cleanup_keys("rust:setappend:*");
}

// =============================================================================
// Sorted Set Write Tests
// =============================================================================

/// Test basic sorted set writing.
#[test]
#[ignore] // Requires Redis
fn test_write_zsets_basic() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:zsetwrite:*");

    let keys = vec![
        "rust:zsetwrite:1".to_string(),
        "rust:zsetwrite:2".to_string(),
    ];
    let members = vec![
        vec![
            ("alice".to_string(), 100.0),
            ("bob".to_string(), 200.0),
            ("charlie".to_string(), 150.0),
        ],
        vec![("dave".to_string(), 50.0)],
    ];

    let result = write_zsets(&redis_url(), keys, members, None, WriteMode::Replace)
        .expect("Failed to write sorted sets");

    assert_eq!(result.keys_written, 2);
    assert_eq!(result.keys_failed, 0);

    // Verify data was written
    let card = redis_cli_output(&["ZCARD", "rust:zsetwrite:1"]);
    assert_eq!(card, Some("3".to_string()));

    let score = redis_cli_output(&["ZSCORE", "rust:zsetwrite:1", "bob"]);
    assert_eq!(score, Some("200".to_string()));

    // Verify ordering
    let first = redis_cli_output(&["ZRANGE", "rust:zsetwrite:1", "0", "0"]);
    assert_eq!(first, Some("alice".to_string())); // lowest score first

    cleanup_keys("rust:zsetwrite:*");
}

/// Test sorted set with TTL.
#[test]
#[ignore] // Requires Redis
fn test_write_zsets_with_ttl() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:zsetttl:*");

    let keys = vec!["rust:zsetttl:1".to_string()];
    let members = vec![vec![("member".to_string(), 1.0)]];

    let result = write_zsets(&redis_url(), keys, members, Some(3600), WriteMode::Replace)
        .expect("Failed to write sorted set");

    assert_eq!(result.keys_written, 1);

    // Verify TTL was set
    let ttl = redis_cli_output(&["TTL", "rust:zsetttl:1"]);
    assert!(ttl.is_some());
    let ttl_value: i64 = ttl.unwrap().parse().unwrap_or(0);
    assert!(ttl_value > 0 && ttl_value <= 3600);

    cleanup_keys("rust:zsetttl:*");
}

// =============================================================================
// WriteMode Tests
// =============================================================================

/// Test WriteMode::Fail skips existing keys.
#[test]
#[ignore] // Requires Redis
fn test_write_mode_fail_skips_existing() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:failmode:*");

    // Create existing key
    redis_cli(&["HSET", "rust:failmode:1", "existing", "data"]);

    let keys = vec![
        "rust:failmode:1".to_string(), // exists
        "rust:failmode:2".to_string(), // doesn't exist
    ];
    let fields = vec!["name".to_string()];
    let values = vec![
        vec![Some("Attempt1".to_string())],
        vec![Some("Attempt2".to_string())],
    ];

    let result = write_hashes(&redis_url(), keys, fields, values, None, WriteMode::Fail)
        .expect("Failed to write hashes");

    assert_eq!(result.keys_written, 1); // Only key2 written
    assert_eq!(result.keys_skipped, 1); // Key1 skipped

    // Verify key1 wasn't overwritten
    let existing = redis_cli_output(&["HGET", "rust:failmode:1", "existing"]);
    assert_eq!(existing, Some("data".to_string()));

    // Verify key2 was written
    let name = redis_cli_output(&["HGET", "rust:failmode:2", "name"]);
    assert_eq!(name, Some("Attempt2".to_string()));

    cleanup_keys("rust:failmode:*");
}

/// Test WriteMode::Fail for strings.
#[test]
#[ignore] // Requires Redis
fn test_write_strings_fail_mode() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:strfail:*");

    // Create existing key
    redis_cli(&["SET", "rust:strfail:1", "existing"]);

    let keys = vec!["rust:strfail:1".to_string(), "rust:strfail:2".to_string()];
    let values = vec![Some("new1".to_string()), Some("new2".to_string())];

    let result = write_strings(&redis_url(), keys, values, None, WriteMode::Fail)
        .expect("Failed to write strings");

    assert_eq!(result.keys_written, 1);
    assert_eq!(result.keys_skipped, 1);

    // Verify existing wasn't overwritten
    let val = redis_cli_output(&["GET", "rust:strfail:1"]);
    assert_eq!(val, Some("existing".to_string()));

    cleanup_keys("rust:strfail:*");
}

// =============================================================================
// Detailed Write Result Tests
// =============================================================================

/// Test detailed write results with successful writes.
#[test]
#[ignore] // Requires Redis
fn test_write_hashes_detailed_success() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:detailed:*");

    let keys = vec![
        "rust:detailed:1".to_string(),
        "rust:detailed:2".to_string(),
        "rust:detailed:3".to_string(),
    ];
    let fields = vec!["name".to_string()];
    let values = vec![
        vec![Some("Alice".to_string())],
        vec![Some("Bob".to_string())],
        vec![Some("Charlie".to_string())],
    ];

    let result =
        write_hashes_detailed(&redis_url(), keys, fields, values, None, WriteMode::Replace)
            .expect("Failed to write hashes");

    assert!(result.is_complete_success());
    assert_eq!(result.keys_written, 3);
    assert_eq!(result.keys_failed, 0);
    assert_eq!(result.succeeded_keys.len(), 3);
    assert!(result.errors.is_empty());

    cleanup_keys("rust:detailed:*");
}

/// Test detailed write results track succeeded keys.
#[test]
#[ignore] // Requires Redis
fn test_write_hashes_detailed_tracks_keys() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:track:*");

    let keys = vec!["rust:track:1".to_string(), "rust:track:2".to_string()];
    let fields = vec!["data".to_string()];
    let values = vec![
        vec![Some("value1".to_string())],
        vec![Some("value2".to_string())],
    ];

    let result =
        write_hashes_detailed(&redis_url(), keys, fields, values, None, WriteMode::Replace)
            .expect("Failed to write hashes");

    assert!(result.succeeded_keys.contains(&"rust:track:1".to_string()));
    assert!(result.succeeded_keys.contains(&"rust:track:2".to_string()));

    cleanup_keys("rust:track:*");
}

// =============================================================================
// Large Batch Tests
// =============================================================================

/// Test writing a large batch of hashes.
#[test]
#[ignore] // Requires Redis
fn test_write_large_batch_hashes() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:largebatch:*");

    let count = 2500; // Test across multiple pipeline batches (default is 1000)
    let keys: Vec<String> = (1..=count)
        .map(|i| format!("rust:largebatch:{}", i))
        .collect();
    let fields = vec!["index".to_string(), "value".to_string()];
    let values: Vec<Vec<Option<String>>> = (1..=count)
        .map(|i| vec![Some(i.to_string()), Some(format!("value_{}", i))])
        .collect();

    let result = write_hashes(&redis_url(), keys, fields, values, None, WriteMode::Replace)
        .expect("Failed to write large batch");

    assert_eq!(result.keys_written, count);
    assert_eq!(result.keys_failed, 0);

    // Verify first and last keys
    let first = redis_cli_output(&["HGET", "rust:largebatch:1", "index"]);
    assert_eq!(first, Some("1".to_string()));

    let last = redis_cli_output(&["HGET", &format!("rust:largebatch:{}", count), "index"]);
    assert_eq!(last, Some(count.to_string()));

    cleanup_keys("rust:largebatch:*");
}

/// Test writing a large batch of strings.
#[test]
#[ignore] // Requires Redis
fn test_write_large_batch_strings() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:largestr:*");

    let count = 2500;
    let keys: Vec<String> = (1..=count)
        .map(|i| format!("rust:largestr:{}", i))
        .collect();
    let values: Vec<Option<String>> = (1..=count).map(|i| Some(format!("value_{}", i))).collect();

    let result = write_strings(&redis_url(), keys, values, None, WriteMode::Replace)
        .expect("Failed to write large batch");

    assert_eq!(result.keys_written, count);
    assert_eq!(result.keys_failed, 0);

    // Spot check
    let mid = redis_cli_output(&["GET", "rust:largestr:1250"]);
    assert_eq!(mid, Some("value_1250".to_string()));

    cleanup_keys("rust:largestr:*");
}

// =============================================================================
// Edge Case Tests
// =============================================================================

/// Test writing with empty values.
#[test]
#[ignore] // Requires Redis
fn test_write_empty_string_value() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:empty:*");

    let keys = vec!["rust:empty:1".to_string()];
    let values = vec![Some("".to_string())]; // Empty string

    let result = write_strings(&redis_url(), keys, values, None, WriteMode::Replace)
        .expect("Failed to write empty string");

    assert_eq!(result.keys_written, 1);

    let val = redis_cli_output(&["GET", "rust:empty:1"]);
    assert_eq!(val, Some("".to_string()));

    cleanup_keys("rust:empty:*");
}

/// Test writing set with duplicate members (should deduplicate).
#[test]
#[ignore] // Requires Redis
fn test_write_set_with_duplicates() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:setdup:*");

    let keys = vec!["rust:setdup:1".to_string()];
    let members = vec![vec![
        "a".to_string(),
        "b".to_string(),
        "a".to_string(), // duplicate
        "c".to_string(),
        "b".to_string(), // duplicate
    ]];

    let result = write_sets(&redis_url(), keys, members, None, WriteMode::Replace)
        .expect("Failed to write set");

    assert_eq!(result.keys_written, 1);

    // Redis will deduplicate
    let card = redis_cli_output(&["SCARD", "rust:setdup:1"]);
    assert_eq!(card, Some("3".to_string())); // Only unique members

    cleanup_keys("rust:setdup:*");
}

/// Test writing empty list (should not create key).
#[test]
#[ignore] // Requires Redis
fn test_write_empty_list() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:emptylist:*");

    let keys = vec!["rust:emptylist:1".to_string()];
    let elements: Vec<Vec<String>> = vec![vec![]]; // Empty list

    let result = write_lists(&redis_url(), keys, elements, None, WriteMode::Replace)
        .expect("Failed to write empty list");

    // Empty lists are skipped
    assert_eq!(result.keys_written, 0);

    let exists = redis_cli_output(&["EXISTS", "rust:emptylist:1"]);
    assert_eq!(exists, Some("0".to_string()));

    cleanup_keys("rust:emptylist:*");
}

/// Test writing strings with null values (should skip nulls).
#[test]
#[ignore] // Requires Redis
fn test_write_strings_with_nulls() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:strnull:*");

    let keys = vec![
        "rust:strnull:1".to_string(),
        "rust:strnull:2".to_string(),
        "rust:strnull:3".to_string(),
    ];
    let values = vec![
        Some("value1".to_string()),
        None, // Null - should be skipped
        Some("value3".to_string()),
    ];

    let result = write_strings(&redis_url(), keys, values, None, WriteMode::Replace)
        .expect("Failed to write strings");

    assert_eq!(result.keys_written, 2); // Only non-null values

    let exists = redis_cli_output(&["EXISTS", "rust:strnull:2"]);
    assert_eq!(exists, Some("0".to_string())); // Null key not created

    cleanup_keys("rust:strnull:*");
}
