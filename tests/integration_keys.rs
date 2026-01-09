//! Integration tests for Redis key management operations.
//!
//! These tests require a running Redis instance.
//! Run with: `cargo test --test integration_keys`

use polars_redis::{
    delete_keys, delete_keys_pattern, exists_keys, get_ttl, key_info, persist_keys, rename_keys,
    set_ttl, set_ttl_individual,
};

mod common;
use common::{cleanup_keys, ensure_redis, get_redis_url, redis_cli, redis_cli_output};

/// Test key_info returns correct information for hashes.
#[tokio::test]
async fn test_key_info_basic() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:keyinfo:*");

    // Create some test keys
    redis_cli(&["HSET", "rust:keyinfo:1", "name", "Alice", "age", "30"]);
    redis_cli(&["SET", "rust:keyinfo:2", "value2"]);
    redis_cli(&["SADD", "rust:keyinfo:3", "member1", "member2"]);

    // Set TTL on one key
    redis_cli(&["EXPIRE", "rust:keyinfo:2", "3600"]);

    let info = tokio::task::spawn_blocking(move || {
        key_info(&url, "rust:keyinfo:*", None).expect("Failed to get key info")
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(info.len(), 3);

    // Find the hash key
    let hash_info = info.iter().find(|k| k.key == "rust:keyinfo:1").unwrap();
    assert_eq!(hash_info.key_type, "hash");
    assert_eq!(hash_info.ttl, -1); // No TTL

    // Find the string key with TTL
    let string_info = info.iter().find(|k| k.key == "rust:keyinfo:2").unwrap();
    assert_eq!(string_info.key_type, "string");
    assert!(string_info.ttl > 0 && string_info.ttl <= 3600);

    // Find the set key
    let set_info = info.iter().find(|k| k.key == "rust:keyinfo:3").unwrap();
    assert_eq!(set_info.key_type, "set");

    cleanup_keys("rust:keyinfo:*");
}

/// Test key_info with memory usage.
#[tokio::test]
async fn test_key_info_with_memory() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:keyinfomem:*");

    redis_cli(&["SET", "rust:keyinfomem:1", "some_value_here"]);

    let info = tokio::task::spawn_blocking(move || {
        key_info(&url, "rust:keyinfomem:*", Some(true)).expect("Failed to get key info")
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(info.len(), 1);
    assert!(info[0].memory_usage.is_some());
    assert!(info[0].memory_usage.unwrap() > 0);
    assert!(info[0].encoding.is_some());

    cleanup_keys("rust:keyinfomem:*");
}

/// Test set_ttl for multiple keys.
#[tokio::test]
async fn test_set_ttl_basic() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:setttl:*");

    // Create test keys
    redis_cli(&["SET", "rust:setttl:1", "value1"]);
    redis_cli(&["SET", "rust:setttl:2", "value2"]);
    redis_cli(&["SET", "rust:setttl:3", "value3"]);

    let keys = vec![
        "rust:setttl:1".to_string(),
        "rust:setttl:2".to_string(),
        "rust:setttl:3".to_string(),
        "rust:setttl:nonexistent".to_string(), // This one doesn't exist
    ];

    let result =
        tokio::task::spawn_blocking(move || set_ttl(&url, &keys, 3600).expect("Failed to set TTL"))
            .await
            .expect("spawn_blocking failed");

    assert_eq!(result.succeeded, 3);
    assert_eq!(result.failed, 1);
    assert_eq!(result.errors.len(), 1);
    assert_eq!(result.errors[0].0, "rust:setttl:nonexistent");

    // Verify TTL was set
    let ttl = redis_cli_output(&["TTL", "rust:setttl:1"]);
    let ttl_value: i64 = ttl.unwrap().parse().unwrap();
    assert!(ttl_value > 0 && ttl_value <= 3600);

    cleanup_keys("rust:setttl:*");
}

/// Test set_ttl_individual with different TTLs per key.
#[tokio::test]
async fn test_set_ttl_individual() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:setttlind:*");

    // Create test keys
    redis_cli(&["SET", "rust:setttlind:1", "value1"]);
    redis_cli(&["SET", "rust:setttlind:2", "value2"]);

    let keys_and_ttls = vec![
        ("rust:setttlind:1".to_string(), 1800i64),
        ("rust:setttlind:2".to_string(), 7200i64),
    ];

    let result = tokio::task::spawn_blocking(move || {
        set_ttl_individual(&url, &keys_and_ttls).expect("Failed to set individual TTLs")
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(result.succeeded, 2);
    assert_eq!(result.failed, 0);

    // Verify different TTLs
    let ttl1: i64 = redis_cli_output(&["TTL", "rust:setttlind:1"])
        .unwrap()
        .parse()
        .unwrap();
    let ttl2: i64 = redis_cli_output(&["TTL", "rust:setttlind:2"])
        .unwrap()
        .parse()
        .unwrap();

    assert!(ttl1 > 0 && ttl1 <= 1800);
    assert!(ttl2 > 1800 && ttl2 <= 7200);

    cleanup_keys("rust:setttlind:*");
}

/// Test delete_keys for multiple keys.
#[tokio::test]
async fn test_delete_keys_basic() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:delkeys:*");

    // Create test keys
    redis_cli(&["SET", "rust:delkeys:1", "value1"]);
    redis_cli(&["SET", "rust:delkeys:2", "value2"]);
    redis_cli(&["SET", "rust:delkeys:3", "value3"]);

    let keys = vec![
        "rust:delkeys:1".to_string(),
        "rust:delkeys:2".to_string(),
        "rust:delkeys:nonexistent".to_string(),
    ];

    let result = tokio::task::spawn_blocking(move || {
        delete_keys(&url, &keys).expect("Failed to delete keys")
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(result.deleted, 2);
    assert_eq!(result.not_found, 1);

    // Verify keys were deleted
    let exists = redis_cli_output(&["EXISTS", "rust:delkeys:1"]);
    assert_eq!(exists, Some("0".to_string()));

    // Key 3 should still exist
    let exists3 = redis_cli_output(&["EXISTS", "rust:delkeys:3"]);
    assert_eq!(exists3, Some("1".to_string()));

    cleanup_keys("rust:delkeys:*");
}

/// Test delete_keys_pattern.
#[tokio::test]
async fn test_delete_keys_pattern() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:delpat:*");

    // Create test keys
    redis_cli(&["SET", "rust:delpat:1", "value1"]);
    redis_cli(&["SET", "rust:delpat:2", "value2"]);
    redis_cli(&["SET", "rust:delpat:3", "value3"]);

    let result = tokio::task::spawn_blocking(move || {
        delete_keys_pattern(&url, "rust:delpat:*").expect("Failed to delete keys")
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(result.deleted, 3);
    assert_eq!(result.not_found, 0);

    // Verify all keys were deleted
    let exists = redis_cli_output(&["EXISTS", "rust:delpat:1"]);
    assert_eq!(exists, Some("0".to_string()));

    cleanup_keys("rust:delpat:*");
}

/// Test rename_keys.
#[tokio::test]
async fn test_rename_keys_basic() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:rename:*");
    cleanup_keys("rust:renamed:*");

    // Create test keys
    redis_cli(&["SET", "rust:rename:1", "value1"]);
    redis_cli(&["SET", "rust:rename:2", "value2"]);

    let renames = vec![
        ("rust:rename:1".to_string(), "rust:renamed:1".to_string()),
        ("rust:rename:2".to_string(), "rust:renamed:2".to_string()),
        (
            "rust:rename:nonexistent".to_string(),
            "rust:renamed:3".to_string(),
        ),
    ];

    let result = tokio::task::spawn_blocking(move || {
        rename_keys(&url, &renames).expect("Failed to rename keys")
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(result.succeeded, 2);
    assert_eq!(result.failed, 1);

    // Verify old keys don't exist
    let old_exists = redis_cli_output(&["EXISTS", "rust:rename:1"]);
    assert_eq!(old_exists, Some("0".to_string()));

    // Verify new keys exist with correct values
    let new_value = redis_cli_output(&["GET", "rust:renamed:1"]);
    assert_eq!(new_value, Some("value1".to_string()));

    cleanup_keys("rust:rename:*");
    cleanup_keys("rust:renamed:*");
}

/// Test persist_keys (remove TTL).
#[tokio::test]
async fn test_persist_keys() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:persist:*");

    // Create test keys with TTL
    redis_cli(&["SET", "rust:persist:1", "value1", "EX", "3600"]);
    redis_cli(&["SET", "rust:persist:2", "value2", "EX", "3600"]);
    redis_cli(&["SET", "rust:persist:3", "value3"]); // No TTL

    let keys = vec![
        "rust:persist:1".to_string(),
        "rust:persist:2".to_string(),
        "rust:persist:3".to_string(), // Already persistent
    ];

    let result = tokio::task::spawn_blocking(move || {
        persist_keys(&url, &keys).expect("Failed to persist keys")
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(result.succeeded, 2);
    assert_eq!(result.failed, 1); // Key 3 had no TTL

    // Verify TTL was removed
    let ttl = redis_cli_output(&["TTL", "rust:persist:1"]);
    assert_eq!(ttl, Some("-1".to_string())); // -1 means no TTL

    cleanup_keys("rust:persist:*");
}

/// Test exists_keys.
#[tokio::test]
async fn test_exists_keys() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:exists:*");

    // Create some test keys
    redis_cli(&["SET", "rust:exists:1", "value1"]);
    redis_cli(&["SET", "rust:exists:2", "value2"]);

    let keys = vec![
        "rust:exists:1".to_string(),
        "rust:exists:2".to_string(),
        "rust:exists:nonexistent".to_string(),
    ];

    let result = tokio::task::spawn_blocking(move || {
        exists_keys(&url, &keys).expect("Failed to check existence")
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(result.len(), 3);

    let exists_1 = result.iter().find(|(k, _)| k == "rust:exists:1").unwrap();
    assert!(exists_1.1);

    let exists_2 = result.iter().find(|(k, _)| k == "rust:exists:2").unwrap();
    assert!(exists_2.1);

    let exists_none = result
        .iter()
        .find(|(k, _)| k == "rust:exists:nonexistent")
        .unwrap();
    assert!(!exists_none.1);

    cleanup_keys("rust:exists:*");
}

/// Test get_ttl for multiple keys.
#[tokio::test]
async fn test_get_ttl() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:getttl:*");

    // Create test keys with different TTLs
    redis_cli(&["SET", "rust:getttl:1", "value1", "EX", "3600"]);
    redis_cli(&["SET", "rust:getttl:2", "value2"]); // No TTL

    let keys = vec![
        "rust:getttl:1".to_string(),
        "rust:getttl:2".to_string(),
        "rust:getttl:nonexistent".to_string(),
    ];

    let result =
        tokio::task::spawn_blocking(move || get_ttl(&url, &keys).expect("Failed to get TTL"))
            .await
            .expect("spawn_blocking failed");

    assert_eq!(result.len(), 3);

    let ttl_1 = result.iter().find(|(k, _)| k == "rust:getttl:1").unwrap();
    assert!(ttl_1.1 > 0 && ttl_1.1 <= 3600);

    let ttl_2 = result.iter().find(|(k, _)| k == "rust:getttl:2").unwrap();
    assert_eq!(ttl_2.1, -1); // No TTL

    let ttl_none = result
        .iter()
        .find(|(k, _)| k == "rust:getttl:nonexistent")
        .unwrap();
    assert_eq!(ttl_none.1, -2); // Key doesn't exist

    cleanup_keys("rust:getttl:*");
}

/// Test key_info with empty result.
#[tokio::test]
async fn test_key_info_empty() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:keyinfoempty:*");

    let info = tokio::task::spawn_blocking(move || {
        key_info(&url, "rust:keyinfoempty:*", None).expect("Failed to get key info")
    })
    .await
    .expect("spawn_blocking failed");

    assert!(info.is_empty());
}

/// Test large batch of keys.
#[tokio::test]
async fn test_large_batch_operations() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:largebatch:*");

    // Create 500 keys
    let count = 500;
    for i in 0..count {
        redis_cli(&[
            "SET",
            &format!("rust:largebatch:{}", i),
            &format!("value{}", i),
        ]);
    }

    // Test key_info on large batch
    let url_clone = url.clone();
    let info = tokio::task::spawn_blocking(move || {
        key_info(&url_clone, "rust:largebatch:*", None)
            .expect("Failed to get key info for large batch")
    })
    .await
    .expect("spawn_blocking failed");
    assert_eq!(info.len(), count);

    // Test set_ttl on large batch
    let keys: Vec<String> = (0..count)
        .map(|i| format!("rust:largebatch:{}", i))
        .collect();
    let url_clone = url.clone();
    let result = tokio::task::spawn_blocking(move || {
        set_ttl(&url_clone, &keys, 3600).expect("Failed to set TTL for large batch")
    })
    .await
    .expect("spawn_blocking failed");
    assert_eq!(result.succeeded, count);

    // Test delete_keys_pattern on large batch
    let delete_result = tokio::task::spawn_blocking(move || {
        delete_keys_pattern(&url, "rust:largebatch:*").expect("Failed to delete large batch")
    })
    .await
    .expect("spawn_blocking failed");
    assert_eq!(delete_result.deleted, count);

    cleanup_keys("rust:largebatch:*");
}
