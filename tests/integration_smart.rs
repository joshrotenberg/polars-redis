//! Integration tests for smart scan module.
//!
//! These tests require a running Redis instance with RediSearch.
//! Run with: `cargo test --test integration_smart --all-features`

use polars_redis::smart::{
    DetectedIndex, ExecutionStrategy, QueryPlan, find_index_for_pattern, list_indexes, plan_query,
};

mod common;
use common::{
    cleanup_keys, create_hash_index, ensure_redis, redis_cli, setup_test_hashes, wait_for_index,
};

/// Test ExecutionStrategy display.
#[test]
fn test_execution_strategy_display() {
    assert_eq!(format!("{}", ExecutionStrategy::Search), "SEARCH");
    assert_eq!(format!("{}", ExecutionStrategy::Scan), "SCAN");
    assert_eq!(format!("{}", ExecutionStrategy::Hybrid), "HYBRID");
}

/// Test QueryPlan explain output.
#[test]
fn test_query_plan_explain() {
    let plan = QueryPlan {
        strategy: ExecutionStrategy::Search,
        index: Some(DetectedIndex {
            name: "test_idx".to_string(),
            prefixes: vec!["test:".to_string()],
            on_type: "HASH".to_string(),
            fields: vec!["name".to_string(), "age".to_string()],
        }),
        server_query: Some("*".to_string()),
        client_filters: Vec::new(),
        warnings: Vec::new(),
    };

    let explanation = plan.explain();
    assert!(explanation.contains("Strategy: SEARCH"));
    assert!(explanation.contains("Index: test_idx"));
    assert!(explanation.contains("test:"));
    assert!(explanation.contains("Server Query: *"));
}

/// Test QueryPlan with warnings and client filters.
#[test]
fn test_query_plan_with_warnings() {
    let mut plan = QueryPlan::new(ExecutionStrategy::Scan);
    plan.warnings.push("No index found for pattern".to_string());
    plan.client_filters.push("age > 30".to_string());

    let explanation = plan.explain();
    assert!(explanation.contains("Strategy: SCAN"));
    assert!(explanation.contains("Warnings:"));
    assert!(explanation.contains("No index found"));
    assert!(explanation.contains("Client Filters:"));
    assert!(explanation.contains("age > 30"));
}

/// Test DetectedIndex clone.
#[test]
fn test_detected_index_clone() {
    let index = DetectedIndex {
        name: "idx".to_string(),
        prefixes: vec!["prefix:".to_string()],
        on_type: "HASH".to_string(),
        fields: vec!["field1".to_string()],
    };

    let cloned = index.clone();
    assert_eq!(cloned.name, index.name);
    assert_eq!(cloned.prefixes, index.prefixes);
    assert_eq!(cloned.on_type, index.on_type);
    assert_eq!(cloned.fields, index.fields);
}

/// Test find_index_for_pattern with no RediSearch (returns None gracefully).
#[tokio::test]
async fn test_find_index_no_redisearch() {
    let url = ensure_redis().await;

    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_connection_manager().await.unwrap();

    // This should return None if no matching index exists
    let result = find_index_for_pattern(&mut conn, "nonexistent:*").await;
    assert!(result.is_ok());
    // Result could be None or Some depending on what indexes exist
}

/// Test list_indexes returns empty when no indexes.
#[tokio::test]
async fn test_list_indexes_empty() {
    let url = ensure_redis().await;

    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_connection_manager().await.unwrap();

    // This should not error even if RediSearch is not available
    let result = list_indexes(&mut conn).await;
    assert!(result.is_ok());
}

/// Test find_index_for_pattern finds matching index.
#[tokio::test]
async fn test_find_index_for_pattern() {
    let url = ensure_redis().await;

    cleanup_keys("rust:smart:find:*");

    // Create an index
    let index_created = create_hash_index("rust_smart_find_idx", "rust:smart:find:");
    if !index_created {
        eprintln!("Skipping test: RediSearch not available");
        return;
    }

    // Create some test data
    setup_test_hashes("rust:smart:find:", 5);
    wait_for_index("rust_smart_find_idx");

    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_connection_manager().await.unwrap();

    let result = find_index_for_pattern(&mut conn, "rust:smart:find:*").await;
    assert!(result.is_ok());

    let index = result.unwrap();
    assert!(index.is_some());

    let idx = index.unwrap();
    assert_eq!(idx.name, "rust_smart_find_idx");
    assert!(idx.prefixes.contains(&"rust:smart:find:".to_string()));
    assert_eq!(idx.on_type, "HASH");
    assert!(idx.fields.contains(&"name".to_string()));

    // Cleanup
    redis_cli(&["FT.DROPINDEX", "rust_smart_find_idx"]);
    cleanup_keys("rust:smart:find:*");
}

/// Test list_indexes returns created indexes.
#[tokio::test]
async fn test_list_indexes() {
    let url = ensure_redis().await;

    cleanup_keys("rust:smart:list:*");

    // Create an index
    let index_created = create_hash_index("rust_smart_list_idx", "rust:smart:list:");
    if !index_created {
        eprintln!("Skipping test: RediSearch not available");
        return;
    }

    wait_for_index("rust_smart_list_idx");

    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_connection_manager().await.unwrap();

    let result = list_indexes(&mut conn).await;
    assert!(result.is_ok());

    let indexes = result.unwrap();
    assert!(!indexes.is_empty());

    // Find our index
    let our_index = indexes.iter().find(|i| i.name == "rust_smart_list_idx");
    assert!(our_index.is_some());

    // Cleanup
    redis_cli(&["FT.DROPINDEX", "rust_smart_list_idx"]);
    cleanup_keys("rust:smart:list:*");
}

/// Test plan_query with index available.
#[tokio::test]
async fn test_plan_query_with_index() {
    let url = ensure_redis().await;

    cleanup_keys("rust:smart:plan:*");

    let index_created = create_hash_index("rust_smart_plan_idx", "rust:smart:plan:");
    if !index_created {
        eprintln!("Skipping test: RediSearch not available");
        return;
    }

    setup_test_hashes("rust:smart:plan:", 3);
    wait_for_index("rust_smart_plan_idx");

    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_connection_manager().await.unwrap();

    let result = plan_query(&mut conn, "rust:smart:plan:*").await;
    assert!(result.is_ok());

    let plan = result.unwrap();
    assert_eq!(plan.strategy, ExecutionStrategy::Search);
    assert!(plan.index.is_some());
    assert!(plan.server_query.is_some());
    assert!(plan.warnings.is_empty());

    // Cleanup
    redis_cli(&["FT.DROPINDEX", "rust_smart_plan_idx"]);
    cleanup_keys("rust:smart:plan:*");
}

/// Test plan_query without index falls back to scan.
#[tokio::test]
async fn test_plan_query_without_index() {
    let url = ensure_redis().await;

    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_connection_manager().await.unwrap();

    // Use a pattern that definitely has no index
    let result = plan_query(&mut conn, "rust:smart:noindex:*").await;
    assert!(result.is_ok());

    let plan = result.unwrap();
    assert_eq!(plan.strategy, ExecutionStrategy::Scan);
    assert!(plan.index.is_none());
    assert!(!plan.warnings.is_empty());
    assert!(plan.warnings[0].contains("No index found"));
}

/// Test index detection with partial prefix match.
#[tokio::test]
async fn test_find_index_partial_prefix() {
    let url = ensure_redis().await;

    cleanup_keys("rust:smart:partial:*");

    // Create index with broader prefix
    let index_created = create_hash_index("rust_smart_partial_idx", "rust:smart:");
    if !index_created {
        eprintln!("Skipping test: RediSearch not available");
        return;
    }

    wait_for_index("rust_smart_partial_idx");

    let client = redis::Client::open(url).unwrap();
    let mut conn = client.get_connection_manager().await.unwrap();

    // Query with more specific pattern should still find the broader index
    let result = find_index_for_pattern(&mut conn, "rust:smart:partial:subkey:*").await;
    assert!(result.is_ok());

    let index = result.unwrap();
    assert!(index.is_some());
    assert_eq!(index.unwrap().name, "rust_smart_partial_idx");

    // Cleanup
    redis_cli(&["FT.DROPINDEX", "rust_smart_partial_idx"]);
    cleanup_keys("rust:smart:partial:*");
}
