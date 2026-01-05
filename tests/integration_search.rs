//! Integration tests for RediSearch operations (FT.SEARCH, FT.AGGREGATE).
//!
//! These tests require a running Redis instance with RediSearch module.
//! Redis 8+ includes RediSearch natively.
//!
//! Run with: `cargo test --test integration_search --all-features`

#![cfg(feature = "search")]

use polars_redis::{HashSchema, HashSearchIterator, RedisType, SearchBatchConfig};

mod common;
use common::{
    cleanup_keys, create_hash_index, redis_available, redis_cli, redis_url, setup_test_hashes,
    wait_for_index,
};

/// Test basic FT.SEARCH with HashSearchIterator.
#[test]
#[ignore] // Requires Redis with RediSearch
fn test_search_hashes_basic() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:search:*");
    setup_test_hashes("rust:search:", 10);

    // Create index
    if !create_hash_index("rust_search_idx", "rust:search:") {
        eprintln!("Skipping test: Failed to create index (RediSearch not available?)");
        cleanup_keys("rust:search:*");
        return;
    }
    wait_for_index("rust_search_idx");

    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
    ])
    .with_key(true);

    let config =
        SearchBatchConfig::new("rust_search_idx".to_string(), "*".to_string()).with_batch_size(100);

    let mut iterator = HashSearchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create search iterator");

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 10);

    // Cleanup
    redis_cli(&["FT.DROPINDEX", "rust_search_idx"]);
    cleanup_keys("rust:search:*");
}

/// Test FT.SEARCH with numeric range filter.
#[test]
#[ignore] // Requires Redis with RediSearch
fn test_search_numeric_range() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:numrange:*");
    setup_test_hashes("rust:numrange:", 20);

    if !create_hash_index("rust_numrange_idx", "rust:numrange:") {
        eprintln!("Skipping test: Failed to create index");
        cleanup_keys("rust:numrange:*");
        return;
    }
    wait_for_index("rust_numrange_idx");

    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
    ])
    .with_key(true);

    // Search for age between 25 and 30 (ages are 21-40, so 25-30 = 6 results)
    let config =
        SearchBatchConfig::new("rust_numrange_idx".to_string(), "@age:[25 30]".to_string())
            .with_batch_size(100);

    let mut iterator = HashSearchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create search iterator");

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 6); // ages 25, 26, 27, 28, 29, 30

    redis_cli(&["FT.DROPINDEX", "rust_numrange_idx"]);
    cleanup_keys("rust:numrange:*");
}

/// Test FT.SEARCH with sort.
#[test]
#[ignore] // Requires Redis with RediSearch
fn test_search_with_sort() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:sort:*");
    setup_test_hashes("rust:sort:", 5);

    if !create_hash_index("rust_sort_idx", "rust:sort:") {
        eprintln!("Skipping test: Failed to create index");
        cleanup_keys("rust:sort:*");
        return;
    }
    wait_for_index("rust_sort_idx");

    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
    ])
    .with_key(false);

    // Sort by age descending
    let config = SearchBatchConfig::new("rust_sort_idx".to_string(), "*".to_string())
        .with_batch_size(100)
        .with_sort_by("age".to_string(), false); // descending

    let mut iterator = HashSearchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create search iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected a batch");

    assert_eq!(batch.num_rows(), 5);

    redis_cli(&["FT.DROPINDEX", "rust_sort_idx"]);
    cleanup_keys("rust:sort:*");
}

/// Test FT.SEARCH with limit.
#[test]
#[ignore] // Requires Redis with RediSearch
fn test_search_with_limit() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:limit:*");
    setup_test_hashes("rust:limit:", 20);

    if !create_hash_index("rust_limit_idx", "rust:limit:") {
        eprintln!("Skipping test: Failed to create index");
        cleanup_keys("rust:limit:*");
        return;
    }
    wait_for_index("rust_limit_idx");

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(true);

    let config = SearchBatchConfig::new("rust_limit_idx".to_string(), "*".to_string())
        .with_batch_size(100)
        .with_max_rows(5); // Only get 5 rows

    let mut iterator = HashSearchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create search iterator");

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 5);

    redis_cli(&["FT.DROPINDEX", "rust_limit_idx"]);
    cleanup_keys("rust:limit:*");
}

/// Test FT.SEARCH total_results tracking.
#[test]
#[ignore] // Requires Redis with RediSearch
fn test_search_total_results() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:total:*");
    setup_test_hashes("rust:total:", 15);

    if !create_hash_index("rust_total_idx", "rust:total:") {
        eprintln!("Skipping test: Failed to create index");
        cleanup_keys("rust:total:*");
        return;
    }
    wait_for_index("rust_total_idx");

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(false);

    let config =
        SearchBatchConfig::new("rust_total_idx".to_string(), "*".to_string()).with_batch_size(5); // Small batch to test pagination

    let mut iterator = HashSearchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create search iterator");

    // Before first batch, total_results should be None
    assert!(iterator.total_results().is_none());

    // Get first batch
    let _ = iterator.next_batch().expect("Failed to get batch");

    // After first batch, total_results should be available
    assert_eq!(iterator.total_results(), Some(15));

    redis_cli(&["FT.DROPINDEX", "rust_total_idx"]);
    cleanup_keys("rust:total:*");
}

/// Test FT.SEARCH with no results.
#[test]
#[ignore] // Requires Redis with RediSearch
fn test_search_no_results() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:noresults:*");
    setup_test_hashes("rust:noresults:", 5);

    if !create_hash_index("rust_noresults_idx", "rust:noresults:") {
        eprintln!("Skipping test: Failed to create index");
        cleanup_keys("rust:noresults:*");
        return;
    }
    wait_for_index("rust_noresults_idx");

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(false);

    // Search for age that doesn't exist
    let config = SearchBatchConfig::new(
        "rust_noresults_idx".to_string(),
        "@age:[1000 2000]".to_string(),
    )
    .with_batch_size(100);

    let mut iterator = HashSearchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create search iterator");

    let batch = iterator.next_batch().expect("Failed to get batch");
    assert!(batch.is_none() || batch.unwrap().num_rows() == 0);

    redis_cli(&["FT.DROPINDEX", "rust_noresults_idx"]);
    cleanup_keys("rust:noresults:*");
}

/// Test FT.SEARCH with projection.
#[test]
#[ignore] // Requires Redis with RediSearch
fn test_search_with_projection() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:searchproj:*");
    setup_test_hashes("rust:searchproj:", 5);

    if !create_hash_index("rust_searchproj_idx", "rust:searchproj:") {
        eprintln!("Skipping test: Failed to create index");
        cleanup_keys("rust:searchproj:*");
        return;
    }
    wait_for_index("rust_searchproj_idx");

    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
        ("active".to_string(), RedisType::Boolean),
    ])
    .with_key(false);

    let config = SearchBatchConfig::new("rust_searchproj_idx".to_string(), "*".to_string())
        .with_batch_size(100);

    // Only project 'name' column
    let projection = Some(vec!["name".to_string()]);

    let mut iterator = HashSearchIterator::new(&redis_url(), schema, config, projection)
        .expect("Failed to create search iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected a batch");

    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.num_rows(), 5);

    redis_cli(&["FT.DROPINDEX", "rust_searchproj_idx"]);
    cleanup_keys("rust:searchproj:*");
}
