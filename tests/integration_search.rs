//! Integration tests for RediSearch operations (FT.SEARCH, FT.AGGREGATE).
//!
//! These tests require a running Redis instance with RediSearch module.
//! Redis 8+ includes RediSearch natively.
//!
//! Run with: `cargo test --test integration_search --all-features`

#![cfg(feature = "search")]

use polars_redis::query_builder::{Predicate, PredicateBuilder};
use polars_redis::{HashSchema, HashSearchIterator, RedisType, SearchBatchConfig};

mod common;
use common::{
    cleanup_keys, create_hash_index, ensure_redis, redis_cli, setup_test_hashes, wait_for_index,
};

/// Test basic FT.SEARCH with HashSearchIterator.
#[tokio::test]
async fn test_search_hashes_basic() {
    let url = ensure_redis().await.to_string();

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

    let total_rows = tokio::task::spawn_blocking(move || {
        let mut iterator = HashSearchIterator::new(&url, schema, config, None)
            .expect("Failed to create search iterator");

        let mut total = 0;
        while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
            total += batch.num_rows();
        }
        total
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(total_rows, 10);

    // Cleanup
    redis_cli(&["FT.DROPINDEX", "rust_search_idx"]);
    cleanup_keys("rust:search:*");
}

/// Test FT.SEARCH with numeric range filter.
#[tokio::test]
async fn test_search_numeric_range() {
    let url = ensure_redis().await.to_string();

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

    let total_rows = tokio::task::spawn_blocking(move || {
        let mut iterator = HashSearchIterator::new(&url, schema, config, None)
            .expect("Failed to create search iterator");

        let mut total = 0;
        while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
            total += batch.num_rows();
        }
        total
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(total_rows, 6); // ages 25, 26, 27, 28, 29, 30

    redis_cli(&["FT.DROPINDEX", "rust_numrange_idx"]);
    cleanup_keys("rust:numrange:*");
}

/// Test FT.SEARCH with sort.
#[tokio::test]
async fn test_search_with_sort() {
    let url = ensure_redis().await.to_string();

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

    let num_rows = tokio::task::spawn_blocking(move || {
        let mut iterator = HashSearchIterator::new(&url, schema, config, None)
            .expect("Failed to create search iterator");

        let batch = iterator
            .next_batch()
            .expect("Failed to get batch")
            .expect("Expected a batch");

        batch.num_rows()
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(num_rows, 5);

    redis_cli(&["FT.DROPINDEX", "rust_sort_idx"]);
    cleanup_keys("rust:sort:*");
}

/// Test FT.SEARCH with limit.
#[tokio::test]
async fn test_search_with_limit() {
    let url = ensure_redis().await.to_string();

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

    let total_rows = tokio::task::spawn_blocking(move || {
        let mut iterator = HashSearchIterator::new(&url, schema, config, None)
            .expect("Failed to create search iterator");

        let mut total = 0;
        while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
            total += batch.num_rows();
        }
        total
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(total_rows, 5);

    redis_cli(&["FT.DROPINDEX", "rust_limit_idx"]);
    cleanup_keys("rust:limit:*");
}

/// Test FT.SEARCH total_results tracking.
#[tokio::test]
async fn test_search_total_results() {
    let url = ensure_redis().await.to_string();

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

    let total_results = tokio::task::spawn_blocking(move || {
        let mut iterator = HashSearchIterator::new(&url, schema, config, None)
            .expect("Failed to create search iterator");

        // Before first batch, total_results should be None
        assert!(iterator.total_results().is_none());

        // Get first batch
        let _ = iterator.next_batch().expect("Failed to get batch");

        // After first batch, total_results should be available
        iterator.total_results()
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(total_results, Some(15));

    redis_cli(&["FT.DROPINDEX", "rust_total_idx"]);
    cleanup_keys("rust:total:*");
}

/// Test FT.SEARCH with no results.
#[tokio::test]
async fn test_search_no_results() {
    let url = ensure_redis().await.to_string();

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

    let is_empty = tokio::task::spawn_blocking(move || {
        let mut iterator = HashSearchIterator::new(&url, schema, config, None)
            .expect("Failed to create search iterator");

        let batch = iterator.next_batch().expect("Failed to get batch");
        batch.is_none() || batch.unwrap().num_rows() == 0
    })
    .await
    .expect("spawn_blocking failed");

    assert!(is_empty);

    redis_cli(&["FT.DROPINDEX", "rust_noresults_idx"]);
    cleanup_keys("rust:noresults:*");
}

/// Test FT.SEARCH with projection.
#[tokio::test]
async fn test_search_with_projection() {
    let url = ensure_redis().await.to_string();

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

    let (num_columns, num_rows) = tokio::task::spawn_blocking(move || {
        let mut iterator = HashSearchIterator::new(&url, schema, config, projection)
            .expect("Failed to create search iterator");

        let batch = iterator
            .next_batch()
            .expect("Failed to get batch")
            .expect("Expected a batch");

        (batch.num_columns(), batch.num_rows())
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(num_columns, 1);
    assert_eq!(num_rows, 5);

    redis_cli(&["FT.DROPINDEX", "rust_searchproj_idx"]);
    cleanup_keys("rust:searchproj:*");
}

// =============================================================================
// Query Builder Tests - Testing PredicateBuilder against actual Redis
// =============================================================================

/// Create a hash index with text fields for advanced search tests.
fn create_text_index(index_name: &str, prefix: &str) -> bool {
    let _ = redis_cli(&["FT.DROPINDEX", index_name]);

    redis_cli(&[
        "FT.CREATE",
        index_name,
        "ON",
        "HASH",
        "PREFIX",
        "1",
        prefix,
        "SCHEMA",
        "title",
        "TEXT",
        "SORTABLE",
        "description",
        "TEXT",
        "category",
        "TAG",
        "price",
        "NUMERIC",
        "SORTABLE",
        "rating",
        "NUMERIC",
        "SORTABLE",
    ])
}

/// Set up test products with text fields for search testing.
fn setup_test_products(prefix: &str) {
    let products = [
        (
            "1",
            "Python Programming Guide",
            "Learn Python programming from basics to advanced",
            "programming",
            "2999",
            "4.5",
        ),
        (
            "2",
            "Rust Systems Programming",
            "Build fast and safe systems with Rust",
            "programming",
            "3499",
            "4.8",
        ),
        (
            "3",
            "JavaScript Web Development",
            "Modern web development with JavaScript",
            "programming",
            "2499",
            "4.2",
        ),
        (
            "4",
            "Data Science Handbook",
            "Complete guide to data science and analytics",
            "data",
            "3999",
            "4.6",
        ),
        (
            "5",
            "Machine Learning Basics",
            "Introduction to machine learning algorithms",
            "data",
            "4499",
            "4.7",
        ),
        (
            "6",
            "Database Design Patterns",
            "Best practices for database architecture",
            "database",
            "2999",
            "4.3",
        ),
        (
            "7",
            "Redis in Action",
            "Practical Redis for real-world applications",
            "database",
            "3299",
            "4.9",
        ),
        (
            "8",
            "Cloud Computing Essentials",
            "Getting started with cloud infrastructure",
            "cloud",
            "2799",
            "4.1",
        ),
        (
            "9",
            "DevOps Practices",
            "Modern DevOps and CI/CD pipelines",
            "cloud",
            "3199",
            "4.4",
        ),
        (
            "10",
            "Kubernetes Guide",
            "Container orchestration with Kubernetes",
            "cloud",
            "3699",
            "4.5",
        ),
    ];

    for (id, title, desc, cat, price, rating) in products {
        let key = format!("{}{}", prefix, id);
        redis_cli(&[
            "HSET",
            &key,
            "title",
            title,
            "description",
            desc,
            "category",
            cat,
            "price",
            price,
            "rating",
            rating,
        ]);
    }
}

/// Test query builder with text search (contains).
#[tokio::test]
async fn test_query_builder_text_search() {
    let url = ensure_redis().await.to_string();

    cleanup_keys("rust:qb:text:*");
    setup_test_products("rust:qb:text:");

    if !create_text_index("rust_qb_text_idx", "rust:qb:text:") {
        eprintln!("Skipping test: Failed to create index");
        cleanup_keys("rust:qb:text:*");
        return;
    }
    wait_for_index("rust_qb_text_idx");

    // Use PredicateBuilder to search for "Python" in title
    let query = PredicateBuilder::new()
        .and(Predicate::text_search("title", "Python"))
        .build();

    let schema = HashSchema::new(vec![
        ("title".to_string(), RedisType::Utf8),
        ("category".to_string(), RedisType::Utf8),
    ])
    .with_key(true);

    let config = SearchBatchConfig::new("rust_qb_text_idx".to_string(), query).with_batch_size(100);

    let total_rows = tokio::task::spawn_blocking(move || {
        let mut iterator = HashSearchIterator::new(&url, schema, config, None)
            .expect("Failed to create search iterator");

        let mut total = 0;
        while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
            total += batch.num_rows();
        }
        total
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(total_rows, 1); // Only "Python Programming Guide"

    redis_cli(&["FT.DROPINDEX", "rust_qb_text_idx"]);
    cleanup_keys("rust:qb:text:*");
}

/// Test query builder with tag search.
#[tokio::test]
async fn test_query_builder_tag_search() {
    let url = ensure_redis().await.to_string();

    cleanup_keys("rust:qb:tag:*");
    setup_test_products("rust:qb:tag:");

    if !create_text_index("rust_qb_tag_idx", "rust:qb:tag:") {
        eprintln!("Skipping test: Failed to create index");
        cleanup_keys("rust:qb:tag:*");
        return;
    }
    wait_for_index("rust_qb_tag_idx");

    // Search for category "programming"
    let query = PredicateBuilder::new()
        .and(Predicate::tag("category", "programming"))
        .build();

    let schema = HashSchema::new(vec![
        ("title".to_string(), RedisType::Utf8),
        ("category".to_string(), RedisType::Utf8),
    ])
    .with_key(true);

    let config = SearchBatchConfig::new("rust_qb_tag_idx".to_string(), query).with_batch_size(100);

    let total_rows = tokio::task::spawn_blocking(move || {
        let mut iterator = HashSearchIterator::new(&url, schema, config, None)
            .expect("Failed to create search iterator");

        let mut total = 0;
        while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
            total += batch.num_rows();
        }
        total
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(total_rows, 3); // Python, Rust, JavaScript

    redis_cli(&["FT.DROPINDEX", "rust_qb_tag_idx"]);
    cleanup_keys("rust:qb:tag:*");
}

/// Test query builder with numeric range.
#[tokio::test]
async fn test_query_builder_numeric_range() {
    let url = ensure_redis().await.to_string();

    cleanup_keys("rust:qb:num:*");
    setup_test_products("rust:qb:num:");

    if !create_text_index("rust_qb_num_idx", "rust:qb:num:") {
        eprintln!("Skipping test: Failed to create index");
        cleanup_keys("rust:qb:num:*");
        return;
    }
    wait_for_index("rust_qb_num_idx");

    // Search for price between 3000 and 4000
    let query = PredicateBuilder::new()
        .and(Predicate::between("price", 3000, 4000))
        .build();

    let schema = HashSchema::new(vec![
        ("title".to_string(), RedisType::Utf8),
        ("price".to_string(), RedisType::Int64),
    ])
    .with_key(true);

    let config = SearchBatchConfig::new("rust_qb_num_idx".to_string(), query).with_batch_size(100);

    let total_rows = tokio::task::spawn_blocking(move || {
        let mut iterator = HashSearchIterator::new(&url, schema, config, None)
            .expect("Failed to create search iterator");

        let mut total = 0;
        while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
            total += batch.num_rows();
        }
        total
    })
    .await
    .expect("spawn_blocking failed");

    // Rust (3499), Data Science (3999), Redis (3299), Kubernetes (3699) = 4 products
    assert_eq!(total_rows, 4);

    redis_cli(&["FT.DROPINDEX", "rust_qb_num_idx"]);
    cleanup_keys("rust:qb:num:*");
}

/// Test query builder with AND combination.
#[tokio::test]
async fn test_query_builder_and_combination() {
    let url = ensure_redis().await.to_string();

    cleanup_keys("rust:qb:and:*");
    setup_test_products("rust:qb:and:");

    if !create_text_index("rust_qb_and_idx", "rust:qb:and:") {
        eprintln!("Skipping test: Failed to create index");
        cleanup_keys("rust:qb:and:*");
        return;
    }
    wait_for_index("rust_qb_and_idx");

    // Search for programming books with rating >= 4.5
    let query = PredicateBuilder::new()
        .and(Predicate::tag("category", "programming"))
        .and(Predicate::gte("rating", 4.5))
        .build();

    let schema = HashSchema::new(vec![
        ("title".to_string(), RedisType::Utf8),
        ("category".to_string(), RedisType::Utf8),
        ("rating".to_string(), RedisType::Float64),
    ])
    .with_key(true);

    let config = SearchBatchConfig::new("rust_qb_and_idx".to_string(), query).with_batch_size(100);

    let total_rows = tokio::task::spawn_blocking(move || {
        let mut iterator = HashSearchIterator::new(&url, schema, config, None)
            .expect("Failed to create search iterator");

        let mut total = 0;
        while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
            total += batch.num_rows();
        }
        total
    })
    .await
    .expect("spawn_blocking failed");

    // Python (4.5) and Rust (4.8) have rating >= 4.5 in programming category
    assert_eq!(total_rows, 2);

    redis_cli(&["FT.DROPINDEX", "rust_qb_and_idx"]);
    cleanup_keys("rust:qb:and:*");
}

/// Test query builder with OR combination using tag_or.
#[tokio::test]
async fn test_query_builder_tag_or() {
    let url = ensure_redis().await.to_string();

    cleanup_keys("rust:qb:or:*");
    setup_test_products("rust:qb:or:");

    if !create_text_index("rust_qb_or_idx", "rust:qb:or:") {
        eprintln!("Skipping test: Failed to create index");
        cleanup_keys("rust:qb:or:*");
        return;
    }
    wait_for_index("rust_qb_or_idx");

    // Search for programming OR database category
    let query = PredicateBuilder::new()
        .and(Predicate::tag_or(
            "category",
            vec!["programming", "database"],
        ))
        .build();

    let schema = HashSchema::new(vec![
        ("title".to_string(), RedisType::Utf8),
        ("category".to_string(), RedisType::Utf8),
    ])
    .with_key(true);

    let config = SearchBatchConfig::new("rust_qb_or_idx".to_string(), query).with_batch_size(100);

    let total_rows = tokio::task::spawn_blocking(move || {
        let mut iterator = HashSearchIterator::new(&url, schema, config, None)
            .expect("Failed to create search iterator");

        let mut total = 0;
        while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
            total += batch.num_rows();
        }
        total
    })
    .await
    .expect("spawn_blocking failed");

    // programming (3) + database (2) = 5 products
    assert_eq!(total_rows, 5);

    redis_cli(&["FT.DROPINDEX", "rust_qb_or_idx"]);
    cleanup_keys("rust:qb:or:*");
}

/// Test query builder with prefix search.
#[tokio::test]
async fn test_query_builder_prefix_search() {
    let url = ensure_redis().await.to_string();

    cleanup_keys("rust:qb:prefix:*");
    setup_test_products("rust:qb:prefix:");

    if !create_text_index("rust_qb_prefix_idx", "rust:qb:prefix:") {
        eprintln!("Skipping test: Failed to create index");
        cleanup_keys("rust:qb:prefix:*");
        return;
    }
    wait_for_index("rust_qb_prefix_idx");

    // Search for titles starting with "Data"
    let query = PredicateBuilder::new()
        .and(Predicate::prefix("title", "Data"))
        .build();

    let schema = HashSchema::new(vec![("title".to_string(), RedisType::Utf8)]).with_key(true);

    let config =
        SearchBatchConfig::new("rust_qb_prefix_idx".to_string(), query).with_batch_size(100);

    let total_rows = tokio::task::spawn_blocking(move || {
        let mut iterator = HashSearchIterator::new(&url, schema, config, None)
            .expect("Failed to create search iterator");

        let mut total = 0;
        while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
            total += batch.num_rows();
        }
        total
    })
    .await
    .expect("spawn_blocking failed");

    // "Data Science Handbook" and "Database Design Patterns"
    assert_eq!(total_rows, 2);

    redis_cli(&["FT.DROPINDEX", "rust_qb_prefix_idx"]);
    cleanup_keys("rust:qb:prefix:*");
}

/// Test query builder with fuzzy search.
#[tokio::test]
async fn test_query_builder_fuzzy_search() {
    let url = ensure_redis().await.to_string();

    cleanup_keys("rust:qb:fuzzy:*");
    setup_test_products("rust:qb:fuzzy:");

    if !create_text_index("rust_qb_fuzzy_idx", "rust:qb:fuzzy:") {
        eprintln!("Skipping test: Failed to create index");
        cleanup_keys("rust:qb:fuzzy:*");
        return;
    }
    wait_for_index("rust_qb_fuzzy_idx");

    // Fuzzy search for "Pythn" (typo for Python) with distance 1
    let query = PredicateBuilder::new()
        .and(Predicate::fuzzy("title", "Pythn", 1))
        .build();

    let schema = HashSchema::new(vec![("title".to_string(), RedisType::Utf8)]).with_key(true);

    let config =
        SearchBatchConfig::new("rust_qb_fuzzy_idx".to_string(), query).with_batch_size(100);

    let total_rows = tokio::task::spawn_blocking(move || {
        let mut iterator = HashSearchIterator::new(&url, schema, config, None)
            .expect("Failed to create search iterator");

        let mut total = 0;
        while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
            total += batch.num_rows();
        }
        total
    })
    .await
    .expect("spawn_blocking failed");

    // Should find "Python Programming Guide" despite typo
    assert_eq!(total_rows, 1);

    redis_cli(&["FT.DROPINDEX", "rust_qb_fuzzy_idx"]);
    cleanup_keys("rust:qb:fuzzy:*");
}

/// Test query builder with NOT (negation).
#[tokio::test]
async fn test_query_builder_negation() {
    let url = ensure_redis().await.to_string();

    cleanup_keys("rust:qb:not:*");
    setup_test_products("rust:qb:not:");

    if !create_text_index("rust_qb_not_idx", "rust:qb:not:") {
        eprintln!("Skipping test: Failed to create index");
        cleanup_keys("rust:qb:not:*");
        return;
    }
    wait_for_index("rust_qb_not_idx");

    // Search for all products NOT in the "cloud" category
    let predicate = Predicate::tag("category", "cloud").negate();
    let query = predicate.to_query();

    let schema = HashSchema::new(vec![
        ("title".to_string(), RedisType::Utf8),
        ("category".to_string(), RedisType::Utf8),
    ])
    .with_key(true);

    let config = SearchBatchConfig::new("rust_qb_not_idx".to_string(), query).with_batch_size(100);

    let total_rows = tokio::task::spawn_blocking(move || {
        let mut iterator = HashSearchIterator::new(&url, schema, config, None)
            .expect("Failed to create search iterator");

        let mut total = 0;
        while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
            total += batch.num_rows();
        }
        total
    })
    .await
    .expect("spawn_blocking failed");

    // 10 total - 3 cloud = 7 products
    assert_eq!(total_rows, 7);

    redis_cli(&["FT.DROPINDEX", "rust_qb_not_idx"]);
    cleanup_keys("rust:qb:not:*");
}

/// Test query builder with complex boolean expression.
#[tokio::test]
async fn test_query_builder_complex_boolean() {
    let url = ensure_redis().await.to_string();

    cleanup_keys("rust:qb:bool:*");
    setup_test_products("rust:qb:bool:");

    if !create_text_index("rust_qb_bool_idx", "rust:qb:bool:") {
        eprintln!("Skipping test: Failed to create index");
        cleanup_keys("rust:qb:bool:*");
        return;
    }
    wait_for_index("rust_qb_bool_idx");

    // Complex query: (programming OR data) AND price < 3500 AND rating >= 4.5
    let predicate = Predicate::tag("category", "programming")
        .or(Predicate::tag("category", "data"))
        .and(Predicate::lt("price", 3500))
        .and(Predicate::gte("rating", 4.5));
    let query = predicate.to_query();

    let schema = HashSchema::new(vec![
        ("title".to_string(), RedisType::Utf8),
        ("category".to_string(), RedisType::Utf8),
        ("price".to_string(), RedisType::Int64),
        ("rating".to_string(), RedisType::Float64),
    ])
    .with_key(true);

    let config = SearchBatchConfig::new("rust_qb_bool_idx".to_string(), query).with_batch_size(100);

    let total_rows = tokio::task::spawn_blocking(move || {
        let mut iterator = HashSearchIterator::new(&url, schema, config, None)
            .expect("Failed to create search iterator");

        let mut total = 0;
        while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
            total += batch.num_rows();
        }
        total
    })
    .await
    .expect("spawn_blocking failed");

    // Programming with price < 3500 and rating >= 4.5: Python (2999, 4.5)
    // Rust is 3499 but rating 4.8, still qualifies
    // Data with price < 3500 and rating >= 4.5: none (Data Science is 3999, ML is 4499)
    // So: Python (2999, 4.5) + Rust (3499, 4.8) = 2
    assert_eq!(total_rows, 2);

    redis_cli(&["FT.DROPINDEX", "rust_qb_bool_idx"]);
    cleanup_keys("rust:qb:bool:*");
}

/// Test query builder with infix (substring) search.
#[tokio::test]
async fn test_query_builder_infix_search() {
    let url = ensure_redis().await.to_string();

    cleanup_keys("rust:qb:infix:*");
    setup_test_products("rust:qb:infix:");

    if !create_text_index("rust_qb_infix_idx", "rust:qb:infix:") {
        eprintln!("Skipping test: Failed to create index");
        cleanup_keys("rust:qb:infix:*");
        return;
    }
    wait_for_index("rust_qb_infix_idx");

    // Search for "Science" substring in title
    let query = PredicateBuilder::new()
        .and(Predicate::infix("title", "Science"))
        .build();

    let schema = HashSchema::new(vec![("title".to_string(), RedisType::Utf8)]).with_key(true);

    let config =
        SearchBatchConfig::new("rust_qb_infix_idx".to_string(), query).with_batch_size(100);

    let total_rows = tokio::task::spawn_blocking(move || {
        let mut iterator = HashSearchIterator::new(&url, schema, config, None)
            .expect("Failed to create search iterator");

        let mut total = 0;
        while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
            total += batch.num_rows();
        }
        total
    })
    .await
    .expect("spawn_blocking failed");

    // "Data Science Handbook" contains "Science"
    assert_eq!(total_rows, 1);

    redis_cli(&["FT.DROPINDEX", "rust_qb_infix_idx"]);
    cleanup_keys("rust:qb:infix:*");
}

/// Test query builder with phrase search.
#[tokio::test]
async fn test_query_builder_phrase_search() {
    let url = ensure_redis().await.to_string();

    cleanup_keys("rust:qb:phrase:*");
    setup_test_products("rust:qb:phrase:");

    if !create_text_index("rust_qb_phrase_idx", "rust:qb:phrase:") {
        eprintln!("Skipping test: Failed to create index");
        cleanup_keys("rust:qb:phrase:*");
        return;
    }
    wait_for_index("rust_qb_phrase_idx");

    // Search for exact phrase "Web Development" in title
    let query = PredicateBuilder::new()
        .and(Predicate::phrase("title", vec!["Web", "Development"]))
        .build();

    let schema = HashSchema::new(vec![("title".to_string(), RedisType::Utf8)]).with_key(true);

    let config =
        SearchBatchConfig::new("rust_qb_phrase_idx".to_string(), query).with_batch_size(100);

    let total_rows = tokio::task::spawn_blocking(move || {
        let mut iterator = HashSearchIterator::new(&url, schema, config, None)
            .expect("Failed to create search iterator");

        let mut total = 0;
        while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
            total += batch.num_rows();
        }
        total
    })
    .await
    .expect("spawn_blocking failed");

    // "JavaScript Web Development" contains the phrase
    assert_eq!(total_rows, 1);

    redis_cli(&["FT.DROPINDEX", "rust_qb_phrase_idx"]);
    cleanup_keys("rust:qb:phrase:*");
}
