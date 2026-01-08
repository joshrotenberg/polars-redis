//! Integration tests for Redis cache operations.
//!
//! These tests require a running Redis instance.
//! Run with: `cargo test --test integration_cache`

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use polars_redis::io::cache::{
    CacheConfig, CacheFormat, IpcCompression, ParquetCompressionType, cache_exists, cache_info,
    cache_record_batch, cache_ttl, delete_cached, get_cached_record_batch,
};

mod common;
use common::{cleanup_keys, ensure_redis, get_redis_url};

/// Helper to create a test RecordBatch.
fn create_test_batch(num_rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let ids: Vec<i64> = (0..num_rows as i64).collect();
    let names: Vec<String> = (0..num_rows).map(|i| format!("name_{}", i)).collect();
    let values: Vec<f64> = (0..num_rows).map(|i| i as f64 * 1.5).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .unwrap()
}

/// Test basic cache and retrieve with IPC format.
#[tokio::test]
async fn test_cache_ipc_basic() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:cache:ipc:*");

    let batch = create_test_batch(100);
    let config = CacheConfig::ipc();

    let (bytes_written, retrieved_rows, retrieved_cols) = tokio::task::spawn_blocking(move || {
        let bytes_written =
            cache_record_batch(&url, "rust:cache:ipc:basic", &batch, &config).unwrap();

        let cached = get_cached_record_batch(&url, "rust:cache:ipc:basic").unwrap();
        let retrieved = cached.unwrap();

        (bytes_written, retrieved.num_rows(), retrieved.num_columns())
    })
    .await
    .expect("spawn_blocking failed");

    assert!(bytes_written > 0);
    assert_eq!(retrieved_rows, 100);
    assert_eq!(retrieved_cols, 3);

    cleanup_keys("rust:cache:ipc:*");
}

/// Test cache with IPC LZ4 compression.
#[tokio::test]
async fn test_cache_ipc_lz4() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:cache:ipclz4:*");

    let batch = create_test_batch(100);
    let config = CacheConfig::ipc().with_ipc_compression(IpcCompression::Lz4);

    let (bytes_written, retrieved_rows) = tokio::task::spawn_blocking(move || {
        let bytes_written =
            cache_record_batch(&url, "rust:cache:ipclz4:basic", &batch, &config).unwrap();

        let cached = get_cached_record_batch(&url, "rust:cache:ipclz4:basic").unwrap();
        let retrieved = cached.unwrap();

        (bytes_written, retrieved.num_rows())
    })
    .await
    .expect("spawn_blocking failed");

    assert!(bytes_written > 0);
    assert_eq!(retrieved_rows, 100);

    cleanup_keys("rust:cache:ipclz4:*");
}

/// Test cache with IPC Zstd compression.
#[tokio::test]
async fn test_cache_ipc_zstd() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:cache:ipczstd:*");

    let batch = create_test_batch(100);
    let config = CacheConfig::ipc().with_ipc_compression(IpcCompression::Zstd);

    let (bytes_written, retrieved_rows) = tokio::task::spawn_blocking(move || {
        let bytes_written =
            cache_record_batch(&url, "rust:cache:ipczstd:basic", &batch, &config).unwrap();

        let cached = get_cached_record_batch(&url, "rust:cache:ipczstd:basic").unwrap();
        let retrieved = cached.unwrap();

        (bytes_written, retrieved.num_rows())
    })
    .await
    .expect("spawn_blocking failed");

    assert!(bytes_written > 0);
    assert_eq!(retrieved_rows, 100);

    cleanup_keys("rust:cache:ipczstd:*");
}

/// Test basic cache and retrieve with Parquet format.
#[tokio::test]
async fn test_cache_parquet_basic() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:cache:parquet:*");

    let batch = create_test_batch(100);
    let config = CacheConfig::parquet();

    let (bytes_written, retrieved_rows, retrieved_cols) = tokio::task::spawn_blocking(move || {
        let bytes_written =
            cache_record_batch(&url, "rust:cache:parquet:basic", &batch, &config).unwrap();

        let cached = get_cached_record_batch(&url, "rust:cache:parquet:basic").unwrap();
        let retrieved = cached.unwrap();

        (bytes_written, retrieved.num_rows(), retrieved.num_columns())
    })
    .await
    .expect("spawn_blocking failed");

    assert!(bytes_written > 0);
    assert_eq!(retrieved_rows, 100);
    assert_eq!(retrieved_cols, 3);

    cleanup_keys("rust:cache:parquet:*");
}

/// Test cache with Parquet Snappy compression.
#[tokio::test]
async fn test_cache_parquet_snappy() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:cache:pqsnappy:*");

    let batch = create_test_batch(100);
    let config = CacheConfig::parquet().with_parquet_compression(ParquetCompressionType::Snappy);

    let (bytes_written, cached_exists) = tokio::task::spawn_blocking(move || {
        let bytes_written =
            cache_record_batch(&url, "rust:cache:pqsnappy:basic", &batch, &config).unwrap();

        let cached = get_cached_record_batch(&url, "rust:cache:pqsnappy:basic").unwrap();

        (bytes_written, cached.is_some())
    })
    .await
    .expect("spawn_blocking failed");

    assert!(bytes_written > 0);
    assert!(cached_exists);

    cleanup_keys("rust:cache:pqsnappy:*");
}

/// Test cache with TTL.
#[tokio::test]
async fn test_cache_with_ttl() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:cache:ttl:*");

    let batch = create_test_batch(10);
    let config = CacheConfig::ipc().with_ttl(3600);

    let ttl = tokio::task::spawn_blocking(move || {
        cache_record_batch(&url, "rust:cache:ttl:basic", &batch, &config).unwrap();

        cache_ttl(&url, "rust:cache:ttl:basic").unwrap()
    })
    .await
    .expect("spawn_blocking failed");

    assert!(ttl.is_some());
    assert!(ttl.unwrap() > 0 && ttl.unwrap() <= 3600);

    cleanup_keys("rust:cache:ttl:*");
}

/// Test cache_exists.
#[tokio::test]
async fn test_cache_exists() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:cache:exists:*");

    let batch = create_test_batch(10);
    let config = CacheConfig::ipc();

    let (before_exists, after_exists) = tokio::task::spawn_blocking(move || {
        // Should not exist initially
        let before = cache_exists(&url, "rust:cache:exists:test").unwrap();

        // Cache it
        cache_record_batch(&url, "rust:cache:exists:test", &batch, &config).unwrap();

        // Should exist now
        let after = cache_exists(&url, "rust:cache:exists:test").unwrap();

        (before, after)
    })
    .await
    .expect("spawn_blocking failed");

    assert!(!before_exists);
    assert!(after_exists);

    cleanup_keys("rust:cache:exists:*");
}

/// Test delete_cached.
#[tokio::test]
async fn test_delete_cached() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:cache:delete:*");

    let batch = create_test_batch(10);
    let config = CacheConfig::ipc();

    let (exists_after_cache, deleted, exists_after_delete, deleted_again) =
        tokio::task::spawn_blocking(move || {
            cache_record_batch(&url, "rust:cache:delete:test", &batch, &config).unwrap();
            let exists_after_cache = cache_exists(&url, "rust:cache:delete:test").unwrap();

            // Delete it
            let deleted = delete_cached(&url, "rust:cache:delete:test").unwrap();

            // Should not exist anymore
            let exists_after_delete = cache_exists(&url, "rust:cache:delete:test").unwrap();

            // Deleting non-existent key returns false
            let deleted_again = delete_cached(&url, "rust:cache:delete:test").unwrap();

            (
                exists_after_cache,
                deleted,
                exists_after_delete,
                deleted_again,
            )
        })
        .await
        .expect("spawn_blocking failed");

    assert!(exists_after_cache);
    assert!(deleted);
    assert!(!exists_after_delete);
    assert!(!deleted_again);

    cleanup_keys("rust:cache:delete:*");
}

/// Test cache_info.
#[tokio::test]
async fn test_cache_info() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:cache:info:*");

    let batch = create_test_batch(100);
    let config = CacheConfig::ipc().with_ttl(7200);

    let info = tokio::task::spawn_blocking(move || {
        cache_record_batch(&url, "rust:cache:info:test", &batch, &config).unwrap();

        cache_info(&url, "rust:cache:info:test").unwrap()
    })
    .await
    .expect("spawn_blocking failed");

    assert!(info.is_some());

    let info = info.unwrap();
    assert_eq!(info.format, CacheFormat::Ipc);
    assert!(info.size_bytes > 0);
    assert!(!info.is_chunked);
    assert_eq!(info.num_chunks, 1);
    assert!(info.ttl.is_some());
    assert!(info.ttl.unwrap() > 0 && info.ttl.unwrap() <= 7200);

    cleanup_keys("rust:cache:info:*");
}

/// Test caching non-existent key returns None.
#[tokio::test]
async fn test_get_nonexistent() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    let cached = tokio::task::spawn_blocking(move || {
        get_cached_record_batch(&url, "rust:cache:nonexistent:key").unwrap()
    })
    .await
    .expect("spawn_blocking failed");

    assert!(cached.is_none());
}

/// Test caching empty batch.
#[tokio::test]
async fn test_cache_empty_batch() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:cache:empty:*");

    let batch = create_test_batch(0);
    let config = CacheConfig::ipc();

    let cached_rows = tokio::task::spawn_blocking(move || {
        cache_record_batch(&url, "rust:cache:empty:test", &batch, &config).unwrap();

        let cached = get_cached_record_batch(&url, "rust:cache:empty:test").unwrap();
        cached.unwrap().num_rows()
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(cached_rows, 0);

    cleanup_keys("rust:cache:empty:*");
}

/// Test caching large batch (to test chunking).
#[tokio::test]
async fn test_cache_large_batch() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:cache:large:*");

    // Create a larger batch
    let batch = create_test_batch(10000);

    // Use small chunk size to force chunking
    let config = CacheConfig::ipc().with_chunk_size(1024);

    let (bytes_written, is_chunked, num_chunks, cached_rows, deleted) =
        tokio::task::spawn_blocking(move || {
            let bytes_written =
                cache_record_batch(&url, "rust:cache:large:test", &batch, &config).unwrap();

            // Get info to verify chunking
            let info = cache_info(&url, "rust:cache:large:test").unwrap().unwrap();

            // Retrieve and verify
            let cached = get_cached_record_batch(&url, "rust:cache:large:test").unwrap();
            let cached_rows = cached.unwrap().num_rows();

            // Delete chunked data
            let deleted = delete_cached(&url, "rust:cache:large:test").unwrap();

            (
                bytes_written,
                info.is_chunked,
                info.num_chunks,
                cached_rows,
                deleted,
            )
        })
        .await
        .expect("spawn_blocking failed");

    assert!(bytes_written > 0);
    assert!(is_chunked);
    assert!(num_chunks > 1);
    assert_eq!(cached_rows, 10000);
    assert!(deleted);

    cleanup_keys("rust:cache:large:*");
}

/// Test cache without chunking.
#[tokio::test]
async fn test_cache_without_chunking() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:cache:nochunk:*");

    let batch = create_test_batch(1000);
    let config = CacheConfig::ipc().without_chunking();

    let is_chunked = tokio::task::spawn_blocking(move || {
        cache_record_batch(&url, "rust:cache:nochunk:test", &batch, &config).unwrap();

        let info = cache_info(&url, "rust:cache:nochunk:test").unwrap();
        info.unwrap().is_chunked
    })
    .await
    .expect("spawn_blocking failed");

    assert!(!is_chunked);

    cleanup_keys("rust:cache:nochunk:*");
}

/// Test overwriting cached data.
#[tokio::test]
async fn test_cache_overwrite() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:cache:overwrite:*");

    let batch1 = create_test_batch(50);
    let batch2 = create_test_batch(100);
    let config = CacheConfig::ipc();

    let (cached1_rows, cached2_rows) = tokio::task::spawn_blocking(move || {
        cache_record_batch(&url, "rust:cache:overwrite:test", &batch1, &config).unwrap();

        let cached1 = get_cached_record_batch(&url, "rust:cache:overwrite:test").unwrap();
        let cached1_rows = cached1.unwrap().num_rows();

        // Overwrite with different batch
        cache_record_batch(&url, "rust:cache:overwrite:test", &batch2, &config).unwrap();

        let cached2 = get_cached_record_batch(&url, "rust:cache:overwrite:test").unwrap();
        let cached2_rows = cached2.unwrap().num_rows();

        (cached1_rows, cached2_rows)
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(cached1_rows, 50);
    assert_eq!(cached2_rows, 100);

    cleanup_keys("rust:cache:overwrite:*");
}
