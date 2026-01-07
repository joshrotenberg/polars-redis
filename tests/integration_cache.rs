//! Integration tests for Redis cache operations.
//!
//! These tests require a running Redis instance.
//! Run with: `cargo test --test integration_cache --all-features`

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use polars_redis::io::cache::{
    CacheConfig, CacheFormat, IpcCompression, ParquetCompressionType, cache_exists, cache_info,
    cache_record_batch, cache_ttl, delete_cached, get_cached_record_batch,
};

mod common;
use common::{cleanup_keys, redis_available, redis_url};

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
#[test]
#[ignore] // Requires Redis
fn test_cache_ipc_basic() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:cache:ipc:*");

    let batch = create_test_batch(100);
    let config = CacheConfig::ipc();

    let bytes_written =
        cache_record_batch(&redis_url(), "rust:cache:ipc:basic", &batch, &config).unwrap();
    assert!(bytes_written > 0);

    let cached = get_cached_record_batch(&redis_url(), "rust:cache:ipc:basic").unwrap();
    assert!(cached.is_some());

    let retrieved = cached.unwrap();
    assert_eq!(retrieved.num_rows(), batch.num_rows());
    assert_eq!(retrieved.num_columns(), batch.num_columns());

    cleanup_keys("rust:cache:ipc:*");
}

/// Test cache with IPC LZ4 compression.
#[test]
#[ignore] // Requires Redis
fn test_cache_ipc_lz4() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:cache:ipclz4:*");

    let batch = create_test_batch(100);
    let config = CacheConfig::ipc().with_ipc_compression(IpcCompression::Lz4);

    let bytes_written =
        cache_record_batch(&redis_url(), "rust:cache:ipclz4:basic", &batch, &config).unwrap();
    assert!(bytes_written > 0);

    let cached = get_cached_record_batch(&redis_url(), "rust:cache:ipclz4:basic").unwrap();
    assert!(cached.is_some());

    let retrieved = cached.unwrap();
    assert_eq!(retrieved.num_rows(), batch.num_rows());

    cleanup_keys("rust:cache:ipclz4:*");
}

/// Test cache with IPC Zstd compression.
#[test]
#[ignore] // Requires Redis
fn test_cache_ipc_zstd() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:cache:ipczstd:*");

    let batch = create_test_batch(100);
    let config = CacheConfig::ipc().with_ipc_compression(IpcCompression::Zstd);

    let bytes_written =
        cache_record_batch(&redis_url(), "rust:cache:ipczstd:basic", &batch, &config).unwrap();
    assert!(bytes_written > 0);

    let cached = get_cached_record_batch(&redis_url(), "rust:cache:ipczstd:basic").unwrap();
    assert!(cached.is_some());

    let retrieved = cached.unwrap();
    assert_eq!(retrieved.num_rows(), batch.num_rows());

    cleanup_keys("rust:cache:ipczstd:*");
}

/// Test basic cache and retrieve with Parquet format.
#[test]
#[ignore] // Requires Redis
fn test_cache_parquet_basic() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:cache:parquet:*");

    let batch = create_test_batch(100);
    let config = CacheConfig::parquet();

    let bytes_written =
        cache_record_batch(&redis_url(), "rust:cache:parquet:basic", &batch, &config).unwrap();
    assert!(bytes_written > 0);

    let cached = get_cached_record_batch(&redis_url(), "rust:cache:parquet:basic").unwrap();
    assert!(cached.is_some());

    let retrieved = cached.unwrap();
    assert_eq!(retrieved.num_rows(), batch.num_rows());
    assert_eq!(retrieved.num_columns(), batch.num_columns());

    cleanup_keys("rust:cache:parquet:*");
}

/// Test cache with Parquet Snappy compression.
#[test]
#[ignore] // Requires Redis
fn test_cache_parquet_snappy() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:cache:pqsnappy:*");

    let batch = create_test_batch(100);
    let config = CacheConfig::parquet().with_parquet_compression(ParquetCompressionType::Snappy);

    let bytes_written =
        cache_record_batch(&redis_url(), "rust:cache:pqsnappy:basic", &batch, &config).unwrap();
    assert!(bytes_written > 0);

    let cached = get_cached_record_batch(&redis_url(), "rust:cache:pqsnappy:basic").unwrap();
    assert!(cached.is_some());

    cleanup_keys("rust:cache:pqsnappy:*");
}

/// Test cache with TTL.
#[test]
#[ignore] // Requires Redis
fn test_cache_with_ttl() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:cache:ttl:*");

    let batch = create_test_batch(10);
    let config = CacheConfig::ipc().with_ttl(3600);

    cache_record_batch(&redis_url(), "rust:cache:ttl:basic", &batch, &config).unwrap();

    // Check TTL is set
    let ttl = cache_ttl(&redis_url(), "rust:cache:ttl:basic").unwrap();
    assert!(ttl.is_some());
    assert!(ttl.unwrap() > 0 && ttl.unwrap() <= 3600);

    cleanup_keys("rust:cache:ttl:*");
}

/// Test cache_exists.
#[test]
#[ignore] // Requires Redis
fn test_cache_exists() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:cache:exists:*");

    let batch = create_test_batch(10);
    let config = CacheConfig::ipc();

    // Should not exist initially
    assert!(!cache_exists(&redis_url(), "rust:cache:exists:test").unwrap());

    // Cache it
    cache_record_batch(&redis_url(), "rust:cache:exists:test", &batch, &config).unwrap();

    // Should exist now
    assert!(cache_exists(&redis_url(), "rust:cache:exists:test").unwrap());

    cleanup_keys("rust:cache:exists:*");
}

/// Test delete_cached.
#[test]
#[ignore] // Requires Redis
fn test_delete_cached() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:cache:delete:*");

    let batch = create_test_batch(10);
    let config = CacheConfig::ipc();

    cache_record_batch(&redis_url(), "rust:cache:delete:test", &batch, &config).unwrap();
    assert!(cache_exists(&redis_url(), "rust:cache:delete:test").unwrap());

    // Delete it
    let deleted = delete_cached(&redis_url(), "rust:cache:delete:test").unwrap();
    assert!(deleted);

    // Should not exist anymore
    assert!(!cache_exists(&redis_url(), "rust:cache:delete:test").unwrap());

    // Deleting non-existent key returns false
    let deleted_again = delete_cached(&redis_url(), "rust:cache:delete:test").unwrap();
    assert!(!deleted_again);

    cleanup_keys("rust:cache:delete:*");
}

/// Test cache_info.
#[test]
#[ignore] // Requires Redis
fn test_cache_info() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:cache:info:*");

    let batch = create_test_batch(100);
    let config = CacheConfig::ipc().with_ttl(7200);

    cache_record_batch(&redis_url(), "rust:cache:info:test", &batch, &config).unwrap();

    let info = cache_info(&redis_url(), "rust:cache:info:test").unwrap();
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
#[test]
#[ignore] // Requires Redis
fn test_get_nonexistent() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let cached = get_cached_record_batch(&redis_url(), "rust:cache:nonexistent:key").unwrap();
    assert!(cached.is_none());
}

/// Test caching empty batch.
#[test]
#[ignore] // Requires Redis
fn test_cache_empty_batch() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:cache:empty:*");

    let batch = create_test_batch(0);
    let config = CacheConfig::ipc();

    cache_record_batch(&redis_url(), "rust:cache:empty:test", &batch, &config).unwrap();

    let cached = get_cached_record_batch(&redis_url(), "rust:cache:empty:test").unwrap();
    assert!(cached.is_some());
    assert_eq!(cached.unwrap().num_rows(), 0);

    cleanup_keys("rust:cache:empty:*");
}

/// Test caching large batch (to test chunking).
#[test]
#[ignore] // Requires Redis
fn test_cache_large_batch() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:cache:large:*");

    // Create a larger batch
    let batch = create_test_batch(10000);

    // Use small chunk size to force chunking
    let config = CacheConfig::ipc().with_chunk_size(1024);

    let bytes_written =
        cache_record_batch(&redis_url(), "rust:cache:large:test", &batch, &config).unwrap();
    assert!(bytes_written > 0);

    // Get info to verify chunking
    let info = cache_info(&redis_url(), "rust:cache:large:test").unwrap();
    assert!(info.is_some());
    let info = info.unwrap();
    assert!(info.is_chunked);
    assert!(info.num_chunks > 1);

    // Retrieve and verify
    let cached = get_cached_record_batch(&redis_url(), "rust:cache:large:test").unwrap();
    assert!(cached.is_some());
    assert_eq!(cached.unwrap().num_rows(), 10000);

    // Delete chunked data
    let deleted = delete_cached(&redis_url(), "rust:cache:large:test").unwrap();
    assert!(deleted);

    cleanup_keys("rust:cache:large:*");
}

/// Test cache without chunking.
#[test]
#[ignore] // Requires Redis
fn test_cache_without_chunking() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:cache:nochunk:*");

    let batch = create_test_batch(1000);
    let config = CacheConfig::ipc().without_chunking();

    cache_record_batch(&redis_url(), "rust:cache:nochunk:test", &batch, &config).unwrap();

    let info = cache_info(&redis_url(), "rust:cache:nochunk:test").unwrap();
    assert!(info.is_some());
    assert!(!info.unwrap().is_chunked);

    cleanup_keys("rust:cache:nochunk:*");
}

/// Test overwriting cached data.
#[test]
#[ignore] // Requires Redis
fn test_cache_overwrite() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    cleanup_keys("rust:cache:overwrite:*");

    let batch1 = create_test_batch(50);
    let batch2 = create_test_batch(100);
    let config = CacheConfig::ipc();

    cache_record_batch(&redis_url(), "rust:cache:overwrite:test", &batch1, &config).unwrap();

    let cached1 = get_cached_record_batch(&redis_url(), "rust:cache:overwrite:test").unwrap();
    assert_eq!(cached1.unwrap().num_rows(), 50);

    // Overwrite with different batch
    cache_record_batch(&redis_url(), "rust:cache:overwrite:test", &batch2, &config).unwrap();

    let cached2 = get_cached_record_batch(&redis_url(), "rust:cache:overwrite:test").unwrap();
    assert_eq!(cached2.unwrap().num_rows(), 100);

    cleanup_keys("rust:cache:overwrite:*");
}
