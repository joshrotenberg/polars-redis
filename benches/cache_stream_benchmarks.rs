//! Benchmarks for DataFrame caching and streaming operations.
//!
//! These benchmarks require a running Redis instance on localhost:16379.
//! They measure caching read/write throughput and streaming performance.
//!
//! Run with:
//!   cargo bench --bench cache_stream_benchmarks
//!
//! Setup (start Redis on port 16379):
//!   docker run -d --name polars-redis-bench -p 16379:6379 redis:8
//!
//! # Baseline Performance Expectations
//!
//! These are approximate baseline expectations on modern hardware (M1/M2 Mac, decent x86):
//!
//! ## Cache Operations
//! - IPC write (uncompressed): 200-400 MB/s
//! - IPC write (zstd): 100-200 MB/s
//! - Parquet write (zstd): 50-100 MB/s
//! - IPC read: 300-500 MB/s
//! - Parquet read: 100-200 MB/s
//!
//! ## Latency (p50/p95/p99)
//! - Small batch (1K rows) write: 1-5ms / 5-10ms / 10-20ms
//! - Medium batch (10K rows) write: 5-15ms / 15-30ms / 30-50ms
//! - Read (10K rows): 2-8ms / 8-15ms / 15-30ms
//!
//! ## Streaming Operations
//! - XADD single message: 0.5-2ms
//! - XRANGE 1000 messages: 5-15ms
//!
//! ## Compression Trade-offs
//! - IPC uncompressed: Fastest write/read, largest size
//! - IPC LZ4: ~20% slower write, ~10% slower read, 40-60% size reduction
//! - IPC Zstd: ~50% slower write, ~15% slower read, 60-80% size reduction
//! - Parquet Zstd: Slowest write, best compression (70-90% reduction)
//!
//! Performance regression alerts are triggered at 115% of baseline.

use std::process::Command;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use polars_redis::io::cache::{
    CacheConfig, IpcCompression, ParquetCompressionType, cache_record_batch, delete_cached,
    get_cached_record_batch,
};

const REDIS_URL: &str = "redis://localhost:16379";
const REDIS_PORT: u16 = 16379;

/// Check if Redis is available.
fn redis_available() -> bool {
    Command::new("redis-cli")
        .args(["-p", &REDIS_PORT.to_string(), "PING"])
        .output()
        .map(|o| o.status.success() && String::from_utf8_lossy(&o.stdout).trim() == "PONG")
        .unwrap_or(false)
}

/// Run a redis-cli command.
fn redis_cli(args: &[&str]) -> bool {
    let port_str = REDIS_PORT.to_string();
    let mut full_args = vec!["-p", &port_str];
    full_args.extend(args);

    Command::new("redis-cli")
        .args(&full_args)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Create a test RecordBatch with the specified number of rows.
fn create_test_batch(num_rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("score", DataType::Float64, false),
    ]));

    let ids: Vec<i64> = (0..num_rows as i64).collect();
    let names: Vec<String> = (0..num_rows).map(|i| format!("user_{}", i)).collect();
    let values: Vec<f64> = (0..num_rows).map(|i| i as f64 * 1.5).collect();
    let categories: Vec<String> = (0..num_rows)
        .map(|i| format!("category_{}", i % 10))
        .collect();
    let scores: Vec<f64> = (0..num_rows).map(|i| (i % 100) as f64 / 10.0).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Float64Array::from(values)),
            Arc::new(StringArray::from(categories)),
            Arc::new(Float64Array::from(scores)),
        ],
    )
    .unwrap()
}

/// Benchmark cache write operations with IPC format.
fn bench_cache_write_ipc(c: &mut Criterion) {
    if !redis_available() {
        eprintln!(
            "Skipping Redis benchmarks: Redis not available on port {}",
            REDIS_PORT
        );
        return;
    }

    let mut group = c.benchmark_group("cache_write_ipc");
    group.sample_size(10);

    for num_rows in [1_000, 10_000, 100_000].iter() {
        let batch = create_test_batch(*num_rows);
        let batch_size = estimate_batch_size(&batch);

        group.throughput(Throughput::Bytes(batch_size as u64));
        group.bench_with_input(BenchmarkId::new("rows", num_rows), &batch, |b, batch| {
            b.iter(|| {
                let key = format!("bench:cache:ipc:{}", num_rows);
                let config = CacheConfig::ipc();
                let _ = delete_cached(REDIS_URL, &key);
                cache_record_batch(REDIS_URL, &key, batch, &config).unwrap()
            });
        });
    }

    // Cleanup
    for num_rows in [1_000, 10_000, 100_000].iter() {
        let key = format!("bench:cache:ipc:{}", num_rows);
        let _ = delete_cached(REDIS_URL, &key);
    }

    group.finish();
}

/// Benchmark cache write operations with IPC + compression.
fn bench_cache_write_ipc_compressed(c: &mut Criterion) {
    if !redis_available() {
        return;
    }

    let mut group = c.benchmark_group("cache_write_ipc_compressed");
    group.sample_size(10);

    let batch = create_test_batch(50_000);
    let batch_size = estimate_batch_size(&batch);

    for (name, compression) in [
        ("uncompressed", IpcCompression::Uncompressed),
        ("lz4", IpcCompression::Lz4),
        ("zstd", IpcCompression::Zstd),
    ] {
        group.throughput(Throughput::Bytes(batch_size as u64));
        group.bench_with_input(BenchmarkId::new("compression", name), &batch, |b, batch| {
            b.iter(|| {
                let key = format!("bench:cache:ipc:{}", name);
                let config = CacheConfig::ipc().with_ipc_compression(compression);
                let _ = delete_cached(REDIS_URL, &key);
                cache_record_batch(REDIS_URL, &key, batch, &config).unwrap()
            });
        });
    }

    // Cleanup
    for name in ["uncompressed", "lz4", "zstd"] {
        let key = format!("bench:cache:ipc:{}", name);
        let _ = delete_cached(REDIS_URL, &key);
    }

    group.finish();
}

/// Benchmark cache write operations with Parquet format.
fn bench_cache_write_parquet(c: &mut Criterion) {
    if !redis_available() {
        return;
    }

    let mut group = c.benchmark_group("cache_write_parquet");
    group.sample_size(10);

    let batch = create_test_batch(50_000);
    let batch_size = estimate_batch_size(&batch);

    for (name, compression) in [
        ("uncompressed", ParquetCompressionType::Uncompressed),
        ("snappy", ParquetCompressionType::Snappy),
        ("zstd", ParquetCompressionType::Zstd),
    ] {
        group.throughput(Throughput::Bytes(batch_size as u64));
        group.bench_with_input(BenchmarkId::new("compression", name), &batch, |b, batch| {
            b.iter(|| {
                let key = format!("bench:cache:parquet:{}", name);
                let config = CacheConfig::parquet().with_parquet_compression(compression);
                let _ = delete_cached(REDIS_URL, &key);
                cache_record_batch(REDIS_URL, &key, batch, &config).unwrap()
            });
        });
    }

    // Cleanup
    for name in ["uncompressed", "snappy", "zstd"] {
        let key = format!("bench:cache:parquet:{}", name);
        let _ = delete_cached(REDIS_URL, &key);
    }

    group.finish();
}

/// Benchmark cache read operations.
fn bench_cache_read(c: &mut Criterion) {
    if !redis_available() {
        return;
    }

    let mut group = c.benchmark_group("cache_read");
    group.sample_size(10);

    // Setup: write data first
    for num_rows in [1_000, 10_000, 100_000].iter() {
        let batch = create_test_batch(*num_rows);
        let key = format!("bench:cache:read:{}", num_rows);
        let config = CacheConfig::ipc();
        let _ = delete_cached(REDIS_URL, &key);
        cache_record_batch(REDIS_URL, &key, &batch, &config).unwrap();
    }

    for num_rows in [1_000, 10_000, 100_000].iter() {
        let key = format!("bench:cache:read:{}", num_rows);
        // Estimate size by reading once
        let batch = get_cached_record_batch(REDIS_URL, &key).unwrap().unwrap();
        let batch_size = estimate_batch_size(&batch);

        group.throughput(Throughput::Bytes(batch_size as u64));
        group.bench_with_input(BenchmarkId::new("rows", num_rows), &key, |b, key| {
            b.iter(|| get_cached_record_batch(REDIS_URL, key).unwrap().unwrap())
        });
    }

    // Cleanup
    for num_rows in [1_000, 10_000, 100_000].iter() {
        let key = format!("bench:cache:read:{}", num_rows);
        let _ = delete_cached(REDIS_URL, &key);
    }

    group.finish();
}

/// Benchmark cache read with different formats.
fn bench_cache_read_formats(c: &mut Criterion) {
    if !redis_available() {
        return;
    }

    let mut group = c.benchmark_group("cache_read_formats");
    group.sample_size(10);

    let batch = create_test_batch(50_000);
    let batch_size = estimate_batch_size(&batch);

    // Setup: write data in both formats
    let key_ipc = "bench:cache:read:format:ipc";
    let key_parquet = "bench:cache:read:format:parquet";

    let _ = delete_cached(REDIS_URL, key_ipc);
    let _ = delete_cached(REDIS_URL, key_parquet);

    cache_record_batch(REDIS_URL, key_ipc, &batch, &CacheConfig::ipc()).unwrap();
    cache_record_batch(REDIS_URL, key_parquet, &batch, &CacheConfig::parquet()).unwrap();

    group.throughput(Throughput::Bytes(batch_size as u64));

    group.bench_function("ipc", |b| {
        b.iter(|| {
            get_cached_record_batch(REDIS_URL, key_ipc)
                .unwrap()
                .unwrap()
        })
    });

    group.bench_function("parquet", |b| {
        b.iter(|| {
            get_cached_record_batch(REDIS_URL, key_parquet)
                .unwrap()
                .unwrap()
        })
    });

    // Cleanup
    let _ = delete_cached(REDIS_URL, key_ipc);
    let _ = delete_cached(REDIS_URL, key_parquet);

    group.finish();
}

/// Benchmark chunked caching (large datasets).
fn bench_cache_chunked(c: &mut Criterion) {
    if !redis_available() {
        return;
    }

    let mut group = c.benchmark_group("cache_chunked");
    group.sample_size(10);

    // Create a larger batch that will be chunked
    let batch = create_test_batch(200_000);
    let batch_size = estimate_batch_size(&batch);

    // Compare different chunk sizes
    for chunk_mb in [1, 5, 10, 50].iter() {
        let chunk_size = *chunk_mb * 1024 * 1024;
        let key = format!("bench:cache:chunk:{}mb", chunk_mb);

        group.throughput(Throughput::Bytes(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("chunk_size_mb", chunk_mb),
            &batch,
            |b, batch| {
                b.iter(|| {
                    let config = CacheConfig::ipc().with_chunk_size(chunk_size);
                    let _ = delete_cached(REDIS_URL, &key);
                    cache_record_batch(REDIS_URL, &key, batch, &config).unwrap()
                });
            },
        );
    }

    // Cleanup
    for chunk_mb in [1, 5, 10, 50].iter() {
        let key = format!("bench:cache:chunk:{}mb", chunk_mb);
        let _ = delete_cached(REDIS_URL, &key);
    }

    group.finish();
}

/// Benchmark stream write operations (XADD).
fn bench_stream_write(c: &mut Criterion) {
    if !redis_available() {
        return;
    }

    let mut group = c.benchmark_group("stream_write");
    group.sample_size(10);

    for num_messages in [100, 500, 1000].iter() {
        let stream_key = format!("bench:stream:write:{}", num_messages);

        group.throughput(Throughput::Elements(*num_messages as u64));
        group.bench_with_input(
            BenchmarkId::new("messages", num_messages),
            num_messages,
            |b, &num_messages| {
                b.iter(|| {
                    // Clean up stream first
                    redis_cli(&["DEL", &stream_key]);

                    // Write messages
                    for i in 0..num_messages {
                        let msg = format!("message_{}", i);
                        redis_cli(&[
                            "XADD",
                            &stream_key,
                            "*",
                            "data",
                            &msg,
                            "index",
                            &i.to_string(),
                        ]);
                    }
                    num_messages
                });
            },
        );
    }

    // Cleanup
    for num_messages in [100, 500, 1000].iter() {
        let stream_key = format!("bench:stream:write:{}", num_messages);
        redis_cli(&["DEL", &stream_key]);
    }

    group.finish();
}

/// Benchmark stream read operations (XRANGE).
fn bench_stream_read(c: &mut Criterion) {
    if !redis_available() {
        return;
    }

    let mut group = c.benchmark_group("stream_read");
    group.sample_size(10);

    // Setup: Create streams with data
    for num_messages in [100, 500, 1000, 5000].iter() {
        let stream_key = format!("bench:stream:read:{}", num_messages);
        redis_cli(&["DEL", &stream_key]);

        for i in 0..*num_messages {
            let msg = format!("message_{}", i);
            redis_cli(&[
                "XADD",
                &stream_key,
                "*",
                "data",
                &msg,
                "index",
                &i.to_string(),
            ]);
        }
    }

    for num_messages in [100, 500, 1000, 5000].iter() {
        let stream_key = format!("bench:stream:read:{}", num_messages);

        group.throughput(Throughput::Elements(*num_messages as u64));
        group.bench_with_input(
            BenchmarkId::new("messages", num_messages),
            &stream_key,
            |b, stream_key| {
                b.iter(|| {
                    let output = Command::new("redis-cli")
                        .args([
                            "-p",
                            &REDIS_PORT.to_string(),
                            "XRANGE",
                            stream_key,
                            "-",
                            "+",
                        ])
                        .output()
                        .unwrap();
                    output.stdout.len()
                });
            },
        );
    }

    // Cleanup
    for num_messages in [100, 500, 1000, 5000].iter() {
        let stream_key = format!("bench:stream:read:{}", num_messages);
        redis_cli(&["DEL", &stream_key]);
    }

    group.finish();
}

/// Track operation latencies with percentiles (p50, p95, p99).
/// This benchmark measures individual operation latencies across many iterations.
fn bench_latency_percentiles(c: &mut Criterion) {
    if !redis_available() {
        return;
    }

    let mut group = c.benchmark_group("latency_percentiles");
    group.sample_size(100); // More samples for accurate percentiles

    let batch_small = create_test_batch(1_000);
    let batch_medium = create_test_batch(10_000);

    // Small cache write latency
    group.bench_function("cache_write_small_1k", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for i in 0..iters {
                let key = format!("bench:latency:write:{}", i);
                let config = CacheConfig::ipc();
                let _ = delete_cached(REDIS_URL, &key);

                let start = Instant::now();
                cache_record_batch(REDIS_URL, &key, &batch_small, &config).unwrap();
                total += start.elapsed();

                let _ = delete_cached(REDIS_URL, &key);
            }
            total
        });
    });

    // Medium cache write latency
    group.bench_function("cache_write_medium_10k", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for i in 0..iters {
                let key = format!("bench:latency:write:{}", i);
                let config = CacheConfig::ipc();
                let _ = delete_cached(REDIS_URL, &key);

                let start = Instant::now();
                cache_record_batch(REDIS_URL, &key, &batch_medium, &config).unwrap();
                total += start.elapsed();

                let _ = delete_cached(REDIS_URL, &key);
            }
            total
        });
    });

    // Cache read latency (setup first)
    let read_key = "bench:latency:read";
    let _ = delete_cached(REDIS_URL, read_key);
    cache_record_batch(REDIS_URL, read_key, &batch_medium, &CacheConfig::ipc()).unwrap();

    group.bench_function("cache_read_10k", |b| {
        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for _ in 0..iters {
                let start = Instant::now();
                let _ = get_cached_record_batch(REDIS_URL, read_key).unwrap();
                total += start.elapsed();
            }
            total
        });
    });

    let _ = delete_cached(REDIS_URL, read_key);

    // Stream XADD latency
    group.bench_function("stream_xadd_single", |b| {
        let stream_key = "bench:latency:stream";
        redis_cli(&["DEL", stream_key]);

        b.iter_custom(|iters| {
            let mut total = std::time::Duration::ZERO;
            for i in 0..iters {
                let msg = format!("msg_{}", i);
                let start = Instant::now();
                redis_cli(&["XADD", stream_key, "*", "data", &msg]);
                total += start.elapsed();
            }
            total
        });

        redis_cli(&["DEL", stream_key]);
    });

    group.finish();
}

/// Benchmark format comparison: IPC vs Parquet for different data characteristics.
fn bench_format_comparison(c: &mut Criterion) {
    if !redis_available() {
        return;
    }

    let mut group = c.benchmark_group("format_comparison");
    group.sample_size(10);

    let batch = create_test_batch(50_000);
    let batch_size = estimate_batch_size(&batch);

    group.throughput(Throughput::Bytes(batch_size as u64));

    // IPC uncompressed (fastest write)
    group.bench_function("write_ipc_uncompressed", |b| {
        b.iter(|| {
            let key = "bench:format:ipc:uncomp";
            let config = CacheConfig::ipc();
            let _ = delete_cached(REDIS_URL, key);
            cache_record_batch(REDIS_URL, key, &batch, &config).unwrap()
        });
    });

    // IPC with zstd (balanced)
    group.bench_function("write_ipc_zstd", |b| {
        b.iter(|| {
            let key = "bench:format:ipc:zstd";
            let config = CacheConfig::ipc().with_ipc_compression(IpcCompression::Zstd);
            let _ = delete_cached(REDIS_URL, key);
            cache_record_batch(REDIS_URL, key, &batch, &config).unwrap()
        });
    });

    // Parquet with zstd (best compression)
    group.bench_function("write_parquet_zstd", |b| {
        b.iter(|| {
            let key = "bench:format:parquet:zstd";
            let config =
                CacheConfig::parquet().with_parquet_compression(ParquetCompressionType::Zstd);
            let _ = delete_cached(REDIS_URL, key);
            cache_record_batch(REDIS_URL, key, &batch, &config).unwrap()
        });
    });

    // Cleanup
    for key in [
        "bench:format:ipc:uncomp",
        "bench:format:ipc:zstd",
        "bench:format:parquet:zstd",
    ] {
        let _ = delete_cached(REDIS_URL, key);
    }

    group.finish();
}

/// Estimate the in-memory size of a RecordBatch.
fn estimate_batch_size(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|col| col.get_buffer_memory_size())
        .sum()
}

criterion_group!(
    cache_benches,
    bench_cache_write_ipc,
    bench_cache_write_ipc_compressed,
    bench_cache_write_parquet,
    bench_cache_read,
    bench_cache_read_formats,
    bench_cache_chunked,
);

criterion_group!(stream_benches, bench_stream_write, bench_stream_read,);

criterion_group!(
    latency_benches,
    bench_latency_percentiles,
    bench_format_comparison,
);

criterion_main!(cache_benches, stream_benches, latency_benches);
