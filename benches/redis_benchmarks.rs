//! Redis-dependent benchmarks for polars-redis operations.
//!
//! These benchmarks require a running Redis instance on localhost:16379.
//! They measure actual Redis scan/write performance with varying configurations.
//!
//! Run with:
//!   cargo bench --bench redis_benchmarks --features "json,search"
//!
//! Setup (start Redis on port 16379):
//!   docker run -d --name polars-redis-bench -p 16379:6379 redis:8

use std::process::Command;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use polars_redis::{BatchConfig, HashBatchIterator, HashSchema, RedisType};

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

/// Clean up keys matching a pattern.
fn cleanup_keys(pattern: &str) {
    let port_str = REDIS_PORT.to_string();

    let output = Command::new("redis-cli")
        .args(["-p", &port_str, "KEYS", pattern])
        .output()
        .ok();

    if let Some(o) = output {
        let stdout = String::from_utf8_lossy(&o.stdout);
        for key in stdout.lines().filter(|s| !s.is_empty()) {
            let _ = Command::new("redis-cli")
                .args(["-p", &port_str, "DEL", key])
                .output();
        }
    }
}

/// Set up test hashes with standard fields.
fn setup_test_hashes(prefix: &str, count: usize) {
    // Use pipeline for faster setup
    let port_str = REDIS_PORT.to_string();

    for i in 1..=count {
        let key = format!("{}{}", prefix, i);
        let name = format!("User{}", i);
        let age = (20 + (i % 50)).to_string();
        let score = format!("{}.{}", i % 100, i % 10);
        let active = if i % 2 == 0 { "true" } else { "false" };

        let _ = Command::new("redis-cli")
            .args([
                "-p", &port_str, "HSET", &key, "name", &name, "age", &age, "score", &score,
                "active", active,
            ])
            .output();
    }
}

/// Benchmark hash scanning with varying data sizes.
fn bench_scan_data_sizes(c: &mut Criterion) {
    if !redis_available() {
        eprintln!(
            "Skipping Redis benchmarks: Redis not available on port {}",
            REDIS_PORT
        );
        return;
    }

    let mut group = c.benchmark_group("scan_data_sizes");
    group.sample_size(10); // Fewer samples for Redis tests

    for size in [100, 500, 1000, 5000].iter() {
        let prefix = format!("bench:size{}:", size);
        cleanup_keys(&format!("{}*", prefix));
        setup_test_hashes(&prefix, *size);

        let schema = HashSchema::new(vec![
            ("name".to_string(), RedisType::Utf8),
            ("age".to_string(), RedisType::Int64),
            ("score".to_string(), RedisType::Float64),
            ("active".to_string(), RedisType::Boolean),
        ])
        .with_key(true);

        let pattern = format!("{}*", prefix);

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("keys", size), size, |b, _| {
            b.iter(|| {
                let config = BatchConfig::new(pattern.clone()).with_batch_size(1000);

                let mut iterator =
                    HashBatchIterator::new(REDIS_URL, schema.clone(), config, None).unwrap();

                let mut total_rows = 0;
                while let Some(batch) = iterator.next_batch().unwrap() {
                    total_rows += batch.num_rows();
                }
                total_rows
            });
        });

        cleanup_keys(&format!("{}*", prefix));
    }

    group.finish();
}

/// Benchmark hash scanning with varying batch sizes.
fn bench_scan_batch_sizes(c: &mut Criterion) {
    if !redis_available() {
        return;
    }

    let mut group = c.benchmark_group("scan_batch_sizes");
    group.sample_size(10);

    let prefix = "bench:batch:";
    let data_size = 1000;

    cleanup_keys(&format!("{}*", prefix));
    setup_test_hashes(prefix, data_size);

    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
        ("score".to_string(), RedisType::Float64),
        ("active".to_string(), RedisType::Boolean),
    ])
    .with_key(true);

    let pattern = format!("{}*", prefix);

    for batch_size in [50, 100, 500, 1000, 5000].iter() {
        group.throughput(Throughput::Elements(data_size as u64));
        group.bench_with_input(
            BenchmarkId::new("batch_size", batch_size),
            batch_size,
            |b, &batch_size| {
                b.iter(|| {
                    let config =
                        BatchConfig::new(pattern.clone()).with_batch_size(batch_size as usize);

                    let mut iterator =
                        HashBatchIterator::new(REDIS_URL, schema.clone(), config, None).unwrap();

                    let mut total_rows = 0;
                    while let Some(batch) = iterator.next_batch().unwrap() {
                        total_rows += batch.num_rows();
                    }
                    total_rows
                });
            },
        );
    }

    cleanup_keys(&format!("{}*", prefix));
    group.finish();
}

/// Benchmark projection pushdown (fewer fields = less data transfer).
fn bench_projection_pushdown(c: &mut Criterion) {
    if !redis_available() {
        return;
    }

    let mut group = c.benchmark_group("projection_pushdown");
    group.sample_size(10);

    let prefix = "bench:proj:";
    let data_size = 1000;

    cleanup_keys(&format!("{}*", prefix));

    // Create hashes with many fields
    let port_str = REDIS_PORT.to_string();
    for i in 1..=data_size {
        let key = format!("{}{}", prefix, i);
        let mut args = vec!["-p", &port_str, "HSET", &key];

        // Add 20 fields
        let fields: Vec<String> = (0..20)
            .flat_map(|j| vec![format!("field_{}", j), format!("value_{}_{}", i, j)])
            .collect();
        let field_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
        args.extend(field_refs);

        let _ = Command::new("redis-cli").args(&args).output();
    }

    let pattern = format!("{}*", prefix);

    // Full schema (20 fields)
    let full_schema = HashSchema::new(
        (0..20)
            .map(|i| (format!("field_{}", i), RedisType::Utf8))
            .collect(),
    )
    .with_key(true);

    // Benchmark with all fields
    group.throughput(Throughput::Elements(data_size as u64));
    group.bench_function("all_20_fields", |b| {
        b.iter(|| {
            let config = BatchConfig::new(pattern.clone()).with_batch_size(1000);

            let mut iterator =
                HashBatchIterator::new(REDIS_URL, full_schema.clone(), config, None).unwrap();

            let mut total_rows = 0;
            while let Some(batch) = iterator.next_batch().unwrap() {
                total_rows += batch.num_rows();
            }
            total_rows
        });
    });

    // Benchmark with projection (5 fields)
    let small_schema = HashSchema::new(
        (0..5)
            .map(|i| (format!("field_{}", i), RedisType::Utf8))
            .collect(),
    )
    .with_key(true);

    group.bench_function("projected_5_fields", |b| {
        b.iter(|| {
            let config = BatchConfig::new(pattern.clone()).with_batch_size(1000);

            let projection = Some(vec![
                "field_0".to_string(),
                "field_1".to_string(),
                "field_2".to_string(),
                "field_3".to_string(),
                "field_4".to_string(),
            ]);

            let mut iterator =
                HashBatchIterator::new(REDIS_URL, small_schema.clone(), config, projection)
                    .unwrap();

            let mut total_rows = 0;
            while let Some(batch) = iterator.next_batch().unwrap() {
                total_rows += batch.num_rows();
            }
            total_rows
        });
    });

    // Benchmark with projection (1 field)
    let single_schema =
        HashSchema::new(vec![("field_0".to_string(), RedisType::Utf8)]).with_key(true);

    group.bench_function("projected_1_field", |b| {
        b.iter(|| {
            let config = BatchConfig::new(pattern.clone()).with_batch_size(1000);

            let projection = Some(vec!["field_0".to_string()]);

            let mut iterator =
                HashBatchIterator::new(REDIS_URL, single_schema.clone(), config, projection)
                    .unwrap();

            let mut total_rows = 0;
            while let Some(batch) = iterator.next_batch().unwrap() {
                total_rows += batch.num_rows();
            }
            total_rows
        });
    });

    cleanup_keys(&format!("{}*", prefix));
    group.finish();
}

/// Benchmark schema inference.
fn bench_schema_inference(c: &mut Criterion) {
    if !redis_available() {
        return;
    }

    let mut group = c.benchmark_group("schema_inference");
    group.sample_size(10);

    let prefix = "bench:infer:";

    cleanup_keys(&format!("{}*", prefix));

    // Create hashes with mixed types
    for i in 1..=100 {
        let key = format!("{}{}", prefix, i);
        let int_val = i.to_string();
        let float_val = format!("{}.{}", i, i % 10);
        let bool_val = if i % 2 == 0 { "true" } else { "false" };

        redis_cli(&[
            "HSET",
            &key,
            "int_field",
            &int_val,
            "float_field",
            &float_val,
            "bool_field",
            bool_val,
            "string_field",
            &format!("text_{}", i),
        ]);
    }

    let pattern = format!("{}*", prefix);

    for sample_size in [10, 50, 100].iter() {
        group.bench_with_input(
            BenchmarkId::new("sample_size", sample_size),
            sample_size,
            |b, &sample_size| {
                b.iter(|| {
                    polars_redis::infer_hash_schema(REDIS_URL, &pattern, sample_size as usize, true)
                        .unwrap()
                });
            },
        );
    }

    cleanup_keys(&format!("{}*", prefix));
    group.finish();
}

/// Benchmark write operations.
fn bench_write_operations(c: &mut Criterion) {
    if !redis_available() {
        return;
    }

    let mut group = c.benchmark_group("write_operations");
    group.sample_size(10);

    for size in [100, 500, 1000].iter() {
        let prefix = format!("bench:write{}:", size);

        // Create test data (keys, fields, values)
        let keys: Vec<String> = (1..=*size).map(|i| format!("{}{}", prefix, i)).collect();
        let fields = vec!["name".to_string(), "age".to_string(), "score".to_string()];
        let values: Vec<Vec<Option<String>>> = (1..=*size)
            .map(|i| {
                vec![
                    Some(format!("User{}", i)),
                    Some((20 + (i % 50)).to_string()),
                    Some(format!("{:.1}", i as f64 * 1.5)),
                ]
            })
            .collect();

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(
            BenchmarkId::new("rows", size),
            &(keys.clone(), fields.clone(), values.clone()),
            |b, (keys, fields, values)| {
                b.iter(|| {
                    cleanup_keys(&format!("{}*", prefix));

                    let result = polars_redis::write_hashes(
                        REDIS_URL,
                        keys.clone(),
                        fields.clone(),
                        values.clone(),
                        None,
                        polars_redis::WriteMode::Replace,
                    )
                    .unwrap();
                    result.keys_written
                });
            },
        );

        cleanup_keys(&format!("{}*", prefix));
    }

    group.finish();
}

criterion_group!(
    redis_benches,
    bench_scan_data_sizes,
    bench_scan_batch_sizes,
    bench_projection_pushdown,
    bench_schema_inference,
    bench_write_operations,
);
criterion_main!(redis_benches);
