//! Benchmarks for polars-redis operations.
//!
//! Run with: cargo bench

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use polars_redis::{BatchConfig, HashSchema, RedisType};

/// Benchmark schema creation.
fn bench_schema_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("schema_creation");

    // Small schema (3 fields)
    group.bench_function("small_3_fields", |b| {
        b.iter(|| {
            let schema = HashSchema::new(vec![
                ("name".to_string(), RedisType::Utf8),
                ("age".to_string(), RedisType::Int64),
                ("active".to_string(), RedisType::Boolean),
            ])
            .with_key(true)
            .with_key_column_name("_key");
            black_box(schema)
        });
    });

    // Medium schema (10 fields)
    group.bench_function("medium_10_fields", |b| {
        b.iter(|| {
            let fields: Vec<(String, RedisType)> = (0..10)
                .map(|i| (format!("field_{}", i), RedisType::Utf8))
                .collect();
            let schema = HashSchema::new(fields)
                .with_key(true)
                .with_key_column_name("_key");
            black_box(schema)
        });
    });

    // Large schema (50 fields)
    group.bench_function("large_50_fields", |b| {
        b.iter(|| {
            let fields: Vec<(String, RedisType)> = (0..50)
                .map(|i| (format!("field_{}", i), RedisType::Utf8))
                .collect();
            let schema = HashSchema::new(fields)
                .with_key(true)
                .with_key_column_name("_key");
            black_box(schema)
        });
    });

    group.finish();
}

/// Benchmark batch config creation.
fn bench_batch_config(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_config");

    group.bench_function("default", |b| {
        b.iter(|| {
            let config = BatchConfig::new("user:*".to_string());
            black_box(config)
        });
    });

    group.bench_function("with_options", |b| {
        b.iter(|| {
            let config = BatchConfig::new("user:*".to_string())
                .with_batch_size(5000)
                .with_count_hint(500);
            black_box(config)
        });
    });

    group.finish();
}

/// Benchmark type parsing performance.
fn bench_type_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("type_parsing");

    for size in [100, 1000, 10000].iter() {
        // Int64 parsing
        let int_values: Vec<String> = (0..*size).map(|i| i.to_string()).collect();
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::new("int64", size), &int_values, |b, values| {
            b.iter(|| {
                let sum: i64 = values.iter().filter_map(|v| v.parse::<i64>().ok()).sum();
                black_box(sum)
            });
        });

        // Float64 parsing
        let float_values: Vec<String> = (0..*size).map(|i| format!("{}.{}", i, i % 100)).collect();
        group.bench_with_input(
            BenchmarkId::new("float64", size),
            &float_values,
            |b, values| {
                b.iter(|| {
                    let sum: f64 = values.iter().filter_map(|v| v.parse::<f64>().ok()).sum();
                    black_box(sum)
                });
            },
        );

        // Boolean parsing
        let bool_values: Vec<String> = (0..*size)
            .map(|i| {
                if i % 2 == 0 {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
            })
            .collect();
        group.bench_with_input(
            BenchmarkId::new("boolean", size),
            &bool_values,
            |b, values| {
                b.iter(|| {
                    let count: usize = values
                        .iter()
                        .filter_map(|v| v.parse::<bool>().ok())
                        .filter(|&b| b)
                        .count();
                    black_box(count)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark Arrow schema generation.
fn bench_arrow_schema(c: &mut Criterion) {
    let mut group = c.benchmark_group("arrow_schema");

    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
        ("email".to_string(), RedisType::Utf8),
        ("score".to_string(), RedisType::Float64),
        ("active".to_string(), RedisType::Boolean),
        ("created_at".to_string(), RedisType::Datetime),
        ("birth_date".to_string(), RedisType::Date),
    ])
    .with_key(true)
    .with_key_column_name("_key")
    .with_ttl(true)
    .with_ttl_column_name("_ttl");

    group.bench_function("to_arrow_schema", |b| {
        b.iter(|| {
            let arrow_schema = schema.to_arrow_schema();
            black_box(arrow_schema)
        });
    });

    group.finish();
}

/// Benchmark projection filtering.
fn bench_projection(c: &mut Criterion) {
    let mut group = c.benchmark_group("projection");

    let all_fields: Vec<String> = (0..50).map(|i| format!("field_{}", i)).collect();

    // No projection
    group.bench_function("no_filter", |b| {
        b.iter(|| {
            let result: Vec<&String> = all_fields.iter().collect();
            black_box(result)
        });
    });

    // Small projection (5 fields)
    let projection_small: std::collections::HashSet<String> =
        (0..5).map(|i| format!("field_{}", i)).collect();
    group.bench_function("5_of_50_fields", |b| {
        b.iter(|| {
            let result: Vec<&String> = all_fields
                .iter()
                .filter(|f| projection_small.contains(*f))
                .collect();
            black_box(result)
        });
    });

    // Medium projection (25 fields)
    let projection_medium: std::collections::HashSet<String> =
        (0..25).map(|i| format!("field_{}", i)).collect();
    group.bench_function("25_of_50_fields", |b| {
        b.iter(|| {
            let result: Vec<&String> = all_fields
                .iter()
                .filter(|f| projection_medium.contains(*f))
                .collect();
            black_box(result)
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_schema_creation,
    bench_batch_config,
    bench_type_parsing,
    bench_arrow_schema,
    bench_projection,
);
criterion_main!(benches);
