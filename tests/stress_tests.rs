//! Stress tests for polars-redis operations.
//!
//! These tests verify behavior under high load conditions:
//! - Large datasets (10k, 100k, 1M keys)
//! - Memory efficiency
//! - Long-running operations
//!
//! Run with:
//!   cargo test --test stress_tests --features "json,search" -- --ignored --nocapture
//!
//! Environment variables:
//! - REDIS_URL: Redis connection URL (default: redis://localhost:16379)
//! - REDIS_PORT: Redis port for CLI commands (default: 16379)

use std::process::Command;
use std::time::{Duration, Instant};

use polars_redis::{BatchConfig, HashBatchIterator, HashSchema, RedisType, WriteMode};

/// Get Redis URL from environment or use default.
fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:16379".to_string())
}

/// Get Redis port from environment or use default.
fn redis_port() -> u16 {
    std::env::var("REDIS_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(16379)
}

/// Check if Redis is available.
fn redis_available() -> bool {
    let port = redis_port();
    Command::new("redis-cli")
        .args(["-p", &port.to_string(), "PING"])
        .output()
        .map(|o| o.status.success() && String::from_utf8_lossy(&o.stdout).trim() == "PONG")
        .unwrap_or(false)
}

/// Run a redis-cli command.
fn redis_cli(args: &[&str]) -> bool {
    let port_str = redis_port().to_string();
    let mut full_args = vec!["-p", &port_str];
    full_args.extend(args);

    Command::new("redis-cli")
        .args(&full_args)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Clean up keys matching a pattern using SCAN (safe for large datasets).
fn cleanup_keys_safe(pattern: &str) {
    let port_str = redis_port().to_string();
    let mut cursor = "0".to_string();

    loop {
        let output = Command::new("redis-cli")
            .args([
                "-p", &port_str, "SCAN", &cursor, "MATCH", pattern, "COUNT", "1000",
            ])
            .output()
            .ok();

        if let Some(o) = output {
            let stdout = String::from_utf8_lossy(&o.stdout);
            let lines: Vec<&str> = stdout.lines().collect();

            if lines.len() >= 2 {
                cursor = lines[0].to_string();

                // Delete keys in this batch
                let keys: Vec<&str> = lines[1..]
                    .iter()
                    .filter(|s| !s.is_empty())
                    .copied()
                    .collect();
                if !keys.is_empty() {
                    let mut del_args = vec!["-p", &port_str, "DEL"];
                    del_args.extend(keys);
                    let _ = Command::new("redis-cli").args(&del_args).output();
                }
            }

            if cursor == "0" {
                break;
            }
        } else {
            break;
        }
    }
}

/// Set up test hashes using pipelining for speed.
fn setup_test_hashes_pipelined(prefix: &str, count: usize) {
    let port_str = redis_port().to_string();

    // Use pipelining for faster setup - batch 1000 commands at a time
    let batch_size = 1000;
    let mut batch = Vec::new();

    for i in 1..=count {
        let key = format!("{}{}", prefix, i);
        let name = format!("User{}", i);
        let age = (20 + (i % 50)).to_string();
        let score = format!("{}.{}", i % 100, i % 10);
        let active = if i % 2 == 0 { "true" } else { "false" };
        let email = format!("user{}@example.com", i);

        batch.push(format!(
            "HSET {} name {} age {} score {} active {} email {}",
            key, name, age, score, active, email
        ));

        if batch.len() >= batch_size || i == count {
            // Execute batch via pipeline
            let pipeline_input = batch.join("\n");
            let mut child = Command::new("redis-cli")
                .args(["-p", &port_str, "--pipe"])
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .expect("Failed to spawn redis-cli");

            if let Some(mut stdin) = child.stdin.take() {
                use std::io::Write;
                let _ = stdin.write_all(pipeline_input.as_bytes());
            }
            let _ = child.wait();

            batch.clear();
        }
    }
}

/// Get current memory usage from Redis.
fn get_redis_memory_usage() -> Option<u64> {
    let port_str = redis_port().to_string();
    let output = Command::new("redis-cli")
        .args(["-p", &port_str, "INFO", "memory"])
        .output()
        .ok()?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        if line.starts_with("used_memory:") {
            return line.split(':').nth(1)?.parse().ok();
        }
    }
    None
}

/// Format bytes as human-readable string.
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

// =============================================================================
// Large Dataset Tests
// =============================================================================

/// Test scanning 10,000 keys.
#[test]
#[ignore]
fn stress_scan_10k_keys() {
    if !redis_available() {
        eprintln!("Skipping: Redis not available");
        return;
    }

    let prefix = "stress:10k:";
    let count = 10_000;

    eprintln!("\n=== Stress Test: Scanning {} keys ===", count);

    // Setup
    eprintln!("Setting up {} hashes...", count);
    cleanup_keys_safe(&format!("{}*", prefix));
    let setup_start = Instant::now();
    setup_test_hashes_pipelined(prefix, count);
    eprintln!("Setup completed in {:?}", setup_start.elapsed());

    let mem_before = get_redis_memory_usage();

    // Test scanning
    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
        ("score".to_string(), RedisType::Float64),
        ("active".to_string(), RedisType::Boolean),
        ("email".to_string(), RedisType::Utf8),
    ])
    .with_key(true);

    let config = BatchConfig::new(format!("{}*", prefix)).with_batch_size(1000);

    let scan_start = Instant::now();
    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let mut total_rows = 0;
    let mut batch_count = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
        batch_count += 1;
    }
    let scan_duration = scan_start.elapsed();

    eprintln!("Scan completed:");
    eprintln!("  - Total rows: {}", total_rows);
    eprintln!("  - Batches: {}", batch_count);
    eprintln!("  - Duration: {:?}", scan_duration);
    eprintln!(
        "  - Throughput: {:.0} rows/sec",
        total_rows as f64 / scan_duration.as_secs_f64()
    );

    if let Some(mem) = mem_before {
        eprintln!("  - Redis memory: {}", format_bytes(mem));
    }

    assert_eq!(total_rows, count);

    // Cleanup
    cleanup_keys_safe(&format!("{}*", prefix));
    eprintln!("=== Test completed ===\n");
}

/// Test scanning 100,000 keys.
#[test]
#[ignore]
fn stress_scan_100k_keys() {
    if !redis_available() {
        eprintln!("Skipping: Redis not available");
        return;
    }

    let prefix = "stress:100k:";
    let count = 100_000;

    eprintln!("\n=== Stress Test: Scanning {} keys ===", count);

    // Setup
    eprintln!("Setting up {} hashes...", count);
    cleanup_keys_safe(&format!("{}*", prefix));
    let setup_start = Instant::now();
    setup_test_hashes_pipelined(prefix, count);
    eprintln!("Setup completed in {:?}", setup_start.elapsed());

    let mem_before = get_redis_memory_usage();

    // Test scanning with larger batch size for efficiency
    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
        ("score".to_string(), RedisType::Float64),
        ("active".to_string(), RedisType::Boolean),
        ("email".to_string(), RedisType::Utf8),
    ])
    .with_key(true);

    let config = BatchConfig::new(format!("{}*", prefix)).with_batch_size(5000);

    let scan_start = Instant::now();
    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let mut total_rows = 0;
    let mut batch_count = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
        batch_count += 1;
    }
    let scan_duration = scan_start.elapsed();

    eprintln!("Scan completed:");
    eprintln!("  - Total rows: {}", total_rows);
    eprintln!("  - Batches: {}", batch_count);
    eprintln!("  - Duration: {:?}", scan_duration);
    eprintln!(
        "  - Throughput: {:.0} rows/sec",
        total_rows as f64 / scan_duration.as_secs_f64()
    );

    if let Some(mem) = mem_before {
        eprintln!("  - Redis memory: {}", format_bytes(mem));
    }

    assert_eq!(total_rows, count);

    // Cleanup
    cleanup_keys_safe(&format!("{}*", prefix));
    eprintln!("=== Test completed ===\n");
}

/// Test scanning 1,000,000 keys (1M).
/// This is a heavy test - run with caution.
#[test]
#[ignore]
fn stress_scan_1m_keys() {
    if !redis_available() {
        eprintln!("Skipping: Redis not available");
        return;
    }

    let prefix = "stress:1m:";
    let count = 1_000_000;

    eprintln!("\n=== Stress Test: Scanning {} keys ===", count);
    eprintln!("WARNING: This test creates 1M keys and may take several minutes");

    // Setup
    eprintln!("Setting up {} hashes...", count);
    cleanup_keys_safe(&format!("{}*", prefix));
    let setup_start = Instant::now();
    setup_test_hashes_pipelined(prefix, count);
    eprintln!("Setup completed in {:?}", setup_start.elapsed());

    let mem_before = get_redis_memory_usage();
    if let Some(mem) = mem_before {
        eprintln!("Redis memory after setup: {}", format_bytes(mem));
    }

    // Test scanning with large batch size
    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
        ("score".to_string(), RedisType::Float64),
        ("active".to_string(), RedisType::Boolean),
        ("email".to_string(), RedisType::Utf8),
    ])
    .with_key(true);

    let config = BatchConfig::new(format!("{}*", prefix)).with_batch_size(10000);

    let scan_start = Instant::now();
    let mut iterator = HashBatchIterator::new(&redis_url(), schema, config, None)
        .expect("Failed to create iterator");

    let mut total_rows = 0;
    let mut batch_count = 0;
    let mut last_report = Instant::now();

    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
        batch_count += 1;

        // Progress report every 5 seconds
        if last_report.elapsed() > Duration::from_secs(5) {
            eprintln!(
                "  Progress: {} rows ({:.1}%)",
                total_rows,
                (total_rows as f64 / count as f64) * 100.0
            );
            last_report = Instant::now();
        }
    }
    let scan_duration = scan_start.elapsed();

    eprintln!("Scan completed:");
    eprintln!("  - Total rows: {}", total_rows);
    eprintln!("  - Batches: {}", batch_count);
    eprintln!("  - Duration: {:?}", scan_duration);
    eprintln!(
        "  - Throughput: {:.0} rows/sec",
        total_rows as f64 / scan_duration.as_secs_f64()
    );

    assert_eq!(total_rows, count);

    // Cleanup
    eprintln!("Cleaning up...");
    let cleanup_start = Instant::now();
    cleanup_keys_safe(&format!("{}*", prefix));
    eprintln!("Cleanup completed in {:?}", cleanup_start.elapsed());
    eprintln!("=== Test completed ===\n");
}

// =============================================================================
// Write Stress Tests
// =============================================================================

/// Test writing 10,000 hashes.
#[test]
#[ignore]
fn stress_write_10k_hashes() {
    if !redis_available() {
        eprintln!("Skipping: Redis not available");
        return;
    }

    let prefix = "stress:write10k:";
    let count = 10_000;

    eprintln!("\n=== Stress Test: Writing {} hashes ===", count);

    cleanup_keys_safe(&format!("{}*", prefix));

    // Prepare data
    let keys: Vec<String> = (1..=count).map(|i| format!("{}{}", prefix, i)).collect();
    let fields = vec![
        "name".to_string(),
        "age".to_string(),
        "score".to_string(),
        "active".to_string(),
        "email".to_string(),
    ];
    let values: Vec<Vec<Option<String>>> = (1..=count)
        .map(|i| {
            vec![
                Some(format!("User{}", i)),
                Some((20 + (i % 50)).to_string()),
                Some(format!("{}.{}", i % 100, i % 10)),
                Some(if i % 2 == 0 { "true" } else { "false" }.to_string()),
                Some(format!("user{}@example.com", i)),
            ]
        })
        .collect();

    let write_start = Instant::now();
    let result =
        polars_redis::write_hashes(&redis_url(), keys, fields, values, None, WriteMode::Replace)
            .expect("Failed to write hashes");
    let write_duration = write_start.elapsed();

    eprintln!("Write completed:");
    eprintln!("  - Keys written: {}", result.keys_written);
    eprintln!("  - Duration: {:?}", write_duration);
    eprintln!(
        "  - Throughput: {:.0} keys/sec",
        result.keys_written as f64 / write_duration.as_secs_f64()
    );

    assert_eq!(result.keys_written, count);

    // Cleanup
    cleanup_keys_safe(&format!("{}*", prefix));
    eprintln!("=== Test completed ===\n");
}

// =============================================================================
// Long-Running Stability Tests
// =============================================================================

/// Test continuous scanning for 60 seconds.
#[test]
#[ignore]
fn stress_continuous_scan_60s() {
    if !redis_available() {
        eprintln!("Skipping: Redis not available");
        return;
    }

    let prefix = "stress:continuous:";
    let count = 5_000;
    let duration = Duration::from_secs(60);

    eprintln!(
        "\n=== Stress Test: Continuous scanning for {:?} ===",
        duration
    );

    // Setup
    cleanup_keys_safe(&format!("{}*", prefix));
    setup_test_hashes_pipelined(prefix, count);
    eprintln!("Setup {} hashes", count);

    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
        ("score".to_string(), RedisType::Float64),
        ("active".to_string(), RedisType::Boolean),
        ("email".to_string(), RedisType::Utf8),
    ])
    .with_key(true);

    let start = Instant::now();
    let mut iteration_count = 0;
    let mut total_rows_scanned = 0;
    let mut errors = 0;

    while start.elapsed() < duration {
        let config = BatchConfig::new(format!("{}*", prefix)).with_batch_size(1000);

        match HashBatchIterator::new(&redis_url(), schema.clone(), config, None) {
            Ok(mut iterator) => {
                while let Ok(Some(batch)) = iterator.next_batch() {
                    total_rows_scanned += batch.num_rows();
                }
                iteration_count += 1;
            },
            Err(e) => {
                eprintln!("Error on iteration {}: {}", iteration_count, e);
                errors += 1;
            },
        }

        // Brief pause between iterations
        std::thread::sleep(Duration::from_millis(10));
    }

    let elapsed = start.elapsed();

    eprintln!("Continuous scan completed:");
    eprintln!("  - Duration: {:?}", elapsed);
    eprintln!("  - Iterations: {}", iteration_count);
    eprintln!("  - Total rows scanned: {}", total_rows_scanned);
    eprintln!("  - Errors: {}", errors);
    eprintln!(
        "  - Avg iteration time: {:.2}ms",
        elapsed.as_millis() as f64 / iteration_count as f64
    );

    assert_eq!(errors, 0, "Should complete without errors");
    assert!(
        iteration_count > 0,
        "Should complete at least one iteration"
    );

    // Cleanup
    cleanup_keys_safe(&format!("{}*", prefix));
    eprintln!("=== Test completed ===\n");
}

/// Test memory stability during repeated operations.
#[test]
#[ignore]
fn stress_memory_stability() {
    if !redis_available() {
        eprintln!("Skipping: Redis not available");
        return;
    }

    let prefix = "stress:memory:";
    let count = 1_000;
    let iterations = 20;

    eprintln!(
        "\n=== Stress Test: Memory stability over {} iterations ===",
        iterations
    );

    cleanup_keys_safe(&format!("{}*", prefix));

    let initial_memory = get_redis_memory_usage();

    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
        ("score".to_string(), RedisType::Float64),
    ])
    .with_key(true);

    let mut memory_samples = Vec::new();

    for i in 1..=iterations {
        // Write data
        let keys: Vec<String> = (1..=count).map(|j| format!("{}{}", prefix, j)).collect();
        let fields = vec!["name".to_string(), "age".to_string(), "score".to_string()];
        let values: Vec<Vec<Option<String>>> = (1..=count)
            .map(|j| {
                vec![
                    Some(format!("User{}", j)),
                    Some((20 + (j % 50)).to_string()),
                    Some(format!("{}.5", j)),
                ]
            })
            .collect();

        polars_redis::write_hashes(&redis_url(), keys, fields, values, None, WriteMode::Replace)
            .expect("Write failed");

        // Scan data
        let config = BatchConfig::new(format!("{}*", prefix)).with_batch_size(500);
        let mut iterator = HashBatchIterator::new(&redis_url(), schema.clone(), config, None)
            .expect("Failed to create iterator");

        let mut rows = 0;
        while let Some(batch) = iterator.next_batch().expect("Scan failed") {
            rows += batch.num_rows();
        }
        assert_eq!(rows, count);

        // Sample memory
        if let Some(mem) = get_redis_memory_usage() {
            memory_samples.push(mem);
        }

        // Delete half the keys to create memory churn
        for j in 1..=(count / 2) {
            let key = format!("{}{}", prefix, j);
            redis_cli(&["DEL", &key]);
        }

        if i % 5 == 0 {
            eprintln!("  Iteration {}/{} completed", i, iterations);
        }
    }

    let final_memory = get_redis_memory_usage();

    eprintln!("Memory stability test completed:");
    if let (Some(initial), Some(final_mem)) = (initial_memory, final_memory) {
        eprintln!("  - Initial memory: {}", format_bytes(initial));
        eprintln!("  - Final memory: {}", format_bytes(final_mem));

        let min_mem = memory_samples.iter().min().copied().unwrap_or(0);
        let max_mem = memory_samples.iter().max().copied().unwrap_or(0);
        eprintln!("  - Min memory during test: {}", format_bytes(min_mem));
        eprintln!("  - Max memory during test: {}", format_bytes(max_mem));
    }

    // Cleanup
    cleanup_keys_safe(&format!("{}*", prefix));
    eprintln!("=== Test completed ===\n");
}

/// Test with varying field sizes (small to large values).
#[test]
#[ignore]
fn stress_varying_field_sizes() {
    if !redis_available() {
        eprintln!("Skipping: Redis not available");
        return;
    }

    let prefix = "stress:fieldsize:";
    let count = 1_000;

    eprintln!("\n=== Stress Test: Varying field sizes ===");

    cleanup_keys_safe(&format!("{}*", prefix));

    // Create hashes with varying field sizes
    let port_str = redis_port().to_string();
    for i in 1..=count {
        let key = format!("{}{}", prefix, i);
        // Field size varies from 10 bytes to 10KB
        let field_size = 10 + (i % 100) * 100;
        let large_value: String = "x".repeat(field_size);

        let _ = Command::new("redis-cli")
            .args([
                "-p",
                &port_str,
                "HSET",
                &key,
                "small",
                &i.to_string(),
                "medium",
                &format!("value_{}", i),
                "large",
                &large_value,
            ])
            .output();
    }

    let mem_after_setup = get_redis_memory_usage();
    eprintln!("Setup {} hashes with varying sizes", count);
    if let Some(mem) = mem_after_setup {
        eprintln!("  Redis memory: {}", format_bytes(mem));
    }

    // Scan all data
    let schema = HashSchema::new(vec![
        ("small".to_string(), RedisType::Int64),
        ("medium".to_string(), RedisType::Utf8),
        ("large".to_string(), RedisType::Utf8),
    ])
    .with_key(true);

    let config = BatchConfig::new(format!("{}*", prefix)).with_batch_size(100);

    let scan_start = Instant::now();
    let mut iterator = HashBatchIterator::new(&redis_url(), schema.clone(), config, None)
        .expect("Failed to create iterator");

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Scan failed") {
        total_rows += batch.num_rows();
    }
    let scan_duration = scan_start.elapsed();

    eprintln!("Full scan completed:");
    eprintln!("  - Rows: {}", total_rows);
    eprintln!("  - Duration: {:?}", scan_duration);

    // Test with projection (only small field)
    let small_schema =
        HashSchema::new(vec![("small".to_string(), RedisType::Int64)]).with_key(true);

    let config = BatchConfig::new(format!("{}*", prefix)).with_batch_size(100);
    let projection = Some(vec!["small".to_string()]);

    let projected_start = Instant::now();
    let mut iterator = HashBatchIterator::new(&redis_url(), small_schema, config, projection)
        .expect("Failed to create iterator");

    let mut projected_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Scan failed") {
        projected_rows += batch.num_rows();
    }
    let projected_duration = projected_start.elapsed();

    eprintln!("Projected scan (small field only):");
    eprintln!("  - Rows: {}", projected_rows);
    eprintln!("  - Duration: {:?}", projected_duration);
    eprintln!(
        "  - Speedup: {:.2}x",
        scan_duration.as_secs_f64() / projected_duration.as_secs_f64()
    );

    assert_eq!(total_rows, count);
    assert_eq!(projected_rows, count);

    // Cleanup
    cleanup_keys_safe(&format!("{}*", prefix));
    eprintln!("=== Test completed ===\n");
}
