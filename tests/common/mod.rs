//! Common utilities for integration tests.
//!
//! This module provides helper functions for setting up test data,
//! connecting to Redis, and cleaning up after tests.
//!
//! ## Redis Connection Strategy
//!
//! Tests automatically detect and use Redis in this order:
//! 1. `REDIS_URL` env var (for CI with GitHub Actions service)
//! 2. Existing Redis on port 6379 (from docker-compose or manual)
//! 3. Auto-start container via docker-wrapper
//!
//! ## Usage
//!
//! ```ignore
//! #[tokio::test]
//! async fn test_example() {
//!     let url = ensure_redis().await;
//!     // ... test code using url ...
//! }
//! ```
//!
//! ## Environment Variables
//!
//! - `REDIS_URL`: Redis connection URL (skips auto-detection)
//! - `REDIS_PORT`: Redis port for CLI commands (default: `6379`)

#![allow(dead_code)]

use std::process::Command;
use std::sync::OnceLock;

use docker_wrapper::template::redis::RedisTemplate;
use docker_wrapper::testing::ContainerGuard;

/// Container name for the test Redis instance (when auto-started).
pub const CONTAINER_NAME: &str = "polars-redis-test";

/// Global container guard - only used if we need to start our own container.
static REDIS_GUARD: OnceLock<tokio::sync::OnceCell<Option<ContainerGuard<RedisTemplate>>>> =
    OnceLock::new();

/// Global Redis URL - set once per test run.
static REDIS_URL: OnceLock<String> = OnceLock::new();

/// Ensure Redis is available and return the connection URL.
///
/// This function:
/// 1. Returns `REDIS_URL` env var if set (CI mode)
/// 2. Returns localhost:6379 if Redis is already running there
/// 3. Starts a container via docker-wrapper and returns its URL
///
/// The result is cached - subsequent calls return the same URL.
pub async fn ensure_redis() -> &'static str {
    // Fast path: already initialized
    if let Some(url) = REDIS_URL.get() {
        return url;
    }

    // Check for REDIS_URL env var first (CI mode)
    if let Ok(url) = std::env::var("REDIS_URL") {
        let _ = REDIS_URL.set(url);
        return REDIS_URL.get().unwrap();
    }

    // Check if Redis is already running on default port
    if redis_available() {
        let _ = REDIS_URL.set("redis://localhost:6379".to_string());
        return REDIS_URL.get().unwrap();
    }

    // Need to start our own container
    let cell = REDIS_GUARD.get_or_init(tokio::sync::OnceCell::new);
    let guard = cell
        .get_or_init(|| async {
            let template = RedisTemplate::new(CONTAINER_NAME);
            match ContainerGuard::new(template)
                .reuse_if_running(true)
                .wait_for_ready(true)
                .keep_on_panic(true)
                .start()
                .await
            {
                Ok(g) => Some(g),
                Err(e) => {
                    eprintln!("Warning: Failed to start Redis container: {}", e);
                    None
                },
            }
        })
        .await;

    let url = match guard {
        Some(g) => g.connection_string(),
        None => "redis://localhost:6379".to_string(), // Fallback
    };

    let _ = REDIS_URL.set(url);
    REDIS_URL.get().unwrap()
}

/// Get or create a Redis container guard.
///
/// **Prefer `ensure_redis()` for new code.** This function is kept for
/// backwards compatibility with existing tests.
pub async fn redis_guard() -> &'static ContainerGuard<RedisTemplate> {
    // Ensure Redis is available (might use existing instance)
    let _ = ensure_redis().await;

    // Return the guard if we started a container
    let cell = REDIS_GUARD.get_or_init(tokio::sync::OnceCell::new);
    let guard = cell.get_or_init(|| async { None }).await;

    // If we didn't start a container, we need to create a dummy guard
    // This is a workaround - ideally callers would use ensure_redis() instead
    guard
        .as_ref()
        .expect("redis_guard() called but Redis was already running. Use ensure_redis() instead.")
}

/// Get the Redis URL (for tests using ensure_redis pattern).
pub fn get_redis_url() -> &'static str {
    REDIS_URL
        .get()
        .map(|s| s.as_str())
        .unwrap_or("redis://localhost:6379")
}

/// Get the Redis URL from the container guard or environment.
///
/// Prefers REDIS_URL env var if set (for CI), otherwise uses the container's
/// connection string.
pub fn redis_url_from_guard(guard: &ContainerGuard<RedisTemplate>) -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| guard.connection_string())
}

/// Get the Redis URL from environment or default.
pub fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string())
}

/// Default Redis port for CLI commands.
/// Override with REDIS_PORT env var for CI.
pub fn redis_port() -> u16 {
    std::env::var("REDIS_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(6379)
}

/// Check if Redis is available at the test URL.
pub fn redis_available() -> bool {
    let port = redis_port();
    let output = Command::new("redis-cli")
        .args(["-p", &port.to_string(), "PING"])
        .output();

    match output {
        Ok(o) => o.status.success() && String::from_utf8_lossy(&o.stdout).trim() == "PONG",
        Err(_) => false,
    }
}

/// Check if Redis is available (sync version for #[ignore] tests).
pub fn redis_available_sync() -> bool {
    redis_available()
}

/// Run a redis-cli command and return success status.
pub fn redis_cli(args: &[&str]) -> bool {
    let port_str = redis_port().to_string();
    let mut full_args = vec!["-p", &port_str];
    full_args.extend(args);

    Command::new("redis-cli")
        .args(&full_args)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Run a redis-cli command and return the output as a string.
pub fn redis_cli_output(args: &[&str]) -> Option<String> {
    let port_str = redis_port().to_string();
    let mut full_args = vec!["-p", &port_str];
    full_args.extend(args);

    Command::new("redis-cli")
        .args(&full_args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
}

/// Clean up all keys matching a pattern.
pub fn cleanup_keys(pattern: &str) {
    let port_str = redis_port().to_string();

    // Get all matching keys
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
///
/// Creates hashes with fields: name, age, active
pub fn setup_test_hashes(prefix: &str, count: usize) {
    for i in 1..=count {
        let key = format!("{}{}", prefix, i);
        let name = format!("User{}", i);
        let age = (20 + i).to_string();
        let active = if i % 2 == 0 { "true" } else { "false" };

        redis_cli(&["HSET", &key, "name", &name, "age", &age, "active", active]);
    }
}

/// Set up test JSON documents with standard structure.
///
/// Creates JSON documents with fields: name, age, email
pub fn setup_test_json(prefix: &str, count: usize) {
    for i in 1..=count {
        let key = format!("{}{}", prefix, i);
        let json = format!(
            r#"{{"name": "User{}", "age": {}, "email": "user{}@example.com"}}"#,
            i,
            20 + i,
            i
        );

        redis_cli(&["JSON.SET", &key, "$", &json]);
    }
}

/// Set up test strings.
pub fn setup_test_strings(prefix: &str, count: usize) {
    for i in 1..=count {
        let key = format!("{}{}", prefix, i);
        let value = format!("value{}", i);
        redis_cli(&["SET", &key, &value]);
    }
}

/// Set up test sets.
pub fn setup_test_sets(prefix: &str, count: usize) {
    for i in 1..=count {
        let key = format!("{}{}", prefix, i);
        redis_cli(&["SADD", &key, "member1", "member2", "member3"]);
    }
}

/// Set up test lists.
pub fn setup_test_lists(prefix: &str, count: usize) {
    for i in 1..=count {
        let key = format!("{}{}", prefix, i);
        redis_cli(&["RPUSH", &key, "item1", "item2", "item3"]);
    }
}

/// Set up test sorted sets.
pub fn setup_test_zsets(prefix: &str, count: usize) {
    for i in 1..=count {
        let key = format!("{}{}", prefix, i);
        redis_cli(&[
            "ZADD", &key, "1.0", "member1", "2.0", "member2", "3.0", "member3",
        ]);
    }
}

/// Create a RediSearch index for hashes.
pub fn create_hash_index(index_name: &str, prefix: &str) -> bool {
    // Drop existing index if any
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
        "name",
        "TEXT",
        "SORTABLE",
        "age",
        "NUMERIC",
        "SORTABLE",
        "active",
        "TAG",
    ])
}

/// Parse a value from FT.INFO output (newline-separated key\nvalue pairs).
fn parse_ft_info_value(output: &str, key: &str) -> Option<String> {
    let lines: Vec<&str> = output.lines().collect();
    for (i, line) in lines.iter().enumerate() {
        if *line == key {
            return lines.get(i + 1).map(|s| s.to_string());
        }
    }
    None
}

/// Wait for index to be ready (simple polling).
/// Waits until indexing is complete (percent_indexed reaches 1 or indexing field is 0).
pub fn wait_for_index(index_name: &str) {
    for _ in 0..50 {
        if let Some(output) = redis_cli_output(&["FT.INFO", index_name]) {
            // Check percent_indexed = 1 (indexing complete)
            if let Some(pct) = parse_ft_info_value(&output, "percent_indexed") {
                if pct == "1" {
                    return;
                }
            }
            // Also check indexing = 0 (not currently indexing)
            if let Some(idx) = parse_ft_info_value(&output, "indexing") {
                if idx == "0" {
                    std::thread::sleep(std::time::Duration::from_millis(50));
                    return;
                }
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}

/// Wait for index to have a specific number of documents.
#[allow(dead_code)]
pub fn wait_for_index_docs(index_name: &str, expected_docs: usize) {
    for _ in 0..50 {
        if let Some(output) = redis_cli_output(&["FT.INFO", index_name]) {
            if let Some(num_str) = parse_ft_info_value(&output, "num_docs") {
                if let Ok(num) = num_str.parse::<usize>() {
                    if num >= expected_docs {
                        return;
                    }
                }
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}
