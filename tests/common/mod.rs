//! Common utilities for integration tests.
//!
//! This module provides helper functions for setting up test data,
//! connecting to Redis, and cleaning up after tests.
//!
//! ## Using ContainerGuard (Recommended)
//!
//! For tests that need Redis, use `redis_guard()` to get a `ContainerGuard`
//! that automatically manages the container lifecycle:
//!
//! ```ignore
//! #[tokio::test]
//! async fn test_example() {
//!     let guard = redis_guard().await;
//!     let url = guard.connection_string();
//!     // ... test code using url ...
//!     // Container is automatically cleaned up when guard is dropped
//! }
//! ```
//!
//! ## Environment Variables
//!
//! - `REDIS_URL`: Redis connection URL (default: from container)
//! - `REDIS_PORT`: Redis port for CLI commands (default: `6379`)
//!
//! For CI, set `REDIS_URL=redis://localhost:6379` and `REDIS_PORT=6379` to use
//! the GitHub Actions Redis service.

#![allow(dead_code)]

use std::process::Command;
use std::sync::OnceLock;

use docker_wrapper::template::redis::RedisTemplate;
use docker_wrapper::testing::ContainerGuard;

/// Container name for the test Redis instance.
pub const CONTAINER_NAME: &str = "polars-redis-test";

/// Global container guard - ensures we only start one container per test run.
static REDIS_GUARD: OnceLock<tokio::sync::OnceCell<ContainerGuard<RedisTemplate>>> =
    OnceLock::new();

/// Get or create a Redis container guard.
///
/// This function ensures only one Redis container is started per test run,
/// and it's automatically cleaned up when all tests complete.
///
/// The container is configured with:
/// - `reuse_if_running(true)` - Reuses existing containers for faster local dev
/// - `wait_for_ready(true)` - Waits for Redis to be ready before returning
/// - `keep_on_panic(true)` - Keeps container running on test failure for debugging
///
/// # Returns
/// A reference to the ContainerGuard that provides the connection string.
pub async fn redis_guard() -> &'static ContainerGuard<RedisTemplate> {
    let cell = REDIS_GUARD.get_or_init(tokio::sync::OnceCell::new);
    cell.get_or_init(|| async {
        let template = RedisTemplate::new(CONTAINER_NAME);
        ContainerGuard::new(template)
            .reuse_if_running(true)
            .wait_for_ready(true)
            .keep_on_panic(true)
            .start()
            .await
            .expect("Failed to start Redis container")
    })
    .await
}

/// Get the Redis URL from the container guard or environment.
///
/// Prefers REDIS_URL env var if set (for CI), otherwise uses the container's
/// connection string.
pub fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string())
}

/// Get the Redis URL from an active container guard.
pub fn redis_url_from_guard(guard: &ContainerGuard<RedisTemplate>) -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| guard.connection_string())
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

/// Wait for index to be ready (simple polling).
pub fn wait_for_index(index_name: &str) {
    for _ in 0..10 {
        if let Some(output) = redis_cli_output(&["FT.INFO", index_name])
            && output.contains("num_docs")
        {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}
