//! Common utilities for integration tests.
//!
//! This module provides helper functions for setting up test data,
//! connecting to Redis, and cleaning up after tests.
//!
//! ## Environment Variables
//!
//! - `REDIS_URL`: Redis connection URL (default: `redis://localhost:16379`)
//! - `REDIS_PORT`: Redis port for CLI commands (default: `16379`)
//!
//! For CI, set `REDIS_URL=redis://localhost:6379` and `REDIS_PORT=6379` to use
//! the GitHub Actions Redis service.
//!
//! For local development, the defaults use docker-wrapper to manage a Redis 8
//! container on port 16379 to avoid conflicts with any local Redis.

#![allow(dead_code)]

use std::process::Command;
use std::sync::OnceLock;

use docker_wrapper::{DockerCommand, RmCommand, RunCommand, StopCommand};

/// Default Redis URL for tests.
/// Override with REDIS_URL env var for CI.
pub fn redis_url() -> String {
    std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:16379".to_string())
}

/// Default Redis port for CLI commands.
/// Override with REDIS_PORT env var for CI.
pub fn redis_port() -> u16 {
    std::env::var("REDIS_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(16379)
}

/// For backward compatibility - returns the URL as a static str equivalent.
/// Prefer using `redis_url()` for new code.
pub const REDIS_URL: &str = "redis://localhost:16379";
pub const REDIS_PORT: u16 = 16379;
pub const CONTAINER_NAME: &str = "polars-redis-test";

/// Global container state - ensures we only start one container per test run.
static CONTAINER_STARTED: OnceLock<bool> = OnceLock::new();

/// Start a Redis 8 container for testing.
///
/// Redis 8 includes the query engine and JSON support natively,
/// so no need for redis-stack.
///
/// The container is started only once per test run and reused across tests.
///
/// Note: In CI, the Redis service is provided externally, so this function
/// will detect that Redis is already available and skip container creation.
pub async fn ensure_redis_container() -> bool {
    // Check if Redis is already available (e.g., in CI)
    if redis_available() {
        let _ = CONTAINER_STARTED.set(true);
        return true;
    }

    // Check if already started by us
    if CONTAINER_STARTED.get().is_some() {
        return true;
    }

    // Stop any existing container with the same name
    let _ = StopCommand::new(CONTAINER_NAME).execute().await;
    let _ = RmCommand::new(CONTAINER_NAME).force().execute().await;

    // Start Redis 8 container on the configured port
    let port = redis_port();
    let result = RunCommand::new("redis:8")
        .name(CONTAINER_NAME)
        .detach()
        .port(port, 6379)
        .rm() // Remove on stop
        .execute()
        .await;

    if result.is_err() {
        eprintln!("Failed to start Redis container: {:?}", result.err());
        return false;
    }

    // Wait for Redis to be ready
    for _ in 0..30 {
        if redis_available() {
            let _ = CONTAINER_STARTED.set(true);
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    eprintln!("Redis container started but not responding");
    false
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

/// Cleanup function to stop the test container (call at end of test suite).
pub async fn stop_redis_container() {
    let _ = StopCommand::new(CONTAINER_NAME).execute().await;
}
