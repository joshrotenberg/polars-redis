//! Integration tests for Redis Cluster operations.
//!
//! These tests require Docker to start a Redis Cluster.
//! Tests use docker-wrapper's RedisClusterTemplate to manage the cluster.
//!
//! ## Environment Variables
//!
//! - `REDIS_CLUSTER_PORT_BASE`: Base port for cluster nodes (default: 17000)
//! - `REDIS_CLUSTER_NODES`: Comma-separated list of node URLs (optional, for CI)
//!
//! For CI with external cluster on ports 7000-7005:
//!   REDIS_CLUSTER_PORT_BASE=7000 cargo test --test integration_cluster ...
//!
//! Run with: `cargo test --test integration_cluster --all-features`
//! Run ignored tests: `cargo test --test integration_cluster --all-features -- --ignored`

#![cfg(feature = "cluster")]

use std::process::Command;
use std::sync::OnceLock;

use docker_wrapper::Template;
use docker_wrapper::template::redis::cluster::{RedisClusterConnection, RedisClusterTemplate};

use arrow::datatypes::DataType;
use polars_redis::{
    BatchConfig, ClusterHashBatchIterator, ClusterJsonBatchIterator, ClusterListBatchIterator,
    ClusterSetBatchIterator, ClusterStringBatchIterator, ClusterZSetBatchIterator, HashSchema,
    JsonSchema, ListSchema, RedisType, SetSchema, StringSchema, ZSetSchema,
};

/// Cluster configuration constants.
const CLUSTER_NAME: &str = "polars-redis-cluster-test";
const DEFAULT_CLUSTER_PORT_BASE: u16 = 17000;
const NUM_MASTERS: usize = 3;

/// Get cluster port base from environment or use default.
fn cluster_port_base() -> u16 {
    std::env::var("REDIS_CLUSTER_PORT_BASE")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_CLUSTER_PORT_BASE)
}

/// Global cluster state - ensures we only start one cluster per test run.
static CLUSTER_STARTED: OnceLock<RedisClusterConnection> = OnceLock::new();

/// Get cluster connection info, starting cluster if needed.
fn get_cluster_connection() -> Option<&'static RedisClusterConnection> {
    CLUSTER_STARTED.get()
}

/// Check if an external cluster is already available (e.g., in CI).
fn external_cluster_available() -> bool {
    let port = cluster_port_base();
    Command::new("redis-cli")
        .args(["-p", &port.to_string(), "CLUSTER", "INFO"])
        .output()
        .map(|o| {
            o.status.success() && String::from_utf8_lossy(&o.stdout).contains("cluster_state:ok")
        })
        .unwrap_or(false)
}

/// Start Redis Cluster for testing (blocking).
///
/// Returns the cluster connection info if successful.
/// If an external cluster is already available (CI), uses that instead.
fn ensure_cluster_started() -> Option<&'static RedisClusterConnection> {
    if let Some(conn) = CLUSTER_STARTED.get() {
        return Some(conn);
    }

    let port_base = cluster_port_base();

    // Check if external cluster is already available (e.g., in CI)
    if external_cluster_available() {
        // For external clusters, we don't have a RedisClusterConnection from docker-wrapper.
        // The cluster_nodes() function will build URLs from the port base instead.
        // We still need to set something in CLUSTER_STARTED to indicate we're ready.
        // Create a minimal template just to get a connection object.
        let template = RedisClusterTemplate::new("external-cluster")
            .num_masters(NUM_MASTERS)
            .port_base(port_base);
        let conn = RedisClusterConnection::from_template(&template);
        let _ = CLUSTER_STARTED.set(conn);
        return CLUSTER_STARTED.get();
    }

    // Create and start the cluster using docker-wrapper
    let template = RedisClusterTemplate::new(CLUSTER_NAME)
        .num_masters(NUM_MASTERS)
        .port_base(port_base)
        .auto_remove();

    // Start cluster (blocking)
    let rt = tokio::runtime::Runtime::new().ok()?;
    let started = rt.block_on(async {
        // Stop any existing cluster first
        let _ = template.stop().await;
        let _ = template.remove().await;

        // Start the cluster
        if template.start().await.is_err() {
            return false;
        }

        // Wait for cluster to be ready
        for _ in 0..60 {
            if let Ok(info) = template.cluster_info().await
                && info.cluster_state == "ok"
            {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
        false
    });

    if !started {
        eprintln!("Failed to start Redis Cluster");
        return None;
    }

    // Store connection info
    let conn = RedisClusterConnection::from_template(&template);
    let _ = CLUSTER_STARTED.set(conn);
    CLUSTER_STARTED.get()
}

/// Check if cluster is available.
fn cluster_available() -> bool {
    ensure_cluster_started().is_some()
}

/// Get cluster node URLs for connecting.
fn cluster_nodes() -> Vec<String> {
    get_cluster_connection()
        .map(|conn| {
            // nodes_string returns comma-separated "host:port" entries
            conn.nodes_string()
                .split(',')
                .map(|n| format!("redis://{}", n.trim()))
                .collect()
        })
        .unwrap_or_default()
}

/// Run redis-cli against the first cluster node.
fn cluster_redis_cli(args: &[&str]) -> bool {
    let port_str = cluster_port_base().to_string();
    let mut full_args = vec!["-p", &port_str, "-c"]; // -c for cluster mode
    full_args.extend(args);

    Command::new("redis-cli")
        .args(&full_args)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Clean up all keys matching a pattern across the cluster.
fn cleanup_cluster_keys(pattern: &str) {
    let port_base = cluster_port_base();
    let port_str = port_base.to_string();

    // In cluster mode, we need to scan each node
    for port_offset in 0..NUM_MASTERS {
        let port = (port_base + port_offset as u16).to_string();

        // Get all matching keys from this node
        let output = Command::new("redis-cli")
            .args(["-p", &port, "KEYS", pattern])
            .output()
            .ok();

        if let Some(o) = output {
            let stdout = String::from_utf8_lossy(&o.stdout);
            for key in stdout.lines().filter(|s| !s.is_empty()) {
                // Use cluster mode to delete (routes to correct node)
                let _ = Command::new("redis-cli")
                    .args(["-p", &port_str, "-c", "DEL", key])
                    .output();
            }
        }
    }
}

/// Set up test hashes in the cluster.
fn setup_cluster_hashes(prefix: &str, count: usize) {
    let port_str = cluster_port_base().to_string();
    for i in 1..=count {
        let key = format!("{}{}", prefix, i);
        let name = format!("User{}", i);
        let age = (20 + i).to_string();
        let active = if i % 2 == 0 { "true" } else { "false" };

        // Use -c for cluster mode routing
        let _ = Command::new("redis-cli")
            .args([
                "-p", &port_str, "-c", "HSET", &key, "name", &name, "age", &age, "active", active,
            ])
            .output();
    }
}

/// Set up test JSON documents in the cluster.
#[allow(dead_code)]
fn setup_cluster_json(prefix: &str, count: usize) {
    let port_str = cluster_port_base().to_string();
    for i in 1..=count {
        let key = format!("{}{}", prefix, i);
        let json = format!(
            r#"{{"name": "User{}", "age": {}, "email": "user{}@example.com"}}"#,
            i,
            20 + i,
            i
        );

        let _ = Command::new("redis-cli")
            .args(["-p", &port_str, "-c", "JSON.SET", &key, "$", &json])
            .output();
    }
}

/// Set up test strings in the cluster.
fn setup_cluster_strings(prefix: &str, count: usize) {
    let port_str = cluster_port_base().to_string();
    for i in 1..=count {
        let key = format!("{}{}", prefix, i);
        let value = format!("value{}", i);

        let _ = Command::new("redis-cli")
            .args(["-p", &port_str, "-c", "SET", &key, &value])
            .output();
    }
}

// =============================================================================
// Hash Cluster Tests
// =============================================================================

/// Test basic hash scanning across a cluster.
#[test]
#[ignore] // Requires Redis Cluster
fn test_cluster_scan_hashes_basic() {
    if !cluster_available() {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let nodes = cluster_nodes();
    if nodes.is_empty() {
        eprintln!("Skipping test: No cluster nodes available");
        return;
    }

    // Setup test data
    cleanup_cluster_keys("cluster:hash:*");
    setup_cluster_hashes("cluster:hash:", 30); // Use 30 to ensure distribution across nodes

    // Create schema
    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
        ("active".to_string(), RedisType::Boolean),
    ])
    .with_key(true)
    .with_key_column_name("_key".to_string());

    // Create config
    let config = BatchConfig::new("cluster:hash:*".to_string())
        .with_batch_size(100)
        .with_count_hint(50);

    // Create cluster iterator
    let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
    let mut iterator = ClusterHashBatchIterator::new(&node_refs, schema, config, None)
        .expect("Failed to create cluster iterator");

    // Collect all batches
    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
        assert!(batch.num_columns() >= 3); // name, age, active (+ _key)
    }

    assert_eq!(total_rows, 30);
    assert!(iterator.is_done());

    // Cleanup
    cleanup_cluster_keys("cluster:hash:*");
}

/// Test cluster hash scanning with projection.
#[test]
#[ignore] // Requires Redis Cluster
fn test_cluster_scan_hashes_projection() {
    if !cluster_available() {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let nodes = cluster_nodes();
    if nodes.is_empty() {
        return;
    }

    cleanup_cluster_keys("cluster:proj:*");
    setup_cluster_hashes("cluster:proj:", 15);

    let schema = HashSchema::new(vec![
        ("name".to_string(), RedisType::Utf8),
        ("age".to_string(), RedisType::Int64),
        ("active".to_string(), RedisType::Boolean),
    ])
    .with_key(false);

    let config = BatchConfig::new("cluster:proj:*".to_string()).with_batch_size(100);

    // Only request 'name' field
    let projection = Some(vec!["name".to_string()]);
    let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
    let mut iterator = ClusterHashBatchIterator::new(&node_refs, schema, config, projection)
        .expect("Failed to create iterator");

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
        // Should only have the projected column
        assert_eq!(batch.num_columns(), 1);
    }

    assert_eq!(total_rows, 15);

    cleanup_cluster_keys("cluster:proj:*");
}

/// Test cluster hash scanning with max_rows limit.
#[test]
#[ignore] // Requires Redis Cluster
fn test_cluster_scan_hashes_max_rows() {
    if !cluster_available() {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let nodes = cluster_nodes();
    if nodes.is_empty() {
        return;
    }

    cleanup_cluster_keys("cluster:maxrows:*");
    setup_cluster_hashes("cluster:maxrows:", 50);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(true);

    let config = BatchConfig::new("cluster:maxrows:*".to_string())
        .with_batch_size(100)
        .with_max_rows(10); // Only get 10 rows

    let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
    let mut iterator = ClusterHashBatchIterator::new(&node_refs, schema, config, None)
        .expect("Failed to create iterator");

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 10);

    cleanup_cluster_keys("cluster:maxrows:*");
}

/// Test cluster hash scanning with small batches.
#[test]
#[ignore] // Requires Redis Cluster
fn test_cluster_scan_hashes_small_batches() {
    if !cluster_available() {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let nodes = cluster_nodes();
    if nodes.is_empty() {
        return;
    }

    cleanup_cluster_keys("cluster:batch:*");
    setup_cluster_hashes("cluster:batch:", 30);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(false);

    let config = BatchConfig::new("cluster:batch:*".to_string())
        .with_batch_size(5) // Very small batch
        .with_count_hint(5);

    let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
    let mut iterator = ClusterHashBatchIterator::new(&node_refs, schema, config, None)
        .expect("Failed to create iterator");

    let mut batch_count = 0;
    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        batch_count += 1;
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 30);
    assert!(batch_count >= 6); // Should have multiple batches

    cleanup_cluster_keys("cluster:batch:*");
}

/// Test cluster hash scanning with TTL.
#[test]
#[ignore] // Requires Redis Cluster
fn test_cluster_scan_hashes_with_ttl() {
    if !cluster_available() {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let nodes = cluster_nodes();
    if nodes.is_empty() {
        return;
    }

    cleanup_cluster_keys("cluster:ttl:*");

    // Create hash with TTL
    cluster_redis_cli(&["HSET", "cluster:ttl:1", "name", "test"]);
    cluster_redis_cli(&["EXPIRE", "cluster:ttl:1", "3600"]);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)])
        .with_key(true)
        .with_ttl(true)
        .with_ttl_column_name("_ttl".to_string());

    let config = BatchConfig::new("cluster:ttl:*".to_string()).with_batch_size(100);

    let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
    let mut iterator = ClusterHashBatchIterator::new(&node_refs, schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected a batch");

    // Should have name, _key, _ttl columns
    assert!(batch.num_columns() >= 3);
    assert_eq!(batch.num_rows(), 1);

    cleanup_cluster_keys("cluster:ttl:*");
}

/// Test cluster hash scanning with row index.
#[test]
#[ignore] // Requires Redis Cluster
fn test_cluster_scan_hashes_with_row_index() {
    if !cluster_available() {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let nodes = cluster_nodes();
    if nodes.is_empty() {
        return;
    }

    cleanup_cluster_keys("cluster:idx:*");
    setup_cluster_hashes("cluster:idx:", 10);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)])
        .with_key(false)
        .with_row_index(true)
        .with_row_index_column_name("_index".to_string());

    let config = BatchConfig::new("cluster:idx:*".to_string()).with_batch_size(100);

    let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
    let mut iterator = ClusterHashBatchIterator::new(&node_refs, schema, config, None)
        .expect("Failed to create iterator");

    let batch = iterator
        .next_batch()
        .expect("Failed to get batch")
        .expect("Expected a batch");

    // Should have name and _index columns
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.num_rows(), 10);

    cleanup_cluster_keys("cluster:idx:*");
}

/// Test cluster hash scanning with no matching keys.
#[test]
#[ignore] // Requires Redis Cluster
fn test_cluster_scan_hashes_no_matches() {
    if !cluster_available() {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let nodes = cluster_nodes();
    if nodes.is_empty() {
        return;
    }

    let schema = HashSchema::new(vec![("field".to_string(), RedisType::Utf8)]).with_key(false);

    let config = BatchConfig::new("nonexistent:cluster:pattern:*".to_string()).with_batch_size(100);

    let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
    let mut iterator = ClusterHashBatchIterator::new(&node_refs, schema, config, None)
        .expect("Failed to create iterator");

    // Should return None immediately
    let batch = iterator.next_batch().expect("Failed to get batch");
    assert!(batch.is_none() || batch.unwrap().num_rows() == 0);
    assert!(iterator.is_done());
}

/// Test rows_yielded tracking in cluster mode.
#[test]
#[ignore] // Requires Redis Cluster
fn test_cluster_scan_hashes_rows_yielded() {
    if !cluster_available() {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let nodes = cluster_nodes();
    if nodes.is_empty() {
        return;
    }

    cleanup_cluster_keys("cluster:yielded:*");
    setup_cluster_hashes("cluster:yielded:", 20);

    let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(false);

    let config = BatchConfig::new("cluster:yielded:*".to_string()).with_batch_size(100);

    let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
    let mut iterator = ClusterHashBatchIterator::new(&node_refs, schema, config, None)
        .expect("Failed to create iterator");

    assert_eq!(iterator.rows_yielded(), 0);

    while iterator
        .next_batch()
        .expect("Failed to get batch")
        .is_some()
    {}

    assert_eq!(iterator.rows_yielded(), 20);

    cleanup_cluster_keys("cluster:yielded:*");
}

// =============================================================================
// String Cluster Tests
// =============================================================================

/// Test basic string scanning across a cluster.
#[test]
#[ignore] // Requires Redis Cluster
fn test_cluster_scan_strings_basic() {
    if !cluster_available() {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let nodes = cluster_nodes();
    if nodes.is_empty() {
        return;
    }

    cleanup_cluster_keys("cluster:string:*");
    setup_cluster_strings("cluster:string:", 25);

    let schema = StringSchema::new(DataType::Utf8).with_key(true);
    let config = BatchConfig::new("cluster:string:*".to_string())
        .with_batch_size(100)
        .with_count_hint(50);

    let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
    let mut iterator = ClusterStringBatchIterator::new(&node_refs, schema, config)
        .expect("Failed to create cluster iterator");

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
        // Should have key and value columns
        assert!(batch.num_columns() >= 2);
    }

    assert_eq!(total_rows, 25);
    assert!(iterator.is_done());

    cleanup_cluster_keys("cluster:string:*");
}

/// Test string scanning with max_rows in cluster mode.
#[test]
#[ignore] // Requires Redis Cluster
fn test_cluster_scan_strings_max_rows() {
    if !cluster_available() {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let nodes = cluster_nodes();
    if nodes.is_empty() {
        return;
    }

    cleanup_cluster_keys("cluster:strmaxrows:*");
    setup_cluster_strings("cluster:strmaxrows:", 40);

    let schema = StringSchema::new(DataType::Utf8).with_key(true);
    let config = BatchConfig::new("cluster:strmaxrows:*".to_string())
        .with_batch_size(100)
        .with_max_rows(15);

    let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
    let mut iterator = ClusterStringBatchIterator::new(&node_refs, schema, config)
        .expect("Failed to create iterator");

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 15);

    cleanup_cluster_keys("cluster:strmaxrows:*");
}

// =============================================================================
// JSON Cluster Tests
// =============================================================================

/// Set up test JSON documents in the cluster.
fn setup_cluster_json_docs(prefix: &str, count: usize) {
    let port_str = cluster_port_base().to_string();
    for i in 1..=count {
        let key = format!("{}{}", prefix, i);
        let json = format!(
            r#"{{"name": "User{}", "age": {}, "email": "user{}@example.com"}}"#,
            i,
            20 + i,
            i
        );

        let _ = Command::new("redis-cli")
            .args(["-p", &port_str, "-c", "JSON.SET", &key, "$", &json])
            .output();
    }
}

/// Test basic JSON scanning across a cluster.
#[test]
#[ignore] // Requires Redis Cluster with RedisJSON
fn test_cluster_scan_json_basic() {
    if !cluster_available() {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let nodes = cluster_nodes();
    if nodes.is_empty() {
        return;
    }

    cleanup_cluster_keys("cluster:json:*");
    setup_cluster_json_docs("cluster:json:", 20);

    let schema = JsonSchema::new(vec![
        ("name".to_string(), DataType::Utf8),
        ("age".to_string(), DataType::Int64),
        ("email".to_string(), DataType::Utf8),
    ])
    .with_key(true);

    let config = BatchConfig::new("cluster:json:*".to_string())
        .with_batch_size(100)
        .with_count_hint(50);

    let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
    let mut iterator = ClusterJsonBatchIterator::new(&node_refs, schema, config, None)
        .expect("Failed to create cluster iterator");

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
        assert!(batch.num_columns() >= 3);
    }

    assert_eq!(total_rows, 20);
    assert!(iterator.is_done());

    cleanup_cluster_keys("cluster:json:*");
}

/// Test JSON scanning with projection in cluster mode.
#[test]
#[ignore] // Requires Redis Cluster with RedisJSON
fn test_cluster_scan_json_projection() {
    if !cluster_available() {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let nodes = cluster_nodes();
    if nodes.is_empty() {
        return;
    }

    cleanup_cluster_keys("cluster:jsonproj:*");
    setup_cluster_json_docs("cluster:jsonproj:", 15);

    let schema = JsonSchema::new(vec![
        ("name".to_string(), DataType::Utf8),
        ("age".to_string(), DataType::Int64),
        ("email".to_string(), DataType::Utf8),
    ])
    .with_key(false);

    let config = BatchConfig::new("cluster:jsonproj:*".to_string()).with_batch_size(100);

    let projection = Some(vec!["name".to_string(), "age".to_string()]);
    let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
    let mut iterator = ClusterJsonBatchIterator::new(&node_refs, schema, config, projection)
        .expect("Failed to create iterator");

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
        assert_eq!(batch.num_columns(), 2);
    }

    assert_eq!(total_rows, 15);

    cleanup_cluster_keys("cluster:jsonproj:*");
}

// =============================================================================
// Set Cluster Tests
// =============================================================================

/// Set up test sets in the cluster.
fn setup_cluster_sets(prefix: &str, count: usize, members_per_set: usize) {
    let port_str = cluster_port_base().to_string();
    for i in 1..=count {
        let key = format!("{}{}", prefix, i);
        for j in 1..=members_per_set {
            let member = format!("member{}_{}", i, j);
            let _ = Command::new("redis-cli")
                .args(["-p", &port_str, "-c", "SADD", &key, &member])
                .output();
        }
    }
}

/// Test basic set scanning across a cluster.
#[test]
#[ignore] // Requires Redis Cluster
fn test_cluster_scan_sets_basic() {
    if !cluster_available() {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let nodes = cluster_nodes();
    if nodes.is_empty() {
        return;
    }

    cleanup_cluster_keys("cluster:set:*");
    setup_cluster_sets("cluster:set:", 10, 5); // 10 sets, 5 members each = 50 rows

    let schema = SetSchema::new().with_key(true);
    let config = BatchConfig::new("cluster:set:*".to_string())
        .with_batch_size(100)
        .with_count_hint(50);

    let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
    let mut iterator = ClusterSetBatchIterator::new(&node_refs, schema, config)
        .expect("Failed to create cluster iterator");

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 50);
    assert!(iterator.is_done());

    cleanup_cluster_keys("cluster:set:*");
}

/// Test set scanning with max_rows in cluster mode.
#[test]
#[ignore] // Requires Redis Cluster
fn test_cluster_scan_sets_max_rows() {
    if !cluster_available() {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let nodes = cluster_nodes();
    if nodes.is_empty() {
        return;
    }

    cleanup_cluster_keys("cluster:setmax:*");
    setup_cluster_sets("cluster:setmax:", 10, 5); // 50 total rows

    let schema = SetSchema::new().with_key(true);
    let config = BatchConfig::new("cluster:setmax:*".to_string())
        .with_batch_size(100)
        .with_max_rows(20);

    let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
    let mut iterator = ClusterSetBatchIterator::new(&node_refs, schema, config)
        .expect("Failed to create iterator");

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 20);

    cleanup_cluster_keys("cluster:setmax:*");
}

// =============================================================================
// List Cluster Tests
// =============================================================================

/// Set up test lists in the cluster.
fn setup_cluster_lists(prefix: &str, count: usize, elements_per_list: usize) {
    let port_str = cluster_port_base().to_string();
    for i in 1..=count {
        let key = format!("{}{}", prefix, i);
        for j in 1..=elements_per_list {
            let element = format!("element{}_{}", i, j);
            let _ = Command::new("redis-cli")
                .args(["-p", &port_str, "-c", "RPUSH", &key, &element])
                .output();
        }
    }
}

/// Test basic list scanning across a cluster.
#[test]
#[ignore] // Requires Redis Cluster
fn test_cluster_scan_lists_basic() {
    if !cluster_available() {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let nodes = cluster_nodes();
    if nodes.is_empty() {
        return;
    }

    cleanup_cluster_keys("cluster:list:*");
    setup_cluster_lists("cluster:list:", 8, 6); // 8 lists, 6 elements each = 48 rows

    let schema = ListSchema::new().with_key(true).with_position(true);
    let config = BatchConfig::new("cluster:list:*".to_string())
        .with_batch_size(100)
        .with_count_hint(50);

    let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
    let mut iterator = ClusterListBatchIterator::new(&node_refs, schema, config)
        .expect("Failed to create cluster iterator");

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 48);
    assert!(iterator.is_done());

    cleanup_cluster_keys("cluster:list:*");
}

/// Test list scanning with max_rows in cluster mode.
#[test]
#[ignore] // Requires Redis Cluster
fn test_cluster_scan_lists_max_rows() {
    if !cluster_available() {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let nodes = cluster_nodes();
    if nodes.is_empty() {
        return;
    }

    cleanup_cluster_keys("cluster:listmax:*");
    setup_cluster_lists("cluster:listmax:", 10, 5); // 50 total rows

    let schema = ListSchema::new().with_key(true);
    let config = BatchConfig::new("cluster:listmax:*".to_string())
        .with_batch_size(100)
        .with_max_rows(25);

    let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
    let mut iterator = ClusterListBatchIterator::new(&node_refs, schema, config)
        .expect("Failed to create iterator");

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 25);

    cleanup_cluster_keys("cluster:listmax:*");
}

// =============================================================================
// ZSet Cluster Tests
// =============================================================================

/// Set up test sorted sets in the cluster.
fn setup_cluster_zsets(prefix: &str, count: usize, members_per_zset: usize) {
    let port_str = cluster_port_base().to_string();
    for i in 1..=count {
        let key = format!("{}{}", prefix, i);
        for j in 1..=members_per_zset {
            let member = format!("player{}_{}", i, j);
            let score = ((i * 100) + j).to_string();
            let _ = Command::new("redis-cli")
                .args(["-p", &port_str, "-c", "ZADD", &key, &score, &member])
                .output();
        }
    }
}

/// Test basic sorted set scanning across a cluster.
#[test]
#[ignore] // Requires Redis Cluster
fn test_cluster_scan_zsets_basic() {
    if !cluster_available() {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let nodes = cluster_nodes();
    if nodes.is_empty() {
        return;
    }

    cleanup_cluster_keys("cluster:zset:*");
    setup_cluster_zsets("cluster:zset:", 10, 5); // 10 zsets, 5 members each = 50 rows

    let schema = ZSetSchema::new().with_key(true).with_rank(true);
    let config = BatchConfig::new("cluster:zset:*".to_string())
        .with_batch_size(100)
        .with_count_hint(50);

    let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
    let mut iterator = ClusterZSetBatchIterator::new(&node_refs, schema, config)
        .expect("Failed to create cluster iterator");

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
        // Should have key, member, score, rank columns
        assert!(batch.num_columns() >= 3);
    }

    assert_eq!(total_rows, 50);
    assert!(iterator.is_done());

    cleanup_cluster_keys("cluster:zset:*");
}

/// Test sorted set scanning with max_rows in cluster mode.
#[test]
#[ignore] // Requires Redis Cluster
fn test_cluster_scan_zsets_max_rows() {
    if !cluster_available() {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let nodes = cluster_nodes();
    if nodes.is_empty() {
        return;
    }

    cleanup_cluster_keys("cluster:zsetmax:*");
    setup_cluster_zsets("cluster:zsetmax:", 10, 5); // 50 total rows

    let schema = ZSetSchema::new().with_key(true);
    let config = BatchConfig::new("cluster:zsetmax:*".to_string())
        .with_batch_size(100)
        .with_max_rows(15);

    let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
    let mut iterator = ClusterZSetBatchIterator::new(&node_refs, schema, config)
        .expect("Failed to create iterator");

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 15);

    cleanup_cluster_keys("cluster:zsetmax:*");
}

/// Test sorted set scanning with custom column names.
#[test]
#[ignore] // Requires Redis Cluster
fn test_cluster_scan_zsets_custom_columns() {
    if !cluster_available() {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let nodes = cluster_nodes();
    if nodes.is_empty() {
        return;
    }

    cleanup_cluster_keys("cluster:zsetcol:*");
    setup_cluster_zsets("cluster:zsetcol:", 5, 4); // 20 total rows

    let schema = ZSetSchema::new()
        .with_key(true)
        .with_key_column_name("redis_key")
        .with_member_column_name("player_name")
        .with_score_column_name("points");

    let config = BatchConfig::new("cluster:zsetcol:*".to_string()).with_batch_size(100);

    let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
    let mut iterator = ClusterZSetBatchIterator::new(&node_refs, schema, config)
        .expect("Failed to create iterator");

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
        // Verify column count (key, member, score)
        assert_eq!(batch.num_columns(), 3);
    }

    assert_eq!(total_rows, 20);

    cleanup_cluster_keys("cluster:zsetcol:*");
}

// =============================================================================
// Data Consistency Tests
// =============================================================================

/// Test that scanning returns all keys distributed across cluster nodes.
/// This verifies keys hash to different slots and are all retrieved.
#[test]
#[ignore] // Requires Redis Cluster
fn test_cluster_data_consistency_all_keys_retrieved() {
    if !cluster_available() {
        eprintln!("Skipping test: Redis Cluster not available");
        return;
    }

    let nodes = cluster_nodes();
    if nodes.is_empty() {
        return;
    }

    cleanup_cluster_keys("cluster:consistency:*");

    // Create keys that will hash to different slots
    // Using varied key names to ensure distribution
    let port_str = cluster_port_base().to_string();
    let test_keys: Vec<String> = (1..=100)
        .map(|i| format!("cluster:consistency:{}", i))
        .collect();

    for key in &test_keys {
        let _ = Command::new("redis-cli")
            .args(["-p", &port_str, "-c", "HSET", key, "value", "test"])
            .output();
    }

    let schema = HashSchema::new(vec![("value".to_string(), RedisType::Utf8)]).with_key(true);
    let config = BatchConfig::new("cluster:consistency:*".to_string()).with_batch_size(200);

    let node_refs: Vec<&str> = nodes.iter().map(|s| s.as_str()).collect();
    let mut iterator = ClusterHashBatchIterator::new(&node_refs, schema, config, None)
        .expect("Failed to create iterator");

    let mut total_rows = 0;
    while let Some(batch) = iterator.next_batch().expect("Failed to get batch") {
        total_rows += batch.num_rows();
    }

    // All 100 keys should be retrieved
    assert_eq!(total_rows, 100);

    cleanup_cluster_keys("cluster:consistency:*");
}
