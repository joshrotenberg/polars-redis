//! Integration tests for Redis Cluster operations.
//!
//! These tests require Docker to start a Redis Cluster.
//! Tests use docker-wrapper's RedisClusterTemplate to manage the cluster.
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
    BatchConfig, ClusterHashBatchIterator, ClusterStringBatchIterator, HashSchema, RedisType,
    StringSchema,
};

/// Cluster configuration constants.
const CLUSTER_NAME: &str = "polars-redis-cluster-test";
const CLUSTER_PORT_BASE: u16 = 17000;
const NUM_MASTERS: usize = 3;

/// Global cluster state - ensures we only start one cluster per test run.
static CLUSTER_STARTED: OnceLock<RedisClusterConnection> = OnceLock::new();

/// Get cluster connection info, starting cluster if needed.
fn get_cluster_connection() -> Option<&'static RedisClusterConnection> {
    CLUSTER_STARTED.get()
}

/// Start Redis Cluster for testing (blocking).
///
/// Returns the cluster connection info if successful.
fn ensure_cluster_started() -> Option<&'static RedisClusterConnection> {
    if let Some(conn) = CLUSTER_STARTED.get() {
        return Some(conn);
    }

    // Create and start the cluster
    let template = RedisClusterTemplate::new(CLUSTER_NAME)
        .num_masters(NUM_MASTERS)
        .port_base(CLUSTER_PORT_BASE)
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
    let port_str = CLUSTER_PORT_BASE.to_string();
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
    let port_str = CLUSTER_PORT_BASE.to_string();

    // In cluster mode, we need to scan each node
    for port_offset in 0..NUM_MASTERS {
        let port = (CLUSTER_PORT_BASE + port_offset as u16).to_string();

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
    let port_str = CLUSTER_PORT_BASE.to_string();
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
    let port_str = CLUSTER_PORT_BASE.to_string();
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
    let port_str = CLUSTER_PORT_BASE.to_string();
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
