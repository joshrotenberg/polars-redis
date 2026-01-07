//! Integration tests for Pipeline and Transaction functionality.
//!
//! These tests require a running Redis instance at localhost:6379.

use polars_redis::pipeline::{CommandResult, Pipeline, Transaction};

/// Test that pipeline creation works.
#[test]
#[ignore] // Requires Redis
fn test_pipeline_basic_operations() {
    let mut pipe = Pipeline::new("redis://localhost:6379").unwrap();

    // Queue some operations
    pipe.set("pipe_test:key1", "value1");
    pipe.set("pipe_test:key2", "value2");
    pipe.get("pipe_test:key1");
    pipe.get("pipe_test:key2");

    assert_eq!(pipe.len(), 4);

    let result = pipe.execute().unwrap();

    assert_eq!(result.results.len(), 4);
    assert!(result.all_succeeded());

    // First two are SET commands (return OK)
    assert!(result.get(0).unwrap().is_ok() || matches!(result.get(0), Some(CommandResult::Int(_))));

    // Cleanup
    let mut cleanup = Pipeline::new("redis://localhost:6379").unwrap();
    cleanup.del(&["pipe_test:key1", "pipe_test:key2"]);
    let _ = cleanup.execute();
}

/// Test pipeline with hash operations.
#[test]
#[ignore] // Requires Redis
fn test_pipeline_hash_operations() {
    let mut pipe = Pipeline::new("redis://localhost:6379").unwrap();

    // Set up a hash
    pipe.hmset(
        "pipe_test:hash1",
        &[("field1", "value1"), ("field2", "value2")],
    );
    pipe.hget("pipe_test:hash1", "field1");
    pipe.hgetall("pipe_test:hash1");
    pipe.hincrby("pipe_test:hash1", "counter", 10);

    let result = pipe.execute().unwrap();

    assert_eq!(result.results.len(), 4);
    assert!(result.all_succeeded());

    // Cleanup
    let mut cleanup = Pipeline::new("redis://localhost:6379").unwrap();
    cleanup.del(&["pipe_test:hash1"]);
    let _ = cleanup.execute();
}

/// Test pipeline with list operations.
#[test]
#[ignore] // Requires Redis
fn test_pipeline_list_operations() {
    let mut pipe = Pipeline::new("redis://localhost:6379").unwrap();

    // Set up a list
    pipe.rpush("pipe_test:list1", &["a", "b", "c"]);
    pipe.llen("pipe_test:list1");
    pipe.lrange("pipe_test:list1", 0, -1);

    let result = pipe.execute().unwrap();

    assert_eq!(result.results.len(), 3);
    assert!(result.all_succeeded());

    // Check list length
    if let Some(CommandResult::Int(len)) = result.get(1) {
        assert_eq!(*len, 3);
    }

    // Cleanup
    let mut cleanup = Pipeline::new("redis://localhost:6379").unwrap();
    cleanup.del(&["pipe_test:list1"]);
    let _ = cleanup.execute();
}

/// Test pipeline with set operations.
#[test]
#[ignore] // Requires Redis
fn test_pipeline_set_operations() {
    let mut pipe = Pipeline::new("redis://localhost:6379").unwrap();

    // Set up a set
    pipe.sadd("pipe_test:set1", &["member1", "member2", "member3"]);
    pipe.scard("pipe_test:set1");
    pipe.sismember("pipe_test:set1", "member1");
    pipe.smembers("pipe_test:set1");

    let result = pipe.execute().unwrap();

    assert_eq!(result.results.len(), 4);
    assert!(result.all_succeeded());

    // Check cardinality
    if let Some(CommandResult::Int(card)) = result.get(1) {
        assert_eq!(*card, 3);
    }

    // Check membership
    if let Some(CommandResult::Int(is_member)) = result.get(2) {
        assert_eq!(*is_member, 1);
    }

    // Cleanup
    let mut cleanup = Pipeline::new("redis://localhost:6379").unwrap();
    cleanup.del(&["pipe_test:set1"]);
    let _ = cleanup.execute();
}

/// Test pipeline with sorted set operations.
#[test]
#[ignore] // Requires Redis
fn test_pipeline_zset_operations() {
    let mut pipe = Pipeline::new("redis://localhost:6379").unwrap();

    // Set up a sorted set
    pipe.zadd("pipe_test:zset1", &[(1.0, "a"), (2.0, "b"), (3.0, "c")]);
    pipe.zcard("pipe_test:zset1");
    pipe.zscore("pipe_test:zset1", "b");
    pipe.zrange("pipe_test:zset1", 0, -1);

    let result = pipe.execute().unwrap();

    assert_eq!(result.results.len(), 4);
    assert!(result.all_succeeded());

    // Check cardinality
    if let Some(CommandResult::Int(card)) = result.get(1) {
        assert_eq!(*card, 3);
    }

    // Cleanup
    let mut cleanup = Pipeline::new("redis://localhost:6379").unwrap();
    cleanup.del(&["pipe_test:zset1"]);
    let _ = cleanup.execute();
}

/// Test transaction basic operations.
#[test]
#[ignore] // Requires Redis
fn test_transaction_basic() {
    let mut tx = Transaction::new("redis://localhost:6379").unwrap();

    // Queue atomic operations
    tx.set("tx_test:key1", "value1");
    tx.set("tx_test:key2", "value2");
    tx.incr("tx_test:counter");
    tx.incr("tx_test:counter");

    assert_eq!(tx.len(), 4);

    let result = tx.execute().unwrap();

    assert_eq!(result.results.len(), 4);
    assert!(result.all_succeeded());

    // Cleanup
    let mut cleanup = Pipeline::new("redis://localhost:6379").unwrap();
    cleanup.del(&["tx_test:key1", "tx_test:key2", "tx_test:counter"]);
    let _ = cleanup.execute();
}

/// Test transaction with hash operations.
#[test]
#[ignore] // Requires Redis
fn test_transaction_hash_operations() {
    let mut tx = Transaction::new("redis://localhost:6379").unwrap();

    // Atomic hash operations
    tx.hset("tx_test:hash1", "field1", "value1");
    tx.hset("tx_test:hash1", "field2", "value2");
    tx.hincrby("tx_test:hash1", "counter", 5);

    let result = tx.execute().unwrap();

    assert_eq!(result.results.len(), 3);
    assert!(result.all_succeeded());

    // Cleanup
    let mut cleanup = Pipeline::new("redis://localhost:6379").unwrap();
    cleanup.del(&["tx_test:hash1"]);
    let _ = cleanup.execute();
}

/// Test pipeline clear and reuse.
#[test]
fn test_pipeline_clear() {
    let mut pipe = Pipeline::new("redis://localhost:6379").unwrap();

    pipe.set("key1", "value1");
    pipe.set("key2", "value2");
    assert_eq!(pipe.len(), 2);

    pipe.clear();
    assert!(pipe.is_empty());
    assert_eq!(pipe.len(), 0);
}

/// Test transaction discard.
#[test]
fn test_transaction_discard() {
    let mut tx = Transaction::new("redis://localhost:6379").unwrap();

    tx.set("key1", "value1");
    tx.set("key2", "value2");
    assert_eq!(tx.len(), 2);

    tx.discard();
    assert!(tx.is_empty());
    assert_eq!(tx.len(), 0);
}

/// Test raw command support.
#[test]
#[ignore] // Requires Redis
fn test_pipeline_raw_command() {
    let mut pipe = Pipeline::new("redis://localhost:6379").unwrap();

    pipe.raw("SET", &["raw_test:key", "raw_value"]);
    pipe.raw("GET", &["raw_test:key"]);

    let result = pipe.execute().unwrap();

    assert_eq!(result.results.len(), 2);
    assert!(result.all_succeeded());

    // Cleanup
    let mut cleanup = Pipeline::new("redis://localhost:6379").unwrap();
    cleanup.del(&["raw_test:key"]);
    let _ = cleanup.execute();
}

/// Test empty pipeline execution.
#[test]
#[ignore] // Requires Redis
fn test_empty_pipeline() {
    let mut pipe = Pipeline::new("redis://localhost:6379").unwrap();
    assert!(pipe.is_empty());

    let result = pipe.execute().unwrap();

    assert_eq!(result.results.len(), 0);
    assert!(result.all_succeeded());
}

/// Test key management commands.
#[test]
#[ignore] // Requires Redis
fn test_pipeline_key_management() {
    let mut pipe = Pipeline::new("redis://localhost:6379").unwrap();

    // Set up keys
    pipe.set("key_mgmt:key1", "value1");
    pipe.set("key_mgmt:key2", "value2");
    pipe.exists(&["key_mgmt:key1", "key_mgmt:key2"]);
    pipe.ttl("key_mgmt:key1");
    pipe.expire("key_mgmt:key1", 3600);
    pipe.key_type("key_mgmt:key1");

    let result = pipe.execute().unwrap();

    assert!(result.all_succeeded());

    // Cleanup
    let mut cleanup = Pipeline::new("redis://localhost:6379").unwrap();
    cleanup.del(&["key_mgmt:key1", "key_mgmt:key2"]);
    let _ = cleanup.execute();
}
