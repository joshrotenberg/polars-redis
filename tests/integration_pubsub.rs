//! Integration tests for Redis Pub/Sub operations.
//!
//! These tests require a running Redis instance.
//! Run with: `cargo test --test integration_pubsub --all-features`

use std::thread;
use std::time::Duration;

use polars_redis::pubsub::{PubSubConfig, collect_pubsub};

mod common;
use common::{redis_available, redis_cli, redis_url};

/// Test PubSubConfig builder.
#[test]
fn test_pubsub_config_builder() {
    let config = PubSubConfig::new(vec!["events".to_string()])
        .with_count(100)
        .with_timeout_ms(5000)
        .with_channel_column()
        .with_timestamp_column()
        .with_pattern();

    assert_eq!(config.channels, vec!["events".to_string()]);
    assert_eq!(config.count, Some(100));
    assert_eq!(config.timeout_ms, Some(5000));
    assert!(config.include_channel);
    assert!(config.include_timestamp);
    assert!(config.pattern_subscribe);
}

/// Test build_schema with all columns.
#[test]
fn test_build_schema_full() {
    let config = PubSubConfig::new(vec!["test".to_string()])
        .with_channel_column()
        .with_timestamp_column();

    let schema = config.build_schema();
    assert_eq!(schema.fields().len(), 3);
    assert_eq!(schema.field(0).name(), "_channel");
    assert_eq!(schema.field(1).name(), "_received_at");
    assert_eq!(schema.field(2).name(), "message");
}

/// Test build_schema minimal (message only).
#[test]
fn test_build_schema_minimal() {
    let config = PubSubConfig::new(vec!["test".to_string()]);

    let schema = config.build_schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "message");
}

/// Test custom column names.
#[test]
fn test_custom_column_names() {
    let config = PubSubConfig::new(vec!["test".to_string()])
        .with_channel_column()
        .with_timestamp_column()
        .with_column_names("chan", "msg", "ts");

    let schema = config.build_schema();
    assert_eq!(schema.field(0).name(), "chan");
    assert_eq!(schema.field(1).name(), "ts");
    assert_eq!(schema.field(2).name(), "msg");
}

/// Test collect_pubsub with timeout (no messages).
#[test]
#[ignore] // Requires Redis
fn test_collect_pubsub_timeout_empty() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let config =
        PubSubConfig::new(vec!["rust:pubsub:empty:channel".to_string()]).with_timeout_ms(100); // Short timeout

    let batch = collect_pubsub(&redis_url(), &config).unwrap();
    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.num_columns(), 1); // Just message column
}

/// Test collect_pubsub receives messages.
#[test]
#[ignore] // Requires Redis
fn test_collect_pubsub_with_messages() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let channel = "rust:pubsub:test:messages";

    // Spawn a thread to publish messages after a short delay
    let channel_clone = channel.to_string();
    let publisher = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        for i in 1..=5 {
            redis_cli(&["PUBLISH", &channel_clone, &format!("message_{}", i)]);
            thread::sleep(Duration::from_millis(50));
        }
    });

    let config = PubSubConfig::new(vec![channel.to_string()])
        .with_count(5)
        .with_timeout_ms(2000)
        .with_channel_column()
        .with_timestamp_column();

    let batch = collect_pubsub(&redis_url(), &config).unwrap();

    publisher.join().unwrap();

    assert_eq!(batch.num_rows(), 5);
    assert_eq!(batch.num_columns(), 3); // channel, timestamp, message
}

/// Test collect_pubsub with pattern subscription.
#[test]
#[ignore] // Requires Redis
fn test_collect_pubsub_pattern() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    // Spawn a thread to publish messages to different channels
    let publisher = thread::spawn(|| {
        thread::sleep(Duration::from_millis(100));
        redis_cli(&["PUBLISH", "rust:pubsub:pattern:a", "msg_a"]);
        thread::sleep(Duration::from_millis(50));
        redis_cli(&["PUBLISH", "rust:pubsub:pattern:b", "msg_b"]);
        thread::sleep(Duration::from_millis(50));
        redis_cli(&["PUBLISH", "rust:pubsub:pattern:c", "msg_c"]);
    });

    let config = PubSubConfig::new(vec!["rust:pubsub:pattern:*".to_string()])
        .with_pattern()
        .with_count(3)
        .with_timeout_ms(2000)
        .with_channel_column();

    let batch = collect_pubsub(&redis_url(), &config).unwrap();

    publisher.join().unwrap();

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 2); // channel, message
}

/// Test collect_pubsub with window_seconds.
#[test]
#[ignore] // Requires Redis
fn test_collect_pubsub_window() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let channel = "rust:pubsub:window:channel";

    // Publish some messages
    let channel_clone = channel.to_string();
    let publisher = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        for i in 1..=3 {
            redis_cli(&["PUBLISH", &channel_clone, &format!("msg_{}", i)]);
            thread::sleep(Duration::from_millis(100));
        }
    });

    let config = PubSubConfig::new(vec![channel.to_string()])
        .with_window_seconds(0.5) // 500ms window
        .with_timeout_ms(1000);

    let batch = collect_pubsub(&redis_url(), &config).unwrap();

    publisher.join().unwrap();

    // Should have collected some messages within the window
    assert!(batch.num_rows() <= 3);
}

/// Test multiple channels subscription.
#[test]
#[ignore] // Requires Redis
fn test_collect_pubsub_multiple_channels() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let channels = vec![
        "rust:pubsub:multi:ch1".to_string(),
        "rust:pubsub:multi:ch2".to_string(),
    ];

    let publisher = thread::spawn(|| {
        thread::sleep(Duration::from_millis(100));
        redis_cli(&["PUBLISH", "rust:pubsub:multi:ch1", "from_ch1"]);
        thread::sleep(Duration::from_millis(50));
        redis_cli(&["PUBLISH", "rust:pubsub:multi:ch2", "from_ch2"]);
    });

    let config = PubSubConfig::new(channels)
        .with_count(2)
        .with_timeout_ms(2000)
        .with_channel_column();

    let batch = collect_pubsub(&redis_url(), &config).unwrap();

    publisher.join().unwrap();

    assert_eq!(batch.num_rows(), 2);
}

/// Test that count limit works.
#[test]
#[ignore] // Requires Redis
fn test_collect_pubsub_count_limit() {
    if !redis_available() {
        eprintln!("Skipping test: Redis not available");
        return;
    }

    let channel = "rust:pubsub:limit:channel";

    // Publish more messages than the count limit
    let channel_clone = channel.to_string();
    let publisher = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        for i in 1..=10 {
            redis_cli(&["PUBLISH", &channel_clone, &format!("msg_{}", i)]);
            thread::sleep(Duration::from_millis(20));
        }
    });

    let config = PubSubConfig::new(vec![channel.to_string()])
        .with_count(3) // Only collect 3
        .with_timeout_ms(2000);

    let batch = collect_pubsub(&redis_url(), &config).unwrap();

    publisher.join().unwrap();

    // Should have exactly 3 messages
    assert_eq!(batch.num_rows(), 3);
}
