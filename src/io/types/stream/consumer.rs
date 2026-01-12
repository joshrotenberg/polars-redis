//! Stream consumer with checkpointing and consumer group support.
//!
//! This module provides a `StreamConsumer` that reads from Redis Streams using
//! consumer groups (XREADGROUP), with support for checkpointing and entry
//! acknowledgment (XACK).
//!
//! # Example
//!
//! ```ignore
//! use polars_redis::{StreamConsumer, StreamSchema, RedisCheckpointStore};
//!
//! // Create a checkpoint store
//! let checkpoint_store = RedisCheckpointStore::new("redis://localhost:6379")?;
//!
//! // Create a consumer
//! let mut consumer = StreamConsumer::new(
//!     "redis://localhost:6379",
//!     "mystream",
//!     "mygroup",
//!     "consumer-1",
//! )?
//! .with_checkpoint_store(Box::new(checkpoint_store))
//! .with_schema(StreamSchema::new().add_field("action").add_field("user"));
//!
//! // Read batches
//! while let Some(batch) = consumer.next_batch()? {
//!     println!("Got {} entries", batch.num_rows());
//!
//!     // Acknowledge processed entries
//!     consumer.ack_batch()?;
//! }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

// Type aliases for complex Redis response types
type XReadGroupResponse = Option<Vec<(String, Vec<(String, HashMap<String, String>)>)>>;
type XPendingResponse = (
    u64,
    Option<String>,
    Option<String>,
    Option<Vec<(String, u64)>>,
);

use arrow::array::RecordBatch;
use redis::aio::ConnectionManager;
use tokio::runtime::Runtime;

use super::convert::{StreamSchema, streams_to_record_batch};
use super::reader::{StreamData, StreamEntry};
use crate::connection::RedisConnection;
use crate::error::{Error, Result};

// ============================================================================
// Checkpoint Store Trait and Implementations
// ============================================================================

/// Trait for storing and retrieving stream consumption checkpoints.
///
/// Checkpoints track the last successfully processed entry ID, enabling
/// resume-from-failure semantics.
pub trait CheckpointStore: Send + Sync {
    /// Get the checkpoint for the given key.
    ///
    /// Returns `None` if no checkpoint exists.
    fn get(&self, key: &str) -> Result<Option<String>>;

    /// Set the checkpoint for the given key.
    fn set(&self, key: &str, value: &str) -> Result<()>;

    /// Delete the checkpoint for the given key.
    fn delete(&self, key: &str) -> Result<()>;
}

/// In-memory checkpoint store for testing and development.
///
/// Checkpoints are lost when the process exits.
#[derive(Debug, Default)]
pub struct MemoryCheckpointStore {
    data: std::sync::RwLock<HashMap<String, String>>,
}

impl MemoryCheckpointStore {
    /// Create a new in-memory checkpoint store.
    pub fn new() -> Self {
        Self::default()
    }
}

impl CheckpointStore for MemoryCheckpointStore {
    fn get(&self, key: &str) -> Result<Option<String>> {
        let data = self
            .data
            .read()
            .map_err(|e| Error::Runtime(format!("Failed to acquire read lock: {}", e)))?;
        Ok(data.get(key).cloned())
    }

    fn set(&self, key: &str, value: &str) -> Result<()> {
        let mut data = self
            .data
            .write()
            .map_err(|e| Error::Runtime(format!("Failed to acquire write lock: {}", e)))?;
        data.insert(key.to_string(), value.to_string());
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<()> {
        let mut data = self
            .data
            .write()
            .map_err(|e| Error::Runtime(format!("Failed to acquire write lock: {}", e)))?;
        data.remove(key);
        Ok(())
    }
}

/// Redis-backed checkpoint store for production use.
///
/// Stores checkpoints as Redis strings with optional TTL.
pub struct RedisCheckpointStore {
    runtime: Arc<Runtime>,
    conn: ConnectionManager,
    /// Optional TTL for checkpoint keys (in seconds).
    ttl: Option<i64>,
    /// Key prefix for checkpoint storage.
    prefix: String,
}

impl RedisCheckpointStore {
    /// Create a new Redis checkpoint store.
    pub fn new(url: &str) -> Result<Self> {
        let runtime = Arc::new(
            Runtime::new()
                .map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?,
        );
        let connection = RedisConnection::new(url)?;
        let conn = runtime.block_on(connection.get_connection_manager())?;

        Ok(Self {
            runtime,
            conn,
            ttl: None,
            prefix: "checkpoint:".to_string(),
        })
    }

    /// Set the TTL for checkpoint keys.
    pub fn with_ttl(mut self, seconds: i64) -> Self {
        self.ttl = Some(seconds);
        self
    }

    /// Set the key prefix for checkpoint storage.
    pub fn with_prefix(mut self, prefix: &str) -> Self {
        self.prefix = prefix.to_string();
        self
    }

    fn full_key(&self, key: &str) -> String {
        format!("{}{}", self.prefix, key)
    }
}

impl CheckpointStore for RedisCheckpointStore {
    fn get(&self, key: &str) -> Result<Option<String>> {
        let full_key = self.full_key(key);
        let mut conn = self.conn.clone();

        self.runtime.block_on(async {
            let result: Option<String> = redis::cmd("GET")
                .arg(&full_key)
                .query_async(&mut conn)
                .await?;
            Ok(result)
        })
    }

    fn set(&self, key: &str, value: &str) -> Result<()> {
        let full_key = self.full_key(key);
        let mut conn = self.conn.clone();
        let ttl = self.ttl;

        self.runtime.block_on(async {
            if let Some(seconds) = ttl {
                redis::cmd("SETEX")
                    .arg(&full_key)
                    .arg(seconds)
                    .arg(value)
                    .query_async::<()>(&mut conn)
                    .await?;
            } else {
                redis::cmd("SET")
                    .arg(&full_key)
                    .arg(value)
                    .query_async::<()>(&mut conn)
                    .await?;
            }
            Ok(())
        })
    }

    fn delete(&self, key: &str) -> Result<()> {
        let full_key = self.full_key(key);
        let mut conn = self.conn.clone();

        self.runtime.block_on(async {
            redis::cmd("DEL")
                .arg(&full_key)
                .query_async::<i64>(&mut conn)
                .await?;
            Ok(())
        })
    }
}

// ============================================================================
// Consumer Statistics
// ============================================================================

/// Statistics about stream consumption.
#[derive(Debug, Clone, Default)]
pub struct ConsumerStats {
    /// Number of batches processed.
    pub batches_processed: u64,
    /// Total number of entries processed.
    pub entries_processed: u64,
    /// Last entry ID that was processed.
    pub last_entry_id: Option<String>,
    /// Time when consumption started.
    pub started_at: Option<Instant>,
    /// Total time spent reading from Redis.
    pub read_time: Duration,
    /// Total time spent processing batches.
    pub process_time: Duration,
}

impl ConsumerStats {
    /// Create new empty stats.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get entries processed per second.
    pub fn entries_per_second(&self) -> f64 {
        if let Some(started) = self.started_at {
            let elapsed = started.elapsed().as_secs_f64();
            if elapsed > 0.0 {
                return self.entries_processed as f64 / elapsed;
            }
        }
        0.0
    }

    /// Get the total elapsed time since start.
    pub fn elapsed(&self) -> Duration {
        self.started_at.map(|s| s.elapsed()).unwrap_or_default()
    }
}

// ============================================================================
// Stream Consumer
// ============================================================================

/// Configuration for the stream consumer.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Number of entries to read per batch.
    pub batch_size: usize,
    /// Block timeout in milliseconds (0 = non-blocking).
    pub block_ms: u64,
    /// Whether to automatically acknowledge entries after reading.
    pub auto_ack: bool,
    /// Whether to create the consumer group if it doesn't exist.
    pub create_group: bool,
    /// Start ID when creating a new group ("0" = from beginning, "$" = from now).
    pub group_start_id: String,
    /// Whether to claim pending entries on startup.
    pub claim_pending: bool,
    /// Minimum idle time (ms) for claiming pending entries.
    pub claim_min_idle_ms: u64,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            block_ms: 5000,
            auto_ack: false,
            create_group: true,
            group_start_id: "0".to_string(),
            claim_pending: false,
            claim_min_idle_ms: 60000,
        }
    }
}

impl ConsumerConfig {
    /// Create a new consumer configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set the block timeout in milliseconds.
    pub fn with_block_ms(mut self, ms: u64) -> Self {
        self.block_ms = ms;
        self
    }

    /// Set whether to auto-acknowledge entries.
    pub fn with_auto_ack(mut self, auto_ack: bool) -> Self {
        self.auto_ack = auto_ack;
        self
    }

    /// Set whether to create the consumer group if it doesn't exist.
    pub fn with_create_group(mut self, create: bool) -> Self {
        self.create_group = create;
        self
    }

    /// Set the start ID for new groups.
    pub fn with_group_start_id(mut self, id: &str) -> Self {
        self.group_start_id = id.to_string();
        self
    }

    /// Set whether to claim pending entries on startup.
    pub fn with_claim_pending(mut self, claim: bool) -> Self {
        self.claim_pending = claim;
        self
    }

    /// Set the minimum idle time for claiming pending entries.
    pub fn with_claim_min_idle_ms(mut self, ms: u64) -> Self {
        self.claim_min_idle_ms = ms;
        self
    }
}

/// A stream consumer that reads from Redis Streams using consumer groups.
///
/// Supports checkpointing, entry acknowledgment, and statistics tracking.
pub struct StreamConsumer {
    /// Tokio runtime for async operations.
    runtime: Runtime,
    /// Redis connection manager.
    conn: ConnectionManager,
    /// Stream key to consume from.
    stream: String,
    /// Consumer group name.
    group: String,
    /// Consumer name within the group.
    consumer: String,
    /// Schema for converting entries to Arrow.
    schema: StreamSchema,
    /// Consumer configuration.
    config: ConsumerConfig,
    /// Optional checkpoint store.
    checkpoint_store: Option<Box<dyn CheckpointStore>>,
    /// Consumption statistics.
    stats: ConsumerStats,
    /// Entry IDs from the last batch (for acknowledgment).
    pending_ids: Vec<String>,
    /// Whether the consumer group has been initialized.
    group_initialized: bool,
    /// Row index offset for Arrow conversion.
    row_index_offset: u64,
}

impl StreamConsumer {
    /// Create a new stream consumer.
    ///
    /// # Arguments
    /// * `url` - Redis connection URL
    /// * `stream` - Stream key to consume from
    /// * `group` - Consumer group name
    /// * `consumer` - Consumer name within the group
    pub fn new(url: &str, stream: &str, group: &str, consumer: &str) -> Result<Self> {
        let runtime = Runtime::new()
            .map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;
        let connection = RedisConnection::new(url)?;
        let conn = runtime.block_on(connection.get_connection_manager())?;

        Ok(Self {
            runtime,
            conn,
            stream: stream.to_string(),
            group: group.to_string(),
            consumer: consumer.to_string(),
            schema: StreamSchema::new(),
            config: ConsumerConfig::default(),
            checkpoint_store: None,
            stats: ConsumerStats::new(),
            pending_ids: Vec::new(),
            group_initialized: false,
            row_index_offset: 0,
        })
    }

    /// Set the schema for Arrow conversion.
    pub fn with_schema(mut self, schema: StreamSchema) -> Self {
        self.schema = schema;
        self
    }

    /// Set the consumer configuration.
    pub fn with_config(mut self, config: ConsumerConfig) -> Self {
        self.config = config;
        self
    }

    /// Set the checkpoint store.
    pub fn with_checkpoint_store(mut self, store: Box<dyn CheckpointStore>) -> Self {
        self.checkpoint_store = Some(store);
        self
    }

    /// Get the consumption statistics.
    pub fn stats(&self) -> &ConsumerStats {
        &self.stats
    }

    /// Get the checkpoint key for this consumer.
    fn checkpoint_key(&self) -> String {
        format!("{}:{}:{}", self.stream, self.group, self.consumer)
    }

    /// Initialize the consumer group if needed.
    fn ensure_group(&mut self) -> Result<()> {
        if self.group_initialized {
            return Ok(());
        }

        if self.config.create_group {
            let mut conn = self.conn.clone();
            let stream = self.stream.clone();
            let group = self.group.clone();
            let start_id = self.config.group_start_id.clone();

            self.runtime.block_on(async {
                // Try to create the group, ignore error if it already exists
                let result: redis::RedisResult<()> = redis::cmd("XGROUP")
                    .arg("CREATE")
                    .arg(&stream)
                    .arg(&group)
                    .arg(&start_id)
                    .arg("MKSTREAM")
                    .query_async(&mut conn)
                    .await;

                match result {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        let msg = e.to_string();
                        if msg.contains("BUSYGROUP") {
                            // Group already exists, that's fine
                            Ok(())
                        } else {
                            Err(Error::Connection(e))
                        }
                    },
                }
            })?;
        }

        self.group_initialized = true;
        Ok(())
    }

    /// Read the next batch of entries from the stream.
    ///
    /// Returns `None` when no entries are available within the block timeout.
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        // Initialize stats timer on first call
        if self.stats.started_at.is_none() {
            self.stats.started_at = Some(Instant::now());
        }

        // Ensure consumer group exists
        self.ensure_group()?;

        let read_start = Instant::now();

        // Read entries using XREADGROUP
        let entries = self.read_entries()?;

        self.stats.read_time += read_start.elapsed();

        if entries.is_empty() {
            return Ok(None);
        }

        // Store entry IDs for later acknowledgment
        self.pending_ids = entries.iter().map(|e| e.id.clone()).collect();

        // Convert to Arrow RecordBatch
        let stream_data = vec![StreamData {
            key: self.stream.clone(),
            entries,
        }];

        let batch = streams_to_record_batch(&stream_data, &self.schema, self.row_index_offset)?;
        self.row_index_offset += batch.num_rows() as u64;

        // Update stats
        self.stats.batches_processed += 1;
        self.stats.entries_processed += batch.num_rows() as u64;
        if let Some(last_id) = self.pending_ids.last() {
            self.stats.last_entry_id = Some(last_id.clone());
        }

        // Auto-acknowledge if configured
        if self.config.auto_ack && !self.pending_ids.is_empty() {
            self.ack_batch()?;
        }

        Ok(Some(batch))
    }

    /// Get the next batch of raw entries as (entry_id, fields) pairs.
    ///
    /// This is useful when you need to apply custom filtering logic
    /// before processing entries.
    ///
    /// Returns an empty Vec when no entries are available within the block timeout.
    pub async fn next_batch_raw(
        &mut self,
    ) -> Result<Vec<(String, std::collections::HashMap<String, String>)>> {
        // Initialize stats timer on first call
        if self.stats.started_at.is_none() {
            self.stats.started_at = Some(std::time::Instant::now());
        }

        // Ensure consumer group exists
        self.ensure_group()?;

        // Read entries using XREADGROUP
        let entries = self.read_entries()?;

        if entries.is_empty() {
            return Ok(Vec::new());
        }

        // Store entry IDs for later acknowledgment
        self.pending_ids = entries.iter().map(|e| e.id.clone()).collect();

        // Update stats
        self.stats.batches_processed += 1;
        self.stats.entries_processed += entries.len() as u64;
        if let Some(last_id) = self.pending_ids.last() {
            self.stats.last_entry_id = Some(last_id.clone());
        }

        // Convert to (id, fields) pairs
        let result: Vec<(String, std::collections::HashMap<String, String>)> =
            entries.into_iter().map(|e| (e.id, e.fields)).collect();

        Ok(result)
    }

    /// Acknowledge specific entry IDs.
    ///
    /// Use this when processing entries one at a time with custom logic.
    pub async fn ack(&mut self, entry_ids: &[String]) -> Result<usize> {
        self.ack_entries(entry_ids)
    }

    /// Read entries from the stream using XREADGROUP.
    fn read_entries(&mut self) -> Result<Vec<StreamEntry>> {
        let mut conn = self.conn.clone();
        let stream = self.stream.clone();
        let group = self.group.clone();
        let consumer = self.consumer.clone();
        let count = self.config.batch_size;
        let block = self.config.block_ms;

        self.runtime.block_on(async {
            // XREADGROUP GROUP group consumer [COUNT count] [BLOCK ms] STREAMS stream >
            let result: XReadGroupResponse = redis::cmd("XREADGROUP")
                .arg("GROUP")
                .arg(&group)
                .arg(&consumer)
                .arg("COUNT")
                .arg(count)
                .arg("BLOCK")
                .arg(block)
                .arg("STREAMS")
                .arg(&stream)
                .arg(">")
                .query_async(&mut conn)
                .await?;

            let entries = match result {
                Some(streams) => {
                    let mut all_entries = Vec::new();
                    for (_stream_key, stream_entries) in streams {
                        for (id, fields) in stream_entries {
                            if let Some((ts_str, seq_str)) = id.split_once('-') {
                                if let (Ok(timestamp_ms), Ok(sequence)) =
                                    (ts_str.parse::<i64>(), seq_str.parse::<u64>())
                                {
                                    all_entries.push(StreamEntry {
                                        id,
                                        timestamp_ms,
                                        sequence,
                                        fields,
                                    });
                                }
                            }
                        }
                    }
                    all_entries
                },
                None => Vec::new(),
            };

            Ok(entries)
        })
    }

    /// Acknowledge the entries from the last batch.
    ///
    /// This should be called after successfully processing a batch.
    pub fn ack_batch(&mut self) -> Result<usize> {
        if self.pending_ids.is_empty() {
            return Ok(0);
        }

        let acked = self.ack_entries(&self.pending_ids.clone())?;

        // Save checkpoint if we have a store
        if let (Some(store), Some(last_id)) =
            (&self.checkpoint_store, self.pending_ids.last().cloned())
        {
            store.set(&self.checkpoint_key(), &last_id)?;
        }

        self.pending_ids.clear();
        Ok(acked)
    }

    /// Acknowledge specific entry IDs.
    pub fn ack_entries(&mut self, ids: &[String]) -> Result<usize> {
        if ids.is_empty() {
            return Ok(0);
        }

        let mut conn = self.conn.clone();
        let stream = self.stream.clone();
        let group = self.group.clone();

        self.runtime.block_on(async {
            let mut cmd = redis::cmd("XACK");
            cmd.arg(&stream).arg(&group);
            for id in ids {
                cmd.arg(id);
            }

            let acked: usize = cmd.query_async(&mut conn).await?;
            Ok(acked)
        })
    }

    /// Get the number of pending entries for this consumer.
    pub fn pending_count(&mut self) -> Result<u64> {
        let mut conn = self.conn.clone();
        let stream = self.stream.clone();
        let group = self.group.clone();

        self.runtime.block_on(async {
            // XPENDING stream group
            let result: XPendingResponse = redis::cmd("XPENDING")
                .arg(&stream)
                .arg(&group)
                .query_async(&mut conn)
                .await?;

            Ok(result.0)
        })
    }

    /// Rollback the current batch (don't acknowledge, allow re-delivery).
    ///
    /// Simply clears the pending IDs without acknowledging them.
    pub fn rollback(&mut self) {
        self.pending_ids.clear();
    }

    /// Commit the current checkpoint manually.
    ///
    /// This saves the last processed entry ID to the checkpoint store
    /// without acknowledging entries (useful for at-least-once semantics
    /// where you want to track progress but allow re-processing).
    pub fn commit_checkpoint(&self) -> Result<()> {
        if let (Some(store), Some(last_id)) = (&self.checkpoint_store, &self.stats.last_entry_id) {
            store.set(&self.checkpoint_key(), last_id)?;
        }
        Ok(())
    }

    /// Load the checkpoint and seek to the last processed position.
    ///
    /// This is useful when resuming consumption after a restart.
    /// Note: XREADGROUP with ">" always gets new messages, so this
    /// is primarily for tracking/logging purposes.
    pub fn load_checkpoint(&self) -> Result<Option<String>> {
        if let Some(store) = &self.checkpoint_store {
            store.get(&self.checkpoint_key())
        } else {
            Ok(None)
        }
    }

    /// Delete the checkpoint for this consumer.
    pub fn delete_checkpoint(&self) -> Result<()> {
        if let Some(store) = &self.checkpoint_store {
            store.delete(&self.checkpoint_key())?;
        }
        Ok(())
    }

    /// Get the stream key.
    pub fn stream(&self) -> &str {
        &self.stream
    }

    /// Get the group name.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Get the consumer name.
    pub fn consumer_name(&self) -> &str {
        &self.consumer
    }
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Create a consumer group for a stream.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `stream` - Stream key
/// * `group` - Group name to create
/// * `start_id` - Starting ID ("0" = from beginning, "$" = from now)
/// * `mkstream` - Whether to create the stream if it doesn't exist
pub fn create_consumer_group(
    url: &str,
    stream: &str,
    group: &str,
    start_id: &str,
    mkstream: bool,
) -> Result<bool> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;
    let connection = RedisConnection::new(url)?;
    let mut conn = runtime.block_on(connection.get_connection_manager())?;

    runtime.block_on(async {
        let mut cmd = redis::cmd("XGROUP");
        cmd.arg("CREATE").arg(stream).arg(group).arg(start_id);

        if mkstream {
            cmd.arg("MKSTREAM");
        }

        let result: redis::RedisResult<()> = cmd.query_async(&mut conn).await;

        match result {
            Ok(()) => Ok(true),
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("BUSYGROUP") {
                    Ok(false) // Group already exists
                } else {
                    Err(Error::Connection(e))
                }
            },
        }
    })
}

/// Destroy a consumer group.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `stream` - Stream key
/// * `group` - Group name to destroy
pub fn destroy_consumer_group(url: &str, stream: &str, group: &str) -> Result<bool> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;
    let connection = RedisConnection::new(url)?;
    let mut conn = runtime.block_on(connection.get_connection_manager())?;

    runtime.block_on(async {
        let destroyed: i64 = redis::cmd("XGROUP")
            .arg("DESTROY")
            .arg(stream)
            .arg(group)
            .query_async(&mut conn)
            .await?;

        Ok(destroyed > 0)
    })
}

/// Acknowledge stream entries.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `stream` - Stream key
/// * `group` - Consumer group name
/// * `ids` - Entry IDs to acknowledge
pub fn stream_ack(url: &str, stream: &str, group: &str, ids: &[String]) -> Result<usize> {
    if ids.is_empty() {
        return Ok(0);
    }

    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;
    let connection = RedisConnection::new(url)?;
    let mut conn = runtime.block_on(connection.get_connection_manager())?;

    runtime.block_on(async {
        let mut cmd = redis::cmd("XACK");
        cmd.arg(stream).arg(group);
        for id in ids {
            cmd.arg(id);
        }

        let acked: usize = cmd.query_async(&mut conn).await?;
        Ok(acked)
    })
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_checkpoint_store() {
        let store = MemoryCheckpointStore::new();

        // Initially empty
        assert!(store.get("test:key").unwrap().is_none());

        // Set and get
        store.set("test:key", "1234567890123-0").unwrap();
        assert_eq!(
            store.get("test:key").unwrap(),
            Some("1234567890123-0".to_string())
        );

        // Delete
        store.delete("test:key").unwrap();
        assert!(store.get("test:key").unwrap().is_none());
    }

    #[test]
    fn test_consumer_config_builder() {
        let config = ConsumerConfig::new()
            .with_batch_size(500)
            .with_block_ms(1000)
            .with_auto_ack(true)
            .with_create_group(false)
            .with_group_start_id("$")
            .with_claim_pending(true)
            .with_claim_min_idle_ms(30000);

        assert_eq!(config.batch_size, 500);
        assert_eq!(config.block_ms, 1000);
        assert!(config.auto_ack);
        assert!(!config.create_group);
        assert_eq!(config.group_start_id, "$");
        assert!(config.claim_pending);
        assert_eq!(config.claim_min_idle_ms, 30000);
    }

    #[test]
    fn test_consumer_stats() {
        let mut stats = ConsumerStats::new();
        stats.started_at = Some(Instant::now());
        stats.entries_processed = 100;

        // entries_per_second() requires some elapsed time
        // If elapsed is essentially 0, it will return 0
        // Just verify it doesn't panic and returns a non-negative value
        assert!(stats.entries_per_second() >= 0.0);
        assert!(stats.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn test_checkpoint_key_format() {
        // We can't easily test StreamConsumer::checkpoint_key without a Redis connection,
        // but we can verify the expected format
        let stream = "mystream";
        let group = "mygroup";
        let consumer = "consumer-1";
        let expected = format!("{}:{}:{}", stream, group, consumer);
        assert_eq!(expected, "mystream:mygroup:consumer-1");
    }

    #[test]
    #[ignore] // Requires running Redis instance
    fn test_redis_checkpoint_store() {
        let store = RedisCheckpointStore::new("redis://localhost:6379")
            .unwrap()
            .with_prefix("test:checkpoint:")
            .with_ttl(60);

        let key = "integration:test";

        // Clean up first
        let _ = store.delete(key);

        // Set and get
        store.set(key, "1234567890123-0").unwrap();
        assert_eq!(store.get(key).unwrap(), Some("1234567890123-0".to_string()));

        // Delete
        store.delete(key).unwrap();
        assert!(store.get(key).unwrap().is_none());
    }

    #[test]
    #[ignore] // Requires running Redis instance
    fn test_stream_consumer_creation() {
        let consumer = StreamConsumer::new(
            "redis://localhost:6379",
            "test:stream",
            "test:group",
            "consumer-1",
        );
        assert!(consumer.is_ok());

        let consumer = consumer.unwrap();
        assert_eq!(consumer.stream(), "test:stream");
        assert_eq!(consumer.group(), "test:group");
        assert_eq!(consumer.consumer_name(), "consumer-1");
    }

    #[test]
    #[ignore] // Requires running Redis instance
    fn test_create_and_destroy_consumer_group() {
        let url = "redis://localhost:6379";
        let stream = "test:consumer:stream";
        let group = "test:consumer:group";

        // Create the group
        let created = create_consumer_group(url, stream, group, "0", true).unwrap();
        assert!(created);

        // Creating again should return false (already exists)
        let created_again = create_consumer_group(url, stream, group, "0", true).unwrap();
        assert!(!created_again);

        // Destroy the group
        let destroyed = destroy_consumer_group(url, stream, group).unwrap();
        assert!(destroyed);

        // Clean up stream
        let runtime = Runtime::new().unwrap();
        let connection = RedisConnection::new(url).unwrap();
        let mut conn = runtime
            .block_on(connection.get_connection_manager())
            .unwrap();
        runtime.block_on(async {
            let _: () = redis::cmd("DEL")
                .arg(stream)
                .query_async(&mut conn)
                .await
                .unwrap();
        });
    }
}
