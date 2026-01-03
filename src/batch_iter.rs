//! Batch iterator for streaming Redis data as Arrow RecordBatches.
//!
//! This module provides the core iteration logic that powers the Polars IO plugin.
//! It handles SCAN iteration, batching, and conversion to Arrow format.

use arrow::array::RecordBatch;
use redis::aio::MultiplexedConnection;
use tokio::runtime::Runtime;

use crate::arrow_convert::hashes_to_record_batch;
use crate::connection::RedisConnection;
use crate::error::{Error, Result};
use crate::hash_reader::{HashData, fetch_hashes};
use crate::schema::HashSchema;

/// Configuration for batch iteration.
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Key pattern to match (e.g., "user:*").
    pub pattern: String,
    /// Number of keys to process per batch.
    pub batch_size: usize,
    /// SCAN COUNT hint for Redis.
    pub count_hint: usize,
    /// Maximum total rows to return (None for unlimited).
    pub max_rows: Option<usize>,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            pattern: "*".to_string(),
            batch_size: 1000,
            count_hint: 100,
            max_rows: None,
        }
    }
}

impl BatchConfig {
    /// Create a new BatchConfig with the given pattern.
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
            ..Default::default()
        }
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set the COUNT hint for SCAN.
    pub fn with_count_hint(mut self, count: usize) -> Self {
        self.count_hint = count;
        self
    }

    /// Set the maximum number of rows to return.
    pub fn with_max_rows(mut self, max: usize) -> Self {
        self.max_rows = Some(max);
        self
    }
}

/// Iterator state for scanning Redis hashes.
pub struct HashBatchIterator {
    /// Tokio runtime for async operations.
    runtime: Runtime,
    /// Redis connection.
    connection: RedisConnection,
    /// Schema for the hash data.
    schema: HashSchema,
    /// Batch configuration.
    config: BatchConfig,
    /// Fields to fetch (None = all fields via HGETALL).
    projection: Option<Vec<String>>,
    /// Current SCAN cursor.
    cursor: u64,
    /// Whether we've started scanning (cursor has moved at least once).
    scan_started: bool,
    /// Whether we've completed the SCAN.
    done: bool,
    /// Total rows yielded so far.
    rows_yielded: usize,
    /// Buffer of keys waiting to be fetched.
    key_buffer: Vec<String>,
}

impl HashBatchIterator {
    /// Create a new HashBatchIterator.
    pub fn new(
        url: &str,
        schema: HashSchema,
        config: BatchConfig,
        projection: Option<Vec<String>>,
    ) -> Result<Self> {
        let runtime = Runtime::new()
            .map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;
        let connection = RedisConnection::new(url)?;

        Ok(Self {
            runtime,
            connection,
            schema,
            config,
            projection,
            cursor: 0,
            scan_started: false,
            done: false,
            rows_yielded: 0,
            key_buffer: Vec::new(),
        })
    }

    /// Get the next batch of data as a RecordBatch.
    ///
    /// Returns None when iteration is complete.
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }

        // Check if we've hit the max rows limit
        if let Some(max) = self.config.max_rows
            && self.rows_yielded >= max
        {
            self.done = true;
            return Ok(None);
        }

        // Accumulate keys until we have enough for a batch
        while self.key_buffer.len() < self.config.batch_size && !self.scan_complete() {
            self.scan_more_keys()?;
        }

        if self.key_buffer.is_empty() {
            self.done = true;
            return Ok(None);
        }

        // Take up to batch_size keys
        let keys_to_fetch: Vec<String> = self
            .key_buffer
            .drain(..self.key_buffer.len().min(self.config.batch_size))
            .collect();

        // Fetch hash data
        let hash_data = self.fetch_batch(&keys_to_fetch)?;

        if hash_data.is_empty() {
            // All keys were deleted between SCAN and fetch, try again
            return self.next_batch();
        }

        // Apply projection to schema if needed
        let effective_schema = match &self.projection {
            Some(cols) => self.schema.project(cols),
            None => self.schema.clone(),
        };

        // Convert to RecordBatch
        let mut batch = hashes_to_record_batch(&hash_data, &effective_schema)?;

        // Apply max rows limit
        if let Some(max) = self.config.max_rows {
            let remaining = max - self.rows_yielded;
            if batch.num_rows() > remaining {
                batch = batch.slice(0, remaining);
                self.done = true;
            }
        }

        self.rows_yielded += batch.num_rows();

        // Check if we're done
        if self.key_buffer.is_empty() && self.scan_complete() {
            self.done = true;
        }

        Ok(Some(batch))
    }

    /// Check if the SCAN is complete.
    fn scan_complete(&self) -> bool {
        // cursor == 0 after at least one iteration means we've completed the full scan
        (self.cursor == 0 && self.scan_started) || self.done
    }

    /// Scan more keys from Redis.
    fn scan_more_keys(&mut self) -> Result<()> {
        let (new_cursor, keys) = self.runtime.block_on(async {
            let mut conn = self.connection.get_async_connection().await?;
            scan_keys_batch(
                &mut conn,
                self.cursor,
                &self.config.pattern,
                self.config.count_hint,
            )
            .await
        })?;

        self.key_buffer.extend(keys);
        self.cursor = new_cursor;
        self.scan_started = true;

        Ok(())
    }

    /// Fetch hash data for a batch of keys.
    fn fetch_batch(&mut self, keys: &[String]) -> Result<Vec<HashData>> {
        let projection = self.projection.as_deref();

        self.runtime.block_on(async {
            let mut conn = self.connection.get_async_connection().await?;
            fetch_hashes(&mut conn, keys, projection).await
        })
    }

    /// Check if iteration is complete.
    pub fn is_done(&self) -> bool {
        self.done
    }

    /// Get the number of rows yielded so far.
    pub fn rows_yielded(&self) -> usize {
        self.rows_yielded
    }
}

/// Scan a batch of keys from Redis.
async fn scan_keys_batch(
    conn: &mut MultiplexedConnection,
    cursor: u64,
    pattern: &str,
    count: usize,
) -> Result<(u64, Vec<String>)> {
    let result: (u64, Vec<String>) = redis::cmd("SCAN")
        .arg(cursor)
        .arg("MATCH")
        .arg(pattern)
        .arg("COUNT")
        .arg(count)
        .query_async(conn)
        .await?;

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::RedisType;

    #[test]
    fn test_batch_config_default() {
        let config = BatchConfig::default();
        assert_eq!(config.pattern, "*");
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.count_hint, 100);
        assert!(config.max_rows.is_none());
    }

    #[test]
    fn test_batch_config_builder() {
        let config = BatchConfig::new("user:*")
            .with_batch_size(500)
            .with_count_hint(200)
            .with_max_rows(10000);

        assert_eq!(config.pattern, "user:*");
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.count_hint, 200);
        assert_eq!(config.max_rows, Some(10000));
    }

    #[test]
    fn test_hash_batch_iterator_creation() {
        let schema = HashSchema::new(vec![
            ("name".to_string(), RedisType::Utf8),
            ("age".to_string(), RedisType::Int64),
        ]);
        let config = BatchConfig::new("test:*");

        // This will fail without a running Redis, but should create the iterator
        let result = HashBatchIterator::new("redis://localhost:6379", schema, config, None);

        // Should succeed even without Redis running (connection is lazy)
        assert!(result.is_ok());
    }
}
