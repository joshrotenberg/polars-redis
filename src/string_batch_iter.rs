//! Batch iterator for streaming Redis string data as Arrow RecordBatches.
//!
//! This module provides the iteration logic for scanning Redis string values
//! and converting them to Arrow format.

use arrow::array::RecordBatch;
use redis::aio::MultiplexedConnection;
use tokio::runtime::Runtime;

use crate::batch_iter::BatchConfig;
use crate::connection::RedisConnection;
use crate::error::{Error, Result};
use crate::string_convert::{StringSchema, strings_to_record_batch};
use crate::string_reader::{StringData, fetch_strings};

/// Iterator for scanning Redis strings and yielding Arrow RecordBatches.
pub struct StringBatchIterator {
    /// Tokio runtime for async operations.
    runtime: Runtime,
    /// Redis connection.
    connection: RedisConnection,
    /// Schema for the string data.
    schema: StringSchema,
    /// Batch configuration.
    config: BatchConfig,
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

impl StringBatchIterator {
    /// Create a new StringBatchIterator.
    pub fn new(url: &str, schema: StringSchema, config: BatchConfig) -> Result<Self> {
        let runtime = Runtime::new()
            .map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;
        let connection = RedisConnection::new(url)?;

        Ok(Self {
            runtime,
            connection,
            schema,
            config,
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

        // Fetch string data
        let string_data = self.fetch_batch(&keys_to_fetch)?;

        if string_data.is_empty() {
            // All keys were deleted between SCAN and fetch, try again
            return self.next_batch();
        }

        // Convert to RecordBatch
        let mut batch = strings_to_record_batch(&string_data, &self.schema)?;

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

    /// Fetch string data for a batch of keys.
    fn fetch_batch(&mut self, keys: &[String]) -> Result<Vec<StringData>> {
        self.runtime.block_on(async {
            let mut conn = self.connection.get_async_connection().await?;
            fetch_strings(&mut conn, keys).await
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
    use arrow::datatypes::DataType;

    #[test]
    fn test_string_batch_iterator_creation() {
        let schema = StringSchema::new(DataType::Utf8);
        let config = BatchConfig::new("test:*");

        // This will fail without a running Redis, but should create the iterator
        let result = StringBatchIterator::new("redis://localhost:6379", schema, config);

        // Should succeed even without Redis running (connection is lazy)
        assert!(result.is_ok());
    }

    #[test]
    fn test_string_batch_iterator_with_int64() {
        let schema = StringSchema::new(DataType::Int64).with_value_column_name("count");
        let config = BatchConfig::new("counter:*").with_batch_size(500);

        let result = StringBatchIterator::new("redis://localhost:6379", schema, config);
        assert!(result.is_ok());
    }
}
