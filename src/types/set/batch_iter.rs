//! Batch iterator for streaming Redis set data as Arrow RecordBatches.

use arrow::array::RecordBatch;
use redis::aio::MultiplexedConnection;
use tokio::runtime::Runtime;

use super::convert::{SetSchema, sets_to_record_batch};
use super::reader::{SetData, fetch_sets};
use crate::connection::RedisConnection;
use crate::error::{Error, Result};
use crate::types::hash::BatchConfig;

/// Iterator for scanning Redis sets and yielding Arrow RecordBatches.
pub struct SetBatchIterator {
    /// Tokio runtime for async operations.
    runtime: Runtime,
    /// Redis connection.
    connection: RedisConnection,
    /// Schema for the set data.
    schema: SetSchema,
    /// Batch configuration.
    config: BatchConfig,
    /// Current SCAN cursor.
    cursor: u64,
    /// Whether we've started scanning.
    scan_started: bool,
    /// Whether we've completed the SCAN.
    done: bool,
    /// Total rows (members) yielded so far.
    rows_yielded: usize,
    /// Current row index offset.
    row_index_offset: u64,
    /// Buffer of keys waiting to be fetched.
    key_buffer: Vec<String>,
}

impl SetBatchIterator {
    /// Create a new SetBatchIterator.
    pub fn new(url: &str, schema: SetSchema, config: BatchConfig) -> Result<Self> {
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
            row_index_offset: 0,
            key_buffer: Vec::new(),
        })
    }

    /// Get the next batch of data as a RecordBatch.
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
        // For sets, we use a smaller key batch since each key can have many members
        let keys_per_batch = (self.config.batch_size / 10).max(10);
        while self.key_buffer.len() < keys_per_batch && !self.scan_complete() {
            self.scan_more_keys()?;
        }

        if self.key_buffer.is_empty() {
            self.done = true;
            return Ok(None);
        }

        // Take keys for this batch
        let keys_to_fetch: Vec<String> = self
            .key_buffer
            .drain(..self.key_buffer.len().min(keys_per_batch))
            .collect();

        // Fetch set data
        let set_data = self.fetch_batch(&keys_to_fetch)?;

        if set_data.is_empty() {
            // All keys were deleted or are not sets, try again
            return self.next_batch();
        }

        // Convert to RecordBatch
        let mut batch = sets_to_record_batch(&set_data, &self.schema, self.row_index_offset)?;

        // Update row index offset
        self.row_index_offset += batch.num_rows() as u64;

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

    /// Fetch set data for a batch of keys.
    fn fetch_batch(&mut self, keys: &[String]) -> Result<Vec<SetData>> {
        self.runtime.block_on(async {
            let mut conn = self.connection.get_async_connection().await?;
            fetch_sets(&mut conn, keys).await
        })
    }

    /// Check if iteration is complete.
    pub fn is_done(&self) -> bool {
        self.done
    }

    /// Get the number of rows (members) yielded so far.
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

    #[test]
    fn test_set_batch_iterator_creation() {
        let schema = SetSchema::new();
        let config = BatchConfig::new("tags:*");

        let result = SetBatchIterator::new("redis://localhost:6379", schema, config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_set_batch_iterator_with_options() {
        let schema = SetSchema::new()
            .with_key(true)
            .with_member_column_name("tag")
            .with_row_index(true);

        let config = BatchConfig::new("tags:*").with_batch_size(500);

        let result = SetBatchIterator::new("redis://localhost:6379", schema, config);
        assert!(result.is_ok());
    }
}
