//! Batch iterator for streaming Redis sorted set data as Arrow RecordBatches.

use arrow::array::RecordBatch;
use redis::aio::ConnectionManager;
use tokio::runtime::Runtime;

use super::convert::{ZSetSchema, zsets_to_record_batch};
use super::reader::{ZSetData, fetch_zsets};
use crate::connection::RedisConnection;
use crate::error::{Error, Result};
use crate::types::hash::BatchConfig;

/// Iterator for scanning Redis sorted sets in batches as Arrow RecordBatches.
///
/// This iterator fetches sorted set keys matching a pattern and retrieves their
/// members with scores, converting them to Arrow RecordBatches for use with Polars.
/// Each member becomes a row in the output.
///
/// # Example
///
/// ```ignore
/// use polars_redis::{ZSetBatchIterator, ZSetSchema, BatchConfig};
///
/// let schema = ZSetSchema::new().with_key(true).with_rank(true);
/// let config = BatchConfig::new("leaderboard:*").with_batch_size(1000);
///
/// let mut iterator = ZSetBatchIterator::new(url, schema, config)?;
///
/// while let Some(batch) = iterator.next_batch()? {
///     println!("Got {} members", batch.num_rows());
/// }
/// ```
///
/// # Output Schema
///
/// - `_key` (optional): The Redis key
/// - `member`: The sorted set member value (Utf8)
/// - `score`: The member's score (Float64)
/// - `rank` (optional): The member's rank in the sorted set (Int64)
pub struct ZSetBatchIterator {
    /// Tokio runtime for async operations.
    runtime: Runtime,
    /// Redis connection manager (cheap to clone, auto-reconnects).
    conn: ConnectionManager,
    /// Schema for the sorted set data.
    schema: ZSetSchema,
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

impl ZSetBatchIterator {
    /// Create a new ZSetBatchIterator.
    pub fn new(url: &str, schema: ZSetSchema, config: BatchConfig) -> Result<Self> {
        let runtime = Runtime::new()
            .map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;
        let connection = RedisConnection::new(url)?;
        let conn = runtime.block_on(connection.get_connection_manager())?;

        Ok(Self {
            runtime,
            conn,
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
        // For sorted sets, we use a smaller key batch since each key can have many members
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

        // Fetch sorted set data
        let zset_data = self.fetch_batch(&keys_to_fetch)?;

        if zset_data.is_empty() {
            // All keys were deleted or are not sorted sets, try again
            return self.next_batch();
        }

        // Convert to RecordBatch
        let mut batch = zsets_to_record_batch(&zset_data, &self.schema, self.row_index_offset)?;

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
        let mut conn = self.conn.clone();
        let (new_cursor, keys) = self.runtime.block_on(scan_keys_batch(
            &mut conn,
            self.cursor,
            &self.config.pattern,
            self.config.count_hint,
        ))?;

        self.key_buffer.extend(keys);
        self.cursor = new_cursor;
        self.scan_started = true;

        Ok(())
    }

    /// Fetch sorted set data for a batch of keys.
    fn fetch_batch(&mut self, keys: &[String]) -> Result<Vec<ZSetData>> {
        let mut conn = self.conn.clone();
        self.runtime.block_on(fetch_zsets(&mut conn, keys))
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
    conn: &mut ConnectionManager,
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
    #[ignore] // Requires running Redis instance
    fn test_zset_batch_iterator_creation() {
        let schema = ZSetSchema::new();
        let config = BatchConfig::new("leaderboard:*");

        let result = ZSetBatchIterator::new("redis://localhost:6379", schema, config);
        assert!(result.is_ok());
    }

    #[test]
    #[ignore] // Requires running Redis instance
    fn test_zset_batch_iterator_with_options() {
        let schema = ZSetSchema::new()
            .with_key(true)
            .with_member_column_name("player")
            .with_score_column_name("points")
            .with_rank(true)
            .with_row_index(true);

        let config = BatchConfig::new("leaderboard:*").with_batch_size(500);

        let result = ZSetBatchIterator::new("redis://localhost:6379", schema, config);
        assert!(result.is_ok());
    }
}
