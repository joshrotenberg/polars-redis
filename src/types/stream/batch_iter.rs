//! Batch iterator for streaming Redis Stream data as Arrow RecordBatches.

use arrow::array::RecordBatch;
use redis::aio::ConnectionManager;
use tokio::runtime::Runtime;

use super::convert::{StreamSchema, streams_to_record_batch};
use super::reader::StreamData;
use crate::connection::RedisConnection;
use crate::error::{Error, Result};
use crate::types::hash::BatchConfig;

/// Iterator for scanning Redis Streams in batches as Arrow RecordBatches.
///
/// This iterator fetches stream keys matching a pattern and retrieves their
/// entries, converting them to Arrow RecordBatches for use with Polars.
/// Each entry becomes a row in the output.
///
/// Supports two modes:
/// 1. Single stream: Provide a specific key and iterate through entries
/// 2. Multi-stream: Provide a pattern to SCAN and fetch entries from each
///
/// # Example
///
/// ```ignore
/// use polars_redis::{StreamBatchIterator, StreamSchema, BatchConfig};
///
/// let schema = StreamSchema::new(vec![
///     ("user_id".to_string(), DataType::Utf8),
///     ("action".to_string(), DataType::Utf8),
/// ]).with_key(true).with_entry_id(true);
///
/// let config = BatchConfig::new("events:*").with_batch_size(1000);
///
/// let mut iterator = StreamBatchIterator::new(url, schema, config)?;
///
/// while let Some(batch) = iterator.next_batch()? {
///     println!("Got {} entries", batch.num_rows());
/// }
/// ```
///
/// # Output Schema
///
/// - `_key` (optional): The Redis stream key
/// - `_entry_id` (optional): The stream entry ID (e.g., "1234567890-0")
/// - `_timestamp` (optional): Entry timestamp extracted from ID (Int64 ms)
/// - User-defined fields from the stream entries
pub struct StreamBatchIterator {
    /// Tokio runtime for async operations.
    runtime: Runtime,
    /// Redis connection manager (cheap to clone, auto-reconnects).
    conn: ConnectionManager,
    /// Schema for the stream data.
    schema: StreamSchema,
    /// Batch configuration.
    config: BatchConfig,
    /// Start ID for XRANGE (default: "-" for oldest).
    start_id: String,
    /// End ID for XRANGE (default: "+" for newest).
    end_id: String,
    /// Maximum entries to fetch per stream.
    count_per_stream: Option<usize>,
    /// Current SCAN cursor.
    cursor: u64,
    /// Whether we've started scanning.
    scan_started: bool,
    /// Whether we've completed the SCAN.
    done: bool,
    /// Total rows (entries) yielded so far.
    rows_yielded: usize,
    /// Current row index offset.
    row_index_offset: u64,
    /// Buffer of keys waiting to be fetched.
    key_buffer: Vec<String>,
}

impl StreamBatchIterator {
    /// Create a new StreamBatchIterator.
    ///
    /// # Arguments
    /// * `url` - Redis connection URL
    /// * `schema` - Schema configuration for the stream
    /// * `config` - Batch configuration (pattern, batch_size, etc.)
    pub fn new(url: &str, schema: StreamSchema, config: BatchConfig) -> Result<Self> {
        let runtime = Runtime::new()
            .map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;
        let connection = RedisConnection::new(url)?;
        let conn = runtime.block_on(connection.get_connection_manager())?;

        Ok(Self {
            runtime,
            conn,
            schema,
            config,
            start_id: "-".to_string(),
            end_id: "+".to_string(),
            count_per_stream: None,
            cursor: 0,
            scan_started: false,
            done: false,
            rows_yielded: 0,
            row_index_offset: 0,
            key_buffer: Vec::new(),
        })
    }

    /// Set the start ID for XRANGE.
    pub fn with_start_id(mut self, id: &str) -> Self {
        self.start_id = id.to_string();
        self
    }

    /// Set the end ID for XRANGE.
    pub fn with_end_id(mut self, id: &str) -> Self {
        self.end_id = id.to_string();
        self
    }

    /// Set the maximum entries to fetch per stream.
    pub fn with_count_per_stream(mut self, count: usize) -> Self {
        self.count_per_stream = Some(count);
        self
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
        // For streams, we use a smaller key batch since each key can have many entries
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

        // Fetch stream data
        let stream_data = self.fetch_batch(&keys_to_fetch)?;

        if stream_data.is_empty() || stream_data.iter().all(|s| s.entries.is_empty()) {
            // All keys were deleted, are not streams, or have no entries in range
            if self.key_buffer.is_empty() && self.scan_complete() {
                self.done = true;
                return Ok(None);
            }
            return self.next_batch();
        }

        // Convert to RecordBatch
        let mut batch = streams_to_record_batch(&stream_data, &self.schema, self.row_index_offset)?;

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

    /// Fetch stream data for a batch of keys.
    fn fetch_batch(&mut self, keys: &[String]) -> Result<Vec<StreamData>> {
        let start_id = self.start_id.clone();
        let end_id = self.end_id.clone();
        let count = self.count_per_stream;
        let mut conn = self.conn.clone();

        self.runtime.block_on(fetch_streams_async(
            &mut conn, keys, &start_id, &end_id, count,
        ))
    }

    /// Check if iteration is complete.
    pub fn is_done(&self) -> bool {
        self.done
    }

    /// Get the number of rows (entries) yielded so far.
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

/// Fetch stream entries for a batch of keys using async connection.
async fn fetch_streams_async(
    conn: &mut ConnectionManager,
    keys: &[String],
    start_id: &str,
    end_id: &str,
    count: Option<usize>,
) -> Result<Vec<StreamData>> {
    use std::collections::HashMap;

    use crate::types::stream::reader::StreamEntry;

    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut results = Vec::with_capacity(keys.len());

    for key in keys {
        let entries: Vec<(String, HashMap<String, String>)> = if let Some(c) = count {
            redis::cmd("XRANGE")
                .arg(key)
                .arg(start_id)
                .arg(end_id)
                .arg("COUNT")
                .arg(c)
                .query_async(conn)
                .await
                .unwrap_or_default()
        } else {
            redis::cmd("XRANGE")
                .arg(key)
                .arg(start_id)
                .arg(end_id)
                .query_async(conn)
                .await
                .unwrap_or_default()
        };

        let mut stream_entries = Vec::with_capacity(entries.len());
        for (id, fields) in entries {
            // Parse entry ID: "1234567890123-0" -> (timestamp_ms, sequence)
            if let Some((ts_str, seq_str)) = id.split_once('-')
                && let (Ok(timestamp_ms), Ok(sequence)) =
                    (ts_str.parse::<i64>(), seq_str.parse::<u64>())
            {
                stream_entries.push(StreamEntry {
                    id,
                    timestamp_ms,
                    sequence,
                    fields,
                });
            }
        }

        results.push(StreamData {
            key: key.clone(),
            entries: stream_entries,
        });
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore] // Requires running Redis instance
    fn test_stream_batch_iterator_creation() {
        let schema = StreamSchema::new();
        let config = BatchConfig::new("events:*");

        let result = StreamBatchIterator::new("redis://localhost:6379", schema, config);
        assert!(result.is_ok());
    }

    #[test]
    #[ignore] // Requires running Redis instance
    fn test_stream_batch_iterator_with_options() {
        let schema = StreamSchema::new()
            .with_key(true)
            .with_id(true)
            .with_timestamp(true)
            .with_sequence(true)
            .add_field("action")
            .add_field("user");

        let config = BatchConfig::new("events:*").with_batch_size(500);

        let result = StreamBatchIterator::new("redis://localhost:6379", schema, config);
        assert!(result.is_ok());

        let iter = result
            .unwrap()
            .with_start_id("-")
            .with_end_id("+")
            .with_count_per_stream(100);

        assert_eq!(iter.start_id, "-");
        assert_eq!(iter.end_id, "+");
        assert_eq!(iter.count_per_stream, Some(100));
    }
}
