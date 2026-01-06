//! Batch iterator for streaming Redis JSON data as Arrow RecordBatches.

use arrow::array::RecordBatch;
use redis::aio::ConnectionManager;
use tokio::runtime::Runtime;

use super::convert::{JsonSchema, json_to_record_batch};
use super::reader::{JsonData, fetch_json};
use crate::connection::RedisConnection;
use crate::error::{Error, Result};
use crate::types::hash::BatchConfig;

/// Iterator for scanning RedisJSON documents in batches as Arrow RecordBatches.
///
/// This iterator fetches JSON keys matching a pattern and retrieves their
/// document contents, converting them to Arrow RecordBatches for use with Polars.
///
/// Requires the RedisJSON module to be loaded on the Redis server.
///
/// # Example
///
/// ```ignore
/// use polars_redis::{JsonBatchIterator, JsonSchema, BatchConfig, DataType};
///
/// let schema = JsonSchema::new(vec![
///     ("$.name".to_string(), DataType::Utf8),
///     ("$.age".to_string(), DataType::Int64),
/// ]).with_key(true);
///
/// let config = BatchConfig::new("doc:*").with_batch_size(1000);
///
/// let mut iterator = JsonBatchIterator::new(url, schema, config, None)?;
///
/// while let Some(batch) = iterator.next_batch()? {
///     println!("Got {} rows", batch.num_rows());
/// }
/// ```
///
/// # Performance
///
/// - Uses `SCAN` with configurable `COUNT` hint for non-blocking iteration
/// - Pipelines multiple `JSON.GET` commands per batch
/// - Supports JSONPath projection (only fetch requested paths)
/// - Memory-efficient streaming (processes one batch at a time)
pub struct JsonBatchIterator {
    /// Tokio runtime for async operations.
    runtime: Runtime,
    /// Redis connection manager (cheap to clone, auto-reconnects).
    conn: ConnectionManager,
    /// Schema for the JSON data.
    schema: JsonSchema,
    /// Batch configuration.
    config: BatchConfig,
    /// Fields to fetch (None = full document).
    projection: Option<Vec<String>>,
    /// Current SCAN cursor.
    cursor: u64,
    /// Whether we've started scanning.
    scan_started: bool,
    /// Whether we've completed the SCAN.
    done: bool,
    /// Total rows yielded so far.
    rows_yielded: usize,
    /// Buffer of keys waiting to be fetched.
    key_buffer: Vec<String>,
    /// Current row offset for row index column.
    row_offset: u64,
}

impl JsonBatchIterator {
    /// Create a new JsonBatchIterator.
    pub fn new(
        url: &str,
        schema: JsonSchema,
        config: BatchConfig,
        projection: Option<Vec<String>>,
    ) -> Result<Self> {
        let runtime = Runtime::new()
            .map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;
        let connection = RedisConnection::new(url)?;
        let conn = runtime.block_on(connection.get_connection_manager())?;

        Ok(Self {
            runtime,
            conn,
            schema,
            config,
            projection,
            cursor: 0,
            scan_started: false,
            done: false,
            rows_yielded: 0,
            key_buffer: Vec::new(),
            row_offset: 0,
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

        // Fetch JSON data
        let json_data = self.fetch_batch(&keys_to_fetch)?;

        if json_data.is_empty() {
            return self.next_batch();
        }

        // Apply projection to schema if needed
        let effective_schema = match &self.projection {
            Some(cols) => self.schema.project(cols),
            None => self.schema.clone(),
        };

        // Convert to RecordBatch with current row offset
        let mut batch = json_to_record_batch(&json_data, &effective_schema, self.row_offset)?;

        // Update row offset for next batch
        self.row_offset += batch.num_rows() as u64;

        // Apply max rows limit
        if let Some(max) = self.config.max_rows {
            let remaining = max - self.rows_yielded;
            if batch.num_rows() > remaining {
                batch = batch.slice(0, remaining);
                self.done = true;
            }
        }

        self.rows_yielded += batch.num_rows();

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
        let (new_cursor, keys) = self.runtime.block_on(async {
            let result: (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(self.cursor)
                .arg("MATCH")
                .arg(&self.config.pattern)
                .arg("COUNT")
                .arg(self.config.count_hint)
                .query_async(&mut conn)
                .await?;
            Ok::<_, Error>(result)
        })?;

        self.key_buffer.extend(keys);
        self.cursor = new_cursor;
        self.scan_started = true;

        Ok(())
    }

    /// Fetch JSON data for a batch of keys.
    fn fetch_batch(&mut self, keys: &[String]) -> Result<Vec<JsonData>> {
        let include_ttl = self.schema.include_ttl();
        let worker_count = self.config.parallel.worker_count();

        // Use parallel fetching if configured
        if worker_count > 1 && keys.len() > worker_count {
            self.fetch_batch_parallel(keys, include_ttl, worker_count)
        } else {
            let projection = self.projection.as_deref();
            let mut conn = self.conn.clone();
            self.runtime
                .block_on(fetch_json(&mut conn, keys, projection, include_ttl))
        }
    }

    /// Fetch JSON data in parallel using multiple workers.
    fn fetch_batch_parallel(
        &mut self,
        keys: &[String],
        include_ttl: bool,
        worker_count: usize,
    ) -> Result<Vec<JsonData>> {
        // Split keys into chunks for parallel processing
        let chunk_size = keys.len().div_ceil(worker_count);
        let chunks: Vec<Vec<String>> = keys.chunks(chunk_size).map(|c| c.to_vec()).collect();

        // Clone what we need before the async block
        let conn = self.conn.clone();
        let projection_owned: Option<Vec<String>> = self.projection.clone();

        self.runtime.block_on(async {
            let mut handles = Vec::with_capacity(chunks.len());

            for chunk in chunks {
                let mut conn = conn.clone();
                let proj = projection_owned.clone();

                let handle = tokio::spawn(async move {
                    fetch_json(&mut conn, &chunk, proj.as_deref(), include_ttl).await
                });
                handles.push(handle);
            }

            // Collect results in order
            let mut all_data = Vec::with_capacity(keys.len());
            for handle in handles {
                match handle.await {
                    Ok(Ok(data)) => all_data.extend(data),
                    Ok(Err(e)) => return Err(e),
                    Err(e) => {
                        return Err(Error::Runtime(format!("Task join error: {}", e)));
                    }
                }
            }

            Ok(all_data)
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    #[ignore] // Requires running Redis instance
    fn test_json_batch_iterator_creation() {
        let schema = JsonSchema::new(vec![
            ("name".to_string(), DataType::Utf8),
            ("count".to_string(), DataType::Int64),
        ]);
        let config = BatchConfig::new("test:*");

        let result = JsonBatchIterator::new("redis://localhost:6379", schema, config, None);
        assert!(result.is_ok());
    }
}
