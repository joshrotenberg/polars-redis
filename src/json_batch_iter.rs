//! Batch iterator for streaming Redis JSON data as Arrow RecordBatches.

use arrow::array::RecordBatch;
use tokio::runtime::Runtime;

use crate::batch_iter::BatchConfig;
use crate::connection::RedisConnection;
use crate::error::{Error, Result};
use crate::json_convert::{JsonSchema, json_to_record_batch};
use crate::json_reader::{JsonData, fetch_json};

/// Iterator state for scanning Redis JSON documents.
pub struct JsonBatchIterator {
    /// Tokio runtime for async operations.
    runtime: Runtime,
    /// Redis connection.
    connection: RedisConnection,
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
        let (new_cursor, keys) = self.runtime.block_on(async {
            let mut conn = self.connection.get_async_connection().await?;
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
        let projection = self.projection.as_deref();
        let include_ttl = self.schema.include_ttl();

        self.runtime.block_on(async {
            let mut conn = self.connection.get_async_connection().await?;
            fetch_json(&mut conn, keys, projection, include_ttl).await
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
