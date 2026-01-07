//! Cluster-aware batch iterator for Redis Stream data.

use std::collections::HashMap;

use arrow::array::RecordBatch;
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use tokio::runtime::Runtime;

use super::convert::{StreamSchema, streams_to_record_batch};
use super::reader::{StreamData, StreamEntry};
use crate::cluster::DirectClusterKeyScanner;
use crate::error::{Error, Result};
use crate::io::types::hash::BatchConfig;

/// Cluster-aware iterator for scanning Redis Streams.
pub struct ClusterStreamBatchIterator {
    /// Tokio runtime for async operations.
    runtime: Runtime,
    /// Cluster connection (cloneable, thread-safe).
    conn: ClusterConnection,
    /// Key scanner that iterates all cluster nodes.
    scanner: DirectClusterKeyScanner,
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
    /// Whether iteration is complete.
    done: bool,
    /// Total rows (entries) yielded so far.
    rows_yielded: usize,
    /// Current row index offset.
    row_index_offset: u64,
    /// Buffer of keys waiting to be fetched.
    key_buffer: Vec<String>,
}

impl ClusterStreamBatchIterator {
    /// Create a new cluster stream batch iterator.
    pub fn new(
        nodes: &[impl AsRef<str>],
        schema: StreamSchema,
        config: BatchConfig,
    ) -> Result<Self> {
        let runtime = Runtime::new()
            .map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

        let node_urls: Vec<String> = nodes.iter().map(|s| s.as_ref().to_string()).collect();
        let cluster_client = ClusterClient::new(node_urls)
            .map_err(|e| Error::InvalidUrl(format!("cluster: {}", e)))?;

        let (conn, scanner) = runtime.block_on(async {
            let conn = cluster_client
                .get_async_connection()
                .await
                .map_err(Error::Connection)?;

            let mut scan_conn = cluster_client
                .get_async_connection()
                .await
                .map_err(Error::Connection)?;

            let scanner =
                DirectClusterKeyScanner::new(&mut scan_conn, &config.pattern, config.count_hint)
                    .await?;

            Ok::<_, Error>((conn, scanner))
        })?;

        Ok(Self {
            runtime,
            conn,
            scanner,
            schema,
            config,
            start_id: "-".to_string(),
            end_id: "+".to_string(),
            count_per_stream: None,
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

        if let Some(max) = self.config.max_rows
            && self.rows_yielded >= max
        {
            self.done = true;
            return Ok(None);
        }

        // For streams, we use a smaller key batch since each key can have many entries
        let keys_per_batch = (self.config.batch_size / 10).max(10);
        while self.key_buffer.len() < keys_per_batch && !self.scanner.is_done() {
            self.scan_more_keys()?;
        }

        if self.key_buffer.is_empty() {
            self.done = true;
            return Ok(None);
        }

        let keys_to_fetch: Vec<String> = self
            .key_buffer
            .drain(..self.key_buffer.len().min(keys_per_batch))
            .collect();

        let stream_data = self.fetch_batch(&keys_to_fetch)?;

        if stream_data.is_empty() || stream_data.iter().all(|s| s.entries.is_empty()) {
            if self.key_buffer.is_empty() && self.scanner.is_done() {
                self.done = true;
                return Ok(None);
            }
            return self.next_batch();
        }

        let mut batch = streams_to_record_batch(&stream_data, &self.schema, self.row_index_offset)?;
        self.row_index_offset += batch.num_rows() as u64;

        if let Some(max) = self.config.max_rows {
            let remaining = max - self.rows_yielded;
            if batch.num_rows() > remaining {
                batch = batch.slice(0, remaining);
                self.done = true;
            }
        }

        self.rows_yielded += batch.num_rows();

        if self.key_buffer.is_empty() && self.scanner.is_done() {
            self.done = true;
        }

        Ok(Some(batch))
    }

    fn scan_more_keys(&mut self) -> Result<()> {
        let keys = self.runtime.block_on(self.scanner.next_batch())?;
        if let Some(k) = keys {
            self.key_buffer.extend(k);
        }
        Ok(())
    }

    fn fetch_batch(&mut self, keys: &[String]) -> Result<Vec<StreamData>> {
        let start_id = self.start_id.clone();
        let end_id = self.end_id.clone();
        let count = self.count_per_stream;
        let mut conn = self.conn.clone();

        self.runtime.block_on(fetch_streams_cluster(
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

    /// Get the number of cluster nodes being scanned.
    pub fn node_count(&self) -> usize {
        self.scanner.node_count()
    }
}

/// Fetch stream entries for a batch of keys using cluster connection.
async fn fetch_streams_cluster(
    conn: &mut ClusterConnection,
    keys: &[String],
    start_id: &str,
    end_id: &str,
    count: Option<usize>,
) -> Result<Vec<StreamData>> {
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
    fn test_cluster_stream_batch_iterator_creation() {
        let schema = StreamSchema::new();
        let config = BatchConfig::new("events:*");

        // This will fail to connect but tests the construction logic
        let result = ClusterStreamBatchIterator::new(&["redis://localhost:7000"], schema, config);
        // Expected to fail since we don't have a cluster running in unit tests
        assert!(result.is_err());
    }
}
