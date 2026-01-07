//! Cluster-aware batch iterator for Redis set data.

use arrow::array::RecordBatch;
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use tokio::runtime::Runtime;

use super::convert::{SetSchema, sets_to_record_batch};
use super::reader::{SetData, fetch_sets_cluster};
use crate::cluster::DirectClusterKeyScanner;
use crate::error::{Error, Result};
use crate::io::types::hash::BatchConfig;

/// Cluster-aware iterator for scanning Redis sets.
pub struct ClusterSetBatchIterator {
    /// Tokio runtime for async operations.
    runtime: Runtime,
    /// Cluster connection (cloneable, thread-safe).
    conn: ClusterConnection,
    /// Key scanner that iterates all cluster nodes.
    scanner: DirectClusterKeyScanner,
    /// Schema for the set data.
    schema: SetSchema,
    /// Batch configuration.
    config: BatchConfig,
    /// Whether iteration is complete.
    done: bool,
    /// Total rows (members) yielded so far.
    rows_yielded: usize,
    /// Current row index offset.
    row_index_offset: u64,
    /// Buffer of keys waiting to be fetched.
    key_buffer: Vec<String>,
}

impl ClusterSetBatchIterator {
    /// Create a new cluster set batch iterator.
    pub fn new(nodes: &[impl AsRef<str>], schema: SetSchema, config: BatchConfig) -> Result<Self> {
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

        if let Some(max) = self.config.max_rows
            && self.rows_yielded >= max
        {
            self.done = true;
            return Ok(None);
        }

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

        let set_data = self.fetch_batch(&keys_to_fetch)?;

        if set_data.is_empty() {
            return self.next_batch();
        }

        let mut batch = sets_to_record_batch(&set_data, &self.schema, self.row_index_offset)?;
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

    fn fetch_batch(&mut self, keys: &[String]) -> Result<Vec<SetData>> {
        let mut conn = self.conn.clone();
        self.runtime.block_on(fetch_sets_cluster(&mut conn, keys))
    }

    pub fn is_done(&self) -> bool {
        self.done
    }

    pub fn rows_yielded(&self) -> usize {
        self.rows_yielded
    }

    pub fn node_count(&self) -> usize {
        self.scanner.node_count()
    }
}
