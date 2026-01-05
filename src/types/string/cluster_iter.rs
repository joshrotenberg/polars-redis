//! Cluster-aware batch iterator for Redis string values.
//!
//! This module provides `ClusterStringBatchIterator` which scans all master nodes
//! in a Redis Cluster to discover keys, then fetches string values using the
//! cluster connection's automatic routing.

use arrow::array::RecordBatch;
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use tokio::runtime::Runtime;

use super::convert::{StringSchema, strings_to_record_batch};
use super::reader::{StringData, fetch_strings_cluster};
use crate::cluster::DirectClusterKeyScanner;
use crate::error::{Error, Result};
use crate::types::hash::BatchConfig;

/// Cluster-aware iterator for scanning Redis string values.
pub struct ClusterStringBatchIterator {
    /// Tokio runtime for async operations.
    runtime: Runtime,
    /// Cluster connection (cloneable, thread-safe).
    conn: ClusterConnection,
    /// Key scanner that iterates all cluster nodes.
    scanner: DirectClusterKeyScanner,
    /// Schema for the string data.
    schema: StringSchema,
    /// Batch configuration.
    config: BatchConfig,
    /// Whether iteration is complete.
    done: bool,
    /// Total rows yielded so far.
    rows_yielded: usize,
    /// Buffer of keys waiting to be fetched.
    key_buffer: Vec<String>,
}

impl ClusterStringBatchIterator {
    /// Create a new cluster string batch iterator.
    ///
    /// # Arguments
    /// * `nodes` - List of initial cluster node URLs
    /// * `schema` - Schema defining the value type
    /// * `config` - Batch configuration (pattern, batch_size, etc.)
    pub fn new(
        nodes: &[impl AsRef<str>],
        schema: StringSchema,
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
            done: false,
            rows_yielded: 0,
            key_buffer: Vec::new(),
        })
    }

    /// Create from a single cluster URL.
    pub fn from_url(url: &str, schema: StringSchema, config: BatchConfig) -> Result<Self> {
        Self::new(&[url], schema, config)
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

        while self.key_buffer.len() < self.config.batch_size && !self.scanner.is_done() {
            self.scan_more_keys()?;
        }

        if self.key_buffer.is_empty() {
            self.done = true;
            return Ok(None);
        }

        let keys_to_fetch: Vec<String> = self
            .key_buffer
            .drain(..self.key_buffer.len().min(self.config.batch_size))
            .collect();

        let string_data = self.fetch_batch(&keys_to_fetch)?;

        if string_data.is_empty() {
            return self.next_batch();
        }

        let mut batch = strings_to_record_batch(&string_data, &self.schema)?;

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

    /// Scan more keys from the cluster.
    fn scan_more_keys(&mut self) -> Result<()> {
        let keys = self.runtime.block_on(self.scanner.next_batch())?;
        if let Some(k) = keys {
            self.key_buffer.extend(k);
        }
        Ok(())
    }

    /// Fetch string data for a batch of keys.
    fn fetch_batch(&mut self, keys: &[String]) -> Result<Vec<StringData>> {
        let worker_count = self.config.parallel.worker_count();

        if worker_count > 1 && keys.len() > worker_count {
            self.fetch_batch_parallel(keys, worker_count)
        } else {
            let mut conn = self.conn.clone();
            self.runtime
                .block_on(fetch_strings_cluster(&mut conn, keys))
        }
    }

    /// Fetch string data in parallel using multiple workers.
    fn fetch_batch_parallel(
        &mut self,
        keys: &[String],
        worker_count: usize,
    ) -> Result<Vec<StringData>> {
        let chunk_size = keys.len().div_ceil(worker_count);
        let chunks: Vec<Vec<String>> = keys.chunks(chunk_size).map(|c| c.to_vec()).collect();

        let conn = self.conn.clone();

        self.runtime.block_on(async {
            let mut handles = Vec::with_capacity(chunks.len());

            for chunk in chunks {
                let mut conn = conn.clone();

                let handle =
                    tokio::spawn(async move { fetch_strings_cluster(&mut conn, &chunk).await });
                handles.push(handle);
            }

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

    /// Get the number of cluster nodes being scanned.
    pub fn node_count(&self) -> usize {
        self.scanner.node_count()
    }
}
