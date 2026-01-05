//! Cluster-aware batch iterator for Redis Cluster deployments.
//!
//! This module provides `ClusterHashBatchIterator` which scans all master nodes
//! in a Redis Cluster to discover keys, then fetches hash data using the
//! cluster connection's automatic routing.
//!
//! # Example
//!
//! ```ignore
//! use polars_redis::{ClusterHashBatchIterator, HashSchema, BatchConfig, RedisType};
//!
//! let schema = HashSchema::new(vec![
//!     ("name".to_string(), RedisType::Utf8),
//!     ("age".to_string(), RedisType::Int64),
//! ]);
//!
//! let config = BatchConfig::new("user:*").with_batch_size(1000);
//!
//! // Connect to cluster
//! let mut iterator = ClusterHashBatchIterator::new(
//!     &["redis://node1:7000", "redis://node2:7001"],
//!     schema,
//!     config,
//!     None,
//! )?;
//!
//! while let Some(batch) = iterator.next_batch()? {
//!     println!("Got {} rows", batch.num_rows());
//! }
//! ```

use arrow::array::RecordBatch;
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use tokio::runtime::Runtime;

use super::convert::hashes_to_record_batch;
use super::reader::{HashData, fetch_hashes_cluster};
use crate::cluster::DirectClusterKeyScanner;
use crate::error::{Error, Result};
use crate::schema::HashSchema;

use super::batch_iter::BatchConfig;

/// Cluster-aware iterator for scanning Redis hashes.
///
/// This iterator discovers keys by scanning all master nodes in the cluster,
/// then fetches hash data using the cluster connection which automatically
/// routes to the correct node for each key.
pub struct ClusterHashBatchIterator {
    /// Tokio runtime for async operations.
    runtime: Runtime,
    /// Cluster connection (cloneable, thread-safe).
    conn: ClusterConnection,
    /// Key scanner that iterates all cluster nodes.
    scanner: DirectClusterKeyScanner,
    /// Schema for the hash data.
    schema: HashSchema,
    /// Batch configuration.
    config: BatchConfig,
    /// Fields to fetch (None = all fields via HGETALL).
    projection: Option<Vec<String>>,
    /// Whether iteration is complete.
    done: bool,
    /// Total rows yielded so far.
    rows_yielded: usize,
    /// Buffer of keys waiting to be fetched.
    key_buffer: Vec<String>,
    /// Current row offset for row index column.
    row_offset: u64,
}

impl ClusterHashBatchIterator {
    /// Create a new cluster hash batch iterator.
    ///
    /// # Arguments
    /// * `nodes` - List of initial cluster node URLs (e.g., ["redis://node1:7000"])
    /// * `schema` - Schema defining the hash fields and types
    /// * `config` - Batch configuration (pattern, batch_size, etc.)
    /// * `projection` - Optional list of fields to fetch (projection pushdown)
    pub fn new(
        nodes: &[impl AsRef<str>],
        schema: HashSchema,
        config: BatchConfig,
        projection: Option<Vec<String>>,
    ) -> Result<Self> {
        let runtime = Runtime::new()
            .map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

        // Create cluster client and connection
        let node_urls: Vec<String> = nodes.iter().map(|s| s.as_ref().to_string()).collect();
        let cluster_client = ClusterClient::new(node_urls)
            .map_err(|e| Error::InvalidUrl(format!("cluster: {}", e)))?;

        let (conn, scanner) = runtime.block_on(async {
            let conn = cluster_client
                .get_async_connection()
                .await
                .map_err(Error::Connection)?;

            // Create scanner using a temporary connection
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
            projection,
            done: false,
            rows_yielded: 0,
            key_buffer: Vec::new(),
            row_offset: 0,
        })
    }

    /// Create from a single cluster URL.
    ///
    /// Convenience method when connecting via a single initial node.
    pub fn from_url(
        url: &str,
        schema: HashSchema,
        config: BatchConfig,
        projection: Option<Vec<String>>,
    ) -> Result<Self> {
        Self::new(&[url], schema, config, projection)
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
        while self.key_buffer.len() < self.config.batch_size && !self.scanner.is_done() {
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

        // Convert to RecordBatch with current row offset
        let mut batch = hashes_to_record_batch(&hash_data, &effective_schema, self.row_offset)?;

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

        // Check if we're done
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

    /// Fetch hash data for a batch of keys.
    fn fetch_batch(&mut self, keys: &[String]) -> Result<Vec<HashData>> {
        let include_ttl = self.schema.include_ttl();
        let worker_count = self.config.parallel.worker_count();

        // Use parallel fetching if configured
        if worker_count > 1 && keys.len() > worker_count {
            self.fetch_batch_parallel(keys, include_ttl, worker_count)
        } else {
            let projection = self.projection.as_deref();
            let mut conn = self.conn.clone();
            self.runtime.block_on(fetch_hashes_cluster(
                &mut conn,
                keys,
                projection,
                include_ttl,
            ))
        }
    }

    /// Fetch hash data in parallel using multiple workers.
    fn fetch_batch_parallel(
        &mut self,
        keys: &[String],
        include_ttl: bool,
        worker_count: usize,
    ) -> Result<Vec<HashData>> {
        // Split keys into chunks for parallel processing
        let chunk_size = keys.len().div_ceil(worker_count);
        let chunks: Vec<Vec<String>> = keys.chunks(chunk_size).map(|c| c.to_vec()).collect();

        let conn = self.conn.clone();
        let projection_owned: Option<Vec<String>> = self.projection.clone();

        self.runtime.block_on(async {
            let mut handles = Vec::with_capacity(chunks.len());

            for chunk in chunks {
                let mut conn = conn.clone();
                let proj = projection_owned.clone();

                let handle = tokio::spawn(async move {
                    fetch_hashes_cluster(&mut conn, &chunk, proj.as_deref(), include_ttl).await
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

    /// Get the number of cluster nodes being scanned.
    pub fn node_count(&self) -> usize {
        self.scanner.node_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_config_for_cluster() {
        let config = BatchConfig::new("user:*")
            .with_batch_size(500)
            .with_count_hint(200);

        assert_eq!(config.pattern, "user:*");
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.count_hint, 200);
    }
}
