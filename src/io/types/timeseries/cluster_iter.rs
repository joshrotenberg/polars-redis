//! Cluster-aware batch iterator for RedisTimeSeries data.

use arrow::array::RecordBatch;
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use tokio::runtime::Runtime;

use super::convert::{TimeSeriesSchema, timeseries_to_record_batch};
use super::reader::{TimeSeriesData, TimeSeriesSample};
use crate::cluster::DirectClusterKeyScanner;
use crate::error::{Error, Result};
use crate::io::types::hash::BatchConfig;

/// Cluster-aware iterator for scanning RedisTimeSeries.
pub struct ClusterTimeSeriesBatchIterator {
    /// Tokio runtime for async operations.
    runtime: Runtime,
    /// Cluster connection (cloneable, thread-safe).
    conn: ClusterConnection,
    /// Key scanner that iterates all cluster nodes.
    scanner: DirectClusterKeyScanner,
    /// Schema for the time series data.
    schema: TimeSeriesSchema,
    /// Batch configuration.
    config: BatchConfig,
    /// Start timestamp/ID for TS.RANGE (default: "-" for oldest).
    start: String,
    /// End timestamp/ID for TS.RANGE (default: "+" for newest).
    end: String,
    /// Maximum samples to fetch per time series.
    count_per_series: Option<usize>,
    /// Aggregation type (avg, sum, min, max, etc.).
    aggregation: Option<String>,
    /// Bucket size in milliseconds for aggregation.
    bucket_size_ms: Option<i64>,
    /// Whether iteration is complete.
    done: bool,
    /// Total rows (samples) yielded so far.
    rows_yielded: usize,
    /// Current row index offset.
    row_index_offset: u64,
    /// Buffer of keys waiting to be fetched.
    key_buffer: Vec<String>,
}

impl ClusterTimeSeriesBatchIterator {
    /// Create a new cluster time series batch iterator.
    pub fn new(
        nodes: &[impl AsRef<str>],
        schema: TimeSeriesSchema,
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
            start: "-".to_string(),
            end: "+".to_string(),
            count_per_series: None,
            aggregation: None,
            bucket_size_ms: None,
            done: false,
            rows_yielded: 0,
            row_index_offset: 0,
            key_buffer: Vec::new(),
        })
    }

    /// Set the start timestamp for TS.RANGE.
    pub fn with_start(mut self, start: &str) -> Self {
        self.start = start.to_string();
        self
    }

    /// Set the end timestamp for TS.RANGE.
    pub fn with_end(mut self, end: &str) -> Self {
        self.end = end.to_string();
        self
    }

    /// Set the maximum samples to fetch per time series.
    pub fn with_count_per_series(mut self, count: usize) -> Self {
        self.count_per_series = Some(count);
        self
    }

    /// Set aggregation type and bucket size.
    ///
    /// # Arguments
    /// * `agg_type` - Aggregation type: avg, sum, min, max, range, count, first, last, std.p, std.s, var.p, var.s
    /// * `bucket_size_ms` - Bucket size in milliseconds
    pub fn with_aggregation(mut self, agg_type: &str, bucket_size_ms: i64) -> Self {
        self.aggregation = Some(agg_type.to_string());
        self.bucket_size_ms = Some(bucket_size_ms);
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

        // For time series, we use a smaller key batch since each key can have many samples
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

        let ts_data = self.fetch_batch(&keys_to_fetch)?;

        if ts_data.is_empty() || ts_data.iter().all(|ts| ts.samples.is_empty()) {
            if self.key_buffer.is_empty() && self.scanner.is_done() {
                self.done = true;
                return Ok(None);
            }
            return self.next_batch();
        }

        let mut batch = timeseries_to_record_batch(&ts_data, &self.schema, self.row_index_offset)?;
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

    fn fetch_batch(&mut self, keys: &[String]) -> Result<Vec<TimeSeriesData>> {
        let start = self.start.clone();
        let end = self.end.clone();
        let count = self.count_per_series;
        let aggregation = self.aggregation.clone();
        let bucket_size_ms = self.bucket_size_ms;
        let mut conn = self.conn.clone();

        self.runtime.block_on(fetch_timeseries_cluster(
            &mut conn,
            keys,
            &start,
            &end,
            count,
            aggregation,
            bucket_size_ms,
        ))
    }

    /// Check if iteration is complete.
    pub fn is_done(&self) -> bool {
        self.done
    }

    /// Get the number of rows (samples) yielded so far.
    pub fn rows_yielded(&self) -> usize {
        self.rows_yielded
    }

    /// Get the number of cluster nodes being scanned.
    pub fn node_count(&self) -> usize {
        self.scanner.node_count()
    }
}

/// Fetch time series data for a batch of keys using cluster connection.
async fn fetch_timeseries_cluster(
    conn: &mut ClusterConnection,
    keys: &[String],
    start: &str,
    end: &str,
    count: Option<usize>,
    aggregation: Option<String>,
    bucket_size_ms: Option<i64>,
) -> Result<Vec<TimeSeriesData>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut results = Vec::with_capacity(keys.len());

    for key in keys {
        let mut cmd = redis::cmd("TS.RANGE");
        cmd.arg(key).arg(start).arg(end);

        if let Some(c) = count {
            cmd.arg("COUNT").arg(c);
        }

        if let (Some(agg_type), Some(bucket)) = (&aggregation, bucket_size_ms) {
            cmd.arg("AGGREGATION").arg(agg_type).arg(bucket);
        }

        // TS.RANGE returns an array of [timestamp, value] pairs
        let samples_raw: Vec<(i64, String)> = cmd.query_async(conn).await.unwrap_or_default();

        let samples: Vec<TimeSeriesSample> = samples_raw
            .into_iter()
            .filter_map(|(ts, val_str)| {
                val_str.parse::<f64>().ok().map(|value| TimeSeriesSample {
                    timestamp_ms: ts,
                    value,
                })
            })
            .collect();

        results.push(TimeSeriesData {
            key: key.clone(),
            labels: Vec::new(),
            samples,
        });
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_timeseries_batch_iterator_creation() {
        let schema = TimeSeriesSchema::new();
        let config = BatchConfig::new("sensor:*");

        // This will fail to connect but tests the construction logic
        let result =
            ClusterTimeSeriesBatchIterator::new(&["redis://localhost:7000"], schema, config);
        // Expected to fail since we don't have a cluster running in unit tests
        assert!(result.is_err());
    }
}
