//! Batch iterator for streaming RedisTimeSeries data as Arrow RecordBatches.

use arrow::array::RecordBatch;
use redis::aio::ConnectionManager;
use tokio::runtime::Runtime;

use super::convert::{TimeSeriesSchema, timeseries_to_record_batch};
use super::reader::{TimeSeriesData, TimeSeriesSample};
use crate::connection::RedisConnection;
use crate::error::{Error, Result};
use crate::types::hash::BatchConfig;

/// Iterator for scanning RedisTimeSeries and yielding Arrow RecordBatches.
pub struct TimeSeriesBatchIterator {
    /// Tokio runtime for async operations.
    runtime: Runtime,
    /// Redis connection manager (cheap to clone, auto-reconnects).
    conn: ConnectionManager,
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
    /// Current SCAN cursor.
    cursor: u64,
    /// Whether we've started scanning.
    scan_started: bool,
    /// Whether we've completed the SCAN.
    done: bool,
    /// Total rows (samples) yielded so far.
    rows_yielded: usize,
    /// Current row index offset.
    row_index_offset: u64,
    /// Buffer of keys waiting to be fetched.
    key_buffer: Vec<String>,
}

impl TimeSeriesBatchIterator {
    /// Create a new TimeSeriesBatchIterator.
    ///
    /// # Arguments
    /// * `url` - Redis connection URL
    /// * `schema` - Schema configuration for the time series
    /// * `config` - Batch configuration (pattern, batch_size, etc.)
    pub fn new(url: &str, schema: TimeSeriesSchema, config: BatchConfig) -> Result<Self> {
        let runtime = Runtime::new()
            .map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;
        let connection = RedisConnection::new(url)?;
        let conn = runtime.block_on(connection.get_connection_manager())?;

        Ok(Self {
            runtime,
            conn,
            schema,
            config,
            start: "-".to_string(),
            end: "+".to_string(),
            count_per_series: None,
            aggregation: None,
            bucket_size_ms: None,
            cursor: 0,
            scan_started: false,
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

        // Check if we've hit the max rows limit
        if let Some(max) = self.config.max_rows
            && self.rows_yielded >= max
        {
            self.done = true;
            return Ok(None);
        }

        // Accumulate keys until we have enough for a batch
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

        // Fetch time series data
        let ts_data = self.fetch_batch(&keys_to_fetch)?;

        if ts_data.is_empty() || ts_data.iter().all(|ts| ts.samples.is_empty()) {
            // All keys were deleted, are not time series, or have no samples in range
            if self.key_buffer.is_empty() && self.scan_complete() {
                self.done = true;
                return Ok(None);
            }
            return self.next_batch();
        }

        // Convert to RecordBatch
        let mut batch = timeseries_to_record_batch(&ts_data, &self.schema, self.row_index_offset)?;

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

    /// Fetch time series data for a batch of keys.
    fn fetch_batch(&mut self, keys: &[String]) -> Result<Vec<TimeSeriesData>> {
        let start = self.start.clone();
        let end = self.end.clone();
        let count = self.count_per_series;
        let aggregation = self.aggregation.clone();
        let bucket_size_ms = self.bucket_size_ms;
        let mut conn = self.conn.clone();

        self.runtime.block_on(fetch_timeseries_async(
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

/// Fetch time series data for a batch of keys using async connection.
async fn fetch_timeseries_async(
    conn: &mut ConnectionManager,
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
    fn test_timeseries_batch_iterator_creation() {
        let schema = TimeSeriesSchema::new();
        let config = BatchConfig::new("sensor:*");

        let result = TimeSeriesBatchIterator::new("redis://localhost:6379", schema, config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_timeseries_batch_iterator_with_options() {
        let schema = TimeSeriesSchema::new()
            .with_key(true)
            .with_value_column_name("temperature")
            .with_row_index(true);

        let config = BatchConfig::new("sensor:*").with_batch_size(500);

        let result = TimeSeriesBatchIterator::new("redis://localhost:6379", schema, config);
        assert!(result.is_ok());

        let iter = result
            .unwrap()
            .with_start("1000")
            .with_end("2000")
            .with_count_per_series(100)
            .with_aggregation("avg", 60000);

        assert_eq!(iter.start, "1000");
        assert_eq!(iter.end, "2000");
        assert_eq!(iter.count_per_series, Some(100));
        assert_eq!(iter.aggregation, Some("avg".to_string()));
        assert_eq!(iter.bucket_size_ms, Some(60000));
    }
}
