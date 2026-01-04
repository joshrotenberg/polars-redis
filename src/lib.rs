//! # polars-redis
//!
//! Query Redis like a database. Transform with Polars. Write back without ETL.
//!
//! This crate provides a Redis IO plugin for [Polars](https://pola.rs/), enabling you to
//! scan Redis data structures as Arrow RecordBatches with support for projection pushdown
//! and batched iteration.
//!
//! ## Supported Redis Types
//!
//! | Type | Read | Write | Description |
//! |------|------|-------|-------------|
//! | Hash | Yes | Yes | Field-level projection pushdown |
//! | JSON | Yes | Yes | RedisJSON documents |
//! | String | Yes | Yes | Simple key-value pairs |
//! | Set | Yes | Yes | Unique members |
//! | List | Yes | Yes | Ordered elements |
//! | Sorted Set | Yes | Yes | Members with scores |
//! | Stream | Yes | No | Timestamped entries |
//! | TimeSeries | Yes | No | Server-side aggregation |
//!
//! ## Quick Start
//!
//! ### Reading Hashes
//!
//! ```no_run
//! use polars_redis::{HashBatchIterator, HashSchema, BatchConfig, RedisType};
//!
//! // Define schema for hash fields
//! let schema = HashSchema::new(vec![
//!     ("name".to_string(), RedisType::Utf8),
//!     ("age".to_string(), RedisType::Int64),
//!     ("active".to_string(), RedisType::Boolean),
//! ])
//! .with_key(true)
//! .with_key_column_name("_key".to_string());
//!
//! // Configure batch iteration
//! let config = BatchConfig::new("user:*".to_string())
//!     .with_batch_size(1000)
//!     .with_count_hint(100);
//!
//! // Create iterator
//! let mut iterator = HashBatchIterator::new(
//!     "redis://localhost:6379",
//!     schema,
//!     config,
//!     None, // projection
//! ).unwrap();
//!
//! // Iterate over batches
//! while let Some(batch) = iterator.next_batch().unwrap() {
//!     println!("Got {} rows", batch.num_rows());
//! }
//! ```
//!
//! ### Writing Hashes
//!
//! ```no_run
//! use polars_redis::{write_hashes, WriteMode};
//!
//! let keys = vec!["user:1".to_string(), "user:2".to_string()];
//! let fields = vec!["name".to_string(), "age".to_string()];
//! let values = vec![
//!     vec![Some("Alice".to_string()), Some("30".to_string())],
//!     vec![Some("Bob".to_string()), Some("25".to_string())],
//! ];
//!
//! let result = write_hashes(
//!     "redis://localhost:6379",
//!     keys,
//!     fields,
//!     values,
//!     Some(3600), // TTL in seconds
//!     WriteMode::Replace,
//! ).unwrap();
//!
//! println!("Wrote {} keys", result.keys_written);
//! ```
//!
//! ### Schema Inference
//!
//! ```no_run
//! use polars_redis::infer_hash_schema;
//!
//! // Sample keys to infer schema
//! let schema = infer_hash_schema(
//!     "redis://localhost:6379",
//!     "user:*",
//!     100,  // sample size
//!     true, // type inference
//! ).unwrap();
//!
//! for (name, dtype) in schema.fields {
//!     println!("{}: {:?}", name, dtype);
//! }
//! ```
//!
//! ## Python Bindings
//!
//! This crate also provides Python bindings via PyO3 when built with the `python` feature.
//! The Python package `polars-redis` wraps these bindings with a high-level API.
//!
//! ## Features
//!
//! - `python` - Enable Python bindings (PyO3)
//! - `json` - Enable RedisJSON support (enabled by default)
//! - `search` - Enable RediSearch support (enabled by default)
//! - `cluster` - Enable Redis Cluster support

#[cfg(feature = "python")]
use arrow::datatypes::DataType;
#[cfg(feature = "python")]
use pyo3::prelude::*;

mod connection;
mod error;
mod infer;
mod scanner;
mod schema;
#[cfg(feature = "search")]
pub mod search;
mod types;
mod write;

pub use connection::RedisConnection;
pub use error::{Error, Result};
pub use infer::{InferredSchema, infer_hash_schema, infer_json_schema};
pub use schema::{HashSchema, RedisType};
pub use types::hash::{BatchConfig, HashBatchIterator};
#[cfg(feature = "search")]
pub use types::hash::{HashSearchIterator, SearchBatchConfig};
pub use types::json::{JsonBatchIterator, JsonSchema};
pub use types::list::{ListBatchIterator, ListSchema};
pub use types::set::{SetBatchIterator, SetSchema};
pub use types::stream::{StreamBatchIterator, StreamSchema};
pub use types::string::{StringBatchIterator, StringSchema};
pub use types::timeseries::{TimeSeriesBatchIterator, TimeSeriesSchema};
pub use types::zset::{ZSetBatchIterator, ZSetSchema};
pub use write::{
    WriteMode, WriteResult, write_hashes, write_json, write_lists, write_sets, write_strings,
    write_zsets,
};

/// Serialize an Arrow RecordBatch to IPC format bytes.
///
/// This is useful for passing data to Python or other Arrow consumers.
///
/// # Example
/// ```ignore
/// let batch = iterator.next_batch()?;
/// let ipc_bytes = polars_redis::batch_to_ipc(&batch)?;
/// // Send ipc_bytes to Python, which can read it with pl.read_ipc()
/// ```
pub fn batch_to_ipc(batch: &arrow::array::RecordBatch) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut writer = arrow::ipc::writer::FileWriter::try_new(&mut buf, batch.schema().as_ref())
            .map_err(|e| Error::Runtime(format!("Failed to create IPC writer: {}", e)))?;

        writer
            .write(batch)
            .map_err(|e| Error::Runtime(format!("Failed to write batch: {}", e)))?;

        writer
            .finish()
            .map_err(|e| Error::Runtime(format!("Failed to finish IPC: {}", e)))?;
    }
    Ok(buf)
}

// ============================================================================
// Python bindings (only when "python" feature is enabled)
// ============================================================================

#[cfg(feature = "python")]
/// Python module definition for polars_redis._internal
#[pymodule(name = "_internal")]
fn polars_redis_internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RedisScanner>()?;
    m.add_class::<PyHashBatchIterator>()?;
    m.add_class::<PyJsonBatchIterator>()?;
    m.add_class::<PyStringBatchIterator>()?;
    m.add_class::<PySetBatchIterator>()?;
    m.add_class::<PyListBatchIterator>()?;
    m.add_class::<PyZSetBatchIterator>()?;
    m.add_class::<PyStreamBatchIterator>()?;
    m.add_class::<PyTimeSeriesBatchIterator>()?;
    #[cfg(feature = "search")]
    m.add_class::<PyHashSearchIterator>()?;
    m.add_function(wrap_pyfunction!(scan_keys, m)?)?;
    m.add_function(wrap_pyfunction!(py_infer_hash_schema, m)?)?;
    m.add_function(wrap_pyfunction!(py_infer_json_schema, m)?)?;
    m.add_function(wrap_pyfunction!(py_write_hashes, m)?)?;
    m.add_function(wrap_pyfunction!(py_write_json, m)?)?;
    m.add_function(wrap_pyfunction!(py_write_strings, m)?)?;
    m.add_function(wrap_pyfunction!(py_write_sets, m)?)?;
    m.add_function(wrap_pyfunction!(py_write_lists, m)?)?;
    m.add_function(wrap_pyfunction!(py_write_zsets, m)?)?;
    Ok(())
}

#[cfg(feature = "python")]
/// Redis scanner that handles SCAN iteration and data fetching.
#[pyclass]
pub struct RedisScanner {
    connection_url: String,
    pattern: String,
    batch_size: usize,
    count_hint: usize,
}

#[cfg(feature = "python")]
#[pymethods]
impl RedisScanner {
    /// Create a new RedisScanner.
    #[new]
    #[pyo3(signature = (connection_url, pattern, batch_size = 1000, count_hint = 100))]
    fn new(connection_url: String, pattern: String, batch_size: usize, count_hint: usize) -> Self {
        Self {
            connection_url,
            pattern,
            batch_size,
            count_hint,
        }
    }

    #[getter]
    fn connection_url(&self) -> &str {
        &self.connection_url
    }

    #[getter]
    fn pattern(&self) -> &str {
        &self.pattern
    }

    #[getter]
    fn batch_size(&self) -> usize {
        self.batch_size
    }

    #[getter]
    fn count_hint(&self) -> usize {
        self.count_hint
    }
}

#[cfg(feature = "python")]
/// Python wrapper for HashBatchIterator.
///
/// This class is used by the Python IO plugin to iterate over Redis hash data
/// and yield Arrow RecordBatches.
#[pyclass]
pub struct PyHashBatchIterator {
    inner: HashBatchIterator,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyHashBatchIterator {
    /// Create a new PyHashBatchIterator.
    ///
    /// # Arguments
    /// * `url` - Redis connection URL
    /// * `pattern` - Key pattern to match
    /// * `schema` - List of (field_name, type_name) tuples
    /// * `batch_size` - Keys per batch
    /// * `count_hint` - SCAN COUNT hint
    /// * `projection` - Optional list of columns to fetch
    /// * `include_key` - Whether to include the Redis key as a column
    /// * `key_column_name` - Name of the key column
    /// * `include_ttl` - Whether to include the TTL as a column
    /// * `ttl_column_name` - Name of the TTL column
    /// * `include_row_index` - Whether to include the row index as a column
    /// * `row_index_column_name` - Name of the row index column
    /// * `max_rows` - Optional maximum rows to return
    #[new]
    #[pyo3(signature = (
        url,
        pattern,
        schema,
        batch_size = 1000,
        count_hint = 100,
        projection = None,
        include_key = true,
        key_column_name = "_key".to_string(),
        include_ttl = false,
        ttl_column_name = "_ttl".to_string(),
        include_row_index = false,
        row_index_column_name = "_index".to_string(),
        max_rows = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        url: String,
        pattern: String,
        schema: Vec<(String, String)>,
        batch_size: usize,
        count_hint: usize,
        projection: Option<Vec<String>>,
        include_key: bool,
        key_column_name: String,
        include_ttl: bool,
        ttl_column_name: String,
        include_row_index: bool,
        row_index_column_name: String,
        max_rows: Option<usize>,
    ) -> PyResult<Self> {
        // Parse schema from Python types
        let field_types: Vec<(String, RedisType)> = schema
            .into_iter()
            .map(|(name, type_str)| {
                let redis_type = match type_str.to_lowercase().as_str() {
                    "utf8" | "str" | "string" => RedisType::Utf8,
                    "int64" | "int" | "integer" => RedisType::Int64,
                    "float64" | "float" | "double" => RedisType::Float64,
                    "bool" | "boolean" => RedisType::Boolean,
                    _ => {
                        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                            "Unknown type '{}' for field '{}'. Supported: utf8, int64, float64, bool",
                            type_str, name
                        )));
                    }
                };
                Ok((name, redis_type))
            })
            .collect::<PyResult<Vec<_>>>()?;

        let hash_schema = HashSchema::new(field_types)
            .with_key(include_key)
            .with_key_column_name(key_column_name)
            .with_ttl(include_ttl)
            .with_ttl_column_name(ttl_column_name)
            .with_row_index(include_row_index)
            .with_row_index_column_name(row_index_column_name);

        let mut config = BatchConfig::new(pattern)
            .with_batch_size(batch_size)
            .with_count_hint(count_hint);

        if let Some(max) = max_rows {
            config = config.with_max_rows(max);
        }

        let inner = HashBatchIterator::new(&url, hash_schema, config, projection)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(Self { inner })
    }

    /// Get the next batch as Arrow IPC bytes.
    ///
    /// Returns None when iteration is complete.
    /// Returns the RecordBatch serialized as Arrow IPC format.
    fn next_batch_ipc(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let batch = self
            .inner
            .next_batch()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        match batch {
            Some(record_batch) => {
                // Serialize to Arrow IPC format
                let mut buf = Vec::new();
                {
                    let mut writer = arrow::ipc::writer::FileWriter::try_new(
                        &mut buf,
                        record_batch.schema().as_ref(),
                    )
                    .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to create IPC writer: {}",
                            e
                        ))
                    })?;

                    writer.write(&record_batch).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to write batch: {}",
                            e
                        ))
                    })?;

                    writer.finish().map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to finish IPC: {}",
                            e
                        ))
                    })?;
                }

                Ok(Some(pyo3::types::PyBytes::new(py, &buf).into()))
            }
            None => Ok(None),
        }
    }

    /// Check if iteration is complete.
    fn is_done(&self) -> bool {
        self.inner.is_done()
    }

    /// Get the number of rows yielded so far.
    fn rows_yielded(&self) -> usize {
        self.inner.rows_yielded()
    }
}

#[cfg(all(feature = "python", feature = "search"))]
/// Python wrapper for HashSearchIterator.
///
/// This class is used to iterate over RediSearch `FT.SEARCH` results
/// and yield Arrow RecordBatches, enabling server-side filtering.
#[pyclass]
pub struct PyHashSearchIterator {
    inner: HashSearchIterator,
}

#[cfg(all(feature = "python", feature = "search"))]
#[pymethods]
impl PyHashSearchIterator {
    /// Create a new PyHashSearchIterator.
    ///
    /// # Arguments
    /// * `url` - Redis connection URL
    /// * `index` - RediSearch index name
    /// * `query` - RediSearch query string (e.g., "@age:[30 +inf]", "*" for all)
    /// * `schema` - List of (field_name, type_name) tuples
    /// * `batch_size` - Documents per batch
    /// * `projection` - Optional list of columns to fetch
    /// * `include_key` - Whether to include the Redis key as a column
    /// * `key_column_name` - Name of the key column
    /// * `include_ttl` - Whether to include the TTL as a column
    /// * `ttl_column_name` - Name of the TTL column
    /// * `include_row_index` - Whether to include the row index as a column
    /// * `row_index_column_name` - Name of the row index column
    /// * `max_rows` - Optional maximum rows to return
    /// * `sort_by` - Optional field to sort by
    /// * `sort_ascending` - Sort direction (default: true)
    #[new]
    #[pyo3(signature = (
        url,
        index,
        query,
        schema,
        batch_size = 1000,
        projection = None,
        include_key = true,
        key_column_name = "_key".to_string(),
        include_ttl = false,
        ttl_column_name = "_ttl".to_string(),
        include_row_index = false,
        row_index_column_name = "_index".to_string(),
        max_rows = None,
        sort_by = None,
        sort_ascending = true
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        url: String,
        index: String,
        query: String,
        schema: Vec<(String, String)>,
        batch_size: usize,
        projection: Option<Vec<String>>,
        include_key: bool,
        key_column_name: String,
        include_ttl: bool,
        ttl_column_name: String,
        include_row_index: bool,
        row_index_column_name: String,
        max_rows: Option<usize>,
        sort_by: Option<String>,
        sort_ascending: bool,
    ) -> PyResult<Self> {
        // Parse schema from Python types
        let field_types: Vec<(String, RedisType)> = schema
            .into_iter()
            .map(|(name, type_str)| {
                let redis_type = match type_str.to_lowercase().as_str() {
                    "utf8" | "str" | "string" => RedisType::Utf8,
                    "int64" | "int" | "integer" => RedisType::Int64,
                    "float64" | "float" | "double" => RedisType::Float64,
                    "bool" | "boolean" => RedisType::Boolean,
                    _ => {
                        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                            "Unknown type '{}' for field '{}'. Supported: utf8, int64, float64, bool",
                            type_str, name
                        )));
                    }
                };
                Ok((name, redis_type))
            })
            .collect::<PyResult<Vec<_>>>()?;

        let hash_schema = HashSchema::new(field_types)
            .with_key(include_key)
            .with_key_column_name(key_column_name)
            .with_ttl(include_ttl)
            .with_ttl_column_name(ttl_column_name)
            .with_row_index(include_row_index)
            .with_row_index_column_name(row_index_column_name);

        let mut config = SearchBatchConfig::new(index, query).with_batch_size(batch_size);

        if let Some(max) = max_rows {
            config = config.with_max_rows(max);
        }

        if let Some(field) = sort_by {
            config = config.with_sort_by(field, sort_ascending);
        }

        let inner = HashSearchIterator::new(&url, hash_schema, config, projection)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(Self { inner })
    }

    /// Get the next batch as Arrow IPC bytes.
    ///
    /// Returns None when iteration is complete.
    /// Returns the RecordBatch serialized as Arrow IPC format.
    fn next_batch_ipc(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let batch = self
            .inner
            .next_batch()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        match batch {
            Some(record_batch) => {
                // Serialize to Arrow IPC format
                let mut buf = Vec::new();
                {
                    let mut writer = arrow::ipc::writer::FileWriter::try_new(
                        &mut buf,
                        record_batch.schema().as_ref(),
                    )
                    .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to create IPC writer: {}",
                            e
                        ))
                    })?;

                    writer.write(&record_batch).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to write batch: {}",
                            e
                        ))
                    })?;

                    writer.finish().map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to finish IPC: {}",
                            e
                        ))
                    })?;
                }

                Ok(Some(pyo3::types::PyBytes::new(py, &buf).into()))
            }
            None => Ok(None),
        }
    }

    /// Check if iteration is complete.
    fn is_done(&self) -> bool {
        self.inner.is_done()
    }

    /// Get the number of rows yielded so far.
    fn rows_yielded(&self) -> usize {
        self.inner.rows_yielded()
    }

    /// Get the total number of matching documents (available after first batch).
    fn total_results(&self) -> Option<usize> {
        self.inner.total_results()
    }
}

#[cfg(feature = "python")]
/// Python wrapper for TimeSeriesBatchIterator.
///
/// This class is used by the Python IO plugin to iterate over RedisTimeSeries data
/// and yield Arrow RecordBatches.
#[pyclass]
pub struct PyTimeSeriesBatchIterator {
    inner: TimeSeriesBatchIterator,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyTimeSeriesBatchIterator {
    /// Create a new PyTimeSeriesBatchIterator.
    ///
    /// # Arguments
    /// * `url` - Redis connection URL
    /// * `pattern` - Key pattern to match
    /// * `batch_size` - Keys per batch
    /// * `count_hint` - SCAN COUNT hint
    /// * `start` - Start timestamp for TS.RANGE (default: "-" for oldest)
    /// * `end` - End timestamp for TS.RANGE (default: "+" for newest)
    /// * `count_per_series` - Max samples per time series (optional)
    /// * `aggregation` - Aggregation type (avg, sum, min, max, etc.)
    /// * `bucket_size_ms` - Bucket size in milliseconds for aggregation
    /// * `include_key` - Whether to include the Redis key as a column
    /// * `key_column_name` - Name of the key column
    /// * `include_timestamp` - Whether to include the timestamp as a column
    /// * `timestamp_column_name` - Name of the timestamp column
    /// * `value_column_name` - Name of the value column
    /// * `include_row_index` - Whether to include the row index as a column
    /// * `row_index_column_name` - Name of the row index column
    /// * `label_columns` - Label names to include as columns
    /// * `max_rows` - Optional maximum rows to return
    #[new]
    #[pyo3(signature = (
        url,
        pattern,
        batch_size = 1000,
        count_hint = 100,
        start = "-".to_string(),
        end = "+".to_string(),
        count_per_series = None,
        aggregation = None,
        bucket_size_ms = None,
        include_key = true,
        key_column_name = "_key".to_string(),
        include_timestamp = true,
        timestamp_column_name = "_ts".to_string(),
        value_column_name = "value".to_string(),
        include_row_index = false,
        row_index_column_name = "_index".to_string(),
        label_columns = vec![],
        max_rows = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        url: String,
        pattern: String,
        batch_size: usize,
        count_hint: usize,
        start: String,
        end: String,
        count_per_series: Option<usize>,
        aggregation: Option<String>,
        bucket_size_ms: Option<i64>,
        include_key: bool,
        key_column_name: String,
        include_timestamp: bool,
        timestamp_column_name: String,
        value_column_name: String,
        include_row_index: bool,
        row_index_column_name: String,
        label_columns: Vec<String>,
        max_rows: Option<usize>,
    ) -> PyResult<Self> {
        let ts_schema = TimeSeriesSchema::new()
            .with_key(include_key)
            .with_key_column_name(&key_column_name)
            .with_timestamp(include_timestamp)
            .with_timestamp_column_name(&timestamp_column_name)
            .with_value_column_name(&value_column_name)
            .with_row_index(include_row_index)
            .with_row_index_column_name(&row_index_column_name)
            .with_label_columns(label_columns);

        let mut config = types::hash::BatchConfig::new(pattern)
            .with_batch_size(batch_size)
            .with_count_hint(count_hint);

        if let Some(max) = max_rows {
            config = config.with_max_rows(max);
        }

        let mut inner = TimeSeriesBatchIterator::new(&url, ts_schema, config)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        inner = inner.with_start(&start).with_end(&end);

        if let Some(count) = count_per_series {
            inner = inner.with_count_per_series(count);
        }

        if let (Some(agg), Some(bucket)) = (aggregation, bucket_size_ms) {
            inner = inner.with_aggregation(&agg, bucket);
        }

        Ok(Self { inner })
    }

    /// Get the next batch as Arrow IPC bytes.
    ///
    /// Returns None when iteration is complete.
    fn next_batch_ipc(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let batch = self
            .inner
            .next_batch()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        match batch {
            Some(record_batch) => {
                let mut buf = Vec::new();
                {
                    let mut writer = arrow::ipc::writer::FileWriter::try_new(
                        &mut buf,
                        record_batch.schema().as_ref(),
                    )
                    .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to create IPC writer: {}",
                            e
                        ))
                    })?;

                    writer.write(&record_batch).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to write batch: {}",
                            e
                        ))
                    })?;

                    writer.finish().map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to finish IPC: {}",
                            e
                        ))
                    })?;
                }

                Ok(Some(pyo3::types::PyBytes::new(py, &buf).into()))
            }
            None => Ok(None),
        }
    }

    /// Check if iteration is complete.
    fn is_done(&self) -> bool {
        self.inner.is_done()
    }

    /// Get the number of rows yielded so far.
    fn rows_yielded(&self) -> usize {
        self.inner.rows_yielded()
    }
}

#[cfg(feature = "python")]
/// Python wrapper for JsonBatchIterator.
///
/// This class is used by the Python IO plugin to iterate over Redis JSON data
/// and yield Arrow RecordBatches.
#[pyclass]
pub struct PyJsonBatchIterator {
    inner: JsonBatchIterator,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyJsonBatchIterator {
    /// Create a new PyJsonBatchIterator.
    ///
    /// # Arguments
    /// * `url` - Redis connection URL
    /// * `pattern` - Key pattern to match
    /// * `schema` - List of (field_name, type_name) tuples
    /// * `batch_size` - Keys per batch
    /// * `count_hint` - SCAN COUNT hint
    /// * `projection` - Optional list of columns to fetch
    /// * `include_key` - Whether to include the Redis key as a column
    /// * `key_column_name` - Name of the key column
    /// * `include_ttl` - Whether to include the TTL as a column
    /// * `ttl_column_name` - Name of the TTL column
    /// * `include_row_index` - Whether to include the row index as a column
    /// * `row_index_column_name` - Name of the row index column
    /// * `max_rows` - Optional maximum rows to return
    #[new]
    #[pyo3(signature = (
        url,
        pattern,
        schema,
        batch_size = 1000,
        count_hint = 100,
        projection = None,
        include_key = true,
        key_column_name = "_key".to_string(),
        include_ttl = false,
        ttl_column_name = "_ttl".to_string(),
        include_row_index = false,
        row_index_column_name = "_index".to_string(),
        max_rows = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        url: String,
        pattern: String,
        schema: Vec<(String, String)>,
        batch_size: usize,
        count_hint: usize,
        projection: Option<Vec<String>>,
        include_key: bool,
        key_column_name: String,
        include_ttl: bool,
        ttl_column_name: String,
        include_row_index: bool,
        row_index_column_name: String,
        max_rows: Option<usize>,
    ) -> PyResult<Self> {
        // Parse schema from Python type strings to Arrow DataTypes
        let field_types: Vec<(String, DataType)> = schema
            .into_iter()
            .map(|(name, type_str)| {
                let dtype = match type_str.to_lowercase().as_str() {
                    "utf8" | "str" | "string" => DataType::Utf8,
                    "int64" | "int" | "integer" => DataType::Int64,
                    "float64" | "float" | "double" => DataType::Float64,
                    "bool" | "boolean" => DataType::Boolean,
                    _ => {
                        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                            "Unknown type '{}' for field '{}'. Supported: utf8, int64, float64, bool",
                            type_str, name
                        )));
                    }
                };
                Ok((name, dtype))
            })
            .collect::<PyResult<Vec<_>>>()?;

        let json_schema = JsonSchema::new(field_types)
            .with_key(include_key)
            .with_key_column_name(key_column_name)
            .with_ttl(include_ttl)
            .with_ttl_column_name(ttl_column_name)
            .with_row_index(include_row_index)
            .with_row_index_column_name(row_index_column_name);

        let mut config = BatchConfig::new(pattern)
            .with_batch_size(batch_size)
            .with_count_hint(count_hint);

        if let Some(max) = max_rows {
            config = config.with_max_rows(max);
        }

        let inner = JsonBatchIterator::new(&url, json_schema, config, projection)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(Self { inner })
    }

    /// Get the next batch as Arrow IPC bytes.
    ///
    /// Returns None when iteration is complete.
    /// Returns the RecordBatch serialized as Arrow IPC format.
    fn next_batch_ipc(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let batch = self
            .inner
            .next_batch()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        match batch {
            Some(record_batch) => {
                // Serialize to Arrow IPC format
                let mut buf = Vec::new();
                {
                    let mut writer = arrow::ipc::writer::FileWriter::try_new(
                        &mut buf,
                        record_batch.schema().as_ref(),
                    )
                    .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to create IPC writer: {}",
                            e
                        ))
                    })?;

                    writer.write(&record_batch).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to write batch: {}",
                            e
                        ))
                    })?;

                    writer.finish().map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to finish IPC: {}",
                            e
                        ))
                    })?;
                }

                Ok(Some(pyo3::types::PyBytes::new(py, &buf).into()))
            }
            None => Ok(None),
        }
    }

    /// Check if iteration is complete.
    fn is_done(&self) -> bool {
        self.inner.is_done()
    }

    /// Get the number of rows yielded so far.
    fn rows_yielded(&self) -> usize {
        self.inner.rows_yielded()
    }
}

#[cfg(feature = "python")]
/// Python wrapper for StringBatchIterator.
///
/// This class is used by the Python IO plugin to iterate over Redis string data
/// and yield Arrow RecordBatches.
#[pyclass]
pub struct PyStringBatchIterator {
    inner: StringBatchIterator,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyStringBatchIterator {
    /// Create a new PyStringBatchIterator.
    ///
    /// # Arguments
    /// * `url` - Redis connection URL
    /// * `pattern` - Key pattern to match
    /// * `value_type` - Type string for value column (utf8, int64, float64, bool, date, datetime)
    /// * `batch_size` - Keys per batch
    /// * `count_hint` - SCAN COUNT hint
    /// * `include_key` - Whether to include the Redis key as a column
    /// * `key_column_name` - Name of the key column
    /// * `value_column_name` - Name of the value column
    /// * `max_rows` - Optional maximum rows to return
    #[new]
    #[pyo3(signature = (
        url,
        pattern,
        value_type = "utf8".to_string(),
        batch_size = 1000,
        count_hint = 100,
        include_key = true,
        key_column_name = "_key".to_string(),
        value_column_name = "value".to_string(),
        max_rows = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        url: String,
        pattern: String,
        value_type: String,
        batch_size: usize,
        count_hint: usize,
        include_key: bool,
        key_column_name: String,
        value_column_name: String,
        max_rows: Option<usize>,
    ) -> PyResult<Self> {
        use arrow::datatypes::TimeUnit;

        // Parse value type from Python type string
        let dtype = match value_type.to_lowercase().as_str() {
            "utf8" | "str" | "string" => DataType::Utf8,
            "int64" | "int" | "integer" => DataType::Int64,
            "float64" | "float" | "double" => DataType::Float64,
            "bool" | "boolean" => DataType::Boolean,
            "date" => DataType::Date32,
            "datetime" => DataType::Timestamp(TimeUnit::Microsecond, None),
            _ => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                    "Unknown value type '{}'. Supported: utf8, int64, float64, bool, date, datetime",
                    value_type
                )));
            }
        };

        let string_schema = StringSchema::new(dtype)
            .with_key(include_key)
            .with_key_column_name(key_column_name)
            .with_value_column_name(value_column_name);

        let mut config = BatchConfig::new(pattern)
            .with_batch_size(batch_size)
            .with_count_hint(count_hint);

        if let Some(max) = max_rows {
            config = config.with_max_rows(max);
        }

        let inner = StringBatchIterator::new(&url, string_schema, config)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(Self { inner })
    }

    /// Get the next batch as Arrow IPC bytes.
    ///
    /// Returns None when iteration is complete.
    /// Returns the RecordBatch serialized as Arrow IPC format.
    fn next_batch_ipc(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let batch = self
            .inner
            .next_batch()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        match batch {
            Some(record_batch) => {
                // Serialize to Arrow IPC format
                let mut buf = Vec::new();
                {
                    let mut writer = arrow::ipc::writer::FileWriter::try_new(
                        &mut buf,
                        record_batch.schema().as_ref(),
                    )
                    .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to create IPC writer: {}",
                            e
                        ))
                    })?;

                    writer.write(&record_batch).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to write batch: {}",
                            e
                        ))
                    })?;

                    writer.finish().map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to finish IPC: {}",
                            e
                        ))
                    })?;
                }

                Ok(Some(pyo3::types::PyBytes::new(py, &buf).into()))
            }
            None => Ok(None),
        }
    }

    /// Check if iteration is complete.
    fn is_done(&self) -> bool {
        self.inner.is_done()
    }

    /// Get the number of rows yielded so far.
    fn rows_yielded(&self) -> usize {
        self.inner.rows_yielded()
    }
}

#[cfg(feature = "python")]
/// Scan Redis keys matching a pattern (for testing connectivity).
#[pyfunction]
#[pyo3(signature = (connection_url, pattern, count = 10))]
fn scan_keys(connection_url: &str, pattern: &str, count: usize) -> PyResult<Vec<String>> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    rt.block_on(async {
        let client = redis::Client::open(connection_url)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyConnectionError, _>(e.to_string()))?;

        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyConnectionError, _>(e.to_string()))?;

        let mut keys: Vec<String> = Vec::new();
        let mut cursor = 0u64;

        loop {
            let (new_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(count)
                .query_async(&mut conn)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

            keys.extend(batch);
            cursor = new_cursor;

            if cursor == 0 || keys.len() >= count {
                break;
            }
        }

        keys.truncate(count);
        Ok(keys)
    })
}

#[cfg(feature = "python")]
/// Infer schema from Redis hashes by sampling keys.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `pattern` - Key pattern to match
/// * `sample_size` - Maximum number of keys to sample (default: 100)
/// * `type_inference` - Whether to infer types (default: true)
///
/// # Returns
/// A tuple of (fields, sample_count) where fields is a list of (name, type) tuples.
#[pyfunction]
#[pyo3(signature = (url, pattern, sample_size = 100, type_inference = true))]
fn py_infer_hash_schema(
    url: &str,
    pattern: &str,
    sample_size: usize,
    type_inference: bool,
) -> PyResult<(Vec<(String, String)>, usize)> {
    let schema = infer_hash_schema(url, pattern, sample_size, type_inference)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Ok((schema.to_type_strings(), schema.sample_count))
}

#[cfg(feature = "python")]
/// Infer schema from RedisJSON documents by sampling keys.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `pattern` - Key pattern to match
/// * `sample_size` - Maximum number of keys to sample (default: 100)
///
/// # Returns
/// A tuple of (fields, sample_count) where fields is a list of (name, type) tuples.
#[pyfunction]
#[pyo3(signature = (url, pattern, sample_size = 100))]
fn py_infer_json_schema(
    url: &str,
    pattern: &str,
    sample_size: usize,
) -> PyResult<(Vec<(String, String)>, usize)> {
    let schema = infer_json_schema(url, pattern, sample_size)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Ok((schema.to_type_strings(), schema.sample_count))
}

#[cfg(feature = "python")]
/// Write hashes to Redis.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - List of Redis keys to write to
/// * `fields` - List of field names
/// * `values` - 2D list of values (rows x columns), same order as fields
/// * `ttl` - Optional TTL in seconds for each key
/// * `if_exists` - How to handle existing keys: "fail", "replace", or "append"
///
/// # Returns
/// A tuple of (keys_written, keys_failed, keys_skipped).
#[pyfunction]
#[pyo3(signature = (url, keys, fields, values, ttl = None, if_exists = "replace".to_string()))]
fn py_write_hashes(
    url: &str,
    keys: Vec<String>,
    fields: Vec<String>,
    values: Vec<Vec<Option<String>>>,
    ttl: Option<i64>,
    if_exists: String,
) -> PyResult<(usize, usize, usize)> {
    let mode: WriteMode = if_exists.parse().map_err(|e: crate::Error| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
    })?;

    let result = write_hashes(url, keys, fields, values, ttl, mode)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Ok((result.keys_written, result.keys_failed, result.keys_skipped))
}

#[cfg(feature = "python")]
/// Python wrapper for ListBatchIterator.
///
/// This class is used by the Python IO plugin to iterate over Redis list data
/// and yield Arrow RecordBatches.
#[pyclass]
pub struct PyListBatchIterator {
    inner: ListBatchIterator,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyListBatchIterator {
    /// Create a new PyListBatchIterator.
    ///
    /// # Arguments
    /// * `url` - Redis connection URL
    /// * `pattern` - Key pattern to match
    /// * `batch_size` - Keys per batch
    /// * `count_hint` - SCAN COUNT hint
    /// * `include_key` - Whether to include the Redis key as a column
    /// * `key_column_name` - Name of the key column
    /// * `element_column_name` - Name of the element column
    /// * `include_position` - Whether to include position index
    /// * `position_column_name` - Name of the position column
    /// * `include_row_index` - Whether to include the row index as a column
    /// * `row_index_column_name` - Name of the row index column
    /// * `max_rows` - Optional maximum rows to return
    #[new]
    #[pyo3(signature = (
        url,
        pattern,
        batch_size = 1000,
        count_hint = 100,
        include_key = true,
        key_column_name = "_key".to_string(),
        element_column_name = "element".to_string(),
        include_position = false,
        position_column_name = "position".to_string(),
        include_row_index = false,
        row_index_column_name = "_index".to_string(),
        max_rows = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        url: String,
        pattern: String,
        batch_size: usize,
        count_hint: usize,
        include_key: bool,
        key_column_name: String,
        element_column_name: String,
        include_position: bool,
        position_column_name: String,
        include_row_index: bool,
        row_index_column_name: String,
        max_rows: Option<usize>,
    ) -> PyResult<Self> {
        let list_schema = ListSchema::new()
            .with_key(include_key)
            .with_key_column_name(&key_column_name)
            .with_element_column_name(&element_column_name)
            .with_position(include_position)
            .with_position_column_name(&position_column_name)
            .with_row_index(include_row_index)
            .with_row_index_column_name(&row_index_column_name);

        let mut config = types::hash::BatchConfig::new(pattern)
            .with_batch_size(batch_size)
            .with_count_hint(count_hint);

        if let Some(max) = max_rows {
            config = config.with_max_rows(max);
        }

        let inner = ListBatchIterator::new(&url, list_schema, config)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(Self { inner })
    }

    /// Get the next batch as Arrow IPC bytes.
    fn next_batch_ipc(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let batch = self
            .inner
            .next_batch()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        match batch {
            Some(record_batch) => {
                let mut buf = Vec::new();
                {
                    let mut writer = arrow::ipc::writer::FileWriter::try_new(
                        &mut buf,
                        record_batch.schema().as_ref(),
                    )
                    .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to create IPC writer: {}",
                            e
                        ))
                    })?;

                    writer.write(&record_batch).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to write batch: {}",
                            e
                        ))
                    })?;

                    writer.finish().map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to finish IPC: {}",
                            e
                        ))
                    })?;
                }

                Ok(Some(pyo3::types::PyBytes::new(py, &buf).into()))
            }
            None => Ok(None),
        }
    }

    /// Check if iteration is complete.
    fn is_done(&self) -> bool {
        self.inner.is_done()
    }

    /// Get the number of rows yielded so far.
    fn rows_yielded(&self) -> usize {
        self.inner.rows_yielded()
    }
}

#[cfg(feature = "python")]
/// Write list elements to Redis.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - List of Redis keys to write to
/// * `elements` - 2D list of elements for each list
/// * `ttl` - Optional TTL in seconds for each key
/// * `if_exists` - How to handle existing keys: "fail", "replace", or "append"
///
/// # Returns
/// A tuple of (keys_written, keys_failed, keys_skipped).
#[pyfunction]
#[pyo3(signature = (url, keys, elements, ttl = None, if_exists = "replace".to_string()))]
fn py_write_lists(
    url: &str,
    keys: Vec<String>,
    elements: Vec<Vec<String>>,
    ttl: Option<i64>,
    if_exists: String,
) -> PyResult<(usize, usize, usize)> {
    let mode: WriteMode = if_exists.parse().map_err(|e: crate::Error| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
    })?;

    let result = write_lists(url, keys, elements, ttl, mode)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Ok((result.keys_written, result.keys_failed, result.keys_skipped))
}

#[cfg(feature = "python")]
/// Write JSON documents to Redis.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - List of Redis keys to write to
/// * `json_strings` - List of JSON strings to write
/// * `ttl` - Optional TTL in seconds for each key
/// * `if_exists` - How to handle existing keys: "fail", "replace", or "append"
///
/// # Returns
/// A tuple of (keys_written, keys_failed, keys_skipped).
#[pyfunction]
#[pyo3(signature = (url, keys, json_strings, ttl = None, if_exists = "replace".to_string()))]
fn py_write_json(
    url: &str,
    keys: Vec<String>,
    json_strings: Vec<String>,
    ttl: Option<i64>,
    if_exists: String,
) -> PyResult<(usize, usize, usize)> {
    let mode: WriteMode = if_exists.parse().map_err(|e: crate::Error| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
    })?;

    let result = write_json(url, keys, json_strings, ttl, mode)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Ok((result.keys_written, result.keys_failed, result.keys_skipped))
}

#[cfg(feature = "python")]
/// Write string values to Redis.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - List of Redis keys to write to
/// * `values` - List of string values to write (None for null)
/// * `ttl` - Optional TTL in seconds for each key
/// * `if_exists` - How to handle existing keys: "fail", "replace", or "append"
///
/// # Returns
/// A tuple of (keys_written, keys_failed, keys_skipped).
#[pyfunction]
#[pyo3(signature = (url, keys, values, ttl = None, if_exists = "replace".to_string()))]
fn py_write_strings(
    url: &str,
    keys: Vec<String>,
    values: Vec<Option<String>>,
    ttl: Option<i64>,
    if_exists: String,
) -> PyResult<(usize, usize, usize)> {
    let mode: WriteMode = if_exists.parse().map_err(|e: crate::Error| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
    })?;

    let result = write_strings(url, keys, values, ttl, mode)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Ok((result.keys_written, result.keys_failed, result.keys_skipped))
}

#[cfg(feature = "python")]
/// Python wrapper for SetBatchIterator.
///
/// This class is used by the Python IO plugin to iterate over Redis set data
/// and yield Arrow RecordBatches.
#[pyclass]
pub struct PySetBatchIterator {
    inner: SetBatchIterator,
}

#[cfg(feature = "python")]
#[pymethods]
impl PySetBatchIterator {
    /// Create a new PySetBatchIterator.
    ///
    /// # Arguments
    /// * `url` - Redis connection URL
    /// * `pattern` - Key pattern to match
    /// * `batch_size` - Keys per batch
    /// * `count_hint` - SCAN COUNT hint
    /// * `include_key` - Whether to include the Redis key as a column
    /// * `key_column_name` - Name of the key column
    /// * `member_column_name` - Name of the member column
    /// * `include_row_index` - Whether to include the row index as a column
    /// * `row_index_column_name` - Name of the row index column
    /// * `max_rows` - Optional maximum rows to return
    #[new]
    #[pyo3(signature = (
        url,
        pattern,
        batch_size = 1000,
        count_hint = 100,
        include_key = true,
        key_column_name = "_key".to_string(),
        member_column_name = "member".to_string(),
        include_row_index = false,
        row_index_column_name = "_index".to_string(),
        max_rows = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        url: String,
        pattern: String,
        batch_size: usize,
        count_hint: usize,
        include_key: bool,
        key_column_name: String,
        member_column_name: String,
        include_row_index: bool,
        row_index_column_name: String,
        max_rows: Option<usize>,
    ) -> PyResult<Self> {
        let set_schema = SetSchema::new()
            .with_key(include_key)
            .with_key_column_name(&key_column_name)
            .with_member_column_name(&member_column_name)
            .with_row_index(include_row_index)
            .with_row_index_column_name(&row_index_column_name);

        let mut config = types::hash::BatchConfig::new(pattern)
            .with_batch_size(batch_size)
            .with_count_hint(count_hint);

        if let Some(max) = max_rows {
            config = config.with_max_rows(max);
        }

        let inner = SetBatchIterator::new(&url, set_schema, config)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(Self { inner })
    }

    /// Get the next batch as Arrow IPC bytes.
    ///
    /// Returns None when iteration is complete.
    fn next_batch_ipc(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let batch = self
            .inner
            .next_batch()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        match batch {
            Some(record_batch) => {
                let mut buf = Vec::new();
                {
                    let mut writer = arrow::ipc::writer::FileWriter::try_new(
                        &mut buf,
                        record_batch.schema().as_ref(),
                    )
                    .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to create IPC writer: {}",
                            e
                        ))
                    })?;

                    writer.write(&record_batch).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to write batch: {}",
                            e
                        ))
                    })?;

                    writer.finish().map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to finish IPC: {}",
                            e
                        ))
                    })?;
                }

                Ok(Some(pyo3::types::PyBytes::new(py, &buf).into()))
            }
            None => Ok(None),
        }
    }

    /// Check if iteration is complete.
    fn is_done(&self) -> bool {
        self.inner.is_done()
    }

    /// Get the number of rows yielded so far.
    fn rows_yielded(&self) -> usize {
        self.inner.rows_yielded()
    }
}

#[cfg(feature = "python")]
/// Write set members to Redis.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - List of Redis keys to write to
/// * `members` - 2D list of members for each set
/// * `ttl` - Optional TTL in seconds for each key
/// * `if_exists` - How to handle existing keys: "fail", "replace", or "append"
///
/// # Returns
/// A tuple of (keys_written, keys_failed, keys_skipped).
#[pyfunction]
#[pyo3(signature = (url, keys, members, ttl = None, if_exists = "replace".to_string()))]
fn py_write_sets(
    url: &str,
    keys: Vec<String>,
    members: Vec<Vec<String>>,
    ttl: Option<i64>,
    if_exists: String,
) -> PyResult<(usize, usize, usize)> {
    let mode: WriteMode = if_exists.parse().map_err(|e: crate::Error| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
    })?;

    let result = write_sets(url, keys, members, ttl, mode)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Ok((result.keys_written, result.keys_failed, result.keys_skipped))
}

#[cfg(feature = "python")]
/// Python wrapper for ZSetBatchIterator.
///
/// This class is used by the Python IO plugin to iterate over Redis sorted set data
/// and yield Arrow RecordBatches.
#[pyclass]
pub struct PyZSetBatchIterator {
    inner: ZSetBatchIterator,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyZSetBatchIterator {
    /// Create a new PyZSetBatchIterator.
    ///
    /// # Arguments
    /// * `url` - Redis connection URL
    /// * `pattern` - Key pattern to match
    /// * `batch_size` - Keys per batch
    /// * `count_hint` - SCAN COUNT hint
    /// * `include_key` - Whether to include the Redis key as a column
    /// * `key_column_name` - Name of the key column
    /// * `member_column_name` - Name of the member column
    /// * `score_column_name` - Name of the score column
    /// * `include_rank` - Whether to include rank index
    /// * `rank_column_name` - Name of the rank column
    /// * `include_row_index` - Whether to include the row index as a column
    /// * `row_index_column_name` - Name of the row index column
    /// * `max_rows` - Optional maximum rows to return
    #[new]
    #[pyo3(signature = (
        url,
        pattern,
        batch_size = 1000,
        count_hint = 100,
        include_key = true,
        key_column_name = "_key".to_string(),
        member_column_name = "member".to_string(),
        score_column_name = "score".to_string(),
        include_rank = false,
        rank_column_name = "rank".to_string(),
        include_row_index = false,
        row_index_column_name = "_index".to_string(),
        max_rows = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        url: String,
        pattern: String,
        batch_size: usize,
        count_hint: usize,
        include_key: bool,
        key_column_name: String,
        member_column_name: String,
        score_column_name: String,
        include_rank: bool,
        rank_column_name: String,
        include_row_index: bool,
        row_index_column_name: String,
        max_rows: Option<usize>,
    ) -> PyResult<Self> {
        let zset_schema = ZSetSchema::new()
            .with_key(include_key)
            .with_key_column_name(&key_column_name)
            .with_member_column_name(&member_column_name)
            .with_score_column_name(&score_column_name)
            .with_rank(include_rank)
            .with_rank_column_name(&rank_column_name)
            .with_row_index(include_row_index)
            .with_row_index_column_name(&row_index_column_name);

        let mut config = types::hash::BatchConfig::new(pattern)
            .with_batch_size(batch_size)
            .with_count_hint(count_hint);

        if let Some(max) = max_rows {
            config = config.with_max_rows(max);
        }

        let inner = ZSetBatchIterator::new(&url, zset_schema, config)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(Self { inner })
    }

    /// Get the next batch as Arrow IPC bytes.
    ///
    /// Returns None when iteration is complete.
    fn next_batch_ipc(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let batch = self
            .inner
            .next_batch()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        match batch {
            Some(record_batch) => {
                let mut buf = Vec::new();
                {
                    let mut writer = arrow::ipc::writer::FileWriter::try_new(
                        &mut buf,
                        record_batch.schema().as_ref(),
                    )
                    .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to create IPC writer: {}",
                            e
                        ))
                    })?;

                    writer.write(&record_batch).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to write batch: {}",
                            e
                        ))
                    })?;

                    writer.finish().map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to finish IPC: {}",
                            e
                        ))
                    })?;
                }

                Ok(Some(pyo3::types::PyBytes::new(py, &buf).into()))
            }
            None => Ok(None),
        }
    }

    /// Check if iteration is complete.
    fn is_done(&self) -> bool {
        self.inner.is_done()
    }

    /// Get the number of rows yielded so far.
    fn rows_yielded(&self) -> usize {
        self.inner.rows_yielded()
    }
}

#[cfg(feature = "python")]
/// Write sorted set members to Redis.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - List of Redis keys to write to
/// * `members_scores` - 2D list of (member, score) tuples for each sorted set
/// * `ttl` - Optional TTL in seconds for each key
/// * `if_exists` - How to handle existing keys: "fail", "replace", or "append"
///
/// # Returns
/// A tuple of (keys_written, keys_failed, keys_skipped).
#[pyfunction]
#[pyo3(signature = (url, keys, members_scores, ttl = None, if_exists = "replace".to_string()))]
fn py_write_zsets(
    url: &str,
    keys: Vec<String>,
    members_scores: Vec<Vec<(String, f64)>>,
    ttl: Option<i64>,
    if_exists: String,
) -> PyResult<(usize, usize, usize)> {
    let mode: WriteMode = if_exists.parse().map_err(|e: crate::Error| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
    })?;

    let result = write_zsets(url, keys, members_scores, ttl, mode)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Ok((result.keys_written, result.keys_failed, result.keys_skipped))
}

#[cfg(feature = "python")]
/// Python wrapper for StreamBatchIterator.
///
/// This class is used by the Python IO plugin to iterate over Redis Stream data
/// and yield Arrow RecordBatches.
#[pyclass]
pub struct PyStreamBatchIterator {
    inner: StreamBatchIterator,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyStreamBatchIterator {
    /// Create a new PyStreamBatchIterator.
    ///
    /// # Arguments
    /// * `url` - Redis connection URL
    /// * `pattern` - Key pattern to match
    /// * `fields` - List of field names to extract from entries
    /// * `batch_size` - Keys per batch
    /// * `count_hint` - SCAN COUNT hint
    /// * `start_id` - Start ID for XRANGE (default: "-" for oldest)
    /// * `end_id` - End ID for XRANGE (default: "+" for newest)
    /// * `count_per_stream` - Max entries per stream (optional)
    /// * `include_key` - Whether to include the Redis key as a column
    /// * `key_column_name` - Name of the key column
    /// * `include_id` - Whether to include the entry ID as a column
    /// * `id_column_name` - Name of the entry ID column
    /// * `include_timestamp` - Whether to include the timestamp as a column
    /// * `timestamp_column_name` - Name of the timestamp column
    /// * `include_sequence` - Whether to include the sequence as a column
    /// * `sequence_column_name` - Name of the sequence column
    /// * `include_row_index` - Whether to include the row index as a column
    /// * `row_index_column_name` - Name of the row index column
    /// * `max_rows` - Optional maximum rows to return
    #[new]
    #[pyo3(signature = (
        url,
        pattern,
        fields = vec![],
        batch_size = 1000,
        count_hint = 100,
        start_id = "-".to_string(),
        end_id = "+".to_string(),
        count_per_stream = None,
        include_key = true,
        key_column_name = "_key".to_string(),
        include_id = true,
        id_column_name = "_id".to_string(),
        include_timestamp = true,
        timestamp_column_name = "_ts".to_string(),
        include_sequence = false,
        sequence_column_name = "_seq".to_string(),
        include_row_index = false,
        row_index_column_name = "_index".to_string(),
        max_rows = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        url: String,
        pattern: String,
        fields: Vec<String>,
        batch_size: usize,
        count_hint: usize,
        start_id: String,
        end_id: String,
        count_per_stream: Option<usize>,
        include_key: bool,
        key_column_name: String,
        include_id: bool,
        id_column_name: String,
        include_timestamp: bool,
        timestamp_column_name: String,
        include_sequence: bool,
        sequence_column_name: String,
        include_row_index: bool,
        row_index_column_name: String,
        max_rows: Option<usize>,
    ) -> PyResult<Self> {
        let stream_schema = StreamSchema::new()
            .with_key(include_key)
            .with_key_column_name(&key_column_name)
            .with_id(include_id)
            .with_id_column_name(&id_column_name)
            .with_timestamp(include_timestamp)
            .with_timestamp_column_name(&timestamp_column_name)
            .with_sequence(include_sequence)
            .with_sequence_column_name(&sequence_column_name)
            .with_row_index(include_row_index)
            .with_row_index_column_name(&row_index_column_name)
            .set_fields(fields);

        let mut config = types::hash::BatchConfig::new(pattern)
            .with_batch_size(batch_size)
            .with_count_hint(count_hint);

        if let Some(max) = max_rows {
            config = config.with_max_rows(max);
        }

        let mut inner = StreamBatchIterator::new(&url, stream_schema, config)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        inner = inner.with_start_id(&start_id).with_end_id(&end_id);

        if let Some(count) = count_per_stream {
            inner = inner.with_count_per_stream(count);
        }

        Ok(Self { inner })
    }

    /// Get the next batch as Arrow IPC bytes.
    ///
    /// Returns None when iteration is complete.
    fn next_batch_ipc(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let batch = self
            .inner
            .next_batch()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        match batch {
            Some(record_batch) => {
                let mut buf = Vec::new();
                {
                    let mut writer = arrow::ipc::writer::FileWriter::try_new(
                        &mut buf,
                        record_batch.schema().as_ref(),
                    )
                    .map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to create IPC writer: {}",
                            e
                        ))
                    })?;

                    writer.write(&record_batch).map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to write batch: {}",
                            e
                        ))
                    })?;

                    writer.finish().map_err(|e| {
                        PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                            "Failed to finish IPC: {}",
                            e
                        ))
                    })?;
                }

                Ok(Some(pyo3::types::PyBytes::new(py, &buf).into()))
            }
            None => Ok(None),
        }
    }

    /// Check if iteration is complete.
    fn is_done(&self) -> bool {
        self.inner.is_done()
    }

    /// Get the number of rows yielded so far.
    fn rows_yielded(&self) -> usize {
        self.inner.rows_yielded()
    }
}
