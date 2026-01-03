//! polars-redis: Redis IO plugin for Polars
//!
//! This crate provides a Polars IO plugin that enables scanning Redis data structures
//! (hashes, JSON documents, strings) as LazyFrames, with support for projection pushdown,
//! predicate pushdown, and batched iteration.

#[cfg(feature = "python")]
use arrow::datatypes::DataType;
#[cfg(feature = "python")]
use pyo3::prelude::*;

mod arrow_convert;
mod batch_iter;
mod connection;
mod error;
mod hash_reader;
mod infer;
mod json_batch_iter;
mod json_convert;
mod json_reader;
mod scanner;
mod schema;
mod write;

pub use batch_iter::{BatchConfig, HashBatchIterator};
pub use connection::RedisConnection;
pub use error::{Error, Result};
pub use infer::{InferredSchema, infer_hash_schema, infer_json_schema};
pub use json_batch_iter::JsonBatchIterator;
pub use json_convert::JsonSchema;
pub use schema::{HashSchema, RedisType};
pub use write::{WriteResult, write_hashes, write_json, write_strings};

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
#[pymodule]
fn _internal(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RedisScanner>()?;
    m.add_class::<PyHashBatchIterator>()?;
    m.add_class::<PyJsonBatchIterator>()?;
    m.add_function(wrap_pyfunction!(scan_keys, m)?)?;
    m.add_function(wrap_pyfunction!(py_infer_hash_schema, m)?)?;
    m.add_function(wrap_pyfunction!(py_infer_json_schema, m)?)?;
    m.add_function(wrap_pyfunction!(py_write_hashes, m)?)?;
    m.add_function(wrap_pyfunction!(py_write_json, m)?)?;
    m.add_function(wrap_pyfunction!(py_write_strings, m)?)?;
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
            .with_key_column_name(key_column_name);

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
            .with_key_column_name(key_column_name);

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
///
/// # Returns
/// A tuple of (keys_written, keys_failed).
#[pyfunction]
fn py_write_hashes(
    url: &str,
    keys: Vec<String>,
    fields: Vec<String>,
    values: Vec<Vec<Option<String>>>,
) -> PyResult<(usize, usize)> {
    let result = write_hashes(url, keys, fields, values)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Ok((result.keys_written, result.keys_failed))
}

#[cfg(feature = "python")]
/// Write JSON documents to Redis.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - List of Redis keys to write to
/// * `json_strings` - List of JSON strings to write
///
/// # Returns
/// A tuple of (keys_written, keys_failed).
#[pyfunction]
fn py_write_json(
    url: &str,
    keys: Vec<String>,
    json_strings: Vec<String>,
) -> PyResult<(usize, usize)> {
    let result = write_json(url, keys, json_strings)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Ok((result.keys_written, result.keys_failed))
}

#[cfg(feature = "python")]
/// Write string values to Redis.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - List of Redis keys to write to
/// * `values` - List of string values to write (None for null)
///
/// # Returns
/// A tuple of (keys_written, keys_failed).
#[pyfunction]
fn py_write_strings(
    url: &str,
    keys: Vec<String>,
    values: Vec<Option<String>>,
) -> PyResult<(usize, usize)> {
    let result = write_strings(url, keys, values)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Ok((result.keys_written, result.keys_failed))
}
