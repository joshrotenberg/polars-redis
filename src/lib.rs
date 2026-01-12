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
#[cfg(feature = "python")]
use std::collections::HashMap;

// Module organization:
// - io/       : DataFrame I/O operations (read, write, cache, search)
// - client/   : General Redis operations (geo, keys, pipeline, pubsub)
// - (top-level): Shared infrastructure (connection, error, schema, options, etc.)

pub mod client;
#[cfg(feature = "cluster")]
pub mod cluster;
mod connection;
mod error;
#[cfg(feature = "search")]
pub mod index;
pub mod io;
pub mod options;
pub mod parallel;
#[cfg(feature = "search")]
pub mod query_builder;
mod schema;
#[cfg(feature = "search")]
pub mod smart;

// Re-exports for backwards compatibility - all public API remains at crate root

// Cluster support
#[cfg(feature = "cluster")]
pub use cluster::{ClusterKeyScanner, DirectClusterKeyScanner};

// Connection
pub use connection::{ConnectionConfig, RedisConn, RedisConnection};

// Error handling
pub use error::{Error, Result};

// Client operations (geo, keys, pipeline, pubsub)
pub use client::geo::{
    GeoAddResult, GeoLocation, GeoSort, GeoUnit, geo_add, geo_dist, geo_dist_matrix, geo_hash,
    geo_pos, geo_radius, geo_radius_by_member,
};
pub use client::keys::{
    DeleteResult, KeyInfo, RenameResult, TtlResult, delete_keys, delete_keys_pattern, exists_keys,
    get_ttl, key_info, persist_keys, rename_keys, set_ttl, set_ttl_individual,
};
pub use client::pipeline::{CommandResult, Pipeline, PipelineResult, Transaction};
pub use client::pubsub::{PubSubConfig, PubSubMessage, PubSubStats, collect_pubsub};

// IO operations (types, write, cache, infer, search)
pub use io::cache::{
    CacheConfig, CacheFormat, CacheInfo, IpcCompression, ParquetCompressionType, cache_exists,
    cache_info, cache_record_batch, cache_ttl, delete_cached, get_cached_record_batch,
};
pub use io::infer::{
    FieldInferenceInfo, InferredSchema, InferredSchemaWithConfidence, infer_hash_schema,
    infer_hash_schema_with_confidence, infer_json_schema,
};
pub use io::multi::{Destination, DestinationResult, MultiClusterWriter, MultiWriteResult};
pub use io::types::hash::{BatchConfig, HashBatchIterator, HashFetcher};
#[cfg(feature = "cluster")]
pub use io::types::hash::{ClusterHashBatchIterator, ClusterHashFetcher};
#[cfg(feature = "search")]
pub use io::types::hash::{HashSearchIterator, SearchBatchConfig};
#[cfg(feature = "cluster")]
pub use io::types::json::ClusterJsonBatchIterator;
pub use io::types::json::{JsonBatchIterator, JsonSchema};
#[cfg(feature = "cluster")]
pub use io::types::list::ClusterListBatchIterator;
pub use io::types::list::{ListBatchIterator, ListSchema};
#[cfg(feature = "cluster")]
pub use io::types::set::ClusterSetBatchIterator;
pub use io::types::set::{SetBatchIterator, SetSchema};
#[cfg(feature = "cluster")]
pub use io::types::stream::ClusterStreamBatchIterator;
pub use io::types::stream::{
    CheckpointStore, ConsumerConfig, ConsumerStats, MemoryCheckpointStore, RedisCheckpointStore,
    StreamBatchIterator, StreamConsumer, StreamSchema, create_consumer_group,
    destroy_consumer_group, stream_ack,
};
#[cfg(feature = "cluster")]
pub use io::types::string::ClusterStringBatchIterator;
pub use io::types::string::{StringBatchIterator, StringSchema};
#[cfg(feature = "cluster")]
pub use io::types::timeseries::ClusterTimeSeriesBatchIterator;
pub use io::types::timeseries::{TimeSeriesBatchIterator, TimeSeriesSchema};
#[cfg(feature = "cluster")]
pub use io::types::zset::ClusterZSetBatchIterator;
pub use io::types::zset::{ZSetBatchIterator, ZSetSchema};
pub use io::write::{
    KeyError, WriteMode, WriteResult, WriteResultDetailed, write_hashes, write_hashes_detailed,
    write_json, write_lists, write_sets, write_strings, write_zsets,
};

// Search/index support
#[cfg(feature = "search")]
pub use index::{
    DistanceMetric, Field, GeoField, GeoShapeField, Index, IndexDiff, IndexInfo, IndexType,
    NumericField, TagField, TextField, VectorAlgorithm, VectorField,
};
#[cfg(feature = "search")]
pub use query_builder::{Predicate, PredicateBuilder, Value};
#[cfg(feature = "search")]
pub use smart::{DetectedIndex, ExecutionStrategy, QueryPlan};

// Options and configuration
pub use options::{
    HashScanOptions, JsonScanOptions, KeyColumn, ParallelStrategy, RowIndex, RowIndexColumn,
    ScanOptions, StreamScanOptions, StringScanOptions, TimeSeriesScanOptions, TtlColumn,
    get_default_batch_size, get_default_count_hint, get_default_timeout_ms,
};
pub use parallel::{FetchResult, KeyBatch, ParallelConfig, ParallelFetch};

// Schema types
pub use schema::{HashSchema, RedisType};

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
    m.add_class::<PyStreamConsumer>()?;
    m.add_class::<PyTimeSeriesBatchIterator>()?;
    #[cfg(feature = "search")]
    m.add_class::<PyHashSearchIterator>()?;
    #[cfg(feature = "search")]
    m.add_function(wrap_pyfunction!(py_aggregate, m)?)?;
    m.add_function(wrap_pyfunction!(scan_keys, m)?)?;
    m.add_function(wrap_pyfunction!(py_infer_hash_schema, m)?)?;
    m.add_function(wrap_pyfunction!(py_infer_json_schema, m)?)?;
    m.add_function(wrap_pyfunction!(py_infer_hash_schema_with_overwrite, m)?)?;
    m.add_function(wrap_pyfunction!(py_infer_hash_schema_with_confidence, m)?)?;
    m.add_function(wrap_pyfunction!(py_infer_json_schema_with_overwrite, m)?)?;
    m.add_function(wrap_pyfunction!(py_write_hashes, m)?)?;
    m.add_function(wrap_pyfunction!(py_write_hashes_detailed, m)?)?;
    m.add_function(wrap_pyfunction!(py_write_json, m)?)?;
    m.add_function(wrap_pyfunction!(py_write_strings, m)?)?;
    m.add_function(wrap_pyfunction!(py_write_sets, m)?)?;
    m.add_function(wrap_pyfunction!(py_write_lists, m)?)?;
    m.add_function(wrap_pyfunction!(py_write_zsets, m)?)?;
    m.add_function(wrap_pyfunction!(py_cache_set, m)?)?;
    m.add_function(wrap_pyfunction!(py_cache_get, m)?)?;
    m.add_function(wrap_pyfunction!(py_cache_delete, m)?)?;
    m.add_function(wrap_pyfunction!(py_cache_exists, m)?)?;
    m.add_function(wrap_pyfunction!(py_cache_ttl, m)?)?;
    m.add_class::<PyPipeline>()?;
    m.add_class::<PyTransaction>()?;
    m.add_class::<PyCommandResult>()?;
    m.add_class::<PyPipelineResult>()?;
    m.add_function(wrap_pyfunction!(py_geo_add, m)?)?;
    m.add_function(wrap_pyfunction!(py_geo_radius, m)?)?;
    m.add_function(wrap_pyfunction!(py_geo_radius_by_member, m)?)?;
    m.add_function(wrap_pyfunction!(py_geo_dist, m)?)?;
    m.add_function(wrap_pyfunction!(py_geo_pos, m)?)?;
    m.add_function(wrap_pyfunction!(py_geo_dist_matrix, m)?)?;
    m.add_function(wrap_pyfunction!(py_geo_hash, m)?)?;
    m.add_function(wrap_pyfunction!(py_key_info, m)?)?;
    m.add_function(wrap_pyfunction!(py_set_ttl, m)?)?;
    m.add_function(wrap_pyfunction!(py_set_ttl_individual, m)?)?;
    m.add_function(wrap_pyfunction!(py_delete_keys, m)?)?;
    m.add_function(wrap_pyfunction!(py_delete_keys_pattern, m)?)?;
    m.add_function(wrap_pyfunction!(py_rename_keys, m)?)?;
    m.add_function(wrap_pyfunction!(py_persist_keys, m)?)?;
    m.add_function(wrap_pyfunction!(py_exists_keys, m)?)?;
    m.add_function(wrap_pyfunction!(py_get_ttl, m)?)?;
    m.add_function(wrap_pyfunction!(py_create_consumer_group, m)?)?;
    m.add_function(wrap_pyfunction!(py_destroy_consumer_group, m)?)?;
    m.add_function(wrap_pyfunction!(py_stream_ack, m)?)?;
    #[cfg(feature = "cluster")]
    m.add_class::<PyClusterHashBatchIterator>()?;
    #[cfg(feature = "cluster")]
    m.add_class::<PyClusterJsonBatchIterator>()?;
    #[cfg(feature = "cluster")]
    m.add_class::<PyClusterStringBatchIterator>()?;
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
    /// * `parallel` - Optional number of parallel workers for fetching
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
        max_rows = None,
        parallel = None
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
        parallel: Option<usize>,
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

        if let Some(workers) = parallel {
            config = config.with_parallel(ParallelStrategy::batches(workers));
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
            },
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

// ============================================================================
// Stream Consumer Python bindings
// ============================================================================

#[cfg(feature = "python")]
/// Python wrapper for StreamConsumer with consumer group support.
///
/// This class provides stream consumption with consumer groups, checkpointing,
/// and entry acknowledgment (XACK).
#[pyclass]
pub struct PyStreamConsumer {
    inner: StreamConsumer,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyStreamConsumer {
    /// Create a new PyStreamConsumer.
    ///
    /// # Arguments
    /// * `url` - Redis connection URL
    /// * `stream` - Stream key to consume from
    /// * `group` - Consumer group name
    /// * `consumer` - Consumer name within the group
    /// * `fields` - List of field names to extract from entries
    /// * `batch_size` - Entries per batch (default: 100)
    /// * `block_ms` - Block timeout in milliseconds (default: 5000)
    /// * `auto_ack` - Automatically acknowledge entries after reading (default: false)
    /// * `create_group` - Create consumer group if it doesn't exist (default: true)
    /// * `group_start_id` - Start ID for new groups ("0" = beginning, "$" = now)
    /// * `include_key` - Whether to include the stream key column
    /// * `include_id` - Whether to include the entry ID column
    /// * `include_timestamp` - Whether to include the timestamp column
    /// * `include_sequence` - Whether to include the sequence column
    #[new]
    #[pyo3(signature = (
        url,
        stream,
        group,
        consumer,
        fields = vec![],
        batch_size = 100,
        block_ms = 5000,
        auto_ack = false,
        create_group = true,
        group_start_id = "0".to_string(),
        include_key = true,
        include_id = true,
        include_timestamp = true,
        include_sequence = false
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        url: String,
        stream: String,
        group: String,
        consumer: String,
        fields: Vec<String>,
        batch_size: usize,
        block_ms: u64,
        auto_ack: bool,
        create_group: bool,
        group_start_id: String,
        include_key: bool,
        include_id: bool,
        include_timestamp: bool,
        include_sequence: bool,
    ) -> PyResult<Self> {
        let schema = StreamSchema::new()
            .with_key(include_key)
            .with_id(include_id)
            .with_timestamp(include_timestamp)
            .with_sequence(include_sequence)
            .set_fields(fields);

        let config = ConsumerConfig::new()
            .with_batch_size(batch_size)
            .with_block_ms(block_ms)
            .with_auto_ack(auto_ack)
            .with_create_group(create_group)
            .with_group_start_id(&group_start_id);

        let inner = StreamConsumer::new(&url, &stream, &group, &consumer)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?
            .with_schema(schema)
            .with_config(config);

        Ok(Self { inner })
    }

    /// Get the next batch as Arrow IPC bytes.
    ///
    /// Returns None when no entries are available within the block timeout.
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
            },
            None => Ok(None),
        }
    }

    /// Acknowledge the entries from the last batch.
    ///
    /// Returns the number of entries acknowledged.
    fn ack_batch(&mut self) -> PyResult<usize> {
        self.inner
            .ack_batch()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Acknowledge specific entry IDs.
    ///
    /// Returns the number of entries acknowledged.
    fn ack_entries(&mut self, ids: Vec<String>) -> PyResult<usize> {
        self.inner
            .ack_entries(&ids)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Get the number of pending entries for this consumer.
    fn pending_count(&mut self) -> PyResult<u64> {
        self.inner
            .pending_count()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
    }

    /// Rollback the current batch (don't acknowledge, allow re-delivery).
    fn rollback(&mut self) {
        self.inner.rollback();
    }

    /// Get the stream key.
    #[getter]
    fn stream(&self) -> &str {
        self.inner.stream()
    }

    /// Get the group name.
    #[getter]
    fn group(&self) -> &str {
        self.inner.group()
    }

    /// Get the consumer name.
    #[getter]
    fn consumer_name(&self) -> &str {
        self.inner.consumer_name()
    }

    /// Get consumption statistics.
    fn stats(&self) -> PyResult<HashMap<String, Py<PyAny>>> {
        let stats = self.inner.stats();
        Python::attach(|py| {
            let mut dict = HashMap::new();
            dict.insert(
                "batches_processed".to_string(),
                stats
                    .batches_processed
                    .into_pyobject(py)?
                    .into_any()
                    .unbind(),
            );
            dict.insert(
                "entries_processed".to_string(),
                stats
                    .entries_processed
                    .into_pyobject(py)?
                    .into_any()
                    .unbind(),
            );
            dict.insert(
                "entries_per_second".to_string(),
                stats
                    .entries_per_second()
                    .into_pyobject(py)?
                    .into_any()
                    .unbind(),
            );
            dict.insert(
                "elapsed_seconds".to_string(),
                stats
                    .elapsed()
                    .as_secs_f64()
                    .into_pyobject(py)?
                    .into_any()
                    .unbind(),
            );
            if let Some(ref last_id) = stats.last_entry_id {
                dict.insert(
                    "last_entry_id".to_string(),
                    last_id.clone().into_pyobject(py)?.into_any().unbind(),
                );
            }
            Ok(dict)
        })
    }
}

#[cfg(feature = "python")]
/// Create a consumer group for a stream.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `stream` - Stream key
/// * `group` - Group name to create
/// * `start_id` - Starting ID ("0" = from beginning, "$" = from now)
/// * `mkstream` - Whether to create the stream if it doesn't exist
///
/// # Returns
/// True if the group was created, False if it already existed.
#[pyfunction]
#[pyo3(signature = (url, stream, group, start_id = "0", mkstream = true))]
fn py_create_consumer_group(
    url: &str,
    stream: &str,
    group: &str,
    start_id: &str,
    mkstream: bool,
) -> PyResult<bool> {
    create_consumer_group(url, stream, group, start_id, mkstream)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
}

#[cfg(feature = "python")]
/// Destroy a consumer group.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `stream` - Stream key
/// * `group` - Group name to destroy
///
/// # Returns
/// True if the group was destroyed, False if it didn't exist.
#[pyfunction]
fn py_destroy_consumer_group(url: &str, stream: &str, group: &str) -> PyResult<bool> {
    destroy_consumer_group(url, stream, group)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
}

#[cfg(feature = "python")]
/// Acknowledge stream entries.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `stream` - Stream key
/// * `group` - Consumer group name
/// * `ids` - Entry IDs to acknowledge
///
/// # Returns
/// Number of entries acknowledged.
#[pyfunction]
fn py_stream_ack(url: &str, stream: &str, group: &str, ids: Vec<String>) -> PyResult<usize> {
    stream_ack(url, stream, group, &ids)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
}

// ============================================================================
// Pipeline and Transaction Python bindings
// ============================================================================

#[cfg(feature = "python")]
/// Python wrapper for Pipeline.
///
/// Pipelines batch multiple Redis commands and execute them in a single round-trip.
#[pyclass]
pub struct PyPipeline {
    inner: client::pipeline::Pipeline,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyPipeline {
    /// Create a new pipeline.
    #[new]
    fn new(url: String) -> PyResult<Self> {
        let inner = client::pipeline::Pipeline::new(&url)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyConnectionError, _>(e.to_string()))?;
        Ok(Self { inner })
    }

    /// Get the number of queued commands.
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    /// Check if the pipeline is empty.
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Clear all queued commands.
    fn clear(&mut self) {
        self.inner.clear();
    }

    // String commands
    /// Queue a SET command.
    fn set(&mut self, key: &str, value: &str) -> PyResult<()> {
        self.inner.set(key, value);
        Ok(())
    }

    /// Queue a SET command with expiration.
    fn set_ex(&mut self, key: &str, value: &str, seconds: i64) -> PyResult<()> {
        self.inner.set_ex(key, value, seconds);
        Ok(())
    }

    /// Queue a GET command.
    fn get(&mut self, key: &str) -> PyResult<()> {
        self.inner.get(key);
        Ok(())
    }

    /// Queue an MGET command.
    fn mget(&mut self, keys: Vec<String>) -> PyResult<()> {
        let keys_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        self.inner.mget(&keys_refs);
        Ok(())
    }

    /// Queue an INCR command.
    fn incr(&mut self, key: &str) -> PyResult<()> {
        self.inner.incr(key);
        Ok(())
    }

    /// Queue an INCRBY command.
    fn incrby(&mut self, key: &str, increment: i64) -> PyResult<()> {
        self.inner.incrby(key, increment);
        Ok(())
    }

    /// Queue a DECR command.
    fn decr(&mut self, key: &str) -> PyResult<()> {
        self.inner.decr(key);
        Ok(())
    }

    // Hash commands
    /// Queue an HSET command.
    fn hset(&mut self, key: &str, field: &str, value: &str) -> PyResult<()> {
        self.inner.hset(key, field, value);
        Ok(())
    }

    /// Queue an HMSET command for multiple fields.
    fn hmset(&mut self, key: &str, fields: HashMap<String, String>) -> PyResult<()> {
        let pairs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        self.inner.hmset(key, &pairs);
        Ok(())
    }

    /// Queue an HGET command.
    fn hget(&mut self, key: &str, field: &str) -> PyResult<()> {
        self.inner.hget(key, field);
        Ok(())
    }

    /// Queue an HGETALL command.
    fn hgetall(&mut self, key: &str) -> PyResult<()> {
        self.inner.hgetall(key);
        Ok(())
    }

    /// Queue an HDEL command.
    fn hdel(&mut self, key: &str, fields: Vec<String>) -> PyResult<()> {
        let fields_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
        self.inner.hdel(key, &fields_refs);
        Ok(())
    }

    /// Queue an HINCRBY command.
    fn hincrby(&mut self, key: &str, field: &str, increment: i64) -> PyResult<()> {
        self.inner.hincrby(key, field, increment);
        Ok(())
    }

    // List commands
    /// Queue an LPUSH command.
    fn lpush(&mut self, key: &str, values: Vec<String>) -> PyResult<()> {
        let values_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        self.inner.lpush(key, &values_refs);
        Ok(())
    }

    /// Queue an RPUSH command.
    fn rpush(&mut self, key: &str, values: Vec<String>) -> PyResult<()> {
        let values_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        self.inner.rpush(key, &values_refs);
        Ok(())
    }

    /// Queue an LRANGE command.
    fn lrange(&mut self, key: &str, start: i64, stop: i64) -> PyResult<()> {
        self.inner.lrange(key, start, stop);
        Ok(())
    }

    /// Queue an LLEN command.
    fn llen(&mut self, key: &str) -> PyResult<()> {
        self.inner.llen(key);
        Ok(())
    }

    // Set commands
    /// Queue an SADD command.
    fn sadd(&mut self, key: &str, members: Vec<String>) -> PyResult<()> {
        let members_refs: Vec<&str> = members.iter().map(|s| s.as_str()).collect();
        self.inner.sadd(key, &members_refs);
        Ok(())
    }

    /// Queue an SMEMBERS command.
    fn smembers(&mut self, key: &str) -> PyResult<()> {
        self.inner.smembers(key);
        Ok(())
    }

    /// Queue an SISMEMBER command.
    fn sismember(&mut self, key: &str, member: &str) -> PyResult<()> {
        self.inner.sismember(key, member);
        Ok(())
    }

    /// Queue an SCARD command.
    fn scard(&mut self, key: &str) -> PyResult<()> {
        self.inner.scard(key);
        Ok(())
    }

    // Sorted set commands
    /// Queue a ZADD command.
    fn zadd(&mut self, key: &str, members: Vec<(f64, String)>) -> PyResult<()> {
        let members_refs: Vec<(f64, &str)> =
            members.iter().map(|(s, m)| (*s, m.as_str())).collect();
        self.inner.zadd(key, &members_refs);
        Ok(())
    }

    /// Queue a ZRANGE command.
    fn zrange(&mut self, key: &str, start: i64, stop: i64) -> PyResult<()> {
        self.inner.zrange(key, start, stop);
        Ok(())
    }

    /// Queue a ZSCORE command.
    fn zscore(&mut self, key: &str, member: &str) -> PyResult<()> {
        self.inner.zscore(key, member);
        Ok(())
    }

    /// Queue a ZCARD command.
    fn zcard(&mut self, key: &str) -> PyResult<()> {
        self.inner.zcard(key);
        Ok(())
    }

    // Key commands
    /// Queue a DEL command.
    fn delete(&mut self, keys: Vec<String>) -> PyResult<()> {
        let keys_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        self.inner.del(&keys_refs);
        Ok(())
    }

    /// Queue an EXISTS command.
    fn exists(&mut self, keys: Vec<String>) -> PyResult<()> {
        let keys_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        self.inner.exists(&keys_refs);
        Ok(())
    }

    /// Queue an EXPIRE command.
    fn expire(&mut self, key: &str, seconds: i64) -> PyResult<()> {
        self.inner.expire(key, seconds);
        Ok(())
    }

    /// Queue a TTL command.
    fn ttl(&mut self, key: &str) -> PyResult<()> {
        self.inner.ttl(key);
        Ok(())
    }

    /// Queue a RENAME command.
    fn rename(&mut self, key: &str, new_key: &str) -> PyResult<()> {
        self.inner.rename(key, new_key);
        Ok(())
    }

    /// Queue a TYPE command.
    fn key_type(&mut self, key: &str) -> PyResult<()> {
        self.inner.key_type(key);
        Ok(())
    }

    /// Queue a raw command with arguments.
    fn raw(&mut self, command: &str, args: Vec<String>) -> PyResult<()> {
        let args_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        self.inner.raw(command, &args_refs);
        Ok(())
    }

    /// Execute all queued commands and return results.
    fn execute(&mut self) -> PyResult<PyPipelineResult> {
        let result = self
            .inner
            .execute()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(PyPipelineResult {
            results: result
                .results
                .into_iter()
                .map(PyCommandResult::from)
                .collect(),
            succeeded: result.succeeded,
            failed: result.failed,
        })
    }
}

#[cfg(feature = "python")]
/// Result of a single command in a pipeline.
#[pyclass]
pub struct PyCommandResult {
    #[pyo3(get)]
    result_type: String,
    /// The value as a simple type (string, int, or None).
    value_str: Option<String>,
    value_int: Option<i64>,
    value_arr: Option<Vec<PyCommandResult>>,
}

#[cfg(feature = "python")]
impl From<client::pipeline::CommandResult> for PyCommandResult {
    fn from(result: client::pipeline::CommandResult) -> Self {
        match result {
            client::pipeline::CommandResult::Ok => PyCommandResult {
                result_type: "ok".to_string(),
                value_str: None,
                value_int: None,
                value_arr: None,
            },
            client::pipeline::CommandResult::String(s) => PyCommandResult {
                result_type: "string".to_string(),
                value_str: Some(s),
                value_int: None,
                value_arr: None,
            },
            client::pipeline::CommandResult::Int(i) => PyCommandResult {
                result_type: "int".to_string(),
                value_str: None,
                value_int: Some(i),
                value_arr: None,
            },
            client::pipeline::CommandResult::Bulk(s) => PyCommandResult {
                result_type: "bulk".to_string(),
                value_str: Some(s),
                value_int: None,
                value_arr: None,
            },
            client::pipeline::CommandResult::Array(arr) => {
                let py_arr: Vec<PyCommandResult> = arr.into_iter().map(|v| v.into()).collect();
                PyCommandResult {
                    result_type: "array".to_string(),
                    value_str: None,
                    value_int: None,
                    value_arr: Some(py_arr),
                }
            },
            client::pipeline::CommandResult::Nil => PyCommandResult {
                result_type: "nil".to_string(),
                value_str: None,
                value_int: None,
                value_arr: None,
            },
            client::pipeline::CommandResult::Error(e) => PyCommandResult {
                result_type: "error".to_string(),
                value_str: Some(e),
                value_int: None,
                value_arr: None,
            },
        }
    }
}

#[cfg(feature = "python")]
#[pymethods]
impl PyCommandResult {
    /// Check if the result is an error.
    fn is_error(&self) -> bool {
        self.result_type == "error"
    }

    /// Check if the result is OK.
    fn is_ok(&self) -> bool {
        self.result_type == "ok"
    }

    /// Check if the result is nil.
    fn is_nil(&self) -> bool {
        self.result_type == "nil"
    }

    /// Get the value. Returns the appropriate Python type based on result_type.
    #[getter]
    fn value(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if let Some(ref s) = self.value_str {
            Ok(s.clone().into_pyobject(py)?.into_any().unbind())
        } else if let Some(i) = self.value_int {
            Ok(i.into_pyobject(py)?.into_any().unbind())
        } else if let Some(ref arr) = self.value_arr {
            // Convert array to Python list
            let list = pyo3::types::PyList::empty(py);
            for item in arr {
                let py_item = Py::new(
                    py,
                    PyCommandResult {
                        result_type: item.result_type.clone(),
                        value_str: item.value_str.clone(),
                        value_int: item.value_int,
                        value_arr: None, // Simplified - nested arrays not deeply copied
                    },
                )?;
                list.append(py_item)?;
            }
            Ok(list.into_any().unbind())
        } else {
            Ok(py.None())
        }
    }

    fn __repr__(&self) -> String {
        if self.value_str.is_some() || self.value_int.is_some() || self.value_arr.is_some() {
            format!("CommandResult({}, ...)", self.result_type)
        } else {
            format!("CommandResult({})", self.result_type)
        }
    }
}

#[cfg(feature = "python")]
/// Result of executing a pipeline.
#[pyclass]
pub struct PyPipelineResult {
    results: Vec<PyCommandResult>,
    #[pyo3(get)]
    succeeded: usize,
    #[pyo3(get)]
    failed: usize,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyPipelineResult {
    /// Check if all commands succeeded.
    fn all_succeeded(&self) -> bool {
        self.failed == 0
    }

    /// Get the results as a list.
    #[getter]
    fn results(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let list = pyo3::types::PyList::empty(py);
        for item in &self.results {
            let py_item = Py::new(
                py,
                PyCommandResult {
                    result_type: item.result_type.clone(),
                    value_str: item.value_str.clone(),
                    value_int: item.value_int,
                    value_arr: None,
                },
            )?;
            list.append(py_item)?;
        }
        Ok(list.into_any().unbind())
    }

    /// Get the result at a specific index.
    fn get(&self, py: Python<'_>, index: usize) -> PyResult<Option<Py<PyCommandResult>>> {
        if let Some(item) = self.results.get(index) {
            let py_item = Py::new(
                py,
                PyCommandResult {
                    result_type: item.result_type.clone(),
                    value_str: item.value_str.clone(),
                    value_int: item.value_int,
                    value_arr: None,
                },
            )?;
            Ok(Some(py_item))
        } else {
            Ok(None)
        }
    }

    fn __len__(&self) -> usize {
        self.results.len()
    }

    fn __repr__(&self) -> String {
        format!(
            "PipelineResult(commands={}, succeeded={}, failed={})",
            self.results.len(),
            self.succeeded,
            self.failed
        )
    }
}

#[cfg(feature = "python")]
/// Python wrapper for Transaction.
///
/// Transactions execute Redis commands atomically using MULTI/EXEC.
#[pyclass]
pub struct PyTransaction {
    inner: client::pipeline::Transaction,
}

#[cfg(feature = "python")]
#[pymethods]
impl PyTransaction {
    /// Create a new transaction.
    #[new]
    fn new(url: String) -> PyResult<Self> {
        let inner = client::pipeline::Transaction::new(&url)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyConnectionError, _>(e.to_string()))?;
        Ok(Self { inner })
    }

    /// Get the number of queued commands.
    fn __len__(&self) -> usize {
        self.inner.len()
    }

    /// Check if the transaction is empty.
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Discard the transaction (clear all queued commands).
    fn discard(&mut self) {
        self.inner.discard();
    }

    // Forward all command methods (same as Pipeline)
    fn set(&mut self, key: &str, value: &str) -> PyResult<()> {
        self.inner.set(key, value);
        Ok(())
    }

    fn set_ex(&mut self, key: &str, value: &str, seconds: i64) -> PyResult<()> {
        self.inner.set_ex(key, value, seconds);
        Ok(())
    }

    fn get(&mut self, key: &str) -> PyResult<()> {
        self.inner.get(key);
        Ok(())
    }

    fn mget(&mut self, keys: Vec<String>) -> PyResult<()> {
        let keys_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        self.inner.mget(&keys_refs);
        Ok(())
    }

    fn incr(&mut self, key: &str) -> PyResult<()> {
        self.inner.incr(key);
        Ok(())
    }

    fn incrby(&mut self, key: &str, increment: i64) -> PyResult<()> {
        self.inner.incrby(key, increment);
        Ok(())
    }

    fn decr(&mut self, key: &str) -> PyResult<()> {
        self.inner.decr(key);
        Ok(())
    }

    fn hset(&mut self, key: &str, field: &str, value: &str) -> PyResult<()> {
        self.inner.hset(key, field, value);
        Ok(())
    }

    fn hmset(&mut self, key: &str, fields: HashMap<String, String>) -> PyResult<()> {
        let pairs: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        self.inner.hmset(key, &pairs);
        Ok(())
    }

    fn hget(&mut self, key: &str, field: &str) -> PyResult<()> {
        self.inner.hget(key, field);
        Ok(())
    }

    fn hgetall(&mut self, key: &str) -> PyResult<()> {
        self.inner.hgetall(key);
        Ok(())
    }

    fn hdel(&mut self, key: &str, fields: Vec<String>) -> PyResult<()> {
        let fields_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
        self.inner.hdel(key, &fields_refs);
        Ok(())
    }

    fn hincrby(&mut self, key: &str, field: &str, increment: i64) -> PyResult<()> {
        self.inner.hincrby(key, field, increment);
        Ok(())
    }

    fn lpush(&mut self, key: &str, values: Vec<String>) -> PyResult<()> {
        let values_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        self.inner.lpush(key, &values_refs);
        Ok(())
    }

    fn rpush(&mut self, key: &str, values: Vec<String>) -> PyResult<()> {
        let values_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
        self.inner.rpush(key, &values_refs);
        Ok(())
    }

    fn lrange(&mut self, key: &str, start: i64, stop: i64) -> PyResult<()> {
        self.inner.lrange(key, start, stop);
        Ok(())
    }

    fn llen(&mut self, key: &str) -> PyResult<()> {
        self.inner.llen(key);
        Ok(())
    }

    fn sadd(&mut self, key: &str, members: Vec<String>) -> PyResult<()> {
        let members_refs: Vec<&str> = members.iter().map(|s| s.as_str()).collect();
        self.inner.sadd(key, &members_refs);
        Ok(())
    }

    fn smembers(&mut self, key: &str) -> PyResult<()> {
        self.inner.smembers(key);
        Ok(())
    }

    fn sismember(&mut self, key: &str, member: &str) -> PyResult<()> {
        self.inner.sismember(key, member);
        Ok(())
    }

    fn scard(&mut self, key: &str) -> PyResult<()> {
        self.inner.scard(key);
        Ok(())
    }

    fn zadd(&mut self, key: &str, members: Vec<(f64, String)>) -> PyResult<()> {
        let members_refs: Vec<(f64, &str)> =
            members.iter().map(|(s, m)| (*s, m.as_str())).collect();
        self.inner.zadd(key, &members_refs);
        Ok(())
    }

    fn zrange(&mut self, key: &str, start: i64, stop: i64) -> PyResult<()> {
        self.inner.zrange(key, start, stop);
        Ok(())
    }

    fn zscore(&mut self, key: &str, member: &str) -> PyResult<()> {
        self.inner.zscore(key, member);
        Ok(())
    }

    fn zcard(&mut self, key: &str) -> PyResult<()> {
        self.inner.zcard(key);
        Ok(())
    }

    fn delete(&mut self, keys: Vec<String>) -> PyResult<()> {
        let keys_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        self.inner.del(&keys_refs);
        Ok(())
    }

    fn exists(&mut self, keys: Vec<String>) -> PyResult<()> {
        let keys_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        self.inner.exists(&keys_refs);
        Ok(())
    }

    fn expire(&mut self, key: &str, seconds: i64) -> PyResult<()> {
        self.inner.expire(key, seconds);
        Ok(())
    }

    fn ttl(&mut self, key: &str) -> PyResult<()> {
        self.inner.ttl(key);
        Ok(())
    }

    fn rename(&mut self, key: &str, new_key: &str) -> PyResult<()> {
        self.inner.rename(key, new_key);
        Ok(())
    }

    fn key_type(&mut self, key: &str) -> PyResult<()> {
        self.inner.key_type(key);
        Ok(())
    }

    fn raw(&mut self, command: &str, args: Vec<String>) -> PyResult<()> {
        let args_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
        self.inner.raw(command, &args_refs);
        Ok(())
    }

    /// Execute the transaction atomically.
    fn execute(&mut self) -> PyResult<PyPipelineResult> {
        let result = self
            .inner
            .execute()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(PyPipelineResult {
            results: result
                .results
                .into_iter()
                .map(PyCommandResult::from)
                .collect(),
            succeeded: result.succeeded,
            failed: result.failed,
        })
    }
}

// ============================================================================
// Cache functions for DataFrame caching
// ============================================================================

#[cfg(feature = "python")]
/// Store bytes in Redis with optional TTL.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key
/// * `data` - Bytes to store
/// * `ttl` - Optional TTL in seconds
///
/// # Returns
/// Number of bytes written.
#[pyfunction]
#[pyo3(signature = (url, key, data, ttl = None))]
fn py_cache_set(url: &str, key: &str, data: &[u8], ttl: Option<i64>) -> PyResult<usize> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    rt.block_on(async {
        let client = redis::Client::open(url)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyConnectionError, _>(e.to_string()))?;

        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyConnectionError, _>(e.to_string()))?;

        let len = data.len();

        if let Some(seconds) = ttl {
            redis::cmd("SETEX")
                .arg(key)
                .arg(seconds)
                .arg(data)
                .query_async::<()>(&mut conn)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        } else {
            redis::cmd("SET")
                .arg(key)
                .arg(data)
                .query_async::<()>(&mut conn)
                .await
                .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        }

        Ok(len)
    })
}

#[cfg(feature = "python")]
/// Retrieve bytes from Redis.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key
///
/// # Returns
/// Bytes if key exists, None otherwise.
#[pyfunction]
fn py_cache_get(py: Python<'_>, url: &str, key: &str) -> PyResult<Option<Py<PyAny>>> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    rt.block_on(async {
        let client = redis::Client::open(url)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyConnectionError, _>(e.to_string()))?;

        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyConnectionError, _>(e.to_string()))?;

        let result: Option<Vec<u8>> = redis::cmd("GET")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        match result {
            Some(data) => Ok(Some(pyo3::types::PyBytes::new(py, &data).into())),
            None => Ok(None),
        }
    })
}

#[cfg(feature = "python")]
/// Delete a key from Redis.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key
///
/// # Returns
/// True if key was deleted, False if it didn't exist.
#[pyfunction]
fn py_cache_delete(url: &str, key: &str) -> PyResult<bool> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    rt.block_on(async {
        let client = redis::Client::open(url)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyConnectionError, _>(e.to_string()))?;

        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyConnectionError, _>(e.to_string()))?;

        let deleted: i64 = redis::cmd("DEL")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(deleted > 0)
    })
}

#[cfg(feature = "python")]
/// Check if a key exists in Redis.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key
///
/// # Returns
/// True if key exists, False otherwise.
#[pyfunction]
fn py_cache_exists(url: &str, key: &str) -> PyResult<bool> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    rt.block_on(async {
        let client = redis::Client::open(url)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyConnectionError, _>(e.to_string()))?;

        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyConnectionError, _>(e.to_string()))?;

        let exists: i64 = redis::cmd("EXISTS")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(exists > 0)
    })
}

#[cfg(feature = "python")]
/// Get the TTL of a key in Redis.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key
///
/// # Returns
/// TTL in seconds, or None if key doesn't exist or has no TTL.
#[pyfunction]
fn py_cache_ttl(url: &str, key: &str) -> PyResult<Option<i64>> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    rt.block_on(async {
        let client = redis::Client::open(url)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyConnectionError, _>(e.to_string()))?;

        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyConnectionError, _>(e.to_string()))?;

        let ttl: i64 = redis::cmd("TTL")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        // TTL returns -2 if key doesn't exist, -1 if no TTL
        if ttl < 0 { Ok(None) } else { Ok(Some(ttl)) }
    })
}

// ============================================================================
// Geospatial Python bindings
// ============================================================================

#[cfg(feature = "python")]
/// Add locations to a geo set.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key for the geo set
/// * `locations` - List of (name, longitude, latitude) tuples
///
/// # Returns
/// A dict with `added` and `updated` counts.
#[pyfunction]
fn py_geo_add(
    url: &str,
    key: &str,
    locations: Vec<(String, f64, f64)>,
) -> PyResult<std::collections::HashMap<String, usize>> {
    use crate::client::geo::geo_add;

    let result = geo_add(url, key, &locations)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    let mut dict = std::collections::HashMap::new();
    dict.insert("added".to_string(), result.added);
    dict.insert("updated".to_string(), result.updated);
    Ok(dict)
}

#[cfg(feature = "python")]
/// Query locations within a radius.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key for the geo set
/// * `longitude` - Center longitude
/// * `latitude` - Center latitude
/// * `radius` - Search radius
/// * `unit` - Distance unit (m, km, mi, ft)
/// * `count` - Optional maximum number of results
/// * `sort` - Optional sort order ("ASC" or "DESC")
///
/// # Returns
/// A list of dicts with name, longitude, latitude, distance.
#[pyfunction]
#[pyo3(signature = (url, key, longitude, latitude, radius, unit = "m", count = None, sort = None))]
#[allow(clippy::too_many_arguments)]
fn py_geo_radius(
    url: &str,
    key: &str,
    longitude: f64,
    latitude: f64,
    radius: f64,
    unit: &str,
    count: Option<usize>,
    sort: Option<&str>,
) -> PyResult<Vec<std::collections::HashMap<String, Py<PyAny>>>> {
    use crate::client::geo::{GeoSort, geo_radius};

    let geo_sort = match sort {
        Some("ASC") | Some("asc") => Some(GeoSort::Asc),
        Some("DESC") | Some("desc") => Some(GeoSort::Desc),
        Some(_) => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "sort must be 'ASC' or 'DESC'",
            ));
        },
        None => None,
    };

    let locations = geo_radius(url, key, longitude, latitude, radius, unit, count, geo_sort)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Python::attach(|py| {
        locations
            .into_iter()
            .map(|loc| {
                let mut dict = std::collections::HashMap::new();
                dict.insert(
                    "name".to_string(),
                    loc.name.into_pyobject(py)?.into_any().unbind(),
                );
                dict.insert(
                    "longitude".to_string(),
                    loc.longitude.into_pyobject(py)?.into_any().unbind(),
                );
                dict.insert(
                    "latitude".to_string(),
                    loc.latitude.into_pyobject(py)?.into_any().unbind(),
                );
                dict.insert(
                    "distance".to_string(),
                    loc.distance.into_pyobject(py)?.into_any().unbind(),
                );
                Ok(dict)
            })
            .collect()
    })
}

#[cfg(feature = "python")]
/// Query locations within a radius of another member.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key for the geo set
/// * `member` - Name of the member to search from
/// * `radius` - Search radius
/// * `unit` - Distance unit (m, km, mi, ft)
/// * `count` - Optional maximum number of results
/// * `sort` - Optional sort order ("ASC" or "DESC")
///
/// # Returns
/// A list of dicts with name, longitude, latitude, distance.
#[pyfunction]
#[pyo3(signature = (url, key, member, radius, unit = "m", count = None, sort = None))]
fn py_geo_radius_by_member(
    url: &str,
    key: &str,
    member: &str,
    radius: f64,
    unit: &str,
    count: Option<usize>,
    sort: Option<&str>,
) -> PyResult<Vec<std::collections::HashMap<String, Py<PyAny>>>> {
    use crate::client::geo::{GeoSort, geo_radius_by_member};

    let geo_sort = match sort {
        Some("ASC") | Some("asc") => Some(GeoSort::Asc),
        Some("DESC") | Some("desc") => Some(GeoSort::Desc),
        Some(_) => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "sort must be 'ASC' or 'DESC'",
            ));
        },
        None => None,
    };

    let locations = geo_radius_by_member(url, key, member, radius, unit, count, geo_sort)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Python::attach(|py| {
        locations
            .into_iter()
            .map(|loc| {
                let mut dict = std::collections::HashMap::new();
                dict.insert(
                    "name".to_string(),
                    loc.name.into_pyobject(py)?.into_any().unbind(),
                );
                dict.insert(
                    "longitude".to_string(),
                    loc.longitude.into_pyobject(py)?.into_any().unbind(),
                );
                dict.insert(
                    "latitude".to_string(),
                    loc.latitude.into_pyobject(py)?.into_any().unbind(),
                );
                dict.insert(
                    "distance".to_string(),
                    loc.distance.into_pyobject(py)?.into_any().unbind(),
                );
                Ok(dict)
            })
            .collect()
    })
}

#[cfg(feature = "python")]
/// Get distance between two members.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key for the geo set
/// * `member1` - First member name
/// * `member2` - Second member name
/// * `unit` - Distance unit (m, km, mi, ft)
///
/// # Returns
/// Distance as float, or None if either member doesn't exist.
#[pyfunction]
#[pyo3(signature = (url, key, member1, member2, unit = "m"))]
fn py_geo_dist(
    url: &str,
    key: &str,
    member1: &str,
    member2: &str,
    unit: &str,
) -> PyResult<Option<f64>> {
    use crate::client::geo::geo_dist;

    geo_dist(url, key, member1, member2, unit)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
}

#[cfg(feature = "python")]
/// Get positions of members.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key for the geo set
/// * `members` - List of member names
///
/// # Returns
/// A list of dicts with name, longitude, latitude.
#[pyfunction]
fn py_geo_pos(
    url: &str,
    key: &str,
    members: Vec<String>,
) -> PyResult<Vec<std::collections::HashMap<String, Py<PyAny>>>> {
    use crate::client::geo::geo_pos;

    let locations = geo_pos(url, key, &members)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Python::attach(|py| {
        locations
            .into_iter()
            .map(|loc| {
                let mut dict = std::collections::HashMap::new();
                dict.insert(
                    "name".to_string(),
                    loc.name.into_pyobject(py)?.into_any().unbind(),
                );
                dict.insert(
                    "longitude".to_string(),
                    loc.longitude.into_pyobject(py)?.into_any().unbind(),
                );
                dict.insert(
                    "latitude".to_string(),
                    loc.latitude.into_pyobject(py)?.into_any().unbind(),
                );
                Ok(dict)
            })
            .collect()
    })
}

#[cfg(feature = "python")]
/// Calculate distance matrix between members.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key for the geo set
/// * `members` - List of member names
/// * `unit` - Distance unit (m, km, mi, ft)
///
/// # Returns
/// A 2D list of distances where result[i][j] is distance from members[i] to members[j].
#[pyfunction]
#[pyo3(signature = (url, key, members, unit = "m"))]
fn py_geo_dist_matrix(
    url: &str,
    key: &str,
    members: Vec<String>,
    unit: &str,
) -> PyResult<Vec<Vec<Option<f64>>>> {
    use crate::client::geo::geo_dist_matrix;

    geo_dist_matrix(url, key, &members, unit)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
}

#[cfg(feature = "python")]
/// Get geohash strings for members.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key for the geo set
/// * `members` - List of member names
///
/// # Returns
/// A list of (name, geohash) tuples. Geohash is None if member doesn't exist.
#[pyfunction]
fn py_geo_hash(
    url: &str,
    key: &str,
    members: Vec<String>,
) -> PyResult<Vec<(String, Option<String>)>> {
    use crate::client::geo::geo_hash;

    geo_hash(url, key, &members)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
}

// ============================================================================
// Key management Python bindings
// ============================================================================

#[cfg(feature = "python")]
/// Get information about keys matching a pattern.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `pattern` - Key pattern to match (e.g., "user:*")
/// * `include_memory` - Whether to include memory usage (slower)
///
/// # Returns
/// A list of dicts with key info: key, key_type, ttl, memory_usage, encoding.
#[pyfunction]
#[pyo3(signature = (url, pattern, include_memory = false))]
fn py_key_info(
    url: &str,
    pattern: &str,
    include_memory: bool,
) -> PyResult<Vec<std::collections::HashMap<String, Py<PyAny>>>> {
    use crate::client::keys::key_info;

    let info = key_info(url, pattern, Some(include_memory))
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Python::attach(|py| {
        info.into_iter()
            .map(|ki| {
                let mut dict = std::collections::HashMap::new();
                dict.insert(
                    "key".to_string(),
                    ki.key.into_pyobject(py)?.into_any().unbind(),
                );
                dict.insert(
                    "key_type".to_string(),
                    ki.key_type.into_pyobject(py)?.into_any().unbind(),
                );
                dict.insert(
                    "ttl".to_string(),
                    ki.ttl.into_pyobject(py)?.into_any().unbind(),
                );
                dict.insert(
                    "memory_usage".to_string(),
                    ki.memory_usage.into_pyobject(py)?.into_any().unbind(),
                );
                dict.insert(
                    "encoding".to_string(),
                    ki.encoding.into_pyobject(py)?.into_any().unbind(),
                );
                Ok(dict)
            })
            .collect()
    })
}

#[cfg(feature = "python")]
/// Set TTL for multiple keys.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - List of keys to set TTL for
/// * `ttl_seconds` - TTL in seconds
///
/// # Returns
/// A dict with `succeeded`, `failed`, and `errors`.
#[pyfunction]
fn py_set_ttl(
    url: &str,
    keys: Vec<String>,
    ttl_seconds: i64,
) -> PyResult<std::collections::HashMap<String, Py<PyAny>>> {
    use crate::client::keys::set_ttl;

    let result = set_ttl(url, &keys, ttl_seconds)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Python::attach(|py| {
        let mut dict = std::collections::HashMap::new();
        dict.insert(
            "succeeded".to_string(),
            result.succeeded.into_pyobject(py)?.into_any().unbind(),
        );
        dict.insert(
            "failed".to_string(),
            result.failed.into_pyobject(py)?.into_any().unbind(),
        );
        dict.insert(
            "errors".to_string(),
            result.errors.into_pyobject(py)?.into_any().unbind(),
        );
        Ok(dict)
    })
}

#[cfg(feature = "python")]
/// Set TTL for multiple keys with individual TTL values.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys_and_ttls` - List of (key, ttl_seconds) tuples
///
/// # Returns
/// A dict with `succeeded`, `failed`, and `errors`.
#[pyfunction]
fn py_set_ttl_individual(
    url: &str,
    keys_and_ttls: Vec<(String, i64)>,
) -> PyResult<std::collections::HashMap<String, Py<PyAny>>> {
    use crate::client::keys::set_ttl_individual;

    let result = set_ttl_individual(url, &keys_and_ttls)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Python::attach(|py| {
        let mut dict = std::collections::HashMap::new();
        dict.insert(
            "succeeded".to_string(),
            result.succeeded.into_pyobject(py)?.into_any().unbind(),
        );
        dict.insert(
            "failed".to_string(),
            result.failed.into_pyobject(py)?.into_any().unbind(),
        );
        dict.insert(
            "errors".to_string(),
            result.errors.into_pyobject(py)?.into_any().unbind(),
        );
        Ok(dict)
    })
}

#[cfg(feature = "python")]
/// Delete multiple keys.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - List of keys to delete
///
/// # Returns
/// A dict with `deleted` and `not_found`.
#[pyfunction]
fn py_delete_keys(
    url: &str,
    keys: Vec<String>,
) -> PyResult<std::collections::HashMap<String, usize>> {
    use crate::client::keys::delete_keys;

    let result = delete_keys(url, &keys)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    let mut dict = std::collections::HashMap::new();
    dict.insert("deleted".to_string(), result.deleted);
    dict.insert("not_found".to_string(), result.not_found);
    Ok(dict)
}

#[cfg(feature = "python")]
/// Delete keys matching a pattern.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `pattern` - Key pattern to match
///
/// # Returns
/// A dict with `deleted` and `not_found`.
#[pyfunction]
fn py_delete_keys_pattern(
    url: &str,
    pattern: &str,
) -> PyResult<std::collections::HashMap<String, usize>> {
    use crate::client::keys::delete_keys_pattern;

    let result = delete_keys_pattern(url, pattern)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    let mut dict = std::collections::HashMap::new();
    dict.insert("deleted".to_string(), result.deleted);
    dict.insert("not_found".to_string(), result.not_found);
    Ok(dict)
}

#[cfg(feature = "python")]
/// Rename multiple keys.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `renames` - List of (old_key, new_key) tuples
///
/// # Returns
/// A dict with `succeeded`, `failed`, and `errors`.
#[pyfunction]
fn py_rename_keys(
    url: &str,
    renames: Vec<(String, String)>,
) -> PyResult<std::collections::HashMap<String, Py<PyAny>>> {
    use crate::client::keys::rename_keys;

    let result = rename_keys(url, &renames)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Python::attach(|py| {
        let mut dict = std::collections::HashMap::new();
        dict.insert(
            "succeeded".to_string(),
            result.succeeded.into_pyobject(py)?.into_any().unbind(),
        );
        dict.insert(
            "failed".to_string(),
            result.failed.into_pyobject(py)?.into_any().unbind(),
        );
        dict.insert(
            "errors".to_string(),
            result.errors.into_pyobject(py)?.into_any().unbind(),
        );
        Ok(dict)
    })
}

#[cfg(feature = "python")]
/// Remove TTL from keys (make them persistent).
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - List of keys to persist
///
/// # Returns
/// A dict with `succeeded`, `failed`, and `errors`.
#[pyfunction]
fn py_persist_keys(
    url: &str,
    keys: Vec<String>,
) -> PyResult<std::collections::HashMap<String, Py<PyAny>>> {
    use crate::client::keys::persist_keys;

    let result = persist_keys(url, &keys)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    Python::attach(|py| {
        let mut dict = std::collections::HashMap::new();
        dict.insert(
            "succeeded".to_string(),
            result.succeeded.into_pyobject(py)?.into_any().unbind(),
        );
        dict.insert(
            "failed".to_string(),
            result.failed.into_pyobject(py)?.into_any().unbind(),
        );
        dict.insert(
            "errors".to_string(),
            result.errors.into_pyobject(py)?.into_any().unbind(),
        );
        Ok(dict)
    })
}

#[cfg(feature = "python")]
/// Check if keys exist.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - List of keys to check
///
/// # Returns
/// A list of (key, exists) tuples.
#[pyfunction]
fn py_exists_keys(url: &str, keys: Vec<String>) -> PyResult<Vec<(String, bool)>> {
    use crate::client::keys::exists_keys;

    exists_keys(url, &keys)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
}

#[cfg(feature = "python")]
/// Get TTL for multiple keys.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - List of keys to get TTL for
///
/// # Returns
/// A list of (key, ttl) tuples. TTL is -1 if no expiry, -2 if key doesn't exist.
#[pyfunction]
fn py_get_ttl(url: &str, keys: Vec<String>) -> PyResult<Vec<(String, i64)>> {
    use crate::client::keys::get_ttl;

    get_ttl(url, &keys)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))
}

// ============================================================================
// Cluster Python bindings (with cluster + python features)
// ============================================================================

#[cfg(all(feature = "python", feature = "cluster"))]
/// Python wrapper for ClusterHashBatchIterator.
///
/// This class is used by the Python IO plugin to iterate over Redis Cluster hash data
/// and yield Arrow RecordBatches.
#[pyclass]
pub struct PyClusterHashBatchIterator {
    inner: ClusterHashBatchIterator,
}

#[cfg(all(feature = "python", feature = "cluster"))]
#[pymethods]
impl PyClusterHashBatchIterator {
    /// Create a new PyClusterHashBatchIterator.
    ///
    /// # Arguments
    /// * `nodes` - List of cluster node URLs
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
    /// * `parallel` - Optional number of parallel workers for fetching
    #[new]
    #[pyo3(signature = (
        nodes,
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
        max_rows = None,
        parallel = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        nodes: Vec<String>,
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
        parallel: Option<usize>,
    ) -> PyResult<Self> {
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

        if let Some(workers) = parallel {
            config = config.with_parallel(ParallelStrategy::batches(workers));
        }

        let inner = ClusterHashBatchIterator::new(&nodes, hash_schema, config, projection)
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
            },
            None => Ok(None),
        }
    }

    fn is_done(&self) -> bool {
        self.inner.is_done()
    }

    fn rows_yielded(&self) -> usize {
        self.inner.rows_yielded()
    }

    fn node_count(&self) -> usize {
        self.inner.node_count()
    }
}

#[cfg(all(feature = "python", feature = "cluster"))]
/// Python wrapper for ClusterJsonBatchIterator.
#[pyclass]
pub struct PyClusterJsonBatchIterator {
    inner: ClusterJsonBatchIterator,
}

#[cfg(all(feature = "python", feature = "cluster"))]
#[pymethods]
impl PyClusterJsonBatchIterator {
    #[new]
    #[pyo3(signature = (
        nodes,
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
        max_rows = None,
        parallel = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        nodes: Vec<String>,
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
        parallel: Option<usize>,
    ) -> PyResult<Self> {
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

        if let Some(workers) = parallel {
            config = config.with_parallel(ParallelStrategy::batches(workers));
        }

        let inner = ClusterJsonBatchIterator::new(&nodes, json_schema, config, projection)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(Self { inner })
    }

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
            },
            None => Ok(None),
        }
    }

    fn is_done(&self) -> bool {
        self.inner.is_done()
    }

    fn rows_yielded(&self) -> usize {
        self.inner.rows_yielded()
    }

    fn node_count(&self) -> usize {
        self.inner.node_count()
    }
}

#[cfg(all(feature = "python", feature = "cluster"))]
/// Python wrapper for ClusterStringBatchIterator.
#[pyclass]
pub struct PyClusterStringBatchIterator {
    inner: ClusterStringBatchIterator,
}

#[cfg(all(feature = "python", feature = "cluster"))]
#[pymethods]
impl PyClusterStringBatchIterator {
    #[new]
    #[pyo3(signature = (
        nodes,
        pattern,
        value_type = "utf8".to_string(),
        batch_size = 1000,
        count_hint = 100,
        include_key = true,
        key_column_name = "_key".to_string(),
        value_column_name = "value".to_string(),
        include_ttl = false,
        ttl_column_name = "_ttl".to_string(),
        max_rows = None,
        parallel = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        nodes: Vec<String>,
        pattern: String,
        value_type: String,
        batch_size: usize,
        count_hint: usize,
        include_key: bool,
        key_column_name: String,
        value_column_name: String,
        include_ttl: bool,
        ttl_column_name: String,
        max_rows: Option<usize>,
        parallel: Option<usize>,
    ) -> PyResult<Self> {
        use arrow::datatypes::TimeUnit;

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
            },
        };

        let string_schema = StringSchema::new(dtype)
            .with_key(include_key)
            .with_key_column_name(key_column_name)
            .with_value_column_name(value_column_name)
            .with_ttl(include_ttl)
            .with_ttl_column_name(ttl_column_name);

        let mut config = BatchConfig::new(pattern)
            .with_batch_size(batch_size)
            .with_count_hint(count_hint);

        if let Some(max) = max_rows {
            config = config.with_max_rows(max);
        }

        if let Some(workers) = parallel {
            config = config.with_parallel(ParallelStrategy::batches(workers));
        }

        let inner = ClusterStringBatchIterator::new(&nodes, string_schema, config)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(Self { inner })
    }

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
            },
            None => Ok(None),
        }
    }

    fn is_done(&self) -> bool {
        self.inner.is_done()
    }

    fn rows_yielded(&self) -> usize {
        self.inner.rows_yielded()
    }

    fn node_count(&self) -> usize {
        self.inner.node_count()
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
            },
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

#[cfg(all(feature = "python", feature = "search"))]
/// Execute FT.AGGREGATE and return aggregated results as a list of dictionaries.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `index` - RediSearch index name
/// * `query` - RediSearch query string (e.g., "@status:active", "*" for all)
/// * `group_by` - List of field names to group by
/// * `reduce` - List of reduce operations as (function, args, alias) tuples
/// * `apply` - Optional list of apply expressions as (expression, alias) tuples
/// * `filter` - Optional post-aggregation filter expression
/// * `sort_by` - Optional list of sort specifications as (field, ascending) tuples
/// * `limit` - Optional maximum number of results
/// * `offset` - Offset for pagination (default: 0)
/// * `load` - Optional list of fields to load from documents
///
/// # Returns
/// A list of dictionaries, where each dictionary represents an aggregated row.
///
/// # Example
/// ```python
/// result = py_aggregate(
///     "redis://localhost:6379",
///     "users_idx",
///     "*",
///     group_by=["city"],
///     reduce=[("COUNT", [], "user_count"), ("AVG", ["age"], "avg_age")],
///     sort_by=[("user_count", False)],
///     limit=10,
/// )
/// for row in result:
///     print(f"{row['city']}: {row['user_count']} users, avg age {row['avg_age']}")
/// ```
#[pyfunction]
#[pyo3(signature = (
    url,
    index,
    query,
    group_by = vec![],
    reduce = vec![],
    apply = None,
    filter = None,
    sort_by = None,
    limit = None,
    offset = 0,
    load = None
))]
#[allow(clippy::too_many_arguments)]
fn py_aggregate(
    url: &str,
    index: &str,
    query: &str,
    group_by: Vec<String>,
    reduce: Vec<(String, Vec<String>, String)>,
    apply: Option<Vec<(String, String)>>,
    filter: Option<String>,
    sort_by: Option<Vec<(String, bool)>>,
    limit: Option<usize>,
    offset: usize,
    load: Option<Vec<String>>,
) -> PyResult<Vec<std::collections::HashMap<String, String>>> {
    use crate::connection::RedisConnection;
    use crate::io::search::{AggregateConfig, ApplyExpr, ReduceOp, SortBy, aggregate};

    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    rt.block_on(async {
        let connection = RedisConnection::new(url)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyConnectionError, _>(e.to_string()))?;

        let mut conn = connection
            .get_connection_manager()
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyConnectionError, _>(e.to_string()))?;

        // Build reduce operations
        let reduce_ops: Vec<ReduceOp> = reduce
            .into_iter()
            .map(|(func, args, alias)| ReduceOp::new(func, args, alias))
            .collect();

        // Build apply expressions
        let apply_exprs: Vec<ApplyExpr> = apply
            .unwrap_or_default()
            .into_iter()
            .map(|(expr, alias)| ApplyExpr::new(expr, alias))
            .collect();

        // Build sort specifications
        let sort_specs: Vec<SortBy> = sort_by
            .unwrap_or_default()
            .into_iter()
            .map(|(field, ascending)| {
                if ascending {
                    SortBy::asc(field)
                } else {
                    SortBy::desc(field)
                }
            })
            .collect();

        // Build config
        let mut config = AggregateConfig::new(index, query)
            .with_group_by(group_by)
            .with_reduce(reduce_ops)
            .with_apply(apply_exprs)
            .with_sort_by(sort_specs)
            .with_offset(offset);

        if let Some(f) = filter {
            config = config.with_filter(f);
        }

        if let Some(l) = limit {
            config = config.with_limit(l);
        }

        if let Some(fields) = load {
            config = config.with_load(fields);
        }

        // Execute aggregate
        let result = aggregate(&mut conn, &config)
            .await
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(result.rows)
    })
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

        let mut config = io::types::hash::BatchConfig::new(pattern)
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
            },
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
    /// * `parallel` - Optional number of parallel workers for fetching
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
        max_rows = None,
        parallel = None
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
        parallel: Option<usize>,
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

        if let Some(workers) = parallel {
            config = config.with_parallel(ParallelStrategy::batches(workers));
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
            },
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
    /// * `include_ttl` - Whether to include the TTL as a column
    /// * `ttl_column_name` - Name of the TTL column
    /// * `max_rows` - Optional maximum rows to return
    /// * `parallel` - Optional number of parallel workers for fetching
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
        include_ttl = false,
        ttl_column_name = "_ttl".to_string(),
        max_rows = None,
        parallel = None
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
        include_ttl: bool,
        ttl_column_name: String,
        max_rows: Option<usize>,
        parallel: Option<usize>,
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
            },
        };

        let string_schema = StringSchema::new(dtype)
            .with_key(include_key)
            .with_key_column_name(key_column_name)
            .with_value_column_name(value_column_name)
            .with_ttl(include_ttl)
            .with_ttl_column_name(ttl_column_name);

        let mut config = BatchConfig::new(pattern)
            .with_batch_size(batch_size)
            .with_count_hint(count_hint);

        if let Some(max) = max_rows {
            config = config.with_max_rows(max);
        }

        if let Some(workers) = parallel {
            config = config.with_parallel(ParallelStrategy::batches(workers));
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
            },
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
/// Infer schema from Redis hashes with optional schema overwrite.
///
/// This function infers a schema by sampling Redis hashes, then applies
/// user-specified type overrides. This is useful when you want to infer
/// most fields but override specific ones.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `pattern` - Key pattern to match
/// * `schema_overwrite` - Optional list of (field_name, type_string) tuples to override
/// * `sample_size` - Maximum number of keys to sample (default: 100)
/// * `type_inference` - Whether to infer types (default: true)
///
/// # Returns
/// A tuple of (fields, sample_count) where fields is a list of (name, type) tuples.
///
/// # Example
/// ```python
/// # Infer schema but force 'age' to be int64 and 'created_at' to be datetime
/// schema, count = py_infer_hash_schema_with_overwrite(
///     "redis://localhost",
///     "user:*",
///     [("age", "int64"), ("created_at", "datetime")],
/// )
/// ```
#[pyfunction]
#[pyo3(signature = (url, pattern, schema_overwrite = None, sample_size = 100, type_inference = true))]
fn py_infer_hash_schema_with_overwrite(
    url: &str,
    pattern: &str,
    schema_overwrite: Option<Vec<(String, String)>>,
    sample_size: usize,
    type_inference: bool,
) -> PyResult<(Vec<(String, String)>, usize)> {
    use crate::schema::RedisType;

    let schema = infer_hash_schema(url, pattern, sample_size, type_inference)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    // Apply overwrite if provided
    let final_schema = if let Some(overwrite) = schema_overwrite {
        let overwrite_typed: Vec<(String, RedisType)> = overwrite
            .into_iter()
            .map(|(name, type_str)| {
                let redis_type = match type_str.as_str() {
                    "utf8" | "string" => RedisType::Utf8,
                    "int64" | "integer" => RedisType::Int64,
                    "float64" | "float" => RedisType::Float64,
                    "bool" | "boolean" => RedisType::Boolean,
                    "date" => RedisType::Date,
                    "datetime" => RedisType::Datetime,
                    _ => RedisType::Utf8,
                };
                (name, redis_type)
            })
            .collect();
        schema.with_overwrite(&overwrite_typed)
    } else {
        schema
    };

    Ok((final_schema.to_type_strings(), final_schema.sample_count))
}

#[cfg(feature = "python")]
/// Infer schema from RedisJSON documents with optional schema overwrite.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `pattern` - Key pattern to match
/// * `schema_overwrite` - Optional list of (field_name, type_string) tuples to override
/// * `sample_size` - Maximum number of keys to sample (default: 100)
///
/// # Returns
/// A tuple of (fields, sample_count) where fields is a list of (name, type) tuples.
#[pyfunction]
#[pyo3(signature = (url, pattern, schema_overwrite = None, sample_size = 100))]
fn py_infer_json_schema_with_overwrite(
    url: &str,
    pattern: &str,
    schema_overwrite: Option<Vec<(String, String)>>,
    sample_size: usize,
) -> PyResult<(Vec<(String, String)>, usize)> {
    use crate::schema::RedisType;

    let schema = infer_json_schema(url, pattern, sample_size)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    // Apply overwrite if provided
    let final_schema = if let Some(overwrite) = schema_overwrite {
        let overwrite_typed: Vec<(String, RedisType)> = overwrite
            .into_iter()
            .map(|(name, type_str)| {
                let redis_type = match type_str.as_str() {
                    "utf8" | "string" => RedisType::Utf8,
                    "int64" | "integer" => RedisType::Int64,
                    "float64" | "float" => RedisType::Float64,
                    "bool" | "boolean" => RedisType::Boolean,
                    "date" => RedisType::Date,
                    "datetime" => RedisType::Datetime,
                    _ => RedisType::Utf8,
                };
                (name, redis_type)
            })
            .collect();
        schema.with_overwrite(&overwrite_typed)
    } else {
        schema
    };

    Ok((final_schema.to_type_strings(), final_schema.sample_count))
}

#[cfg(feature = "python")]
/// Infer schema from Redis hashes with detailed confidence information.
///
/// This function returns confidence scores for each field, indicating how
/// reliably the type was inferred. Use this when you need to validate
/// schema quality before processing large datasets.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `pattern` - Key pattern to match
/// * `sample_size` - Maximum number of keys to sample (default: 100)
///
/// # Returns
/// A dict with:
/// - `fields`: List of (name, type) tuples
/// - `sample_count`: Number of keys sampled
/// - `field_info`: Dict mapping field names to confidence info dicts
/// - `average_confidence`: Overall average confidence score
/// - `all_confident`: Whether all fields have confidence >= 0.9
///
/// Each field_info dict contains:
/// - `type`: Inferred type name
/// - `confidence`: Score from 0.0 to 1.0
/// - `samples`: Total samples for this field
/// - `valid`: Number of samples matching the inferred type
/// - `nulls`: Number of null/missing values
/// - `null_ratio`: Percentage of null values
/// - `type_candidates`: Dict of type names to match counts
#[pyfunction]
#[pyo3(signature = (url, pattern, sample_size = 100))]
fn py_infer_hash_schema_with_confidence(
    py: Python<'_>,
    url: &str,
    pattern: &str,
    sample_size: usize,
) -> PyResult<HashMap<String, Py<PyAny>>> {
    let schema = infer_hash_schema_with_confidence(url, pattern, sample_size)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    let mut result: HashMap<String, Py<PyAny>> = HashMap::new();

    // Convert fields to Python list of tuples
    let fields: Vec<(String, String)> = schema
        .fields
        .iter()
        .map(|(name, dtype)| {
            let type_str = match dtype {
                crate::schema::RedisType::Utf8 => "utf8",
                crate::schema::RedisType::Int64 => "int64",
                crate::schema::RedisType::Float64 => "float64",
                crate::schema::RedisType::Boolean => "bool",
                crate::schema::RedisType::Date => "date",
                crate::schema::RedisType::Datetime => "datetime",
            };
            (name.clone(), type_str.to_string())
        })
        .collect();

    result.insert(
        "fields".to_string(),
        fields.into_pyobject(py)?.into_any().unbind(),
    );
    result.insert(
        "sample_count".to_string(),
        schema.sample_count.into_pyobject(py)?.into_any().unbind(),
    );

    // Convert field_info to Python dict
    let mut field_info_py: HashMap<String, HashMap<String, Py<PyAny>>> = HashMap::new();
    for (name, info) in &schema.field_info {
        let mut info_dict: HashMap<String, Py<PyAny>> = HashMap::new();
        let type_str = match info.inferred_type {
            crate::schema::RedisType::Utf8 => "utf8",
            crate::schema::RedisType::Int64 => "int64",
            crate::schema::RedisType::Float64 => "float64",
            crate::schema::RedisType::Boolean => "bool",
            crate::schema::RedisType::Date => "date",
            crate::schema::RedisType::Datetime => "datetime",
        };
        info_dict.insert(
            "type".to_string(),
            type_str.into_pyobject(py)?.into_any().unbind(),
        );
        info_dict.insert(
            "confidence".to_string(),
            info.confidence.into_pyobject(py)?.into_any().unbind(),
        );
        info_dict.insert(
            "samples".to_string(),
            info.samples.into_pyobject(py)?.into_any().unbind(),
        );
        info_dict.insert(
            "valid".to_string(),
            info.valid.into_pyobject(py)?.into_any().unbind(),
        );
        info_dict.insert(
            "nulls".to_string(),
            info.nulls.into_pyobject(py)?.into_any().unbind(),
        );
        info_dict.insert(
            "null_ratio".to_string(),
            info.null_ratio().into_pyobject(py)?.into_any().unbind(),
        );
        info_dict.insert(
            "type_candidates".to_string(),
            info.type_candidates
                .clone()
                .into_pyobject(py)?
                .into_any()
                .unbind(),
        );

        field_info_py.insert(name.clone(), info_dict);
    }
    result.insert(
        "field_info".to_string(),
        field_info_py.into_pyobject(py)?.into_any().unbind(),
    );

    // Add convenience fields
    result.insert(
        "average_confidence".to_string(),
        schema
            .average_confidence()
            .into_pyobject(py)?
            .into_any()
            .unbind(),
    );
    // Boolean needs special handling due to PyO3's Borrowed type
    let all_confident_bool = pyo3::types::PyBool::new(py, schema.all_confident(0.9));
    result.insert(
        "all_confident".to_string(),
        all_confident_bool.to_owned().into_any().unbind(),
    );

    Ok(result)
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
/// Write hashes to Redis with detailed per-key error reporting.
///
/// This is similar to `py_write_hashes` but returns detailed information about
/// which specific keys succeeded or failed, enabling retry logic and better
/// error handling in production workflows.
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
/// A dict with keys:
/// - `keys_written`: Number of keys successfully written
/// - `keys_failed`: Number of keys that failed
/// - `keys_skipped`: Number of keys skipped (when mode is "fail" and key exists)
/// - `succeeded_keys`: List of keys that were successfully written
/// - `failed_keys`: List of keys that failed to write
/// - `errors`: Dict mapping failed keys to their error messages
#[pyfunction]
#[pyo3(signature = (url, keys, fields, values, ttl = None, if_exists = "replace".to_string()))]
fn py_write_hashes_detailed(
    url: &str,
    keys: Vec<String>,
    fields: Vec<String>,
    values: Vec<Vec<Option<String>>>,
    ttl: Option<i64>,
    if_exists: String,
) -> PyResult<std::collections::HashMap<String, Py<PyAny>>> {
    use pyo3::IntoPyObjectExt;

    let mode: WriteMode = if_exists.parse().map_err(|e: crate::Error| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(e.to_string())
    })?;

    let result = write_hashes_detailed(url, keys, fields, values, ttl, mode)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

    // Build the response dictionary
    Python::attach(|py| {
        let mut dict = std::collections::HashMap::new();

        dict.insert(
            "keys_written".to_string(),
            result.keys_written.into_py_any(py)?,
        );
        dict.insert(
            "keys_failed".to_string(),
            result.keys_failed.into_py_any(py)?,
        );
        dict.insert(
            "keys_skipped".to_string(),
            result.keys_skipped.into_py_any(py)?,
        );
        dict.insert(
            "succeeded_keys".to_string(),
            result.succeeded_keys.into_py_any(py)?,
        );

        // Build failed_keys list and errors dict
        let failed_keys: Vec<String> = result.errors.iter().map(|e| e.key.clone()).collect();
        dict.insert("failed_keys".to_string(), failed_keys.into_py_any(py)?);

        let errors: std::collections::HashMap<String, String> = result
            .errors
            .into_iter()
            .map(|e| (e.key, e.error))
            .collect();
        dict.insert("errors".to_string(), errors.into_py_any(py)?);

        Ok(dict)
    })
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

        let mut config = io::types::hash::BatchConfig::new(pattern)
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
            },
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

        let mut config = io::types::hash::BatchConfig::new(pattern)
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
            },
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

        let mut config = io::types::hash::BatchConfig::new(pattern)
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
            },
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

        let mut config = io::types::hash::BatchConfig::new(pattern)
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
            },
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
