//! DataFrame caching in Redis using Arrow IPC or Parquet format.
//!
//! This module provides functions for caching Arrow RecordBatches in Redis,
//! enabling use of Redis as a high-performance distributed cache for
//! intermediate computation results.
//!
//! For large datasets, data is automatically chunked across multiple Redis keys
//! to avoid memory issues and Redis' 512MB value limit.
//!
//! # Example
//!
//! ```no_run
//! use arrow::array::{Int64Array, StringArray};
//! use arrow::datatypes::{DataType, Field, Schema};
//! use arrow::record_batch::RecordBatch;
//! use polars_redis::cache::{cache_record_batch, get_cached_record_batch, CacheFormat, CacheConfig};
//! use std::sync::Arc;
//!
//! // Create a RecordBatch
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("id", DataType::Int64, false),
//!     Field::new("name", DataType::Utf8, false),
//! ]));
//!
//! let batch = RecordBatch::try_new(
//!     schema,
//!     vec![
//!         Arc::new(Int64Array::from(vec![1, 2, 3])),
//!         Arc::new(StringArray::from(vec!["a", "b", "c"])),
//!     ],
//! ).unwrap();
//!
//! // Cache with default config (IPC format, no compression)
//! let config = CacheConfig::default();
//! cache_record_batch("redis://localhost:6379", "my_result", &batch, &config).unwrap();
//!
//! // Retrieve later
//! let cached = get_cached_record_batch("redis://localhost:6379", "my_result").unwrap();
//! assert!(cached.is_some());
//! ```

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{RecordBatch, RecordBatchReader};
use arrow::ipc::reader::FileReader as IpcReader;
use arrow::ipc::writer::FileWriter as IpcWriter;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::Compression as ParquetCompression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};

use crate::connection::RedisConnection;
use crate::error::{Error, Result};

/// Default chunk size in bytes (100 MB).
pub const DEFAULT_CHUNK_SIZE: usize = 100 * 1024 * 1024;

/// Serialization format for cached data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CacheFormat {
    /// Arrow IPC format (fast serialization/deserialization).
    #[default]
    Ipc,
    /// Parquet format (better compression, supports projection).
    Parquet,
}

impl CacheFormat {
    fn as_str(&self) -> &'static str {
        match self {
            CacheFormat::Ipc => "ipc",
            CacheFormat::Parquet => "parquet",
        }
    }

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "ipc" => Ok(CacheFormat::Ipc),
            "parquet" => Ok(CacheFormat::Parquet),
            _ => Err(Error::InvalidInput(format!("Invalid cache format: {}", s))),
        }
    }
}

/// Compression options for IPC format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IpcCompression {
    #[default]
    Uncompressed,
    Lz4,
    Zstd,
}

/// Compression options for Parquet format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ParquetCompressionType {
    Uncompressed,
    Snappy,
    Lz4,
    #[default]
    Zstd,
}

/// Configuration for caching operations.
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Serialization format.
    pub format: CacheFormat,
    /// IPC compression (only used when format is IPC).
    pub ipc_compression: IpcCompression,
    /// Parquet compression (only used when format is Parquet).
    pub parquet_compression: ParquetCompressionType,
    /// Compression level (codec-specific, only for zstd/gzip).
    pub compression_level: Option<i32>,
    /// Time-to-live in seconds. None means no expiration.
    pub ttl: Option<i64>,
    /// Chunk size in bytes. Set to 0 to disable chunking.
    pub chunk_size: usize,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            format: CacheFormat::Ipc,
            ipc_compression: IpcCompression::Uncompressed,
            parquet_compression: ParquetCompressionType::Zstd,
            compression_level: None,
            ttl: None,
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }
}

impl CacheConfig {
    /// Create a new config with IPC format.
    pub fn ipc() -> Self {
        Self::default()
    }

    /// Create a new config with Parquet format.
    pub fn parquet() -> Self {
        Self {
            format: CacheFormat::Parquet,
            ..Default::default()
        }
    }

    /// Set the TTL in seconds.
    pub fn with_ttl(mut self, seconds: i64) -> Self {
        self.ttl = Some(seconds);
        self
    }

    /// Set IPC compression.
    pub fn with_ipc_compression(mut self, compression: IpcCompression) -> Self {
        self.ipc_compression = compression;
        self
    }

    /// Set Parquet compression.
    pub fn with_parquet_compression(mut self, compression: ParquetCompressionType) -> Self {
        self.parquet_compression = compression;
        self
    }

    /// Set compression level.
    pub fn with_compression_level(mut self, level: i32) -> Self {
        self.compression_level = Some(level);
        self
    }

    /// Set chunk size in bytes. Set to 0 to disable chunking.
    pub fn with_chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = size;
        self
    }

    /// Disable chunking.
    pub fn without_chunking(mut self) -> Self {
        self.chunk_size = 0;
        self
    }
}

/// Metadata stored for chunked data.
#[derive(Debug, Serialize, Deserialize)]
struct ChunkMetadata {
    format: String,
    total_size: usize,
    num_chunks: usize,
    chunk_size: usize,
}

/// Information about cached data.
#[derive(Debug, Clone)]
pub struct CacheInfo {
    /// Serialization format used.
    pub format: CacheFormat,
    /// Total size in bytes.
    pub size_bytes: usize,
    /// Whether data is stored in chunks.
    pub is_chunked: bool,
    /// Number of chunks (1 if not chunked).
    pub num_chunks: usize,
    /// Size of each chunk in bytes.
    pub chunk_size: usize,
    /// Remaining TTL in seconds, or None if no TTL.
    pub ttl: Option<i64>,
}

// ============================================================================
// Core cache functions
// ============================================================================

/// Cache a RecordBatch in Redis.
///
/// Serializes the RecordBatch using the configured format and stores it in Redis.
/// For large data, it is automatically chunked across multiple keys.
///
/// # Arguments
/// * `url` - Redis connection URL (e.g., "redis://localhost:6379")
/// * `key` - Redis key for storage
/// * `batch` - The RecordBatch to cache
/// * `config` - Cache configuration
///
/// # Returns
/// Number of bytes written to Redis.
pub fn cache_record_batch(
    url: &str,
    key: &str,
    batch: &RecordBatch,
    config: &CacheConfig,
) -> Result<usize> {
    let data = serialize_batch(batch, config)?;

    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    rt.block_on(async { cache_bytes_async(url, key, &data, config).await })
}

/// Retrieve a cached RecordBatch from Redis.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key to retrieve
///
/// # Returns
/// The cached RecordBatch, or None if the key doesn't exist.
pub fn get_cached_record_batch(url: &str, key: &str) -> Result<Option<RecordBatch>> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    rt.block_on(async { get_cached_async(url, key).await })
}

/// Delete a cached RecordBatch from Redis.
///
/// Handles both single-key and chunked storage.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key to delete
///
/// # Returns
/// True if the key was deleted, False if it didn't exist.
pub fn delete_cached(url: &str, key: &str) -> Result<bool> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    rt.block_on(async { delete_cached_async(url, key).await })
}

/// Check if a cached RecordBatch exists in Redis.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key to check
///
/// # Returns
/// True if the key exists.
pub fn cache_exists(url: &str, key: &str) -> Result<bool> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    rt.block_on(async { cache_exists_async(url, key).await })
}

/// Get the remaining TTL of a cached RecordBatch.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key to check
///
/// # Returns
/// Remaining TTL in seconds, or None if key doesn't exist or has no TTL.
pub fn cache_ttl(url: &str, key: &str) -> Result<Option<i64>> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    rt.block_on(async { cache_ttl_async(url, key).await })
}

/// Get information about a cached RecordBatch.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key to check
///
/// # Returns
/// Cache info, or None if key doesn't exist.
pub fn cache_info(url: &str, key: &str) -> Result<Option<CacheInfo>> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    rt.block_on(async { cache_info_async(url, key).await })
}

// ============================================================================
// Async implementations
// ============================================================================

async fn cache_bytes_async(
    url: &str,
    key: &str,
    data: &[u8],
    config: &CacheConfig,
) -> Result<usize> {
    let connection = RedisConnection::new(url)?;
    let mut conn = connection.get_connection_manager().await?;

    // If chunking is disabled or data fits in one chunk, store directly
    if config.chunk_size == 0 || data.len() <= config.chunk_size {
        set_with_ttl(&mut conn, key, data, config.ttl).await?;
        return Ok(data.len());
    }

    // Chunked storage
    let total_size = data.len();
    let num_chunks = total_size.div_ceil(config.chunk_size);
    let mut total_written = 0;

    // Store metadata
    let metadata = ChunkMetadata {
        format: config.format.as_str().to_string(),
        total_size,
        num_chunks,
        chunk_size: config.chunk_size,
    };
    let meta_key = format!("{}:meta", key);
    let meta_bytes = serde_json::to_vec(&metadata)
        .map_err(|e| Error::Runtime(format!("Failed to serialize metadata: {}", e)))?;
    set_with_ttl(&mut conn, &meta_key, &meta_bytes, config.ttl).await?;
    total_written += meta_bytes.len();

    // Store chunks
    for i in 0..num_chunks {
        let start = i * config.chunk_size;
        let end = std::cmp::min(start + config.chunk_size, total_size);
        let chunk_data = &data[start..end];
        let chunk_key = format!("{}:chunk:{}", key, i);
        set_with_ttl(&mut conn, &chunk_key, chunk_data, config.ttl).await?;
        total_written += chunk_data.len();
    }

    Ok(total_written)
}

async fn get_cached_async(url: &str, key: &str) -> Result<Option<RecordBatch>> {
    let connection = RedisConnection::new(url)?;
    let mut conn = connection.get_connection_manager().await?;

    // Check for chunked storage first
    let meta_key = format!("{}:meta", key);
    let meta_data: Option<Vec<u8>> = redis::cmd("GET")
        .arg(&meta_key)
        .query_async(&mut conn)
        .await
        .map_err(|e| Error::Runtime(e.to_string()))?;

    let (data, format) = if let Some(meta_bytes) = meta_data {
        // Chunked storage - read metadata and reassemble
        let metadata: ChunkMetadata = serde_json::from_slice(&meta_bytes)
            .map_err(|e| Error::Runtime(format!("Failed to parse metadata: {}", e)))?;

        let mut chunks = Vec::with_capacity(metadata.num_chunks);
        for i in 0..metadata.num_chunks {
            let chunk_key = format!("{}:chunk:{}", key, i);
            let chunk_data: Option<Vec<u8>> = redis::cmd("GET")
                .arg(&chunk_key)
                .query_async(&mut conn)
                .await
                .map_err(|e| Error::Runtime(e.to_string()))?;

            match chunk_data {
                Some(c) => chunks.push(c),
                None => return Ok(None), // Missing chunk - data corrupted or expired
            }
        }

        let data: Vec<u8> = chunks.into_iter().flatten().collect();
        let format = CacheFormat::from_str(&metadata.format)?;
        (data, format)
    } else {
        // Single key storage
        let data: Option<Vec<u8>> = redis::cmd("GET")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(|e| Error::Runtime(e.to_string()))?;

        match data {
            Some(d) => (d, CacheFormat::Ipc), // Default to IPC for single-key storage
            None => return Ok(None),
        }
    };

    let batch = deserialize_batch(&data, format)?;
    Ok(Some(batch))
}

async fn delete_cached_async(url: &str, key: &str) -> Result<bool> {
    let connection = RedisConnection::new(url)?;
    let mut conn = connection.get_connection_manager().await?;

    // Check for chunked storage
    let meta_key = format!("{}:meta", key);
    let meta_data: Option<Vec<u8>> = redis::cmd("GET")
        .arg(&meta_key)
        .query_async(&mut conn)
        .await
        .map_err(|e| Error::Runtime(e.to_string()))?;

    if let Some(meta_bytes) = meta_data {
        // Delete all chunks
        let metadata: ChunkMetadata = serde_json::from_slice(&meta_bytes)
            .map_err(|e| Error::Runtime(format!("Failed to parse metadata: {}", e)))?;

        for i in 0..metadata.num_chunks {
            let chunk_key = format!("{}:chunk:{}", key, i);
            redis::cmd("DEL")
                .arg(&chunk_key)
                .query_async::<i64>(&mut conn)
                .await
                .map_err(|e| Error::Runtime(e.to_string()))?;
        }

        redis::cmd("DEL")
            .arg(&meta_key)
            .query_async::<i64>(&mut conn)
            .await
            .map_err(|e| Error::Runtime(e.to_string()))?;

        Ok(true)
    } else {
        // Single key storage
        let deleted: i64 = redis::cmd("DEL")
            .arg(key)
            .query_async(&mut conn)
            .await
            .map_err(|e| Error::Runtime(e.to_string()))?;

        Ok(deleted > 0)
    }
}

async fn cache_exists_async(url: &str, key: &str) -> Result<bool> {
    let connection = RedisConnection::new(url)?;
    let mut conn = connection.get_connection_manager().await?;

    // Check for chunked storage first
    let meta_key = format!("{}:meta", key);
    let meta_exists: i64 = redis::cmd("EXISTS")
        .arg(&meta_key)
        .query_async(&mut conn)
        .await
        .map_err(|e| Error::Runtime(e.to_string()))?;

    if meta_exists > 0 {
        return Ok(true);
    }

    // Check single key
    let exists: i64 = redis::cmd("EXISTS")
        .arg(key)
        .query_async(&mut conn)
        .await
        .map_err(|e| Error::Runtime(e.to_string()))?;

    Ok(exists > 0)
}

async fn cache_ttl_async(url: &str, key: &str) -> Result<Option<i64>> {
    let connection = RedisConnection::new(url)?;
    let mut conn = connection.get_connection_manager().await?;

    // Check for chunked storage first
    let meta_key = format!("{}:meta", key);
    let meta_exists: i64 = redis::cmd("EXISTS")
        .arg(&meta_key)
        .query_async(&mut conn)
        .await
        .map_err(|e| Error::Runtime(e.to_string()))?;

    let target_key = if meta_exists > 0 { &meta_key } else { key };

    let ttl: i64 = redis::cmd("TTL")
        .arg(target_key)
        .query_async(&mut conn)
        .await
        .map_err(|e| Error::Runtime(e.to_string()))?;

    // TTL returns -2 if key doesn't exist, -1 if no TTL
    if ttl < 0 { Ok(None) } else { Ok(Some(ttl)) }
}

async fn cache_info_async(url: &str, key: &str) -> Result<Option<CacheInfo>> {
    let connection = RedisConnection::new(url)?;
    let mut conn = connection.get_connection_manager().await?;

    // Check for chunked storage first
    let meta_key = format!("{}:meta", key);
    let meta_data: Option<Vec<u8>> = redis::cmd("GET")
        .arg(&meta_key)
        .query_async(&mut conn)
        .await
        .map_err(|e| Error::Runtime(e.to_string()))?;

    if let Some(meta_bytes) = meta_data {
        let metadata: ChunkMetadata = serde_json::from_slice(&meta_bytes)
            .map_err(|e| Error::Runtime(format!("Failed to parse metadata: {}", e)))?;

        let ttl: i64 = redis::cmd("TTL")
            .arg(&meta_key)
            .query_async(&mut conn)
            .await
            .map_err(|e| Error::Runtime(e.to_string()))?;

        return Ok(Some(CacheInfo {
            format: CacheFormat::from_str(&metadata.format)?,
            size_bytes: metadata.total_size,
            is_chunked: true,
            num_chunks: metadata.num_chunks,
            chunk_size: metadata.chunk_size,
            ttl: if ttl < 0 { None } else { Some(ttl) },
        }));
    }

    // Check single key
    let data: Option<Vec<u8>> = redis::cmd("GET")
        .arg(key)
        .query_async(&mut conn)
        .await
        .map_err(|e| Error::Runtime(e.to_string()))?;

    match data {
        Some(d) => {
            let ttl: i64 = redis::cmd("TTL")
                .arg(key)
                .query_async(&mut conn)
                .await
                .map_err(|e| Error::Runtime(e.to_string()))?;

            Ok(Some(CacheInfo {
                format: CacheFormat::Ipc, // Default - can't determine without parsing
                size_bytes: d.len(),
                is_chunked: false,
                num_chunks: 1,
                chunk_size: d.len(),
                ttl: if ttl < 0 { None } else { Some(ttl) },
            }))
        }
        None => Ok(None),
    }
}

// ============================================================================
// Helper functions
// ============================================================================

async fn set_with_ttl(
    conn: &mut redis::aio::ConnectionManager,
    key: &str,
    data: &[u8],
    ttl: Option<i64>,
) -> Result<()> {
    if let Some(seconds) = ttl {
        redis::cmd("SETEX")
            .arg(key)
            .arg(seconds)
            .arg(data)
            .query_async::<()>(conn)
            .await
            .map_err(|e| Error::Runtime(e.to_string()))?;
    } else {
        redis::cmd("SET")
            .arg(key)
            .arg(data)
            .query_async::<()>(conn)
            .await
            .map_err(|e| Error::Runtime(e.to_string()))?;
    }
    Ok(())
}

fn serialize_batch(batch: &RecordBatch, config: &CacheConfig) -> Result<Vec<u8>> {
    match config.format {
        CacheFormat::Ipc => serialize_ipc(batch, config.ipc_compression),
        CacheFormat::Parquet => {
            serialize_parquet(batch, config.parquet_compression, config.compression_level)
        }
    }
}

fn serialize_ipc(batch: &RecordBatch, compression: IpcCompression) -> Result<Vec<u8>> {
    let mut buf = Vec::new();

    let options = match compression {
        IpcCompression::Uncompressed => None,
        IpcCompression::Lz4 => Some(arrow::ipc::CompressionType::LZ4_FRAME),
        IpcCompression::Zstd => Some(arrow::ipc::CompressionType::ZSTD),
    };

    {
        let mut writer = if let Some(codec) = options {
            IpcWriter::try_new_with_options(
                &mut buf,
                batch.schema().as_ref(),
                arrow::ipc::writer::IpcWriteOptions::default()
                    .try_with_compression(Some(codec))
                    .map_err(|e| Error::Runtime(format!("Failed to set compression: {}", e)))?,
            )
            .map_err(|e| Error::Runtime(format!("Failed to create IPC writer: {}", e)))?
        } else {
            IpcWriter::try_new(&mut buf, batch.schema().as_ref())
                .map_err(|e| Error::Runtime(format!("Failed to create IPC writer: {}", e)))?
        };

        writer
            .write(batch)
            .map_err(|e| Error::Runtime(format!("Failed to write batch: {}", e)))?;

        writer
            .finish()
            .map_err(|e| Error::Runtime(format!("Failed to finish IPC: {}", e)))?;
    }

    Ok(buf)
}

fn serialize_parquet(
    batch: &RecordBatch,
    compression: ParquetCompressionType,
    level: Option<i32>,
) -> Result<Vec<u8>> {
    let mut buf = Vec::new();

    let parquet_compression = match compression {
        ParquetCompressionType::Uncompressed => ParquetCompression::UNCOMPRESSED,
        ParquetCompressionType::Snappy => ParquetCompression::SNAPPY,
        ParquetCompressionType::Lz4 => ParquetCompression::LZ4,
        ParquetCompressionType::Zstd => {
            let zstd_level = level
                .map(|l| parquet::basic::ZstdLevel::try_new(l).unwrap_or_default())
                .unwrap_or_default();
            ParquetCompression::ZSTD(zstd_level)
        }
    };

    let props = WriterProperties::builder()
        .set_compression(parquet_compression)
        .build();

    {
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
            .map_err(|e| Error::Runtime(format!("Failed to create Parquet writer: {}", e)))?;

        writer
            .write(batch)
            .map_err(|e| Error::Runtime(format!("Failed to write batch: {}", e)))?;

        writer
            .close()
            .map_err(|e| Error::Runtime(format!("Failed to close Parquet writer: {}", e)))?;
    }

    Ok(buf)
}

fn deserialize_batch(data: &[u8], format: CacheFormat) -> Result<RecordBatch> {
    match format {
        CacheFormat::Ipc => deserialize_ipc(data),
        CacheFormat::Parquet => deserialize_parquet(data),
    }
}

fn deserialize_ipc(data: &[u8]) -> Result<RecordBatch> {
    let cursor = Cursor::new(data);
    let reader = IpcReader::try_new(cursor, None)
        .map_err(|e| Error::Runtime(format!("Failed to create IPC reader: {}", e)))?;

    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Runtime(format!("Failed to read IPC batches: {}", e)))?;

    if batches.is_empty() {
        // Return empty batch with schema
        Ok(RecordBatch::new_empty(schema))
    } else if batches.len() == 1 {
        Ok(batches.into_iter().next().unwrap())
    } else {
        // Concatenate multiple batches
        arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| Error::Runtime(format!("Failed to concatenate batches: {}", e)))
    }
}

fn deserialize_parquet(data: &[u8]) -> Result<RecordBatch> {
    let bytes = Bytes::copy_from_slice(data);
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| Error::Runtime(format!("Failed to create Parquet reader: {}", e)))?
        .build()
        .map_err(|e| Error::Runtime(format!("Failed to build Parquet reader: {}", e)))?;

    let schema = Arc::new(reader.schema().as_ref().clone());
    let batches: Vec<RecordBatch> = reader
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::Runtime(format!("Failed to read Parquet batches: {}", e)))?;

    if batches.is_empty() {
        Ok(RecordBatch::new_empty(schema))
    } else if batches.len() == 1 {
        Ok(batches.into_iter().next().unwrap())
    } else {
        arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| Error::Runtime(format!("Failed to concatenate batches: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Float64Array::from(vec![1.1, 2.2, 3.3])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_serialize_deserialize_ipc() {
        let batch = create_test_batch();
        let config = CacheConfig::ipc();

        let data = serialize_batch(&batch, &config).unwrap();
        let result = deserialize_batch(&data, CacheFormat::Ipc).unwrap();

        assert_eq!(batch.num_rows(), result.num_rows());
        assert_eq!(batch.num_columns(), result.num_columns());
    }

    #[test]
    fn test_serialize_deserialize_ipc_compressed() {
        let batch = create_test_batch();

        for compression in [IpcCompression::Lz4, IpcCompression::Zstd] {
            let data = serialize_ipc(&batch, compression).unwrap();
            let result = deserialize_ipc(&data).unwrap();

            assert_eq!(batch.num_rows(), result.num_rows());
            assert_eq!(batch.num_columns(), result.num_columns());
        }
    }

    #[test]
    fn test_serialize_deserialize_parquet() {
        let batch = create_test_batch();
        let config = CacheConfig::parquet();

        let data = serialize_batch(&batch, &config).unwrap();
        let result = deserialize_batch(&data, CacheFormat::Parquet).unwrap();

        assert_eq!(batch.num_rows(), result.num_rows());
        assert_eq!(batch.num_columns(), result.num_columns());
    }

    #[test]
    fn test_serialize_deserialize_parquet_compressions() {
        let batch = create_test_batch();

        for compression in [
            ParquetCompressionType::Uncompressed,
            ParquetCompressionType::Snappy,
            ParquetCompressionType::Zstd,
        ] {
            let data = serialize_parquet(&batch, compression, None).unwrap();
            let result = deserialize_parquet(&data).unwrap();

            assert_eq!(batch.num_rows(), result.num_rows());
            assert_eq!(batch.num_columns(), result.num_columns());
        }
    }

    #[test]
    fn test_cache_config_builder() {
        let config = CacheConfig::parquet()
            .with_ttl(3600)
            .with_parquet_compression(ParquetCompressionType::Zstd)
            .with_compression_level(3)
            .with_chunk_size(50 * 1024 * 1024);

        assert_eq!(config.format, CacheFormat::Parquet);
        assert_eq!(config.ttl, Some(3600));
        assert_eq!(config.parquet_compression, ParquetCompressionType::Zstd);
        assert_eq!(config.compression_level, Some(3));
        assert_eq!(config.chunk_size, 50 * 1024 * 1024);
    }

    #[test]
    fn test_cache_format_conversion() {
        assert_eq!(CacheFormat::Ipc.as_str(), "ipc");
        assert_eq!(CacheFormat::Parquet.as_str(), "parquet");
        assert_eq!(CacheFormat::from_str("ipc").unwrap(), CacheFormat::Ipc);
        assert_eq!(
            CacheFormat::from_str("parquet").unwrap(),
            CacheFormat::Parquet
        );
        assert!(CacheFormat::from_str("invalid").is_err());
    }
}
