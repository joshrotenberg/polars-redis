//! Key management utilities for Redis.
//!
//! This module provides DataFrame-friendly operations for managing Redis keys,
//! including TTL management, bulk delete, rename, and key info retrieval.
//!
//! # Key Info
//!
//! ```ignore
//! use polars_redis::client::keys::key_info;
//!
//! // Get info for all keys matching a pattern
//! let info = key_info("redis://localhost:6379", "user:*", None)?;
//! // Returns Vec<KeyInfo> with key, type, ttl, memory_usage, encoding
//! ```
//!
//! # Bulk TTL
//!
//! ```ignore
//! use polars_redis::client::keys::set_ttl;
//!
//! // Set TTL for multiple keys
//! let result = set_ttl("redis://localhost:6379", &keys, 3600)?;
//! ```
//!
//! # Bulk Delete
//!
//! ```ignore
//! use polars_redis::client::keys::delete_keys;
//!
//! // Delete multiple keys
//! let deleted = delete_keys("redis://localhost:6379", &keys)?;
//! ```
//!
//! # Bulk Rename
//!
//! ```ignore
//! use polars_redis::client::keys::rename_keys;
//!
//! // Rename multiple keys
//! let result = rename_keys("redis://localhost:6379", &renames)?;
//! ```

use redis::Value;
use tokio::runtime::Runtime;

use crate::connection::RedisConnection;
use crate::error::{Error, Result};

/// Default batch size for pipelined operations.
const DEFAULT_BATCH_SIZE: usize = 1000;

/// Information about a Redis key.
#[derive(Debug, Clone)]
pub struct KeyInfo {
    /// The key name.
    pub key: String,
    /// The key type (string, hash, list, set, zset, stream).
    pub key_type: String,
    /// TTL in seconds (-1 if no expiry, -2 if key doesn't exist).
    pub ttl: i64,
    /// Memory usage in bytes (requires MEMORY USAGE command).
    pub memory_usage: Option<i64>,
    /// Internal encoding (requires OBJECT ENCODING command).
    pub encoding: Option<String>,
}

/// Result of a bulk TTL operation.
#[derive(Debug, Clone)]
pub struct TtlResult {
    /// Number of keys that had TTL set successfully.
    pub succeeded: usize,
    /// Number of keys that failed (e.g., key doesn't exist).
    pub failed: usize,
    /// Keys that failed with their error messages.
    pub errors: Vec<(String, String)>,
}

/// Result of a bulk delete operation.
#[derive(Debug, Clone)]
pub struct DeleteResult {
    /// Number of keys deleted.
    pub deleted: usize,
    /// Number of keys that didn't exist.
    pub not_found: usize,
}

/// Result of a bulk rename operation.
#[derive(Debug, Clone)]
pub struct RenameResult {
    /// Number of keys renamed successfully.
    pub succeeded: usize,
    /// Number of keys that failed to rename.
    pub failed: usize,
    /// Keys that failed with their error messages.
    pub errors: Vec<(String, String)>,
}

/// Get information about keys matching a pattern.
///
/// Uses SCAN to iterate keys safely, then retrieves TYPE, TTL, and optionally
/// MEMORY USAGE and OBJECT ENCODING for each key.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `pattern` - Key pattern to match (e.g., "user:*")
/// * `include_memory` - Whether to include memory usage (slower)
///
/// # Example
/// ```ignore
/// let info = key_info("redis://localhost:6379", "user:*", Some(true))?;
/// for k in info {
///     println!("{}: type={}, ttl={}", k.key, k.key_type, k.ttl);
/// }
/// ```
pub fn key_info(url: &str, pattern: &str, include_memory: Option<bool>) -> Result<Vec<KeyInfo>> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;
    let include_mem = include_memory.unwrap_or(false);

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;

        // Scan for keys matching the pattern
        let keys = scan_keys_async(&mut conn, pattern).await?;

        if keys.is_empty() {
            return Ok(Vec::new());
        }

        // Get info for each key in batches
        let mut results = Vec::with_capacity(keys.len());

        for batch in keys.chunks(DEFAULT_BATCH_SIZE) {
            let batch_info = get_key_info_batch(&mut conn, batch, include_mem).await?;
            results.extend(batch_info);
        }

        Ok(results)
    })
}

/// Scan keys matching a pattern.
async fn scan_keys_async(
    conn: &mut redis::aio::MultiplexedConnection,
    pattern: &str,
) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    let mut cursor: u64 = 0;

    loop {
        let (new_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(1000)
            .query_async(conn)
            .await
            .map_err(Error::Connection)?;

        keys.extend(batch);
        cursor = new_cursor;

        if cursor == 0 {
            break;
        }
    }

    Ok(keys)
}

/// Get info for a batch of keys.
async fn get_key_info_batch(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: &[String],
    include_memory: bool,
) -> Result<Vec<KeyInfo>> {
    // Pipeline TYPE and TTL for all keys
    let mut pipe = redis::pipe();

    for key in keys {
        pipe.cmd("TYPE").arg(key);
        pipe.cmd("TTL").arg(key);
        if include_memory {
            pipe.cmd("MEMORY").arg("USAGE").arg(key);
            pipe.cmd("OBJECT").arg("ENCODING").arg(key);
        }
    }

    let values: Vec<Value> = pipe.query_async(conn).await.map_err(Error::Connection)?;

    let step = if include_memory { 4 } else { 2 };
    let mut results = Vec::with_capacity(keys.len());

    for (i, key) in keys.iter().enumerate() {
        let base = i * step;

        let key_type = match &values.get(base) {
            Some(Value::SimpleString(s)) => s.clone(),
            Some(Value::BulkString(b)) => String::from_utf8_lossy(b).to_string(),
            _ => "unknown".to_string(),
        };

        let ttl = match &values.get(base + 1) {
            Some(Value::Int(i)) => *i,
            _ => -2,
        };

        let (memory_usage, encoding) = if include_memory {
            let mem = match &values.get(base + 2) {
                Some(Value::Int(i)) => Some(*i),
                _ => None,
            };
            let enc = match &values.get(base + 3) {
                Some(Value::SimpleString(s)) => Some(s.clone()),
                Some(Value::BulkString(b)) => Some(String::from_utf8_lossy(b).to_string()),
                _ => None,
            };
            (mem, enc)
        } else {
            (None, None)
        };

        results.push(KeyInfo {
            key: key.clone(),
            key_type,
            ttl,
            memory_usage,
            encoding,
        });
    }

    Ok(results)
}

/// Set TTL for multiple keys.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - Keys to set TTL for
/// * `ttl_seconds` - TTL in seconds
///
/// # Returns
/// A `TtlResult` with success/failure counts.
pub fn set_ttl(url: &str, keys: &[String], ttl_seconds: i64) -> Result<TtlResult> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        set_ttl_async(&mut conn, keys, ttl_seconds).await
    })
}

/// Set TTL for keys with individual TTL values.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys_and_ttls` - Pairs of (key, ttl_seconds)
///
/// # Returns
/// A `TtlResult` with success/failure counts.
pub fn set_ttl_individual(url: &str, keys_and_ttls: &[(String, i64)]) -> Result<TtlResult> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        set_ttl_individual_async(&mut conn, keys_and_ttls).await
    })
}

async fn set_ttl_async(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: &[String],
    ttl_seconds: i64,
) -> Result<TtlResult> {
    let mut succeeded = 0;
    let mut failed = 0;
    let mut errors = Vec::new();

    for batch in keys.chunks(DEFAULT_BATCH_SIZE) {
        let mut pipe = redis::pipe();

        for key in batch {
            pipe.cmd("EXPIRE").arg(key).arg(ttl_seconds);
        }

        let results: Vec<Value> = pipe.query_async(conn).await.map_err(Error::Connection)?;

        for (i, result) in results.iter().enumerate() {
            match result {
                Value::Int(1) => succeeded += 1,
                Value::Int(0) => {
                    failed += 1;
                    errors.push((batch[i].clone(), "Key does not exist".to_string()));
                },
                _ => {
                    failed += 1;
                    errors.push((batch[i].clone(), "Unexpected response".to_string()));
                },
            }
        }
    }

    Ok(TtlResult {
        succeeded,
        failed,
        errors,
    })
}

async fn set_ttl_individual_async(
    conn: &mut redis::aio::MultiplexedConnection,
    keys_and_ttls: &[(String, i64)],
) -> Result<TtlResult> {
    let mut succeeded = 0;
    let mut failed = 0;
    let mut errors = Vec::new();

    for batch in keys_and_ttls.chunks(DEFAULT_BATCH_SIZE) {
        let mut pipe = redis::pipe();

        for (key, ttl) in batch {
            pipe.cmd("EXPIRE").arg(key).arg(*ttl);
        }

        let results: Vec<Value> = pipe.query_async(conn).await.map_err(Error::Connection)?;

        for (i, result) in results.iter().enumerate() {
            match result {
                Value::Int(1) => succeeded += 1,
                Value::Int(0) => {
                    failed += 1;
                    errors.push((batch[i].0.clone(), "Key does not exist".to_string()));
                },
                _ => {
                    failed += 1;
                    errors.push((batch[i].0.clone(), "Unexpected response".to_string()));
                },
            }
        }
    }

    Ok(TtlResult {
        succeeded,
        failed,
        errors,
    })
}

/// Delete multiple keys.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - Keys to delete
///
/// # Returns
/// A `DeleteResult` with deleted and not_found counts.
pub fn delete_keys(url: &str, keys: &[String]) -> Result<DeleteResult> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        delete_keys_async(&mut conn, keys).await
    })
}

async fn delete_keys_async(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: &[String],
) -> Result<DeleteResult> {
    let mut deleted = 0;
    let total = keys.len();

    for batch in keys.chunks(DEFAULT_BATCH_SIZE) {
        let mut pipe = redis::pipe();

        for key in batch {
            pipe.cmd("DEL").arg(key);
        }

        let results: Vec<Value> = pipe.query_async(conn).await.map_err(Error::Connection)?;

        for result in results {
            if let Value::Int(n) = result {
                deleted += n as usize;
            }
        }
    }

    Ok(DeleteResult {
        deleted,
        not_found: total - deleted,
    })
}

/// Delete keys matching a pattern.
///
/// Uses SCAN to find keys, then deletes them in batches.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `pattern` - Key pattern to match
///
/// # Returns
/// A `DeleteResult` with deleted and not_found counts.
pub fn delete_keys_pattern(url: &str, pattern: &str) -> Result<DeleteResult> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;

        // First scan for keys
        let keys = scan_keys_async(&mut conn, pattern).await?;

        if keys.is_empty() {
            return Ok(DeleteResult {
                deleted: 0,
                not_found: 0,
            });
        }

        delete_keys_async(&mut conn, &keys).await
    })
}

/// Rename multiple keys.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `renames` - Pairs of (old_key, new_key)
///
/// # Returns
/// A `RenameResult` with success/failure counts.
pub fn rename_keys(url: &str, renames: &[(String, String)]) -> Result<RenameResult> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        rename_keys_async(&mut conn, renames).await
    })
}

async fn rename_keys_async(
    conn: &mut redis::aio::MultiplexedConnection,
    renames: &[(String, String)],
) -> Result<RenameResult> {
    let mut succeeded = 0;
    let mut failed = 0;
    let mut errors = Vec::new();

    // RENAME doesn't work well in pipelines because it errors on non-existent keys
    // and causes the whole pipeline to fail. Execute individually instead.
    for (old_key, new_key) in renames {
        let result: std::result::Result<String, _> = redis::cmd("RENAME")
            .arg(old_key)
            .arg(new_key)
            .query_async(conn)
            .await;

        match result {
            Ok(_) => succeeded += 1,
            Err(err) => {
                failed += 1;
                errors.push((old_key.clone(), err.to_string()));
            },
        }
    }

    Ok(RenameResult {
        succeeded,
        failed,
        errors,
    })
}

/// Remove TTL from keys (make them persistent).
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - Keys to persist
///
/// # Returns
/// A `TtlResult` with success/failure counts.
pub fn persist_keys(url: &str, keys: &[String]) -> Result<TtlResult> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        persist_keys_async(&mut conn, keys).await
    })
}

async fn persist_keys_async(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: &[String],
) -> Result<TtlResult> {
    let mut succeeded = 0;
    let mut failed = 0;
    let mut errors = Vec::new();

    for batch in keys.chunks(DEFAULT_BATCH_SIZE) {
        let mut pipe = redis::pipe();

        for key in batch {
            pipe.cmd("PERSIST").arg(key);
        }

        let results: Vec<Value> = pipe.query_async(conn).await.map_err(Error::Connection)?;

        for (i, result) in results.iter().enumerate() {
            match result {
                Value::Int(1) => succeeded += 1,
                Value::Int(0) => {
                    failed += 1;
                    errors.push((
                        batch[i].clone(),
                        "Key does not exist or has no TTL".to_string(),
                    ));
                },
                _ => {
                    failed += 1;
                    errors.push((batch[i].clone(), "Unexpected response".to_string()));
                },
            }
        }
    }

    Ok(TtlResult {
        succeeded,
        failed,
        errors,
    })
}

/// Check if keys exist.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - Keys to check
///
/// # Returns
/// Vector of (key, exists) pairs.
pub fn exists_keys(url: &str, keys: &[String]) -> Result<Vec<(String, bool)>> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        exists_keys_async(&mut conn, keys).await
    })
}

async fn exists_keys_async(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: &[String],
) -> Result<Vec<(String, bool)>> {
    let mut results = Vec::with_capacity(keys.len());

    for batch in keys.chunks(DEFAULT_BATCH_SIZE) {
        let mut pipe = redis::pipe();

        for key in batch {
            pipe.cmd("EXISTS").arg(key);
        }

        let values: Vec<Value> = pipe.query_async(conn).await.map_err(Error::Connection)?;

        for (i, value) in values.iter().enumerate() {
            let exists = matches!(value, Value::Int(1));
            results.push((batch[i].clone(), exists));
        }
    }

    Ok(results)
}

/// Get TTL for multiple keys.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - Keys to get TTL for
///
/// # Returns
/// Vector of (key, ttl) pairs. TTL is -1 if no expiry, -2 if key doesn't exist.
pub fn get_ttl(url: &str, keys: &[String]) -> Result<Vec<(String, i64)>> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        get_ttl_async(&mut conn, keys).await
    })
}

async fn get_ttl_async(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: &[String],
) -> Result<Vec<(String, i64)>> {
    let mut results = Vec::with_capacity(keys.len());

    for batch in keys.chunks(DEFAULT_BATCH_SIZE) {
        let mut pipe = redis::pipe();

        for key in batch {
            pipe.cmd("TTL").arg(key);
        }

        let values: Vec<Value> = pipe.query_async(conn).await.map_err(Error::Connection)?;

        for (i, value) in values.iter().enumerate() {
            let ttl = match value {
                Value::Int(n) => *n,
                _ => -2,
            };
            results.push((batch[i].clone(), ttl));
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_info_struct() {
        let info = KeyInfo {
            key: "test:key".to_string(),
            key_type: "string".to_string(),
            ttl: 3600,
            memory_usage: Some(100),
            encoding: Some("embstr".to_string()),
        };

        assert_eq!(info.key, "test:key");
        assert_eq!(info.key_type, "string");
        assert_eq!(info.ttl, 3600);
        assert_eq!(info.memory_usage, Some(100));
        assert_eq!(info.encoding, Some("embstr".to_string()));
    }

    #[test]
    fn test_ttl_result() {
        let result = TtlResult {
            succeeded: 5,
            failed: 2,
            errors: vec![("key1".to_string(), "error1".to_string())],
        };

        assert_eq!(result.succeeded, 5);
        assert_eq!(result.failed, 2);
        assert_eq!(result.errors.len(), 1);
    }

    #[test]
    fn test_delete_result() {
        let result = DeleteResult {
            deleted: 10,
            not_found: 2,
        };

        assert_eq!(result.deleted, 10);
        assert_eq!(result.not_found, 2);
    }

    #[test]
    fn test_rename_result() {
        let result = RenameResult {
            succeeded: 8,
            failed: 1,
            errors: vec![("old_key".to_string(), "no such key".to_string())],
        };

        assert_eq!(result.succeeded, 8);
        assert_eq!(result.failed, 1);
        assert_eq!(result.errors.len(), 1);
    }
}
