//! Write support for Redis data structures.
//!
//! This module provides functionality to write Polars DataFrames to Redis
//! as hashes or JSON documents.
//!
//! Write operations use Redis pipelining for improved performance, processing
//! keys in configurable batches (default: 1000 keys per batch).

use std::collections::HashSet;

use tokio::runtime::Runtime;

use crate::connection::RedisConnection;
use crate::error::{Error, Result};

/// Default batch size for pipelined write operations.
const DEFAULT_WRITE_BATCH_SIZE: usize = 1000;

/// Write mode for handling existing keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WriteMode {
    /// Fail if any key already exists.
    Fail,
    /// Replace existing keys (default behavior).
    #[default]
    Replace,
    /// Append to existing keys (for hashes: merge fields; for JSON/strings: same as replace).
    Append,
}

impl std::str::FromStr for WriteMode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "fail" => Ok(WriteMode::Fail),
            "replace" => Ok(WriteMode::Replace),
            "append" => Ok(WriteMode::Append),
            _ => Err(Error::InvalidInput(format!(
                "Invalid write mode '{}'. Expected: fail, replace, or append",
                s
            ))),
        }
    }
}

/// Result of a write operation.
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// Number of keys written.
    pub keys_written: usize,
    /// Number of keys that failed to write (if any).
    pub keys_failed: usize,
    /// Number of keys skipped because they already exist (when mode is Fail).
    pub keys_skipped: usize,
}

/// Write hashes to Redis from field data.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - List of Redis keys to write to
/// * `fields` - List of field names
/// * `values` - 2D list of values (rows x columns), same order as fields
/// * `ttl` - Optional TTL in seconds for each key
/// * `if_exists` - How to handle existing keys (fail, replace, append)
///
/// # Returns
/// A `WriteResult` with the number of keys written.
pub fn write_hashes(
    url: &str,
    keys: Vec<String>,
    fields: Vec<String>,
    values: Vec<Vec<Option<String>>>,
    ttl: Option<i64>,
    if_exists: WriteMode,
) -> Result<WriteResult> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        write_hashes_async(&mut conn, keys, fields, values, ttl, if_exists).await
    })
}

/// Async implementation of hash writing with pipelining.
async fn write_hashes_async(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: Vec<String>,
    fields: Vec<String>,
    values: Vec<Vec<Option<String>>>,
    ttl: Option<i64>,
    if_exists: WriteMode,
) -> Result<WriteResult> {
    let mut keys_written = 0;
    let mut keys_failed = 0;
    let mut keys_skipped = 0;

    // Process in batches for better performance
    for batch_start in (0..keys.len()).step_by(DEFAULT_WRITE_BATCH_SIZE) {
        let batch_end = (batch_start + DEFAULT_WRITE_BATCH_SIZE).min(keys.len());
        let batch_keys = &keys[batch_start..batch_end];

        // For Fail mode, check existence of all keys in batch first
        let existing_keys: HashSet<usize> = if if_exists == WriteMode::Fail {
            let mut pipe = redis::pipe();
            for key in batch_keys {
                pipe.exists(key);
            }
            let exists_results: Vec<bool> = pipe.query_async(conn).await.unwrap_or_default();
            exists_results
                .into_iter()
                .enumerate()
                .filter_map(|(i, exists)| if exists { Some(i) } else { None })
                .collect()
        } else {
            HashSet::new()
        };

        // For Replace mode, delete existing keys in batch
        if if_exists == WriteMode::Replace {
            let mut del_pipe = redis::pipe();
            for key in batch_keys {
                del_pipe.del(key).ignore();
            }
            let _ = del_pipe.query_async::<()>(conn).await;
        }

        // Build pipeline for HSET operations
        let mut pipe = redis::pipe();
        let mut batch_indices: Vec<usize> = Vec::new();

        for (batch_idx, key) in batch_keys.iter().enumerate() {
            let global_idx = batch_start + batch_idx;

            // Skip if key exists and mode is Fail
            if existing_keys.contains(&batch_idx) {
                keys_skipped += 1;
                continue;
            }

            if global_idx >= values.len() {
                break;
            }

            let row = &values[global_idx];
            let mut hash_data: Vec<(&str, &str)> = Vec::new();

            for (j, field) in fields.iter().enumerate() {
                if j < row.len()
                    && let Some(value) = &row[j]
                {
                    hash_data.push((field.as_str(), value.as_str()));
                }
            }

            if !hash_data.is_empty() {
                pipe.hset_multiple(key, &hash_data);
                if let Some(seconds) = ttl {
                    pipe.expire(key, seconds);
                }
                batch_indices.push(batch_idx);
            }
        }

        // Execute pipeline if there are commands
        if !batch_indices.is_empty() {
            match pipe.query_async::<()>(conn).await {
                Ok(_) => keys_written += batch_indices.len(),
                Err(_) => keys_failed += batch_indices.len(),
            }
        }
    }

    Ok(WriteResult {
        keys_written,
        keys_failed,
        keys_skipped,
    })
}

/// Write JSON documents to Redis.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - List of Redis keys to write to
/// * `json_strings` - List of JSON strings to write
/// * `ttl` - Optional TTL in seconds for each key
/// * `if_exists` - How to handle existing keys (fail, replace, append)
///
/// # Returns
/// A `WriteResult` with the number of keys written.
pub fn write_json(
    url: &str,
    keys: Vec<String>,
    json_strings: Vec<String>,
    ttl: Option<i64>,
    if_exists: WriteMode,
) -> Result<WriteResult> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        write_json_async(&mut conn, keys, json_strings, ttl, if_exists).await
    })
}

/// Async implementation of JSON writing with pipelining.
async fn write_json_async(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: Vec<String>,
    json_strings: Vec<String>,
    ttl: Option<i64>,
    if_exists: WriteMode,
) -> Result<WriteResult> {
    let mut keys_written = 0;
    let mut keys_failed = 0;
    let mut keys_skipped = 0;

    let items: Vec<_> = keys.iter().zip(json_strings.iter()).collect();

    // Process in batches for better performance
    for batch_start in (0..items.len()).step_by(DEFAULT_WRITE_BATCH_SIZE) {
        let batch_end = (batch_start + DEFAULT_WRITE_BATCH_SIZE).min(items.len());
        let batch_items = &items[batch_start..batch_end];

        // For Fail mode, check existence of all keys in batch first
        let existing_keys: HashSet<usize> = if if_exists == WriteMode::Fail {
            let mut pipe = redis::pipe();
            for (key, _) in batch_items {
                pipe.exists(*key);
            }
            let exists_results: Vec<bool> = pipe.query_async(conn).await.unwrap_or_default();
            exists_results
                .into_iter()
                .enumerate()
                .filter_map(|(i, exists)| if exists { Some(i) } else { None })
                .collect()
        } else {
            HashSet::new()
        };

        // Build pipeline for JSON.SET operations
        let mut pipe = redis::pipe();
        let mut batch_count = 0;

        for (batch_idx, (key, json_str)) in batch_items.iter().enumerate() {
            // Skip if key exists and mode is Fail
            if existing_keys.contains(&batch_idx) {
                keys_skipped += 1;
                continue;
            }

            pipe.cmd("JSON.SET").arg(*key).arg("$").arg(*json_str);
            if let Some(seconds) = ttl {
                pipe.expire(*key, seconds);
            }
            batch_count += 1;
        }

        // Execute pipeline if there are commands
        if batch_count > 0 {
            match pipe.query_async::<()>(conn).await {
                Ok(_) => keys_written += batch_count,
                Err(_) => keys_failed += batch_count,
            }
        }
    }

    Ok(WriteResult {
        keys_written,
        keys_failed,
        keys_skipped,
    })
}

/// Write string values to Redis.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - List of Redis keys to write to
/// * `values` - List of string values to write
/// * `ttl` - Optional TTL in seconds for each key
/// * `if_exists` - How to handle existing keys (fail, replace, append)
///
/// # Returns
/// A `WriteResult` with the number of keys written.
pub fn write_strings(
    url: &str,
    keys: Vec<String>,
    values: Vec<Option<String>>,
    ttl: Option<i64>,
    if_exists: WriteMode,
) -> Result<WriteResult> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        write_strings_async(&mut conn, keys, values, ttl, if_exists).await
    })
}

/// Async implementation of string writing with pipelining.
async fn write_strings_async(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: Vec<String>,
    values: Vec<Option<String>>,
    ttl: Option<i64>,
    if_exists: WriteMode,
) -> Result<WriteResult> {
    let mut keys_written = 0;
    let mut keys_failed = 0;
    let mut keys_skipped = 0;

    // Collect non-null items
    let items: Vec<_> = keys
        .iter()
        .zip(values.iter())
        .filter_map(|(k, v)| v.as_ref().map(|val| (k, val)))
        .collect();

    // Process in batches for better performance
    for batch_start in (0..items.len()).step_by(DEFAULT_WRITE_BATCH_SIZE) {
        let batch_end = (batch_start + DEFAULT_WRITE_BATCH_SIZE).min(items.len());
        let batch_items = &items[batch_start..batch_end];

        // For Fail mode, check existence of all keys in batch first
        let existing_keys: HashSet<usize> = if if_exists == WriteMode::Fail {
            let mut pipe = redis::pipe();
            for (key, _) in batch_items {
                pipe.exists(*key);
            }
            let exists_results: Vec<bool> = pipe.query_async(conn).await.unwrap_or_default();
            exists_results
                .into_iter()
                .enumerate()
                .filter_map(|(i, exists)| if exists { Some(i) } else { None })
                .collect()
        } else {
            HashSet::new()
        };

        // Build pipeline for SET operations
        let mut pipe = redis::pipe();
        let mut batch_count = 0;

        for (batch_idx, (key, val)) in batch_items.iter().enumerate() {
            // Skip if key exists and mode is Fail
            if existing_keys.contains(&batch_idx) {
                keys_skipped += 1;
                continue;
            }

            // For Append mode on strings, we could use APPEND command,
            // but that concatenates strings which is probably not what users want.
            // So we treat Append same as Replace for strings.
            if let Some(seconds) = ttl {
                // SETEX for atomic set with TTL
                pipe.cmd("SETEX").arg(*key).arg(seconds).arg(*val);
            } else {
                pipe.set(*key, *val);
            }
            batch_count += 1;
        }

        // Execute pipeline if there are commands
        if batch_count > 0 {
            match pipe.query_async::<()>(conn).await {
                Ok(_) => keys_written += batch_count,
                Err(_) => keys_failed += batch_count,
            }
        }
    }

    Ok(WriteResult {
        keys_written,
        keys_failed,
        keys_skipped,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_result_creation() {
        let result = WriteResult {
            keys_written: 10,
            keys_failed: 2,
            keys_skipped: 1,
        };
        assert_eq!(result.keys_written, 10);
        assert_eq!(result.keys_failed, 2);
        assert_eq!(result.keys_skipped, 1);
    }

    #[test]
    fn test_write_mode_from_str() {
        use std::str::FromStr;
        assert_eq!(WriteMode::from_str("fail").unwrap(), WriteMode::Fail);
        assert_eq!(WriteMode::from_str("FAIL").unwrap(), WriteMode::Fail);
        assert_eq!(WriteMode::from_str("replace").unwrap(), WriteMode::Replace);
        assert_eq!(WriteMode::from_str("Replace").unwrap(), WriteMode::Replace);
        assert_eq!(WriteMode::from_str("append").unwrap(), WriteMode::Append);
        assert_eq!(WriteMode::from_str("APPEND").unwrap(), WriteMode::Append);
        assert!(WriteMode::from_str("invalid").is_err());
    }

    #[test]
    fn test_write_mode_default() {
        assert_eq!(WriteMode::default(), WriteMode::Replace);
    }
}
