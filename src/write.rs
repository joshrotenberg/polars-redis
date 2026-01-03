//! Write support for Redis data structures.
//!
//! This module provides functionality to write Polars DataFrames to Redis
//! as hashes or JSON documents.

use std::collections::HashMap;

use redis::AsyncCommands;
use tokio::runtime::Runtime;

use crate::connection::RedisConnection;
use crate::error::{Error, Result};

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

/// Async implementation of hash writing.
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

    for (i, key) in keys.iter().enumerate() {
        if i >= values.len() {
            break;
        }

        // Check if key exists when mode is Fail
        if if_exists == WriteMode::Fail {
            let exists: bool = conn.exists(key).await.unwrap_or(false);
            if exists {
                keys_skipped += 1;
                continue;
            }
        }

        let row = &values[i];
        let mut hash_data: HashMap<&str, &str> = HashMap::new();

        for (j, field) in fields.iter().enumerate() {
            if j < row.len()
                && let Some(value) = &row[j]
            {
                hash_data.insert(field.as_str(), value.as_str());
            }
        }

        if !hash_data.is_empty() {
            // For Replace mode, delete the key first to ensure clean replacement
            if if_exists == WriteMode::Replace {
                let _ = conn.del::<_, ()>(key).await;
            }
            // For Append mode (and Replace after delete), just set the fields
            // HSET naturally merges/overwrites fields

            match conn
                .hset_multiple::<_, _, _, ()>(key, &hash_data.into_iter().collect::<Vec<_>>())
                .await
            {
                Ok(_) => {
                    // Set TTL if provided
                    if let Some(seconds) = ttl {
                        let _ = conn.expire::<_, ()>(key, seconds).await;
                    }
                    keys_written += 1;
                }
                Err(_) => keys_failed += 1,
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

/// Async implementation of JSON writing.
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

    for (key, json_str) in keys.iter().zip(json_strings.iter()) {
        // Check if key exists when mode is Fail
        if if_exists == WriteMode::Fail {
            let exists: bool = conn.exists(key).await.unwrap_or(false);
            if exists {
                keys_skipped += 1;
                continue;
            }
        }

        // JSON.SET with NX option for Fail mode could be used, but we already checked above
        // For Replace and Append, JSON.SET overwrites the entire document
        match redis::cmd("JSON.SET")
            .arg(key)
            .arg("$")
            .arg(json_str)
            .query_async::<Option<String>>(conn)
            .await
        {
            Ok(_) => {
                // Set TTL if provided
                if let Some(seconds) = ttl {
                    let _ = conn.expire::<_, ()>(key, seconds).await;
                }
                keys_written += 1;
            }
            Err(_) => keys_failed += 1,
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

/// Async implementation of string writing.
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

    for (key, value) in keys.iter().zip(values.iter()) {
        // Skip null values
        let Some(val) = value else {
            continue;
        };

        // Check if key exists when mode is Fail
        if if_exists == WriteMode::Fail {
            let exists: bool = conn.exists(key).await.unwrap_or(false);
            if exists {
                keys_skipped += 1;
                continue;
            }
        }

        // For Append mode on strings, we could use APPEND command,
        // but that concatenates strings which is probably not what users want.
        // So we treat Append same as Replace for strings.
        // Use SETEX for atomic set with TTL, or SET without TTL
        let result = if let Some(seconds) = ttl {
            conn.set_ex::<_, _, ()>(key, val, seconds as u64).await
        } else {
            conn.set::<_, _, ()>(key, val).await
        };

        match result {
            Ok(_) => keys_written += 1,
            Err(_) => keys_failed += 1,
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
