//! Write support for Redis data structures.
//!
//! This module provides functionality to write Polars DataFrames to Redis
//! as hashes or JSON documents.
//!
//! Write operations use Redis pipelining for improved performance, processing
//! keys in configurable batches (default: 1000 keys per batch).
//!
//! # Error Granularity
//!
//! By default, write operations report aggregate success/failure counts.
//! For per-key error details, use the `_detailed` variants which return
//! [`WriteResultDetailed`] with information about which specific keys failed.

use std::collections::{HashMap, HashSet};

use redis::Value;
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

/// Result of a write operation (basic).
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// Number of keys successfully written.
    pub keys_written: usize,
    /// Number of keys that failed to write.
    pub keys_failed: usize,
    /// Number of keys skipped because they already exist (when mode is Fail).
    pub keys_skipped: usize,
}

/// Error information for a single key.
#[derive(Debug, Clone)]
pub struct KeyError {
    /// The Redis key that failed.
    pub key: String,
    /// The error message from Redis.
    pub error: String,
}

/// Detailed result of a write operation with per-key error information.
///
/// This provides granular error reporting for production workflows where
/// partial success is acceptable and retry logic is needed.
///
/// # Example
///
/// ```ignore
/// let result = write_hashes_detailed(url, keys, fields, values, None, WriteMode::Replace)?;
///
/// println!("Succeeded: {}", result.keys_written);
/// println!("Failed: {}", result.keys_failed);
///
/// for error in &result.errors {
///     eprintln!("Key {} failed: {}", error.key, error.error);
/// }
///
/// // Get list of failed keys for retry
/// let failed_keys: Vec<&str> = result.failed_keys();
/// ```
#[derive(Debug, Clone)]
pub struct WriteResultDetailed {
    /// Number of keys successfully written.
    pub keys_written: usize,
    /// Number of keys that failed to write.
    pub keys_failed: usize,
    /// Number of keys skipped because they already exist (when mode is Fail).
    pub keys_skipped: usize,
    /// List of keys that were successfully written.
    pub succeeded_keys: Vec<String>,
    /// Detailed error information for each failed key.
    pub errors: Vec<KeyError>,
}

impl WriteResultDetailed {
    /// Create a new empty detailed result.
    pub fn new() -> Self {
        Self {
            keys_written: 0,
            keys_failed: 0,
            keys_skipped: 0,
            succeeded_keys: Vec::new(),
            errors: Vec::new(),
        }
    }

    /// Get a list of keys that failed to write.
    pub fn failed_keys(&self) -> Vec<&str> {
        self.errors.iter().map(|e| e.key.as_str()).collect()
    }

    /// Get a map of failed keys to their error messages.
    pub fn error_map(&self) -> HashMap<&str, &str> {
        self.errors
            .iter()
            .map(|e| (e.key.as_str(), e.error.as_str()))
            .collect()
    }

    /// Check if all keys were written successfully.
    pub fn is_complete_success(&self) -> bool {
        self.keys_failed == 0
    }

    /// Convert to a basic WriteResult (discards per-key details).
    pub fn to_basic(&self) -> WriteResult {
        WriteResult {
            keys_written: self.keys_written,
            keys_failed: self.keys_failed,
            keys_skipped: self.keys_skipped,
        }
    }
}

impl Default for WriteResultDetailed {
    fn default() -> Self {
        Self::new()
    }
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

/// Write hashes to Redis with detailed per-key error reporting.
///
/// This is similar to [`write_hashes`] but returns detailed information about
/// which specific keys succeeded or failed, enabling retry logic and better
/// error handling in production workflows.
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
/// A [`WriteResultDetailed`] with per-key success/failure information.
///
/// # Example
///
/// ```ignore
/// let result = write_hashes_detailed(
///     "redis://localhost:6379",
///     keys,
///     fields,
///     values,
///     None,
///     WriteMode::Replace,
/// )?;
///
/// if !result.is_complete_success() {
///     for error in &result.errors {
///         eprintln!("Failed to write {}: {}", error.key, error.error);
///     }
/// }
/// ```
pub fn write_hashes_detailed(
    url: &str,
    keys: Vec<String>,
    fields: Vec<String>,
    values: Vec<Vec<Option<String>>>,
    ttl: Option<i64>,
    if_exists: WriteMode,
) -> Result<WriteResultDetailed> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        write_hashes_detailed_async(&mut conn, keys, fields, values, ttl, if_exists).await
    })
}

/// Async implementation of detailed hash writing with per-key error tracking.
async fn write_hashes_detailed_async(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: Vec<String>,
    fields: Vec<String>,
    values: Vec<Vec<Option<String>>>,
    ttl: Option<i64>,
    if_exists: WriteMode,
) -> Result<WriteResultDetailed> {
    let mut result = WriteResultDetailed::new();
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

        // Build pipeline for HSET operations, tracking which key each command belongs to
        let mut pipe = redis::pipe();
        // Track (key_string, commands_for_this_key) for each key in the pipeline
        let mut key_command_counts: Vec<(String, usize)> = Vec::new();

        for (batch_idx, key) in batch_keys.iter().enumerate() {
            let global_idx = batch_start + batch_idx;

            // Skip if key exists and mode is Fail
            if existing_keys.contains(&batch_idx) {
                result.keys_skipped += 1;
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
                let mut cmd_count = 1;
                if let Some(seconds) = ttl {
                    pipe.expire(key, seconds);
                    cmd_count += 1;
                }
                key_command_counts.push((key.clone(), cmd_count));
            }
        }

        // Execute pipeline and collect individual results
        if !key_command_counts.is_empty() {
            match pipe.query_async::<Vec<Value>>(conn).await {
                Ok(responses) => {
                    // Process responses, mapping back to keys
                    let mut response_idx = 0;
                    for (key, cmd_count) in &key_command_counts {
                        let mut key_succeeded = true;
                        let mut key_error = String::new();

                        // Check all commands for this key
                        for _ in 0..*cmd_count {
                            if response_idx < responses.len() {
                                if let Value::ServerError(err) = &responses[response_idx] {
                                    key_succeeded = false;
                                    key_error = err.to_string();
                                }
                                response_idx += 1;
                            }
                        }

                        if key_succeeded {
                            result.keys_written += 1;
                            result.succeeded_keys.push(key.clone());
                        } else {
                            result.keys_failed += 1;
                            result.errors.push(KeyError {
                                key: key.clone(),
                                error: key_error,
                            });
                        }
                    }
                }
                Err(e) => {
                    // Entire pipeline failed - mark all keys as failed
                    for (key, _) in key_command_counts {
                        result.keys_failed += 1;
                        result.errors.push(KeyError {
                            key,
                            error: e.to_string(),
                        });
                    }
                }
            }
        }
    }

    Ok(result)
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

/// Write list elements to Redis.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - List of Redis keys to write to
/// * `elements` - 2D list of elements for each list
/// * `ttl` - Optional TTL in seconds for each key
/// * `if_exists` - How to handle existing keys (fail, replace, append)
///
/// # Returns
/// A `WriteResult` with the number of keys written.
pub fn write_lists(
    url: &str,
    keys: Vec<String>,
    elements: Vec<Vec<String>>,
    ttl: Option<i64>,
    if_exists: WriteMode,
) -> Result<WriteResult> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        write_lists_async(&mut conn, keys, elements, ttl, if_exists).await
    })
}

/// Async implementation of list writing with pipelining.
async fn write_lists_async(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: Vec<String>,
    elements: Vec<Vec<String>>,
    ttl: Option<i64>,
    if_exists: WriteMode,
) -> Result<WriteResult> {
    let mut keys_written = 0;
    let mut keys_failed = 0;
    let mut keys_skipped = 0;

    let items: Vec<_> = keys.iter().zip(elements.iter()).collect();

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

        // For Replace mode, delete existing keys in batch
        if if_exists == WriteMode::Replace {
            let mut del_pipe = redis::pipe();
            for (key, _) in batch_items {
                del_pipe.del(*key).ignore();
            }
            let _ = del_pipe.query_async::<()>(conn).await;
        }

        // Build pipeline for RPUSH operations
        let mut pipe = redis::pipe();
        let mut batch_count = 0;

        for (batch_idx, (key, list_elements)) in batch_items.iter().enumerate() {
            // Skip if key exists and mode is Fail
            if existing_keys.contains(&batch_idx) {
                keys_skipped += 1;
                continue;
            }

            // Skip empty lists
            if list_elements.is_empty() {
                continue;
            }

            pipe.rpush(*key, *list_elements);
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

/// Write set members to Redis.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - List of Redis keys to write to
/// * `members` - 2D list of members for each set
/// * `ttl` - Optional TTL in seconds for each key
/// * `if_exists` - How to handle existing keys (fail, replace, append)
///
/// # Returns
/// A `WriteResult` with the number of keys written.
pub fn write_sets(
    url: &str,
    keys: Vec<String>,
    members: Vec<Vec<String>>,
    ttl: Option<i64>,
    if_exists: WriteMode,
) -> Result<WriteResult> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        write_sets_async(&mut conn, keys, members, ttl, if_exists).await
    })
}

/// Async implementation of set writing with pipelining.
async fn write_sets_async(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: Vec<String>,
    members: Vec<Vec<String>>,
    ttl: Option<i64>,
    if_exists: WriteMode,
) -> Result<WriteResult> {
    let mut keys_written = 0;
    let mut keys_failed = 0;
    let mut keys_skipped = 0;

    let items: Vec<_> = keys.iter().zip(members.iter()).collect();

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

        // For Replace mode, delete existing keys in batch
        if if_exists == WriteMode::Replace {
            let mut del_pipe = redis::pipe();
            for (key, _) in batch_items {
                del_pipe.del(*key).ignore();
            }
            let _ = del_pipe.query_async::<()>(conn).await;
        }

        // Build pipeline for SADD operations
        let mut pipe = redis::pipe();
        let mut batch_count = 0;

        for (batch_idx, (key, set_members)) in batch_items.iter().enumerate() {
            // Skip if key exists and mode is Fail
            if existing_keys.contains(&batch_idx) {
                keys_skipped += 1;
                continue;
            }

            // Skip empty sets
            if set_members.is_empty() {
                continue;
            }

            pipe.sadd(*key, *set_members);
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

/// Write sorted set members to Redis.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - List of Redis keys to write to
/// * `members` - 2D list of (member, score) pairs for each sorted set
/// * `ttl` - Optional TTL in seconds for each key
/// * `if_exists` - How to handle existing keys (fail, replace, append)
///
/// # Returns
/// A `WriteResult` with the number of keys written.
pub fn write_zsets(
    url: &str,
    keys: Vec<String>,
    members: Vec<Vec<(String, f64)>>,
    ttl: Option<i64>,
    if_exists: WriteMode,
) -> Result<WriteResult> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        write_zsets_async(&mut conn, keys, members, ttl, if_exists).await
    })
}

/// Async implementation of sorted set writing with pipelining.
async fn write_zsets_async(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: Vec<String>,
    members: Vec<Vec<(String, f64)>>,
    ttl: Option<i64>,
    if_exists: WriteMode,
) -> Result<WriteResult> {
    let mut keys_written = 0;
    let mut keys_failed = 0;
    let mut keys_skipped = 0;

    let items: Vec<_> = keys.iter().zip(members.iter()).collect();

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

        // For Replace mode, delete existing keys in batch
        if if_exists == WriteMode::Replace {
            let mut del_pipe = redis::pipe();
            for (key, _) in batch_items {
                del_pipe.del(*key).ignore();
            }
            let _ = del_pipe.query_async::<()>(conn).await;
        }

        // Build pipeline for ZADD operations
        let mut pipe = redis::pipe();
        let mut batch_count = 0;

        for (batch_idx, (key, zset_members)) in batch_items.iter().enumerate() {
            // Skip if key exists and mode is Fail
            if existing_keys.contains(&batch_idx) {
                keys_skipped += 1;
                continue;
            }

            // Skip empty sorted sets
            if zset_members.is_empty() {
                continue;
            }

            // ZADD expects (score, member) pairs
            let score_members: Vec<(f64, &str)> =
                zset_members.iter().map(|(m, s)| (*s, m.as_str())).collect();

            pipe.zadd_multiple(*key, &score_members);
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

    #[test]
    fn test_write_result_detailed_new() {
        let result = WriteResultDetailed::new();
        assert_eq!(result.keys_written, 0);
        assert_eq!(result.keys_failed, 0);
        assert_eq!(result.keys_skipped, 0);
        assert!(result.succeeded_keys.is_empty());
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_write_result_detailed_complete_success() {
        let mut result = WriteResultDetailed::new();
        result.keys_written = 5;
        result.succeeded_keys = vec!["key1".into(), "key2".into()];

        assert!(result.is_complete_success());
        assert!(result.failed_keys().is_empty());
    }

    #[test]
    fn test_write_result_detailed_with_failures() {
        let mut result = WriteResultDetailed::new();
        result.keys_written = 3;
        result.keys_failed = 2;
        result.succeeded_keys = vec!["key1".into(), "key2".into(), "key3".into()];
        result.errors = vec![
            KeyError {
                key: "key4".into(),
                error: "WRONGTYPE".into(),
            },
            KeyError {
                key: "key5".into(),
                error: "OOM".into(),
            },
        ];

        assert!(!result.is_complete_success());

        let failed = result.failed_keys();
        assert_eq!(failed.len(), 2);
        assert!(failed.contains(&"key4"));
        assert!(failed.contains(&"key5"));

        let error_map = result.error_map();
        assert_eq!(error_map.get("key4"), Some(&"WRONGTYPE"));
        assert_eq!(error_map.get("key5"), Some(&"OOM"));
    }

    #[test]
    fn test_write_result_detailed_to_basic() {
        let mut result = WriteResultDetailed::new();
        result.keys_written = 10;
        result.keys_failed = 2;
        result.keys_skipped = 3;
        result.succeeded_keys = vec!["key1".into()];
        result.errors = vec![KeyError {
            key: "key2".into(),
            error: "error".into(),
        }];

        let basic = result.to_basic();
        assert_eq!(basic.keys_written, 10);
        assert_eq!(basic.keys_failed, 2);
        assert_eq!(basic.keys_skipped, 3);
    }

    #[test]
    fn test_key_error_creation() {
        let error = KeyError {
            key: "user:123".into(),
            error: "WRONGTYPE Operation against a key holding the wrong kind of value".into(),
        };
        assert_eq!(error.key, "user:123");
        assert!(error.error.contains("WRONGTYPE"));
    }
}
