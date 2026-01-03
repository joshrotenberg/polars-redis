//! Write support for Redis data structures.
//!
//! This module provides functionality to write Polars DataFrames to Redis
//! as hashes or JSON documents.

use std::collections::HashMap;

use redis::AsyncCommands;
use tokio::runtime::Runtime;

use crate::connection::RedisConnection;
use crate::error::{Error, Result};

/// Result of a write operation.
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// Number of keys written.
    pub keys_written: usize,
    /// Number of keys that failed to write (if any).
    pub keys_failed: usize,
}

/// Write hashes to Redis from field data.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - List of Redis keys to write to
/// * `fields` - List of field names
/// * `values` - 2D list of values (rows x columns), same order as fields
///
/// # Returns
/// A `WriteResult` with the number of keys written.
pub fn write_hashes(
    url: &str,
    keys: Vec<String>,
    fields: Vec<String>,
    values: Vec<Vec<Option<String>>>,
) -> Result<WriteResult> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        write_hashes_async(&mut conn, keys, fields, values).await
    })
}

/// Async implementation of hash writing.
async fn write_hashes_async(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: Vec<String>,
    fields: Vec<String>,
    values: Vec<Vec<Option<String>>>,
) -> Result<WriteResult> {
    let mut keys_written = 0;
    let mut keys_failed = 0;

    for (i, key) in keys.iter().enumerate() {
        if i >= values.len() {
            break;
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
            match conn
                .hset_multiple::<_, _, _, ()>(key, &hash_data.into_iter().collect::<Vec<_>>())
                .await
            {
                Ok(_) => keys_written += 1,
                Err(_) => keys_failed += 1,
            }
        }
    }

    Ok(WriteResult {
        keys_written,
        keys_failed,
    })
}

/// Write JSON documents to Redis.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `keys` - List of Redis keys to write to
/// * `json_strings` - List of JSON strings to write
///
/// # Returns
/// A `WriteResult` with the number of keys written.
pub fn write_json(url: &str, keys: Vec<String>, json_strings: Vec<String>) -> Result<WriteResult> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        write_json_async(&mut conn, keys, json_strings).await
    })
}

/// Async implementation of JSON writing.
async fn write_json_async(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: Vec<String>,
    json_strings: Vec<String>,
) -> Result<WriteResult> {
    let mut keys_written = 0;
    let mut keys_failed = 0;

    for (key, json_str) in keys.iter().zip(json_strings.iter()) {
        match redis::cmd("JSON.SET")
            .arg(key)
            .arg("$")
            .arg(json_str)
            .query_async::<Option<String>>(conn)
            .await
        {
            Ok(_) => keys_written += 1,
            Err(_) => keys_failed += 1,
        }
    }

    Ok(WriteResult {
        keys_written,
        keys_failed,
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
        };
        assert_eq!(result.keys_written, 10);
        assert_eq!(result.keys_failed, 2);
    }
}
