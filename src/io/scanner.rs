//! SCAN iteration logic for Redis key scanning.

#![allow(dead_code)] // Functions will be used when we wire up the Python bindings

use crate::connection::RedisConnection;
use crate::error::Result;
use redis::AsyncCommands;

/// Configuration for Redis SCAN operations.
#[derive(Debug, Clone)]
pub struct ScanConfig {
    /// Key pattern to match (e.g., "user:*").
    pub pattern: String,
    /// COUNT hint for SCAN command.
    pub count_hint: usize,
    /// Maximum number of keys to return (None for unlimited).
    pub max_keys: Option<usize>,
}

impl Default for ScanConfig {
    fn default() -> Self {
        Self {
            pattern: "*".to_string(),
            count_hint: 100,
            max_keys: None,
        }
    }
}

impl ScanConfig {
    /// Create a new ScanConfig with the given pattern.
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
            ..Default::default()
        }
    }

    /// Set the COUNT hint.
    pub fn with_count_hint(mut self, count: usize) -> Self {
        self.count_hint = count;
        self
    }

    /// Set the maximum number of keys to return.
    pub fn with_max_keys(mut self, max: usize) -> Self {
        self.max_keys = Some(max);
        self
    }
}

/// Scan keys from Redis matching a pattern.
///
/// Returns a vector of matching keys. For large keyspaces, consider using
/// the iterator-based approach instead.
pub async fn scan_keys(conn: &RedisConnection, config: &ScanConfig) -> Result<Vec<String>> {
    let mut redis_conn = conn.get_async_connection().await?;
    let mut keys: Vec<String> = Vec::new();
    let mut cursor = 0u64;

    loop {
        let (new_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(&config.pattern)
            .arg("COUNT")
            .arg(config.count_hint)
            .query_async(&mut redis_conn)
            .await?;

        keys.extend(batch);
        cursor = new_cursor;

        // Check if we've hit our limit
        if let Some(max) = config.max_keys
            && keys.len() >= max
        {
            keys.truncate(max);
            break;
        }

        // Cursor 0 means we've completed the full scan
        if cursor == 0 {
            break;
        }
    }

    Ok(keys)
}

/// Fetch hash data for a batch of keys.
///
/// Uses pipelining for efficiency.
pub async fn fetch_hashes(
    conn: &RedisConnection,
    keys: &[String],
    fields: Option<&[String]>,
) -> Result<Vec<std::collections::HashMap<String, String>>> {
    let mut redis_conn = conn.get_async_connection().await?;
    let mut pipe = redis::pipe();

    for key in keys {
        if let Some(fields) = fields {
            // Projection pushdown: only fetch specified fields
            pipe.cmd("HMGET").arg(key).arg(fields);
        } else {
            // Fetch all fields
            pipe.cmd("HGETALL").arg(key);
        }
    }

    let results: Vec<std::collections::HashMap<String, String>> =
        pipe.query_async(&mut redis_conn).await?;

    Ok(results)
}

/// Fetch string values for a batch of keys.
///
/// Uses MGET for efficiency.
pub async fn fetch_strings(conn: &RedisConnection, keys: &[String]) -> Result<Vec<Option<String>>> {
    let mut redis_conn = conn.get_async_connection().await?;
    let values: Vec<Option<String>> = redis_conn.mget(keys).await?;
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_config_default() {
        let config = ScanConfig::default();
        assert_eq!(config.pattern, "*");
        assert_eq!(config.count_hint, 100);
        assert!(config.max_keys.is_none());
    }

    #[test]
    fn test_scan_config_builder() {
        let config = ScanConfig::new("user:*")
            .with_count_hint(500)
            .with_max_keys(1000);

        assert_eq!(config.pattern, "user:*");
        assert_eq!(config.count_hint, 500);
        assert_eq!(config.max_keys, Some(1000));
    }
}
