//! String reading operations for Redis.
//!
//! This module provides functionality for reading Redis string values in batches.

use redis::aio::ConnectionManager;
#[cfg(feature = "cluster")]
use redis::cluster_async::ClusterConnection;

use crate::error::Result;

/// Result of fetching a single string from Redis.
#[derive(Debug, Clone)]
pub struct StringData {
    /// The Redis key.
    pub key: String,
    /// The string value (None if key doesn't exist).
    pub value: Option<String>,
}

/// Fetch multiple string values using MGET.
///
/// Uses a single MGET command for efficiency.
/// Returns data in the same order as the input keys.
/// If a key doesn't exist, returns None for that key's value.
pub async fn fetch_strings(
    conn: &mut ConnectionManager,
    keys: &[String],
) -> Result<Vec<StringData>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    // MGET returns Vec<Option<String>>
    let results: Vec<Option<String>> = redis::cmd("MGET").arg(keys).query_async(conn).await?;

    Ok(keys
        .iter()
        .zip(results)
        .map(|(key, value)| StringData {
            key: key.clone(),
            value,
        })
        .collect())
}

// ============================================================================
// Cluster support (with cluster feature)
// ============================================================================

/// Fetch multiple string values from a cluster using MGET.
///
/// Note: In cluster mode, MGET only works for keys in the same hash slot.
/// For keys across different slots, we use pipelined GET commands.
#[cfg(feature = "cluster")]
pub async fn fetch_strings_cluster(
    conn: &mut ClusterConnection,
    keys: &[String],
) -> Result<Vec<StringData>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    // In cluster mode, MGET might fail if keys are in different slots
    // Use pipelined GET commands for safety
    let mut pipe = redis::pipe();
    for key in keys {
        pipe.cmd("GET").arg(key);
    }

    let results: Vec<Option<String>> = pipe.query_async(conn).await?;

    Ok(keys
        .iter()
        .zip(results)
        .map(|(key, value)| StringData {
            key: key.clone(),
            value,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_data_creation() {
        let data = StringData {
            key: "cache:1".to_string(),
            value: Some("hello world".to_string()),
        };

        assert_eq!(data.key, "cache:1");
        assert_eq!(data.value, Some("hello world".to_string()));
    }

    #[test]
    fn test_string_data_missing() {
        let data = StringData {
            key: "cache:missing".to_string(),
            value: None,
        };

        assert_eq!(data.key, "cache:missing");
        assert!(data.value.is_none());
    }
}
