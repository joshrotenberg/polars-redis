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
    /// TTL in seconds (-1 = no expiry, -2 = key doesn't exist, None = TTL not requested).
    pub ttl: Option<i64>,
}

/// Fetch multiple string values using MGET.
///
/// Uses a single MGET command for efficiency.
/// Returns data in the same order as the input keys.
/// If a key doesn't exist, returns None for that key's value.
pub async fn fetch_strings(
    conn: &mut ConnectionManager,
    keys: &[String],
    include_ttl: bool,
) -> Result<Vec<StringData>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    // MGET returns Vec<Option<String>>
    let results: Vec<Option<String>> = redis::cmd("MGET").arg(keys).query_async(conn).await?;

    // Optionally fetch TTLs
    let ttls = if include_ttl {
        fetch_ttls(conn, keys).await?
    } else {
        vec![None; keys.len()]
    };

    Ok(keys
        .iter()
        .zip(results)
        .zip(ttls)
        .map(|((key, value), ttl)| StringData {
            key: key.clone(),
            value,
            ttl,
        })
        .collect())
}

/// Fetch TTLs for multiple keys using pipelined TTL commands.
async fn fetch_ttls(conn: &mut ConnectionManager, keys: &[String]) -> Result<Vec<Option<i64>>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut pipe = redis::pipe();
    for key in keys {
        pipe.cmd("TTL").arg(key);
    }

    let results: Vec<i64> = pipe.query_async(conn).await?;
    Ok(results.into_iter().map(Some).collect())
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
    include_ttl: bool,
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

    // Optionally fetch TTLs
    let ttls = if include_ttl {
        fetch_ttls_cluster(conn, keys).await?
    } else {
        vec![None; keys.len()]
    };

    Ok(keys
        .iter()
        .zip(results)
        .zip(ttls)
        .map(|((key, value), ttl)| StringData {
            key: key.clone(),
            value,
            ttl,
        })
        .collect())
}

/// Fetch TTLs for multiple keys from a cluster using pipelined TTL commands.
#[cfg(feature = "cluster")]
async fn fetch_ttls_cluster(
    conn: &mut ClusterConnection,
    keys: &[String],
) -> Result<Vec<Option<i64>>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut pipe = redis::pipe();
    for key in keys {
        pipe.cmd("TTL").arg(key);
    }

    let results: Vec<i64> = pipe.query_async(conn).await?;
    Ok(results.into_iter().map(Some).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_data_creation() {
        let data = StringData {
            key: "cache:1".to_string(),
            value: Some("hello world".to_string()),
            ttl: None,
        };

        assert_eq!(data.key, "cache:1");
        assert_eq!(data.value, Some("hello world".to_string()));
        assert!(data.ttl.is_none());
    }

    #[test]
    fn test_string_data_missing() {
        let data = StringData {
            key: "cache:missing".to_string(),
            value: None,
            ttl: None,
        };

        assert_eq!(data.key, "cache:missing");
        assert!(data.value.is_none());
    }

    #[test]
    fn test_string_data_with_ttl() {
        let data = StringData {
            key: "cache:expiring".to_string(),
            value: Some("temporary".to_string()),
            ttl: Some(3600),
        };

        assert_eq!(data.key, "cache:expiring");
        assert_eq!(data.value, Some("temporary".to_string()));
        assert_eq!(data.ttl, Some(3600));
    }

    #[test]
    fn test_string_data_no_expiry() {
        let data = StringData {
            key: "cache:permanent".to_string(),
            value: Some("forever".to_string()),
            ttl: Some(-1), // -1 means no expiry
        };

        assert_eq!(data.ttl, Some(-1));
    }
}
