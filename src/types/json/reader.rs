//! JSON reading operations for Redis.
//!
//! This module provides functionality for reading RedisJSON documents in batches,
//! with support for JSONPath projection.

use redis::aio::ConnectionManager;

use crate::error::Result;

/// Result of fetching a single JSON document from Redis.
#[derive(Debug, Clone)]
pub struct JsonData {
    /// The Redis key.
    pub key: String,
    /// The JSON document as a string (will be parsed later).
    pub json: Option<String>,
    /// TTL in seconds (-1 = no expiry, -2 = key doesn't exist, None = TTL not requested).
    pub ttl: Option<i64>,
}

/// Fetch JSON documents from multiple keys using JSON.GET.
///
/// Uses pipelining for efficiency. Returns data in the same order as the input keys.
/// If a key doesn't exist or isn't a JSON document, returns None for that key.
pub async fn fetch_json_all(
    conn: &mut ConnectionManager,
    keys: &[String],
    include_ttl: bool,
) -> Result<Vec<JsonData>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut pipe = redis::pipe();
    for key in keys {
        pipe.cmd("JSON.GET").arg(key).arg("$");
    }

    let results: Vec<Option<String>> = pipe.query_async(conn).await?;

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
        .map(|((key, json), ttl)| JsonData {
            key: key.clone(),
            json,
            ttl,
        })
        .collect())
}

/// Fetch specific paths from JSON documents using JSON.GET with multiple paths.
///
/// This enables projection pushdown - only fetching the paths we need.
/// Uses pipelining for efficiency.
pub async fn fetch_json_paths(
    conn: &mut ConnectionManager,
    keys: &[String],
    paths: &[String],
    include_ttl: bool,
) -> Result<Vec<JsonData>> {
    if keys.is_empty() || paths.is_empty() {
        return Ok(Vec::new());
    }

    let mut pipe = redis::pipe();
    for key in keys {
        let mut cmd = redis::cmd("JSON.GET");
        cmd.arg(key);
        for path in paths {
            cmd.arg(format!("$.{}", path));
        }
        pipe.add_command(cmd);
    }

    let results: Vec<Option<String>> = pipe.query_async(conn).await?;

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
        .map(|((key, json), ttl)| JsonData {
            key: key.clone(),
            json,
            ttl,
        })
        .collect())
}

/// Fetch TTLs for multiple keys using pipelining.
///
/// Returns a vector of Option<i64> where:
/// - Some(ttl) where ttl >= 0: key has TTL in seconds
/// - Some(-1): key exists but has no expiry
/// - Some(-2): key doesn't exist
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

/// Fetch JSON data with optional path projection.
///
/// If `paths` is Some, uses JSON.GET with specific paths.
/// If `paths` is None, uses JSON.GET $ to fetch the full document.
pub async fn fetch_json(
    conn: &mut ConnectionManager,
    keys: &[String],
    paths: Option<&[String]>,
    include_ttl: bool,
) -> Result<Vec<JsonData>> {
    match paths {
        Some(p) if !p.is_empty() => fetch_json_paths(conn, keys, p, include_ttl).await,
        _ => fetch_json_all(conn, keys, include_ttl).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_data_creation() {
        let data = JsonData {
            key: "doc:1".to_string(),
            json: Some(r#"{"name":"Alice","age":30}"#.to_string()),
            ttl: None,
        };

        assert_eq!(data.key, "doc:1");
        assert!(data.json.is_some());
        assert_eq!(data.ttl, None);
    }

    #[test]
    fn test_json_data_missing() {
        let data = JsonData {
            key: "doc:missing".to_string(),
            json: None,
            ttl: Some(-2),
        };

        assert_eq!(data.key, "doc:missing");
        assert!(data.json.is_none());
        assert_eq!(data.ttl, Some(-2));
    }

    #[test]
    fn test_json_data_with_ttl() {
        let data = JsonData {
            key: "doc:1".to_string(),
            json: Some(r#"{"name":"Alice"}"#.to_string()),
            ttl: Some(3600),
        };

        assert_eq!(data.ttl, Some(3600));
    }
}
