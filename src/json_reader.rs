//! JSON reading operations for Redis.
//!
//! This module provides functionality for reading RedisJSON documents in batches,
//! with support for JSONPath projection.

use redis::aio::MultiplexedConnection;

use crate::error::Result;

/// Result of fetching a single JSON document from Redis.
#[derive(Debug, Clone)]
pub struct JsonData {
    /// The Redis key.
    pub key: String,
    /// The JSON document as a string (will be parsed later).
    pub json: Option<String>,
}

/// Fetch JSON documents from multiple keys using JSON.GET.
///
/// Uses pipelining for efficiency. Returns data in the same order as the input keys.
/// If a key doesn't exist or isn't a JSON document, returns None for that key.
pub async fn fetch_json_all(
    conn: &mut MultiplexedConnection,
    keys: &[String],
) -> Result<Vec<JsonData>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut pipe = redis::pipe();
    for key in keys {
        pipe.cmd("JSON.GET").arg(key).arg("$");
    }

    let results: Vec<Option<String>> = pipe.query_async(conn).await?;

    Ok(keys
        .iter()
        .zip(results)
        .map(|(key, json)| JsonData {
            key: key.clone(),
            json,
        })
        .collect())
}

/// Fetch specific paths from JSON documents using JSON.GET with multiple paths.
///
/// This enables projection pushdown - only fetching the paths we need.
/// Uses pipelining for efficiency.
pub async fn fetch_json_paths(
    conn: &mut MultiplexedConnection,
    keys: &[String],
    paths: &[String],
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

    Ok(keys
        .iter()
        .zip(results)
        .map(|(key, json)| JsonData {
            key: key.clone(),
            json,
        })
        .collect())
}

/// Fetch JSON data with optional path projection.
///
/// If `paths` is Some, uses JSON.GET with specific paths.
/// If `paths` is None, uses JSON.GET $ to fetch the full document.
pub async fn fetch_json(
    conn: &mut MultiplexedConnection,
    keys: &[String],
    paths: Option<&[String]>,
) -> Result<Vec<JsonData>> {
    match paths {
        Some(p) if !p.is_empty() => fetch_json_paths(conn, keys, p).await,
        _ => fetch_json_all(conn, keys).await,
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
        };

        assert_eq!(data.key, "doc:1");
        assert!(data.json.is_some());
    }

    #[test]
    fn test_json_data_missing() {
        let data = JsonData {
            key: "doc:missing".to_string(),
            json: None,
        };

        assert_eq!(data.key, "doc:missing");
        assert!(data.json.is_none());
    }
}
