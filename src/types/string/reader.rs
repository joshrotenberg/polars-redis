//! String reading operations for Redis.
//!
//! This module provides functionality for reading Redis string values in batches.

use redis::aio::MultiplexedConnection;

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
    conn: &mut MultiplexedConnection,
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
