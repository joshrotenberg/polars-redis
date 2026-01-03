//! Hash reading operations for Redis.
//!
//! This module provides functionality for reading Redis hashes in batches,
//! with support for projection pushdown (fetching only specific fields).

use std::collections::HashMap;

use redis::aio::MultiplexedConnection;

use crate::error::Result;

/// Result of fetching a single hash from Redis.
#[derive(Debug, Clone)]
pub struct HashData {
    /// The Redis key.
    pub key: String,
    /// Field-value pairs from the hash.
    pub fields: HashMap<String, Option<String>>,
}

/// Fetch all fields from multiple hashes using HGETALL.
///
/// Uses pipelining for efficiency. Returns data in the same order as the input keys.
/// If a key doesn't exist or isn't a hash, returns an empty HashMap for that key.
pub async fn fetch_hashes_all(
    conn: &mut MultiplexedConnection,
    keys: &[String],
) -> Result<Vec<HashData>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut pipe = redis::pipe();
    for key in keys {
        pipe.hgetall(key);
    }

    let results: Vec<HashMap<String, String>> = pipe.query_async(conn).await?;

    Ok(keys
        .iter()
        .zip(results)
        .map(|(key, fields)| HashData {
            key: key.clone(),
            fields: fields.into_iter().map(|(k, v)| (k, Some(v))).collect(),
        })
        .collect())
}

/// Fetch specific fields from multiple hashes using HMGET.
///
/// This enables projection pushdown - only fetching the fields we need.
/// Uses pipelining for efficiency. Returns data in the same order as the input keys.
/// Missing fields are represented as None.
pub async fn fetch_hashes_fields(
    conn: &mut MultiplexedConnection,
    keys: &[String],
    fields: &[String],
) -> Result<Vec<HashData>> {
    if keys.is_empty() || fields.is_empty() {
        return Ok(Vec::new());
    }

    let mut pipe = redis::pipe();
    for key in keys {
        pipe.cmd("HMGET").arg(key).arg(fields);
    }

    // HMGET returns Vec<Option<String>> for each key
    let results: Vec<Vec<Option<String>>> = pipe.query_async(conn).await?;

    Ok(keys
        .iter()
        .zip(results)
        .map(|(key, values)| {
            let field_map: HashMap<String, Option<String>> = fields
                .iter()
                .zip(values)
                .map(|(field, value)| (field.clone(), value))
                .collect();

            HashData {
                key: key.clone(),
                fields: field_map,
            }
        })
        .collect())
}

/// Fetch hash data with optional projection pushdown.
///
/// If `fields` is Some, uses HMGET to fetch only those fields.
/// If `fields` is None, uses HGETALL to fetch all fields.
pub async fn fetch_hashes(
    conn: &mut MultiplexedConnection,
    keys: &[String],
    fields: Option<&[String]>,
) -> Result<Vec<HashData>> {
    match fields {
        Some(f) => fetch_hashes_fields(conn, keys, f).await,
        None => fetch_hashes_all(conn, keys).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_data_creation() {
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), Some("Alice".to_string()));
        fields.insert("age".to_string(), Some("30".to_string()));

        let data = HashData {
            key: "user:1".to_string(),
            fields,
        };

        assert_eq!(data.key, "user:1");
        assert_eq!(data.fields.get("name"), Some(&Some("Alice".to_string())));
        assert_eq!(data.fields.get("age"), Some(&Some("30".to_string())));
    }

    #[test]
    fn test_hash_data_with_missing_field() {
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), Some("Alice".to_string()));
        fields.insert("email".to_string(), None); // Missing field

        let data = HashData {
            key: "user:1".to_string(),
            fields,
        };

        assert_eq!(data.fields.get("name"), Some(&Some("Alice".to_string())));
        assert_eq!(data.fields.get("email"), Some(&None));
    }
}
