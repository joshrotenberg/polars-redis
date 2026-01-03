//! Schema inference for Redis data.
//!
//! This module provides functionality to infer Polars schemas from Redis data
//! by sampling keys and analyzing field values.

use std::collections::{HashMap, HashSet};

use redis::AsyncCommands;
use redis::aio::MultiplexedConnection;
use tokio::runtime::Runtime;

use crate::connection::RedisConnection;
use crate::error::{Error, Result};
use crate::schema::RedisType;

/// Inferred schema from Redis data.
#[derive(Debug, Clone)]
pub struct InferredSchema {
    /// Field names and their inferred types.
    pub fields: Vec<(String, RedisType)>,
    /// Number of keys sampled.
    pub sample_count: usize,
}

impl InferredSchema {
    /// Convert to a list of (field_name, type_string) tuples for Python.
    pub fn to_type_strings(&self) -> Vec<(String, String)> {
        self.fields
            .iter()
            .map(|(name, dtype)| {
                let type_str = match dtype {
                    RedisType::Utf8 => "utf8",
                    RedisType::Int64 => "int64",
                    RedisType::Float64 => "float64",
                    RedisType::Boolean => "bool",
                    RedisType::Date => "date",
                    RedisType::Datetime => "datetime",
                };
                (name.clone(), type_str.to_string())
            })
            .collect()
    }
}

/// Infer schema from Redis hashes.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `pattern` - Key pattern to match
/// * `sample_size` - Maximum number of keys to sample
/// * `type_inference` - Whether to infer types (if false, all fields are Utf8)
///
/// # Returns
/// An `InferredSchema` with field names and types.
pub fn infer_hash_schema(
    url: &str,
    pattern: &str,
    sample_size: usize,
    type_inference: bool,
) -> Result<InferredSchema> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        infer_hash_schema_async(&mut conn, pattern, sample_size, type_inference).await
    })
}

/// Async implementation of hash schema inference.
async fn infer_hash_schema_async(
    conn: &mut MultiplexedConnection,
    pattern: &str,
    sample_size: usize,
    type_inference: bool,
) -> Result<InferredSchema> {
    // Collect sample keys
    let keys = scan_sample_keys(conn, pattern, sample_size).await?;

    if keys.is_empty() {
        return Ok(InferredSchema {
            fields: vec![],
            sample_count: 0,
        });
    }

    // Collect all field names and their values
    let mut field_values: HashMap<String, Vec<Option<String>>> = HashMap::new();

    for key in &keys {
        let hash_data: HashMap<String, String> = conn.hgetall(key).await?;

        // Track which fields this hash has
        let fields_in_hash: HashSet<&String> = hash_data.keys().collect();

        // Add values for fields that exist
        for (field, value) in &hash_data {
            field_values
                .entry(field.clone())
                .or_default()
                .push(Some(value.clone()));
        }

        // Add None for fields that don't exist in this hash but exist in others
        for (field, values) in &mut field_values {
            if !fields_in_hash.contains(field) {
                values.push(None);
            }
        }
    }

    // Infer types for each field
    let mut fields: Vec<(String, RedisType)> = field_values
        .into_iter()
        .map(|(name, values)| {
            let dtype = if type_inference {
                infer_type_from_values(&values)
            } else {
                RedisType::Utf8
            };
            (name, dtype)
        })
        .collect();

    // Sort fields alphabetically for consistent ordering
    fields.sort_by(|a, b| a.0.cmp(&b.0));

    Ok(InferredSchema {
        fields,
        sample_count: keys.len(),
    })
}

/// Infer schema from RedisJSON documents.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `pattern` - Key pattern to match
/// * `sample_size` - Maximum number of keys to sample
///
/// # Returns
/// An `InferredSchema` with field names and types.
pub fn infer_json_schema(url: &str, pattern: &str, sample_size: usize) -> Result<InferredSchema> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        infer_json_schema_async(&mut conn, pattern, sample_size).await
    })
}

/// Async implementation of JSON schema inference.
async fn infer_json_schema_async(
    conn: &mut MultiplexedConnection,
    pattern: &str,
    sample_size: usize,
) -> Result<InferredSchema> {
    // Collect sample keys
    let keys = scan_sample_keys(conn, pattern, sample_size).await?;

    if keys.is_empty() {
        return Ok(InferredSchema {
            fields: vec![],
            sample_count: 0,
        });
    }

    // Collect all field names and their values
    let mut field_values: HashMap<String, Vec<Option<serde_json::Value>>> = HashMap::new();

    for key in &keys {
        // Fetch JSON document
        let json_str: Option<String> = redis::cmd("JSON.GET")
            .arg(key)
            .arg("$")
            .query_async(conn)
            .await?;

        if let Some(json_str) = json_str {
            // Parse JSON - Redis returns an array wrapper
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&json_str) {
                let doc = match parsed {
                    serde_json::Value::Array(mut arr) if !arr.is_empty() => arr.remove(0),
                    other => other,
                };

                if let serde_json::Value::Object(obj) = doc {
                    let fields_in_doc: HashSet<&String> = obj.keys().collect();

                    // Add values for fields that exist
                    for (field, value) in &obj {
                        field_values
                            .entry(field.clone())
                            .or_default()
                            .push(Some(value.clone()));
                    }

                    // Add None for fields that don't exist in this doc but exist in others
                    for (field, values) in &mut field_values {
                        if !fields_in_doc.contains(field) {
                            values.push(None);
                        }
                    }
                }
            }
        }
    }

    // Infer types for each field
    let mut fields: Vec<(String, RedisType)> = field_values
        .into_iter()
        .map(|(name, values)| {
            let dtype = infer_type_from_json_values(&values);
            (name, dtype)
        })
        .collect();

    // Sort fields alphabetically for consistent ordering
    fields.sort_by(|a, b| a.0.cmp(&b.0));

    Ok(InferredSchema {
        fields,
        sample_count: keys.len(),
    })
}

/// Scan for sample keys matching a pattern.
async fn scan_sample_keys(
    conn: &mut MultiplexedConnection,
    pattern: &str,
    max_keys: usize,
) -> Result<Vec<String>> {
    let mut keys = Vec::new();
    let mut cursor: u64 = 0;

    loop {
        let (new_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(100)
            .query_async(conn)
            .await?;

        keys.extend(batch);
        cursor = new_cursor;

        if cursor == 0 || keys.len() >= max_keys {
            break;
        }
    }

    // Truncate to max_keys
    keys.truncate(max_keys);
    Ok(keys)
}

/// Infer type from a collection of string values.
fn infer_type_from_values(values: &[Option<String>]) -> RedisType {
    let non_null_values: Vec<&str> = values.iter().filter_map(|v| v.as_deref()).collect();

    if non_null_values.is_empty() {
        return RedisType::Utf8;
    }

    // Try Int64
    if non_null_values.iter().all(|v| v.parse::<i64>().is_ok()) {
        return RedisType::Int64;
    }

    // Try Float64
    if non_null_values.iter().all(|v| v.parse::<f64>().is_ok()) {
        return RedisType::Float64;
    }

    // Try Boolean
    if non_null_values
        .iter()
        .all(|v| is_boolean_string(v.to_lowercase().as_str()))
    {
        return RedisType::Boolean;
    }

    // Default to Utf8
    RedisType::Utf8
}

/// Infer type from a collection of JSON values.
fn infer_type_from_json_values(values: &[Option<serde_json::Value>]) -> RedisType {
    let non_null_values: Vec<&serde_json::Value> =
        values.iter().filter_map(|v| v.as_ref()).collect();

    if non_null_values.is_empty() {
        return RedisType::Utf8;
    }

    // Check if all values are the same JSON type
    let first_type = json_value_type(non_null_values[0]);

    if non_null_values
        .iter()
        .all(|v| json_value_type(v) == first_type)
    {
        match first_type {
            "boolean" => RedisType::Boolean,
            "integer" => RedisType::Int64,
            "number" => RedisType::Float64,
            _ => RedisType::Utf8,
        }
    } else {
        // Mixed types - check if all numeric
        if non_null_values
            .iter()
            .all(|v| matches!(json_value_type(v), "integer" | "number"))
        {
            RedisType::Float64
        } else {
            RedisType::Utf8
        }
    }
}

/// Get the type of a JSON value as a string.
fn json_value_type(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "integer"
            } else {
                "number"
            }
        }
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// Check if a string represents a boolean value.
fn is_boolean_string(s: &str) -> bool {
    matches!(
        s,
        "true" | "false" | "1" | "0" | "yes" | "no" | "t" | "f" | "y" | "n"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_infer_type_int() {
        let values = vec![
            Some("1".to_string()),
            Some("42".to_string()),
            Some("-10".to_string()),
        ];
        assert!(matches!(infer_type_from_values(&values), RedisType::Int64));
    }

    #[test]
    fn test_infer_type_float() {
        let values = vec![
            Some("1.5".to_string()),
            Some("42.0".to_string()),
            Some("-10.25".to_string()),
        ];
        assert!(matches!(
            infer_type_from_values(&values),
            RedisType::Float64
        ));
    }

    #[test]
    fn test_infer_type_mixed_numeric() {
        // Mix of int-looking and float-looking strings -> Float64
        let values = vec![
            Some("1".to_string()),
            Some("42.5".to_string()),
            Some("-10".to_string()),
        ];
        assert!(matches!(
            infer_type_from_values(&values),
            RedisType::Float64
        ));
    }

    #[test]
    fn test_infer_type_bool() {
        let values = vec![
            Some("true".to_string()),
            Some("false".to_string()),
            Some("True".to_string()),
        ];
        assert!(matches!(
            infer_type_from_values(&values),
            RedisType::Boolean
        ));
    }

    #[test]
    fn test_infer_type_string() {
        let values = vec![
            Some("hello".to_string()),
            Some("world".to_string()),
            Some("123abc".to_string()),
        ];
        assert!(matches!(infer_type_from_values(&values), RedisType::Utf8));
    }

    #[test]
    fn test_infer_type_with_nulls() {
        let values = vec![Some("42".to_string()), None, Some("100".to_string())];
        assert!(matches!(infer_type_from_values(&values), RedisType::Int64));
    }

    #[test]
    fn test_infer_type_all_nulls() {
        let values: Vec<Option<String>> = vec![None, None, None];
        assert!(matches!(infer_type_from_values(&values), RedisType::Utf8));
    }

    #[test]
    fn test_infer_json_type_bool() {
        let values = vec![
            Some(serde_json::Value::Bool(true)),
            Some(serde_json::Value::Bool(false)),
        ];
        assert!(matches!(
            infer_type_from_json_values(&values),
            RedisType::Boolean
        ));
    }

    #[test]
    fn test_infer_json_type_int() {
        let values = vec![
            Some(serde_json::json!(42)),
            Some(serde_json::json!(-10)),
            Some(serde_json::json!(0)),
        ];
        assert!(matches!(
            infer_type_from_json_values(&values),
            RedisType::Int64
        ));
    }

    #[test]
    fn test_infer_json_type_float() {
        let values = vec![
            Some(serde_json::json!(42.5)),
            Some(serde_json::json!(-10.25)),
        ];
        assert!(matches!(
            infer_type_from_json_values(&values),
            RedisType::Float64
        ));
    }

    #[test]
    fn test_infer_json_type_string() {
        let values = vec![
            Some(serde_json::json!("hello")),
            Some(serde_json::json!("world")),
        ];
        assert!(matches!(
            infer_type_from_json_values(&values),
            RedisType::Utf8
        ));
    }
}
