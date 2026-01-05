//! Schema inference for Redis data.
//!
//! This module provides functionality to infer Polars schemas from Redis data
//! by sampling keys and analyzing field values.
//!
//! # Confidence Scores
//!
//! The `_with_confidence` variants of inference functions return detailed
//! statistics about type inference quality:
//!
//! ```ignore
//! let result = infer_hash_schema_with_confidence(url, pattern, sample_size)?;
//!
//! for (field, info) in &result.field_info {
//!     if info.confidence < 0.9 {
//!         println!("Warning: {} has low confidence ({:.0}%)", field, info.confidence * 100.0);
//!     }
//! }
//! ```

use std::collections::{HashMap, HashSet};

use redis::AsyncCommands;
use redis::aio::ConnectionManager;
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

/// Detailed inference information for a single field.
#[derive(Debug, Clone)]
pub struct FieldInferenceInfo {
    /// The inferred type for this field.
    pub inferred_type: RedisType,
    /// Confidence score from 0.0 to 1.0.
    /// 1.0 means all sampled values matched the inferred type.
    pub confidence: f64,
    /// Total number of samples for this field.
    pub samples: usize,
    /// Number of samples that successfully parsed as the inferred type.
    pub valid: usize,
    /// Number of null/missing values.
    pub nulls: usize,
    /// Type candidates that were considered, with their match counts.
    pub type_candidates: HashMap<String, usize>,
}

impl FieldInferenceInfo {
    /// Check if confidence is above a threshold (default: 0.9).
    pub fn is_confident(&self, threshold: f64) -> bool {
        self.confidence >= threshold
    }

    /// Get the percentage of null values.
    pub fn null_ratio(&self) -> f64 {
        if self.samples == 0 {
            0.0
        } else {
            self.nulls as f64 / self.samples as f64
        }
    }
}

/// Inferred schema with detailed confidence information.
#[derive(Debug, Clone)]
pub struct InferredSchemaWithConfidence {
    /// Field names and their inferred types.
    pub fields: Vec<(String, RedisType)>,
    /// Number of keys sampled.
    pub sample_count: usize,
    /// Detailed inference information for each field.
    pub field_info: HashMap<String, FieldInferenceInfo>,
}

impl InferredSchemaWithConfidence {
    /// Convert to a basic InferredSchema (discards confidence info).
    pub fn to_basic(&self) -> InferredSchema {
        InferredSchema {
            fields: self.fields.clone(),
            sample_count: self.sample_count,
        }
    }

    /// Get fields with confidence below a threshold.
    pub fn low_confidence_fields(&self, threshold: f64) -> Vec<(&str, f64)> {
        self.field_info
            .iter()
            .filter(|(_, info)| info.confidence < threshold)
            .map(|(name, info)| (name.as_str(), info.confidence))
            .collect()
    }

    /// Check if all fields have confidence above a threshold.
    pub fn all_confident(&self, threshold: f64) -> bool {
        self.field_info
            .values()
            .all(|info| info.confidence >= threshold)
    }

    /// Get overall average confidence across all fields.
    pub fn average_confidence(&self) -> f64 {
        if self.field_info.is_empty() {
            1.0
        } else {
            let sum: f64 = self.field_info.values().map(|info| info.confidence).sum();
            sum / self.field_info.len() as f64
        }
    }
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

    /// Apply schema overwrite - merge user-specified types with inferred types.
    ///
    /// User-specified types take precedence over inferred types. Fields that
    /// exist in the overwrite but not in the inferred schema are added.
    ///
    /// # Arguments
    /// * `overwrite` - User-specified field types that override inferred types
    ///
    /// # Returns
    /// A new `InferredSchema` with merged fields.
    ///
    /// # Example
    /// ```
    /// use polars_redis::infer::InferredSchema;
    /// use polars_redis::schema::RedisType;
    ///
    /// let inferred = InferredSchema {
    ///     fields: vec![
    ///         ("name".to_string(), RedisType::Utf8),
    ///         ("age".to_string(), RedisType::Utf8),  // Inferred as string
    ///         ("score".to_string(), RedisType::Float64),
    ///     ],
    ///     sample_count: 10,
    /// };
    ///
    /// // Override age to be Int64
    /// let overwrite = vec![
    ///     ("age".to_string(), RedisType::Int64),
    /// ];
    ///
    /// let merged = inferred.with_overwrite(&overwrite);
    /// // merged.fields now has age as Int64
    /// ```
    pub fn with_overwrite(self, overwrite: &[(String, RedisType)]) -> Self {
        let overwrite_map: HashMap<&str, &RedisType> =
            overwrite.iter().map(|(k, v)| (k.as_str(), v)).collect();

        // Track which fields exist in the original schema
        let existing_fields: HashSet<String> = self.fields.iter().map(|(k, _)| k.clone()).collect();

        // Start with existing fields, applying overwrites
        let mut fields: Vec<(String, RedisType)> = self
            .fields
            .into_iter()
            .map(|(name, dtype)| {
                if let Some(&override_type) = overwrite_map.get(name.as_str()) {
                    (name, *override_type)
                } else {
                    (name, dtype)
                }
            })
            .collect();

        // Add any fields from overwrite that weren't in the inferred schema
        for (name, dtype) in overwrite {
            if !existing_fields.contains(name) {
                fields.push((name.clone(), *dtype));
            }
        }

        // Re-sort alphabetically
        fields.sort_by(|a, b| a.0.cmp(&b.0));

        Self {
            fields,
            sample_count: self.sample_count,
        }
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
    let mut conn = runtime.block_on(connection.get_connection_manager())?;

    runtime.block_on(infer_hash_schema_async(
        &mut conn,
        pattern,
        sample_size,
        type_inference,
    ))
}

/// Infer schema from Redis hashes with detailed confidence information.
///
/// This function returns confidence scores for each field, indicating how
/// reliably the type was inferred. Use this when you need to:
/// - Validate schema quality before processing large datasets
/// - Identify fields that may need schema overrides
/// - Debug type inference issues
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `pattern` - Key pattern to match
/// * `sample_size` - Maximum number of keys to sample
///
/// # Returns
/// An `InferredSchemaWithConfidence` with field types and confidence data.
///
/// # Example
/// ```ignore
/// let result = infer_hash_schema_with_confidence(
///     "redis://localhost:6379",
///     "user:*",
///     100,
/// )?;
///
/// // Check for low-confidence fields
/// for (field, confidence) in result.low_confidence_fields(0.9) {
///     eprintln!("Warning: {} has {:.0}% confidence", field, confidence * 100.0);
/// }
///
/// // Decide whether to proceed
/// if result.all_confident(0.8) {
///     let schema = result.to_basic();
///     // Use schema for reading
/// } else {
///     // Consider using schema overrides
/// }
/// ```
pub fn infer_hash_schema_with_confidence(
    url: &str,
    pattern: &str,
    sample_size: usize,
) -> Result<InferredSchemaWithConfidence> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;
    let mut conn = runtime.block_on(connection.get_connection_manager())?;

    runtime.block_on(infer_hash_schema_with_confidence_async(
        &mut conn,
        pattern,
        sample_size,
    ))
}

/// Async implementation of hash schema inference with confidence.
async fn infer_hash_schema_with_confidence_async(
    conn: &mut ConnectionManager,
    pattern: &str,
    sample_size: usize,
) -> Result<InferredSchemaWithConfidence> {
    // Collect sample keys
    let keys = scan_sample_keys(conn, pattern, sample_size).await?;

    if keys.is_empty() {
        return Ok(InferredSchemaWithConfidence {
            fields: vec![],
            sample_count: 0,
            field_info: HashMap::new(),
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

    // Infer types for each field with confidence
    let mut fields: Vec<(String, RedisType)> = Vec::new();
    let mut field_info: HashMap<String, FieldInferenceInfo> = HashMap::new();

    for (name, values) in field_values {
        let (dtype, info) = infer_type_from_values_with_confidence(&values);
        fields.push((name.clone(), dtype));
        field_info.insert(name, info);
    }

    // Sort fields alphabetically for consistent ordering
    fields.sort_by(|a, b| a.0.cmp(&b.0));

    Ok(InferredSchemaWithConfidence {
        fields,
        sample_count: keys.len(),
        field_info,
    })
}

/// Async implementation of hash schema inference.
async fn infer_hash_schema_async(
    conn: &mut ConnectionManager,
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
    let mut conn = runtime.block_on(connection.get_connection_manager())?;

    runtime.block_on(infer_json_schema_async(&mut conn, pattern, sample_size))
}

/// Async implementation of JSON schema inference.
async fn infer_json_schema_async(
    conn: &mut ConnectionManager,
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
    conn: &mut ConnectionManager,
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
    infer_type_from_values_with_confidence(values).0
}

/// Infer type from a collection of string values with detailed confidence info.
///
/// Returns (inferred_type, FieldInferenceInfo).
fn infer_type_from_values_with_confidence(
    values: &[Option<String>],
) -> (RedisType, FieldInferenceInfo) {
    let total_samples = values.len();
    let null_count = values.iter().filter(|v| v.is_none()).count();
    let non_null_values: Vec<&str> = values.iter().filter_map(|v| v.as_deref()).collect();

    if non_null_values.is_empty() {
        return (
            RedisType::Utf8,
            FieldInferenceInfo {
                inferred_type: RedisType::Utf8,
                confidence: 1.0, // No data means we default to Utf8 with full confidence
                samples: total_samples,
                valid: 0,
                nulls: null_count,
                type_candidates: HashMap::new(),
            },
        );
    }

    // Count how many values parse successfully for each type
    let mut type_candidates: HashMap<String, usize> = HashMap::new();

    let int_count = non_null_values
        .iter()
        .filter(|v| v.parse::<i64>().is_ok())
        .count();
    let float_count = non_null_values
        .iter()
        .filter(|v| v.parse::<f64>().is_ok())
        .count();
    let bool_count = non_null_values
        .iter()
        .filter(|v| is_boolean_string(v.to_lowercase().as_str()))
        .count();

    type_candidates.insert("int64".to_string(), int_count);
    type_candidates.insert("float64".to_string(), float_count);
    type_candidates.insert("bool".to_string(), bool_count);
    type_candidates.insert("utf8".to_string(), non_null_values.len()); // Everything is valid as Utf8

    let non_null_count = non_null_values.len();

    // Determine best type (most specific that matches all values)
    let (inferred_type, valid_count) = if int_count == non_null_count {
        (RedisType::Int64, int_count)
    } else if float_count == non_null_count {
        (RedisType::Float64, float_count)
    } else if bool_count == non_null_count {
        (RedisType::Boolean, bool_count)
    } else {
        // Fall back to Utf8 - use the best non-Utf8 candidate for confidence
        let best_specific = [
            (RedisType::Int64, int_count),
            (RedisType::Float64, float_count),
            (RedisType::Boolean, bool_count),
        ]
        .into_iter()
        .max_by_key(|(_, count)| *count);

        if let Some((best_type, best_count)) = best_specific {
            if best_count > 0 && best_count as f64 / non_null_count as f64 >= 0.5 {
                // More than half match a specific type, but not all - low confidence
                (best_type, best_count)
            } else {
                (RedisType::Utf8, non_null_count)
            }
        } else {
            (RedisType::Utf8, non_null_count)
        }
    };

    // Calculate confidence as ratio of valid values to total non-null values
    let confidence = if non_null_count == 0 {
        1.0
    } else {
        valid_count as f64 / non_null_count as f64
    };

    (
        inferred_type,
        FieldInferenceInfo {
            inferred_type,
            confidence,
            samples: total_samples,
            valid: valid_count,
            nulls: null_count,
            type_candidates,
        },
    )
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

    #[test]
    fn test_schema_overwrite_basic() {
        let inferred = InferredSchema {
            fields: vec![
                ("age".to_string(), RedisType::Utf8),
                ("name".to_string(), RedisType::Utf8),
                ("score".to_string(), RedisType::Float64),
            ],
            sample_count: 10,
        };

        // Override age to Int64
        let overwrite = vec![("age".to_string(), RedisType::Int64)];
        let merged = inferred.with_overwrite(&overwrite);

        assert_eq!(merged.fields.len(), 3);
        assert_eq!(merged.sample_count, 10);

        // Find age and verify it's Int64
        let age_field = merged.fields.iter().find(|(n, _)| n == "age").unwrap();
        assert!(matches!(age_field.1, RedisType::Int64));

        // name should still be Utf8
        let name_field = merged.fields.iter().find(|(n, _)| n == "name").unwrap();
        assert!(matches!(name_field.1, RedisType::Utf8));
    }

    #[test]
    fn test_schema_overwrite_adds_new_fields() {
        let inferred = InferredSchema {
            fields: vec![("name".to_string(), RedisType::Utf8)],
            sample_count: 5,
        };

        // Add a field that wasn't inferred
        let overwrite = vec![("extra_field".to_string(), RedisType::Int64)];
        let merged = inferred.with_overwrite(&overwrite);

        assert_eq!(merged.fields.len(), 2);

        // extra_field should be added
        let extra = merged
            .fields
            .iter()
            .find(|(n, _)| n == "extra_field")
            .unwrap();
        assert!(matches!(extra.1, RedisType::Int64));
    }

    #[test]
    fn test_schema_overwrite_empty() {
        let inferred = InferredSchema {
            fields: vec![
                ("a".to_string(), RedisType::Utf8),
                ("b".to_string(), RedisType::Int64),
            ],
            sample_count: 10,
        };

        let overwrite: Vec<(String, RedisType)> = vec![];
        let merged = inferred.with_overwrite(&overwrite);

        assert_eq!(merged.fields.len(), 2);
    }

    #[test]
    fn test_schema_overwrite_multiple() {
        let inferred = InferredSchema {
            fields: vec![
                ("a".to_string(), RedisType::Utf8),
                ("b".to_string(), RedisType::Utf8),
                ("c".to_string(), RedisType::Utf8),
            ],
            sample_count: 10,
        };

        let overwrite = vec![
            ("a".to_string(), RedisType::Int64),
            ("c".to_string(), RedisType::Boolean),
            ("d".to_string(), RedisType::Float64),
        ];
        let merged = inferred.with_overwrite(&overwrite);

        assert_eq!(merged.fields.len(), 4);

        let a = merged.fields.iter().find(|(n, _)| n == "a").unwrap();
        assert!(matches!(a.1, RedisType::Int64));

        let b = merged.fields.iter().find(|(n, _)| n == "b").unwrap();
        assert!(matches!(b.1, RedisType::Utf8));

        let c = merged.fields.iter().find(|(n, _)| n == "c").unwrap();
        assert!(matches!(c.1, RedisType::Boolean));

        let d = merged.fields.iter().find(|(n, _)| n == "d").unwrap();
        assert!(matches!(d.1, RedisType::Float64));
    }

    // ========================================================================
    // Property-Based Tests
    // ========================================================================

    /// Helper to infer type from a single value.
    fn infer_single(s: &str) -> RedisType {
        infer_type_from_values(&[Some(s.to_string())])
    }

    /// Helper to infer type from a single JSON value.
    fn infer_single_json(v: &serde_json::Value) -> RedisType {
        infer_type_from_json_values(&[Some(v.clone())])
    }

    mod proptest_tests {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            /// Any valid i64 should be inferred as Int64.
            #[test]
            fn prop_infer_int64(value in any::<i64>()) {
                let result = infer_single(&value.to_string());
                prop_assert_eq!(result, RedisType::Int64);
            }

            /// Any valid f64 with decimal should be inferred as Float64.
            #[test]
            fn prop_infer_float64(value in any::<f64>().prop_filter("Must be finite", |v| v.is_finite())) {
                // Format with decimal to ensure it's recognized as float
                let s = format!("{:.1}", value);
                let result = infer_single(&s);
                prop_assert_eq!(result, RedisType::Float64);
            }

            /// Arbitrary non-numeric, non-boolean strings should be inferred as Utf8.
            #[test]
            fn prop_infer_utf8(s in "[a-zA-Z]{2}[a-zA-Z0-9]*") {
                // Exclude strings that could be booleans (t, f, y, n, true, false, yes, no)
                let lower = s.to_lowercase();
                prop_assume!(!matches!(lower.as_str(), "true" | "false" | "yes" | "no" | "t" | "f" | "y" | "n"));
                let result = infer_single(&s);
                prop_assert_eq!(result, RedisType::Utf8);
            }

            /// Boolean strings should be inferred as Boolean.
            #[test]
            fn prop_infer_boolean(b in prop::bool::ANY) {
                let s = if b { "true" } else { "false" };
                let result = infer_single(s);
                prop_assert_eq!(result, RedisType::Boolean);
            }

            /// Schema overwrite should preserve sample_count.
            #[test]
            fn prop_overwrite_preserves_sample_count(count in 1usize..1000) {
                let inferred = InferredSchema {
                    fields: vec![("x".to_string(), RedisType::Utf8)],
                    sample_count: count,
                };
                let merged = inferred.with_overwrite(&[("x".to_string(), RedisType::Int64)]);
                prop_assert_eq!(merged.sample_count, count);
            }

            /// Schema overwrite should include all original fields.
            #[test]
            fn prop_overwrite_includes_originals(
                field_count in 1usize..20,
            ) {
                let fields: Vec<(String, RedisType)> = (0..field_count)
                    .map(|i| (format!("field_{}", i), RedisType::Utf8))
                    .collect();

                let inferred = InferredSchema {
                    fields: fields.clone(),
                    sample_count: 10,
                };

                let merged = inferred.with_overwrite(&[]);
                prop_assert_eq!(merged.fields.len(), field_count);
            }

            /// Schema overwrite with same field should replace type.
            #[test]
            fn prop_overwrite_replaces_type(
                field_name in "[a-z]+",
            ) {
                let inferred = InferredSchema {
                    fields: vec![(field_name.clone(), RedisType::Utf8)],
                    sample_count: 5,
                };

                let merged = inferred.with_overwrite(&[(field_name.clone(), RedisType::Int64)]);

                let field = merged.fields.iter().find(|(n, _)| n == &field_name).unwrap();
                prop_assert!(matches!(field.1, RedisType::Int64));
            }
        }
    }

    // ========================================================================
    // Edge Case Tests
    // ========================================================================

    #[test]
    fn test_infer_type_whitespace() {
        // Whitespace-only strings should be Utf8
        assert_eq!(infer_single("   "), RedisType::Utf8);
        assert_eq!(infer_single("\t"), RedisType::Utf8);
        assert_eq!(infer_single("\n"), RedisType::Utf8);
    }

    #[test]
    fn test_infer_type_special_numbers() {
        // Hexadecimal should be Utf8 (not parsed as number)
        assert_eq!(infer_single("0xFF"), RedisType::Utf8);

        // Octal notation should be Utf8
        assert_eq!(infer_single("0o777"), RedisType::Utf8);

        // Binary notation should be Utf8
        assert_eq!(infer_single("0b1010"), RedisType::Utf8);
    }

    #[test]
    fn test_infer_type_numeric_edge_cases() {
        // Leading zeros - still valid integer
        assert_eq!(infer_single("007"), RedisType::Int64);

        // Plus sign prefix
        assert_eq!(infer_single("+42"), RedisType::Int64);

        // Scientific notation
        assert_eq!(infer_single("1e10"), RedisType::Float64);
        assert_eq!(infer_single("1E10"), RedisType::Float64);
        assert_eq!(infer_single("1.5e-3"), RedisType::Float64);
    }

    #[test]
    fn test_infer_type_boolean_variations() {
        // Case insensitive - lowercase only for boolean detection
        assert_eq!(infer_single("true"), RedisType::Boolean);
        assert_eq!(infer_single("false"), RedisType::Boolean);
        assert_eq!(infer_single("yes"), RedisType::Boolean);
        assert_eq!(infer_single("no"), RedisType::Boolean);

        // Not boolean (uppercase or unrecognized)
        assert_eq!(infer_single("yep"), RedisType::Utf8);
        assert_eq!(infer_single("nope"), RedisType::Utf8);
    }

    #[test]
    fn test_infer_json_type_nested() {
        // Nested objects should be Utf8 (we don't recurse)
        let nested = serde_json::json!({"inner": {"deep": 123}});
        assert_eq!(infer_single_json(&nested), RedisType::Utf8);

        // Arrays should be Utf8
        let arr = serde_json::json!([1, 2, 3]);
        assert_eq!(infer_single_json(&arr), RedisType::Utf8);
    }

    #[test]
    fn test_schema_overwrite_case_sensitive() {
        let inferred = InferredSchema {
            fields: vec![("Name".to_string(), RedisType::Utf8)],
            sample_count: 5,
        };

        // Different case should add new field, not overwrite
        let merged = inferred.with_overwrite(&[("name".to_string(), RedisType::Int64)]);
        assert_eq!(merged.fields.len(), 2);
    }

    // ========================================================================
    // Confidence Score Tests
    // ========================================================================

    #[test]
    fn test_confidence_all_integers() {
        let values = vec![
            Some("1".to_string()),
            Some("42".to_string()),
            Some("-10".to_string()),
        ];
        let (dtype, info) = infer_type_from_values_with_confidence(&values);

        assert_eq!(dtype, RedisType::Int64);
        assert_eq!(info.confidence, 1.0);
        assert_eq!(info.samples, 3);
        assert_eq!(info.valid, 3);
        assert_eq!(info.nulls, 0);
        assert_eq!(info.type_candidates.get("int64"), Some(&3));
    }

    #[test]
    fn test_confidence_with_nulls() {
        let values = vec![Some("42".to_string()), None, Some("100".to_string()), None];
        let (dtype, info) = infer_type_from_values_with_confidence(&values);

        assert_eq!(dtype, RedisType::Int64);
        assert_eq!(info.confidence, 1.0); // Confidence based on non-null values
        assert_eq!(info.samples, 4);
        assert_eq!(info.valid, 2);
        assert_eq!(info.nulls, 2);
        assert!((info.null_ratio() - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_confidence_mixed_types_low_confidence() {
        // 3 integers, 2 strings -> should have lower confidence
        // Note: integers also parse as floats, so float64 count is also 3
        let values = vec![
            Some("1".to_string()),
            Some("2".to_string()),
            Some("3".to_string()),
            Some("hello".to_string()),
            Some("world".to_string()),
        ];
        let (dtype, info) = infer_type_from_values_with_confidence(&values);

        // Falls back to Float64 with 60% confidence (3/5) - ints also parse as floats
        assert_eq!(dtype, RedisType::Float64);
        assert!((info.confidence - 0.6).abs() < 0.001);
        assert!(!info.is_confident(0.9));
        assert!(info.is_confident(0.5));
    }

    #[test]
    fn test_confidence_all_nulls() {
        let values: Vec<Option<String>> = vec![None, None, None];
        let (dtype, info) = infer_type_from_values_with_confidence(&values);

        assert_eq!(dtype, RedisType::Utf8);
        assert_eq!(info.confidence, 1.0); // Default with full confidence
        assert_eq!(info.samples, 3);
        assert_eq!(info.valid, 0);
        assert_eq!(info.nulls, 3);
    }

    #[test]
    fn test_confidence_empty() {
        let values: Vec<Option<String>> = vec![];
        let (dtype, info) = infer_type_from_values_with_confidence(&values);

        assert_eq!(dtype, RedisType::Utf8);
        assert_eq!(info.confidence, 1.0);
        assert_eq!(info.samples, 0);
    }

    #[test]
    fn test_field_inference_info_is_confident() {
        let info = FieldInferenceInfo {
            inferred_type: RedisType::Int64,
            confidence: 0.85,
            samples: 100,
            valid: 85,
            nulls: 0,
            type_candidates: HashMap::new(),
        };

        assert!(info.is_confident(0.8));
        assert!(!info.is_confident(0.9));
    }

    #[test]
    fn test_field_inference_info_null_ratio() {
        let info = FieldInferenceInfo {
            inferred_type: RedisType::Int64,
            confidence: 1.0,
            samples: 100,
            valid: 75,
            nulls: 25,
            type_candidates: HashMap::new(),
        };

        assert!((info.null_ratio() - 0.25).abs() < 0.001);
    }

    #[test]
    fn test_inferred_schema_with_confidence_to_basic() {
        let mut field_info = HashMap::new();
        field_info.insert(
            "age".to_string(),
            FieldInferenceInfo {
                inferred_type: RedisType::Int64,
                confidence: 1.0,
                samples: 10,
                valid: 10,
                nulls: 0,
                type_candidates: HashMap::new(),
            },
        );

        let schema = InferredSchemaWithConfidence {
            fields: vec![("age".to_string(), RedisType::Int64)],
            sample_count: 10,
            field_info,
        };

        let basic = schema.to_basic();
        assert_eq!(basic.fields.len(), 1);
        assert_eq!(basic.sample_count, 10);
    }

    #[test]
    fn test_inferred_schema_with_confidence_low_confidence_fields() {
        let mut field_info = HashMap::new();
        field_info.insert(
            "good".to_string(),
            FieldInferenceInfo {
                inferred_type: RedisType::Int64,
                confidence: 0.95,
                samples: 100,
                valid: 95,
                nulls: 0,
                type_candidates: HashMap::new(),
            },
        );
        field_info.insert(
            "bad".to_string(),
            FieldInferenceInfo {
                inferred_type: RedisType::Float64,
                confidence: 0.6,
                samples: 100,
                valid: 60,
                nulls: 0,
                type_candidates: HashMap::new(),
            },
        );

        let schema = InferredSchemaWithConfidence {
            fields: vec![
                ("bad".to_string(), RedisType::Float64),
                ("good".to_string(), RedisType::Int64),
            ],
            sample_count: 100,
            field_info,
        };

        let low = schema.low_confidence_fields(0.9);
        assert_eq!(low.len(), 1);
        assert_eq!(low[0].0, "bad");
        assert!((low[0].1 - 0.6).abs() < 0.001);
    }

    #[test]
    fn test_inferred_schema_with_confidence_all_confident() {
        let mut field_info = HashMap::new();
        field_info.insert(
            "a".to_string(),
            FieldInferenceInfo {
                inferred_type: RedisType::Int64,
                confidence: 0.95,
                samples: 100,
                valid: 95,
                nulls: 0,
                type_candidates: HashMap::new(),
            },
        );
        field_info.insert(
            "b".to_string(),
            FieldInferenceInfo {
                inferred_type: RedisType::Utf8,
                confidence: 1.0,
                samples: 100,
                valid: 100,
                nulls: 0,
                type_candidates: HashMap::new(),
            },
        );

        let schema = InferredSchemaWithConfidence {
            fields: vec![
                ("a".to_string(), RedisType::Int64),
                ("b".to_string(), RedisType::Utf8),
            ],
            sample_count: 100,
            field_info,
        };

        assert!(schema.all_confident(0.9));
        assert!(!schema.all_confident(0.99));
    }

    #[test]
    fn test_inferred_schema_with_confidence_average() {
        let mut field_info = HashMap::new();
        field_info.insert(
            "a".to_string(),
            FieldInferenceInfo {
                inferred_type: RedisType::Int64,
                confidence: 1.0,
                samples: 100,
                valid: 100,
                nulls: 0,
                type_candidates: HashMap::new(),
            },
        );
        field_info.insert(
            "b".to_string(),
            FieldInferenceInfo {
                inferred_type: RedisType::Float64,
                confidence: 0.8,
                samples: 100,
                valid: 80,
                nulls: 0,
                type_candidates: HashMap::new(),
            },
        );

        let schema = InferredSchemaWithConfidence {
            fields: vec![
                ("a".to_string(), RedisType::Int64),
                ("b".to_string(), RedisType::Float64),
            ],
            sample_count: 100,
            field_info,
        };

        assert!((schema.average_confidence() - 0.9).abs() < 0.001);
    }

    #[test]
    fn test_confidence_type_candidates() {
        let values = vec![
            Some("1".to_string()),
            Some("2".to_string()),
            Some("3.5".to_string()),
        ];
        let (_, info) = infer_type_from_values_with_confidence(&values);

        // All 3 are valid floats, only 2 are valid ints
        assert_eq!(info.type_candidates.get("float64"), Some(&3));
        assert_eq!(info.type_candidates.get("int64"), Some(&2));
        assert_eq!(info.type_candidates.get("utf8"), Some(&3));
    }
}
