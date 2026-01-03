//! Schema definitions for Redis data mapping to Arrow/Polars types.
//!
//! Redis stores everything as strings, so we need a schema to know how to
//! interpret the data when converting to Arrow arrays.

use std::collections::HashMap;

use arrow::datatypes::{DataType, Field, Schema};

use crate::error::{Error, Result};

/// Supported data types for Redis field conversion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisType {
    /// UTF-8 string (no conversion needed).
    Utf8,
    /// 64-bit signed integer.
    Int64,
    /// 64-bit floating point.
    Float64,
    /// Boolean (parsed from "true"/"false", "1"/"0", etc.).
    Boolean,
}

impl RedisType {
    /// Convert to Arrow DataType.
    pub fn to_arrow_type(&self) -> DataType {
        match self {
            RedisType::Utf8 => DataType::Utf8,
            RedisType::Int64 => DataType::Int64,
            RedisType::Float64 => DataType::Float64,
            RedisType::Boolean => DataType::Boolean,
        }
    }

    /// Parse a string value according to this type.
    pub fn parse(&self, value: &str) -> Result<TypedValue> {
        match self {
            RedisType::Utf8 => Ok(TypedValue::Utf8(value.to_string())),
            RedisType::Int64 => value.parse::<i64>().map(TypedValue::Int64).map_err(|e| {
                Error::TypeConversion(format!("Failed to parse '{}' as i64: {}", value, e))
            }),
            RedisType::Float64 => value.parse::<f64>().map(TypedValue::Float64).map_err(|e| {
                Error::TypeConversion(format!("Failed to parse '{}' as f64: {}", value, e))
            }),
            RedisType::Boolean => parse_boolean(value)
                .map(TypedValue::Boolean)
                .ok_or_else(|| {
                    Error::TypeConversion(format!("Failed to parse '{}' as boolean", value))
                }),
        }
    }
}

/// A typed value after parsing from Redis string.
#[derive(Debug, Clone, PartialEq)]
pub enum TypedValue {
    Utf8(String),
    Int64(i64),
    Float64(f64),
    Boolean(bool),
}

/// Parse a string as a boolean value.
///
/// Accepts: "true", "false", "1", "0", "yes", "no" (case-insensitive).
fn parse_boolean(s: &str) -> Option<bool> {
    match s.to_lowercase().as_str() {
        "true" | "1" | "yes" | "t" | "y" => Some(true),
        "false" | "0" | "no" | "f" | "n" => Some(false),
        _ => None,
    }
}

/// Schema for a Redis hash, mapping field names to types.
#[derive(Debug, Clone)]
pub struct HashSchema {
    /// Ordered list of field names.
    fields: Vec<String>,
    /// Map from field name to type.
    types: HashMap<String, RedisType>,
    /// Whether to include the Redis key as a column.
    include_key: bool,
    /// Name of the key column (if included).
    key_column_name: String,
}

impl HashSchema {
    /// Create a new HashSchema from a list of (field_name, type) pairs.
    pub fn new(field_types: Vec<(String, RedisType)>) -> Self {
        let fields: Vec<String> = field_types.iter().map(|(name, _)| name.clone()).collect();
        let types: HashMap<String, RedisType> = field_types.into_iter().collect();

        Self {
            fields,
            types,
            include_key: true,
            key_column_name: "_key".to_string(),
        }
    }

    /// Set whether to include the Redis key as a column.
    pub fn with_key(mut self, include: bool) -> Self {
        self.include_key = include;
        self
    }

    /// Set the name of the key column.
    pub fn with_key_column_name(mut self, name: impl Into<String>) -> Self {
        self.key_column_name = name.into();
        self
    }

    /// Get the ordered field names.
    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    /// Get the type for a field.
    pub fn field_type(&self, name: &str) -> Option<RedisType> {
        self.types.get(name).copied()
    }

    /// Whether the key column is included.
    pub fn include_key(&self) -> bool {
        self.include_key
    }

    /// Get the key column name.
    pub fn key_column_name(&self) -> &str {
        &self.key_column_name
    }

    /// Convert to Arrow Schema.
    pub fn to_arrow_schema(&self) -> Schema {
        let mut arrow_fields: Vec<Field> = Vec::with_capacity(self.fields.len() + 1);

        // Add key column first if included
        if self.include_key {
            arrow_fields.push(Field::new(&self.key_column_name, DataType::Utf8, false));
        }

        // Add data fields
        for field_name in &self.fields {
            if let Some(redis_type) = self.types.get(field_name) {
                // Fields are nullable since Redis might not have all fields
                arrow_fields.push(Field::new(field_name, redis_type.to_arrow_type(), true));
            }
        }

        Schema::new(arrow_fields)
    }

    /// Get a subset schema with only the specified columns (for projection pushdown).
    pub fn project(&self, columns: &[String]) -> Self {
        let projected_fields: Vec<String> = columns
            .iter()
            .filter(|c| {
                // Include if it's a data field (not the key column)
                self.types.contains_key(*c)
            })
            .cloned()
            .collect();

        let projected_types: HashMap<String, RedisType> = projected_fields
            .iter()
            .filter_map(|f| self.types.get(f).map(|t| (f.clone(), *t)))
            .collect();

        // Check if key column is requested
        let include_key = self.include_key && columns.contains(&self.key_column_name);

        Self {
            fields: projected_fields,
            types: projected_types,
            include_key,
            key_column_name: self.key_column_name.clone(),
        }
    }
}

impl Default for HashSchema {
    fn default() -> Self {
        Self {
            fields: Vec::new(),
            types: HashMap::new(),
            include_key: true,
            key_column_name: "_key".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_type_to_arrow() {
        assert_eq!(RedisType::Utf8.to_arrow_type(), DataType::Utf8);
        assert_eq!(RedisType::Int64.to_arrow_type(), DataType::Int64);
        assert_eq!(RedisType::Float64.to_arrow_type(), DataType::Float64);
        assert_eq!(RedisType::Boolean.to_arrow_type(), DataType::Boolean);
    }

    #[test]
    fn test_parse_int64() {
        assert_eq!(RedisType::Int64.parse("42").unwrap(), TypedValue::Int64(42));
        assert_eq!(
            RedisType::Int64.parse("-100").unwrap(),
            TypedValue::Int64(-100)
        );
        assert!(RedisType::Int64.parse("not_a_number").is_err());
    }

    #[test]
    fn test_parse_float64() {
        assert_eq!(
            RedisType::Float64.parse("3.5").unwrap(),
            TypedValue::Float64(3.5)
        );
        assert_eq!(
            RedisType::Float64.parse("-0.5").unwrap(),
            TypedValue::Float64(-0.5)
        );
        assert!(RedisType::Float64.parse("not_a_float").is_err());
    }

    #[test]
    fn test_parse_boolean() {
        assert_eq!(
            RedisType::Boolean.parse("true").unwrap(),
            TypedValue::Boolean(true)
        );
        assert_eq!(
            RedisType::Boolean.parse("FALSE").unwrap(),
            TypedValue::Boolean(false)
        );
        assert_eq!(
            RedisType::Boolean.parse("1").unwrap(),
            TypedValue::Boolean(true)
        );
        assert_eq!(
            RedisType::Boolean.parse("0").unwrap(),
            TypedValue::Boolean(false)
        );
        assert_eq!(
            RedisType::Boolean.parse("yes").unwrap(),
            TypedValue::Boolean(true)
        );
        assert_eq!(
            RedisType::Boolean.parse("no").unwrap(),
            TypedValue::Boolean(false)
        );
        assert!(RedisType::Boolean.parse("maybe").is_err());
    }

    #[test]
    fn test_hash_schema_creation() {
        let schema = HashSchema::new(vec![
            ("name".to_string(), RedisType::Utf8),
            ("age".to_string(), RedisType::Int64),
        ]);

        assert_eq!(schema.fields(), &["name", "age"]);
        assert_eq!(schema.field_type("name"), Some(RedisType::Utf8));
        assert_eq!(schema.field_type("age"), Some(RedisType::Int64));
        assert_eq!(schema.field_type("missing"), None);
    }

    #[test]
    fn test_hash_schema_to_arrow() {
        let schema = HashSchema::new(vec![
            ("name".to_string(), RedisType::Utf8),
            ("age".to_string(), RedisType::Int64),
            ("active".to_string(), RedisType::Boolean),
        ]);

        let arrow_schema = schema.to_arrow_schema();
        assert_eq!(arrow_schema.fields().len(), 4); // _key + 3 fields

        assert_eq!(arrow_schema.field(0).name(), "_key");
        assert_eq!(arrow_schema.field(0).data_type(), &DataType::Utf8);

        assert_eq!(arrow_schema.field(1).name(), "name");
        assert_eq!(arrow_schema.field(2).name(), "age");
        assert_eq!(arrow_schema.field(3).name(), "active");
    }

    #[test]
    fn test_hash_schema_without_key() {
        let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(false);

        let arrow_schema = schema.to_arrow_schema();
        assert_eq!(arrow_schema.fields().len(), 1);
        assert_eq!(arrow_schema.field(0).name(), "name");
    }

    #[test]
    fn test_hash_schema_projection() {
        let schema = HashSchema::new(vec![
            ("name".to_string(), RedisType::Utf8),
            ("age".to_string(), RedisType::Int64),
            ("email".to_string(), RedisType::Utf8),
        ]);

        // Project to only name and email
        let projected = schema.project(&["name".to_string(), "email".to_string()]);
        assert_eq!(projected.fields(), &["name", "email"]);
        assert!(!projected.include_key()); // Key not in projection

        // Project with key
        let projected_with_key = schema.project(&["_key".to_string(), "name".to_string()]);
        assert_eq!(projected_with_key.fields(), &["name"]);
        assert!(projected_with_key.include_key());
    }
}
