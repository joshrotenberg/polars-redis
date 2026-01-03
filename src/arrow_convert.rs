//! Conversion from Redis data to Arrow arrays.
//!
//! This module handles the conversion of Redis hash data into Arrow RecordBatches
//! that can be consumed by Polars.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
};

use crate::error::{Error, Result};
use crate::hash_reader::HashData;
use crate::schema::{HashSchema, RedisType};

/// Convert a batch of Redis hash data to an Arrow RecordBatch.
///
/// This function takes hash data from Redis and converts it to a typed Arrow
/// RecordBatch according to the provided schema.
pub fn hashes_to_record_batch(data: &[HashData], schema: &HashSchema) -> Result<RecordBatch> {
    let arrow_schema = Arc::new(schema.to_arrow_schema());
    let num_rows = data.len();

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(arrow_schema.fields().len());

    // Build key column if included
    if schema.include_key() {
        let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
        for row in data {
            builder.append_value(&row.key);
        }
        arrays.push(Arc::new(builder.finish()));
    }

    // Build data columns
    for field_name in schema.fields() {
        let redis_type = schema
            .field_type(field_name)
            .ok_or_else(|| Error::SchemaMismatch(format!("Unknown field: {}", field_name)))?;

        let array = build_column(data, field_name, redis_type)?;
        arrays.push(array);
    }

    RecordBatch::try_new(arrow_schema, arrays)
        .map_err(|e| Error::TypeConversion(format!("Failed to create RecordBatch: {}", e)))
}

/// Build an Arrow array for a single column from hash data.
fn build_column(data: &[HashData], field_name: &str, redis_type: RedisType) -> Result<ArrayRef> {
    match redis_type {
        RedisType::Utf8 => build_utf8_column(data, field_name),
        RedisType::Int64 => build_int64_column(data, field_name),
        RedisType::Float64 => build_float64_column(data, field_name),
        RedisType::Boolean => build_boolean_column(data, field_name),
    }
}

/// Build a UTF-8 string column.
fn build_utf8_column(data: &[HashData], field_name: &str) -> Result<ArrayRef> {
    let mut builder = StringBuilder::with_capacity(data.len(), data.len() * 32);

    for row in data {
        match row.fields.get(field_name) {
            Some(Some(value)) => builder.append_value(value),
            Some(None) | None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Build an Int64 column, parsing string values.
fn build_int64_column(data: &[HashData], field_name: &str) -> Result<ArrayRef> {
    let mut builder = Int64Builder::with_capacity(data.len());

    for row in data {
        match row.fields.get(field_name) {
            Some(Some(value)) => {
                let parsed = value.parse::<i64>().map_err(|e| {
                    Error::TypeConversion(format!(
                        "Failed to parse '{}' as i64 for field '{}': {}",
                        value, field_name, e
                    ))
                })?;
                builder.append_value(parsed);
            }
            Some(None) | None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Build a Float64 column, parsing string values.
fn build_float64_column(data: &[HashData], field_name: &str) -> Result<ArrayRef> {
    let mut builder = Float64Builder::with_capacity(data.len());

    for row in data {
        match row.fields.get(field_name) {
            Some(Some(value)) => {
                let parsed = value.parse::<f64>().map_err(|e| {
                    Error::TypeConversion(format!(
                        "Failed to parse '{}' as f64 for field '{}': {}",
                        value, field_name, e
                    ))
                })?;
                builder.append_value(parsed);
            }
            Some(None) | None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Build a Boolean column, parsing string values.
fn build_boolean_column(data: &[HashData], field_name: &str) -> Result<ArrayRef> {
    let mut builder = BooleanBuilder::with_capacity(data.len());

    for row in data {
        match row.fields.get(field_name) {
            Some(Some(value)) => {
                let parsed = parse_bool(value).ok_or_else(|| {
                    Error::TypeConversion(format!(
                        "Failed to parse '{}' as boolean for field '{}'",
                        value, field_name
                    ))
                })?;
                builder.append_value(parsed);
            }
            Some(None) | None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Parse a string as a boolean.
fn parse_bool(s: &str) -> Option<bool> {
    // Strip surrounding quotes if present (Redis CLI sometimes adds them)
    let s = s.trim_matches('"').trim_matches('\'');
    match s.to_lowercase().as_str() {
        "true" | "1" | "yes" | "t" | "y" => Some(true),
        "false" | "0" | "no" | "f" | "n" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_hash_data(key: &str, fields: Vec<(&str, Option<&str>)>) -> HashData {
        HashData {
            key: key.to_string(),
            fields: fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.map(|s| s.to_string())))
                .collect(),
        }
    }

    #[test]
    fn test_hashes_to_record_batch_basic() {
        let schema = HashSchema::new(vec![
            ("name".to_string(), RedisType::Utf8),
            ("age".to_string(), RedisType::Int64),
        ]);

        let data = vec![
            make_hash_data("user:1", vec![("name", Some("Alice")), ("age", Some("30"))]),
            make_hash_data("user:2", vec![("name", Some("Bob")), ("age", Some("25"))]),
        ];

        let batch = hashes_to_record_batch(&data, &schema).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3); // _key, name, age
    }

    #[test]
    fn test_hashes_to_record_batch_with_nulls() {
        let schema = HashSchema::new(vec![
            ("name".to_string(), RedisType::Utf8),
            ("age".to_string(), RedisType::Int64),
        ]);

        let data = vec![
            make_hash_data("user:1", vec![("name", Some("Alice")), ("age", None)]),
            make_hash_data("user:2", vec![("name", None), ("age", Some("25"))]),
        ];

        let batch = hashes_to_record_batch(&data, &schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_hashes_to_record_batch_no_key() {
        let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(false);

        let data = vec![make_hash_data("user:1", vec![("name", Some("Alice"))])];

        let batch = hashes_to_record_batch(&data, &schema).unwrap();

        assert_eq!(batch.num_columns(), 1); // Just name, no _key
        assert_eq!(batch.schema().field(0).name(), "name");
    }

    #[test]
    fn test_hashes_to_record_batch_all_types() {
        let schema = HashSchema::new(vec![
            ("name".to_string(), RedisType::Utf8),
            ("age".to_string(), RedisType::Int64),
            ("score".to_string(), RedisType::Float64),
            ("active".to_string(), RedisType::Boolean),
        ]);

        let data = vec![make_hash_data(
            "user:1",
            vec![
                ("name", Some("Alice")),
                ("age", Some("30")),
                ("score", Some("95.5")),
                ("active", Some("true")),
            ],
        )];

        let batch = hashes_to_record_batch(&data, &schema).unwrap();
        assert_eq!(batch.num_columns(), 5); // _key + 4 fields
    }

    #[test]
    fn test_hashes_to_record_batch_parse_error() {
        let schema = HashSchema::new(vec![("age".to_string(), RedisType::Int64)]);

        let data = vec![make_hash_data(
            "user:1",
            vec![("age", Some("not_a_number"))],
        )];

        let result = hashes_to_record_batch(&data, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_data() {
        let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]);

        let data: Vec<HashData> = vec![];
        let batch = hashes_to_record_batch(&data, &schema).unwrap();

        assert_eq!(batch.num_rows(), 0);
    }
}
