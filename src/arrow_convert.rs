//! Conversion from Redis data to Arrow arrays.
//!
//! This module handles the conversion of Redis hash data into Arrow RecordBatches
//! that can be consumed by Polars.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, RecordBatch,
    StringBuilder, TimestampMicrosecondBuilder, UInt64Builder,
};

use crate::error::{Error, Result};
use crate::hash_reader::HashData;
use crate::schema::{HashSchema, RedisType};

/// Convert a batch of Redis hash data to an Arrow RecordBatch.
///
/// This function takes hash data from Redis and converts it to a typed Arrow
/// RecordBatch according to the provided schema.
///
/// # Arguments
/// * `data` - The hash data from Redis
/// * `schema` - The schema defining the column types
/// * `row_offset` - Starting row index for this batch (used when include_row_index is true)
pub fn hashes_to_record_batch(
    data: &[HashData],
    schema: &HashSchema,
    row_offset: u64,
) -> Result<RecordBatch> {
    let arrow_schema = Arc::new(schema.to_arrow_schema());
    let num_rows = data.len();

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(arrow_schema.fields().len());

    // Build row index column if included (first column)
    if schema.include_row_index() {
        let mut builder = UInt64Builder::with_capacity(num_rows);
        for i in 0..num_rows {
            builder.append_value(row_offset + i as u64);
        }
        arrays.push(Arc::new(builder.finish()));
    }

    // Build key column if included
    if schema.include_key() {
        let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
        for row in data {
            builder.append_value(&row.key);
        }
        arrays.push(Arc::new(builder.finish()));
    }

    // Build TTL column if included
    if schema.include_ttl() {
        let mut builder = Int64Builder::with_capacity(num_rows);
        for row in data {
            match row.ttl {
                Some(ttl) => builder.append_value(ttl),
                None => builder.append_null(),
            }
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
        RedisType::Date => build_date_column(data, field_name),
        RedisType::Datetime => build_datetime_column(data, field_name),
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
                let parsed = value.parse::<i64>().map_err(|_| {
                    Error::TypeConversion(format!(
                        "Cannot parse '{}' as Int64 for field '{}'. Expected a valid integer (e.g., '42', '-100')",
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

/// Build a Float64 column, parsing string values.
fn build_float64_column(data: &[HashData], field_name: &str) -> Result<ArrayRef> {
    let mut builder = Float64Builder::with_capacity(data.len());

    for row in data {
        match row.fields.get(field_name) {
            Some(Some(value)) => {
                let parsed = value.parse::<f64>().map_err(|_| {
                    Error::TypeConversion(format!(
                        "Cannot parse '{}' as Float64 for field '{}'. Expected a valid number (e.g., '3.14', '-0.5')",
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

/// Build a Boolean column, parsing string values.
fn build_boolean_column(data: &[HashData], field_name: &str) -> Result<ArrayRef> {
    let mut builder = BooleanBuilder::with_capacity(data.len());

    for row in data {
        match row.fields.get(field_name) {
            Some(Some(value)) => {
                let parsed = parse_bool(value).ok_or_else(|| {
                    Error::TypeConversion(format!(
                        "Cannot parse '{}' as Boolean for field '{}'. Expected: true/false, yes/no, 1/0, t/f, y/n",
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

/// Build a Date32 column (days since Unix epoch), parsing string values.
fn build_date_column(data: &[HashData], field_name: &str) -> Result<ArrayRef> {
    use crate::schema::parse_date;

    let mut builder = Date32Builder::with_capacity(data.len());

    for row in data {
        match row.fields.get(field_name) {
            Some(Some(value)) => {
                let parsed = parse_date(value).ok_or_else(|| {
                    Error::TypeConversion(format!(
                        "Cannot parse '{}' as Date for field '{}'. Expected: YYYY-MM-DD (e.g., '2024-01-15') or epoch days",
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

/// Build a Timestamp column (microseconds since Unix epoch), parsing string values.
fn build_datetime_column(data: &[HashData], field_name: &str) -> Result<ArrayRef> {
    use crate::schema::parse_datetime;

    let mut builder = TimestampMicrosecondBuilder::with_capacity(data.len());

    for row in data {
        match row.fields.get(field_name) {
            Some(Some(value)) => {
                let parsed = parse_datetime(value).ok_or_else(|| {
                    Error::TypeConversion(format!(
                        "Cannot parse '{}' as Datetime for field '{}'. Expected: ISO 8601 (e.g., '2024-01-15T10:30:00') or Unix timestamp",
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
            ttl: None,
        }
    }

    fn make_hash_data_with_ttl(
        key: &str,
        fields: Vec<(&str, Option<&str>)>,
        ttl: Option<i64>,
    ) -> HashData {
        HashData {
            key: key.to_string(),
            fields: fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.map(|s| s.to_string())))
                .collect(),
            ttl,
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

        let batch = hashes_to_record_batch(&data, &schema, 0).unwrap();

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

        let batch = hashes_to_record_batch(&data, &schema, 0).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_hashes_to_record_batch_no_key() {
        let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_key(false);

        let data = vec![make_hash_data("user:1", vec![("name", Some("Alice"))])];

        let batch = hashes_to_record_batch(&data, &schema, 0).unwrap();

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

        let batch = hashes_to_record_batch(&data, &schema, 0).unwrap();
        assert_eq!(batch.num_columns(), 5); // _key + 4 fields
    }

    #[test]
    fn test_hashes_to_record_batch_parse_error() {
        let schema = HashSchema::new(vec![("age".to_string(), RedisType::Int64)]);

        let data = vec![make_hash_data(
            "user:1",
            vec![("age", Some("not_a_number"))],
        )];

        let result = hashes_to_record_batch(&data, &schema, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_data() {
        let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]);

        let data: Vec<HashData> = vec![];
        let batch = hashes_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_hashes_to_record_batch_with_ttl() {
        let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_ttl(true);

        let data = vec![
            make_hash_data_with_ttl("user:1", vec![("name", Some("Alice"))], Some(3600)),
            make_hash_data_with_ttl("user:2", vec![("name", Some("Bob"))], Some(-1)), // No expiry
            make_hash_data_with_ttl("user:3", vec![("name", Some("Charlie"))], Some(7200)),
        ];

        let batch = hashes_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3); // _key, _ttl, name
        assert_eq!(batch.schema().field(1).name(), "_ttl");
    }

    #[test]
    fn test_hashes_to_record_batch_with_ttl_custom_name() {
        let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)])
            .with_ttl(true)
            .with_ttl_column_name("expires_in");

        let data = vec![make_hash_data_with_ttl(
            "user:1",
            vec![("name", Some("Alice"))],
            Some(3600),
        )];

        let batch = hashes_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.num_columns(), 3); // _key, expires_in, name
        assert_eq!(batch.schema().field(1).name(), "expires_in");
    }

    #[test]
    fn test_hashes_to_record_batch_with_date() {
        let schema = HashSchema::new(vec![
            ("name".to_string(), RedisType::Utf8),
            ("birth_date".to_string(), RedisType::Date),
        ]);

        let data = vec![
            make_hash_data(
                "user:1",
                vec![("name", Some("Alice")), ("birth_date", Some("2024-01-15"))],
            ),
            make_hash_data(
                "user:2",
                vec![("name", Some("Bob")), ("birth_date", Some("19737"))],
            ),
        ];

        let batch = hashes_to_record_batch(&data, &schema, 0).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3); // _key, name, birth_date
    }

    #[test]
    fn test_hashes_to_record_batch_with_datetime() {
        let schema = HashSchema::new(vec![
            ("name".to_string(), RedisType::Utf8),
            ("created_at".to_string(), RedisType::Datetime),
        ]);

        let data = vec![
            make_hash_data(
                "user:1",
                vec![
                    ("name", Some("Alice")),
                    ("created_at", Some("2024-01-15T10:30:00")),
                ],
            ),
            make_hash_data(
                "user:2",
                vec![
                    ("name", Some("Bob")),
                    ("created_at", Some("1705315800")), // Unix timestamp in seconds
                ],
            ),
        ];

        let batch = hashes_to_record_batch(&data, &schema, 0).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3); // _key, name, created_at
    }

    #[test]
    fn test_hashes_to_record_batch_date_with_nulls() {
        let schema = HashSchema::new(vec![
            ("name".to_string(), RedisType::Utf8),
            ("birth_date".to_string(), RedisType::Date),
        ]);

        let data = vec![
            make_hash_data(
                "user:1",
                vec![("name", Some("Alice")), ("birth_date", Some("2024-01-15"))],
            ),
            make_hash_data("user:2", vec![("name", Some("Bob")), ("birth_date", None)]),
        ];

        let batch = hashes_to_record_batch(&data, &schema, 0).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_hashes_to_record_batch_all_types_including_temporal() {
        let schema = HashSchema::new(vec![
            ("name".to_string(), RedisType::Utf8),
            ("age".to_string(), RedisType::Int64),
            ("score".to_string(), RedisType::Float64),
            ("active".to_string(), RedisType::Boolean),
            ("birth_date".to_string(), RedisType::Date),
            ("created_at".to_string(), RedisType::Datetime),
        ]);

        let data = vec![make_hash_data(
            "user:1",
            vec![
                ("name", Some("Alice")),
                ("age", Some("30")),
                ("score", Some("95.5")),
                ("active", Some("true")),
                ("birth_date", Some("1990-05-15")),
                ("created_at", Some("2024-01-15T10:30:00Z")),
            ],
        )];

        let batch = hashes_to_record_batch(&data, &schema, 0).unwrap();
        assert_eq!(batch.num_columns(), 7); // _key + 6 fields
    }

    #[test]
    fn test_hashes_to_record_batch_with_row_index() {
        let schema =
            HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_row_index(true);

        let data = vec![
            make_hash_data("user:1", vec![("name", Some("Alice"))]),
            make_hash_data("user:2", vec![("name", Some("Bob"))]),
            make_hash_data("user:3", vec![("name", Some("Charlie"))]),
        ];

        let batch = hashes_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3); // _index, _key, name
        assert_eq!(batch.schema().field(0).name(), "_index");

        // Verify row indices
        let index_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(index_col.value(0), 0);
        assert_eq!(index_col.value(1), 1);
        assert_eq!(index_col.value(2), 2);
    }

    #[test]
    fn test_hashes_to_record_batch_with_row_index_offset() {
        let schema =
            HashSchema::new(vec![("name".to_string(), RedisType::Utf8)]).with_row_index(true);

        let data = vec![
            make_hash_data("user:1", vec![("name", Some("Alice"))]),
            make_hash_data("user:2", vec![("name", Some("Bob"))]),
        ];

        // Simulate second batch starting at offset 100
        let batch = hashes_to_record_batch(&data, &schema, 100).unwrap();

        let index_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(index_col.value(0), 100);
        assert_eq!(index_col.value(1), 101);
    }

    #[test]
    fn test_hashes_to_record_batch_with_row_index_custom_name() {
        let schema = HashSchema::new(vec![("name".to_string(), RedisType::Utf8)])
            .with_row_index(true)
            .with_row_index_column_name("row_num");

        let data = vec![make_hash_data("user:1", vec![("name", Some("Alice"))])];

        let batch = hashes_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.schema().field(0).name(), "row_num");
    }
}
