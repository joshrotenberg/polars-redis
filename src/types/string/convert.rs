//! Conversion from Redis string data to Arrow arrays.
//!
//! This module handles the conversion of Redis string values into Arrow RecordBatches
//! that can be consumed by Polars.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, RecordBatch,
    StringBuilder, TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use super::reader::StringData;
use crate::error::{Error, Result};

/// Schema for Redis string values.
#[derive(Debug, Clone)]
pub struct StringSchema {
    /// The Arrow DataType for values.
    value_type: DataType,
    /// Whether to include the Redis key as a column.
    include_key: bool,
    /// Name of the key column.
    key_column_name: String,
    /// Name of the value column.
    value_column_name: String,
}

impl StringSchema {
    /// Create a new StringSchema with the given value type.
    pub fn new(value_type: DataType) -> Self {
        Self {
            value_type,
            include_key: true,
            key_column_name: "_key".to_string(),
            value_column_name: "value".to_string(),
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

    /// Set the name of the value column.
    pub fn with_value_column_name(mut self, name: impl Into<String>) -> Self {
        self.value_column_name = name.into();
        self
    }

    /// Get the value type.
    pub fn value_type(&self) -> &DataType {
        &self.value_type
    }

    /// Whether the key column is included.
    pub fn include_key(&self) -> bool {
        self.include_key
    }

    /// Get the key column name.
    pub fn key_column_name(&self) -> &str {
        &self.key_column_name
    }

    /// Get the value column name.
    pub fn value_column_name(&self) -> &str {
        &self.value_column_name
    }

    /// Convert to Arrow Schema.
    pub fn to_arrow_schema(&self) -> Schema {
        let mut fields = Vec::with_capacity(2);

        if self.include_key {
            fields.push(Field::new(&self.key_column_name, DataType::Utf8, false));
        }

        // Value is nullable (key might not exist)
        fields.push(Field::new(
            &self.value_column_name,
            self.value_type.clone(),
            true,
        ));

        Schema::new(fields)
    }
}

impl Default for StringSchema {
    fn default() -> Self {
        Self::new(DataType::Utf8)
    }
}

/// Convert a batch of Redis string data to an Arrow RecordBatch.
pub fn strings_to_record_batch(data: &[StringData], schema: &StringSchema) -> Result<RecordBatch> {
    let arrow_schema = Arc::new(schema.to_arrow_schema());
    let num_rows = data.len();

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(2);

    // Build key column if included
    if schema.include_key() {
        let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
        for row in data {
            builder.append_value(&row.key);
        }
        arrays.push(Arc::new(builder.finish()));
    }

    // Build value column based on type
    let value_array = build_value_column(data, schema.value_type())?;
    arrays.push(value_array);

    RecordBatch::try_new(arrow_schema, arrays)
        .map_err(|e| Error::TypeConversion(format!("Failed to create RecordBatch: {}", e)))
}

/// Build the value column based on the schema's value type.
fn build_value_column(data: &[StringData], value_type: &DataType) -> Result<ArrayRef> {
    match value_type {
        DataType::Utf8 => build_utf8_column(data),
        DataType::Int64 => build_int64_column(data),
        DataType::Float64 => build_float64_column(data),
        DataType::Boolean => build_boolean_column(data),
        DataType::Date32 => build_date_column(data),
        DataType::Timestamp(TimeUnit::Microsecond, None) => build_datetime_column(data),
        _ => Err(Error::TypeConversion(format!(
            "Unsupported value type: {:?}",
            value_type
        ))),
    }
}

/// Build a UTF-8 string column.
fn build_utf8_column(data: &[StringData]) -> Result<ArrayRef> {
    let mut builder = StringBuilder::with_capacity(data.len(), data.len() * 32);

    for row in data {
        match &row.value {
            Some(value) => builder.append_value(value),
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Build an Int64 column, parsing string values.
fn build_int64_column(data: &[StringData]) -> Result<ArrayRef> {
    let mut builder = Int64Builder::with_capacity(data.len());

    for row in data {
        match &row.value {
            Some(value) => {
                let parsed = value.parse::<i64>().map_err(|e| {
                    Error::TypeConversion(format!(
                        "Failed to parse '{}' as i64 for key '{}': {}",
                        value, row.key, e
                    ))
                })?;
                builder.append_value(parsed);
            }
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Build a Float64 column, parsing string values.
fn build_float64_column(data: &[StringData]) -> Result<ArrayRef> {
    let mut builder = Float64Builder::with_capacity(data.len());

    for row in data {
        match &row.value {
            Some(value) => {
                let parsed = value.parse::<f64>().map_err(|e| {
                    Error::TypeConversion(format!(
                        "Failed to parse '{}' as f64 for key '{}': {}",
                        value, row.key, e
                    ))
                })?;
                builder.append_value(parsed);
            }
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Build a Boolean column, parsing string values.
fn build_boolean_column(data: &[StringData]) -> Result<ArrayRef> {
    let mut builder = BooleanBuilder::with_capacity(data.len());

    for row in data {
        match &row.value {
            Some(value) => {
                let parsed = parse_bool(value).ok_or_else(|| {
                    Error::TypeConversion(format!(
                        "Failed to parse '{}' as boolean for key '{}'",
                        value, row.key
                    ))
                })?;
                builder.append_value(parsed);
            }
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Build a Date32 column, parsing string values.
fn build_date_column(data: &[StringData]) -> Result<ArrayRef> {
    let mut builder = Date32Builder::with_capacity(data.len());

    for row in data {
        match &row.value {
            Some(value) => {
                let parsed = parse_date(value).ok_or_else(|| {
                    Error::TypeConversion(format!(
                        "Failed to parse '{}' as date for key '{}'",
                        value, row.key
                    ))
                })?;
                builder.append_value(parsed);
            }
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Build a Timestamp column, parsing string values.
fn build_datetime_column(data: &[StringData]) -> Result<ArrayRef> {
    let mut builder = TimestampMicrosecondBuilder::with_capacity(data.len());

    for row in data {
        match &row.value {
            Some(value) => {
                let parsed = parse_datetime(value).ok_or_else(|| {
                    Error::TypeConversion(format!(
                        "Failed to parse '{}' as datetime for key '{}'",
                        value, row.key
                    ))
                })?;
                builder.append_value(parsed);
            }
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Parse a string as a boolean.
fn parse_bool(s: &str) -> Option<bool> {
    let s = s.trim_matches('"').trim_matches('\'');
    match s.to_lowercase().as_str() {
        "true" | "1" | "yes" | "t" | "y" => Some(true),
        "false" | "0" | "no" | "f" | "n" => Some(false),
        _ => None,
    }
}

/// Parse a string as a date (days since Unix epoch).
///
/// Accepts:
/// - ISO 8601 date: "2024-01-15"
/// - Epoch days as integer: "19738"
fn parse_date(s: &str) -> Option<i32> {
    // Try parsing as epoch days first (integer)
    if let Ok(days) = s.parse::<i32>() {
        return Some(days);
    }

    // Try parsing as YYYY-MM-DD
    if s.len() >= 10 {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() >= 3
            && let (Ok(year), Ok(month), Ok(day)) = (
                parts[0].parse::<i32>(),
                parts[1].parse::<u32>(),
                parts[2].chars().take(2).collect::<String>().parse::<u32>(),
            )
        {
            return days_since_epoch(year, month, day);
        }
    }

    None
}

/// Calculate days since Unix epoch for a given date.
fn days_since_epoch(year: i32, month: u32, day: u32) -> Option<i32> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    let days_in_month = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let is_leap = |y: i32| (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);

    let mut total_days: i32 = 0;

    if year >= 1970 {
        for y in 1970..year {
            total_days += if is_leap(y) { 366 } else { 365 };
        }
    } else {
        for y in year..1970 {
            total_days -= if is_leap(y) { 366 } else { 365 };
        }
    }

    for m in 1..month {
        total_days += days_in_month[m as usize];
        if m == 2 && is_leap(year) {
            total_days += 1;
        }
    }

    total_days += day as i32 - 1;

    Some(total_days)
}

/// Parse a string as a datetime (microseconds since Unix epoch).
///
/// Accepts:
/// - ISO 8601 datetime: "2024-01-15T10:30:00"
/// - Unix timestamp (seconds): "1705315800"
/// - Unix timestamp (milliseconds): "1705315800000"
/// - Unix timestamp (microseconds): "1705315800000000"
fn parse_datetime(s: &str) -> Option<i64> {
    let s = s.trim();

    // Try parsing as numeric timestamp
    if let Ok(ts) = s.parse::<i64>() {
        if ts < 10_000_000_000 {
            return Some(ts * 1_000_000);
        } else if ts < 10_000_000_000_000 {
            return Some(ts * 1_000);
        } else {
            return Some(ts);
        }
    }

    // Try parsing ISO 8601 datetime
    parse_iso8601_datetime(s)
}

/// Parse ISO 8601 datetime string to microseconds since epoch.
fn parse_iso8601_datetime(s: &str) -> Option<i64> {
    let s = s.trim_end_matches('Z');

    let parts: Vec<&str> = s.split('T').collect();
    if parts.len() != 2 {
        let parts: Vec<&str> = s.split(' ').collect();
        if parts.len() != 2 {
            return None;
        }
        return parse_datetime_parts(parts[0], parts[1]);
    }

    parse_datetime_parts(parts[0], parts[1])
}

/// Parse date and time parts into microseconds since epoch.
fn parse_datetime_parts(date_str: &str, time_str: &str) -> Option<i64> {
    let date_parts: Vec<&str> = date_str.split('-').collect();
    if date_parts.len() != 3 {
        return None;
    }

    let year = date_parts[0].parse::<i32>().ok()?;
    let month = date_parts[1].parse::<u32>().ok()?;
    let day = date_parts[2].parse::<u32>().ok()?;

    let time_str = time_str.split('+').next()?.split('-').next()?;
    let time_parts: Vec<&str> = time_str.split(':').collect();
    if time_parts.len() < 2 {
        return None;
    }

    let hour = time_parts[0].parse::<u32>().ok()?;
    let minute = time_parts[1].parse::<u32>().ok()?;

    let (second, microsecond) = if time_parts.len() >= 3 {
        let sec_parts: Vec<&str> = time_parts[2].split('.').collect();
        let sec = sec_parts[0].parse::<u32>().ok()?;
        let usec = if sec_parts.len() > 1 {
            let frac = sec_parts[1];
            let padded = format!("{:0<6}", frac);
            padded[..6].parse::<u32>().unwrap_or(0)
        } else {
            0
        };
        (sec, usec)
    } else {
        (0, 0)
    };

    let days = days_since_epoch(year, month, day)?;
    let day_us = days as i64 * 24 * 60 * 60 * 1_000_000;
    let time_us =
        (hour as i64 * 3600 + minute as i64 * 60 + second as i64) * 1_000_000 + microsecond as i64;

    Some(day_us + time_us)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_string_data(key: &str, value: Option<&str>) -> StringData {
        StringData {
            key: key.to_string(),
            value: value.map(|s| s.to_string()),
        }
    }

    #[test]
    fn test_string_schema_default() {
        let schema = StringSchema::default();
        assert!(schema.include_key());
        assert_eq!(schema.key_column_name(), "_key");
        assert_eq!(schema.value_column_name(), "value");
        assert_eq!(schema.value_type(), &DataType::Utf8);
    }

    #[test]
    fn test_string_schema_builder() {
        let schema = StringSchema::new(DataType::Int64)
            .with_key(false)
            .with_value_column_name("count");

        assert!(!schema.include_key());
        assert_eq!(schema.value_column_name(), "count");
        assert_eq!(schema.value_type(), &DataType::Int64);
    }

    #[test]
    fn test_strings_to_record_batch_utf8() {
        let schema = StringSchema::new(DataType::Utf8);
        let data = vec![
            make_string_data("key:1", Some("hello")),
            make_string_data("key:2", Some("world")),
            make_string_data("key:3", None),
        ];

        let batch = strings_to_record_batch(&data, &schema).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2); // _key, value
    }

    #[test]
    fn test_strings_to_record_batch_int64() {
        let schema = StringSchema::new(DataType::Int64);
        let data = vec![
            make_string_data("counter:1", Some("100")),
            make_string_data("counter:2", Some("-50")),
            make_string_data("counter:3", None),
        ];

        let batch = strings_to_record_batch(&data, &schema).unwrap();
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn test_strings_to_record_batch_float64() {
        let schema = StringSchema::new(DataType::Float64);
        let data = vec![
            make_string_data("price:1", Some("19.99")),
            make_string_data("price:2", Some("0.5")),
        ];

        let batch = strings_to_record_batch(&data, &schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_strings_to_record_batch_boolean() {
        let schema = StringSchema::new(DataType::Boolean);
        let data = vec![
            make_string_data("flag:1", Some("true")),
            make_string_data("flag:2", Some("false")),
            make_string_data("flag:3", Some("1")),
            make_string_data("flag:4", Some("0")),
        ];

        let batch = strings_to_record_batch(&data, &schema).unwrap();
        assert_eq!(batch.num_rows(), 4);
    }

    #[test]
    fn test_strings_to_record_batch_no_key() {
        let schema = StringSchema::new(DataType::Utf8).with_key(false);
        let data = vec![make_string_data("key:1", Some("hello"))];

        let batch = strings_to_record_batch(&data, &schema).unwrap();
        assert_eq!(batch.num_columns(), 1); // Just value
        assert_eq!(batch.schema().field(0).name(), "value");
    }

    #[test]
    fn test_strings_to_record_batch_parse_error() {
        let schema = StringSchema::new(DataType::Int64);
        let data = vec![make_string_data("key:1", Some("not_a_number"))];

        let result = strings_to_record_batch(&data, &schema);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_data() {
        let schema = StringSchema::new(DataType::Utf8);
        let data: Vec<StringData> = vec![];

        let batch = strings_to_record_batch(&data, &schema).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_strings_to_record_batch_date() {
        let schema = StringSchema::new(DataType::Date32);
        let data = vec![
            make_string_data("date:1", Some("2024-01-15")),
            make_string_data("date:2", Some("19737")),
        ];

        let batch = strings_to_record_batch(&data, &schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_strings_to_record_batch_datetime() {
        let schema = StringSchema::new(DataType::Timestamp(TimeUnit::Microsecond, None));
        let data = vec![
            make_string_data("ts:1", Some("2024-01-15T10:30:00")),
            make_string_data("ts:2", Some("1705315800")),
        ];

        let batch = strings_to_record_batch(&data, &schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }
}
