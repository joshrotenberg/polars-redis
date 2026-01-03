//! Schema definitions for Redis data mapping to Arrow/Polars types.
//!
//! Redis stores everything as strings, so we need a schema to know how to
//! interpret the data when converting to Arrow arrays.

use std::collections::HashMap;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

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
    /// Date (days since epoch, parsed from "YYYY-MM-DD" or epoch days).
    Date,
    /// Datetime with microsecond precision (parsed from ISO 8601 or Unix timestamp).
    Datetime,
}

impl RedisType {
    /// Convert to Arrow DataType.
    pub fn to_arrow_type(&self) -> DataType {
        match self {
            RedisType::Utf8 => DataType::Utf8,
            RedisType::Int64 => DataType::Int64,
            RedisType::Float64 => DataType::Float64,
            RedisType::Boolean => DataType::Boolean,
            RedisType::Date => DataType::Date32,
            RedisType::Datetime => DataType::Timestamp(TimeUnit::Microsecond, None),
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
            RedisType::Date => parse_date(value).map(TypedValue::Date).ok_or_else(|| {
                Error::TypeConversion(format!("Failed to parse '{}' as date", value))
            }),
            RedisType::Datetime => {
                parse_datetime(value)
                    .map(TypedValue::Datetime)
                    .ok_or_else(|| {
                        Error::TypeConversion(format!("Failed to parse '{}' as datetime", value))
                    })
            }
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
    /// Date as days since Unix epoch (1970-01-01).
    Date(i32),
    /// Datetime as microseconds since Unix epoch.
    Datetime(i64),
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

/// Parse a string as a date (days since Unix epoch).
///
/// Accepts:
/// - ISO 8601 date: "2024-01-15"
/// - Epoch days as integer: "19738"
pub fn parse_date(s: &str) -> Option<i32> {
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
            // Calculate days since epoch (1970-01-01)
            // This is a simplified calculation - not accounting for all edge cases
            return days_since_epoch(year, month, day);
        }
    }

    None
}

/// Calculate days since Unix epoch for a given date.
fn days_since_epoch(year: i32, month: u32, day: u32) -> Option<i32> {
    // Validate basic ranges
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    // Days in each month (non-leap year)
    let days_in_month = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

    let is_leap = |y: i32| (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);

    // Count days from 1970 to this year
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

    // Add days for months in current year
    for m in 1..month {
        total_days += days_in_month[m as usize];
        if m == 2 && is_leap(year) {
            total_days += 1;
        }
    }

    // Add days in current month
    total_days += day as i32 - 1;

    Some(total_days)
}

/// Parse a string as a datetime (microseconds since Unix epoch).
///
/// Accepts:
/// - ISO 8601 datetime: "2024-01-15T10:30:00", "2024-01-15T10:30:00Z", "2024-01-15T10:30:00.123456Z"
/// - Unix timestamp (seconds): "1705315800"
/// - Unix timestamp (milliseconds): "1705315800000"
/// - Unix timestamp (microseconds): "1705315800000000"
pub fn parse_datetime(s: &str) -> Option<i64> {
    let s = s.trim();

    // Try parsing as numeric timestamp
    if let Ok(ts) = s.parse::<i64>() {
        // Heuristic to detect timestamp unit:
        // - < 1e10: seconds (up to year 2286)
        // - < 1e13: milliseconds (up to year 2286)
        // - >= 1e13: microseconds
        if ts < 10_000_000_000 {
            // Seconds -> microseconds
            return Some(ts * 1_000_000);
        } else if ts < 10_000_000_000_000 {
            // Milliseconds -> microseconds
            return Some(ts * 1_000);
        } else {
            // Already microseconds
            return Some(ts);
        }
    }

    // Try parsing ISO 8601 datetime
    parse_iso8601_datetime(s)
}

/// Parse ISO 8601 datetime string to microseconds since epoch.
fn parse_iso8601_datetime(s: &str) -> Option<i64> {
    // Remove trailing Z if present
    let s = s.trim_end_matches('Z');

    // Split into date and time parts
    let parts: Vec<&str> = s.split('T').collect();
    if parts.len() != 2 {
        // Try space separator
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
    // Parse date
    let date_parts: Vec<&str> = date_str.split('-').collect();
    if date_parts.len() != 3 {
        return None;
    }

    let year = date_parts[0].parse::<i32>().ok()?;
    let month = date_parts[1].parse::<u32>().ok()?;
    let day = date_parts[2].parse::<u32>().ok()?;

    // Parse time (handle fractional seconds)
    let time_str = time_str.split('+').next()?.split('-').next()?; // Remove timezone offset
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
            // Pad or truncate to 6 digits for microseconds
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

    // Get days since epoch
    let days = days_since_epoch(year, month, day)?;

    // Convert to microseconds
    let day_us = days as i64 * 24 * 60 * 60 * 1_000_000;
    let time_us =
        (hour as i64 * 3600 + minute as i64 * 60 + second as i64) * 1_000_000 + microsecond as i64;

    Some(day_us + time_us)
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
    /// Whether to include the TTL as a column.
    include_ttl: bool,
    /// Name of the TTL column (if included).
    ttl_column_name: String,
    /// Whether to include the row index as a column.
    include_row_index: bool,
    /// Name of the row index column (if included).
    row_index_column_name: String,
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
            include_ttl: false,
            ttl_column_name: "_ttl".to_string(),
            include_row_index: false,
            row_index_column_name: "_index".to_string(),
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

    /// Set whether to include the TTL as a column.
    pub fn with_ttl(mut self, include: bool) -> Self {
        self.include_ttl = include;
        self
    }

    /// Set the name of the TTL column.
    pub fn with_ttl_column_name(mut self, name: impl Into<String>) -> Self {
        self.ttl_column_name = name.into();
        self
    }

    /// Set whether to include the row index as a column.
    pub fn with_row_index(mut self, include: bool) -> Self {
        self.include_row_index = include;
        self
    }

    /// Set the name of the row index column.
    pub fn with_row_index_column_name(mut self, name: impl Into<String>) -> Self {
        self.row_index_column_name = name.into();
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

    /// Whether the TTL column is included.
    pub fn include_ttl(&self) -> bool {
        self.include_ttl
    }

    /// Get the TTL column name.
    pub fn ttl_column_name(&self) -> &str {
        &self.ttl_column_name
    }

    /// Whether the row index column is included.
    pub fn include_row_index(&self) -> bool {
        self.include_row_index
    }

    /// Get the row index column name.
    pub fn row_index_column_name(&self) -> &str {
        &self.row_index_column_name
    }

    /// Convert to Arrow Schema.
    pub fn to_arrow_schema(&self) -> Schema {
        let mut arrow_fields: Vec<Field> = Vec::with_capacity(self.fields.len() + 3);

        // Add row index column first if included
        if self.include_row_index {
            arrow_fields.push(Field::new(
                &self.row_index_column_name,
                DataType::UInt64,
                false,
            ));
        }

        // Add key column if included
        if self.include_key {
            arrow_fields.push(Field::new(&self.key_column_name, DataType::Utf8, false));
        }

        // Add TTL column if included (Int64, nullable - returns -1 for no TTL, -2 for missing key)
        if self.include_ttl {
            arrow_fields.push(Field::new(&self.ttl_column_name, DataType::Int64, true));
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
                // Include if it's a data field (not the key column, TTL column, or row index column)
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

        // Check if TTL column is requested
        let include_ttl = self.include_ttl && columns.contains(&self.ttl_column_name);

        // Check if row index column is requested
        let include_row_index =
            self.include_row_index && columns.contains(&self.row_index_column_name);

        Self {
            fields: projected_fields,
            types: projected_types,
            include_key,
            key_column_name: self.key_column_name.clone(),
            include_ttl,
            ttl_column_name: self.ttl_column_name.clone(),
            include_row_index,
            row_index_column_name: self.row_index_column_name.clone(),
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
            include_ttl: false,
            ttl_column_name: "_ttl".to_string(),
            include_row_index: false,
            row_index_column_name: "_index".to_string(),
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
        assert_eq!(RedisType::Date.to_arrow_type(), DataType::Date32);
        assert_eq!(
            RedisType::Datetime.to_arrow_type(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
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

    #[test]
    fn test_parse_date_iso() {
        // 2024-01-15 is 19737 days since 1970-01-01
        let result = RedisType::Date.parse("2024-01-15").unwrap();
        assert!(matches!(result, TypedValue::Date(_)));
        if let TypedValue::Date(days) = result {
            // Verify it's a reasonable value (around 19737)
            assert!(days > 19000 && days < 20000);
        }
    }

    #[test]
    fn test_parse_date_epoch_days() {
        assert_eq!(
            RedisType::Date.parse("19737").unwrap(),
            TypedValue::Date(19737)
        );
        assert_eq!(RedisType::Date.parse("0").unwrap(), TypedValue::Date(0));
    }

    #[test]
    fn test_parse_date_invalid() {
        assert!(RedisType::Date.parse("not-a-date").is_err());
        assert!(RedisType::Date.parse("2024-13-01").is_err()); // Invalid month
        assert!(RedisType::Date.parse("2024-01-32").is_err()); // Invalid day
    }

    #[test]
    fn test_parse_datetime_iso() {
        // Test basic ISO 8601
        let result = RedisType::Datetime.parse("2024-01-15T10:30:00").unwrap();
        assert!(matches!(result, TypedValue::Datetime(_)));

        // Test with Z suffix
        let result = RedisType::Datetime.parse("2024-01-15T10:30:00Z").unwrap();
        assert!(matches!(result, TypedValue::Datetime(_)));

        // Test with fractional seconds
        let result = RedisType::Datetime
            .parse("2024-01-15T10:30:00.123456Z")
            .unwrap();
        assert!(matches!(result, TypedValue::Datetime(_)));
    }

    #[test]
    fn test_parse_datetime_unix_seconds() {
        // Unix timestamp in seconds (2024-01-15 10:30:00 UTC approximately)
        let result = RedisType::Datetime.parse("1705315800").unwrap();
        if let TypedValue::Datetime(us) = result {
            // Should be converted to microseconds
            assert_eq!(us, 1_705_315_800_000_000);
        } else {
            panic!("Expected Datetime");
        }
    }

    #[test]
    fn test_parse_datetime_unix_milliseconds() {
        let result = RedisType::Datetime.parse("1705315800000").unwrap();
        if let TypedValue::Datetime(us) = result {
            // Should be converted to microseconds
            assert_eq!(us, 1_705_315_800_000_000);
        } else {
            panic!("Expected Datetime");
        }
    }

    #[test]
    fn test_parse_datetime_unix_microseconds() {
        let result = RedisType::Datetime.parse("1705315800000000").unwrap();
        if let TypedValue::Datetime(us) = result {
            // Already in microseconds
            assert_eq!(us, 1_705_315_800_000_000);
        } else {
            panic!("Expected Datetime");
        }
    }

    #[test]
    fn test_parse_datetime_invalid() {
        assert!(RedisType::Datetime.parse("not-a-datetime").is_err());
        assert!(RedisType::Datetime.parse("2024-01-15").is_err()); // Date only, no time
    }

    #[test]
    fn test_days_since_epoch() {
        // 1970-01-01 should be day 0
        assert_eq!(days_since_epoch(1970, 1, 1), Some(0));

        // 1970-01-02 should be day 1
        assert_eq!(days_since_epoch(1970, 1, 2), Some(1));

        // 1971-01-01 should be 365
        assert_eq!(days_since_epoch(1971, 1, 1), Some(365));

        // 1972-01-01 should be 365 + 365 = 730
        assert_eq!(days_since_epoch(1972, 1, 1), Some(730));

        // 1973-01-01 should be 730 + 366 (1972 is leap) = 1096
        assert_eq!(days_since_epoch(1973, 1, 1), Some(1096));
    }
}
