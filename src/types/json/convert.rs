//! Conversion from Redis JSON data to Arrow arrays.
//!
//! This module handles the conversion of RedisJSON documents into Arrow RecordBatches
//! that can be consumed by Polars.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch, StringBuilder,
    UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};

use super::reader::JsonData;
use crate::error::{Error, Result};

/// Schema for RedisJSON documents, mapping JSONPath expressions to Arrow types.
///
/// Defines how RedisJSON document fields should be extracted and converted to
/// Arrow/Polars columns. Uses JSONPath notation for field access.
///
/// # Example
///
/// ```ignore
/// use polars_redis::JsonSchema;
/// use arrow::datatypes::DataType;
///
/// let schema = JsonSchema::new(vec![
///     ("$.name".to_string(), DataType::Utf8),
///     ("$.user.age".to_string(), DataType::Int64),
///     ("$.metadata.score".to_string(), DataType::Float64),
/// ])
/// .with_key(true)
/// .with_ttl(true);
/// ```
///
/// # Optional Metadata Columns
///
/// - Key column: Include the Redis key as a column (default: true, name: "_key")
/// - TTL column: Include the key's TTL in seconds (default: false, name: "_ttl")
/// - Row index: Include a row number column (default: false, name: "_index")
#[derive(Debug, Clone)]
pub struct JsonSchema {
    /// Ordered list of field names to extract.
    fields: Vec<String>,
    /// Arrow data types for each field.
    types: Vec<DataType>,
    /// Whether to include the Redis key as a column.
    include_key: bool,
    /// Name of the key column.
    key_column_name: String,
    /// Whether to include the TTL as a column.
    include_ttl: bool,
    /// Name of the TTL column.
    ttl_column_name: String,
    /// Whether to include the row index as a column.
    include_row_index: bool,
    /// Name of the row index column.
    row_index_column_name: String,
}

impl JsonSchema {
    /// Create a new JsonSchema from field names and types.
    pub fn new(field_types: Vec<(String, DataType)>) -> Self {
        let (fields, types): (Vec<_>, Vec<_>) = field_types.into_iter().unzip();
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

    /// Get the field names.
    pub fn fields(&self) -> &[String] {
        &self.fields
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

        if self.include_row_index {
            arrow_fields.push(Field::new(
                &self.row_index_column_name,
                DataType::UInt64,
                false,
            ));
        }

        if self.include_key {
            arrow_fields.push(Field::new(&self.key_column_name, DataType::Utf8, false));
        }

        if self.include_ttl {
            arrow_fields.push(Field::new(&self.ttl_column_name, DataType::Int64, true));
        }

        for (name, dtype) in self.fields.iter().zip(self.types.iter()) {
            arrow_fields.push(Field::new(name, dtype.clone(), true));
        }

        Schema::new(arrow_fields)
    }

    /// Get a subset schema with only the specified columns.
    pub fn project(&self, columns: &[String]) -> Self {
        let mut projected_fields = Vec::new();
        let mut projected_types = Vec::new();

        for col in columns {
            if let Some(idx) = self.fields.iter().position(|f| f == col) {
                projected_fields.push(self.fields[idx].clone());
                projected_types.push(self.types[idx].clone());
            }
        }

        let include_key = self.include_key && columns.contains(&self.key_column_name);
        let include_ttl = self.include_ttl && columns.contains(&self.ttl_column_name);
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

/// Normalize JSON response from Redis JSON.GET.
///
/// Redis JSON.GET returns different formats depending on how it's called:
/// - With `$` path: `[{...}]` (array with the object)
/// - With multiple paths like `$.name $.price`: `{"$.name": [...], "$.price": [...]}`
///
/// This function normalizes both formats to a flat object `{"name": ..., "price": ...}`.
fn normalize_json_response(parsed: serde_json::Value) -> Option<serde_json::Value> {
    match parsed {
        // Format 1: Array from single $ path - unwrap first element
        serde_json::Value::Array(mut arr) => arr.pop(),

        // Format 2: Object with path keys like {"$.name": [...], "$.price": [...]}
        serde_json::Value::Object(map) => {
            // Check if this is a path-keyed response (keys start with "$.")
            let is_path_response = map.keys().any(|k| k.starts_with("$."));

            if is_path_response {
                // Convert {"$.name": ["value"], "$.price": [123]} to {"name": "value", "price": 123}
                let mut normalized = serde_json::Map::new();
                for (key, value) in map {
                    // Strip "$." prefix from key
                    let field_name = key.strip_prefix("$.").unwrap_or(&key).to_string();

                    // Unwrap the array to get the actual value
                    let unwrapped = match value {
                        serde_json::Value::Array(mut arr) if !arr.is_empty() => arr.remove(0),
                        other => other,
                    };
                    normalized.insert(field_name, unwrapped);
                }
                Some(serde_json::Value::Object(normalized))
            } else {
                // Regular object, return as-is
                Some(serde_json::Value::Object(map))
            }
        }

        // Other types, return as-is
        other => Some(other),
    }
}

/// Convert a batch of Redis JSON data to an Arrow RecordBatch.
///
/// # Arguments
/// * `data` - The JSON data from Redis
/// * `schema` - The schema defining the column types
/// * `row_offset` - Starting row index for this batch (used when include_row_index is true)
pub fn json_to_record_batch(
    data: &[JsonData],
    schema: &JsonSchema,
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

    // Parse JSON and build columns
    let parsed: Vec<Option<serde_json::Value>> = data
        .iter()
        .map(|d| {
            d.json.as_ref().and_then(|s| {
                let parsed: serde_json::Value = serde_json::from_str(s).ok()?;
                normalize_json_response(parsed)
            })
        })
        .collect();

    // Build each field column
    for (idx, field_name) in schema.fields().iter().enumerate() {
        let dtype = &schema.types[idx];
        let array = build_json_column(&parsed, field_name, dtype)?;
        arrays.push(array);
    }

    RecordBatch::try_new(arrow_schema, arrays)
        .map_err(|e| Error::TypeConversion(format!("Failed to create RecordBatch: {}", e)))
}

/// Build an Arrow array for a single column from parsed JSON.
fn build_json_column(
    data: &[Option<serde_json::Value>],
    field_name: &str,
    dtype: &DataType,
) -> Result<ArrayRef> {
    match dtype {
        DataType::Utf8 => build_utf8_json_column(data, field_name),
        DataType::Int64 => build_int64_json_column(data, field_name),
        DataType::Float64 => build_float64_json_column(data, field_name),
        DataType::Boolean => build_boolean_json_column(data, field_name),
        _ => Err(Error::TypeConversion(format!(
            "Unsupported type {:?} for JSON field '{}'",
            dtype, field_name
        ))),
    }
}

fn build_utf8_json_column(
    data: &[Option<serde_json::Value>],
    field_name: &str,
) -> Result<ArrayRef> {
    let mut builder = StringBuilder::with_capacity(data.len(), data.len() * 32);

    for row in data {
        match row {
            Some(obj) => match obj.get(field_name) {
                Some(serde_json::Value::String(s)) => builder.append_value(s),
                Some(serde_json::Value::Null) | None => builder.append_null(),
                Some(v) => builder.append_value(v.to_string()),
            },
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn build_int64_json_column(
    data: &[Option<serde_json::Value>],
    field_name: &str,
) -> Result<ArrayRef> {
    let mut builder = Int64Builder::with_capacity(data.len());

    for row in data {
        match row {
            Some(obj) => match obj.get(field_name) {
                Some(serde_json::Value::Number(n)) => {
                    if let Some(i) = n.as_i64() {
                        builder.append_value(i);
                    } else if let Some(f) = n.as_f64() {
                        builder.append_value(f as i64);
                    } else {
                        builder.append_null();
                    }
                }
                Some(serde_json::Value::Null) | None => builder.append_null(),
                Some(v) => {
                    return Err(Error::TypeConversion(format!(
                        "Cannot convert {:?} to i64 for field '{}'",
                        v, field_name
                    )));
                }
            },
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn build_float64_json_column(
    data: &[Option<serde_json::Value>],
    field_name: &str,
) -> Result<ArrayRef> {
    let mut builder = Float64Builder::with_capacity(data.len());

    for row in data {
        match row {
            Some(obj) => match obj.get(field_name) {
                Some(serde_json::Value::Number(n)) => {
                    if let Some(f) = n.as_f64() {
                        builder.append_value(f);
                    } else {
                        builder.append_null();
                    }
                }
                Some(serde_json::Value::Null) | None => builder.append_null(),
                Some(v) => {
                    return Err(Error::TypeConversion(format!(
                        "Cannot convert {:?} to f64 for field '{}'",
                        v, field_name
                    )));
                }
            },
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()))
}

fn build_boolean_json_column(
    data: &[Option<serde_json::Value>],
    field_name: &str,
) -> Result<ArrayRef> {
    let mut builder = BooleanBuilder::with_capacity(data.len());

    for row in data {
        match row {
            Some(obj) => match obj.get(field_name) {
                Some(serde_json::Value::Bool(b)) => builder.append_value(*b),
                Some(serde_json::Value::Null) | None => builder.append_null(),
                Some(v) => {
                    return Err(Error::TypeConversion(format!(
                        "Cannot convert {:?} to bool for field '{}'",
                        v, field_name
                    )));
                }
            },
            None => builder.append_null(),
        }
    }

    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_json_data(key: &str, json: Option<&str>) -> JsonData {
        JsonData {
            key: key.to_string(),
            json: json.map(|s| s.to_string()),
            ttl: None,
        }
    }

    fn make_json_data_with_ttl(key: &str, json: Option<&str>, ttl: Option<i64>) -> JsonData {
        JsonData {
            key: key.to_string(),
            json: json.map(|s| s.to_string()),
            ttl,
        }
    }

    #[test]
    fn test_json_to_record_batch_basic() {
        let schema = JsonSchema::new(vec![
            ("name".to_string(), DataType::Utf8),
            ("age".to_string(), DataType::Int64),
        ]);

        let data = vec![
            make_json_data("doc:1", Some(r#"[{"name":"Alice","age":30}]"#)),
            make_json_data("doc:2", Some(r#"[{"name":"Bob","age":25}]"#)),
        ];

        let batch = json_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3); // _key, name, age
    }

    #[test]
    fn test_json_to_record_batch_with_nulls() {
        let schema = JsonSchema::new(vec![
            ("name".to_string(), DataType::Utf8),
            ("age".to_string(), DataType::Int64),
        ]);

        let data = vec![
            make_json_data("doc:1", Some(r#"[{"name":"Alice"}]"#)), // missing age
            make_json_data("doc:2", None),                          // missing document
        ];

        let batch = json_to_record_batch(&data, &schema, 0).unwrap();
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn test_json_to_record_batch_all_types() {
        let schema = JsonSchema::new(vec![
            ("name".to_string(), DataType::Utf8),
            ("count".to_string(), DataType::Int64),
            ("score".to_string(), DataType::Float64),
            ("active".to_string(), DataType::Boolean),
        ]);

        let data = vec![make_json_data(
            "doc:1",
            Some(r#"[{"name":"Test","count":42,"score":3.5,"active":true}]"#),
        )];

        let batch = json_to_record_batch(&data, &schema, 0).unwrap();
        assert_eq!(batch.num_columns(), 5); // _key + 4 fields
    }

    #[test]
    fn test_json_schema_projection() {
        let schema = JsonSchema::new(vec![
            ("name".to_string(), DataType::Utf8),
            ("age".to_string(), DataType::Int64),
            ("email".to_string(), DataType::Utf8),
        ]);

        let projected = schema.project(&["name".to_string(), "email".to_string()]);
        assert_eq!(projected.fields(), &["name", "email"]);
        assert!(!projected.include_key());
    }

    #[test]
    fn test_json_to_record_batch_with_ttl() {
        let schema = JsonSchema::new(vec![("name".to_string(), DataType::Utf8)]).with_ttl(true);

        let data = vec![
            make_json_data_with_ttl("doc:1", Some(r#"[{"name":"Alice"}]"#), Some(3600)),
            make_json_data_with_ttl("doc:2", Some(r#"[{"name":"Bob"}]"#), Some(-1)),
        ];

        let batch = json_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3); // _key, _ttl, name
        assert_eq!(batch.schema().field(1).name(), "_ttl");
    }

    #[test]
    fn test_json_to_record_batch_with_row_index() {
        let schema =
            JsonSchema::new(vec![("name".to_string(), DataType::Utf8)]).with_row_index(true);

        let data = vec![
            make_json_data("doc:1", Some(r#"[{"name":"Alice"}]"#)),
            make_json_data("doc:2", Some(r#"[{"name":"Bob"}]"#)),
            make_json_data("doc:3", Some(r#"[{"name":"Charlie"}]"#)),
        ];

        let batch = json_to_record_batch(&data, &schema, 0).unwrap();

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
    fn test_json_to_record_batch_with_row_index_offset() {
        let schema =
            JsonSchema::new(vec![("name".to_string(), DataType::Utf8)]).with_row_index(true);

        let data = vec![
            make_json_data("doc:1", Some(r#"[{"name":"Alice"}]"#)),
            make_json_data("doc:2", Some(r#"[{"name":"Bob"}]"#)),
        ];

        // Simulate second batch starting at offset 50
        let batch = json_to_record_batch(&data, &schema, 50).unwrap();

        let index_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(index_col.value(0), 50);
        assert_eq!(index_col.value(1), 51);
    }
}
