//! Arrow conversion for Redis set data.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringBuilder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema};

use super::reader::SetData;
use crate::error::Result;

/// Schema configuration for Redis set scanning.
///
/// Defines output columns when scanning Redis sets. Each set member becomes
/// a row in the output DataFrame.
///
/// # Example
///
/// ```ignore
/// use polars_redis::SetSchema;
///
/// let schema = SetSchema::new()
///     .with_key(true)
///     .with_member_column_name("tag");
/// ```
///
/// # Output Schema
///
/// - `_key` (optional): The Redis key
/// - `member`: The set member value (Utf8)
/// - `_index` (optional): Row number
#[derive(Debug, Clone)]
pub struct SetSchema {
    /// Whether to include the Redis key as a column.
    pub include_key: bool,
    /// Name of the key column.
    pub key_column_name: String,
    /// Name of the member column.
    pub member_column_name: String,
    /// Whether to include a row index column.
    pub include_row_index: bool,
    /// Name of the row index column.
    pub row_index_column_name: String,
}

impl Default for SetSchema {
    fn default() -> Self {
        Self {
            include_key: true,
            key_column_name: "_key".to_string(),
            member_column_name: "member".to_string(),
            include_row_index: false,
            row_index_column_name: "_index".to_string(),
        }
    }
}

impl SetSchema {
    /// Create a new SetSchema with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set whether to include the key column.
    pub fn with_key(mut self, include: bool) -> Self {
        self.include_key = include;
        self
    }

    /// Set the key column name.
    pub fn with_key_column_name(mut self, name: &str) -> Self {
        self.key_column_name = name.to_string();
        self
    }

    /// Set the member column name.
    pub fn with_member_column_name(mut self, name: &str) -> Self {
        self.member_column_name = name.to_string();
        self
    }

    /// Set whether to include a row index column.
    pub fn with_row_index(mut self, include: bool) -> Self {
        self.include_row_index = include;
        self
    }

    /// Set the row index column name.
    pub fn with_row_index_column_name(mut self, name: &str) -> Self {
        self.row_index_column_name = name.to_string();
        self
    }

    /// Build the Arrow schema for this configuration.
    pub fn to_arrow_schema(&self) -> Schema {
        let mut fields = Vec::new();

        if self.include_row_index {
            fields.push(Field::new(
                &self.row_index_column_name,
                DataType::UInt64,
                false,
            ));
        }

        if self.include_key {
            fields.push(Field::new(&self.key_column_name, DataType::Utf8, false));
        }

        fields.push(Field::new(&self.member_column_name, DataType::Utf8, false));

        Schema::new(fields)
    }
}

/// Convert set data to an Arrow RecordBatch.
///
/// Each set member becomes a row in the output.
pub fn sets_to_record_batch(
    data: &[SetData],
    schema: &SetSchema,
    row_index_offset: u64,
) -> Result<RecordBatch> {
    // Count total members across all sets
    let total_members: usize = data.iter().map(|s| s.members.len()).sum();

    let arrow_schema = Arc::new(schema.to_arrow_schema());
    let mut columns: Vec<ArrayRef> = Vec::new();

    // Row index column
    if schema.include_row_index {
        let mut builder = UInt64Builder::with_capacity(total_members);
        let mut idx = row_index_offset;
        for set_data in data {
            for _ in &set_data.members {
                builder.append_value(idx);
                idx += 1;
            }
        }
        columns.push(Arc::new(builder.finish()));
    }

    // Key column
    if schema.include_key {
        let mut builder = StringBuilder::with_capacity(total_members, total_members * 32);
        for set_data in data {
            for _ in &set_data.members {
                builder.append_value(&set_data.key);
            }
        }
        columns.push(Arc::new(builder.finish()));
    }

    // Member column
    let mut builder = StringBuilder::with_capacity(total_members, total_members * 64);
    for set_data in data {
        for member in &set_data.members {
            builder.append_value(member);
        }
    }
    columns.push(Arc::new(builder.finish()));

    RecordBatch::try_new(arrow_schema, columns).map_err(|e| {
        crate::error::Error::TypeConversion(format!("Failed to create RecordBatch: {}", e))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_schema_default() {
        let schema = SetSchema::new();
        assert!(schema.include_key);
        assert_eq!(schema.key_column_name, "_key");
        assert_eq!(schema.member_column_name, "member");
        assert!(!schema.include_row_index);
    }

    #[test]
    fn test_set_schema_builder() {
        let schema = SetSchema::new()
            .with_key(false)
            .with_member_column_name("value")
            .with_row_index(true)
            .with_row_index_column_name("idx");

        assert!(!schema.include_key);
        assert_eq!(schema.member_column_name, "value");
        assert!(schema.include_row_index);
        assert_eq!(schema.row_index_column_name, "idx");
    }

    #[test]
    fn test_sets_to_record_batch_basic() {
        let data = vec![
            SetData {
                key: "set:1".to_string(),
                members: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            },
            SetData {
                key: "set:2".to_string(),
                members: vec!["x".to_string(), "y".to_string()],
            },
        ];

        let schema = SetSchema::new();
        let batch = sets_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.num_rows(), 5); // 3 + 2 members
        assert_eq!(batch.num_columns(), 2); // key + member
    }

    #[test]
    fn test_sets_to_record_batch_with_row_index() {
        let data = vec![SetData {
            key: "set:1".to_string(),
            members: vec!["a".to_string(), "b".to_string()],
        }];

        let schema = SetSchema::new().with_row_index(true);
        let batch = sets_to_record_batch(&data, &schema, 10).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3); // index + key + member

        // Check row indices start at offset
        let idx_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(idx_col.value(0), 10);
        assert_eq!(idx_col.value(1), 11);
    }

    #[test]
    fn test_sets_to_record_batch_no_key() {
        let data = vec![SetData {
            key: "set:1".to_string(),
            members: vec!["a".to_string()],
        }];

        let schema = SetSchema::new().with_key(false);
        let batch = sets_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.num_columns(), 1); // member only
    }

    #[test]
    fn test_empty_set() {
        let data = vec![SetData {
            key: "empty:set".to_string(),
            members: vec![],
        }];

        let schema = SetSchema::new();
        let batch = sets_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.num_rows(), 0);
    }
}
