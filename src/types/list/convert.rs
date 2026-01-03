//! Arrow conversion for Redis list data.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Builder, RecordBatch, StringBuilder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema};

use super::reader::ListData;
use crate::error::Result;

/// Schema configuration for Redis list scanning.
#[derive(Debug, Clone)]
pub struct ListSchema {
    /// Whether to include the Redis key as a column.
    pub include_key: bool,
    /// Name of the key column.
    pub key_column_name: String,
    /// Name of the element column.
    pub element_column_name: String,
    /// Whether to include position index within each list.
    pub include_position: bool,
    /// Name of the position column.
    pub position_column_name: String,
    /// Whether to include a global row index column.
    pub include_row_index: bool,
    /// Name of the row index column.
    pub row_index_column_name: String,
}

impl Default for ListSchema {
    fn default() -> Self {
        Self {
            include_key: true,
            key_column_name: "_key".to_string(),
            element_column_name: "element".to_string(),
            include_position: false,
            position_column_name: "position".to_string(),
            include_row_index: false,
            row_index_column_name: "_index".to_string(),
        }
    }
}

impl ListSchema {
    /// Create a new ListSchema with default settings.
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

    /// Set the element column name.
    pub fn with_element_column_name(mut self, name: &str) -> Self {
        self.element_column_name = name.to_string();
        self
    }

    /// Set whether to include position index within each list.
    pub fn with_position(mut self, include: bool) -> Self {
        self.include_position = include;
        self
    }

    /// Set the position column name.
    pub fn with_position_column_name(mut self, name: &str) -> Self {
        self.position_column_name = name.to_string();
        self
    }

    /// Set whether to include a global row index column.
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

        if self.include_position {
            fields.push(Field::new(
                &self.position_column_name,
                DataType::Int64,
                false,
            ));
        }

        fields.push(Field::new(&self.element_column_name, DataType::Utf8, false));

        Schema::new(fields)
    }
}

/// Convert list data to an Arrow RecordBatch.
///
/// Each list element becomes a row in the output.
pub fn lists_to_record_batch(
    data: &[ListData],
    schema: &ListSchema,
    row_index_offset: u64,
) -> Result<RecordBatch> {
    // Count total elements across all lists
    let total_elements: usize = data.iter().map(|l| l.elements.len()).sum();

    let arrow_schema = Arc::new(schema.to_arrow_schema());
    let mut columns: Vec<ArrayRef> = Vec::new();

    // Row index column (global)
    if schema.include_row_index {
        let mut builder = UInt64Builder::with_capacity(total_elements);
        let mut idx = row_index_offset;
        for list_data in data {
            for _ in &list_data.elements {
                builder.append_value(idx);
                idx += 1;
            }
        }
        columns.push(Arc::new(builder.finish()));
    }

    // Key column
    if schema.include_key {
        let mut builder = StringBuilder::with_capacity(total_elements, total_elements * 32);
        for list_data in data {
            for _ in &list_data.elements {
                builder.append_value(&list_data.key);
            }
        }
        columns.push(Arc::new(builder.finish()));
    }

    // Position column (index within each list)
    if schema.include_position {
        let mut builder = Int64Builder::with_capacity(total_elements);
        for list_data in data {
            for (pos, _) in list_data.elements.iter().enumerate() {
                builder.append_value(pos as i64);
            }
        }
        columns.push(Arc::new(builder.finish()));
    }

    // Element column
    let mut builder = StringBuilder::with_capacity(total_elements, total_elements * 64);
    for list_data in data {
        for element in &list_data.elements {
            builder.append_value(element);
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
    fn test_list_schema_default() {
        let schema = ListSchema::new();
        assert!(schema.include_key);
        assert_eq!(schema.key_column_name, "_key");
        assert_eq!(schema.element_column_name, "element");
        assert!(!schema.include_position);
        assert!(!schema.include_row_index);
    }

    #[test]
    fn test_list_schema_builder() {
        let schema = ListSchema::new()
            .with_key(false)
            .with_element_column_name("value")
            .with_position(true)
            .with_position_column_name("idx")
            .with_row_index(true);

        assert!(!schema.include_key);
        assert_eq!(schema.element_column_name, "value");
        assert!(schema.include_position);
        assert_eq!(schema.position_column_name, "idx");
        assert!(schema.include_row_index);
    }

    #[test]
    fn test_lists_to_record_batch_basic() {
        let data = vec![
            ListData {
                key: "list:1".to_string(),
                elements: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            },
            ListData {
                key: "list:2".to_string(),
                elements: vec!["x".to_string(), "y".to_string()],
            },
        ];

        let schema = ListSchema::new();
        let batch = lists_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.num_rows(), 5); // 3 + 2 elements
        assert_eq!(batch.num_columns(), 2); // key + element
    }

    #[test]
    fn test_lists_to_record_batch_with_position() {
        let data = vec![ListData {
            key: "list:1".to_string(),
            elements: vec!["a".to_string(), "b".to_string(), "c".to_string()],
        }];

        let schema = ListSchema::new().with_position(true);
        let batch = lists_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3); // key + position + element

        // Check positions
        let pos_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(pos_col.value(0), 0);
        assert_eq!(pos_col.value(1), 1);
        assert_eq!(pos_col.value(2), 2);
    }

    #[test]
    fn test_lists_to_record_batch_with_row_index() {
        let data = vec![ListData {
            key: "list:1".to_string(),
            elements: vec!["a".to_string(), "b".to_string()],
        }];

        let schema = ListSchema::new().with_row_index(true);
        let batch = lists_to_record_batch(&data, &schema, 10).unwrap();

        assert_eq!(batch.num_rows(), 2);

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
    fn test_empty_list() {
        let data = vec![ListData {
            key: "empty:list".to_string(),
            elements: vec![],
        }];

        let schema = ListSchema::new();
        let batch = lists_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.num_rows(), 0);
    }
}
