//! Arrow conversion for Redis Stream data.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringBuilder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use super::reader::StreamData;
use crate::error::Result;

/// Schema configuration for Redis Stream scanning.
///
/// Defines output columns when scanning Redis Streams. Each stream entry becomes
/// a row in the output DataFrame.
///
/// # Example
///
/// ```ignore
/// use polars_redis::StreamSchema;
///
/// let schema = StreamSchema::new(vec![
///     "user_id".to_string(),
///     "action".to_string(),
///     "payload".to_string(),
/// ])
/// .with_key(true)
/// .with_id(true)
/// .with_timestamp(true);
/// ```
///
/// # Output Schema
///
/// - `_key` (optional): The Redis stream key
/// - `_id` (optional): The entry ID (e.g., "1234567890123-0")
/// - `_timestamp` (optional): Timestamp extracted from entry ID (Int64 ms)
/// - `_sequence` (optional): Sequence number from entry ID
/// - User-defined fields extracted from entry data
/// - `_index` (optional): Global row number
#[derive(Debug, Clone)]
pub struct StreamSchema {
    /// Whether to include the Redis key as a column.
    pub include_key: bool,
    /// Name of the key column.
    pub key_column_name: String,
    /// Whether to include the entry ID as a column.
    pub include_id: bool,
    /// Name of the entry ID column.
    pub id_column_name: String,
    /// Whether to include the timestamp as a column.
    pub include_timestamp: bool,
    /// Name of the timestamp column.
    pub timestamp_column_name: String,
    /// Whether to include the sequence number as a column.
    pub include_sequence: bool,
    /// Name of the sequence column.
    pub sequence_column_name: String,
    /// Field names to extract from entries.
    pub fields: Vec<String>,
    /// Whether to include a global row index column.
    pub include_row_index: bool,
    /// Name of the row index column.
    pub row_index_column_name: String,
}

impl Default for StreamSchema {
    fn default() -> Self {
        Self {
            include_key: true,
            key_column_name: "_key".to_string(),
            include_id: true,
            id_column_name: "_id".to_string(),
            include_timestamp: true,
            timestamp_column_name: "_ts".to_string(),
            include_sequence: false,
            sequence_column_name: "_seq".to_string(),
            fields: Vec::new(),
            include_row_index: false,
            row_index_column_name: "_index".to_string(),
        }
    }
}

impl StreamSchema {
    /// Create a new StreamSchema with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new StreamSchema with the specified fields.
    pub fn with_fields(fields: Vec<String>) -> Self {
        Self {
            fields,
            ..Default::default()
        }
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

    /// Set whether to include the entry ID column.
    pub fn with_id(mut self, include: bool) -> Self {
        self.include_id = include;
        self
    }

    /// Set the entry ID column name.
    pub fn with_id_column_name(mut self, name: &str) -> Self {
        self.id_column_name = name.to_string();
        self
    }

    /// Set whether to include the timestamp column.
    pub fn with_timestamp(mut self, include: bool) -> Self {
        self.include_timestamp = include;
        self
    }

    /// Set the timestamp column name.
    pub fn with_timestamp_column_name(mut self, name: &str) -> Self {
        self.timestamp_column_name = name.to_string();
        self
    }

    /// Set whether to include the sequence column.
    pub fn with_sequence(mut self, include: bool) -> Self {
        self.include_sequence = include;
        self
    }

    /// Set the sequence column name.
    pub fn with_sequence_column_name(mut self, name: &str) -> Self {
        self.sequence_column_name = name.to_string();
        self
    }

    /// Add a field to extract from entries.
    pub fn add_field(mut self, name: &str) -> Self {
        self.fields.push(name.to_string());
        self
    }

    /// Set the fields to extract from entries.
    pub fn set_fields(mut self, fields: Vec<String>) -> Self {
        self.fields = fields;
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
        let mut arrow_fields = Vec::new();

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

        if self.include_id {
            arrow_fields.push(Field::new(&self.id_column_name, DataType::Utf8, false));
        }

        if self.include_timestamp {
            arrow_fields.push(Field::new(
                &self.timestamp_column_name,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ));
        }

        if self.include_sequence {
            arrow_fields.push(Field::new(
                &self.sequence_column_name,
                DataType::UInt64,
                false,
            ));
        }

        // Add user-defined fields (all as nullable Utf8)
        for field_name in &self.fields {
            arrow_fields.push(Field::new(field_name, DataType::Utf8, true));
        }

        Schema::new(arrow_fields)
    }
}

/// Convert stream data to an Arrow RecordBatch.
///
/// Each stream entry becomes a row in the output.
pub fn streams_to_record_batch(
    data: &[StreamData],
    schema: &StreamSchema,
    row_index_offset: u64,
) -> Result<RecordBatch> {
    // Count total entries across all streams
    let total_entries: usize = data.iter().map(|s| s.entries.len()).sum();

    let arrow_schema = Arc::new(schema.to_arrow_schema());
    let mut columns: Vec<ArrayRef> = Vec::new();

    // Row index column (global)
    if schema.include_row_index {
        let mut builder = UInt64Builder::with_capacity(total_entries);
        let mut idx = row_index_offset;
        for stream_data in data {
            for _ in &stream_data.entries {
                builder.append_value(idx);
                idx += 1;
            }
        }
        columns.push(Arc::new(builder.finish()));
    }

    // Key column
    if schema.include_key {
        let mut builder = StringBuilder::with_capacity(total_entries, total_entries * 32);
        for stream_data in data {
            for _ in &stream_data.entries {
                builder.append_value(&stream_data.key);
            }
        }
        columns.push(Arc::new(builder.finish()));
    }

    // Entry ID column
    if schema.include_id {
        let mut builder = StringBuilder::with_capacity(total_entries, total_entries * 24);
        for stream_data in data {
            for entry in &stream_data.entries {
                builder.append_value(&entry.id);
            }
        }
        columns.push(Arc::new(builder.finish()));
    }

    // Timestamp column (milliseconds since epoch)
    if schema.include_timestamp {
        let mut values = Vec::with_capacity(total_entries);
        for stream_data in data {
            for entry in &stream_data.entries {
                values.push(entry.timestamp_ms);
            }
        }
        let ts_array = arrow::array::TimestampMillisecondArray::from(values);
        columns.push(Arc::new(ts_array));
    }

    // Sequence column
    if schema.include_sequence {
        let mut builder = UInt64Builder::with_capacity(total_entries);
        for stream_data in data {
            for entry in &stream_data.entries {
                builder.append_value(entry.sequence);
            }
        }
        columns.push(Arc::new(builder.finish()));
    }

    // User-defined fields
    for field_name in &schema.fields {
        let mut builder = StringBuilder::with_capacity(total_entries, total_entries * 32);
        for stream_data in data {
            for entry in &stream_data.entries {
                match entry.fields.get(field_name) {
                    Some(value) => builder.append_value(value),
                    None => builder.append_null(),
                }
            }
        }
        columns.push(Arc::new(builder.finish()));
    }

    RecordBatch::try_new(arrow_schema, columns).map_err(|e| {
        crate::error::Error::TypeConversion(format!("Failed to create RecordBatch: {}", e))
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::io::types::stream::reader::StreamEntry;

    #[test]
    fn test_stream_schema_default() {
        let schema = StreamSchema::new();
        assert!(schema.include_key);
        assert!(schema.include_id);
        assert!(schema.include_timestamp);
        assert!(!schema.include_sequence);
        assert!(!schema.include_row_index);
        assert!(schema.fields.is_empty());
    }

    #[test]
    fn test_stream_schema_builder() {
        let schema = StreamSchema::new()
            .with_key(false)
            .with_id(false)
            .with_timestamp(true)
            .with_sequence(true)
            .add_field("action")
            .add_field("user");

        assert!(!schema.include_key);
        assert!(!schema.include_id);
        assert!(schema.include_timestamp);
        assert!(schema.include_sequence);
        assert_eq!(schema.fields, vec!["action", "user"]);
    }

    #[test]
    fn test_streams_to_record_batch_basic() {
        let mut fields = HashMap::new();
        fields.insert("action".to_string(), "login".to_string());

        let data = vec![StreamData {
            key: "stream:1".to_string(),
            entries: vec![
                StreamEntry {
                    id: "1234567890123-0".to_string(),
                    timestamp_ms: 1234567890123,
                    sequence: 0,
                    fields: fields.clone(),
                },
                StreamEntry {
                    id: "1234567890124-0".to_string(),
                    timestamp_ms: 1234567890124,
                    sequence: 0,
                    fields: fields.clone(),
                },
            ],
        }];

        let schema = StreamSchema::new().add_field("action");
        let batch = streams_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.num_rows(), 2);
        // key + id + timestamp + action = 4 columns
        assert_eq!(batch.num_columns(), 4);
    }

    #[test]
    fn test_streams_to_record_batch_with_sequence() {
        let data = vec![StreamData {
            key: "stream:1".to_string(),
            entries: vec![StreamEntry {
                id: "1234567890123-5".to_string(),
                timestamp_ms: 1234567890123,
                sequence: 5,
                fields: HashMap::new(),
            }],
        }];

        let schema = StreamSchema::new().with_sequence(true);
        let batch = streams_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.num_rows(), 1);

        // Check sequence value
        let seq_col = batch
            .column(3) // key, id, timestamp, sequence
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(seq_col.value(0), 5);
    }

    #[test]
    fn test_streams_to_record_batch_missing_field() {
        use arrow::array::Array;

        let mut fields1 = HashMap::new();
        fields1.insert("action".to_string(), "login".to_string());

        let fields2 = HashMap::new(); // No action field

        let data = vec![StreamData {
            key: "stream:1".to_string(),
            entries: vec![
                StreamEntry {
                    id: "1234567890123-0".to_string(),
                    timestamp_ms: 1234567890123,
                    sequence: 0,
                    fields: fields1,
                },
                StreamEntry {
                    id: "1234567890124-0".to_string(),
                    timestamp_ms: 1234567890124,
                    sequence: 0,
                    fields: fields2,
                },
            ],
        }];

        let schema = StreamSchema::new().add_field("action");
        let batch = streams_to_record_batch(&data, &schema, 0).unwrap();

        // Check that second entry has null action
        let action_col = batch
            .column(3) // key, id, timestamp, action
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(action_col.value(0), "login");
        assert!(action_col.is_null(1));
    }

    #[test]
    fn test_streams_to_record_batch_with_row_index() {
        let data = vec![StreamData {
            key: "stream:1".to_string(),
            entries: vec![
                StreamEntry {
                    id: "1234567890123-0".to_string(),
                    timestamp_ms: 1234567890123,
                    sequence: 0,
                    fields: HashMap::new(),
                },
                StreamEntry {
                    id: "1234567890124-0".to_string(),
                    timestamp_ms: 1234567890124,
                    sequence: 0,
                    fields: HashMap::new(),
                },
            ],
        }];

        let schema = StreamSchema::new().with_row_index(true);
        let batch = streams_to_record_batch(&data, &schema, 100).unwrap();

        // Check row indices start at offset
        let idx_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(idx_col.value(0), 100);
        assert_eq!(idx_col.value(1), 101);
    }

    #[test]
    fn test_empty_stream() {
        let data = vec![StreamData {
            key: "empty:stream".to_string(),
            entries: vec![],
        }];

        let schema = StreamSchema::new();
        let batch = streams_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.num_rows(), 0);
    }
}
