//! Arrow conversion for RedisTimeSeries data.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Builder, RecordBatch, StringBuilder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use super::reader::TimeSeriesData;
use crate::error::Result;

/// Schema configuration for RedisTimeSeries scanning.
#[derive(Debug, Clone)]
pub struct TimeSeriesSchema {
    /// Whether to include the Redis key as a column.
    pub include_key: bool,
    /// Name of the key column.
    pub key_column_name: String,
    /// Whether to include the timestamp as a column.
    pub include_timestamp: bool,
    /// Name of the timestamp column.
    pub timestamp_column_name: String,
    /// Name of the value column.
    pub value_column_name: String,
    /// Whether to include a global row index column.
    pub include_row_index: bool,
    /// Name of the row index column.
    pub row_index_column_name: String,
    /// Label names to include as columns (from TS.MRANGE with labels).
    pub label_columns: Vec<String>,
}

impl Default for TimeSeriesSchema {
    fn default() -> Self {
        Self {
            include_key: true,
            key_column_name: "_key".to_string(),
            include_timestamp: true,
            timestamp_column_name: "_ts".to_string(),
            value_column_name: "value".to_string(),
            include_row_index: false,
            row_index_column_name: "_index".to_string(),
            label_columns: Vec::new(),
        }
    }
}

impl TimeSeriesSchema {
    /// Create a new TimeSeriesSchema with default settings.
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

    /// Set the value column name.
    pub fn with_value_column_name(mut self, name: &str) -> Self {
        self.value_column_name = name.to_string();
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

    /// Add a label to include as a column.
    pub fn add_label_column(mut self, name: &str) -> Self {
        self.label_columns.push(name.to_string());
        self
    }

    /// Set the labels to include as columns.
    pub fn with_label_columns(mut self, labels: Vec<String>) -> Self {
        self.label_columns = labels;
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

        if self.include_timestamp {
            fields.push(Field::new(
                &self.timestamp_column_name,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ));
        }

        // Value column (always included)
        fields.push(Field::new(
            &self.value_column_name,
            DataType::Float64,
            false,
        ));

        // Label columns (nullable)
        for label in &self.label_columns {
            fields.push(Field::new(label, DataType::Utf8, true));
        }

        Schema::new(fields)
    }
}

/// Convert time series data to an Arrow RecordBatch.
///
/// Each sample becomes a row in the output.
pub fn timeseries_to_record_batch(
    data: &[TimeSeriesData],
    schema: &TimeSeriesSchema,
    row_index_offset: u64,
) -> Result<RecordBatch> {
    // Count total samples across all time series
    let total_samples: usize = data.iter().map(|ts| ts.samples.len()).sum();

    let arrow_schema = Arc::new(schema.to_arrow_schema());
    let mut columns: Vec<ArrayRef> = Vec::new();

    // Row index column (global)
    if schema.include_row_index {
        let mut builder = UInt64Builder::with_capacity(total_samples);
        let mut idx = row_index_offset;
        for ts_data in data {
            for _ in &ts_data.samples {
                builder.append_value(idx);
                idx += 1;
            }
        }
        columns.push(Arc::new(builder.finish()));
    }

    // Key column
    if schema.include_key {
        let mut builder = StringBuilder::with_capacity(total_samples, total_samples * 32);
        for ts_data in data {
            for _ in &ts_data.samples {
                builder.append_value(&ts_data.key);
            }
        }
        columns.push(Arc::new(builder.finish()));
    }

    // Timestamp column
    if schema.include_timestamp {
        let mut values = Vec::with_capacity(total_samples);
        for ts_data in data {
            for sample in &ts_data.samples {
                values.push(sample.timestamp_ms);
            }
        }
        let ts_array = arrow::array::TimestampMillisecondArray::from(values);
        columns.push(Arc::new(ts_array));
    }

    // Value column
    let mut builder = Float64Builder::with_capacity(total_samples);
    for ts_data in data {
        for sample in &ts_data.samples {
            builder.append_value(sample.value);
        }
    }
    columns.push(Arc::new(builder.finish()));

    // Label columns
    for label_name in &schema.label_columns {
        let mut builder = StringBuilder::with_capacity(total_samples, total_samples * 16);
        for ts_data in data {
            // Find the label value for this time series
            let label_value = ts_data
                .labels
                .iter()
                .find(|(k, _)| k == label_name)
                .map(|(_, v)| v.as_str());

            for _ in &ts_data.samples {
                match label_value {
                    Some(v) => builder.append_value(v),
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
    use super::*;
    use crate::types::timeseries::reader::TimeSeriesSample;

    #[test]
    fn test_timeseries_schema_default() {
        let schema = TimeSeriesSchema::new();
        assert!(schema.include_key);
        assert!(schema.include_timestamp);
        assert_eq!(schema.key_column_name, "_key");
        assert_eq!(schema.timestamp_column_name, "_ts");
        assert_eq!(schema.value_column_name, "value");
        assert!(!schema.include_row_index);
        assert!(schema.label_columns.is_empty());
    }

    #[test]
    fn test_timeseries_schema_builder() {
        let schema = TimeSeriesSchema::new()
            .with_key(false)
            .with_value_column_name("temperature")
            .with_row_index(true)
            .add_label_column("location")
            .add_label_column("unit");

        assert!(!schema.include_key);
        assert_eq!(schema.value_column_name, "temperature");
        assert!(schema.include_row_index);
        assert_eq!(schema.label_columns, vec!["location", "unit"]);
    }

    #[test]
    fn test_timeseries_to_record_batch_basic() {
        let data = vec![TimeSeriesData {
            key: "sensor:1".to_string(),
            labels: vec![],
            samples: vec![
                TimeSeriesSample {
                    timestamp_ms: 1000,
                    value: 20.5,
                },
                TimeSeriesSample {
                    timestamp_ms: 2000,
                    value: 21.0,
                },
                TimeSeriesSample {
                    timestamp_ms: 3000,
                    value: 21.5,
                },
            ],
        }];

        let schema = TimeSeriesSchema::new();
        let batch = timeseries_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3); // key + timestamp + value
    }

    #[test]
    fn test_timeseries_to_record_batch_with_labels() {
        let data = vec![TimeSeriesData {
            key: "sensor:1".to_string(),
            labels: vec![
                ("location".to_string(), "us".to_string()),
                ("unit".to_string(), "celsius".to_string()),
            ],
            samples: vec![TimeSeriesSample {
                timestamp_ms: 1000,
                value: 20.5,
            }],
        }];

        let schema = TimeSeriesSchema::new()
            .add_label_column("location")
            .add_label_column("unit");
        let batch = timeseries_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 5); // key + timestamp + value + location + unit

        // Check label values
        let location_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(location_col.value(0), "us");

        let unit_col = batch
            .column(4)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(unit_col.value(0), "celsius");
    }

    #[test]
    fn test_timeseries_to_record_batch_with_row_index() {
        let data = vec![TimeSeriesData {
            key: "sensor:1".to_string(),
            labels: vec![],
            samples: vec![
                TimeSeriesSample {
                    timestamp_ms: 1000,
                    value: 20.5,
                },
                TimeSeriesSample {
                    timestamp_ms: 2000,
                    value: 21.0,
                },
            ],
        }];

        let schema = TimeSeriesSchema::new().with_row_index(true);
        let batch = timeseries_to_record_batch(&data, &schema, 100).unwrap();

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
    fn test_timeseries_values() {
        let data = vec![TimeSeriesData {
            key: "sensor:1".to_string(),
            labels: vec![],
            samples: vec![
                TimeSeriesSample {
                    timestamp_ms: 1000,
                    value: 20.5,
                },
                TimeSeriesSample {
                    timestamp_ms: 2000,
                    value: -15.3,
                },
            ],
        }];

        let schema = TimeSeriesSchema::new()
            .with_key(false)
            .with_timestamp(false);
        let batch = timeseries_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.num_columns(), 1); // Just value

        let value_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!((value_col.value(0) - 20.5).abs() < f64::EPSILON);
        assert!((value_col.value(1) - (-15.3)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_empty_timeseries() {
        let data = vec![TimeSeriesData {
            key: "empty:ts".to_string(),
            labels: vec![],
            samples: vec![],
        }];

        let schema = TimeSeriesSchema::new();
        let batch = timeseries_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.num_rows(), 0);
    }
}
