//! Arrow conversion for Redis sorted set data.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Float64Builder, Int64Builder, RecordBatch, StringBuilder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};

use super::reader::ZSetData;
use crate::error::Result;

/// Schema configuration for Redis sorted set scanning.
///
/// Defines output columns when scanning Redis sorted sets. Each member becomes
/// a row in the output DataFrame, including its score.
///
/// # Example
///
/// ```ignore
/// use polars_redis::ZSetSchema;
///
/// let schema = ZSetSchema::new()
///     .with_key(true)
///     .with_rank(true)  // Include member's rank
///     .with_member_column_name("player")
///     .with_score_column_name("points");
/// ```
///
/// # Output Schema
///
/// - `_key` (optional): The Redis key
/// - `member`: The sorted set member value (Utf8)
/// - `score`: The member's score (Float64)
/// - `rank` (optional): Member's rank by score (Int64)
/// - `_index` (optional): Global row number
#[derive(Debug, Clone)]
pub struct ZSetSchema {
    /// Whether to include the Redis key as a column.
    pub include_key: bool,
    /// Name of the key column.
    pub key_column_name: String,
    /// Name of the member column.
    pub member_column_name: String,
    /// Name of the score column.
    pub score_column_name: String,
    /// Whether to include rank (position by score).
    pub include_rank: bool,
    /// Name of the rank column.
    pub rank_column_name: String,
    /// Whether to include a global row index column.
    pub include_row_index: bool,
    /// Name of the row index column.
    pub row_index_column_name: String,
}

impl Default for ZSetSchema {
    fn default() -> Self {
        Self {
            include_key: true,
            key_column_name: "_key".to_string(),
            member_column_name: "member".to_string(),
            score_column_name: "score".to_string(),
            include_rank: false,
            rank_column_name: "rank".to_string(),
            include_row_index: false,
            row_index_column_name: "_index".to_string(),
        }
    }
}

impl ZSetSchema {
    /// Create a new ZSetSchema with default settings.
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

    /// Set the score column name.
    pub fn with_score_column_name(mut self, name: &str) -> Self {
        self.score_column_name = name.to_string();
        self
    }

    /// Set whether to include rank column.
    pub fn with_rank(mut self, include: bool) -> Self {
        self.include_rank = include;
        self
    }

    /// Set the rank column name.
    pub fn with_rank_column_name(mut self, name: &str) -> Self {
        self.rank_column_name = name.to_string();
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

        if self.include_rank {
            fields.push(Field::new(&self.rank_column_name, DataType::Int64, false));
        }

        fields.push(Field::new(&self.member_column_name, DataType::Utf8, false));
        fields.push(Field::new(
            &self.score_column_name,
            DataType::Float64,
            false,
        ));

        Schema::new(fields)
    }
}

/// Convert sorted set data to an Arrow RecordBatch.
///
/// Each member becomes a row with its score.
pub fn zsets_to_record_batch(
    data: &[ZSetData],
    schema: &ZSetSchema,
    row_index_offset: u64,
) -> Result<RecordBatch> {
    // Count total members across all sorted sets
    let total_members: usize = data.iter().map(|z| z.members.len()).sum();

    let arrow_schema = Arc::new(schema.to_arrow_schema());
    let mut columns: Vec<ArrayRef> = Vec::new();

    // Row index column (global)
    if schema.include_row_index {
        let mut builder = UInt64Builder::with_capacity(total_members);
        let mut idx = row_index_offset;
        for zset_data in data {
            for _ in &zset_data.members {
                builder.append_value(idx);
                idx += 1;
            }
        }
        columns.push(Arc::new(builder.finish()));
    }

    // Key column
    if schema.include_key {
        let mut builder = StringBuilder::with_capacity(total_members, total_members * 32);
        for zset_data in data {
            for _ in &zset_data.members {
                builder.append_value(&zset_data.key);
            }
        }
        columns.push(Arc::new(builder.finish()));
    }

    // Rank column (position within each sorted set, 0-indexed)
    if schema.include_rank {
        let mut builder = Int64Builder::with_capacity(total_members);
        for zset_data in data {
            for (rank, _) in zset_data.members.iter().enumerate() {
                builder.append_value(rank as i64);
            }
        }
        columns.push(Arc::new(builder.finish()));
    }

    // Member column
    let mut member_builder = StringBuilder::with_capacity(total_members, total_members * 64);
    for zset_data in data {
        for (member, _) in &zset_data.members {
            member_builder.append_value(member);
        }
    }
    columns.push(Arc::new(member_builder.finish()));

    // Score column
    let mut score_builder = Float64Builder::with_capacity(total_members);
    for zset_data in data {
        for (_, score) in &zset_data.members {
            score_builder.append_value(*score);
        }
    }
    columns.push(Arc::new(score_builder.finish()));

    RecordBatch::try_new(arrow_schema, columns).map_err(|e| {
        crate::error::Error::TypeConversion(format!("Failed to create RecordBatch: {}", e))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zset_schema_default() {
        let schema = ZSetSchema::new();
        assert!(schema.include_key);
        assert_eq!(schema.key_column_name, "_key");
        assert_eq!(schema.member_column_name, "member");
        assert_eq!(schema.score_column_name, "score");
        assert!(!schema.include_rank);
        assert!(!schema.include_row_index);
    }

    #[test]
    fn test_zset_schema_builder() {
        let schema = ZSetSchema::new()
            .with_key(false)
            .with_member_column_name("player")
            .with_score_column_name("points")
            .with_rank(true)
            .with_rank_column_name("position")
            .with_row_index(true);

        assert!(!schema.include_key);
        assert_eq!(schema.member_column_name, "player");
        assert_eq!(schema.score_column_name, "points");
        assert!(schema.include_rank);
        assert_eq!(schema.rank_column_name, "position");
        assert!(schema.include_row_index);
    }

    #[test]
    fn test_zsets_to_record_batch_basic() {
        let data = vec![
            ZSetData {
                key: "leaderboard:1".to_string(),
                members: vec![
                    ("alice".to_string(), 100.0),
                    ("bob".to_string(), 85.5),
                    ("carol".to_string(), 92.0),
                ],
            },
            ZSetData {
                key: "leaderboard:2".to_string(),
                members: vec![("dave".to_string(), 78.0), ("eve".to_string(), 95.0)],
            },
        ];

        let schema = ZSetSchema::new();
        let batch = zsets_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.num_rows(), 5); // 3 + 2 members
        assert_eq!(batch.num_columns(), 3); // key + member + score
    }

    #[test]
    fn test_zsets_to_record_batch_with_rank() {
        let data = vec![ZSetData {
            key: "scores".to_string(),
            members: vec![
                ("first".to_string(), 100.0),
                ("second".to_string(), 90.0),
                ("third".to_string(), 80.0),
            ],
        }];

        let schema = ZSetSchema::new().with_rank(true);
        let batch = zsets_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 4); // key + rank + member + score

        // Check ranks
        let rank_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(rank_col.value(0), 0);
        assert_eq!(rank_col.value(1), 1);
        assert_eq!(rank_col.value(2), 2);
    }

    #[test]
    fn test_zsets_to_record_batch_scores() {
        let data = vec![ZSetData {
            key: "scores".to_string(),
            members: vec![("a".to_string(), 1.5), ("b".to_string(), 2.5)],
        }];

        let schema = ZSetSchema::new().with_key(false);
        let batch = zsets_to_record_batch(&data, &schema, 0).unwrap();

        // Check scores
        let score_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!((score_col.value(0) - 1.5).abs() < f64::EPSILON);
        assert!((score_col.value(1) - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_empty_zset() {
        let data = vec![ZSetData {
            key: "empty:zset".to_string(),
            members: vec![],
        }];

        let schema = ZSetSchema::new();
        let batch = zsets_to_record_batch(&data, &schema, 0).unwrap();

        assert_eq!(batch.num_rows(), 0);
    }
}
