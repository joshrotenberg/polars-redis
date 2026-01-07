//! Search-based iterator for streaming RediSearch results as Arrow RecordBatches.
//!
//! This module provides iteration over RediSearch `FT.SEARCH` results, enabling
//! server-side filtering (predicate pushdown) for efficient data retrieval.

use arrow::array::RecordBatch;
use redis::aio::ConnectionManager;
use tokio::runtime::Runtime;

use super::convert::hashes_to_record_batch;
use super::reader::HashData;
use crate::connection::RedisConnection;
use crate::error::{Error, Result};
use crate::io::search::{SearchConfig, search_hashes};
use crate::schema::HashSchema;

/// Configuration for search-based batch iteration.
#[derive(Debug, Clone)]
pub struct SearchBatchConfig {
    /// RediSearch index name.
    pub index: String,
    /// Search query string (e.g., "@age:[30 +inf]").
    pub query: String,
    /// Number of documents to fetch per batch.
    pub batch_size: usize,
    /// Maximum total rows to return (None for unlimited).
    pub max_rows: Option<usize>,
    /// Sort by field and direction (field_name, ascending).
    pub sort_by: Option<(String, bool)>,
}

impl SearchBatchConfig {
    /// Create a new SearchBatchConfig with the given index and query.
    ///
    /// # Arguments
    /// * `index` - The RediSearch index name
    /// * `query` - The search query (e.g., "@field:value", "*" for all)
    pub fn new(index: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            index: index.into(),
            query: query.into(),
            batch_size: 1000,
            max_rows: None,
            sort_by: None,
        }
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set the maximum number of rows to return.
    pub fn with_max_rows(mut self, max: usize) -> Self {
        self.max_rows = Some(max);
        self
    }

    /// Set the sort field and direction.
    ///
    /// # Arguments
    /// * `field` - Field name to sort by
    /// * `ascending` - True for ascending, false for descending
    pub fn with_sort_by(mut self, field: impl Into<String>, ascending: bool) -> Self {
        self.sort_by = Some((field.into(), ascending));
        self
    }
}

/// Iterator for RediSearch FT.SEARCH results.
///
/// Unlike `HashBatchIterator` which uses SCAN, this iterator uses RediSearch's
/// `FT.SEARCH` with LIMIT for pagination, enabling server-side filtering.
pub struct HashSearchIterator {
    /// Tokio runtime for async operations.
    runtime: Runtime,
    /// Redis connection manager.
    conn: ConnectionManager,
    /// Schema for the hash data.
    schema: HashSchema,
    /// Search configuration.
    config: SearchBatchConfig,
    /// Fields to return (None = all indexed fields).
    projection: Option<Vec<String>>,
    /// Current offset for pagination.
    offset: usize,
    /// Total number of matching documents (from first query).
    total_results: Option<usize>,
    /// Whether we've completed fetching all results.
    done: bool,
    /// Total rows yielded so far.
    rows_yielded: usize,
    /// Current row offset for row index column.
    row_offset: u64,
}

impl HashSearchIterator {
    /// Create a new HashSearchIterator.
    ///
    /// # Arguments
    /// * `url` - Redis connection URL
    /// * `schema` - Schema defining the expected fields and types
    /// * `config` - Search configuration (index, query, etc.)
    /// * `projection` - Optional list of fields to return
    pub fn new(
        url: &str,
        schema: HashSchema,
        config: SearchBatchConfig,
        projection: Option<Vec<String>>,
    ) -> Result<Self> {
        let runtime = Runtime::new()
            .map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;
        let connection = RedisConnection::new(url)?;
        let conn = runtime.block_on(connection.get_connection_manager())?;

        Ok(Self {
            runtime,
            conn,
            schema,
            config,
            projection,
            offset: 0,
            total_results: None,
            done: false,
            rows_yielded: 0,
            row_offset: 0,
        })
    }

    /// Get the next batch of search results as a RecordBatch.
    ///
    /// Returns None when iteration is complete.
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }

        // Check if we've hit the max rows limit
        if let Some(max) = self.config.max_rows
            && self.rows_yielded >= max
        {
            self.done = true;
            return Ok(None);
        }

        // Check if we've fetched all results (based on total from first query)
        if let Some(total) = self.total_results
            && self.offset >= total
        {
            self.done = true;
            return Ok(None);
        }

        // Calculate how many to fetch in this batch
        let mut batch_limit = self.config.batch_size;
        if let Some(max) = self.config.max_rows {
            let remaining = max - self.rows_yielded;
            batch_limit = batch_limit.min(remaining);
        }

        // Execute search
        let hash_data = self.fetch_batch(batch_limit)?;

        if hash_data.is_empty() {
            self.done = true;
            return Ok(None);
        }

        // Apply projection to schema if needed
        let effective_schema = match &self.projection {
            Some(cols) => self.schema.project(cols),
            None => self.schema.clone(),
        };

        // Convert to RecordBatch with current row offset
        let mut batch = hashes_to_record_batch(&hash_data, &effective_schema, self.row_offset)?;

        // Update row offset for next batch
        self.row_offset += batch.num_rows() as u64;

        // Apply max rows limit
        if let Some(max) = self.config.max_rows {
            let remaining = max - self.rows_yielded;
            if batch.num_rows() > remaining {
                batch = batch.slice(0, remaining);
                self.done = true;
            }
        }

        self.rows_yielded += batch.num_rows();

        // Check if we've fetched all results
        if let Some(total) = self.total_results
            && self.offset >= total
        {
            self.done = true;
        }

        Ok(Some(batch))
    }

    /// Fetch a batch of search results.
    fn fetch_batch(&mut self, limit: usize) -> Result<Vec<HashData>> {
        let mut search_config = SearchConfig::new(&self.config.index, &self.config.query)
            .with_offset(self.offset)
            .with_limit(limit);

        if let Some((ref field, ascending)) = self.config.sort_by {
            search_config = search_config.with_sort_by(field.clone(), ascending);
        }

        let return_fields = self.projection.as_deref();
        let mut conn = self.conn.clone();

        let result =
            self.runtime
                .block_on(search_hashes(&mut conn, &search_config, return_fields))?;

        // Store total results count from first query
        if self.total_results.is_none() {
            self.total_results = Some(result.total);
        }

        // Update offset for next batch
        self.offset += result.documents.len();

        Ok(result.documents)
    }

    /// Check if iteration is complete.
    pub fn is_done(&self) -> bool {
        self.done
    }

    /// Get the number of rows yielded so far.
    pub fn rows_yielded(&self) -> usize {
        self.rows_yielded
    }

    /// Get the total number of matching documents (available after first batch).
    pub fn total_results(&self) -> Option<usize> {
        self.total_results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::RedisType;

    #[test]
    fn test_search_batch_config_new() {
        let config = SearchBatchConfig::new("users_idx", "@age:[30 +inf]");

        assert_eq!(config.index, "users_idx");
        assert_eq!(config.query, "@age:[30 +inf]");
        assert_eq!(config.batch_size, 1000);
        assert!(config.max_rows.is_none());
        assert!(config.sort_by.is_none());
    }

    #[test]
    fn test_search_batch_config_builder() {
        let config = SearchBatchConfig::new("products", "*")
            .with_batch_size(500)
            .with_max_rows(10000)
            .with_sort_by("price", false);

        assert_eq!(config.index, "products");
        assert_eq!(config.query, "*");
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.max_rows, Some(10000));
        assert_eq!(config.sort_by, Some(("price".to_string(), false)));
    }

    #[test]
    #[ignore] // Requires running Redis instance
    fn test_hash_search_iterator_creation() {
        let schema = HashSchema::new(vec![
            ("name".to_string(), RedisType::Utf8),
            ("age".to_string(), RedisType::Int64),
        ]);
        let config = SearchBatchConfig::new("users_idx", "*");

        let result = HashSearchIterator::new("redis://localhost:6379", schema, config, None);
        assert!(result.is_ok());
    }
}
