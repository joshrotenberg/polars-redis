//! RediSearch integration for server-side filtering.
//!
//! This module provides support for querying Redis data using RediSearch's
//! `FT.SEARCH` command, enabling predicate pushdown for efficient data retrieval.
//!
//! # Example
//!
//! ```ignore
//! use polars_redis::search::{SearchConfig, search_hashes};
//!
//! // Search for users over 30 years old
//! let config = SearchConfig::new("users_idx", "@age:[30 +inf]")
//!     .with_limit(100)
//!     .with_sort_by("age", true);
//!
//! let results = search_hashes(&mut conn, &config, None).await?;
//! ```

use std::collections::HashMap;

use redis::aio::ConnectionManager;

use crate::error::Result;
use crate::types::hash::HashData;

/// Configuration for RediSearch FT.SEARCH queries.
#[derive(Debug, Clone)]
pub struct SearchConfig {
    /// RediSearch index name.
    pub index: String,
    /// Query string (e.g., "@name:john @age:[25 50]").
    pub query: String,
    /// Maximum number of results to return.
    pub limit: Option<usize>,
    /// Offset for pagination.
    pub offset: usize,
    /// Sort by field and direction (field_name, ascending).
    pub sort_by: Option<(String, bool)>,
    /// Whether to return document content (default: true).
    pub nocontent: bool,
}

impl SearchConfig {
    /// Create a new SearchConfig with the given index and query.
    ///
    /// # Arguments
    /// * `index` - The RediSearch index name
    /// * `query` - The search query (e.g., "@field:value", "*" for all)
    pub fn new(index: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            index: index.into(),
            query: query.into(),
            limit: None,
            offset: 0,
            sort_by: None,
            nocontent: false,
        }
    }

    /// Set the maximum number of results to return.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the offset for pagination.
    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = offset;
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

    /// Set whether to return only document IDs (no content).
    pub fn with_nocontent(mut self, nocontent: bool) -> Self {
        self.nocontent = nocontent;
        self
    }
}

/// Result of an FT.SEARCH query.
#[derive(Debug)]
pub struct SearchResult {
    /// Total number of matching documents.
    pub total: usize,
    /// Documents returned in this batch.
    pub documents: Vec<HashData>,
}

/// Execute FT.SEARCH and return matching hash documents.
///
/// # Arguments
/// * `conn` - Redis connection manager
/// * `config` - Search configuration
/// * `return_fields` - Optional list of fields to return (None = all fields)
///
/// # Returns
/// A `SearchResult` containing the total count and matching documents.
pub async fn search_hashes(
    conn: &mut ConnectionManager,
    config: &SearchConfig,
    return_fields: Option<&[String]>,
) -> Result<SearchResult> {
    let mut cmd = redis::cmd("FT.SEARCH");
    cmd.arg(&config.index).arg(&config.query);

    // Add RETURN clause if specific fields requested
    if let Some(fields) = return_fields {
        cmd.arg("RETURN").arg(fields.len());
        for field in fields {
            cmd.arg(field);
        }
    }

    // Add SORTBY if specified
    if let Some((field, ascending)) = &config.sort_by {
        cmd.arg("SORTBY").arg(field);
        if *ascending {
            cmd.arg("ASC");
        } else {
            cmd.arg("DESC");
        }
    }

    // Add LIMIT for pagination
    let limit = config.limit.unwrap_or(10); // RediSearch default is 10
    cmd.arg("LIMIT").arg(config.offset).arg(limit);

    // Execute query
    let result: redis::Value = cmd.query_async(conn).await?;

    // Parse response
    parse_search_response(result)
}

/// Parse FT.SEARCH response into SearchResult.
///
/// FT.SEARCH returns:
/// ```text
/// 1) (integer) total_results
/// 2) "doc:1"           # key
/// 3) ["field1", "value1", "field2", "value2", ...]
/// 4) "doc:2"
/// 5) ["field1", "value1", ...]
/// ```
fn parse_search_response(value: redis::Value) -> Result<SearchResult> {
    match value {
        redis::Value::Array(arr) if !arr.is_empty() => {
            // First element is total count
            let total = match &arr[0] {
                redis::Value::Int(n) => *n as usize,
                _ => 0,
            };

            let mut documents = Vec::new();
            let mut i = 1;

            while i < arr.len() {
                // Document key
                let key = match &arr[i] {
                    redis::Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                    redis::Value::SimpleString(s) => s.clone(),
                    _ => {
                        i += 1;
                        continue;
                    }
                };
                i += 1;

                // Document fields (array of field-value pairs)
                if i < arr.len() {
                    let fields = match &arr[i] {
                        redis::Value::Array(field_arr) => parse_field_array(field_arr),
                        _ => HashMap::new(),
                    };
                    i += 1;

                    documents.push(HashData {
                        key,
                        fields,
                        ttl: None,
                    });
                }
            }

            Ok(SearchResult { total, documents })
        }
        _ => Ok(SearchResult {
            total: 0,
            documents: Vec::new(),
        }),
    }
}

/// Parse field array from FT.SEARCH response.
fn parse_field_array(arr: &[redis::Value]) -> HashMap<String, Option<String>> {
    let mut fields = HashMap::new();
    let mut i = 0;

    while i + 1 < arr.len() {
        let field_name = match &arr[i] {
            redis::Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
            redis::Value::SimpleString(s) => s.clone(),
            _ => {
                i += 2;
                continue;
            }
        };

        let field_value = match &arr[i + 1] {
            redis::Value::BulkString(bytes) => Some(String::from_utf8_lossy(bytes).to_string()),
            redis::Value::SimpleString(s) => Some(s.clone()),
            redis::Value::Nil => None,
            _ => None,
        };

        fields.insert(field_name, field_value);
        i += 2;
    }

    fields
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_search_config_builder() {
        let config = SearchConfig::new("users_idx", "@age:[30 +inf]")
            .with_limit(100)
            .with_offset(50)
            .with_sort_by("age", true);

        assert_eq!(config.index, "users_idx");
        assert_eq!(config.query, "@age:[30 +inf]");
        assert_eq!(config.limit, Some(100));
        assert_eq!(config.offset, 50);
        assert_eq!(config.sort_by, Some(("age".to_string(), true)));
    }

    #[test]
    fn test_search_config_defaults() {
        let config = SearchConfig::new("idx", "*");

        assert_eq!(config.index, "idx");
        assert_eq!(config.query, "*");
        assert_eq!(config.limit, None);
        assert_eq!(config.offset, 0);
        assert_eq!(config.sort_by, None);
        assert!(!config.nocontent);
    }

    #[test]
    fn test_parse_empty_response() {
        let result = parse_search_response(redis::Value::Array(vec![redis::Value::Int(0)]));
        assert!(result.is_ok());
        let search_result = result.unwrap();
        assert_eq!(search_result.total, 0);
        assert!(search_result.documents.is_empty());
    }

    #[test]
    fn test_parse_field_array() {
        let arr = vec![
            redis::Value::BulkString(b"name".to_vec()),
            redis::Value::BulkString(b"Alice".to_vec()),
            redis::Value::BulkString(b"age".to_vec()),
            redis::Value::BulkString(b"30".to_vec()),
        ];

        let fields = parse_field_array(&arr);
        assert_eq!(fields.get("name"), Some(&Some("Alice".to_string())));
        assert_eq!(fields.get("age"), Some(&Some("30".to_string())));
    }
}
