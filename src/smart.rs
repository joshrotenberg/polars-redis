//! Smart scan with automatic index detection.
//!
//! This module provides a higher-level abstraction that automatically detects
//! whether a RediSearch index exists for a key pattern and optimizes query
//! execution accordingly.
//!
//! # Example
//!
//! ```ignore
//! use polars_redis::smart::{explain_scan, smart_scan_hashes, ExecutionStrategy};
//!
//! // Explain what strategy would be used
//! let plan = explain_scan("redis://localhost:6379", "user:*")?;
//! println!("Strategy: {}", plan.strategy);
//! if let Some(idx) = &plan.index {
//!     println!("Using index: {}", idx.name);
//! }
//!
//! // Smart scan automatically chooses best strategy
//! let schema = HashSchema::new(vec![
//!     ("name".to_string(), RedisType::Utf8),
//!     ("age".to_string(), RedisType::Int64),
//! ]);
//! let config = BatchConfig::new("user:*");
//! let mut iter = smart_scan_hashes("redis://localhost:6379", schema, config, None)?;
//! while let Some(batch) = iter.next_batch()? {
//!     println!("Got {} rows", batch.num_rows());
//! }
//! ```

use std::fmt;

use redis::aio::ConnectionManager;
use tokio::runtime::Runtime;

use crate::connection::RedisConnection;
use crate::error::{Error, Result};
use crate::io::types::hash::{BatchConfig, HashBatchIterator};
#[cfg(feature = "search")]
use crate::io::types::hash::{HashSearchIterator, SearchBatchConfig};
use crate::schema::HashSchema;

/// Strategy for executing a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionStrategy {
    /// Use FT.SEARCH with index.
    Search,
    /// Use SCAN without index.
    Scan,
    /// Use FT.SEARCH + client-side filtering.
    Hybrid,
}

impl fmt::Display for ExecutionStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Search => write!(f, "SEARCH"),
            Self::Scan => write!(f, "SCAN"),
            Self::Hybrid => write!(f, "HYBRID"),
        }
    }
}

/// Information about an auto-detected index.
#[derive(Debug, Clone)]
pub struct DetectedIndex {
    /// Index name.
    pub name: String,
    /// Key prefixes covered by this index.
    pub prefixes: Vec<String>,
    /// Data type: "HASH" or "JSON".
    pub on_type: String,
    /// Field names in this index.
    pub fields: Vec<String>,
}

/// Execution plan for a query.
#[derive(Debug, Clone)]
pub struct QueryPlan {
    /// The execution strategy.
    pub strategy: ExecutionStrategy,
    /// The detected index, if any.
    pub index: Option<DetectedIndex>,
    /// Server-side query string, if applicable.
    pub server_query: Option<String>,
    /// Client-side filter expressions.
    pub client_filters: Vec<String>,
    /// Warnings about the execution plan.
    pub warnings: Vec<String>,
}

impl QueryPlan {
    /// Create a new query plan.
    pub fn new(strategy: ExecutionStrategy) -> Self {
        Self {
            strategy,
            index: None,
            server_query: None,
            client_filters: Vec::new(),
            warnings: Vec::new(),
        }
    }

    /// Return a human-readable explanation of the query plan.
    pub fn explain(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!("Strategy: {}", self.strategy));

        if let Some(index) = &self.index {
            lines.push(format!("Index: {}", index.name));
            lines.push(format!("  Prefixes: {}", index.prefixes.join(", ")));
            lines.push(format!("  Type: {}", index.on_type));
        }

        if let Some(query) = &self.server_query {
            lines.push(format!("Server Query: {}", query));
        }

        if !self.client_filters.is_empty() {
            lines.push("Client Filters:".to_string());
            for filter in &self.client_filters {
                lines.push(format!("  - {}", filter));
            }
        }

        if !self.warnings.is_empty() {
            lines.push("Warnings:".to_string());
            for warning in &self.warnings {
                lines.push(format!("  - {}", warning));
            }
        }

        lines.join("\n")
    }
}

/// Find a RediSearch index that covers the given key pattern.
///
/// # Arguments
/// * `conn` - Redis connection manager
/// * `pattern` - Key pattern (e.g., "user:*")
///
/// # Returns
/// `Some(DetectedIndex)` if a matching index is found, `None` otherwise.
pub async fn find_index_for_pattern(
    conn: &mut ConnectionManager,
    pattern: &str,
) -> Result<Option<DetectedIndex>> {
    // Get list of all indexes
    let indexes: Vec<String> = match redis::cmd("FT._LIST").query_async(conn).await {
        Ok(indexes) => indexes,
        Err(_) => return Ok(None), // RediSearch not available
    };

    // Extract prefix from pattern (e.g., "user:*" -> "user:")
    let pattern_prefix = pattern.strip_suffix('*').unwrap_or(pattern);

    for index_name in indexes {
        if let Ok(Some(index_info)) = get_index_info(conn, &index_name).await {
            // Check if any prefix matches our pattern
            for prefix in &index_info.prefixes {
                if pattern_prefix.starts_with(prefix) || prefix.starts_with(pattern_prefix) {
                    return Ok(Some(index_info));
                }
            }
        }
    }

    Ok(None)
}

/// Get detailed information about an index.
async fn get_index_info(
    conn: &mut ConnectionManager,
    index_name: &str,
) -> Result<Option<DetectedIndex>> {
    let info: Vec<redis::Value> = match redis::cmd("FT.INFO")
        .arg(index_name)
        .query_async(conn)
        .await
    {
        Ok(info) => info,
        Err(_) => return Ok(None),
    };

    // Parse the flat list response into a map
    let mut info_map = std::collections::HashMap::new();
    let mut i = 0;
    while i + 1 < info.len() {
        let key = match &info[i] {
            redis::Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
            redis::Value::SimpleString(s) => s.clone(),
            _ => {
                i += 2;
                continue;
            },
        };
        info_map.insert(key, info[i + 1].clone());
        i += 2;
    }

    // Get index definition
    let index_def = match info_map.get("index_definition") {
        Some(redis::Value::Array(arr)) => {
            let mut def_map = std::collections::HashMap::new();
            let mut j = 0;
            while j + 1 < arr.len() {
                let k = match &arr[j] {
                    redis::Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                    redis::Value::SimpleString(s) => s.clone(),
                    _ => {
                        j += 2;
                        continue;
                    },
                };
                def_map.insert(k, arr[j + 1].clone());
                j += 2;
            }
            def_map
        },
        _ => return Ok(None),
    };

    // Get prefixes
    let prefixes = match index_def.get("prefixes") {
        Some(redis::Value::Array(arr)) => arr
            .iter()
            .filter_map(|v| match v {
                redis::Value::BulkString(bytes) => Some(String::from_utf8_lossy(bytes).to_string()),
                redis::Value::SimpleString(s) => Some(s.clone()),
                _ => None,
            })
            .collect(),
        Some(redis::Value::BulkString(bytes)) => {
            vec![String::from_utf8_lossy(bytes).to_string()]
        },
        _ => Vec::new(),
    };

    // Get key_type
    let on_type = match index_def.get("key_type") {
        Some(redis::Value::BulkString(bytes)) => String::from_utf8_lossy(bytes).to_string(),
        Some(redis::Value::SimpleString(s)) => s.clone(),
        _ => "HASH".to_string(),
    };

    // Get field names from attributes
    let fields = match info_map.get("attributes") {
        Some(redis::Value::Array(arr)) => {
            let mut fields = Vec::new();
            for attr in arr {
                if let redis::Value::Array(attr_arr) = attr {
                    let mut j = 0;
                    while j + 1 < attr_arr.len() {
                        let k = match &attr_arr[j] {
                            redis::Value::BulkString(bytes) => {
                                String::from_utf8_lossy(bytes).to_string()
                            },
                            redis::Value::SimpleString(s) => s.clone(),
                            _ => {
                                j += 2;
                                continue;
                            },
                        };
                        if k == "identifier" {
                            if let redis::Value::BulkString(bytes) = &attr_arr[j + 1] {
                                fields.push(String::from_utf8_lossy(bytes).to_string());
                            } else if let redis::Value::SimpleString(s) = &attr_arr[j + 1] {
                                fields.push(s.clone());
                            }
                            break;
                        }
                        j += 2;
                    }
                }
            }
            fields
        },
        _ => Vec::new(),
    };

    Ok(Some(DetectedIndex {
        name: index_name.to_string(),
        prefixes,
        on_type,
        fields,
    }))
}

/// List all RediSearch indexes.
///
/// # Arguments
/// * `conn` - Redis connection manager
///
/// # Returns
/// A list of `DetectedIndex` for all available indexes.
pub async fn list_indexes(conn: &mut ConnectionManager) -> Result<Vec<DetectedIndex>> {
    let indexes: Vec<String> = match redis::cmd("FT._LIST").query_async(conn).await {
        Ok(indexes) => indexes,
        Err(_) => return Ok(Vec::new()), // RediSearch not available
    };

    let mut result = Vec::new();
    for index_name in indexes {
        if let Ok(Some(index_info)) = get_index_info(conn, &index_name).await {
            result.push(index_info);
        }
    }

    Ok(result)
}

/// Plan query execution for a given pattern.
///
/// # Arguments
/// * `conn` - Redis connection manager
/// * `pattern` - Key pattern (e.g., "user:*")
///
/// # Returns
/// A `QueryPlan` describing the optimal execution strategy.
pub async fn plan_query(conn: &mut ConnectionManager, pattern: &str) -> Result<QueryPlan> {
    let index_info = find_index_for_pattern(conn, pattern).await?;

    if let Some(index) = index_info {
        Ok(QueryPlan {
            strategy: ExecutionStrategy::Search,
            index: Some(index),
            server_query: Some("*".to_string()),
            client_filters: Vec::new(),
            warnings: Vec::new(),
        })
    } else {
        let mut plan = QueryPlan::new(ExecutionStrategy::Scan);
        plan.warnings.push(
            "No index found for pattern. All filtering will be done client-side.".to_string(),
        );
        Ok(plan)
    }
}

// ============================================================================
// Synchronous API
// ============================================================================

/// Explain what scan strategy would be used for a pattern.
///
/// This is a synchronous wrapper that returns a query plan without executing.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `pattern` - Key pattern (e.g., "user:*")
///
/// # Returns
/// A `QueryPlan` describing the optimal execution strategy.
///
/// # Example
///
/// ```ignore
/// let plan = explain_scan("redis://localhost:6379", "user:*")?;
/// println!("{}", plan.explain());
/// ```
pub fn explain_scan(url: &str, pattern: &str) -> Result<QueryPlan> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;
    let connection = RedisConnection::new(url)?;
    let mut conn = runtime.block_on(connection.get_connection_manager())?;

    runtime.block_on(plan_query(&mut conn, pattern))
}

/// Result of smart scan detection.
pub enum SmartScanResult {
    /// Using SCAN (no index found).
    Scan(HashBatchIterator),
    /// Using FT.SEARCH (index found).
    #[cfg(feature = "search")]
    Search(HashSearchIterator),
}

impl std::fmt::Debug for SmartScanResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SmartScanResult::Scan(_) => write!(f, "SmartScanResult::Scan(...)"),
            #[cfg(feature = "search")]
            SmartScanResult::Search(_) => write!(f, "SmartScanResult::Search(...)"),
        }
    }
}

impl SmartScanResult {
    /// Get the next batch from the iterator.
    pub fn next_batch(&mut self) -> Result<Option<arrow::array::RecordBatch>> {
        match self {
            SmartScanResult::Scan(iter) => iter.next_batch(),
            #[cfg(feature = "search")]
            SmartScanResult::Search(iter) => iter.next_batch(),
        }
    }

    /// Check if iteration is complete.
    pub fn is_done(&self) -> bool {
        match self {
            SmartScanResult::Scan(iter) => iter.is_done(),
            #[cfg(feature = "search")]
            SmartScanResult::Search(iter) => iter.is_done(),
        }
    }

    /// Get the number of rows yielded so far.
    pub fn rows_yielded(&self) -> usize {
        match self {
            SmartScanResult::Scan(iter) => iter.rows_yielded(),
            #[cfg(feature = "search")]
            SmartScanResult::Search(iter) => iter.rows_yielded(),
        }
    }

    /// Get the execution strategy being used.
    pub fn strategy(&self) -> ExecutionStrategy {
        match self {
            SmartScanResult::Scan(_) => ExecutionStrategy::Scan,
            #[cfg(feature = "search")]
            SmartScanResult::Search(_) => ExecutionStrategy::Search,
        }
    }
}

/// Smart scan for hashes that auto-detects and uses RediSearch if available.
///
/// This function checks if a RediSearch index exists for the given pattern.
/// If found, it uses FT.SEARCH for efficient server-side filtering.
/// Otherwise, it falls back to SCAN.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `schema` - Hash schema
/// * `config` - Batch configuration with pattern
/// * `projection` - Optional list of fields to fetch
///
/// # Returns
/// A `SmartScanResult` that can be used to iterate over batches.
///
/// # Example
///
/// ```ignore
/// let schema = HashSchema::new(vec![
///     ("name".to_string(), RedisType::Utf8),
///     ("age".to_string(), RedisType::Int64),
/// ]);
/// let config = BatchConfig::new("user:*");
///
/// let mut iter = smart_scan_hashes("redis://localhost:6379", schema, config, None)?;
/// while let Some(batch) = iter.next_batch()? {
///     println!("Got {} rows using {:?}", batch.num_rows(), iter.strategy());
/// }
/// ```
pub fn smart_scan_hashes(
    url: &str,
    schema: HashSchema,
    config: BatchConfig,
    projection: Option<Vec<String>>,
) -> Result<SmartScanResult> {
    // First, check if an index exists
    let plan = explain_scan(url, &config.pattern)?;

    match plan.strategy {
        #[cfg(feature = "search")]
        ExecutionStrategy::Search if plan.index.is_some() => {
            let index = plan.index.unwrap();

            // Convert BatchConfig to SearchBatchConfig
            let search_config =
                SearchBatchConfig::new(&index.name, "*").with_batch_size(config.batch_size);

            let iter = HashSearchIterator::new(url, schema, search_config, projection)?;
            Ok(SmartScanResult::Search(iter))
        },
        _ => {
            // Fall back to SCAN
            let iter = HashBatchIterator::new(url, schema, config, projection)?;
            Ok(SmartScanResult::Scan(iter))
        },
    }
}

/// Smart scan with query plan - returns both iterator and plan.
///
/// This is useful when you want to know what strategy was chosen.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `schema` - Hash schema
/// * `config` - Batch configuration with pattern
/// * `projection` - Optional list of fields to fetch
///
/// # Returns
/// A tuple of (SmartScanResult, QueryPlan).
pub fn smart_scan_hashes_with_plan(
    url: &str,
    schema: HashSchema,
    config: BatchConfig,
    projection: Option<Vec<String>>,
) -> Result<(SmartScanResult, QueryPlan)> {
    let plan = explain_scan(url, &config.pattern)?;
    let iter = smart_scan_hashes(url, schema, config, projection)?;
    Ok((iter, plan))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_strategy_display() {
        assert_eq!(format!("{}", ExecutionStrategy::Search), "SEARCH");
        assert_eq!(format!("{}", ExecutionStrategy::Scan), "SCAN");
        assert_eq!(format!("{}", ExecutionStrategy::Hybrid), "HYBRID");
    }

    #[test]
    fn test_query_plan_explain() {
        let plan = QueryPlan {
            strategy: ExecutionStrategy::Search,
            index: Some(DetectedIndex {
                name: "users_idx".to_string(),
                prefixes: vec!["user:".to_string()],
                on_type: "HASH".to_string(),
                fields: vec!["name".to_string(), "age".to_string()],
            }),
            server_query: Some("*".to_string()),
            client_filters: Vec::new(),
            warnings: Vec::new(),
        };

        let explanation = plan.explain();
        assert!(explanation.contains("Strategy: SEARCH"));
        assert!(explanation.contains("Index: users_idx"));
        assert!(explanation.contains("user:"));
    }

    #[test]
    fn test_query_plan_with_warnings() {
        let mut plan = QueryPlan::new(ExecutionStrategy::Scan);
        plan.warnings.push("No index found".to_string());
        plan.client_filters.push("age > 30".to_string());

        let explanation = plan.explain();
        assert!(explanation.contains("Strategy: SCAN"));
        assert!(explanation.contains("Warnings:"));
        assert!(explanation.contains("No index found"));
        assert!(explanation.contains("Client Filters:"));
        assert!(explanation.contains("age > 30"));
    }

    #[test]
    fn test_detected_index_clone() {
        let index = DetectedIndex {
            name: "test_idx".to_string(),
            prefixes: vec!["test:".to_string()],
            on_type: "HASH".to_string(),
            fields: vec!["field1".to_string()],
        };

        let cloned = index.clone();
        assert_eq!(cloned.name, index.name);
        assert_eq!(cloned.prefixes, index.prefixes);
    }
}
