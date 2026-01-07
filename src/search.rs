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

/// Options for SUMMARIZE in FT.SEARCH results.
///
/// SUMMARIZE returns a snippet of the matching text with the search terms
/// highlighted, useful for search result previews.
#[derive(Debug, Clone, Default)]
pub struct SummarizeOptions {
    /// Fields to summarize (None = all TEXT fields).
    pub fields: Option<Vec<String>>,
    /// Number of fragments to return per field.
    pub frags: Option<usize>,
    /// Length of each fragment in words.
    pub len: Option<usize>,
    /// Separator between fragments.
    pub separator: Option<String>,
}

impl SummarizeOptions {
    /// Create new summarize options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set fields to summarize.
    pub fn with_fields(mut self, fields: Vec<String>) -> Self {
        self.fields = Some(fields);
        self
    }

    /// Set number of fragments per field.
    pub fn with_frags(mut self, frags: usize) -> Self {
        self.frags = Some(frags);
        self
    }

    /// Set fragment length in words.
    pub fn with_len(mut self, len: usize) -> Self {
        self.len = Some(len);
        self
    }

    /// Set separator between fragments.
    pub fn with_separator(mut self, separator: impl Into<String>) -> Self {
        self.separator = Some(separator.into());
        self
    }
}

/// Options for HIGHLIGHT in FT.SEARCH results.
///
/// HIGHLIGHT wraps matching terms with open/close tags for display.
#[derive(Debug, Clone)]
pub struct HighlightOptions {
    /// Fields to highlight (None = all TEXT fields).
    pub fields: Option<Vec<String>>,
    /// Opening tag for highlighted terms.
    pub open_tag: String,
    /// Closing tag for highlighted terms.
    pub close_tag: String,
}

impl Default for HighlightOptions {
    fn default() -> Self {
        Self {
            fields: None,
            open_tag: "<b>".to_string(),
            close_tag: "</b>".to_string(),
        }
    }
}

impl HighlightOptions {
    /// Create new highlight options with default tags.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set fields to highlight.
    pub fn with_fields(mut self, fields: Vec<String>) -> Self {
        self.fields = Some(fields);
        self
    }

    /// Set opening tag for highlighted terms.
    pub fn with_open_tag(mut self, tag: impl Into<String>) -> Self {
        self.open_tag = tag.into();
        self
    }

    /// Set closing tag for highlighted terms.
    pub fn with_close_tag(mut self, tag: impl Into<String>) -> Self {
        self.close_tag = tag.into();
        self
    }

    /// Set both open and close tags.
    pub fn with_tags(mut self, open: impl Into<String>, close: impl Into<String>) -> Self {
        self.open_tag = open.into();
        self.close_tag = close.into();
        self
    }
}

/// Configuration for RediSearch FT.SEARCH queries.
///
/// This struct provides comprehensive configuration for FT.SEARCH including
/// query modifiers, result formatting, and advanced options.
///
/// # Example
///
/// ```ignore
/// use polars_redis::search::{SearchConfig, HighlightOptions};
///
/// let config = SearchConfig::new("articles_idx", "python programming")
///     .with_verbatim(true)  // Exact match, no stemming
///     .with_language("english")
///     .with_highlight(HighlightOptions::new().with_tags("<em>", "</em>"))
///     .with_limit(100);
/// ```
#[derive(Debug, Clone, Default)]
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
    /// Whether to include relevance scores.
    pub withscores: bool,
    /// Disable stemming (exact term matching).
    pub verbatim: bool,
    /// Include stop words in the query.
    pub nostopwords: bool,
    /// Language for stemming.
    pub language: Option<String>,
    /// Custom scorer function (e.g., "BM25", "TFIDF", "DISMAX").
    pub scorer: Option<String>,
    /// Limit search to specific document keys.
    pub inkeys: Option<Vec<String>>,
    /// Limit search to specific fields.
    pub infields: Option<Vec<String>>,
    /// Query timeout in milliseconds.
    pub timeout: Option<u64>,
    /// RediSearch dialect version (1, 2, 3, or 4).
    pub dialect: Option<u8>,
    /// Additional PARAMS for parameterized queries.
    pub params: Option<HashMap<String, Vec<u8>>>,
    /// Options for generating text snippets.
    pub summarize: Option<SummarizeOptions>,
    /// Options for highlighting matching terms.
    pub highlight: Option<HighlightOptions>,
    /// Default slop for phrase queries.
    pub slop: Option<usize>,
    /// Whether phrase terms must appear in order.
    pub inorder: Option<bool>,
    /// Query expander to use.
    pub expander: Option<String>,
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
            ..Default::default()
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

    /// Include relevance scores in results.
    pub fn with_scores(mut self, withscores: bool) -> Self {
        self.withscores = withscores;
        self
    }

    /// Disable stemming for exact term matching.
    pub fn with_verbatim(mut self, verbatim: bool) -> Self {
        self.verbatim = verbatim;
        self
    }

    /// Include stop words in the query.
    pub fn with_nostopwords(mut self, nostopwords: bool) -> Self {
        self.nostopwords = nostopwords;
        self
    }

    /// Set the language for stemming.
    pub fn with_language(mut self, language: impl Into<String>) -> Self {
        self.language = Some(language.into());
        self
    }

    /// Set the scoring function (e.g., "BM25", "TFIDF", "DISMAX").
    pub fn with_scorer(mut self, scorer: impl Into<String>) -> Self {
        self.scorer = Some(scorer.into());
        self
    }

    /// Limit search to specific document keys.
    pub fn with_inkeys(mut self, keys: Vec<String>) -> Self {
        self.inkeys = Some(keys);
        self
    }

    /// Limit search to specific fields.
    pub fn with_infields(mut self, fields: Vec<String>) -> Self {
        self.infields = Some(fields);
        self
    }

    /// Set query timeout in milliseconds.
    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.timeout = Some(timeout_ms);
        self
    }

    /// Set RediSearch dialect version (1, 2, 3, or 4).
    pub fn with_dialect(mut self, dialect: u8) -> Self {
        self.dialect = Some(dialect);
        self
    }

    /// Set additional PARAMS for parameterized queries.
    pub fn with_params(mut self, params: HashMap<String, Vec<u8>>) -> Self {
        self.params = Some(params);
        self
    }

    /// Configure text summarization for search results.
    pub fn with_summarize(mut self, options: SummarizeOptions) -> Self {
        self.summarize = Some(options);
        self
    }

    /// Configure term highlighting for search results.
    pub fn with_highlight(mut self, options: HighlightOptions) -> Self {
        self.highlight = Some(options);
        self
    }

    /// Set default slop for phrase queries.
    pub fn with_slop(mut self, slop: usize) -> Self {
        self.slop = Some(slop);
        self
    }

    /// Set whether phrase terms must appear in order.
    pub fn with_inorder(mut self, inorder: bool) -> Self {
        self.inorder = Some(inorder);
        self
    }

    /// Set the query expander to use.
    pub fn with_expander(mut self, expander: impl Into<String>) -> Self {
        self.expander = Some(expander.into());
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

// ============================================================================
// FT.AGGREGATE Support
// ============================================================================

/// Reduce operation for FT.AGGREGATE.
///
/// Represents a REDUCE clause like `REDUCE AVG 1 @age AS avg_age`.
#[derive(Debug, Clone)]
pub struct ReduceOp {
    /// Reduce function name (COUNT, SUM, AVG, MIN, MAX, etc.)
    pub function: String,
    /// Arguments to the reduce function (field names without @)
    pub args: Vec<String>,
    /// Alias for the result
    pub alias: String,
}

impl ReduceOp {
    /// Create a new reduce operation.
    ///
    /// # Arguments
    /// * `function` - The reduce function (e.g., "COUNT", "AVG", "SUM")
    /// * `args` - Field names to aggregate (empty for COUNT)
    /// * `alias` - Output field name
    pub fn new(
        function: impl Into<String>,
        args: Vec<impl Into<String>>,
        alias: impl Into<String>,
    ) -> Self {
        Self {
            function: function.into(),
            args: args.into_iter().map(|a| a.into()).collect(),
            alias: alias.into(),
        }
    }

    /// Create COUNT(*) operation.
    pub fn count(alias: impl Into<String>) -> Self {
        Self::new("COUNT", Vec::<String>::new(), alias)
    }

    /// Create COUNT_DISTINCT operation.
    pub fn count_distinct(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new("COUNT_DISTINCT", vec![field.into()], alias)
    }

    /// Create SUM operation.
    pub fn sum(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new("SUM", vec![field.into()], alias)
    }

    /// Create AVG operation.
    pub fn avg(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new("AVG", vec![field.into()], alias)
    }

    /// Create MIN operation.
    pub fn min(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new("MIN", vec![field.into()], alias)
    }

    /// Create MAX operation.
    pub fn max(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new("MAX", vec![field.into()], alias)
    }

    /// Create FIRST_VALUE operation.
    pub fn first(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new("FIRST_VALUE", vec![field.into()], alias)
    }

    /// Create TOLIST operation (collect values into a list).
    pub fn to_list(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new("TOLIST", vec![field.into()], alias)
    }

    /// Create QUANTILE operation.
    pub fn quantile(field: impl Into<String>, quantile: f64, alias: impl Into<String>) -> Self {
        Self::new("QUANTILE", vec![field.into(), quantile.to_string()], alias)
    }

    /// Create STDDEV operation.
    pub fn stddev(field: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::new("STDDEV", vec![field.into()], alias)
    }
}

/// Apply expression for computed fields.
///
/// Represents an APPLY clause like `APPLY "upper(@name)" AS upper_name`.
#[derive(Debug, Clone)]
pub struct ApplyExpr {
    /// The expression to apply (e.g., "upper(@name)", "@price * @quantity")
    pub expression: String,
    /// Alias for the result
    pub alias: String,
}

impl ApplyExpr {
    /// Create a new apply expression.
    pub fn new(expression: impl Into<String>, alias: impl Into<String>) -> Self {
        Self {
            expression: expression.into(),
            alias: alias.into(),
        }
    }
}

/// Sort specification for aggregation results.
#[derive(Debug, Clone)]
pub struct SortBy {
    /// Field to sort by
    pub field: String,
    /// Sort direction (true = ascending, false = descending)
    pub ascending: bool,
}

impl SortBy {
    /// Create ascending sort.
    pub fn asc(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            ascending: true,
        }
    }

    /// Create descending sort.
    pub fn desc(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            ascending: false,
        }
    }
}

/// Configuration for RediSearch FT.AGGREGATE queries.
///
/// # Example
///
/// ```ignore
/// use polars_redis::search::{AggregateConfig, ReduceOp, SortBy};
///
/// let config = AggregateConfig::new("users_idx", "*")
///     .with_group_by(vec!["city"])
///     .with_reduce(vec![
///         ReduceOp::count("user_count"),
///         ReduceOp::avg("age", "avg_age"),
///     ])
///     .with_sort_by(vec![SortBy::desc("user_count")])
///     .with_limit(10);
/// ```
#[derive(Debug, Clone)]
pub struct AggregateConfig {
    /// RediSearch index name.
    pub index: String,
    /// Query string (e.g., "@status:active", "*" for all).
    pub query: String,
    /// Fields to group by.
    pub group_by: Vec<String>,
    /// Reduce operations.
    pub reduce: Vec<ReduceOp>,
    /// Apply expressions for computed fields.
    pub apply: Vec<ApplyExpr>,
    /// Post-aggregation filter expression.
    pub filter: Option<String>,
    /// Sort specifications.
    pub sort_by: Vec<SortBy>,
    /// Maximum results to return.
    pub limit: Option<usize>,
    /// Offset for pagination.
    pub offset: usize,
    /// Load additional fields from the document.
    pub load: Vec<String>,
}

impl AggregateConfig {
    /// Create a new AggregateConfig.
    ///
    /// # Arguments
    /// * `index` - The RediSearch index name
    /// * `query` - The search query (e.g., "@field:value", "*" for all)
    pub fn new(index: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            index: index.into(),
            query: query.into(),
            group_by: Vec::new(),
            reduce: Vec::new(),
            apply: Vec::new(),
            filter: None,
            sort_by: Vec::new(),
            limit: None,
            offset: 0,
            load: Vec::new(),
        }
    }

    /// Set fields to group by.
    pub fn with_group_by(mut self, fields: Vec<impl Into<String>>) -> Self {
        self.group_by = fields.into_iter().map(|f| f.into()).collect();
        self
    }

    /// Add a reduce operation.
    pub fn with_reduce(mut self, ops: Vec<ReduceOp>) -> Self {
        self.reduce = ops;
        self
    }

    /// Add an apply expression.
    pub fn with_apply(mut self, exprs: Vec<ApplyExpr>) -> Self {
        self.apply = exprs;
        self
    }

    /// Set a post-aggregation filter.
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Set sort specifications.
    pub fn with_sort_by(mut self, sorts: Vec<SortBy>) -> Self {
        self.sort_by = sorts;
        self
    }

    /// Set the maximum number of results.
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the offset for pagination.
    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    /// Set fields to load from documents.
    pub fn with_load(mut self, fields: Vec<impl Into<String>>) -> Self {
        self.load = fields.into_iter().map(|f| f.into()).collect();
        self
    }
}

/// Result of an FT.AGGREGATE query.
#[derive(Debug)]
pub struct AggregateResult {
    /// Aggregated rows (each row is a map of field -> value).
    pub rows: Vec<HashMap<String, String>>,
}

/// Execute FT.AGGREGATE and return aggregated results.
///
/// # Arguments
/// * `conn` - Redis connection manager
/// * `config` - Aggregate configuration
///
/// # Returns
/// An `AggregateResult` containing the aggregated rows.
///
/// # Example
///
/// ```ignore
/// use polars_redis::search::{AggregateConfig, ReduceOp, aggregate};
///
/// let config = AggregateConfig::new("users_idx", "*")
///     .with_group_by(vec!["city"])
///     .with_reduce(vec![
///         ReduceOp::count("user_count"),
///         ReduceOp::avg("age", "avg_age"),
///     ]);
///
/// let result = aggregate(&mut conn, &config).await?;
/// for row in result.rows {
///     println!("{:?}", row);
/// }
/// ```
pub async fn aggregate(
    conn: &mut ConnectionManager,
    config: &AggregateConfig,
) -> Result<AggregateResult> {
    let mut cmd = redis::cmd("FT.AGGREGATE");
    cmd.arg(&config.index).arg(&config.query);

    // Helper to normalize field names: strip @ if present, then add it back
    fn field_ref(field: &str) -> String {
        let name = field.strip_prefix('@').unwrap_or(field);
        format!("@{}", name)
    }

    // LOAD clause
    if !config.load.is_empty() {
        cmd.arg("LOAD").arg(config.load.len());
        for field in &config.load {
            cmd.arg(field_ref(field));
        }
    }

    // GROUPBY clause
    // Note: REDUCE can only be used after GROUPBY, so if we have reduce ops
    // but no group_by fields, use GROUPBY 0 for global aggregation
    if !config.group_by.is_empty() || !config.reduce.is_empty() {
        cmd.arg("GROUPBY").arg(config.group_by.len());
        for field in &config.group_by {
            cmd.arg(field_ref(field));
        }

        // REDUCE clauses (must follow GROUPBY)
        for reduce in &config.reduce {
            cmd.arg("REDUCE")
                .arg(&reduce.function)
                .arg(reduce.args.len());
            for arg in &reduce.args {
                // Check if arg looks like a number (for QUANTILE etc.)
                if arg.parse::<f64>().is_ok() {
                    cmd.arg(arg);
                } else {
                    cmd.arg(field_ref(arg));
                }
            }
            cmd.arg("AS").arg(&reduce.alias);
        }
    }

    // APPLY clauses
    for apply in &config.apply {
        cmd.arg("APPLY")
            .arg(&apply.expression)
            .arg("AS")
            .arg(&apply.alias);
    }

    // FILTER clause
    if let Some(filter) = &config.filter {
        cmd.arg("FILTER").arg(filter);
    }

    // SORTBY clause
    if !config.sort_by.is_empty() {
        cmd.arg("SORTBY").arg(config.sort_by.len() * 2);
        for sort in &config.sort_by {
            cmd.arg(field_ref(&sort.field));
            cmd.arg(if sort.ascending { "ASC" } else { "DESC" });
        }
    }

    // LIMIT clause
    if let Some(limit) = config.limit {
        cmd.arg("LIMIT").arg(config.offset).arg(limit);
    }

    // Execute query
    let result: redis::Value = cmd.query_async(conn).await?;

    // Parse response
    parse_aggregate_response(result)
}

/// Parse FT.AGGREGATE response into AggregateResult.
///
/// FT.AGGREGATE returns:
/// ```text
/// 1) (integer) num_results (note: this is not reliable for aggregates)
/// 2) ["field1", "value1", "field2", "value2", ...]
/// 3) ["field1", "value1", "field2", "value2", ...]
/// ...
/// ```
fn parse_aggregate_response(value: redis::Value) -> Result<AggregateResult> {
    match value {
        redis::Value::Array(arr) if arr.len() > 1 => {
            let mut rows = Vec::new();

            // Skip first element (result count, not reliable for aggregates)
            for item in arr.into_iter().skip(1) {
                if let redis::Value::Array(field_arr) = item {
                    let row = parse_aggregate_row(&field_arr);
                    if !row.is_empty() {
                        rows.push(row);
                    }
                }
            }

            Ok(AggregateResult { rows })
        }
        _ => Ok(AggregateResult { rows: Vec::new() }),
    }
}

/// Parse a single row from FT.AGGREGATE response.
fn parse_aggregate_row(arr: &[redis::Value]) -> HashMap<String, String> {
    let mut row = HashMap::new();
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
            redis::Value::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
            redis::Value::SimpleString(s) => s.clone(),
            redis::Value::Int(n) => n.to_string(),
            redis::Value::Double(f) => f.to_string(),
            _ => String::new(),
        };

        row.insert(field_name, field_value);
        i += 2;
    }

    row
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

    // FT.AGGREGATE tests

    #[test]
    fn test_reduce_op_helpers() {
        let count = ReduceOp::count("total");
        assert_eq!(count.function, "COUNT");
        assert!(count.args.is_empty());
        assert_eq!(count.alias, "total");

        let avg = ReduceOp::avg("age", "avg_age");
        assert_eq!(avg.function, "AVG");
        assert_eq!(avg.args, vec!["age"]);
        assert_eq!(avg.alias, "avg_age");

        let sum = ReduceOp::sum("amount", "total_amount");
        assert_eq!(sum.function, "SUM");
        assert_eq!(sum.args, vec!["amount"]);

        let quantile = ReduceOp::quantile("score", 0.95, "p95");
        assert_eq!(quantile.function, "QUANTILE");
        assert_eq!(quantile.args, vec!["score", "0.95"]);
    }

    #[test]
    fn test_sort_by() {
        let asc = SortBy::asc("name");
        assert_eq!(asc.field, "name");
        assert!(asc.ascending);

        let desc = SortBy::desc("count");
        assert_eq!(desc.field, "count");
        assert!(!desc.ascending);
    }

    #[test]
    fn test_aggregate_config_builder() {
        let config = AggregateConfig::new("users_idx", "@status:active")
            .with_group_by(vec!["city", "country"])
            .with_reduce(vec![
                ReduceOp::count("user_count"),
                ReduceOp::avg("age", "avg_age"),
            ])
            .with_sort_by(vec![SortBy::desc("user_count")])
            .with_limit(10)
            .with_offset(5);

        assert_eq!(config.index, "users_idx");
        assert_eq!(config.query, "@status:active");
        assert_eq!(config.group_by, vec!["city", "country"]);
        assert_eq!(config.reduce.len(), 2);
        assert_eq!(config.sort_by.len(), 1);
        assert_eq!(config.limit, Some(10));
        assert_eq!(config.offset, 5);
    }

    #[test]
    fn test_aggregate_config_defaults() {
        let config = AggregateConfig::new("idx", "*");

        assert_eq!(config.index, "idx");
        assert_eq!(config.query, "*");
        assert!(config.group_by.is_empty());
        assert!(config.reduce.is_empty());
        assert!(config.apply.is_empty());
        assert!(config.filter.is_none());
        assert!(config.sort_by.is_empty());
        assert_eq!(config.limit, None);
        assert_eq!(config.offset, 0);
    }

    #[test]
    fn test_aggregate_config_with_apply() {
        let config = AggregateConfig::new("idx", "*").with_apply(vec![
            ApplyExpr::new("upper(@name)", "upper_name"),
            ApplyExpr::new("@price * @quantity", "total"),
        ]);

        assert_eq!(config.apply.len(), 2);
        assert_eq!(config.apply[0].expression, "upper(@name)");
        assert_eq!(config.apply[0].alias, "upper_name");
    }

    #[test]
    fn test_aggregate_config_with_filter() {
        let config = AggregateConfig::new("idx", "*")
            .with_group_by(vec!["city"])
            .with_reduce(vec![ReduceOp::count("cnt")])
            .with_filter("@cnt > 10");

        assert_eq!(config.filter, Some("@cnt > 10".to_string()));
    }

    #[test]
    fn test_parse_aggregate_empty_response() {
        let result = parse_aggregate_response(redis::Value::Array(vec![redis::Value::Int(0)]));
        assert!(result.is_ok());
        let agg_result = result.unwrap();
        assert!(agg_result.rows.is_empty());
    }

    #[test]
    fn test_parse_aggregate_row() {
        let arr = vec![
            redis::Value::BulkString(b"city".to_vec()),
            redis::Value::BulkString(b"New York".to_vec()),
            redis::Value::BulkString(b"user_count".to_vec()),
            redis::Value::BulkString(b"150".to_vec()),
            redis::Value::BulkString(b"avg_age".to_vec()),
            redis::Value::BulkString(b"32.5".to_vec()),
        ];

        let row = parse_aggregate_row(&arr);
        assert_eq!(row.get("city"), Some(&"New York".to_string()));
        assert_eq!(row.get("user_count"), Some(&"150".to_string()));
        assert_eq!(row.get("avg_age"), Some(&"32.5".to_string()));
    }

    #[test]
    fn test_parse_aggregate_response_with_rows() {
        let response = redis::Value::Array(vec![
            redis::Value::Int(2), // count (not reliable)
            redis::Value::Array(vec![
                redis::Value::BulkString(b"city".to_vec()),
                redis::Value::BulkString(b"NYC".to_vec()),
                redis::Value::BulkString(b"count".to_vec()),
                redis::Value::BulkString(b"100".to_vec()),
            ]),
            redis::Value::Array(vec![
                redis::Value::BulkString(b"city".to_vec()),
                redis::Value::BulkString(b"LA".to_vec()),
                redis::Value::BulkString(b"count".to_vec()),
                redis::Value::BulkString(b"80".to_vec()),
            ]),
        ]);

        let result = parse_aggregate_response(response).unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].get("city"), Some(&"NYC".to_string()));
        assert_eq!(result.rows[0].get("count"), Some(&"100".to_string()));
        assert_eq!(result.rows[1].get("city"), Some(&"LA".to_string()));
    }
}
