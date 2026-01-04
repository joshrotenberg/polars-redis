//! Configuration options for Redis operations.
//!
//! This module provides unified configuration structs following the polars-io pattern:
//! - Builder pattern with `with_*` methods
//! - Sensible defaults via `Default` trait
//! - Environment variable overrides for common settings
//!
//! # Example
//!
//! ```ignore
//! use polars_redis::options::{ScanOptions, KeyColumn, TtlColumn};
//!
//! let scan = ScanOptions::new("user:*")
//!     .with_batch_size(500)
//!     .with_n_rows(10000);
//!
//! let key = KeyColumn::enabled().with_name("redis_key");
//! let ttl = TtlColumn::enabled();
//! ```

use std::sync::LazyLock;

/// Row index configuration, matching polars-io pattern.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RowIndex {
    /// Column name for the row index.
    pub name: String,
    /// Starting offset for the index.
    pub offset: u64,
}

impl Default for RowIndex {
    fn default() -> Self {
        Self {
            name: "_index".to_string(),
            offset: 0,
        }
    }
}

impl RowIndex {
    /// Create a new RowIndex with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            offset: 0,
        }
    }

    /// Set the starting offset.
    pub fn with_offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self
    }
}

/// Common scan options shared across all Redis data types.
#[derive(Debug, Clone)]
pub struct ScanOptions {
    /// Key pattern to match (e.g., "user:*").
    pub pattern: String,
    /// Number of keys to process per batch.
    pub batch_size: usize,
    /// SCAN COUNT hint for Redis.
    pub count_hint: usize,
    /// Maximum total rows to return (None for unlimited).
    pub n_rows: Option<usize>,
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            pattern: "*".to_string(),
            batch_size: get_default_batch_size(),
            count_hint: get_default_count_hint(),
            n_rows: None,
        }
    }
}

impl ScanOptions {
    /// Create new ScanOptions with the given pattern.
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            pattern: pattern.into(),
            ..Default::default()
        }
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set the COUNT hint for SCAN.
    pub fn with_count_hint(mut self, count: usize) -> Self {
        self.count_hint = count;
        self
    }

    /// Set the maximum number of rows to return.
    pub fn with_n_rows(mut self, n: usize) -> Self {
        self.n_rows = Some(n);
        self
    }
}

/// Key column configuration.
#[derive(Debug, Clone)]
pub struct KeyColumn {
    /// Whether to include the Redis key as a column.
    pub enabled: bool,
    /// Column name for the key.
    pub name: String,
}

impl Default for KeyColumn {
    fn default() -> Self {
        Self {
            enabled: true,
            name: "_key".to_string(),
        }
    }
}

impl KeyColumn {
    /// Create enabled key column with default name.
    pub fn enabled() -> Self {
        Self::default()
    }

    /// Create disabled key column.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            name: "_key".to_string(),
        }
    }

    /// Set the column name.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }
}

/// TTL column configuration.
#[derive(Debug, Clone)]
pub struct TtlColumn {
    /// Whether to include the TTL as a column.
    pub enabled: bool,
    /// Column name for the TTL.
    pub name: String,
}

impl Default for TtlColumn {
    fn default() -> Self {
        Self {
            enabled: false,
            name: "_ttl".to_string(),
        }
    }
}

impl TtlColumn {
    /// Create enabled TTL column with default name.
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            name: "_ttl".to_string(),
        }
    }

    /// Create disabled TTL column.
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Set the column name.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }
}

/// Row index column configuration.
#[derive(Debug, Clone)]
pub struct RowIndexColumn {
    /// Whether to include the row index as a column.
    pub enabled: bool,
    /// Column name for the row index.
    pub name: String,
    /// Starting offset for the index.
    pub offset: u64,
}

impl Default for RowIndexColumn {
    fn default() -> Self {
        Self {
            enabled: false,
            name: "_index".to_string(),
            offset: 0,
        }
    }
}

impl RowIndexColumn {
    /// Create enabled row index column with default name.
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            name: "_index".to_string(),
            offset: 0,
        }
    }

    /// Create disabled row index column.
    pub fn disabled() -> Self {
        Self::default()
    }

    /// Set the column name.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Set the starting offset.
    pub fn with_offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self
    }

    /// Convert to RowIndex if enabled.
    pub fn to_row_index(&self) -> Option<RowIndex> {
        if self.enabled {
            Some(RowIndex {
                name: self.name.clone(),
                offset: self.offset,
            })
        } else {
            None
        }
    }
}

// ============================================================================
// Type-specific scan options (following polars-io pattern)
// ============================================================================

/// Options for scanning Redis hashes.
///
/// # Example
///
/// ```ignore
/// use polars_redis::options::HashScanOptions;
///
/// let opts = HashScanOptions::new("user:*")
///     .with_batch_size(500)
///     .with_ttl(true)
///     .with_projection(vec!["name", "email"]);
/// ```
#[derive(Debug, Clone)]
pub struct HashScanOptions {
    /// Base scan options.
    pub scan: ScanOptions,
    /// Whether to include the key column.
    pub include_key: bool,
    /// Custom name for the key column.
    pub key_column_name: Option<String>,
    /// Whether to include TTL.
    pub include_ttl: bool,
    /// Custom name for the TTL column.
    pub ttl_column_name: Option<String>,
    /// Row index configuration.
    pub row_index: Option<RowIndex>,
    /// Fields to fetch (None = all via HGETALL).
    pub projection: Option<Vec<String>>,
}

impl Default for HashScanOptions {
    fn default() -> Self {
        Self {
            scan: ScanOptions::default(),
            include_key: true,
            key_column_name: None,
            include_ttl: false,
            ttl_column_name: None,
            row_index: None,
            projection: None,
        }
    }
}

impl HashScanOptions {
    /// Create new HashScanOptions with the given pattern.
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            scan: ScanOptions::new(pattern),
            ..Default::default()
        }
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.scan.batch_size = size;
        self
    }

    /// Set the COUNT hint for SCAN.
    pub fn with_count_hint(mut self, count: usize) -> Self {
        self.scan.count_hint = count;
        self
    }

    /// Set the maximum number of rows to return.
    pub fn with_n_rows(mut self, n: usize) -> Self {
        self.scan.n_rows = Some(n);
        self
    }

    /// Enable or disable the key column.
    pub fn with_key(mut self, include: bool) -> Self {
        self.include_key = include;
        self
    }

    /// Set a custom name for the key column.
    pub fn with_key_column_name(mut self, name: impl Into<String>) -> Self {
        self.key_column_name = Some(name.into());
        self
    }

    /// Enable or disable the TTL column.
    pub fn with_ttl(mut self, include: bool) -> Self {
        self.include_ttl = include;
        self
    }

    /// Set a custom name for the TTL column.
    pub fn with_ttl_column_name(mut self, name: impl Into<String>) -> Self {
        self.ttl_column_name = Some(name.into());
        self
    }

    /// Set the row index configuration.
    pub fn with_row_index(mut self, name: impl Into<String>, offset: u64) -> Self {
        self.row_index = Some(RowIndex {
            name: name.into(),
            offset,
        });
        self
    }

    /// Set the fields to fetch (projection).
    pub fn with_projection(mut self, fields: Vec<impl Into<String>>) -> Self {
        self.projection = Some(fields.into_iter().map(Into::into).collect());
        self
    }
}

/// Options for scanning Redis JSON documents.
///
/// # Example
///
/// ```ignore
/// use polars_redis::options::JsonScanOptions;
///
/// let opts = JsonScanOptions::new("doc:*")
///     .with_batch_size(500)
///     .with_path("$.user");
/// ```
#[derive(Debug, Clone)]
pub struct JsonScanOptions {
    /// Base scan options.
    pub scan: ScanOptions,
    /// Whether to include the key column.
    pub include_key: bool,
    /// Custom name for the key column.
    pub key_column_name: Option<String>,
    /// Whether to include TTL.
    pub include_ttl: bool,
    /// Custom name for the TTL column.
    pub ttl_column_name: Option<String>,
    /// Row index configuration.
    pub row_index: Option<RowIndex>,
    /// JSON path to extract (None = root "$").
    pub path: Option<String>,
    /// Fields to fetch from the JSON document.
    pub projection: Option<Vec<String>>,
}

impl Default for JsonScanOptions {
    fn default() -> Self {
        Self {
            scan: ScanOptions::default(),
            include_key: true,
            key_column_name: None,
            include_ttl: false,
            ttl_column_name: None,
            row_index: None,
            path: None,
            projection: None,
        }
    }
}

impl JsonScanOptions {
    /// Create new JsonScanOptions with the given pattern.
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            scan: ScanOptions::new(pattern),
            ..Default::default()
        }
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.scan.batch_size = size;
        self
    }

    /// Set the COUNT hint for SCAN.
    pub fn with_count_hint(mut self, count: usize) -> Self {
        self.scan.count_hint = count;
        self
    }

    /// Set the maximum number of rows to return.
    pub fn with_n_rows(mut self, n: usize) -> Self {
        self.scan.n_rows = Some(n);
        self
    }

    /// Enable or disable the key column.
    pub fn with_key(mut self, include: bool) -> Self {
        self.include_key = include;
        self
    }

    /// Set a custom name for the key column.
    pub fn with_key_column_name(mut self, name: impl Into<String>) -> Self {
        self.key_column_name = Some(name.into());
        self
    }

    /// Enable or disable the TTL column.
    pub fn with_ttl(mut self, include: bool) -> Self {
        self.include_ttl = include;
        self
    }

    /// Set a custom name for the TTL column.
    pub fn with_ttl_column_name(mut self, name: impl Into<String>) -> Self {
        self.ttl_column_name = Some(name.into());
        self
    }

    /// Set the row index configuration.
    pub fn with_row_index(mut self, name: impl Into<String>, offset: u64) -> Self {
        self.row_index = Some(RowIndex {
            name: name.into(),
            offset,
        });
        self
    }

    /// Set the JSON path to extract.
    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Set the fields to fetch (projection).
    pub fn with_projection(mut self, fields: Vec<impl Into<String>>) -> Self {
        self.projection = Some(fields.into_iter().map(Into::into).collect());
        self
    }
}

/// Options for scanning Redis strings.
///
/// # Example
///
/// ```ignore
/// use polars_redis::options::StringScanOptions;
///
/// let opts = StringScanOptions::new("counter:*")
///     .with_batch_size(1000)
///     .with_value_column_name("count");
/// ```
#[derive(Debug, Clone)]
pub struct StringScanOptions {
    /// Base scan options.
    pub scan: ScanOptions,
    /// Whether to include the key column.
    pub include_key: bool,
    /// Custom name for the key column.
    pub key_column_name: Option<String>,
    /// Custom name for the value column.
    pub value_column_name: Option<String>,
    /// Row index configuration.
    pub row_index: Option<RowIndex>,
}

impl Default for StringScanOptions {
    fn default() -> Self {
        Self {
            scan: ScanOptions::default(),
            include_key: true,
            key_column_name: None,
            value_column_name: None,
            row_index: None,
        }
    }
}

impl StringScanOptions {
    /// Create new StringScanOptions with the given pattern.
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            scan: ScanOptions::new(pattern),
            ..Default::default()
        }
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.scan.batch_size = size;
        self
    }

    /// Set the COUNT hint for SCAN.
    pub fn with_count_hint(mut self, count: usize) -> Self {
        self.scan.count_hint = count;
        self
    }

    /// Set the maximum number of rows to return.
    pub fn with_n_rows(mut self, n: usize) -> Self {
        self.scan.n_rows = Some(n);
        self
    }

    /// Enable or disable the key column.
    pub fn with_key(mut self, include: bool) -> Self {
        self.include_key = include;
        self
    }

    /// Set a custom name for the key column.
    pub fn with_key_column_name(mut self, name: impl Into<String>) -> Self {
        self.key_column_name = Some(name.into());
        self
    }

    /// Set a custom name for the value column.
    pub fn with_value_column_name(mut self, name: impl Into<String>) -> Self {
        self.value_column_name = Some(name.into());
        self
    }

    /// Set the row index configuration.
    pub fn with_row_index(mut self, name: impl Into<String>, offset: u64) -> Self {
        self.row_index = Some(RowIndex {
            name: name.into(),
            offset,
        });
        self
    }
}

/// Options for scanning Redis streams.
///
/// # Example
///
/// ```ignore
/// use polars_redis::options::StreamScanOptions;
///
/// let opts = StreamScanOptions::new("events:*")
///     .with_start_id("-")
///     .with_end_id("+")
///     .with_count_per_stream(1000);
/// ```
#[derive(Debug, Clone)]
pub struct StreamScanOptions {
    /// Base scan options.
    pub scan: ScanOptions,
    /// Whether to include the key column.
    pub include_key: bool,
    /// Custom name for the key column.
    pub key_column_name: Option<String>,
    /// Whether to include the entry ID.
    pub include_id: bool,
    /// Whether to include timestamp.
    pub include_timestamp: bool,
    /// Whether to include sequence number.
    pub include_sequence: bool,
    /// Row index configuration.
    pub row_index: Option<RowIndex>,
    /// Start ID for XRANGE (default: "-").
    pub start_id: String,
    /// End ID for XRANGE (default: "+").
    pub end_id: String,
    /// Maximum entries per stream.
    pub count_per_stream: Option<usize>,
    /// Fields to extract from stream entries.
    pub fields: Option<Vec<String>>,
}

impl Default for StreamScanOptions {
    fn default() -> Self {
        Self {
            scan: ScanOptions::default(),
            include_key: true,
            key_column_name: None,
            include_id: true,
            include_timestamp: true,
            include_sequence: false,
            row_index: None,
            start_id: "-".to_string(),
            end_id: "+".to_string(),
            count_per_stream: None,
            fields: None,
        }
    }
}

impl StreamScanOptions {
    /// Create new StreamScanOptions with the given pattern.
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            scan: ScanOptions::new(pattern),
            ..Default::default()
        }
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.scan.batch_size = size;
        self
    }

    /// Set the COUNT hint for SCAN.
    pub fn with_count_hint(mut self, count: usize) -> Self {
        self.scan.count_hint = count;
        self
    }

    /// Set the maximum number of rows to return.
    pub fn with_n_rows(mut self, n: usize) -> Self {
        self.scan.n_rows = Some(n);
        self
    }

    /// Enable or disable the key column.
    pub fn with_key(mut self, include: bool) -> Self {
        self.include_key = include;
        self
    }

    /// Enable or disable the entry ID column.
    pub fn with_id(mut self, include: bool) -> Self {
        self.include_id = include;
        self
    }

    /// Enable or disable the timestamp column.
    pub fn with_timestamp(mut self, include: bool) -> Self {
        self.include_timestamp = include;
        self
    }

    /// Enable or disable the sequence column.
    pub fn with_sequence(mut self, include: bool) -> Self {
        self.include_sequence = include;
        self
    }

    /// Set the row index configuration.
    pub fn with_row_index(mut self, name: impl Into<String>, offset: u64) -> Self {
        self.row_index = Some(RowIndex {
            name: name.into(),
            offset,
        });
        self
    }

    /// Set the start ID for XRANGE.
    pub fn with_start_id(mut self, id: impl Into<String>) -> Self {
        self.start_id = id.into();
        self
    }

    /// Set the end ID for XRANGE.
    pub fn with_end_id(mut self, id: impl Into<String>) -> Self {
        self.end_id = id.into();
        self
    }

    /// Set the maximum entries to fetch per stream.
    pub fn with_count_per_stream(mut self, count: usize) -> Self {
        self.count_per_stream = Some(count);
        self
    }

    /// Set the fields to extract from stream entries.
    pub fn with_fields(mut self, fields: Vec<impl Into<String>>) -> Self {
        self.fields = Some(fields.into_iter().map(Into::into).collect());
        self
    }
}

/// Options for scanning Redis time series.
///
/// # Example
///
/// ```ignore
/// use polars_redis::options::TimeSeriesScanOptions;
///
/// let opts = TimeSeriesScanOptions::new("sensor:*")
///     .with_start("1000")
///     .with_end("2000")
///     .with_aggregation("avg", 60000);
/// ```
#[derive(Debug, Clone)]
pub struct TimeSeriesScanOptions {
    /// Base scan options.
    pub scan: ScanOptions,
    /// Whether to include the key column.
    pub include_key: bool,
    /// Custom name for the key column.
    pub key_column_name: Option<String>,
    /// Custom name for the timestamp column.
    pub timestamp_column_name: Option<String>,
    /// Custom name for the value column.
    pub value_column_name: Option<String>,
    /// Row index configuration.
    pub row_index: Option<RowIndex>,
    /// Start timestamp for TS.RANGE (default: "-").
    pub start: String,
    /// End timestamp for TS.RANGE (default: "+").
    pub end: String,
    /// Maximum samples per time series.
    pub count_per_series: Option<usize>,
    /// Aggregation type (avg, sum, min, max, etc.).
    pub aggregation: Option<String>,
    /// Bucket size in milliseconds for aggregation.
    pub bucket_size_ms: Option<i64>,
}

impl Default for TimeSeriesScanOptions {
    fn default() -> Self {
        Self {
            scan: ScanOptions::default(),
            include_key: true,
            key_column_name: None,
            timestamp_column_name: None,
            value_column_name: None,
            row_index: None,
            start: "-".to_string(),
            end: "+".to_string(),
            count_per_series: None,
            aggregation: None,
            bucket_size_ms: None,
        }
    }
}

impl TimeSeriesScanOptions {
    /// Create new TimeSeriesScanOptions with the given pattern.
    pub fn new(pattern: impl Into<String>) -> Self {
        Self {
            scan: ScanOptions::new(pattern),
            ..Default::default()
        }
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.scan.batch_size = size;
        self
    }

    /// Set the COUNT hint for SCAN.
    pub fn with_count_hint(mut self, count: usize) -> Self {
        self.scan.count_hint = count;
        self
    }

    /// Set the maximum number of rows to return.
    pub fn with_n_rows(mut self, n: usize) -> Self {
        self.scan.n_rows = Some(n);
        self
    }

    /// Enable or disable the key column.
    pub fn with_key(mut self, include: bool) -> Self {
        self.include_key = include;
        self
    }

    /// Set a custom name for the key column.
    pub fn with_key_column_name(mut self, name: impl Into<String>) -> Self {
        self.key_column_name = Some(name.into());
        self
    }

    /// Set a custom name for the timestamp column.
    pub fn with_timestamp_column_name(mut self, name: impl Into<String>) -> Self {
        self.timestamp_column_name = Some(name.into());
        self
    }

    /// Set a custom name for the value column.
    pub fn with_value_column_name(mut self, name: impl Into<String>) -> Self {
        self.value_column_name = Some(name.into());
        self
    }

    /// Set the row index configuration.
    pub fn with_row_index(mut self, name: impl Into<String>, offset: u64) -> Self {
        self.row_index = Some(RowIndex {
            name: name.into(),
            offset,
        });
        self
    }

    /// Set the start timestamp for TS.RANGE.
    pub fn with_start(mut self, start: impl Into<String>) -> Self {
        self.start = start.into();
        self
    }

    /// Set the end timestamp for TS.RANGE.
    pub fn with_end(mut self, end: impl Into<String>) -> Self {
        self.end = end.into();
        self
    }

    /// Set the maximum samples to fetch per time series.
    pub fn with_count_per_series(mut self, count: usize) -> Self {
        self.count_per_series = Some(count);
        self
    }

    /// Set aggregation type and bucket size.
    pub fn with_aggregation(mut self, agg_type: impl Into<String>, bucket_size_ms: i64) -> Self {
        self.aggregation = Some(agg_type.into());
        self.bucket_size_ms = Some(bucket_size_ms);
        self
    }
}

// ============================================================================
// Environment variable configuration
// ============================================================================

/// Default batch size from environment or fallback.
static DEFAULT_BATCH_SIZE: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("POLARS_REDIS_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000)
});

/// Default count hint from environment or fallback.
static DEFAULT_COUNT_HINT: LazyLock<usize> = LazyLock::new(|| {
    std::env::var("POLARS_REDIS_COUNT_HINT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100)
});

/// Default connection timeout in milliseconds.
static DEFAULT_TIMEOUT_MS: LazyLock<u64> = LazyLock::new(|| {
    std::env::var("POLARS_REDIS_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30000)
});

/// Get the default batch size.
pub fn get_default_batch_size() -> usize {
    *DEFAULT_BATCH_SIZE
}

/// Get the default count hint.
pub fn get_default_count_hint() -> usize {
    *DEFAULT_COUNT_HINT
}

/// Get the default timeout in milliseconds.
pub fn get_default_timeout_ms() -> u64 {
    *DEFAULT_TIMEOUT_MS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_options_default() {
        let opts = ScanOptions::default();
        assert_eq!(opts.pattern, "*");
        assert_eq!(opts.batch_size, get_default_batch_size());
        assert_eq!(opts.count_hint, get_default_count_hint());
        assert!(opts.n_rows.is_none());
    }

    #[test]
    fn test_scan_options_builder() {
        let opts = ScanOptions::new("user:*")
            .with_batch_size(500)
            .with_count_hint(50)
            .with_n_rows(1000);

        assert_eq!(opts.pattern, "user:*");
        assert_eq!(opts.batch_size, 500);
        assert_eq!(opts.count_hint, 50);
        assert_eq!(opts.n_rows, Some(1000));
    }

    #[test]
    fn test_key_column() {
        let enabled = KeyColumn::enabled().with_name("redis_key");
        assert!(enabled.enabled);
        assert_eq!(enabled.name, "redis_key");

        let disabled = KeyColumn::disabled();
        assert!(!disabled.enabled);
    }

    #[test]
    fn test_ttl_column() {
        let enabled = TtlColumn::enabled().with_name("expiry");
        assert!(enabled.enabled);
        assert_eq!(enabled.name, "expiry");

        let disabled = TtlColumn::disabled();
        assert!(!disabled.enabled);
    }

    #[test]
    fn test_row_index_column() {
        let col = RowIndexColumn::enabled()
            .with_name("row_num")
            .with_offset(100);

        assert!(col.enabled);
        assert_eq!(col.name, "row_num");
        assert_eq!(col.offset, 100);

        let row_index = col.to_row_index().unwrap();
        assert_eq!(row_index.name, "row_num");
        assert_eq!(row_index.offset, 100);
    }

    #[test]
    fn test_row_index() {
        let idx = RowIndex::new("idx").with_offset(50);
        assert_eq!(idx.name, "idx");
        assert_eq!(idx.offset, 50);
    }

    #[test]
    fn test_hash_scan_options() {
        let opts = HashScanOptions::new("user:*")
            .with_batch_size(500)
            .with_count_hint(50)
            .with_n_rows(1000)
            .with_key(true)
            .with_key_column_name("redis_key")
            .with_ttl(true)
            .with_ttl_column_name("expiry")
            .with_row_index("idx", 10)
            .with_projection(vec!["name", "email"]);

        assert_eq!(opts.scan.pattern, "user:*");
        assert_eq!(opts.scan.batch_size, 500);
        assert_eq!(opts.scan.count_hint, 50);
        assert_eq!(opts.scan.n_rows, Some(1000));
        assert!(opts.include_key);
        assert_eq!(opts.key_column_name, Some("redis_key".to_string()));
        assert!(opts.include_ttl);
        assert_eq!(opts.ttl_column_name, Some("expiry".to_string()));
        assert_eq!(
            opts.row_index.as_ref().map(|r| &r.name),
            Some(&"idx".to_string())
        );
        assert_eq!(opts.row_index.as_ref().map(|r| r.offset), Some(10));
        assert_eq!(
            opts.projection,
            Some(vec!["name".to_string(), "email".to_string()])
        );
    }

    #[test]
    fn test_json_scan_options() {
        let opts = JsonScanOptions::new("doc:*")
            .with_batch_size(250)
            .with_path("$.user")
            .with_projection(vec!["name", "age"]);

        assert_eq!(opts.scan.pattern, "doc:*");
        assert_eq!(opts.scan.batch_size, 250);
        assert_eq!(opts.path, Some("$.user".to_string()));
        assert_eq!(
            opts.projection,
            Some(vec!["name".to_string(), "age".to_string()])
        );
    }

    #[test]
    fn test_string_scan_options() {
        let opts = StringScanOptions::new("counter:*")
            .with_batch_size(1000)
            .with_value_column_name("count")
            .with_key(false);

        assert_eq!(opts.scan.pattern, "counter:*");
        assert_eq!(opts.scan.batch_size, 1000);
        assert_eq!(opts.value_column_name, Some("count".to_string()));
        assert!(!opts.include_key);
    }

    #[test]
    fn test_stream_scan_options() {
        let opts = StreamScanOptions::new("events:*")
            .with_start_id("1000-0")
            .with_end_id("2000-0")
            .with_count_per_stream(100)
            .with_id(true)
            .with_timestamp(true)
            .with_sequence(true)
            .with_fields(vec!["action", "user"]);

        assert_eq!(opts.scan.pattern, "events:*");
        assert_eq!(opts.start_id, "1000-0");
        assert_eq!(opts.end_id, "2000-0");
        assert_eq!(opts.count_per_stream, Some(100));
        assert!(opts.include_id);
        assert!(opts.include_timestamp);
        assert!(opts.include_sequence);
        assert_eq!(
            opts.fields,
            Some(vec!["action".to_string(), "user".to_string()])
        );
    }

    #[test]
    fn test_timeseries_scan_options() {
        let opts = TimeSeriesScanOptions::new("sensor:*")
            .with_start("1000")
            .with_end("2000")
            .with_count_per_series(500)
            .with_aggregation("avg", 60000)
            .with_value_column_name("temperature")
            .with_timestamp_column_name("ts");

        assert_eq!(opts.scan.pattern, "sensor:*");
        assert_eq!(opts.start, "1000");
        assert_eq!(opts.end, "2000");
        assert_eq!(opts.count_per_series, Some(500));
        assert_eq!(opts.aggregation, Some("avg".to_string()));
        assert_eq!(opts.bucket_size_ms, Some(60000));
        assert_eq!(opts.value_column_name, Some("temperature".to_string()));
        assert_eq!(opts.timestamp_column_name, Some("ts".to_string()));
    }
}
