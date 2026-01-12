//! CDC-style replication pipeline for Redis streams.
//!
//! This module provides a `ReplicationPipeline` that consumes from a source stream,
//! applies optional filters, and replicates data to multiple destination clusters.
//!
//! # Filtering
//!
//! The Rust implementation supports basic field-level predicates for filtering.
//! For complex filtering with full Polars expressions, use the Python API.
//!
//! ## Supported Filters
//!
//! - `eq(field, value)` - Field equals value
//! - `ne(field, value)` - Field not equals value
//! - `gt(field, value)` - Field greater than (numeric)
//! - `gte(field, value)` - Field greater than or equal (numeric)
//! - `lt(field, value)` - Field less than (numeric)
//! - `lte(field, value)` - Field less than or equal (numeric)
//! - `in_list(field, values)` - Field value in list
//! - `not_in_list(field, values)` - Field value not in list
//! - `contains(field, substring)` - Field contains substring
//! - `starts_with(field, prefix)` - Field starts with prefix
//! - `ends_with(field, suffix)` - Field ends with suffix
//! - `is_null(field)` - Field is null/missing
//! - `is_not_null(field)` - Field is not null
//!
//! Filters can be combined with `and()` and `or()`.
//!
//! # Example
//!
//! ```ignore
//! use polars_redis::io::replication::{ReplicationPipeline, ReplicationConfig, Filter};
//! use polars_redis::io::multi::Destination;
//!
//! // Create filter for active users in US regions
//! let filter = Filter::new()
//!     .eq("status", "active")
//!     .and()
//!     .in_list("region", vec!["us-east", "us-west"]);
//!
//! // Configure destinations
//! let destinations = vec![
//!     Destination::new("redis://replica1:6379").with_name("replica1"),
//!     Destination::new("redis://replica2:6379").with_name("replica2"),
//! ];
//!
//! // Create and run pipeline
//! let config = ReplicationConfig::new("redis://source:6379", "events", destinations)
//!     .with_filter(filter)
//!     .with_consumer_group("replicator", "worker-1");
//!
//! let mut pipeline = ReplicationPipeline::new(config)?;
//! pipeline.run()?;
//! ```
//!
//! # Limitations vs Python API
//!
//! The Rust filtering is intentionally simpler than Python's full Polars expressions.
//! Use cases requiring complex transformations should use Python, or implement custom
//! filtering logic using the `StreamConsumer` and `MultiClusterWriter` components directly.
//!
//! ## What Rust supports:
//! - Field equality/comparison predicates
//! - String matching (contains, starts_with, ends_with)
//! - Null checks
//! - AND/OR combinations
//!
//! ## What requires Python (or manual implementation):
//! - Computed columns / transformations
//! - Aggregations
//! - Complex boolean logic with grouping
//! - Type coercion
//! - Regular expressions
//!
//! # Building Custom Filtering Logic
//!
//! When Rust's built-in filters are insufficient, you can build custom pipelines
//! using `StreamConsumer` and `MultiClusterWriter` directly:
//!
//! ```ignore
//! use polars_redis::{StreamConsumer, ConsumerConfig, RedisCheckpointStore};
//! use polars_redis::io::multi::{MultiClusterWriter, Destination};
//!
//! // Create consumer
//! let mut consumer = StreamConsumer::new(
//!     "redis://source:6379",
//!     "events",
//!     "my-group",
//!     "worker-1",
//! )?
//! .with_config(ConsumerConfig::new().with_batch_size(100));
//!
//! // Create writer for destinations
//! let writer = MultiClusterWriter::new(vec![
//!     Destination::new("redis://replica1:6379"),
//!     Destination::new("redis://replica2:6379"),
//! ]);
//!
//! // Custom filtering loop
//! loop {
//!     // Get raw entries for custom processing
//!     let entries = consumer.next_batch_raw().await?;
//!
//!     for (entry_id, fields) in entries {
//!         // Custom logic: regex matching, computed fields, aggregation, etc.
//!         let should_replicate = custom_filter(&fields);
//!
//!         if should_replicate {
//!             // Transform if needed
//!             let transformed = transform_entry(&fields);
//!
//!             // Write to destinations
//!             writer.write_hash(&format!("event:{}", entry_id), &transformed)?;
//!         }
//!
//!         // Acknowledge
//!         consumer.ack(&[entry_id]).await?;
//!     }
//! }
//! ```
//!
//! ## Comparison: Python vs Rust Filtering
//!
//! | Feature | Rust Filter | Python (Polars) |
//! |---------|-------------|-----------------|
//! | Field equality | `eq("status", "active")` | `pl.col("status") == "active"` |
//! | Numeric comparison | `gt("age", 18)` | `pl.col("age") > 18` |
//! | String contains | `contains("email", "@")` | `pl.col("email").str.contains("@")` |
//! | Regex matching | Manual implementation | `pl.col("email").str.contains(r"^\w+@")` |
//! | Computed column | Manual implementation | `pl.col("price") * pl.col("qty")` |
//! | Type coercion | Manual implementation | `pl.col("age").cast(pl.Int64)` |
//! | Nested boolean | `and()` / `or()` only | Full expression tree |
//!
//! For production CDC pipelines with complex logic, Python offers more flexibility.
//! For simple field-based routing, Rust provides lower overhead.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use redis::AsyncCommands;
use tokio::runtime::Runtime;

use crate::error::{Error, Result};
use crate::io::multi::Destination;
use crate::io::types::stream::{ConsumerConfig, RedisCheckpointStore, StreamConsumer};

// ============================================================================
// Filter Predicates
// ============================================================================

/// A value that can be compared in a filter predicate.
#[derive(Debug, Clone)]
pub enum FilterValue {
    /// String value.
    String(String),
    /// Integer value.
    Int(i64),
    /// Float value.
    Float(f64),
    /// Boolean value.
    Bool(bool),
    /// Null value.
    Null,
}

impl From<&str> for FilterValue {
    fn from(s: &str) -> Self {
        FilterValue::String(s.to_string())
    }
}

impl From<String> for FilterValue {
    fn from(s: String) -> Self {
        FilterValue::String(s)
    }
}

impl From<i64> for FilterValue {
    fn from(v: i64) -> Self {
        FilterValue::Int(v)
    }
}

impl From<i32> for FilterValue {
    fn from(v: i32) -> Self {
        FilterValue::Int(v as i64)
    }
}

impl From<f64> for FilterValue {
    fn from(v: f64) -> Self {
        FilterValue::Float(v)
    }
}

impl From<bool> for FilterValue {
    fn from(v: bool) -> Self {
        FilterValue::Bool(v)
    }
}

/// A single filter predicate.
#[derive(Debug, Clone)]
pub enum Predicate {
    /// Field equals value.
    Eq(String, FilterValue),
    /// Field not equals value.
    Ne(String, FilterValue),
    /// Field greater than value (numeric).
    Gt(String, FilterValue),
    /// Field greater than or equal value (numeric).
    Gte(String, FilterValue),
    /// Field less than value (numeric).
    Lt(String, FilterValue),
    /// Field less than or equal value (numeric).
    Lte(String, FilterValue),
    /// Field value in list.
    InList(String, Vec<FilterValue>),
    /// Field value not in list.
    NotInList(String, Vec<FilterValue>),
    /// Field contains substring.
    Contains(String, String),
    /// Field starts with prefix.
    StartsWith(String, String),
    /// Field ends with suffix.
    EndsWith(String, String),
    /// Field is null/missing.
    IsNull(String),
    /// Field is not null.
    IsNotNull(String),
    /// Logical AND of predicates.
    And(Vec<Predicate>),
    /// Logical OR of predicates.
    Or(Vec<Predicate>),
}

impl Predicate {
    /// Evaluate this predicate against a row (field -> value map).
    pub fn evaluate(&self, row: &HashMap<String, String>) -> bool {
        match self {
            Predicate::Eq(field, value) => {
                row.get(field).is_some_and(|v| Self::compare_eq(v, value))
            },
            Predicate::Ne(field, value) => {
                row.get(field).is_none_or(|v| !Self::compare_eq(v, value))
            },
            Predicate::Gt(field, value) => {
                row.get(field).is_some_and(|v| Self::compare_gt(v, value))
            },
            Predicate::Gte(field, value) => row
                .get(field)
                .is_some_and(|v| Self::compare_eq(v, value) || Self::compare_gt(v, value)),
            Predicate::Lt(field, value) => {
                row.get(field).is_some_and(|v| Self::compare_lt(v, value))
            },
            Predicate::Lte(field, value) => row
                .get(field)
                .is_some_and(|v| Self::compare_eq(v, value) || Self::compare_lt(v, value)),
            Predicate::InList(field, values) => row
                .get(field)
                .is_some_and(|v| values.iter().any(|val| Self::compare_eq(v, val))),
            Predicate::NotInList(field, values) => row
                .get(field)
                .is_none_or(|v| !values.iter().any(|val| Self::compare_eq(v, val))),
            Predicate::Contains(field, substring) => {
                row.get(field).is_some_and(|v| v.contains(substring))
            },
            Predicate::StartsWith(field, prefix) => {
                row.get(field).is_some_and(|v| v.starts_with(prefix))
            },
            Predicate::EndsWith(field, suffix) => {
                row.get(field).is_some_and(|v| v.ends_with(suffix))
            },
            Predicate::IsNull(field) => !row.contains_key(field),
            Predicate::IsNotNull(field) => row.contains_key(field),
            Predicate::And(predicates) => predicates.iter().all(|p| p.evaluate(row)),
            Predicate::Or(predicates) => predicates.iter().any(|p| p.evaluate(row)),
        }
    }

    fn compare_eq(field_value: &str, filter_value: &FilterValue) -> bool {
        match filter_value {
            FilterValue::String(s) => field_value == s,
            FilterValue::Int(i) => field_value.parse::<i64>() == Ok(*i),
            FilterValue::Float(f) => field_value
                .parse::<f64>()
                .is_ok_and(|v| (v - f).abs() < f64::EPSILON),
            FilterValue::Bool(b) => {
                let parsed = matches!(field_value.to_lowercase().as_str(), "true" | "1" | "yes");
                parsed == *b
            },
            FilterValue::Null => false,
        }
    }

    fn compare_gt(field_value: &str, filter_value: &FilterValue) -> bool {
        match filter_value {
            FilterValue::Int(i) => field_value.parse::<i64>().is_ok_and(|v| v > *i),
            FilterValue::Float(f) => field_value.parse::<f64>().is_ok_and(|v| v > *f),
            FilterValue::String(s) => field_value > s.as_str(),
            _ => false,
        }
    }

    fn compare_lt(field_value: &str, filter_value: &FilterValue) -> bool {
        match filter_value {
            FilterValue::Int(i) => field_value.parse::<i64>().is_ok_and(|v| v < *i),
            FilterValue::Float(f) => field_value.parse::<f64>().is_ok_and(|v| v < *f),
            FilterValue::String(s) => field_value < s.as_str(),
            _ => false,
        }
    }
}

/// Builder for creating filter predicates.
///
/// # Example
///
/// ```ignore
/// let filter = Filter::new()
///     .eq("status", "active")
///     .and()
///     .gt("age", 18)
///     .and()
///     .in_list("region", vec!["us", "eu"]);
/// ```
#[derive(Debug, Clone, Default)]
pub struct Filter {
    predicates: Vec<Predicate>,
    pending_combinator: Option<Combinator>,
}

#[derive(Debug, Clone, Copy)]
enum Combinator {
    And,
    Or,
}

impl Filter {
    /// Create a new empty filter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an equality predicate.
    pub fn eq<F: Into<String>, V: Into<FilterValue>>(mut self, field: F, value: V) -> Self {
        self.add_predicate(Predicate::Eq(field.into(), value.into()));
        self
    }

    /// Add a not-equals predicate.
    pub fn ne<F: Into<String>, V: Into<FilterValue>>(mut self, field: F, value: V) -> Self {
        self.add_predicate(Predicate::Ne(field.into(), value.into()));
        self
    }

    /// Add a greater-than predicate.
    pub fn gt<F: Into<String>, V: Into<FilterValue>>(mut self, field: F, value: V) -> Self {
        self.add_predicate(Predicate::Gt(field.into(), value.into()));
        self
    }

    /// Add a greater-than-or-equal predicate.
    pub fn gte<F: Into<String>, V: Into<FilterValue>>(mut self, field: F, value: V) -> Self {
        self.add_predicate(Predicate::Gte(field.into(), value.into()));
        self
    }

    /// Add a less-than predicate.
    pub fn lt<F: Into<String>, V: Into<FilterValue>>(mut self, field: F, value: V) -> Self {
        self.add_predicate(Predicate::Lt(field.into(), value.into()));
        self
    }

    /// Add a less-than-or-equal predicate.
    pub fn lte<F: Into<String>, V: Into<FilterValue>>(mut self, field: F, value: V) -> Self {
        self.add_predicate(Predicate::Lte(field.into(), value.into()));
        self
    }

    /// Add an in-list predicate.
    pub fn in_list<F: Into<String>, V: Into<FilterValue>>(
        mut self,
        field: F,
        values: Vec<V>,
    ) -> Self {
        let filter_values: Vec<FilterValue> = values.into_iter().map(|v| v.into()).collect();
        self.add_predicate(Predicate::InList(field.into(), filter_values));
        self
    }

    /// Add a not-in-list predicate.
    pub fn not_in_list<F: Into<String>, V: Into<FilterValue>>(
        mut self,
        field: F,
        values: Vec<V>,
    ) -> Self {
        let filter_values: Vec<FilterValue> = values.into_iter().map(|v| v.into()).collect();
        self.add_predicate(Predicate::NotInList(field.into(), filter_values));
        self
    }

    /// Add a contains predicate.
    pub fn contains<F: Into<String>, S: Into<String>>(mut self, field: F, substring: S) -> Self {
        self.add_predicate(Predicate::Contains(field.into(), substring.into()));
        self
    }

    /// Add a starts-with predicate.
    pub fn starts_with<F: Into<String>, S: Into<String>>(mut self, field: F, prefix: S) -> Self {
        self.add_predicate(Predicate::StartsWith(field.into(), prefix.into()));
        self
    }

    /// Add an ends-with predicate.
    pub fn ends_with<F: Into<String>, S: Into<String>>(mut self, field: F, suffix: S) -> Self {
        self.add_predicate(Predicate::EndsWith(field.into(), suffix.into()));
        self
    }

    /// Add an is-null predicate.
    pub fn is_null<F: Into<String>>(mut self, field: F) -> Self {
        self.add_predicate(Predicate::IsNull(field.into()));
        self
    }

    /// Add an is-not-null predicate.
    pub fn is_not_null<F: Into<String>>(mut self, field: F) -> Self {
        self.add_predicate(Predicate::IsNotNull(field.into()));
        self
    }

    /// Combine subsequent predicates with AND.
    pub fn and(mut self) -> Self {
        self.pending_combinator = Some(Combinator::And);
        self
    }

    /// Combine subsequent predicates with OR.
    pub fn or(mut self) -> Self {
        self.pending_combinator = Some(Combinator::Or);
        self
    }

    fn add_predicate(&mut self, predicate: Predicate) {
        match self.pending_combinator.take() {
            Some(Combinator::And) if !self.predicates.is_empty() => {
                // Wrap existing predicates and new one in AND
                let existing = std::mem::take(&mut self.predicates);
                let mut all = existing;
                all.push(predicate);
                self.predicates.push(Predicate::And(all));
            },
            Some(Combinator::Or) if !self.predicates.is_empty() => {
                // Wrap existing predicates and new one in OR
                let existing = std::mem::take(&mut self.predicates);
                let mut all = existing;
                all.push(predicate);
                self.predicates.push(Predicate::Or(all));
            },
            _ => {
                self.predicates.push(predicate);
            },
        }
    }

    /// Build the final predicate.
    pub fn build(self) -> Option<Predicate> {
        match self.predicates.len() {
            0 => None,
            1 => Some(self.predicates.into_iter().next().unwrap()),
            _ => Some(Predicate::And(self.predicates)),
        }
    }

    /// Check if the filter is empty (no predicates).
    pub fn is_empty(&self) -> bool {
        self.predicates.is_empty()
    }

    /// Evaluate the filter against a row.
    pub fn evaluate(&self, row: &HashMap<String, String>) -> bool {
        if self.predicates.is_empty() {
            return true;
        }
        self.predicates.iter().all(|p| p.evaluate(row))
    }
}

// ============================================================================
// Replication Configuration
// ============================================================================

/// Configuration for a replication destination with optional filtering.
#[derive(Debug, Clone)]
pub struct ReplicationDestination {
    /// The destination cluster.
    pub destination: Destination,
    /// Optional filter for this destination.
    pub filter: Option<Filter>,
}

impl ReplicationDestination {
    /// Create a new replication destination.
    pub fn new(destination: Destination) -> Self {
        Self {
            destination,
            filter: None,
        }
    }

    /// Add a filter for this destination.
    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }
}

/// Configuration for the replication pipeline.
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    /// Source Redis URL.
    pub source_url: String,
    /// Source stream key.
    pub stream_key: String,
    /// Destinations with optional per-destination filters.
    pub destinations: Vec<ReplicationDestination>,
    /// Consumer group name.
    pub group_name: String,
    /// Consumer name within the group.
    pub consumer_name: String,
    /// Global filter applied before destination-specific filters.
    pub global_filter: Option<Filter>,
    /// Batch size for reading from stream.
    pub batch_size: usize,
    /// Block timeout in milliseconds for XREADGROUP.
    pub block_ms: Option<u64>,
    /// Whether to create the consumer group if it doesn't exist.
    pub create_group: bool,
    /// Whether to acknowledge entries after successful write.
    pub auto_ack: bool,
    /// Checkpoint after this many entries.
    pub checkpoint_interval: usize,
}

impl ReplicationConfig {
    /// Create a new replication config.
    pub fn new<S: Into<String>>(
        source_url: S,
        stream_key: S,
        destinations: Vec<Destination>,
    ) -> Self {
        Self {
            source_url: source_url.into(),
            stream_key: stream_key.into(),
            destinations: destinations
                .into_iter()
                .map(ReplicationDestination::new)
                .collect(),
            group_name: "replication".to_string(),
            consumer_name: "worker".to_string(),
            global_filter: None,
            batch_size: 100,
            block_ms: Some(5000),
            create_group: true,
            auto_ack: true,
            checkpoint_interval: 100,
        }
    }

    /// Set the consumer group and consumer name.
    pub fn with_consumer_group<G: Into<String>, C: Into<String>>(
        mut self,
        group: G,
        consumer: C,
    ) -> Self {
        self.group_name = group.into();
        self.consumer_name = consumer.into();
        self
    }

    /// Set a global filter applied to all entries.
    pub fn with_filter(mut self, filter: Filter) -> Self {
        self.global_filter = Some(filter);
        self
    }

    /// Set destinations with per-destination filters.
    pub fn with_destinations(mut self, destinations: Vec<ReplicationDestination>) -> Self {
        self.destinations = destinations;
        self
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set the block timeout in milliseconds.
    pub fn with_block_ms(mut self, ms: u64) -> Self {
        self.block_ms = Some(ms);
        self
    }

    /// Disable blocking (poll mode).
    pub fn without_blocking(mut self) -> Self {
        self.block_ms = None;
        self
    }

    /// Set whether to create the consumer group.
    pub fn with_create_group(mut self, create: bool) -> Self {
        self.create_group = create;
        self
    }

    /// Set whether to auto-acknowledge entries.
    pub fn with_auto_ack(mut self, ack: bool) -> Self {
        self.auto_ack = ack;
        self
    }

    /// Set the checkpoint interval.
    pub fn with_checkpoint_interval(mut self, interval: usize) -> Self {
        self.checkpoint_interval = interval;
        self
    }
}

// ============================================================================
// Replication Statistics
// ============================================================================

/// Statistics for a replication run.
#[derive(Debug, Clone, Default)]
pub struct ReplicationStats {
    /// Total entries read from source.
    pub entries_read: usize,
    /// Entries passing global filter.
    pub entries_after_global_filter: usize,
    /// Entries written per destination.
    pub entries_written: HashMap<String, usize>,
    /// Entries filtered out per destination.
    pub entries_filtered: HashMap<String, usize>,
    /// Errors per destination.
    pub errors: HashMap<String, Vec<String>>,
    /// Total duration.
    pub duration: Duration,
    /// Last processed entry ID.
    pub last_entry_id: Option<String>,
}

impl ReplicationStats {
    /// Get total entries written across all destinations.
    pub fn total_written(&self) -> usize {
        self.entries_written.values().sum()
    }

    /// Get total errors across all destinations.
    pub fn total_errors(&self) -> usize {
        self.errors.values().map(|v| v.len()).sum()
    }

    /// Check if replication completed without errors.
    pub fn is_success(&self) -> bool {
        self.total_errors() == 0
    }
}

// ============================================================================
// Replication Pipeline
// ============================================================================

/// CDC-style replication pipeline.
///
/// Consumes entries from a source stream, applies filters, and replicates
/// to multiple destination clusters.
pub struct ReplicationPipeline {
    config: ReplicationConfig,
    runtime: Runtime,
    stats: ReplicationStats,
}

impl ReplicationPipeline {
    /// Create a new replication pipeline.
    pub fn new(config: ReplicationConfig) -> Result<Self> {
        let runtime = Runtime::new()
            .map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

        Ok(Self {
            config,
            runtime,
            stats: ReplicationStats::default(),
        })
    }

    /// Run the replication pipeline once (process available entries).
    ///
    /// Returns statistics about the replication run.
    pub fn run_once(&mut self) -> Result<ReplicationStats> {
        let start = Instant::now();
        self.stats = ReplicationStats::default();

        // Create consumer
        let mut consumer = StreamConsumer::new(
            &self.config.source_url,
            &self.config.stream_key,
            &self.config.group_name,
            &self.config.consumer_name,
        )?;

        consumer = consumer
            .with_config(
                ConsumerConfig::new()
                    .with_batch_size(self.config.batch_size)
                    .with_create_group(self.config.create_group)
                    .with_block_ms(self.config.block_ms.unwrap_or(5000)),
            )
            .with_checkpoint_store(Box::new(RedisCheckpointStore::new(
                &self.config.source_url,
            )?));

        // Process entries - extract what we need to avoid borrow conflicts
        let config = self.config.clone();
        let result = self.runtime.block_on(Self::process_entries_async(
            &mut consumer,
            &config,
            &mut self.stats,
        ))?;

        let _ = result; // consume the bool

        self.stats.duration = start.elapsed();
        Ok(self.stats.clone())
    }

    /// Run the replication pipeline continuously until stopped.
    ///
    /// This method blocks and processes entries as they arrive.
    /// Use `run_once` for single-pass processing.
    pub fn run(&mut self) -> Result<ReplicationStats> {
        let start = Instant::now();
        self.stats = ReplicationStats::default();

        // Create consumer
        let mut consumer = StreamConsumer::new(
            &self.config.source_url,
            &self.config.stream_key,
            &self.config.group_name,
            &self.config.consumer_name,
        )?;

        consumer = consumer
            .with_config(
                ConsumerConfig::new()
                    .with_batch_size(self.config.batch_size)
                    .with_create_group(self.config.create_group)
                    .with_block_ms(self.config.block_ms.unwrap_or(5000)),
            )
            .with_checkpoint_store(Box::new(RedisCheckpointStore::new(
                &self.config.source_url,
            )?));

        // Continuous processing loop
        let config = self.config.clone();
        loop {
            let had_entries = self.runtime.block_on(Self::process_entries_async(
                &mut consumer,
                &config,
                &mut self.stats,
            ))?;

            if !had_entries && config.block_ms.is_none() {
                // No entries and not blocking - exit
                break;
            }
        }

        self.stats.duration = start.elapsed();
        Ok(self.stats.clone())
    }

    async fn process_entries_async(
        consumer: &mut StreamConsumer,
        config: &ReplicationConfig,
        stats: &mut ReplicationStats,
    ) -> Result<bool> {
        // Read batch from consumer
        let entries: Vec<(String, HashMap<String, String>)> = consumer.next_batch_raw().await?;

        if entries.is_empty() {
            return Ok(false);
        }

        stats.entries_read += entries.len();
        let mut last_id: Option<String> = None;

        // Process each entry
        for (entry_id, fields) in &entries {
            last_id = Some(entry_id.clone());

            // Apply global filter
            if let Some(ref filter) = config.global_filter {
                if !filter.evaluate(fields) {
                    continue;
                }
            }

            stats.entries_after_global_filter += 1;

            // Write to each destination (with per-destination filtering)
            for repl_dest in &config.destinations {
                let dest_name = repl_dest.destination.display_name();

                // Apply destination-specific filter
                if let Some(ref filter) = repl_dest.filter {
                    if !filter.evaluate(fields) {
                        *stats.entries_filtered.entry(dest_name.clone()).or_insert(0) += 1;
                        continue;
                    }
                }

                // Write to destination
                match Self::write_entry_to_destination(&repl_dest.destination, entry_id, fields)
                    .await
                {
                    Ok(()) => {
                        *stats.entries_written.entry(dest_name).or_insert(0) += 1;
                    },
                    Err(e) => {
                        stats
                            .errors
                            .entry(dest_name)
                            .or_default()
                            .push(e.to_string());
                    },
                }
            }

            // Acknowledge if configured
            if config.auto_ack {
                consumer.ack(std::slice::from_ref(entry_id)).await?;
            }
        }

        if let Some(id) = last_id {
            stats.last_entry_id = Some(id);
        }

        Ok(true)
    }

    async fn write_entry_to_destination(
        destination: &Destination,
        entry_id: &str,
        fields: &HashMap<String, String>,
    ) -> Result<()> {
        let client = redis::Client::open(destination.url.as_str()).map_err(Error::Connection)?;

        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(Error::Connection)?;

        // Create key with optional prefix
        let key = if destination.key_prefix.is_empty() {
            entry_id.to_string()
        } else {
            format!("{}:{}", destination.key_prefix, entry_id)
        };

        // Write as hash
        let field_vec: Vec<(&str, &str)> = fields
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        conn.hset_multiple::<_, _, _, ()>(&key, &field_vec)
            .await
            .map_err(Error::Connection)?;

        Ok(())
    }

    /// Get current statistics.
    pub fn stats(&self) -> &ReplicationStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_eq() {
        let filter = Filter::new().eq("status", "active");

        let mut row = HashMap::new();
        row.insert("status".to_string(), "active".to_string());
        assert!(filter.evaluate(&row));

        row.insert("status".to_string(), "inactive".to_string());
        assert!(!filter.evaluate(&row));
    }

    #[test]
    fn test_filter_ne() {
        let filter = Filter::new().ne("status", "deleted");

        let mut row = HashMap::new();
        row.insert("status".to_string(), "active".to_string());
        assert!(filter.evaluate(&row));

        row.insert("status".to_string(), "deleted".to_string());
        assert!(!filter.evaluate(&row));
    }

    #[test]
    fn test_filter_gt_lt() {
        let filter = Filter::new().gt("age", 18);

        let mut row = HashMap::new();
        row.insert("age".to_string(), "25".to_string());
        assert!(filter.evaluate(&row));

        row.insert("age".to_string(), "18".to_string());
        assert!(!filter.evaluate(&row));

        row.insert("age".to_string(), "15".to_string());
        assert!(!filter.evaluate(&row));
    }

    #[test]
    fn test_filter_in_list() {
        let filter = Filter::new().in_list("region", vec!["us", "eu"]);

        let mut row = HashMap::new();
        row.insert("region".to_string(), "us".to_string());
        assert!(filter.evaluate(&row));

        row.insert("region".to_string(), "eu".to_string());
        assert!(filter.evaluate(&row));

        row.insert("region".to_string(), "asia".to_string());
        assert!(!filter.evaluate(&row));
    }

    #[test]
    fn test_filter_contains() {
        let filter = Filter::new().contains("email", "@example.com");

        let mut row = HashMap::new();
        row.insert("email".to_string(), "user@example.com".to_string());
        assert!(filter.evaluate(&row));

        row.insert("email".to_string(), "user@other.com".to_string());
        assert!(!filter.evaluate(&row));
    }

    #[test]
    fn test_filter_is_null() {
        let filter = Filter::new().is_null("deleted_at");

        let row = HashMap::new();
        assert!(filter.evaluate(&row));

        let mut row2 = HashMap::new();
        row2.insert("deleted_at".to_string(), "2024-01-01".to_string());
        assert!(!filter.evaluate(&row2));
    }

    #[test]
    fn test_filter_and() {
        let filter = Filter::new().eq("status", "active").and().gt("age", 18);

        let mut row = HashMap::new();
        row.insert("status".to_string(), "active".to_string());
        row.insert("age".to_string(), "25".to_string());
        assert!(filter.evaluate(&row));

        row.insert("status".to_string(), "inactive".to_string());
        assert!(!filter.evaluate(&row));
    }

    #[test]
    fn test_filter_or() {
        let filter = Filter::new()
            .eq("role", "admin")
            .or()
            .eq("role", "superuser");

        let mut row = HashMap::new();
        row.insert("role".to_string(), "admin".to_string());
        assert!(filter.evaluate(&row));

        row.insert("role".to_string(), "superuser".to_string());
        assert!(filter.evaluate(&row));

        row.insert("role".to_string(), "user".to_string());
        assert!(!filter.evaluate(&row));
    }

    #[test]
    fn test_filter_empty() {
        let filter = Filter::new();

        let row = HashMap::new();
        assert!(filter.evaluate(&row));
        assert!(filter.is_empty());
    }

    #[test]
    fn test_replication_config_builder() {
        let config = ReplicationConfig::new(
            "redis://localhost:6379",
            "events",
            vec![Destination::new("redis://replica:6379")],
        )
        .with_consumer_group("my-group", "worker-1")
        .with_batch_size(200)
        .with_block_ms(10000)
        .with_auto_ack(false);

        assert_eq!(config.source_url, "redis://localhost:6379");
        assert_eq!(config.stream_key, "events");
        assert_eq!(config.group_name, "my-group");
        assert_eq!(config.consumer_name, "worker-1");
        assert_eq!(config.batch_size, 200);
        assert_eq!(config.block_ms, Some(10000));
        assert!(!config.auto_ack);
    }

    #[test]
    fn test_replication_stats() {
        let mut stats = ReplicationStats::default();
        stats.entries_written.insert("replica1".to_string(), 100);
        stats.entries_written.insert("replica2".to_string(), 95);
        stats
            .errors
            .insert("replica2".to_string(), vec!["timeout".to_string()]);

        assert_eq!(stats.total_written(), 195);
        assert_eq!(stats.total_errors(), 1);
        assert!(!stats.is_success());
    }
}
