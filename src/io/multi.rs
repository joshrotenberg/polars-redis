//! Multi-cluster write operations.
//!
//! This module provides functionality for writing data to multiple Redis
//! clusters or instances in parallel.
//!
//! # Example
//!
//! ```ignore
//! use polars_redis::{MultiClusterWriter, Destination, WriteMode};
//!
//! let writer = MultiClusterWriter::new(vec![
//!     Destination::new("redis://cluster1:6379").with_name("primary"),
//!     Destination::new("redis://cluster2:6379").with_name("secondary"),
//! ]);
//!
//! let results = writer.write_hashes(
//!     keys,
//!     fields,
//!     values,
//!     Some(3600),
//!     WriteMode::Replace,
//! )?;
//!
//! for result in results {
//!     println!("{}: {} keys written", result.destination_name, result.keys_written);
//! }
//! ```

use std::time::{Duration, Instant};

use tokio::runtime::Runtime;

use crate::error::{Error, Result};
use crate::io::write::{WriteMode, WriteResult, write_hashes, write_json};

/// Configuration for a write destination.
#[derive(Debug, Clone)]
pub struct Destination {
    /// Redis connection URL.
    pub url: String,
    /// Optional name for this destination.
    pub name: Option<String>,
    /// Key prefix to prepend to all keys.
    pub key_prefix: String,
    /// Timeout in milliseconds for write operations.
    pub timeout_ms: u64,
}

impl Destination {
    /// Create a new destination with the given URL.
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            name: None,
            key_prefix: String::new(),
            timeout_ms: 30000,
        }
    }

    /// Set the destination name.
    pub fn with_name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// Set the key prefix.
    pub fn with_key_prefix(mut self, prefix: &str) -> Self {
        self.key_prefix = prefix.to_string();
        self
    }

    /// Set the timeout in milliseconds.
    pub fn with_timeout_ms(mut self, ms: u64) -> Self {
        self.timeout_ms = ms;
        self
    }

    /// Get the display name for this destination.
    pub fn display_name(&self) -> String {
        self.name.clone().unwrap_or_else(|| self.url.clone())
    }

    /// Apply the key prefix to a key.
    pub fn prefix_key(&self, key: &str) -> String {
        if self.key_prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}{}", self.key_prefix, key)
        }
    }
}

/// Result of writing to a single destination.
#[derive(Debug, Clone)]
pub struct DestinationResult {
    /// Name of the destination.
    pub destination_name: String,
    /// URL of the destination.
    pub destination_url: String,
    /// Number of keys successfully written.
    pub keys_written: usize,
    /// Number of keys that failed to write.
    pub keys_failed: usize,
    /// Whether the write was successful overall.
    pub success: bool,
    /// Error message if the write failed.
    pub error: Option<String>,
    /// Duration of the write operation in milliseconds.
    pub duration_ms: f64,
}

impl DestinationResult {
    /// Create a successful result.
    pub fn success(dest: &Destination, write_result: &WriteResult, duration: Duration) -> Self {
        Self {
            destination_name: dest.display_name(),
            destination_url: dest.url.clone(),
            keys_written: write_result.keys_written,
            keys_failed: write_result.keys_failed,
            success: true,
            error: None,
            duration_ms: duration.as_secs_f64() * 1000.0,
        }
    }

    /// Create a failed result.
    pub fn failure(dest: &Destination, error: String, duration: Duration) -> Self {
        Self {
            destination_name: dest.display_name(),
            destination_url: dest.url.clone(),
            keys_written: 0,
            keys_failed: 0,
            success: false,
            error: Some(error),
            duration_ms: duration.as_secs_f64() * 1000.0,
        }
    }
}

/// Result of a multi-cluster write operation.
#[derive(Debug, Clone)]
pub struct MultiWriteResult {
    /// Results for each destination.
    pub results: Vec<DestinationResult>,
    /// Total keys written across all destinations.
    pub total_keys_written: usize,
    /// Total keys failed across all destinations.
    pub total_keys_failed: usize,
    /// Number of destinations that succeeded.
    pub destinations_succeeded: usize,
    /// Number of destinations that failed.
    pub destinations_failed: usize,
    /// Total duration in milliseconds.
    pub duration_ms: f64,
}

impl MultiWriteResult {
    /// Create a new result from destination results.
    pub fn from_results(results: Vec<DestinationResult>, total_duration: Duration) -> Self {
        let total_keys_written = results.iter().map(|r| r.keys_written).sum();
        let total_keys_failed = results.iter().map(|r| r.keys_failed).sum();
        let destinations_succeeded = results.iter().filter(|r| r.success).count();
        let destinations_failed = results.iter().filter(|r| !r.success).count();

        Self {
            results,
            total_keys_written,
            total_keys_failed,
            destinations_succeeded,
            destinations_failed,
            duration_ms: total_duration.as_secs_f64() * 1000.0,
        }
    }

    /// Check if all destinations succeeded.
    pub fn all_succeeded(&self) -> bool {
        self.destinations_failed == 0
    }
}

/// Writer for multiple Redis clusters/instances.
///
/// Executes writes to multiple destinations in parallel.
pub struct MultiClusterWriter {
    /// Destinations to write to.
    destinations: Vec<Destination>,
    /// Tokio runtime for async operations.
    runtime: Runtime,
}

impl MultiClusterWriter {
    /// Create a new multi-cluster writer.
    pub fn new(destinations: Vec<Destination>) -> Result<Self> {
        let runtime = Runtime::new()
            .map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

        Ok(Self {
            destinations,
            runtime,
        })
    }

    /// Get the destinations.
    pub fn destinations(&self) -> &[Destination] {
        &self.destinations
    }

    /// Write hashes to all destinations in parallel.
    ///
    /// # Arguments
    /// * `keys` - Keys to write
    /// * `fields` - Field names
    /// * `values` - Values for each key (outer) and field (inner)
    /// * `ttl` - Optional TTL in seconds
    /// * `mode` - Write mode
    pub fn write_hashes(
        &self,
        keys: Vec<String>,
        fields: Vec<String>,
        values: Vec<Vec<Option<String>>>,
        ttl: Option<i64>,
        mode: WriteMode,
    ) -> Result<MultiWriteResult> {
        let start = Instant::now();

        let results: Vec<DestinationResult> = self.runtime.block_on(async {
            let futures: Vec<_> = self
                .destinations
                .iter()
                .map(|dest| {
                    let dest = dest.clone();
                    let keys = keys.clone();
                    let fields = fields.clone();
                    let values = values.clone();

                    async move {
                        let op_start = Instant::now();

                        // Apply key prefix
                        let prefixed_keys: Vec<String> =
                            keys.iter().map(|k| dest.prefix_key(k)).collect();

                        // Clone URL before moving dest
                        let url = dest.url.clone();
                        let timeout = dest.timeout_ms;

                        // Execute write with timeout
                        let result = tokio::time::timeout(
                            Duration::from_millis(timeout),
                            tokio::task::spawn_blocking(move || {
                                write_hashes(&url, prefixed_keys, fields, values, ttl, mode)
                            }),
                        )
                        .await;

                        let duration = op_start.elapsed();

                        match result {
                            Ok(Ok(Ok(write_result))) => {
                                DestinationResult::success(&dest, &write_result, duration)
                            },
                            Ok(Ok(Err(e))) => {
                                DestinationResult::failure(&dest, e.to_string(), duration)
                            },
                            Ok(Err(e)) => DestinationResult::failure(
                                &dest,
                                format!("Task panic: {}", e),
                                duration,
                            ),
                            Err(_) => DestinationResult::failure(
                                &dest,
                                format!("Timeout after {}ms", dest.timeout_ms),
                                duration,
                            ),
                        }
                    }
                })
                .collect();

            futures::future::join_all(futures).await
        });

        Ok(MultiWriteResult::from_results(results, start.elapsed()))
    }

    /// Write JSON documents to all destinations in parallel.
    ///
    /// # Arguments
    /// * `keys` - Keys to write
    /// * `documents` - JSON documents as strings
    /// * `ttl` - Optional TTL in seconds
    /// * `mode` - Write mode
    pub fn write_json(
        &self,
        keys: Vec<String>,
        documents: Vec<String>,
        ttl: Option<i64>,
        mode: WriteMode,
    ) -> Result<MultiWriteResult> {
        let start = Instant::now();

        let results: Vec<DestinationResult> = self.runtime.block_on(async {
            let futures: Vec<_> = self
                .destinations
                .iter()
                .map(|dest| {
                    let dest = dest.clone();
                    let keys = keys.clone();
                    let documents = documents.clone();

                    async move {
                        let op_start = Instant::now();

                        // Apply key prefix
                        let prefixed_keys: Vec<String> =
                            keys.iter().map(|k| dest.prefix_key(k)).collect();

                        // Clone URL before moving dest
                        let url = dest.url.clone();
                        let timeout = dest.timeout_ms;

                        // Execute write with timeout
                        let result = tokio::time::timeout(
                            Duration::from_millis(timeout),
                            tokio::task::spawn_blocking(move || {
                                write_json(&url, prefixed_keys, documents, ttl, mode)
                            }),
                        )
                        .await;

                        let duration = op_start.elapsed();

                        match result {
                            Ok(Ok(Ok(write_result))) => {
                                DestinationResult::success(&dest, &write_result, duration)
                            },
                            Ok(Ok(Err(e))) => {
                                DestinationResult::failure(&dest, e.to_string(), duration)
                            },
                            Ok(Err(e)) => DestinationResult::failure(
                                &dest,
                                format!("Task panic: {}", e),
                                duration,
                            ),
                            Err(_) => DestinationResult::failure(
                                &dest,
                                format!("Timeout after {}ms", dest.timeout_ms),
                                duration,
                            ),
                        }
                    }
                })
                .collect();

            futures::future::join_all(futures).await
        });

        Ok(MultiWriteResult::from_results(results, start.elapsed()))
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_destination_new() {
        let dest = Destination::new("redis://localhost:6379");
        assert_eq!(dest.url, "redis://localhost:6379");
        assert!(dest.name.is_none());
        assert!(dest.key_prefix.is_empty());
        assert_eq!(dest.timeout_ms, 30000);
    }

    #[test]
    fn test_destination_builder() {
        let dest = Destination::new("redis://localhost:6379")
            .with_name("primary")
            .with_key_prefix("app:")
            .with_timeout_ms(5000);

        assert_eq!(dest.url, "redis://localhost:6379");
        assert_eq!(dest.name, Some("primary".to_string()));
        assert_eq!(dest.key_prefix, "app:");
        assert_eq!(dest.timeout_ms, 5000);
    }

    #[test]
    fn test_destination_display_name() {
        let dest1 = Destination::new("redis://localhost:6379");
        assert_eq!(dest1.display_name(), "redis://localhost:6379");

        let dest2 = Destination::new("redis://localhost:6379").with_name("primary");
        assert_eq!(dest2.display_name(), "primary");
    }

    #[test]
    fn test_destination_prefix_key() {
        let dest1 = Destination::new("redis://localhost:6379");
        assert_eq!(dest1.prefix_key("user:1"), "user:1");

        let dest2 = Destination::new("redis://localhost:6379").with_key_prefix("app:");
        assert_eq!(dest2.prefix_key("user:1"), "app:user:1");
    }

    #[test]
    fn test_destination_result_success() {
        let dest = Destination::new("redis://localhost:6379").with_name("test");
        let write_result = WriteResult {
            keys_written: 10,
            keys_failed: 0,
            keys_skipped: 0,
        };
        let duration = Duration::from_millis(100);

        let result = DestinationResult::success(&dest, &write_result, duration);

        assert_eq!(result.destination_name, "test");
        assert_eq!(result.keys_written, 10);
        assert_eq!(result.keys_failed, 0);
        assert!(result.success);
        assert!(result.error.is_none());
        assert!(result.duration_ms >= 100.0);
    }

    #[test]
    fn test_destination_result_failure() {
        let dest = Destination::new("redis://localhost:6379").with_name("test");
        let duration = Duration::from_millis(50);

        let result = DestinationResult::failure(&dest, "Connection refused".to_string(), duration);

        assert_eq!(result.destination_name, "test");
        assert_eq!(result.keys_written, 0);
        assert!(!result.success);
        assert_eq!(result.error, Some("Connection refused".to_string()));
    }

    #[test]
    fn test_multi_write_result() {
        let results = vec![
            DestinationResult {
                destination_name: "primary".to_string(),
                destination_url: "redis://host1:6379".to_string(),
                keys_written: 100,
                keys_failed: 5,
                success: true,
                error: None,
                duration_ms: 50.0,
            },
            DestinationResult {
                destination_name: "secondary".to_string(),
                destination_url: "redis://host2:6379".to_string(),
                keys_written: 0,
                keys_failed: 0,
                success: false,
                error: Some("Connection refused".to_string()),
                duration_ms: 10.0,
            },
        ];

        let multi_result = MultiWriteResult::from_results(results, Duration::from_millis(100));

        assert_eq!(multi_result.total_keys_written, 100);
        assert_eq!(multi_result.total_keys_failed, 5);
        assert_eq!(multi_result.destinations_succeeded, 1);
        assert_eq!(multi_result.destinations_failed, 1);
        assert!(!multi_result.all_succeeded());
    }

    #[test]
    fn test_multi_write_result_all_succeeded() {
        let results = vec![
            DestinationResult {
                destination_name: "primary".to_string(),
                destination_url: "redis://host1:6379".to_string(),
                keys_written: 100,
                keys_failed: 0,
                success: true,
                error: None,
                duration_ms: 50.0,
            },
            DestinationResult {
                destination_name: "secondary".to_string(),
                destination_url: "redis://host2:6379".to_string(),
                keys_written: 100,
                keys_failed: 0,
                success: true,
                error: None,
                duration_ms: 45.0,
            },
        ];

        let multi_result = MultiWriteResult::from_results(results, Duration::from_millis(60));

        assert_eq!(multi_result.total_keys_written, 200);
        assert_eq!(multi_result.destinations_succeeded, 2);
        assert_eq!(multi_result.destinations_failed, 0);
        assert!(multi_result.all_succeeded());
    }

    #[test]
    fn test_multi_cluster_writer_creation() {
        let destinations = vec![
            Destination::new("redis://localhost:6379").with_name("primary"),
            Destination::new("redis://localhost:6380").with_name("secondary"),
        ];

        let writer = MultiClusterWriter::new(destinations);
        assert!(writer.is_ok());

        let writer = writer.unwrap();
        assert_eq!(writer.destinations().len(), 2);
    }
}
