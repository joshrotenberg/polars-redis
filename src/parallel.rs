//! Parallel batch fetching infrastructure.
//!
//! This module provides utilities for parallelizing Redis fetch operations
//! while maintaining a single SCAN cursor for key discovery.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
//! │  SCAN Keys  │────▶│  Key Buffer  │────▶│  Fetch Workers  │
//! │  (single)   │     │  (channel)   │     │  (N parallel)   │
//! └─────────────┘     └──────────────┘     └─────────────────┘
//!                                                   │
//!                                                   ▼
//!                                          ┌─────────────────┐
//!                                          │  Result Buffer  │
//!                                          │  (channel)      │
//!                                          └─────────────────┘
//! ```
//!
//! # Example
//!
//! ```ignore
//! use polars_redis::parallel::{ParallelFetcher, FetchTask};
//!
//! let fetcher = ParallelFetcher::new(conn, 4); // 4 workers
//! fetcher.submit(keys);
//! let results = fetcher.collect().await?;
//! ```

use std::sync::Arc;

use redis::aio::ConnectionManager;
use tokio::sync::mpsc;

use crate::error::Result;

/// Default channel buffer size for key batches.
const DEFAULT_CHANNEL_SIZE: usize = 16;

/// A batch of keys to be fetched.
#[derive(Debug)]
pub struct KeyBatch {
    /// The keys to fetch.
    pub keys: Vec<String>,
    /// Batch sequence number for ordering.
    pub sequence: u64,
}

/// Result from a fetch operation.
#[derive(Debug)]
pub struct FetchResult<T> {
    /// The fetched data.
    pub data: Vec<T>,
    /// Batch sequence number for ordering.
    pub sequence: u64,
}

/// Configuration for parallel fetching.
#[derive(Debug, Clone)]
pub struct ParallelConfig {
    /// Number of worker tasks.
    pub workers: usize,
    /// Channel buffer size.
    pub channel_size: usize,
    /// Whether to preserve ordering.
    pub preserve_order: bool,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        Self {
            workers: 4,
            channel_size: DEFAULT_CHANNEL_SIZE,
            preserve_order: false,
        }
    }
}

impl ParallelConfig {
    /// Create a new config with the given worker count.
    pub fn new(workers: usize) -> Self {
        Self {
            workers: workers.max(1),
            ..Default::default()
        }
    }

    /// Set the channel buffer size.
    pub fn with_channel_size(mut self, size: usize) -> Self {
        self.channel_size = size;
        self
    }

    /// Enable ordering preservation (slower but deterministic).
    pub fn with_preserve_order(mut self, preserve: bool) -> Self {
        self.preserve_order = preserve;
        self
    }
}

/// Trait for types that can be fetched in parallel.
///
/// Implementors define how to fetch a batch of keys from Redis.
pub trait ParallelFetch: Send + Sync + 'static {
    /// The output type for each fetched item.
    type Output: Send + 'static;

    /// Fetch data for the given keys.
    ///
    /// This is called by worker tasks and should be efficient for batch operations.
    fn fetch(
        &self,
        conn: ConnectionManager,
        keys: Vec<String>,
    ) -> impl std::future::Future<Output = Result<Vec<Self::Output>>> + Send;
}

/// Parallel fetcher that distributes key batches across worker tasks.
pub struct ParallelFetcher<F: ParallelFetch> {
    /// Configuration.
    config: ParallelConfig,
    /// Redis connection (cloned for each worker).
    conn: ConnectionManager,
    /// The fetch implementation.
    fetcher: Arc<F>,
    /// Sender for submitting key batches.
    key_tx: Option<mpsc::Sender<KeyBatch>>,
    /// Receiver for fetch results.
    result_rx: Option<mpsc::Receiver<FetchResult<F::Output>>>,
    /// Next sequence number for batches.
    next_sequence: u64,
}

impl<F: ParallelFetch> ParallelFetcher<F> {
    /// Create a new parallel fetcher.
    pub fn new(conn: ConnectionManager, fetcher: F, config: ParallelConfig) -> Self {
        Self {
            config,
            conn,
            fetcher: Arc::new(fetcher),
            key_tx: None,
            result_rx: None,
            next_sequence: 0,
        }
    }

    /// Start the worker tasks.
    ///
    /// This must be called before submitting batches.
    pub fn start(&mut self) {
        let (key_tx, key_rx) = mpsc::channel::<KeyBatch>(self.config.channel_size);
        let (result_tx, result_rx) =
            mpsc::channel::<FetchResult<F::Output>>(self.config.channel_size);

        // Share the receiver across workers
        let key_rx = Arc::new(tokio::sync::Mutex::new(key_rx));

        // Spawn worker tasks
        for _ in 0..self.config.workers {
            let conn = self.conn.clone();
            let fetcher = Arc::clone(&self.fetcher);
            let key_rx = Arc::clone(&key_rx);
            let result_tx = result_tx.clone();

            tokio::spawn(async move {
                loop {
                    // Get next batch from shared receiver
                    let batch = {
                        let mut rx = key_rx.lock().await;
                        rx.recv().await
                    };

                    match batch {
                        Some(KeyBatch { keys, sequence }) => {
                            // Fetch the data
                            match fetcher.fetch(conn.clone(), keys).await {
                                Ok(data) => {
                                    let _ = result_tx.send(FetchResult { data, sequence }).await;
                                }
                                Err(_e) => {
                                    // Error in fetch, continue processing
                                    // TODO: Consider adding error channel for reporting
                                }
                            }
                        }
                        None => break, // Channel closed, exit worker
                    }
                }
            });
        }

        self.key_tx = Some(key_tx);
        self.result_rx = Some(result_rx);
    }

    /// Submit a batch of keys for fetching.
    pub async fn submit(&mut self, keys: Vec<String>) -> Result<()> {
        if let Some(tx) = &self.key_tx {
            let batch = KeyBatch {
                keys,
                sequence: self.next_sequence,
            };
            self.next_sequence += 1;
            tx.send(batch)
                .await
                .map_err(|_| crate::error::Error::Channel("Channel closed".to_string()))?;
        }
        Ok(())
    }

    /// Close the input channel and signal workers to finish.
    pub fn finish_submitting(&mut self) {
        self.key_tx = None;
    }

    /// Receive the next result.
    pub async fn recv(&mut self) -> Option<FetchResult<F::Output>> {
        if let Some(rx) = &mut self.result_rx {
            rx.recv().await
        } else {
            None
        }
    }

    /// Collect all remaining results.
    pub async fn collect_all(&mut self) -> Vec<FetchResult<F::Output>> {
        let mut results = Vec::new();
        while let Some(result) = self.recv().await {
            results.push(result);
        }

        // Sort by sequence if ordering is required
        if self.config.preserve_order {
            results.sort_by_key(|r| r.sequence);
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parallel_config_default() {
        let config = ParallelConfig::default();
        assert_eq!(config.workers, 4);
        assert_eq!(config.channel_size, DEFAULT_CHANNEL_SIZE);
        assert!(!config.preserve_order);
    }

    #[test]
    fn test_parallel_config_builder() {
        let config = ParallelConfig::new(8)
            .with_channel_size(32)
            .with_preserve_order(true);
        assert_eq!(config.workers, 8);
        assert_eq!(config.channel_size, 32);
        assert!(config.preserve_order);
    }

    #[test]
    fn test_parallel_config_min_workers() {
        let config = ParallelConfig::new(0);
        assert_eq!(config.workers, 1); // Minimum 1 worker
    }

    #[test]
    fn test_key_batch() {
        let batch = KeyBatch {
            keys: vec!["a".to_string(), "b".to_string()],
            sequence: 42,
        };
        assert_eq!(batch.keys.len(), 2);
        assert_eq!(batch.sequence, 42);
    }
}
