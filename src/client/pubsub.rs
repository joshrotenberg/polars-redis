//! Redis Pub/Sub support for collecting messages into Arrow RecordBatches.
//!
//! This module provides functions for subscribing to Redis Pub/Sub channels
//! and accumulating messages into DataFrames for real-time analytics.
//!
//! # Example
//!
//! ```no_run
//! use polars_redis::client::pubsub::{collect_pubsub, PubSubConfig};
//!
//! // Collect 100 messages from a channel
//! let config = PubSubConfig::new(vec!["events".to_string()])
//!     .with_count(100)
//!     .with_timeout_ms(5000);
//!
//! let batch = collect_pubsub("redis://localhost:6379", &config).unwrap();
//! println!("Collected {} messages", batch.num_rows());
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{RecordBatch, StringBuilder, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use futures::StreamExt;

use crate::error::{Error, Result};

/// Configuration for Pub/Sub message collection.
#[derive(Debug, Clone)]
pub struct PubSubConfig {
    /// Channel names or patterns to subscribe to.
    pub channels: Vec<String>,
    /// Whether channels are patterns (PSUBSCRIBE vs SUBSCRIBE).
    pub pattern_subscribe: bool,
    /// Maximum number of messages to collect.
    pub count: Option<usize>,
    /// Timeout in milliseconds.
    pub timeout_ms: Option<u64>,
    /// Time window for collection in seconds.
    pub window_seconds: Option<f64>,
    /// Whether to include the channel name as a column.
    pub include_channel: bool,
    /// Whether to include the receive timestamp as a column.
    pub include_timestamp: bool,
    /// Column name for the channel.
    pub channel_column_name: String,
    /// Column name for the message payload.
    pub message_column_name: String,
    /// Column name for the timestamp.
    pub timestamp_column_name: String,
}

impl PubSubConfig {
    /// Create a new PubSubConfig with the given channels.
    pub fn new(channels: Vec<String>) -> Self {
        Self {
            channels,
            pattern_subscribe: false,
            count: None,
            timeout_ms: None,
            window_seconds: None,
            include_channel: false,
            include_timestamp: false,
            channel_column_name: "_channel".to_string(),
            message_column_name: "message".to_string(),
            timestamp_column_name: "_received_at".to_string(),
        }
    }

    /// Use pattern subscription (PSUBSCRIBE).
    pub fn with_pattern(mut self) -> Self {
        self.pattern_subscribe = true;
        self
    }

    /// Set maximum number of messages to collect.
    pub fn with_count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }

    /// Set timeout in milliseconds.
    pub fn with_timeout_ms(mut self, ms: u64) -> Self {
        self.timeout_ms = Some(ms);
        self
    }

    /// Set time window for collection in seconds.
    pub fn with_window_seconds(mut self, seconds: f64) -> Self {
        self.window_seconds = Some(seconds);
        self
    }

    /// Include the channel name as a column.
    pub fn with_channel_column(mut self) -> Self {
        self.include_channel = true;
        self
    }

    /// Include the receive timestamp as a column.
    pub fn with_timestamp_column(mut self) -> Self {
        self.include_timestamp = true;
        self
    }

    /// Set column names.
    pub fn with_column_names(
        mut self,
        channel: impl Into<String>,
        message: impl Into<String>,
        timestamp: impl Into<String>,
    ) -> Self {
        self.channel_column_name = channel.into();
        self.message_column_name = message.into();
        self.timestamp_column_name = timestamp.into();
        self
    }

    /// Build the Arrow schema for the output.
    pub fn build_schema(&self) -> Schema {
        let mut fields = Vec::new();

        if self.include_channel {
            fields.push(Field::new(&self.channel_column_name, DataType::Utf8, false));
        }

        if self.include_timestamp {
            fields.push(Field::new(
                &self.timestamp_column_name,
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ));
        }

        fields.push(Field::new(&self.message_column_name, DataType::Utf8, true));

        Schema::new(fields)
    }
}

/// A collected Pub/Sub message.
#[derive(Debug, Clone)]
pub struct PubSubMessage {
    /// The channel the message was received on.
    pub channel: String,
    /// The message payload.
    pub payload: String,
    /// Timestamp when the message was received (microseconds since epoch).
    pub received_at: i64,
}

/// Collect messages from Redis Pub/Sub channels into a RecordBatch.
///
/// Subscribes to the specified channels and collects messages until:
/// - The specified count is reached
/// - The timeout expires
/// - The time window ends
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `config` - Collection configuration
///
/// # Returns
/// A RecordBatch containing the collected messages.
pub fn collect_pubsub(url: &str, config: &PubSubConfig) -> Result<RecordBatch> {
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    rt.block_on(async { collect_pubsub_async(url, config).await })
}

async fn collect_pubsub_async(url: &str, config: &PubSubConfig) -> Result<RecordBatch> {
    let client = redis::Client::open(url)
        .map_err(|e| Error::Runtime(format!("Failed to open Redis client: {}", e)))?;

    let mut pubsub = client
        .get_async_pubsub()
        .await
        .map_err(|e| Error::Runtime(format!("Failed to get pubsub connection: {}", e)))?;

    // Subscribe to channels
    for channel in &config.channels {
        if config.pattern_subscribe {
            pubsub.psubscribe(channel).await.map_err(|e| {
                Error::Runtime(format!("Failed to psubscribe to {}: {}", channel, e))
            })?;
        } else {
            pubsub.subscribe(channel).await.map_err(|e| {
                Error::Runtime(format!("Failed to subscribe to {}: {}", channel, e))
            })?;
        }
    }

    let mut messages: Vec<PubSubMessage> = Vec::new();
    let start_time = Instant::now();

    // Calculate end conditions
    let max_count = config.count.unwrap_or(usize::MAX);
    let timeout = config.timeout_ms.map(Duration::from_millis);
    let window = config.window_seconds.map(Duration::from_secs_f64);

    // Use the more restrictive of timeout and window
    let deadline = match (timeout, window) {
        (Some(t), Some(w)) => Some(start_time + t.min(w)),
        (Some(t), None) => Some(start_time + t),
        (None, Some(w)) => Some(start_time + w),
        (None, None) => None,
    };

    let mut stream = pubsub.on_message();

    loop {
        // Check if we've collected enough messages
        if messages.len() >= max_count {
            break;
        }

        // Calculate remaining time
        let remaining = deadline.map(|d| d.saturating_duration_since(Instant::now()));
        if let Some(rem) = remaining
            && rem.is_zero()
        {
            break;
        }

        // Wait for next message with timeout
        let msg_future = stream.next();
        let result = if let Some(rem) = remaining {
            tokio::time::timeout(rem, msg_future).await
        } else {
            // No deadline - wait indefinitely but with a reasonable poll interval
            tokio::time::timeout(Duration::from_secs(60), msg_future).await
        };

        match result {
            Ok(Some(msg)) => {
                let channel = msg.get_channel_name().to_string();
                let payload = msg.get_payload::<String>().unwrap_or_default();
                let received_at = chrono::Utc::now().timestamp_micros();

                messages.push(PubSubMessage {
                    channel,
                    payload,
                    received_at,
                });
            },
            Ok(None) => {
                // Stream ended
                break;
            },
            Err(_) => {
                // Timeout - check if we should continue
                if deadline.is_some() {
                    break;
                }
            },
        }
    }

    // Build RecordBatch from collected messages
    build_record_batch(&messages, config)
}

fn build_record_batch(messages: &[PubSubMessage], config: &PubSubConfig) -> Result<RecordBatch> {
    let schema = Arc::new(config.build_schema());
    let mut arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::new();

    if config.include_channel {
        let mut builder = StringBuilder::new();
        for msg in messages {
            builder.append_value(&msg.channel);
        }
        arrays.push(Arc::new(builder.finish()));
    }

    if config.include_timestamp {
        let timestamps: Vec<i64> = messages.iter().map(|m| m.received_at).collect();
        arrays.push(Arc::new(TimestampMicrosecondArray::from(timestamps)));
    }

    // Message column
    let mut msg_builder = StringBuilder::new();
    for msg in messages {
        msg_builder.append_value(&msg.payload);
    }
    arrays.push(Arc::new(msg_builder.finish()));

    RecordBatch::try_new(schema, arrays)
        .map_err(|e| Error::Runtime(format!("Failed to create RecordBatch: {}", e)))
}

/// Statistics about a Pub/Sub collection session.
#[derive(Debug, Clone)]
pub struct PubSubStats {
    /// Number of messages collected.
    pub message_count: usize,
    /// Duration of the collection session.
    pub duration_ms: u64,
    /// Messages per second.
    pub messages_per_second: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pubsub_config_builder() {
        let config = PubSubConfig::new(vec!["test".to_string()])
            .with_pattern()
            .with_count(100)
            .with_timeout_ms(5000)
            .with_channel_column()
            .with_timestamp_column();

        assert!(config.pattern_subscribe);
        assert_eq!(config.count, Some(100));
        assert_eq!(config.timeout_ms, Some(5000));
        assert!(config.include_channel);
        assert!(config.include_timestamp);
    }

    #[test]
    fn test_build_schema() {
        let config = PubSubConfig::new(vec!["test".to_string()])
            .with_channel_column()
            .with_timestamp_column();

        let schema = config.build_schema();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "_channel");
        assert_eq!(schema.field(1).name(), "_received_at");
        assert_eq!(schema.field(2).name(), "message");
    }

    #[test]
    fn test_build_schema_minimal() {
        let config = PubSubConfig::new(vec!["test".to_string()]);

        let schema = config.build_schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "message");
    }

    #[test]
    fn test_build_record_batch_empty() {
        let config = PubSubConfig::new(vec!["test".to_string()]);
        let messages: Vec<PubSubMessage> = vec![];

        let batch = build_record_batch(&messages, &config).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 1);
    }

    #[test]
    fn test_build_record_batch_with_messages() {
        let config = PubSubConfig::new(vec!["test".to_string()])
            .with_channel_column()
            .with_timestamp_column();

        let messages = vec![
            PubSubMessage {
                channel: "events".to_string(),
                payload: "hello".to_string(),
                received_at: 1000000,
            },
            PubSubMessage {
                channel: "events".to_string(),
                payload: "world".to_string(),
                received_at: 2000000,
            },
        ];

        let batch = build_record_batch(&messages, &config).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);
    }
}
