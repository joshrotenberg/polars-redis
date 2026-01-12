//! Redis Stream type support.
//!
//! This module provides functionality for reading Redis Streams as Arrow RecordBatches.
//! Each stream entry becomes a row, with the entry ID parsed into timestamp and sequence.
//!
//! ## Consumer Group Support
//!
//! The `consumer` submodule provides `StreamConsumer` for reading streams with consumer
//! groups, checkpointing, and entry acknowledgment.

mod batch_iter;
#[cfg(feature = "cluster")]
mod cluster_iter;
mod consumer;
mod convert;
mod reader;

pub use batch_iter::StreamBatchIterator;
#[cfg(feature = "cluster")]
pub use cluster_iter::ClusterStreamBatchIterator;
pub use consumer::{
    CheckpointStore, ConsumerConfig, ConsumerStats, MemoryCheckpointStore, RedisCheckpointStore,
    StreamConsumer, create_consumer_group, destroy_consumer_group, stream_ack,
};
pub use convert::StreamSchema;
