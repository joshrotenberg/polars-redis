//! Redis Stream type support.
//!
//! This module provides functionality for reading Redis Streams as Arrow RecordBatches.
//! Each stream entry becomes a row, with the entry ID parsed into timestamp and sequence.

mod batch_iter;
#[cfg(feature = "cluster")]
mod cluster_iter;
mod convert;
mod reader;

pub use batch_iter::StreamBatchIterator;
#[cfg(feature = "cluster")]
pub use cluster_iter::ClusterStreamBatchIterator;
pub use convert::StreamSchema;
