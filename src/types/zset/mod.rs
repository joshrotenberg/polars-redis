//! Redis sorted set (zset) type support.
//!
//! This module provides functionality for reading Redis sorted sets as Arrow RecordBatches.
//! Each member becomes a row with its score, plus optional key and rank columns.

mod batch_iter;
#[cfg(feature = "cluster")]
mod cluster_iter;
mod convert;
mod reader;

pub use batch_iter::ZSetBatchIterator;
#[cfg(feature = "cluster")]
pub use cluster_iter::ClusterZSetBatchIterator;
pub use convert::ZSetSchema;
