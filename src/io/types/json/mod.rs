//! RedisJSON document support.
//!
//! This module provides functionality for reading RedisJSON documents as Arrow RecordBatches.

mod batch_iter;
#[cfg(feature = "cluster")]
mod cluster_iter;
mod convert;
mod reader;

pub use batch_iter::JsonBatchIterator;
#[cfg(feature = "cluster")]
pub use cluster_iter::ClusterJsonBatchIterator;
pub use convert::JsonSchema;
