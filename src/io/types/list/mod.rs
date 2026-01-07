//! Redis list type support.
//!
//! This module provides functionality for reading Redis lists as Arrow RecordBatches.
//! Each list element becomes a row, with optional key column and position index.

mod batch_iter;
#[cfg(feature = "cluster")]
mod cluster_iter;
mod convert;
mod reader;

pub use batch_iter::ListBatchIterator;
#[cfg(feature = "cluster")]
pub use cluster_iter::ClusterListBatchIterator;
pub use convert::ListSchema;
