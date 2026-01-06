//! Redis string type support.
//!
//! This module provides functionality for reading Redis strings as Arrow RecordBatches.

mod batch_iter;
#[cfg(feature = "cluster")]
mod cluster_iter;
mod convert;
mod reader;

pub use batch_iter::StringBatchIterator;
#[cfg(feature = "cluster")]
pub use cluster_iter::ClusterStringBatchIterator;
pub use convert::StringSchema;
