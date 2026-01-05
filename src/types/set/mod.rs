//! Redis set type support.
//!
//! This module provides functionality for reading Redis sets as Arrow RecordBatches.
//! Each set member becomes a row, with optional key column to identify which set
//! the member belongs to.

mod batch_iter;
#[cfg(feature = "cluster")]
mod cluster_iter;
mod convert;
mod reader;

pub use batch_iter::SetBatchIterator;
#[cfg(feature = "cluster")]
pub use cluster_iter::ClusterSetBatchIterator;
pub use convert::SetSchema;
