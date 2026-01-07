//! Redis hash type support.
//!
//! This module provides functionality for reading Redis hashes as Arrow RecordBatches.

mod batch_iter;
#[cfg(feature = "cluster")]
mod cluster_iter;
mod convert;
pub(crate) mod reader;
#[cfg(feature = "search")]
mod search_iter;

pub use batch_iter::{BatchConfig, HashBatchIterator};
#[cfg(feature = "cluster")]
pub use cluster_iter::ClusterHashBatchIterator;
#[cfg(feature = "cluster")]
pub use reader::ClusterHashFetcher;
pub(crate) use reader::HashData;
pub use reader::HashFetcher;
#[cfg(feature = "search")]
pub use search_iter::{HashSearchIterator, SearchBatchConfig};
