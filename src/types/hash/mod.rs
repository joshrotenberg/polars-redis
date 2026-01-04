//! Redis hash type support.
//!
//! This module provides functionality for reading Redis hashes as Arrow RecordBatches.

mod batch_iter;
mod convert;
pub(crate) mod reader;
#[cfg(feature = "search")]
mod search_iter;

pub use batch_iter::{BatchConfig, HashBatchIterator};
pub(crate) use reader::HashData;
#[cfg(feature = "search")]
pub use search_iter::{HashSearchIterator, SearchBatchConfig};
