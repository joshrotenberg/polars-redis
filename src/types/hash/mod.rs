//! Redis hash type support.
//!
//! This module provides functionality for reading Redis hashes as Arrow RecordBatches.

mod batch_iter;
mod convert;
mod reader;

pub use batch_iter::{BatchConfig, HashBatchIterator};
