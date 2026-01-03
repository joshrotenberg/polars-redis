//! Redis string type support.
//!
//! This module provides functionality for reading Redis strings as Arrow RecordBatches.

mod batch_iter;
mod convert;
mod reader;

pub use batch_iter::StringBatchIterator;
pub use convert::StringSchema;
