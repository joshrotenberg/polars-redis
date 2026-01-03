//! RedisJSON document support.
//!
//! This module provides functionality for reading RedisJSON documents as Arrow RecordBatches.

mod batch_iter;
mod convert;
mod reader;

pub use batch_iter::JsonBatchIterator;
pub use convert::JsonSchema;
