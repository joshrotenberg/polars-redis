//! Redis sorted set (zset) type support.
//!
//! This module provides functionality for reading Redis sorted sets as Arrow RecordBatches.
//! Each member becomes a row with its score, plus optional key and rank columns.

mod batch_iter;
mod convert;
mod reader;

pub use batch_iter::ZSetBatchIterator;
pub use convert::ZSetSchema;
