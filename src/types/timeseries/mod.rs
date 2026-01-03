//! RedisTimeSeries type support.
//!
//! This module provides functionality for reading RedisTimeSeries data as Arrow RecordBatches.
//! Each sample becomes a row with timestamp and value columns.

mod batch_iter;
mod convert;
mod reader;

pub use batch_iter::TimeSeriesBatchIterator;
pub use convert::TimeSeriesSchema;
