//! RedisTimeSeries type support.
//!
//! This module provides functionality for reading RedisTimeSeries data as Arrow RecordBatches.
//! Each sample becomes a row with timestamp and value columns.

mod batch_iter;
#[cfg(feature = "cluster")]
mod cluster_iter;
mod convert;
mod reader;

pub use batch_iter::TimeSeriesBatchIterator;
#[cfg(feature = "cluster")]
pub use cluster_iter::ClusterTimeSeriesBatchIterator;
pub use convert::TimeSeriesSchema;
