//! DataFrame I/O operations for Redis.
//!
//! This module contains all functionality related to reading and writing
//! DataFrames (Arrow RecordBatches) to and from Redis.
//!
//! ## Submodules
//!
//! - [`types`] - Type-specific iterators and converters for each Redis data type
//! - [`write`] - Functions for writing DataFrames to Redis
//! - [`cache`] - DataFrame caching in Redis
//! - [`search`] - RediSearch query execution
//! - [`infer`] - Schema inference from Redis data
//! - [`scanner`] - Key scanning utilities

pub mod cache;
pub mod infer;
pub mod scanner;
#[cfg(feature = "search")]
pub mod search;
pub mod types;
pub mod write;
