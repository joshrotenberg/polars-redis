//! General Redis client operations.
//!
//! This module contains Redis operations that don't necessarily return
//! DataFrames but provide ergonomic access to Redis functionality.
//!
//! ## Submodules
//!
//! - [`geo`] - Geospatial operations (GEOADD, GEOSEARCH, etc.)
//! - [`keys`] - Key management (TTL, delete, rename, info)
//! - [`pipeline`] - Transaction and pipeline support
//! - [`pubsub`] - Pub/Sub message collection

pub mod geo;
pub mod keys;
pub mod pipeline;
pub mod pubsub;
