//! Hash reading operations for Redis.
//!
//! This module provides functionality for reading Redis hashes in batches,
//! with support for projection pushdown (fetching only specific fields).
//!
//! # Connection Support
//!
//! The module supports both single-node Redis and Redis Cluster:
//! - Single-node: Use functions with `ConnectionManager`
//! - Cluster: Use functions with `ClusterConnection` (with `cluster` feature)
//!
//! For a unified interface, use the `RedisConn` enum with cluster-aware functions.

use std::collections::HashMap;

use redis::aio::ConnectionManager;
#[cfg(feature = "cluster")]
use redis::cluster_async::ClusterConnection;

use crate::error::Result;
use crate::parallel::ParallelFetch;

/// Result of fetching a single hash from Redis.
#[derive(Debug, Clone)]
pub struct HashData {
    /// The Redis key.
    pub key: String,
    /// Field-value pairs from the hash.
    pub fields: HashMap<String, Option<String>>,
    /// TTL in seconds (-1 = no expiry, -2 = key doesn't exist, None = TTL not requested).
    pub ttl: Option<i64>,
}

/// Fetch all fields from multiple hashes using HGETALL.
///
/// Uses pipelining for efficiency. Returns data in the same order as the input keys.
/// If a key doesn't exist or isn't a hash, returns an empty HashMap for that key.
pub async fn fetch_hashes_all(
    conn: &mut ConnectionManager,
    keys: &[String],
    include_ttl: bool,
) -> Result<Vec<HashData>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut pipe = redis::pipe();
    for key in keys {
        pipe.hgetall(key);
    }

    let results: Vec<HashMap<String, String>> = pipe.query_async(conn).await?;

    // Optionally fetch TTLs
    let ttls = if include_ttl {
        fetch_ttls(conn, keys).await?
    } else {
        vec![None; keys.len()]
    };

    Ok(keys
        .iter()
        .zip(results)
        .zip(ttls)
        .map(|((key, fields), ttl)| HashData {
            key: key.clone(),
            fields: fields.into_iter().map(|(k, v)| (k, Some(v))).collect(),
            ttl,
        })
        .collect())
}

/// Fetch specific fields from multiple hashes using HMGET.
///
/// This enables projection pushdown - only fetching the fields we need.
/// Uses pipelining for efficiency. Returns data in the same order as the input keys.
/// Missing fields are represented as None.
pub async fn fetch_hashes_fields(
    conn: &mut ConnectionManager,
    keys: &[String],
    fields: &[String],
    include_ttl: bool,
) -> Result<Vec<HashData>> {
    if keys.is_empty() || fields.is_empty() {
        return Ok(Vec::new());
    }

    let mut pipe = redis::pipe();
    for key in keys {
        pipe.cmd("HMGET").arg(key).arg(fields);
    }

    // HMGET returns Vec<Option<String>> for each key
    let results: Vec<Vec<Option<String>>> = pipe.query_async(conn).await?;

    // Optionally fetch TTLs
    let ttls = if include_ttl {
        fetch_ttls(conn, keys).await?
    } else {
        vec![None; keys.len()]
    };

    Ok(keys
        .iter()
        .zip(results)
        .zip(ttls)
        .map(|((key, values), ttl)| {
            let field_map: HashMap<String, Option<String>> = fields
                .iter()
                .zip(values)
                .map(|(field, value)| (field.clone(), value))
                .collect();

            HashData {
                key: key.clone(),
                fields: field_map,
                ttl,
            }
        })
        .collect())
}

/// Fetch TTLs for multiple keys using pipelining.
///
/// Returns a vector of Option<i64> where:
/// - Some(ttl) where ttl >= 0: key has TTL in seconds
/// - Some(-1): key exists but has no expiry
/// - Some(-2): key doesn't exist
async fn fetch_ttls(conn: &mut ConnectionManager, keys: &[String]) -> Result<Vec<Option<i64>>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut pipe = redis::pipe();
    for key in keys {
        pipe.cmd("TTL").arg(key);
    }

    let results: Vec<i64> = pipe.query_async(conn).await?;
    Ok(results.into_iter().map(Some).collect())
}

/// Fetch hash data with optional projection pushdown.
///
/// If `fields` is Some, uses HMGET to fetch only those fields.
/// If `fields` is None, uses HGETALL to fetch all fields.
pub async fn fetch_hashes(
    conn: &mut ConnectionManager,
    keys: &[String],
    fields: Option<&[String]>,
    include_ttl: bool,
) -> Result<Vec<HashData>> {
    match fields {
        Some(f) => fetch_hashes_fields(conn, keys, f, include_ttl).await,
        None => fetch_hashes_all(conn, keys, include_ttl).await,
    }
}

// ============================================================================
// Cluster support (with cluster feature)
// ============================================================================

/// Fetch all fields from multiple hashes using HGETALL on a cluster.
///
/// Uses pipelining for efficiency. The ClusterConnection automatically routes
/// each command to the correct node based on key hash slots.
#[cfg(feature = "cluster")]
pub async fn fetch_hashes_all_cluster(
    conn: &mut ClusterConnection,
    keys: &[String],
    include_ttl: bool,
) -> Result<Vec<HashData>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut pipe = redis::pipe();
    for key in keys {
        pipe.hgetall(key);
    }

    let results: Vec<HashMap<String, String>> = pipe.query_async(conn).await?;

    // Optionally fetch TTLs
    let ttls = if include_ttl {
        fetch_ttls_cluster(conn, keys).await?
    } else {
        vec![None; keys.len()]
    };

    Ok(keys
        .iter()
        .zip(results)
        .zip(ttls)
        .map(|((key, fields), ttl)| HashData {
            key: key.clone(),
            fields: fields.into_iter().map(|(k, v)| (k, Some(v))).collect(),
            ttl,
        })
        .collect())
}

/// Fetch specific fields from multiple hashes using HMGET on a cluster.
#[cfg(feature = "cluster")]
pub async fn fetch_hashes_fields_cluster(
    conn: &mut ClusterConnection,
    keys: &[String],
    fields: &[String],
    include_ttl: bool,
) -> Result<Vec<HashData>> {
    if keys.is_empty() || fields.is_empty() {
        return Ok(Vec::new());
    }

    let mut pipe = redis::pipe();
    for key in keys {
        pipe.cmd("HMGET").arg(key).arg(fields);
    }

    let results: Vec<Vec<Option<String>>> = pipe.query_async(conn).await?;

    let ttls = if include_ttl {
        fetch_ttls_cluster(conn, keys).await?
    } else {
        vec![None; keys.len()]
    };

    Ok(keys
        .iter()
        .zip(results)
        .zip(ttls)
        .map(|((key, values), ttl)| {
            let field_map: HashMap<String, Option<String>> = fields
                .iter()
                .zip(values)
                .map(|(field, value)| (field.clone(), value))
                .collect();

            HashData {
                key: key.clone(),
                fields: field_map,
                ttl,
            }
        })
        .collect())
}

/// Fetch TTLs for multiple keys on a cluster.
#[cfg(feature = "cluster")]
async fn fetch_ttls_cluster(
    conn: &mut ClusterConnection,
    keys: &[String],
) -> Result<Vec<Option<i64>>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut pipe = redis::pipe();
    for key in keys {
        pipe.cmd("TTL").arg(key);
    }

    let results: Vec<i64> = pipe.query_async(conn).await?;
    Ok(results.into_iter().map(Some).collect())
}

/// Fetch hash data from a cluster with optional projection pushdown.
#[cfg(feature = "cluster")]
pub async fn fetch_hashes_cluster(
    conn: &mut ClusterConnection,
    keys: &[String],
    fields: Option<&[String]>,
    include_ttl: bool,
) -> Result<Vec<HashData>> {
    match fields {
        Some(f) => fetch_hashes_fields_cluster(conn, keys, f, include_ttl).await,
        None => fetch_hashes_all_cluster(conn, keys, include_ttl).await,
    }
}

// ============================================================================
// Parallel fetch implementation
// ============================================================================

/// Fetcher for parallel hash operations.
///
/// Implements the `ParallelFetch` trait to enable parallel batch fetching
/// of Redis hashes across multiple worker tasks.
///
/// # Example
///
/// ```ignore
/// use polars_redis::parallel::{ParallelFetcher, ParallelConfig};
/// use polars_redis::io::types::hash::reader::HashFetcher;
///
/// let fetcher = HashFetcher::new(None, false); // All fields, no TTL
/// let mut parallel = ParallelFetcher::new(conn, fetcher, ParallelConfig::new(4));
/// parallel.start();
/// parallel.submit(keys).await?;
/// ```
#[derive(Debug, Clone)]
pub struct HashFetcher {
    /// Fields to fetch (None = all fields via HGETALL).
    fields: Option<Vec<String>>,
    /// Whether to include TTL information.
    include_ttl: bool,
}

impl HashFetcher {
    /// Create a new hash fetcher.
    ///
    /// # Arguments
    ///
    /// * `fields` - Specific fields to fetch, or None for all fields
    /// * `include_ttl` - Whether to fetch TTL for each key
    pub fn new(fields: Option<Vec<String>>, include_ttl: bool) -> Self {
        Self {
            fields,
            include_ttl,
        }
    }

    /// Create a fetcher for all fields.
    pub fn all_fields() -> Self {
        Self::new(None, false)
    }

    /// Create a fetcher with projection.
    pub fn with_fields(fields: Vec<String>) -> Self {
        Self::new(Some(fields), false)
    }

    /// Enable TTL fetching.
    pub fn with_ttl(mut self) -> Self {
        self.include_ttl = true;
        self
    }
}

impl ParallelFetch for HashFetcher {
    type Output = HashData;

    async fn fetch(
        &self,
        mut conn: ConnectionManager,
        keys: Vec<String>,
    ) -> Result<Vec<Self::Output>> {
        fetch_hashes(&mut conn, &keys, self.fields.as_deref(), self.include_ttl).await
    }
}

/// Fetcher for parallel hash operations on a cluster.
#[cfg(feature = "cluster")]
#[derive(Debug, Clone)]
pub struct ClusterHashFetcher {
    /// Fields to fetch (None = all fields via HGETALL).
    fields: Option<Vec<String>>,
    /// Whether to include TTL information.
    include_ttl: bool,
}

#[cfg(feature = "cluster")]
impl ClusterHashFetcher {
    /// Create a new cluster hash fetcher.
    pub fn new(fields: Option<Vec<String>>, include_ttl: bool) -> Self {
        Self {
            fields,
            include_ttl,
        }
    }

    /// Create a fetcher for all fields.
    pub fn all_fields() -> Self {
        Self::new(None, false)
    }

    /// Create a fetcher with projection.
    pub fn with_fields(fields: Vec<String>) -> Self {
        Self::new(Some(fields), false)
    }

    /// Enable TTL fetching.
    pub fn with_ttl(mut self) -> Self {
        self.include_ttl = true;
        self
    }

    /// Fetch hashes from the cluster.
    pub async fn fetch(
        &self,
        conn: &mut ClusterConnection,
        keys: Vec<String>,
    ) -> Result<Vec<HashData>> {
        fetch_hashes_cluster(conn, &keys, self.fields.as_deref(), self.include_ttl).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_data_creation() {
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), Some("Alice".to_string()));
        fields.insert("age".to_string(), Some("30".to_string()));

        let data = HashData {
            key: "user:1".to_string(),
            fields,
            ttl: None,
        };

        assert_eq!(data.key, "user:1");
        assert_eq!(data.fields.get("name"), Some(&Some("Alice".to_string())));
        assert_eq!(data.fields.get("age"), Some(&Some("30".to_string())));
        assert_eq!(data.ttl, None);
    }

    #[test]
    fn test_hash_data_with_missing_field() {
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), Some("Alice".to_string()));
        fields.insert("email".to_string(), None); // Missing field

        let data = HashData {
            key: "user:1".to_string(),
            fields,
            ttl: Some(3600), // 1 hour TTL
        };

        assert_eq!(data.fields.get("name"), Some(&Some("Alice".to_string())));
        assert_eq!(data.fields.get("email"), Some(&None));
        assert_eq!(data.ttl, Some(3600));
    }

    #[test]
    fn test_hash_data_with_no_expiry() {
        let data = HashData {
            key: "user:1".to_string(),
            fields: HashMap::new(),
            ttl: Some(-1), // No expiry
        };

        assert_eq!(data.ttl, Some(-1));
    }
}
