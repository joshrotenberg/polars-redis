//! Redis connection management.
//!
//! This module provides connection abstractions for both single-node Redis
//! and Redis Cluster deployments.
//!
//! # Connection Types
//!
//! - [`RedisConnection`]: Single-node Redis connection wrapper
//! - [`ClusterConnection`] (with `cluster` feature): Redis Cluster connection
//! - [`RedisConn`]: Unified connection enum that works with both
//!
//! # URL Schemes
//!
//! - `redis://host:port` - Single-node connection
//! - `redis+cluster://host:port` - Cluster connection (connects via initial node)
//! - Multiple URLs - Cluster connection with multiple initial nodes
//!
//! # Example
//!
//! ```ignore
//! use polars_redis::connection::{RedisConnection, ConnectionConfig};
//!
//! // Single node
//! let conn = RedisConnection::new("redis://localhost:6379")?;
//!
//! // Cluster (with cluster feature)
//! let conn = RedisConnection::from_config(ConnectionConfig::cluster(&["redis://node1:7000"]))?;
//! ```

use crate::error::{Error, Result};
use redis::aio::{ConnectionManager, MultiplexedConnection};
#[cfg(feature = "cluster")]
use redis::cluster::ClusterClient;
#[cfg(feature = "cluster")]
use redis::cluster_async::ClusterConnection;
use redis::{Client, Cmd, FromRedisValue, Pipeline};

/// Configuration for Redis connections.
///
/// Supports both single-node and cluster configurations.
#[derive(Debug, Clone)]
pub enum ConnectionConfig {
    /// Single Redis node connection.
    Single {
        /// Redis URL (e.g., "redis://localhost:6379")
        url: String,
    },
    /// Redis Cluster connection.
    #[cfg(feature = "cluster")]
    Cluster {
        /// Initial cluster node URLs
        nodes: Vec<String>,
    },
}

impl ConnectionConfig {
    /// Create a single-node configuration.
    pub fn single(url: impl Into<String>) -> Self {
        Self::Single { url: url.into() }
    }

    /// Create a cluster configuration.
    #[cfg(feature = "cluster")]
    pub fn cluster(nodes: &[impl AsRef<str>]) -> Self {
        Self::Cluster {
            nodes: nodes.iter().map(|s| s.as_ref().to_string()).collect(),
        }
    }

    /// Parse a URL and determine the connection type.
    ///
    /// URLs starting with `redis+cluster://` are treated as cluster connections.
    /// Standard `redis://` URLs are single-node connections.
    pub fn from_url(url: &str) -> Self {
        #[cfg(feature = "cluster")]
        if url.starts_with("redis+cluster://") {
            // Convert redis+cluster:// to redis:// for the client
            let node_url = url.replace("redis+cluster://", "redis://");
            return Self::Cluster {
                nodes: vec![node_url],
            };
        }

        Self::Single {
            url: url.to_string(),
        }
    }

    /// Check if this is a cluster configuration.
    pub fn is_cluster(&self) -> bool {
        #[cfg(feature = "cluster")]
        if matches!(self, Self::Cluster { .. }) {
            return true;
        }
        false
    }
}

/// Unified async connection that works for both single-node and cluster.
///
/// This enum abstracts over the different connection types, providing a unified
/// interface for executing Redis commands.
#[derive(Clone)]
pub enum RedisConn {
    /// Single-node connection with auto-reconnect.
    Single(ConnectionManager),
    /// Cluster connection (cloneable, thread-safe, internally pooled).
    #[cfg(feature = "cluster")]
    Cluster(ClusterConnection),
}

impl RedisConn {
    /// Execute a Redis command and parse the result.
    pub async fn query_async<T: FromRedisValue>(&mut self, cmd: &Cmd) -> Result<T> {
        match self {
            Self::Single(conn) => cmd.query_async(conn).await.map_err(Error::Connection),
            #[cfg(feature = "cluster")]
            Self::Cluster(conn) => cmd.query_async(conn).await.map_err(Error::Connection),
        }
    }

    /// Execute a pipeline and parse the results.
    pub async fn pipe_query_async<T: FromRedisValue>(&mut self, pipe: &Pipeline) -> Result<T> {
        match self {
            Self::Single(conn) => pipe.query_async(conn).await.map_err(Error::Connection),
            #[cfg(feature = "cluster")]
            Self::Cluster(conn) => pipe.query_async(conn).await.map_err(Error::Connection),
        }
    }

    /// Check if this is a cluster connection.
    pub fn is_cluster(&self) -> bool {
        #[cfg(feature = "cluster")]
        if matches!(self, Self::Cluster(_)) {
            return true;
        }
        false
    }

    /// Get the underlying ConnectionManager (panics if cluster).
    ///
    /// Use this only when you specifically need single-node connection features.
    pub fn as_single(&mut self) -> &mut ConnectionManager {
        match self {
            Self::Single(conn) => conn,
            #[cfg(feature = "cluster")]
            Self::Cluster(_) => panic!("Cannot get single connection from cluster"),
        }
    }

    /// Get the underlying ClusterConnection (panics if single).
    ///
    /// Use this only when you specifically need cluster connection features.
    #[cfg(feature = "cluster")]
    pub fn as_cluster(&mut self) -> &mut ClusterConnection {
        match self {
            Self::Cluster(conn) => conn,
            Self::Single(_) => panic!("Cannot get cluster connection from single"),
        }
    }
}

/// Redis connection wrapper that manages connection lifecycle.
///
/// Supports both single-node and cluster connections based on configuration.
pub struct RedisConnection {
    config: ConnectionConfig,
    client: Option<Client>,
    #[cfg(feature = "cluster")]
    cluster_client: Option<ClusterClient>,
}

impl RedisConnection {
    /// Create a new Redis connection from a URL.
    ///
    /// # Arguments
    /// * `url` - Redis connection URL (e.g., "redis://localhost:6379")
    ///
    /// For cluster connections, use `redis+cluster://` scheme or [`from_config`].
    ///
    /// # Examples
    /// ```ignore
    /// // Single node
    /// let conn = RedisConnection::new("redis://localhost:6379")?;
    ///
    /// // Cluster (auto-detected from scheme)
    /// let conn = RedisConnection::new("redis+cluster://node1:7000")?;
    /// ```
    pub fn new(url: &str) -> Result<Self> {
        let config = ConnectionConfig::from_url(url);
        Self::from_config(config)
    }

    /// Create a connection from a configuration.
    ///
    /// # Examples
    /// ```ignore
    /// // Single node
    /// let config = ConnectionConfig::single("redis://localhost:6379");
    /// let conn = RedisConnection::from_config(config)?;
    ///
    /// // Cluster with multiple initial nodes
    /// let config = ConnectionConfig::cluster(&[
    ///     "redis://node1:7000",
    ///     "redis://node2:7001",
    /// ]);
    /// let conn = RedisConnection::from_config(config)?;
    /// ```
    pub fn from_config(config: ConnectionConfig) -> Result<Self> {
        match &config {
            ConnectionConfig::Single { url } => {
                let client = Client::open(url.as_str())
                    .map_err(|e| Error::InvalidUrl(format!("{}: {}", url, e)))?;
                Ok(Self {
                    config,
                    client: Some(client),
                    #[cfg(feature = "cluster")]
                    cluster_client: None,
                })
            },
            #[cfg(feature = "cluster")]
            ConnectionConfig::Cluster { nodes } => {
                let cluster_client = ClusterClient::new(nodes.clone())
                    .map_err(|e| Error::InvalidUrl(format!("cluster: {}", e)))?;
                Ok(Self {
                    config,
                    client: None,
                    cluster_client: Some(cluster_client),
                })
            },
        }
    }

    /// Create a cluster connection from multiple node URLs.
    ///
    /// # Arguments
    /// * `nodes` - List of initial cluster node URLs
    ///
    /// # Examples
    /// ```ignore
    /// let conn = RedisConnection::new_cluster(&[
    ///     "redis://node1:7000",
    ///     "redis://node2:7001",
    ///     "redis://node3:7002",
    /// ])?;
    /// ```
    #[cfg(feature = "cluster")]
    pub fn new_cluster(nodes: &[impl AsRef<str>]) -> Result<Self> {
        let config = ConnectionConfig::cluster(nodes);
        Self::from_config(config)
    }

    /// Check if this is a cluster connection.
    pub fn is_cluster(&self) -> bool {
        self.config.is_cluster()
    }

    /// Get a unified async connection.
    ///
    /// Returns a [`RedisConn`] that works with both single-node and cluster.
    pub async fn get_connection(&self) -> Result<RedisConn> {
        match &self.config {
            ConnectionConfig::Single { .. } => {
                let manager = self.get_connection_manager().await?;
                Ok(RedisConn::Single(manager))
            },
            #[cfg(feature = "cluster")]
            ConnectionConfig::Cluster { .. } => {
                let cluster = self.get_cluster_connection().await?;
                Ok(RedisConn::Cluster(cluster))
            },
        }
    }

    /// Get an async multiplexed connection (single-node only).
    pub async fn get_async_connection(&self) -> Result<MultiplexedConnection> {
        let client = self.client.as_ref().ok_or_else(|| {
            Error::Runtime("Cannot get async connection from cluster config".to_string())
        })?;
        client
            .get_multiplexed_async_connection()
            .await
            .map_err(Error::Connection)
    }

    /// Get a ConnectionManager for async operations with auto-reconnection.
    ///
    /// ConnectionManager is cheap to clone and provides automatic reconnection
    /// on connection failures. Preferred over `get_async_connection()` for
    /// long-running operations.
    pub async fn get_connection_manager(&self) -> Result<ConnectionManager> {
        let client = self.client.as_ref().ok_or_else(|| {
            Error::Runtime("Cannot get connection manager from cluster config".to_string())
        })?;
        ConnectionManager::new(client.clone())
            .await
            .map_err(Error::Connection)
    }

    /// Get a cluster async connection.
    ///
    /// ClusterConnection is cheap to clone, thread-safe, and has internal pooling.
    #[cfg(feature = "cluster")]
    pub async fn get_cluster_connection(&self) -> Result<ClusterConnection> {
        let cluster_client = self.cluster_client.as_ref().ok_or_else(|| {
            Error::Runtime("Cannot get cluster connection from single-node config".to_string())
        })?;
        cluster_client
            .get_async_connection()
            .await
            .map_err(Error::Connection)
    }

    /// Get a sync connection (for simple operations, single-node only).
    pub fn get_sync_connection(&self) -> Result<redis::Connection> {
        let client = self.client.as_ref().ok_or_else(|| {
            Error::Runtime("Cannot get sync connection from cluster config".to_string())
        })?;
        client.get_connection().map_err(Error::Connection)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_url() {
        let result = RedisConnection::new("not-a-valid-url");
        assert!(result.is_err());
    }

    #[test]
    fn test_valid_url_parsing() {
        // This should parse successfully even without a running Redis
        let result = RedisConnection::new("redis://localhost:6379");
        assert!(result.is_ok());
        assert!(!result.unwrap().is_cluster());
    }

    #[test]
    fn test_connection_config_single() {
        let config = ConnectionConfig::single("redis://localhost:6379");
        assert!(!config.is_cluster());
    }

    #[test]
    fn test_connection_config_from_url_single() {
        let config = ConnectionConfig::from_url("redis://localhost:6379");
        assert!(!config.is_cluster());
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn test_connection_config_cluster() {
        let config = ConnectionConfig::cluster(&["redis://node1:7000", "redis://node2:7001"]);
        assert!(config.is_cluster());
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn test_connection_config_from_url_cluster() {
        let config = ConnectionConfig::from_url("redis+cluster://node1:7000");
        assert!(config.is_cluster());
    }

    #[cfg(feature = "cluster")]
    #[test]
    fn test_cluster_connection_creation() {
        // This should parse successfully even without a running cluster
        let result = RedisConnection::new_cluster(&["redis://localhost:7000"]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_cluster());
    }
}
