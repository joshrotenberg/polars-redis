//! Redis connection management.

use crate::error::{Error, Result};
use redis::Client;
use redis::aio::MultiplexedConnection;

/// Redis connection wrapper that manages connection lifecycle.
pub struct RedisConnection {
    client: Client,
}

impl RedisConnection {
    /// Create a new Redis connection from a URL.
    ///
    /// # Arguments
    /// * `url` - Redis connection URL (e.g., "redis://localhost:6379")
    ///
    /// # Examples
    /// ```ignore
    /// let conn = RedisConnection::new("redis://localhost:6379")?;
    /// ```
    pub fn new(url: &str) -> Result<Self> {
        let client = Client::open(url).map_err(|e| Error::InvalidUrl(format!("{}: {}", url, e)))?;
        Ok(Self { client })
    }

    /// Get an async multiplexed connection.
    pub async fn get_async_connection(&self) -> Result<MultiplexedConnection> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(Error::Connection)
    }

    /// Get a sync connection (for simple operations).
    pub fn get_sync_connection(&self) -> Result<redis::Connection> {
        self.client.get_connection().map_err(Error::Connection)
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
    }
}
