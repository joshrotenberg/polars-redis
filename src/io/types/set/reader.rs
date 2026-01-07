//! Redis set data fetching.

use redis::aio::ConnectionManager;
#[cfg(feature = "cluster")]
use redis::cluster_async::ClusterConnection;

use crate::error::Result;

/// Data from a single Redis set.
#[derive(Debug, Clone)]
pub struct SetData {
    /// The Redis key.
    pub key: String,
    /// The set members.
    pub members: Vec<String>,
}

/// Fetch set members for a batch of keys.
pub async fn fetch_sets(conn: &mut ConnectionManager, keys: &[String]) -> Result<Vec<SetData>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    // Use pipeline to fetch all sets
    let mut pipe = redis::pipe();
    for key in keys {
        pipe.smembers(key);
    }

    let results: Vec<Vec<String>> = pipe.query_async(conn).await?;

    let mut set_data = Vec::with_capacity(keys.len());
    for (key, members) in keys.iter().zip(results.into_iter()) {
        // Only include non-empty sets (key might have been deleted or is wrong type)
        if !members.is_empty() {
            set_data.push(SetData {
                key: key.clone(),
                members,
            });
        }
    }

    Ok(set_data)
}

/// Fetch set members for a batch of keys from a cluster.
#[cfg(feature = "cluster")]
pub async fn fetch_sets_cluster(
    conn: &mut ClusterConnection,
    keys: &[String],
) -> Result<Vec<SetData>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut pipe = redis::pipe();
    for key in keys {
        pipe.smembers(key);
    }

    let results: Vec<Vec<String>> = pipe.query_async(conn).await?;

    let mut set_data = Vec::with_capacity(keys.len());
    for (key, members) in keys.iter().zip(results.into_iter()) {
        if !members.is_empty() {
            set_data.push(SetData {
                key: key.clone(),
                members,
            });
        }
    }

    Ok(set_data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_data_creation() {
        let data = SetData {
            key: "tags:user:1".to_string(),
            members: vec![
                "rust".to_string(),
                "redis".to_string(),
                "polars".to_string(),
            ],
        };

        assert_eq!(data.key, "tags:user:1");
        assert_eq!(data.members.len(), 3);
        assert!(data.members.contains(&"rust".to_string()));
    }

    #[test]
    fn test_set_data_empty() {
        let data = SetData {
            key: "empty:set".to_string(),
            members: vec![],
        };

        assert!(data.members.is_empty());
    }
}
