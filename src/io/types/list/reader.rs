//! Redis list data fetching.

use redis::aio::ConnectionManager;
#[cfg(feature = "cluster")]
use redis::cluster_async::ClusterConnection;

use crate::error::Result;

/// Data from a single Redis list.
#[derive(Debug, Clone)]
pub struct ListData {
    /// The Redis key.
    pub key: String,
    /// The list elements (in order).
    pub elements: Vec<String>,
}

/// Fetch list elements for a batch of keys.
pub async fn fetch_lists(conn: &mut ConnectionManager, keys: &[String]) -> Result<Vec<ListData>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    // Use pipeline to fetch all lists
    let mut pipe = redis::pipe();
    for key in keys {
        // LRANGE key 0 -1 gets all elements
        pipe.lrange(key, 0, -1);
    }

    let results: Vec<Vec<String>> = pipe.query_async(conn).await?;

    let mut list_data = Vec::with_capacity(keys.len());
    for (key, elements) in keys.iter().zip(results.into_iter()) {
        // Only include non-empty lists (key might have been deleted or is wrong type)
        if !elements.is_empty() {
            list_data.push(ListData {
                key: key.clone(),
                elements,
            });
        }
    }

    Ok(list_data)
}

/// Fetch list elements for a batch of keys from a cluster.
#[cfg(feature = "cluster")]
pub async fn fetch_lists_cluster(
    conn: &mut ClusterConnection,
    keys: &[String],
) -> Result<Vec<ListData>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut pipe = redis::pipe();
    for key in keys {
        pipe.lrange(key, 0, -1);
    }

    let results: Vec<Vec<String>> = pipe.query_async(conn).await?;

    let mut list_data = Vec::with_capacity(keys.len());
    for (key, elements) in keys.iter().zip(results.into_iter()) {
        if !elements.is_empty() {
            list_data.push(ListData {
                key: key.clone(),
                elements,
            });
        }
    }

    Ok(list_data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_data_creation() {
        let data = ListData {
            key: "queue:jobs".to_string(),
            elements: vec!["job1".to_string(), "job2".to_string(), "job3".to_string()],
        };

        assert_eq!(data.key, "queue:jobs");
        assert_eq!(data.elements.len(), 3);
        assert_eq!(data.elements[0], "job1");
        assert_eq!(data.elements[2], "job3");
    }

    #[test]
    fn test_list_data_preserves_order() {
        let data = ListData {
            key: "ordered:list".to_string(),
            elements: vec![
                "first".to_string(),
                "second".to_string(),
                "third".to_string(),
            ],
        };

        // Lists preserve insertion order
        assert_eq!(data.elements[0], "first");
        assert_eq!(data.elements[1], "second");
        assert_eq!(data.elements[2], "third");
    }

    #[test]
    fn test_list_data_empty() {
        let data = ListData {
            key: "empty:list".to_string(),
            elements: vec![],
        };

        assert!(data.elements.is_empty());
    }
}
