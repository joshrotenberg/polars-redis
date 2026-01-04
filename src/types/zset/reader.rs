//! Redis sorted set data fetching.

use redis::aio::ConnectionManager;

use crate::error::Result;

/// Data from a single Redis sorted set.
#[derive(Debug, Clone)]
pub struct ZSetData {
    /// The Redis key.
    pub key: String,
    /// The members with their scores, ordered by score ascending.
    pub members: Vec<(String, f64)>,
}

/// Fetch sorted set members for a batch of keys.
pub async fn fetch_zsets(conn: &mut ConnectionManager, keys: &[String]) -> Result<Vec<ZSetData>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    // Use pipeline to fetch all sorted sets
    let mut pipe = redis::pipe();
    for key in keys {
        // ZRANGE key 0 -1 WITHSCORES gets all members with scores
        pipe.cmd("ZRANGE").arg(key).arg(0).arg(-1).arg("WITHSCORES");
    }

    // Results come as flat arrays: [member1, score1, member2, score2, ...]
    let results: Vec<Vec<String>> = pipe.query_async(conn).await?;

    let mut zset_data = Vec::with_capacity(keys.len());
    for (key, flat_result) in keys.iter().zip(results.into_iter()) {
        // Parse the flat array into (member, score) pairs
        let mut members = Vec::with_capacity(flat_result.len() / 2);
        let mut iter = flat_result.into_iter();
        while let (Some(member), Some(score_str)) = (iter.next(), iter.next()) {
            if let Ok(score) = score_str.parse::<f64>() {
                members.push((member, score));
            }
        }

        // Only include non-empty sorted sets
        if !members.is_empty() {
            zset_data.push(ZSetData {
                key: key.clone(),
                members,
            });
        }
    }

    Ok(zset_data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zset_data_creation() {
        let data = ZSetData {
            key: "leaderboard:game1".to_string(),
            members: vec![
                ("player1".to_string(), 100.0),
                ("player2".to_string(), 85.5),
                ("player3".to_string(), 92.0),
            ],
        };

        assert_eq!(data.key, "leaderboard:game1");
        assert_eq!(data.members.len(), 3);
        assert_eq!(data.members[0].0, "player1");
        assert!((data.members[0].1 - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_zset_data_with_negative_scores() {
        let data = ZSetData {
            key: "temperatures".to_string(),
            members: vec![
                ("freezing".to_string(), -10.0),
                ("cold".to_string(), 5.0),
                ("warm".to_string(), 25.0),
            ],
        };

        assert!((data.members[0].1 - (-10.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_zset_data_empty() {
        let data = ZSetData {
            key: "empty:zset".to_string(),
            members: vec![],
        };

        assert!(data.members.is_empty());
    }
}
