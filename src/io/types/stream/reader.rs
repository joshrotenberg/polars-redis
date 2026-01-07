//! Redis Stream data fetching.

use std::collections::HashMap;

use crate::error::{Error, Result};

/// Data from a single stream entry.
#[derive(Debug, Clone)]
pub struct StreamEntry {
    /// Entry ID (e.g., "1234567890123-0").
    pub id: String,
    /// Timestamp in milliseconds (parsed from ID).
    pub timestamp_ms: i64,
    /// Sequence number within the millisecond.
    pub sequence: u64,
    /// Field-value pairs from the entry.
    pub fields: HashMap<String, String>,
}

/// Data from a Redis stream.
#[derive(Debug, Clone)]
pub struct StreamData {
    /// The Redis key.
    pub key: String,
    /// Stream entries.
    pub entries: Vec<StreamEntry>,
}

/// Parse a stream entry ID into (timestamp_ms, sequence).
fn parse_entry_id(id: &str) -> Result<(i64, u64)> {
    let parts: Vec<&str> = id.split('-').collect();
    if parts.len() != 2 {
        return Err(Error::TypeConversion(format!(
            "Invalid stream entry ID format: {}",
            id
        )));
    }

    let timestamp_ms = parts[0]
        .parse::<i64>()
        .map_err(|_| Error::TypeConversion(format!("Invalid timestamp in entry ID: {}", id)))?;

    let sequence = parts[1]
        .parse::<u64>()
        .map_err(|_| Error::TypeConversion(format!("Invalid sequence in entry ID: {}", id)))?;

    Ok((timestamp_ms, sequence))
}

/// Fetch stream entries from Redis using XRANGE.
#[allow(dead_code)]
pub fn fetch_streams(
    conn: &mut redis::Connection,
    keys: &[String],
    start_id: &str,
    end_id: &str,
    count: Option<usize>,
) -> Result<Vec<StreamData>> {
    let mut results = Vec::with_capacity(keys.len());

    for key in keys {
        let entries: Vec<(String, HashMap<String, String>)> = if let Some(c) = count {
            redis::cmd("XRANGE")
                .arg(key)
                .arg(start_id)
                .arg(end_id)
                .arg("COUNT")
                .arg(c)
                .query(conn)?
        } else {
            redis::cmd("XRANGE")
                .arg(key)
                .arg(start_id)
                .arg(end_id)
                .query(conn)?
        };

        let mut stream_entries = Vec::with_capacity(entries.len());
        for (id, fields) in entries {
            let (timestamp_ms, sequence) = parse_entry_id(&id)?;
            stream_entries.push(StreamEntry {
                id,
                timestamp_ms,
                sequence,
                fields,
            });
        }

        results.push(StreamData {
            key: key.clone(),
            entries: stream_entries,
        });
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_entry_id() {
        let (ts, seq) = parse_entry_id("1234567890123-0").unwrap();
        assert_eq!(ts, 1234567890123);
        assert_eq!(seq, 0);

        let (ts, seq) = parse_entry_id("1234567890123-42").unwrap();
        assert_eq!(ts, 1234567890123);
        assert_eq!(seq, 42);
    }

    #[test]
    fn test_parse_entry_id_invalid() {
        assert!(parse_entry_id("invalid").is_err());
        assert!(parse_entry_id("123-abc").is_err());
        assert!(parse_entry_id("abc-123").is_err());
    }

    #[test]
    fn test_stream_entry_creation() {
        let mut fields = HashMap::new();
        fields.insert("name".to_string(), "Alice".to_string());
        fields.insert("action".to_string(), "login".to_string());

        let entry = StreamEntry {
            id: "1234567890123-0".to_string(),
            timestamp_ms: 1234567890123,
            sequence: 0,
            fields,
        };

        assert_eq!(entry.id, "1234567890123-0");
        assert_eq!(entry.timestamp_ms, 1234567890123);
        assert_eq!(entry.sequence, 0);
        assert_eq!(entry.fields.get("name"), Some(&"Alice".to_string()));
    }
}
