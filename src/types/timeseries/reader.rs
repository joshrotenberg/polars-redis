//! RedisTimeSeries data fetching.

use crate::error::Result;

/// A single sample from a time series.
#[derive(Debug, Clone)]
pub struct TimeSeriesSample {
    /// Timestamp in milliseconds since epoch.
    pub timestamp_ms: i64,
    /// The value at this timestamp.
    pub value: f64,
}

/// Data from a single RedisTimeSeries key.
#[derive(Debug, Clone)]
pub struct TimeSeriesData {
    /// The Redis key.
    pub key: String,
    /// Labels associated with this time series (if requested).
    pub labels: Vec<(String, String)>,
    /// The samples.
    pub samples: Vec<TimeSeriesSample>,
}

/// Fetch time series samples using TS.RANGE.
#[allow(dead_code)]
pub fn fetch_timeseries(
    conn: &mut redis::Connection,
    keys: &[String],
    start: &str,
    end: &str,
    count: Option<usize>,
    aggregation: Option<(&str, i64)>,
) -> Result<Vec<TimeSeriesData>> {
    let mut results = Vec::with_capacity(keys.len());

    for key in keys {
        let mut cmd = redis::cmd("TS.RANGE");
        cmd.arg(key).arg(start).arg(end);

        if let Some(c) = count {
            cmd.arg("COUNT").arg(c);
        }

        if let Some((agg_type, bucket_ms)) = aggregation {
            cmd.arg("AGGREGATION").arg(agg_type).arg(bucket_ms);
        }

        // TS.RANGE returns an array of [timestamp, value] pairs
        let samples_raw: Vec<(i64, String)> = cmd.query(conn).unwrap_or_default();

        let samples: Vec<TimeSeriesSample> = samples_raw
            .into_iter()
            .filter_map(|(ts, val_str)| {
                val_str.parse::<f64>().ok().map(|value| TimeSeriesSample {
                    timestamp_ms: ts,
                    value,
                })
            })
            .collect();

        results.push(TimeSeriesData {
            key: key.clone(),
            labels: Vec::new(),
            samples,
        });
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timeseries_sample_creation() {
        let sample = TimeSeriesSample {
            timestamp_ms: 1234567890123,
            value: 42.5,
        };

        assert_eq!(sample.timestamp_ms, 1234567890123);
        assert!((sample.value - 42.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_timeseries_data_creation() {
        let data = TimeSeriesData {
            key: "sensor:temp:1".to_string(),
            labels: vec![
                ("location".to_string(), "us".to_string()),
                ("unit".to_string(), "celsius".to_string()),
            ],
            samples: vec![
                TimeSeriesSample {
                    timestamp_ms: 1000,
                    value: 20.5,
                },
                TimeSeriesSample {
                    timestamp_ms: 2000,
                    value: 21.0,
                },
            ],
        };

        assert_eq!(data.key, "sensor:temp:1");
        assert_eq!(data.labels.len(), 2);
        assert_eq!(data.samples.len(), 2);
    }

    #[test]
    fn test_timeseries_data_empty() {
        let data = TimeSeriesData {
            key: "empty:ts".to_string(),
            labels: vec![],
            samples: vec![],
        };

        assert!(data.samples.is_empty());
        assert!(data.labels.is_empty());
    }
}
