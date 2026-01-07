//! Geospatial utilities for Redis.
//!
//! This module provides DataFrame-friendly operations for Redis GEO commands,
//! enabling bulk add, radius queries, distance calculations, and position lookups.
//!
//! # Add Locations
//!
//! ```ignore
//! use polars_redis::geo::geo_add;
//!
//! let locations = vec![
//!     ("office", -122.4, 37.7),
//!     ("cafe", -122.5, 37.8),
//! ];
//! geo_add("redis://localhost:6379", "places", &locations)?;
//! ```
//!
//! # Radius Query
//!
//! ```ignore
//! use polars_redis::geo::geo_radius;
//!
//! let nearby = geo_radius(
//!     "redis://localhost:6379",
//!     "places",
//!     -122.4, 37.7,
//!     10.0,
//!     "km",
//!     None,
//! )?;
//! // Returns Vec<GeoLocation> with name, distance, lon, lat
//! ```
//!
//! # Distance Between Members
//!
//! ```ignore
//! use polars_redis::geo::geo_dist;
//!
//! let distance = geo_dist(
//!     "redis://localhost:6379",
//!     "places",
//!     "office",
//!     "cafe",
//!     "km",
//! )?;
//! ```

use std::str::FromStr;

use redis::Value;
use tokio::runtime::Runtime;

use crate::connection::RedisConnection;
use crate::error::{Error, Result};

/// A geographic location with optional distance.
#[derive(Debug, Clone)]
pub struct GeoLocation {
    /// The member name.
    pub name: String,
    /// Longitude coordinate (None if member doesn't exist).
    pub longitude: Option<f64>,
    /// Latitude coordinate (None if member doesn't exist).
    pub latitude: Option<f64>,
    /// Distance from query point (only set for radius queries).
    pub distance: Option<f64>,
    /// Geohash value (optional).
    pub geohash: Option<i64>,
}

/// Result of a geo_add operation.
#[derive(Debug, Clone)]
pub struct GeoAddResult {
    /// Number of new members added.
    pub added: usize,
    /// Number of members updated (already existed).
    pub updated: usize,
}

/// Distance unit for geo queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoUnit {
    /// Meters
    Meters,
    /// Kilometers
    Kilometers,
    /// Miles
    Miles,
    /// Feet
    Feet,
}

impl GeoUnit {
    /// Convert to Redis unit string.
    pub fn as_str(&self) -> &'static str {
        match self {
            GeoUnit::Meters => "m",
            GeoUnit::Kilometers => "km",
            GeoUnit::Miles => "mi",
            GeoUnit::Feet => "ft",
        }
    }
}

impl FromStr for GeoUnit {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "m" | "meters" => Ok(GeoUnit::Meters),
            "km" | "kilometers" => Ok(GeoUnit::Kilometers),
            "mi" | "miles" => Ok(GeoUnit::Miles),
            "ft" | "feet" => Ok(GeoUnit::Feet),
            _ => Err(Error::Runtime(format!(
                "Invalid geo unit '{}'. Use: m, km, mi, ft",
                s
            ))),
        }
    }
}

/// Sort order for radius queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoSort {
    /// Sort ascending (nearest first).
    Asc,
    /// Sort descending (farthest first).
    Desc,
}

/// Add locations to a geo set.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key for the geo set
/// * `locations` - List of (name, longitude, latitude) tuples
///
/// # Returns
/// A `GeoAddResult` with added and updated counts.
///
/// # Example
/// ```ignore
/// let locations = vec![
///     ("office".to_string(), -122.4, 37.7),
///     ("cafe".to_string(), -122.5, 37.8),
/// ];
/// let result = geo_add("redis://localhost:6379", "places", &locations)?;
/// println!("Added {} new locations", result.added);
/// ```
pub fn geo_add(url: &str, key: &str, locations: &[(String, f64, f64)]) -> Result<GeoAddResult> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        geo_add_async(&mut conn, key, locations).await
    })
}

async fn geo_add_async(
    conn: &mut redis::aio::MultiplexedConnection,
    key: &str,
    locations: &[(String, f64, f64)],
) -> Result<GeoAddResult> {
    if locations.is_empty() {
        return Ok(GeoAddResult {
            added: 0,
            updated: 0,
        });
    }

    let mut cmd = redis::cmd("GEOADD");
    cmd.arg(key);

    for (name, lon, lat) in locations {
        cmd.arg(*lon).arg(*lat).arg(name);
    }

    let added: i64 = cmd.query_async(conn).await.map_err(Error::Connection)?;

    Ok(GeoAddResult {
        added: added as usize,
        updated: locations.len() - added as usize,
    })
}

/// Query locations within a radius.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key for the geo set
/// * `longitude` - Center longitude
/// * `latitude` - Center latitude
/// * `radius` - Search radius
/// * `unit` - Distance unit (m, km, mi, ft)
/// * `count` - Optional maximum number of results
/// * `sort` - Optional sort order (ASC or DESC by distance)
///
/// # Returns
/// A vector of `GeoLocation` with name, coordinates, and distance.
#[allow(clippy::too_many_arguments)]
pub fn geo_radius(
    url: &str,
    key: &str,
    longitude: f64,
    latitude: f64,
    radius: f64,
    unit: &str,
    count: Option<usize>,
    sort: Option<GeoSort>,
) -> Result<Vec<GeoLocation>> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;
    let geo_unit = GeoUnit::from_str(unit)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        geo_radius_async(
            &mut conn, key, longitude, latitude, radius, geo_unit, count, sort,
        )
        .await
    })
}

#[allow(clippy::too_many_arguments)]
async fn geo_radius_async(
    conn: &mut redis::aio::MultiplexedConnection,
    key: &str,
    longitude: f64,
    latitude: f64,
    radius: f64,
    unit: GeoUnit,
    count: Option<usize>,
    sort: Option<GeoSort>,
) -> Result<Vec<GeoLocation>> {
    // Use GEOSEARCH (Redis 6.2+) which replaces deprecated GEORADIUS
    let mut cmd = redis::cmd("GEOSEARCH");
    cmd.arg(key)
        .arg("FROMMEMBER")
        .arg("__dummy__") // We'll use FROMLONLAT instead
        .arg("BYRADIUS")
        .arg(radius)
        .arg(unit.as_str());

    // Actually use FROMLONLAT
    let mut cmd = redis::cmd("GEOSEARCH");
    cmd.arg(key)
        .arg("FROMLONLAT")
        .arg(longitude)
        .arg(latitude)
        .arg("BYRADIUS")
        .arg(radius)
        .arg(unit.as_str())
        .arg("WITHCOORD")
        .arg("WITHDIST");

    if let Some(n) = count {
        cmd.arg("COUNT").arg(n);
    }

    match sort {
        Some(GeoSort::Asc) => {
            cmd.arg("ASC");
        },
        Some(GeoSort::Desc) => {
            cmd.arg("DESC");
        },
        None => {},
    }

    let result: Value = cmd.query_async(conn).await.map_err(Error::Connection)?;

    parse_geo_search_result(result)
}

/// Search locations within a radius of another member.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key for the geo set
/// * `member` - Name of the member to search from
/// * `radius` - Search radius
/// * `unit` - Distance unit (m, km, mi, ft)
/// * `count` - Optional maximum number of results
/// * `sort` - Optional sort order
///
/// # Returns
/// A vector of `GeoLocation` with name, coordinates, and distance.
pub fn geo_radius_by_member(
    url: &str,
    key: &str,
    member: &str,
    radius: f64,
    unit: &str,
    count: Option<usize>,
    sort: Option<GeoSort>,
) -> Result<Vec<GeoLocation>> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;
    let geo_unit = GeoUnit::from_str(unit)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        geo_radius_by_member_async(&mut conn, key, member, radius, geo_unit, count, sort).await
    })
}

async fn geo_radius_by_member_async(
    conn: &mut redis::aio::MultiplexedConnection,
    key: &str,
    member: &str,
    radius: f64,
    unit: GeoUnit,
    count: Option<usize>,
    sort: Option<GeoSort>,
) -> Result<Vec<GeoLocation>> {
    let mut cmd = redis::cmd("GEOSEARCH");
    cmd.arg(key)
        .arg("FROMMEMBER")
        .arg(member)
        .arg("BYRADIUS")
        .arg(radius)
        .arg(unit.as_str())
        .arg("WITHCOORD")
        .arg("WITHDIST");

    if let Some(n) = count {
        cmd.arg("COUNT").arg(n);
    }

    match sort {
        Some(GeoSort::Asc) => {
            cmd.arg("ASC");
        },
        Some(GeoSort::Desc) => {
            cmd.arg("DESC");
        },
        None => {},
    }

    let result: Value = cmd.query_async(conn).await.map_err(Error::Connection)?;

    parse_geo_search_result(result)
}

/// Get distance between two members.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key for the geo set
/// * `member1` - First member name
/// * `member2` - Second member name
/// * `unit` - Distance unit (m, km, mi, ft)
///
/// # Returns
/// Distance as f64, or None if either member doesn't exist.
pub fn geo_dist(
    url: &str,
    key: &str,
    member1: &str,
    member2: &str,
    unit: &str,
) -> Result<Option<f64>> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;
    let geo_unit = GeoUnit::from_str(unit)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        geo_dist_async(&mut conn, key, member1, member2, geo_unit).await
    })
}

async fn geo_dist_async(
    conn: &mut redis::aio::MultiplexedConnection,
    key: &str,
    member1: &str,
    member2: &str,
    unit: GeoUnit,
) -> Result<Option<f64>> {
    let result: Value = redis::cmd("GEODIST")
        .arg(key)
        .arg(member1)
        .arg(member2)
        .arg(unit.as_str())
        .query_async(conn)
        .await
        .map_err(Error::Connection)?;

    match result {
        Value::BulkString(bytes) => {
            let s = String::from_utf8_lossy(&bytes);
            Ok(s.parse().ok())
        },
        Value::Nil => Ok(None),
        _ => Ok(None),
    }
}

/// Get positions of members.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key for the geo set
/// * `members` - List of member names
///
/// # Returns
/// A vector of `GeoLocation` with name and coordinates.
/// Members that don't exist will have (0, 0) coordinates.
pub fn geo_pos(url: &str, key: &str, members: &[String]) -> Result<Vec<GeoLocation>> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        geo_pos_async(&mut conn, key, members).await
    })
}

async fn geo_pos_async(
    conn: &mut redis::aio::MultiplexedConnection,
    key: &str,
    members: &[String],
) -> Result<Vec<GeoLocation>> {
    if members.is_empty() {
        return Ok(Vec::new());
    }

    let mut cmd = redis::cmd("GEOPOS");
    cmd.arg(key);
    for member in members {
        cmd.arg(member);
    }

    let result: Value = cmd.query_async(conn).await.map_err(Error::Connection)?;

    let mut locations = Vec::with_capacity(members.len());

    if let Value::Array(positions) = result {
        for (i, pos) in positions.into_iter().enumerate() {
            let name = members.get(i).cloned().unwrap_or_default();
            match pos {
                Value::Array(coords) if coords.len() == 2 => {
                    let lon = parse_coord(&coords[0]);
                    let lat = parse_coord(&coords[1]);
                    locations.push(GeoLocation {
                        name,
                        longitude: Some(lon),
                        latitude: Some(lat),
                        distance: None,
                        geohash: None,
                    });
                },
                Value::Nil => {
                    // Member doesn't exist - add with None coordinates
                    locations.push(GeoLocation {
                        name,
                        longitude: None,
                        latitude: None,
                        distance: None,
                        geohash: None,
                    });
                },
                _ => {
                    locations.push(GeoLocation {
                        name,
                        longitude: None,
                        latitude: None,
                        distance: None,
                        geohash: None,
                    });
                },
            }
        }
    }

    Ok(locations)
}

/// Calculate distance matrix between members.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key for the geo set
/// * `members` - List of member names
/// * `unit` - Distance unit (m, km, mi, ft)
///
/// # Returns
/// A 2D matrix of distances where result[i][j] is the distance from members[i] to members[j].
/// Returns None for pairs where either member doesn't exist.
pub fn geo_dist_matrix(
    url: &str,
    key: &str,
    members: &[String],
    unit: &str,
) -> Result<Vec<Vec<Option<f64>>>> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;
    let geo_unit = GeoUnit::from_str(unit)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        geo_dist_matrix_async(&mut conn, key, members, geo_unit).await
    })
}

async fn geo_dist_matrix_async(
    conn: &mut redis::aio::MultiplexedConnection,
    key: &str,
    members: &[String],
    unit: GeoUnit,
) -> Result<Vec<Vec<Option<f64>>>> {
    let n = members.len();
    if n == 0 {
        return Ok(Vec::new());
    }

    // Build all pairs and pipeline the requests
    let mut pipe = redis::pipe();
    for i in 0..n {
        for j in 0..n {
            pipe.cmd("GEODIST")
                .arg(key)
                .arg(&members[i])
                .arg(&members[j])
                .arg(unit.as_str());
        }
    }

    let results: Vec<Value> = pipe.query_async(conn).await.map_err(Error::Connection)?;

    // Parse results into matrix
    let mut matrix = vec![vec![None; n]; n];
    for (idx, val) in results.iter().enumerate() {
        let i = idx / n;
        let j = idx % n;
        matrix[i][j] = match val {
            Value::BulkString(bytes) => {
                let s = String::from_utf8_lossy(bytes);
                s.parse().ok()
            },
            Value::Nil => None,
            _ => None,
        };
    }

    Ok(matrix)
}

/// Get geohash strings for members.
///
/// # Arguments
/// * `url` - Redis connection URL
/// * `key` - Redis key for the geo set
/// * `members` - List of member names
///
/// # Returns
/// A vector of (member, geohash) tuples. Geohash is None if member doesn't exist.
pub fn geo_hash(url: &str, key: &str, members: &[String]) -> Result<Vec<(String, Option<String>)>> {
    let runtime =
        Runtime::new().map_err(|e| Error::Runtime(format!("Failed to create runtime: {}", e)))?;

    let connection = RedisConnection::new(url)?;

    runtime.block_on(async {
        let mut conn = connection.get_async_connection().await?;
        geo_hash_async(&mut conn, key, members).await
    })
}

async fn geo_hash_async(
    conn: &mut redis::aio::MultiplexedConnection,
    key: &str,
    members: &[String],
) -> Result<Vec<(String, Option<String>)>> {
    if members.is_empty() {
        return Ok(Vec::new());
    }

    let mut cmd = redis::cmd("GEOHASH");
    cmd.arg(key);
    for member in members {
        cmd.arg(member);
    }

    let result: Value = cmd.query_async(conn).await.map_err(Error::Connection)?;

    let mut hashes = Vec::with_capacity(members.len());

    if let Value::Array(values) = result {
        for (i, val) in values.into_iter().enumerate() {
            let name = members.get(i).cloned().unwrap_or_default();
            let hash = match val {
                Value::BulkString(bytes) => Some(String::from_utf8_lossy(&bytes).to_string()),
                Value::Nil => None,
                _ => None,
            };
            hashes.push((name, hash));
        }
    }

    Ok(hashes)
}

// Helper functions

fn parse_coord(value: &Value) -> f64 {
    match value {
        Value::BulkString(bytes) => {
            let s = String::from_utf8_lossy(bytes);
            s.parse().unwrap_or(0.0)
        },
        Value::Double(d) => *d,
        _ => 0.0,
    }
}

fn parse_geo_search_result(result: Value) -> Result<Vec<GeoLocation>> {
    let mut locations = Vec::new();

    if let Value::Array(items) = result {
        for item in items {
            if let Value::Array(parts) = item {
                // Expected format: [name, distance, [lon, lat]]
                // or [name, distance, hash, [lon, lat]] with WITHHASH
                let name = match parts.first() {
                    Some(Value::BulkString(bytes)) => String::from_utf8_lossy(bytes).to_string(),
                    _ => continue,
                };

                let distance = match parts.get(1) {
                    Some(Value::BulkString(bytes)) => {
                        let s = String::from_utf8_lossy(bytes);
                        s.parse().ok()
                    },
                    Some(Value::Double(d)) => Some(*d),
                    _ => None,
                };

                // Find the coordinate array (last element that's an array of 2)
                let (lon, lat) = parts
                    .iter()
                    .rev()
                    .find_map(|p| {
                        if let Value::Array(coords) = p {
                            if coords.len() == 2 {
                                let lon = parse_coord(&coords[0]);
                                let lat = parse_coord(&coords[1]);
                                return Some((lon, lat));
                            }
                        }
                        None
                    })
                    .unwrap_or((0.0, 0.0));

                locations.push(GeoLocation {
                    name,
                    longitude: Some(lon),
                    latitude: Some(lat),
                    distance,
                    geohash: None,
                });
            }
        }
    }

    Ok(locations)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_geo_unit_from_str() {
        assert_eq!(GeoUnit::from_str("m").unwrap(), GeoUnit::Meters);
        assert_eq!(GeoUnit::from_str("km").unwrap(), GeoUnit::Kilometers);
        assert_eq!(GeoUnit::from_str("mi").unwrap(), GeoUnit::Miles);
        assert_eq!(GeoUnit::from_str("ft").unwrap(), GeoUnit::Feet);
        assert_eq!(GeoUnit::from_str("meters").unwrap(), GeoUnit::Meters);
        assert_eq!(
            GeoUnit::from_str("kilometers").unwrap(),
            GeoUnit::Kilometers
        );
        assert!(GeoUnit::from_str("invalid").is_err());
    }

    #[test]
    fn test_geo_unit_as_str() {
        assert_eq!(GeoUnit::Meters.as_str(), "m");
        assert_eq!(GeoUnit::Kilometers.as_str(), "km");
        assert_eq!(GeoUnit::Miles.as_str(), "mi");
        assert_eq!(GeoUnit::Feet.as_str(), "ft");
    }

    #[test]
    fn test_geo_location_struct() {
        let loc = GeoLocation {
            name: "test".to_string(),
            longitude: Some(-122.4),
            latitude: Some(37.7),
            distance: Some(1.5),
            geohash: None,
        };

        assert_eq!(loc.name, "test");
        assert_eq!(loc.longitude, Some(-122.4));
        assert_eq!(loc.latitude, Some(37.7));
        assert_eq!(loc.distance, Some(1.5));
    }

    #[test]
    fn test_geo_add_result() {
        let result = GeoAddResult {
            added: 5,
            updated: 2,
        };

        assert_eq!(result.added, 5);
        assert_eq!(result.updated, 2);
    }
}
