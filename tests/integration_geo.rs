//! Integration tests for Redis geospatial operations.
//!
//! These tests require a running Redis instance.
//! Run with: `cargo test --test integration_geo`

use polars_redis::{
    GeoSort, geo_add, geo_dist, geo_dist_matrix, geo_hash, geo_pos, geo_radius,
    geo_radius_by_member,
};

mod common;
use common::{cleanup_keys, ensure_redis, get_redis_url};

/// Test geo_add adds locations correctly.
#[tokio::test]
async fn test_geo_add_basic() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:geotest:*");

    let locations = vec![
        ("New York".to_string(), -74.006, 40.7128),
        ("Los Angeles".to_string(), -118.2437, 34.0522),
        ("Chicago".to_string(), -87.6298, 41.8781),
    ];

    let (result, result2) = tokio::task::spawn_blocking(move || {
        let result =
            geo_add(&url, "rust:geotest:cities", &locations).expect("Failed to add geo locations");

        // Add same locations again - should update not add
        let result2 =
            geo_add(&url, "rust:geotest:cities", &locations).expect("Failed to add geo locations");

        (result, result2)
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(result.added, 3);
    assert_eq!(result.updated, 0);
    assert_eq!(result2.added, 0);
    assert_eq!(result2.updated, 3);

    cleanup_keys("rust:geotest:*");
}

/// Test geo_add with mixed new and existing locations.
#[tokio::test]
async fn test_geo_add_mixed() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:geomixed:*");

    let (result_added, result_updated) = tokio::task::spawn_blocking(move || {
        // Add initial locations
        let initial = vec![
            ("New York".to_string(), -74.006, 40.7128),
            ("Los Angeles".to_string(), -118.2437, 34.0522),
        ];

        geo_add(&url, "rust:geomixed:cities", &initial).expect("Failed to add initial");

        // Add mixed - one new, one update
        let mixed = vec![
            ("Los Angeles".to_string(), -118.2437, 34.0522), // Update
            ("Chicago".to_string(), -87.6298, 41.8781),      // New
        ];

        let result = geo_add(&url, "rust:geomixed:cities", &mixed).expect("Failed to add mixed");
        (result.added, result.updated)
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(result_added, 1);
    assert_eq!(result_updated, 1);

    cleanup_keys("rust:geomixed:*");
}

/// Test geo_radius finds locations within radius.
#[tokio::test]
async fn test_geo_radius_basic() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:georadius:*");

    let result = tokio::task::spawn_blocking(move || {
        // Add cities
        let locations = vec![
            ("New York".to_string(), -74.006, 40.7128),
            ("Philadelphia".to_string(), -75.1652, 39.9526), // ~130km from NYC
            ("Boston".to_string(), -71.0589, 42.3601),       // ~306km from NYC
            ("Los Angeles".to_string(), -118.2437, 34.0522), // ~3936km from NYC
        ];

        geo_add(&url, "rust:georadius:cities", &locations).expect("Failed to add locations");

        // Find cities within 200km of NYC
        geo_radius(
            &url,
            "rust:georadius:cities",
            -74.006,
            40.7128,
            200.0,
            "km",
            None,
            Some(GeoSort::Asc),
        )
        .expect("Failed to query radius")
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(result.len(), 2); // NYC and Philadelphia
    assert_eq!(result[0].name, "New York");
    assert!(result[0].distance.unwrap() < 1.0); // NYC is at center
    assert_eq!(result[1].name, "Philadelphia");

    cleanup_keys("rust:georadius:*");
}

/// Test geo_radius with different units.
#[tokio::test]
async fn test_geo_radius_units() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:geounits:*");

    let (result_m_len, result_mi_len) = tokio::task::spawn_blocking(move || {
        let locations = vec![
            ("Point A".to_string(), 0.0, 0.0),
            ("Point B".to_string(), 0.01, 0.0), // ~1.11km east
        ];

        geo_add(&url, "rust:geounits:points", &locations).expect("Failed to add locations");

        // Query in meters
        let result_m = geo_radius(
            &url,
            "rust:geounits:points",
            0.0,
            0.0,
            2000.0, // 2km in meters
            "m",
            None,
            None,
        )
        .expect("Failed to query radius in meters");

        // Query in miles (should get both points as they're close)
        let result_mi = geo_radius(
            &url,
            "rust:geounits:points",
            0.0,
            0.0,
            1.0, // 1 mile
            "mi",
            None,
            None,
        )
        .expect("Failed to query radius in miles");

        (result_m.len(), result_mi.len())
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(result_m_len, 2);
    assert_eq!(result_mi_len, 2);

    cleanup_keys("rust:geounits:*");
}

/// Test geo_radius with count limit.
#[tokio::test]
async fn test_geo_radius_with_count() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:geocount:*");

    let result = tokio::task::spawn_blocking(move || {
        let locations = vec![
            ("A".to_string(), 0.0, 0.0),
            ("B".to_string(), 0.001, 0.0),
            ("C".to_string(), 0.002, 0.0),
            ("D".to_string(), 0.003, 0.0),
            ("E".to_string(), 0.004, 0.0),
        ];

        geo_add(&url, "rust:geocount:points", &locations).expect("Failed to add locations");

        // Get only 2 closest
        geo_radius(
            &url,
            "rust:geocount:points",
            0.0,
            0.0,
            1000.0,
            "km",
            Some(2),
            Some(GeoSort::Asc),
        )
        .expect("Failed to query radius")
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].name, "A");
    assert_eq!(result[1].name, "B");

    cleanup_keys("rust:geocount:*");
}

/// Test geo_radius_by_member.
#[tokio::test]
async fn test_geo_radius_by_member() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:geomember:*");

    let result = tokio::task::spawn_blocking(move || {
        let locations = vec![
            ("New York".to_string(), -74.006, 40.7128),
            ("Philadelphia".to_string(), -75.1652, 39.9526),
            ("Boston".to_string(), -71.0589, 42.3601),
            ("Los Angeles".to_string(), -118.2437, 34.0522),
        ];

        geo_add(&url, "rust:geomember:cities", &locations).expect("Failed to add locations");

        // Find cities within 400km of Boston
        geo_radius_by_member(
            &url,
            "rust:geomember:cities",
            "Boston",
            400.0,
            "km",
            None,
            Some(GeoSort::Asc),
        )
        .expect("Failed to query by member")
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(result.len(), 2); // Boston and NYC
    assert_eq!(result[0].name, "Boston");
    assert_eq!(result[1].name, "New York");

    cleanup_keys("rust:geomember:*");
}

/// Test geo_dist returns correct distance.
#[tokio::test]
async fn test_geo_dist_basic() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:geodist:*");

    let (dist_km, dist_mi) = tokio::task::spawn_blocking(move || {
        let locations = vec![
            ("New York".to_string(), -74.006, 40.7128),
            ("Los Angeles".to_string(), -118.2437, 34.0522),
        ];

        geo_add(&url, "rust:geodist:cities", &locations).expect("Failed to add locations");

        // Get distance in km
        let dist_km = geo_dist(&url, "rust:geodist:cities", "New York", "Los Angeles", "km")
            .expect("Failed to get distance");

        // Get distance in miles
        let dist_mi = geo_dist(&url, "rust:geodist:cities", "New York", "Los Angeles", "mi")
            .expect("Failed to get distance");

        (dist_km, dist_mi)
    })
    .await
    .expect("spawn_blocking failed");

    assert!(dist_km.is_some());
    let km = dist_km.unwrap();
    assert!(km > 3900.0 && km < 4000.0); // ~3936km

    assert!(dist_mi.is_some());
    let mi = dist_mi.unwrap();
    assert!(mi > 2400.0 && mi < 2500.0); // ~2445 miles

    cleanup_keys("rust:geodist:*");
}

/// Test geo_dist returns None for non-existent member.
#[tokio::test]
async fn test_geo_dist_nonexistent() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:geodistnone:*");

    let dist = tokio::task::spawn_blocking(move || {
        let locations = vec![("New York".to_string(), -74.006, 40.7128)];

        geo_add(&url, "rust:geodistnone:cities", &locations).expect("Failed to add locations");

        // Get distance to non-existent member
        geo_dist(
            &url,
            "rust:geodistnone:cities",
            "New York",
            "NonExistent",
            "km",
        )
        .expect("Failed to get distance")
    })
    .await
    .expect("spawn_blocking failed");

    assert!(dist.is_none());

    cleanup_keys("rust:geodistnone:*");
}

/// Test geo_pos returns correct positions.
#[tokio::test]
async fn test_geo_pos_basic() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:geopos:*");

    let result = tokio::task::spawn_blocking(move || {
        let locations = vec![
            ("New York".to_string(), -74.006, 40.7128),
            ("Los Angeles".to_string(), -118.2437, 34.0522),
        ];

        geo_add(&url, "rust:geopos:cities", &locations).expect("Failed to add locations");

        let members = vec![
            "New York".to_string(),
            "Los Angeles".to_string(),
            "NonExistent".to_string(),
        ];

        geo_pos(&url, "rust:geopos:cities", &members).expect("Failed to get positions")
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(result.len(), 3);

    // Check New York
    let ny = result.iter().find(|l| l.name == "New York").unwrap();
    assert!(ny.longitude.is_some());
    assert!(ny.latitude.is_some());
    let ny_lon = ny.longitude.unwrap();
    let ny_lat = ny.latitude.unwrap();
    assert!((ny_lon - (-74.006)).abs() < 0.001);
    assert!((ny_lat - 40.7128).abs() < 0.001);

    // Check LA
    let la = result.iter().find(|l| l.name == "Los Angeles").unwrap();
    assert!(la.longitude.is_some());
    assert!(la.latitude.is_some());

    // Check non-existent (should have None coordinates)
    let none = result.iter().find(|l| l.name == "NonExistent").unwrap();
    assert!(none.longitude.is_none());
    assert!(none.latitude.is_none());

    cleanup_keys("rust:geopos:*");
}

/// Test geo_dist_matrix computes pairwise distances.
#[tokio::test]
async fn test_geo_dist_matrix_basic() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:geomatrix:*");

    let matrix = tokio::task::spawn_blocking(move || {
        let locations = vec![
            ("A".to_string(), 0.0, 0.0),
            ("B".to_string(), 1.0, 0.0),
            ("C".to_string(), 0.0, 1.0),
        ];

        geo_add(&url, "rust:geomatrix:points", &locations).expect("Failed to add locations");

        let members = vec!["A".to_string(), "B".to_string(), "C".to_string()];

        geo_dist_matrix(&url, "rust:geomatrix:points", &members, "km")
            .expect("Failed to compute matrix")
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(matrix.len(), 3);
    assert_eq!(matrix[0].len(), 3);

    // Diagonal should be 0 (distance to self)
    assert!((matrix[0][0].unwrap() - 0.0).abs() < 0.001);
    assert!((matrix[1][1].unwrap() - 0.0).abs() < 0.001);
    assert!((matrix[2][2].unwrap() - 0.0).abs() < 0.001);

    // Matrix should be symmetric
    assert!((matrix[0][1].unwrap() - matrix[1][0].unwrap()).abs() < 0.001);
    assert!((matrix[0][2].unwrap() - matrix[2][0].unwrap()).abs() < 0.001);
    assert!((matrix[1][2].unwrap() - matrix[2][1].unwrap()).abs() < 0.001);

    // A to B and A to C should be roughly equal (1 degree each)
    let ab = matrix[0][1].unwrap();
    let ac = matrix[0][2].unwrap();
    // Both should be ~111km (1 degree at equator)
    assert!(ab > 100.0 && ab < 120.0);
    assert!(ac > 100.0 && ac < 120.0);

    cleanup_keys("rust:geomatrix:*");
}

/// Test geo_hash returns geohash strings.
#[tokio::test]
async fn test_geo_hash_basic() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:geohash:*");

    let result = tokio::task::spawn_blocking(move || {
        let locations = vec![
            ("New York".to_string(), -74.006, 40.7128),
            ("Los Angeles".to_string(), -118.2437, 34.0522),
        ];

        geo_add(&url, "rust:geohash:cities", &locations).expect("Failed to add locations");

        let members = vec![
            "New York".to_string(),
            "Los Angeles".to_string(),
            "NonExistent".to_string(),
        ];

        geo_hash(&url, "rust:geohash:cities", &members).expect("Failed to get geohashes")
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(result.len(), 3);

    // Check that we got hashes for existing members
    let ny = result.iter().find(|(m, _)| m == "New York").unwrap();
    assert!(ny.1.is_some());
    let ny_hash = ny.1.as_ref().unwrap();
    assert!(!ny_hash.is_empty());
    // NYC geohash should start with "dr5r" (approximate)
    assert!(ny_hash.starts_with("dr5r"));

    let la = result.iter().find(|(m, _)| m == "Los Angeles").unwrap();
    assert!(la.1.is_some());
    let la_hash = la.1.as_ref().unwrap();
    assert!(!la_hash.is_empty());
    // LA geohash should start with "9q5c" (approximate)
    assert!(la_hash.starts_with("9q5c"));

    // Non-existent should have None
    let none = result.iter().find(|(m, _)| m == "NonExistent").unwrap();
    assert!(none.1.is_none());

    cleanup_keys("rust:geohash:*");
}

/// Test empty geo set returns empty results.
#[tokio::test]
async fn test_geo_empty_set() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:geoempty:*");

    let result = tokio::task::spawn_blocking(move || {
        // Query empty set
        geo_radius(
            &url,
            "rust:geoempty:cities",
            0.0,
            0.0,
            1000.0,
            "km",
            None,
            None,
        )
        .expect("Failed to query empty set")
    })
    .await
    .expect("spawn_blocking failed");

    assert!(result.is_empty());

    cleanup_keys("rust:geoempty:*");
}

/// Test large geo set operations.
#[tokio::test]
async fn test_geo_large_set() {
    let _ = ensure_redis().await;
    let url = get_redis_url().to_string();

    cleanup_keys("rust:geolarge:*");

    let (result_added, radius_result_len) = tokio::task::spawn_blocking(move || {
        // Add 1000 locations in a grid
        // Redis GEOADD requires: -180 <= lon <= 180, -85.05112878 <= lat <= 85.05112878
        let mut locations = Vec::new();
        for i in 0..100 {
            for j in 0..10 {
                let name = format!("point_{}_{}", i, j);
                let lon = -179.0 + (i as f64 * 3.58); // -179 to 179
                let lat = -85.0 + (j as f64 * 17.0); // -85 to 85
                locations.push((name, lon, lat));
            }
        }

        let result =
            geo_add(&url, "rust:geolarge:points", &locations).expect("Failed to add locations");

        // Query around center
        let radius_result = geo_radius(
            &url,
            "rust:geolarge:points",
            0.0,
            0.0,
            5000.0,
            "km",
            Some(100),
            Some(GeoSort::Asc),
        )
        .expect("Failed to query radius");

        (result.added, radius_result.len())
    })
    .await
    .expect("spawn_blocking failed");

    assert_eq!(result_added, 1000);

    // Should get some results near the equator
    assert!(radius_result_len > 0);
    assert!(radius_result_len <= 100);

    cleanup_keys("rust:geolarge:*");
}
