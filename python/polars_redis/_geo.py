"""Geospatial utilities for polars-redis.

This module provides functions for working with Redis GEO data structures,
including adding locations, querying by radius, calculating distances, and
converting results to Polars DataFrames.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import polars as pl

from polars_redis._internal import (
    py_geo_add,
    py_geo_dist,
    py_geo_dist_matrix,
    py_geo_hash,
    py_geo_pos,
    py_geo_radius,
    py_geo_radius_by_member,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

# Type alias for geo unit
GeoUnit = Literal["m", "km", "mi", "ft"]

# Type alias for geo sort order
GeoSort = Literal["asc", "desc"]


@dataclass
class GeoAddResult:
    """Result of a geo add operation."""

    added: int
    """Number of new locations added."""

    updated: int
    """Number of existing locations updated."""


def geo_add(
    url: str,
    key: str,
    locations: Sequence[tuple[str, float, float]],
) -> GeoAddResult:
    """Add locations to a Redis GEO set.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        key: Redis key for the geo set.
        locations: Sequence of (member, longitude, latitude) tuples.

    Returns:
        GeoAddResult with counts of added and updated locations.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> # Add some cities
        >>> result = redis.geo_add(
        ...     "redis://localhost",
        ...     "cities",
        ...     [
        ...         ("New York", -74.006, 40.7128),
        ...         ("Los Angeles", -118.2437, 34.0522),
        ...         ("Chicago", -87.6298, 41.8781),
        ...     ],
        ... )
        >>> print(f"Added {result.added}, updated {result.updated}")
    """
    result = py_geo_add(url, key, list(locations))
    return GeoAddResult(
        added=result["added"],
        updated=result["updated"],
    )


def geo_add_from_dataframe(
    url: str,
    key: str,
    df: pl.DataFrame,
    member_column: str,
    longitude_column: str,
    latitude_column: str,
) -> GeoAddResult:
    """Add locations from a DataFrame to a Redis GEO set.

    This is useful when you have location data in a DataFrame and want to
    add it to Redis for geospatial queries.

    Args:
        url: Redis connection URL.
        key: Redis key for the geo set.
        df: DataFrame with location data.
        member_column: Name of the column containing member names.
        longitude_column: Name of the column containing longitude values.
        latitude_column: Name of the column containing latitude values.

    Returns:
        GeoAddResult with counts of added and updated locations.

    Example:
        >>> import polars as pl
        >>> import polars_redis as redis
        >>>
        >>> # Create DataFrame with locations
        >>> df = pl.DataFrame({
        ...     "city": ["New York", "Los Angeles", "Chicago"],
        ...     "lon": [-74.006, -118.2437, -87.6298],
        ...     "lat": [40.7128, 34.0522, 41.8781],
        ... })
        >>>
        >>> # Add to Redis
        >>> result = redis.geo_add_from_dataframe(
        ...     "redis://localhost", "cities", df, "city", "lon", "lat"
        ... )
    """
    locations = list(
        zip(
            df[member_column].to_list(),
            df[longitude_column].to_list(),
            df[latitude_column].to_list(),
            strict=True,
        )
    )
    result = py_geo_add(url, key, locations)
    return GeoAddResult(
        added=result["added"],
        updated=result["updated"],
    )


def geo_radius(
    url: str,
    key: str,
    longitude: float,
    latitude: float,
    radius: float,
    unit: GeoUnit = "km",
    *,
    count: int | None = None,
    sort: GeoSort | None = None,
) -> pl.DataFrame:
    """Query locations within a radius of a point.

    Args:
        url: Redis connection URL.
        key: Redis key for the geo set.
        longitude: Center point longitude.
        latitude: Center point latitude.
        radius: Search radius.
        unit: Distance unit ("m", "km", "mi", "ft"). Default is "km".
        count: Maximum number of results to return.
        sort: Sort order by distance ("asc" or "desc").

    Returns:
        DataFrame with columns: member, longitude, latitude, distance.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> # Find cities within 500km of a point
        >>> df = redis.geo_radius(
        ...     "redis://localhost",
        ...     "cities",
        ...     -74.006, 40.7128,  # New York coordinates
        ...     500, "km",
        ...     sort="asc",
        ... )
        >>> print(df)
        shape: (2, 4)
        +----------+-----------+----------+----------+
        | member   | longitude | latitude | distance |
        | ---      | ---       | ---      | ---      |
        | str      | f64       | f64      | f64      |
        +----------+-----------+----------+----------+
        | New York | -74.006   | 40.7128  | 0.0      |
        | Boston   | -71.0589  | 42.3601  | 306.2    |
        +----------+-----------+----------+----------+
    """
    result = py_geo_radius(url, key, longitude, latitude, radius, unit, count, sort)

    if not result:
        return pl.DataFrame(
            schema={
                "member": pl.Utf8,
                "longitude": pl.Float64,
                "latitude": pl.Float64,
                "distance": pl.Float64,
            }
        )

    return pl.DataFrame(result)


def geo_radius_by_member(
    url: str,
    key: str,
    member: str,
    radius: float,
    unit: GeoUnit = "km",
    *,
    count: int | None = None,
    sort: GeoSort | None = None,
) -> pl.DataFrame:
    """Query locations within a radius of another member.

    This is useful when you want to find nearby locations relative to
    an existing member in the geo set.

    Args:
        url: Redis connection URL.
        key: Redis key for the geo set.
        member: Name of the member to search from.
        radius: Search radius.
        unit: Distance unit ("m", "km", "mi", "ft"). Default is "km".
        count: Maximum number of results to return.
        sort: Sort order by distance ("asc" or "desc").

    Returns:
        DataFrame with columns: member, longitude, latitude, distance.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> # Find cities within 1000km of Chicago
        >>> df = redis.geo_radius_by_member(
        ...     "redis://localhost",
        ...     "cities",
        ...     "Chicago",
        ...     1000, "km",
        ...     sort="asc",
        ... )
    """
    result = py_geo_radius_by_member(url, key, member, radius, unit, count, sort)

    if not result:
        return pl.DataFrame(
            schema={
                "member": pl.Utf8,
                "longitude": pl.Float64,
                "latitude": pl.Float64,
                "distance": pl.Float64,
            }
        )

    return pl.DataFrame(result)


def geo_dist(
    url: str,
    key: str,
    member1: str,
    member2: str,
    unit: GeoUnit = "km",
) -> float | None:
    """Get the distance between two members.

    Args:
        url: Redis connection URL.
        key: Redis key for the geo set.
        member1: First member name.
        member2: Second member name.
        unit: Distance unit ("m", "km", "mi", "ft"). Default is "km".

    Returns:
        Distance between members, or None if either member doesn't exist.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> # Get distance between New York and Los Angeles
        >>> dist = redis.geo_dist(
        ...     "redis://localhost",
        ...     "cities",
        ...     "New York",
        ...     "Los Angeles",
        ...     "km",
        ... )
        >>> print(f"Distance: {dist:.2f} km")
        Distance: 3935.75 km
    """
    return py_geo_dist(url, key, member1, member2, unit)


def geo_pos(
    url: str,
    key: str,
    members: Sequence[str],
) -> pl.DataFrame:
    """Get the positions of members.

    Args:
        url: Redis connection URL.
        key: Redis key for the geo set.
        members: Member names to look up.

    Returns:
        DataFrame with columns: member, longitude, latitude.
        Longitude and latitude are null for members that don't exist.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> # Get positions of cities
        >>> df = redis.geo_pos(
        ...     "redis://localhost",
        ...     "cities",
        ...     ["New York", "Los Angeles", "Unknown"],
        ... )
        >>> print(df)
        shape: (3, 3)
        +-------------+-----------+----------+
        | member      | longitude | latitude |
        | ---         | ---       | ---      |
        | str         | f64       | f64      |
        +-------------+-----------+----------+
        | New York    | -74.006   | 40.7128  |
        | Los Angeles | -118.2437 | 34.0522  |
        | Unknown     | null      | null     |
        +-------------+-----------+----------+
    """
    result = py_geo_pos(url, key, list(members))

    if not result:
        return pl.DataFrame(
            schema={
                "member": pl.Utf8,
                "longitude": pl.Float64,
                "latitude": pl.Float64,
            }
        )

    return pl.DataFrame(result)


def geo_dist_matrix(
    url: str,
    key: str,
    members: Sequence[str],
    unit: GeoUnit = "km",
) -> pl.DataFrame:
    """Compute a pairwise distance matrix between members.

    This computes the distance between all pairs of members and returns
    a matrix-style DataFrame. Useful for clustering or finding the
    closest/farthest pairs.

    Args:
        url: Redis connection URL.
        key: Redis key for the geo set.
        members: Member names to compute distances for.
        unit: Distance unit ("m", "km", "mi", "ft"). Default is "km".

    Returns:
        DataFrame with member names as the first column and remaining
        columns named after each member containing distances.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> # Compute distance matrix
        >>> df = redis.geo_dist_matrix(
        ...     "redis://localhost",
        ...     "cities",
        ...     ["New York", "Los Angeles", "Chicago"],
        ...     "km",
        ... )
        >>> print(df)
        shape: (3, 4)
        +-------------+----------+-------------+---------+
        | member      | New York | Los Angeles | Chicago |
        | ---         | ---      | ---         | ---     |
        | str         | f64      | f64         | f64     |
        +-------------+----------+-------------+---------+
        | New York    | 0.0      | 3935.75     | 1144.28 |
        | Los Angeles | 3935.75  | 0.0         | 2806.97 |
        | Chicago     | 1144.28  | 2806.97     | 0.0     |
        +-------------+----------+-------------+---------+
    """
    matrix = py_geo_dist_matrix(url, key, list(members), unit)
    member_list = list(members)

    # Build DataFrame with member column + distance columns
    data: dict[str, list] = {"member": member_list}
    for i, member in enumerate(member_list):
        data[member] = matrix[i]

    return pl.DataFrame(data)


def geo_hash(
    url: str,
    key: str,
    members: Sequence[str],
) -> pl.DataFrame:
    """Get geohash strings for members.

    Geohash is a string representation of a location that can be used
    for prefix-based proximity searches.

    Args:
        url: Redis connection URL.
        key: Redis key for the geo set.
        members: Member names to get geohashes for.

    Returns:
        DataFrame with columns: member, geohash.
        Geohash is null for members that don't exist.

    Example:
        >>> import polars_redis as redis
        >>>
        >>> # Get geohashes
        >>> df = redis.geo_hash(
        ...     "redis://localhost",
        ...     "cities",
        ...     ["New York", "Los Angeles"],
        ... )
        >>> print(df)
        shape: (2, 2)
        +-------------+-------------+
        | member      | geohash     |
        | ---         | ---         |
        | str         | str         |
        +-------------+-------------+
        | New York    | dr5regw3pp0 |
        | Los Angeles | 9q5ctr1fzp0 |
        +-------------+-------------+
    """
    result = py_geo_hash(url, key, list(members))

    if not result:
        return pl.DataFrame(
            schema={
                "member": pl.Utf8,
                "geohash": pl.Utf8,
            }
        )

    return pl.DataFrame({"member": [r[0] for r in result], "geohash": [r[1] for r in result]})
