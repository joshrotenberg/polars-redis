#!/usr/bin/env python3
"""
Example: Scanning Redis hashes with polars-redis Python API.

This example demonstrates the high-level Python API that wraps the
Rust implementation and integrates with Polars' IO plugin system.

Prerequisites:
    - Redis running on localhost:6379
    - Sample data loaded (run setup_sample_data.py first)
    - polars-redis installed: pip install polars-redis

Run with: python scan_hashes.py
"""

import os

import polars as pl

# Import the polars-redis plugin
import polars_redis as redis


def main():
    url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    print(f"Connecting to: {url}\n")

    # =========================================================================
    # Example 1: Basic scanning - returns a LazyFrame
    # =========================================================================
    print("=== Example 1: Basic Hash Scanning ===\n")

    # Define schema for user hashes
    schema = {
        "name": pl.Utf8,
        "email": pl.Utf8,
        "age": pl.Int64,
        "score": pl.Float64,
        "active": pl.Boolean,
    }

    # scan_hashes returns a LazyFrame - no data fetched yet!
    lf = redis.scan_hashes(
        url,
        pattern="user:*",
        schema=schema,
        include_key=True,
        key_column_name="_key",
    )

    print(f"LazyFrame schema: {lf.collect_schema()}")
    print(f"Type: {type(lf)}\n")

    # Collect to execute the scan
    df = lf.collect()
    print(f"Collected {len(df)} rows")
    print(df.head(5))
    print()

    # =========================================================================
    # Example 2: Projection pushdown - Polars optimizer kicks in
    # =========================================================================
    print("=== Example 2: Projection Pushdown ===\n")

    lf = redis.scan_hashes(
        url,
        pattern="user:*",
        schema=schema,
        include_key=True,
    )

    # Only select name and age - the optimizer will push this down
    # to the Redis layer, using HMGET instead of HGETALL
    result = lf.select(["_key", "name", "age"]).collect()

    print("Only fetched columns: name, age (plus key)")
    print(result.head(5))
    print()

    # =========================================================================
    # Example 3: Filtering with predicate
    # =========================================================================
    print("=== Example 3: Filtering ===\n")

    lf = redis.scan_hashes(
        url,
        pattern="user:*",
        schema=schema,
    )

    # Filter users over 30 with score > 50
    # Note: Without FT.SEARCH index, filtering happens in Polars after fetch
    result = (
        lf.filter(pl.col("age") > 30)
        .filter(pl.col("score") > 50.0)
        .select(["name", "age", "score"])
        .sort("score", descending=True)
        .collect()
    )

    print(f"Users over 30 with score > 50: {len(result)} rows")
    print(result.head(10))
    print()

    # =========================================================================
    # Example 4: Aggregation
    # =========================================================================
    print("=== Example 4: Aggregation ===\n")

    lf = redis.scan_hashes(
        url,
        pattern="user:*",
        schema=schema,
    )

    # Group by active status and compute stats
    stats = (
        lf.group_by("active")
        .agg(
            [
                pl.len().alias("count"),
                pl.col("age").mean().alias("avg_age"),
                pl.col("score").mean().alias("avg_score"),
                pl.col("score").max().alias("max_score"),
            ]
        )
        .collect()
    )

    print("Stats by active status:")
    print(stats)
    print()

    # =========================================================================
    # Example 5: Using .head() for limit pushdown
    # =========================================================================
    print("=== Example 5: head() Pushdown ===\n")

    lf = redis.scan_hashes(
        url,
        pattern="user:*",
        schema=schema,
    )

    # .head(5) will push n_rows=5 down to the scanner
    result = lf.head(5).collect()
    print(f"Using .head(5): {len(result)} rows")
    print(result)
    print()

    # =========================================================================
    # Example 6: Joining Redis data with local data
    # =========================================================================
    print("=== Example 6: Join with Local Data ===\n")

    # Scan users from Redis
    users_lf = redis.scan_hashes(
        url,
        pattern="user:*",
        schema={"name": pl.Utf8},
        include_key=True,
    )

    # Create some local "orders" data
    orders = pl.DataFrame(
        {
            "user_key": ["user:1", "user:1", "user:2", "user:3", "user:3", "user:3"],
            "order_id": [101, 102, 201, 301, 302, 303],
            "amount": [99.99, 149.50, 299.00, 49.99, 79.99, 199.99],
        }
    ).lazy()

    # Join them
    result = (
        users_lf.join(orders, left_on="_key", right_on="user_key", how="inner")
        .group_by(["_key", "name"])
        .agg(
            [
                pl.len().alias("order_count"),
                pl.col("amount").sum().alias("total_spent"),
            ]
        )
        .sort("total_spent", descending=True)
        .collect()
    )

    print("User order totals:")
    print(result)
    print()

    # =========================================================================
    # Example 7: Custom batch configuration
    # =========================================================================
    print("=== Example 7: Custom Batch Configuration ===\n")

    lf = redis.scan_hashes(
        url,
        pattern="user:*",
        schema=schema,
        # Batch tuning options
        batch_size=25,  # Rows per batch yielded to Polars
        count_hint=50,  # SCAN COUNT hint to Redis
    )

    result = lf.collect()
    print(f"Scanned with custom batch settings: {len(result)} rows")
    print()

    # =========================================================================
    # Example 8: Explain the query plan
    # =========================================================================
    print("=== Example 8: Query Plan ===\n")

    lf = redis.scan_hashes(
        url,
        pattern="user:*",
        schema=schema,
        include_key=True,
    )

    query = lf.filter(pl.col("age") > 25).select(["_key", "name", "age"]).head(100)

    print("Query plan (shows pushdown):")
    print(query.explain())
    print()

    # =========================================================================
    # Example 9: Error handling
    # =========================================================================
    print("=== Example 9: Error Handling ===\n")

    try:
        # This should return empty if pattern matches no keys
        lf = redis.scan_hashes(
            url,
            pattern="nonexistent:*",
            schema={"foo": pl.Utf8},
        )
        result = lf.collect()
        print(f"Empty pattern result: {len(result)} rows")
        print(result)
    except Exception as e:
        print(f"Error: {e}")

    print()

    print("=== All examples complete ===")


if __name__ == "__main__":
    main()
