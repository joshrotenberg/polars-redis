#!/usr/bin/env python3
"""Example: Scanning Redis JSON documents with polars-redis.

This example demonstrates how to scan RedisJSON documents and work with them
as Polars LazyFrames.

Prerequisites:
    - Redis 8+ running on localhost:6379 (has native JSON support)
    - Sample data loaded (run setup_sample_data.py first)
"""

import polars as pl
import polars_redis


def main() -> None:
    url = "redis://localhost:6379"

    # Define the schema for our product JSON documents
    schema = {
        "name": pl.Utf8,
        "category": pl.Utf8,
        "price": pl.Float64,
        "quantity": pl.Int64,
        "in_stock": pl.Boolean,
    }

    # Basic scan
    print("=== Basic JSON Scan ===")
    lf = polars_redis.scan_json(url, pattern="product:*", schema=schema)
    df = lf.collect()
    print(f"Found {len(df)} products")
    print(df.head(5))
    print()

    # Filter by category
    print("=== Filter by Category ===")
    lf = polars_redis.scan_json(url, pattern="product:*", schema=schema)
    df = lf.filter(pl.col("category") == "Electronics").collect()
    print(f"Found {len(df)} electronics products")
    print(df)
    print()

    # Find expensive items
    print("=== Expensive Items (price > 500) ===")
    lf = polars_redis.scan_json(url, pattern="product:*", schema=schema)
    df = lf.filter(pl.col("price") > 500).select(["name", "price"]).collect()
    print(df)
    print()

    # Aggregation by category
    print("=== Aggregation by Category ===")
    lf = polars_redis.scan_json(url, pattern="product:*", schema=schema)
    df = (
        lf.group_by("category")
        .agg(
            pl.col("price").mean().alias("avg_price"),
            pl.col("quantity").sum().alias("total_quantity"),
            pl.len().alias("product_count"),
        )
        .sort("avg_price", descending=True)
        .collect()
    )
    print(df)
    print()

    # In-stock items only
    print("=== In-Stock Items ===")
    lf = polars_redis.scan_json(url, pattern="product:*", schema=schema)
    df = (
        lf.filter(pl.col("in_stock") == True)  # noqa: E712
        .select(["name", "quantity"])
        .sort("quantity", descending=True)
        .collect()
    )
    print(df)
    print()

    # Calculate inventory value
    print("=== Inventory Value ===")
    lf = polars_redis.scan_json(url, pattern="product:*", schema=schema)
    df = (
        lf.with_columns((pl.col("price") * pl.col("quantity")).alias("total_value"))
        .select(["name", "price", "quantity", "total_value"])
        .sort("total_value", descending=True)
        .collect()
    )
    print(df)
    total = df["total_value"].sum()
    print(f"\nTotal inventory value: ${total:,.2f}")


if __name__ == "__main__":
    main()
