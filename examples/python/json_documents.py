"""Example: working with RedisJSON documents."""

import polars as pl
import polars_redis as redis

URL = "redis://localhost:6379"


def main():
    # Scan JSON documents
    schema = {
        "title": pl.Utf8,
        "category": pl.Utf8,
        "price": pl.Float64,
        "in_stock": pl.Boolean,
    }

    lf = redis.scan_json(URL, pattern="product:*", schema=schema)

    # Aggregate by category
    summary = (
        lf.group_by("category")
        .agg(
            [
                pl.len().alias("count"),
                pl.col("price").mean().alias("avg_price"),
                pl.col("price").max().alias("max_price"),
            ]
        )
        .sort("avg_price", descending=True)
        .collect()
    )

    print("Products by category:")
    print(summary)

    # Write new products
    products = pl.DataFrame(
        {
            "title": ["Widget", "Gadget"],
            "category": ["tools", "electronics"],
            "price": [19.99, 49.99],
            "in_stock": [True, True],
        }
    )

    count = redis.write_json(
        products,
        URL,
        key_column=None,
        key_prefix="product:",
    )
    print(f"\nWrote {count} products")


if __name__ == "__main__":
    main()
