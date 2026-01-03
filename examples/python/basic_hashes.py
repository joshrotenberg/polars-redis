"""Basic example: scanning and writing Redis hashes."""

import polars as pl
import polars_redis as redis

URL = "redis://localhost:6379"


def main():
    # Define schema
    schema = {
        "name": pl.Utf8,
        "age": pl.Int64,
        "score": pl.Float64,
        "active": pl.Boolean,
    }

    # Scan hashes matching pattern
    lf = redis.scan_hashes(URL, pattern="user:*", schema=schema)

    # Filter and select (projection pushdown fetches only needed fields)
    df = lf.filter(pl.col("age") > 25).select(["_key", "name", "age"]).collect()

    print("Users over 25:")
    print(df)

    # Write new data back to Redis
    new_users = pl.DataFrame(
        {
            "name": ["Charlie", "Diana"],
            "age": [28, 35],
            "score": [88.5, 92.0],
            "active": [True, False],
        }
    )

    # Auto-generate keys from row index
    count = redis.write_hashes(
        new_users,
        URL,
        key_column=None,
        key_prefix="user:new:",
        ttl=3600,  # 1 hour TTL
    )
    print(f"\nWrote {count} new hashes")


if __name__ == "__main__":
    main()
