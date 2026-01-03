"""Example: TTL and metadata columns."""

import polars as pl
import polars_redis as redis

URL = "redis://localhost:6379"


def main():
    schema = {"name": pl.Utf8, "email": pl.Utf8}

    # Include TTL and row index columns
    lf = redis.scan_hashes(
        URL,
        pattern="user:*",
        schema=schema,
        include_key=True,
        include_ttl=True,
        include_row_index=True,
    )

    df = lf.collect()
    print("Users with metadata:")
    print(df)

    # Find keys expiring soon (TTL < 1 hour)
    expiring = df.filter((pl.col("_ttl") > 0) & (pl.col("_ttl") < 3600))
    print(f"\nKeys expiring within 1 hour: {len(expiring)}")

    # Keys without expiry (TTL = -1)
    no_expiry = df.filter(pl.col("_ttl") == -1)
    print(f"Keys without expiry: {len(no_expiry)}")


if __name__ == "__main__":
    main()
