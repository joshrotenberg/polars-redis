"""Example: working with Redis strings."""

import polars as pl
import polars_redis as redis

URL = "redis://localhost:6379"


def main():
    # Scan string values as integers (counters)
    lf = redis.scan_strings(
        URL,
        pattern="counter:*",
        value_type=pl.Int64,
    )

    # Sum all counters
    total = lf.select(pl.col("value").sum().alias("total")).collect()
    print(f"Total count: {total['total'][0]}")

    # Scan as strings for cache entries
    cache = redis.read_strings(URL, pattern="cache:*")
    print(f"\nCache entries: {len(cache)}")
    print(cache.head())

    # Write counters
    counters = pl.DataFrame({"value": ["100", "200", "300"]})

    count = redis.write_strings(
        counters,
        URL,
        key_column=None,
        key_prefix="counter:page:",
        ttl=86400,  # 1 day TTL
    )
    print(f"\nWrote {count} counters")


if __name__ == "__main__":
    main()
