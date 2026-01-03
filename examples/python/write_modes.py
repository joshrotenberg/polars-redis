"""Example: write modes (fail, replace, append)."""

import polars as pl
import polars_redis as redis

URL = "redis://localhost:6379"


def main():
    # Sample data
    users = pl.DataFrame(
        {
            "_key": ["user:1", "user:2"],
            "name": ["Alice", "Bob"],
            "age": [30, 25],
        }
    )

    # Replace mode (default): overwrites existing keys
    count = redis.write_hashes(users, URL, if_exists="replace")
    print(f"Replace mode: wrote {count} hashes")

    # Fail mode: skips keys that already exist
    count = redis.write_hashes(users, URL, if_exists="fail")
    print(f"Fail mode: wrote {count} hashes (0 because keys exist)")

    # Append mode: merges fields into existing hashes
    updates = pl.DataFrame(
        {
            "_key": ["user:1", "user:2"],
            "score": [95.5, 88.0],  # Add new field
        }
    )
    count = redis.write_hashes(updates, URL, if_exists="append")
    print(f"Append mode: updated {count} hashes with new field")

    # Verify
    df = redis.read_hashes(
        URL,
        pattern="user:*",
        schema={"name": pl.Utf8, "age": pl.Int64, "score": pl.Float64},
    )
    print("\nFinal data:")
    print(df)


if __name__ == "__main__":
    main()
