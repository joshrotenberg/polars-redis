#!/usr/bin/env python3
"""Load Redis Bike Co data into Redis using polars-redis.

This script demonstrates how to load JSON data into Redis as JSON documents
using polars-redis write functions.

Usage:
    python load_data.py [--url REDIS_URL]

Requirements:
    - Redis Stack running (for JSON support)
    - polars-redis installed: pip install polars-redis
"""

import argparse
import json
from pathlib import Path

import polars as pl
import polars_redis as redis


def load_bikes(url: str, data_dir: Path) -> int:
    """Load bike data from JSON file into Redis.

    Args:
        url: Redis connection URL
        data_dir: Path to the data directory

    Returns:
        Number of bikes loaded
    """
    # Read the JSON file
    with open(data_dir / "bikes.json") as f:
        data = json.load(f)

    bikes = data["data"]

    # Convert to a flat DataFrame structure
    # We'll flatten the nested specs object
    rows = []
    for bike in bikes:
        row = {
            "stockcode": bike["stockcode"],
            "model": bike["model"],
            "brand": bike["brand"],
            "price": bike["price"],
            "type": bike["type"],
            "description": bike["description"],
            "material": bike["specs"]["material"],
            "weight": bike["specs"]["weight"],
        }
        rows.append(row)

    df = pl.DataFrame(rows)

    print(f"Loaded {len(df)} bikes from JSON file")
    print(f"Schema: {df.schema}")
    print(f"\nSample data:")
    print(df.head(3))

    # Write to Redis as JSON documents
    # Key pattern: redisbikeco:bike:{stockcode}
    result = redis.write_json(
        df,
        url=url,
        key_column="stockcode",
        key_prefix="redisbikeco:bike:",
    )

    print(f"\nWrote {result.success_count} bikes to Redis")
    if result.failure_count > 0:
        print(f"  Failures: {result.failure_count}")

    return result.success_count


def load_stores(url: str, data_dir: Path) -> int:
    """Load store data from JSON file into Redis.

    Args:
        url: Redis connection URL
        data_dir: Path to the data directory

    Returns:
        Number of stores loaded
    """
    # Read the JSON file
    with open(data_dir / "stores.json") as f:
        data = json.load(f)

    stores = data["data"]

    # Convert to a flat DataFrame structure
    rows = []
    for store in stores:
        row = {
            "storecode": store["storecode"],
            "storename": store["storename"],
            "street": store["address"]["street"],
            "city": store["address"]["city"],
            "state": store["address"]["state"],
            "pin": store["address"]["pin"],
            "country": store["address"]["country"],
            "position": store["position"],
            "amenities": ",".join(store["amenities"]),  # Store as comma-separated
        }
        rows.append(row)

    df = pl.DataFrame(rows)

    print(f"\nLoaded {len(df)} stores from JSON file")
    print(f"Schema: {df.schema}")
    print(f"\nSample data:")
    print(df.head(3))

    # Write to Redis as JSON documents
    # Key pattern: redisbikeco:store:{storecode}
    result = redis.write_json(
        df,
        url=url,
        key_column="storecode",
        key_prefix="redisbikeco:store:",
    )

    print(f"\nWrote {result.success_count} stores to Redis")
    if result.failure_count > 0:
        print(f"  Failures: {result.failure_count}")

    return result.success_count


def create_search_index(url: str) -> None:
    """Create RediSearch index for bikes (requires redis-py).

    This creates an index for searching bikes by brand, type, price, etc.
    """
    try:
        import redis as redis_py

        r = redis_py.from_url(url)

        # Drop existing index if it exists
        try:
            r.execute_command("FT.DROPINDEX", "bikes_idx", "DD")
            print("Dropped existing bikes_idx index")
        except Exception:
            pass

        # Create new index
        r.execute_command(
            "FT.CREATE",
            "bikes_idx",
            "ON",
            "JSON",
            "PREFIX",
            "1",
            "redisbikeco:bike:",
            "SCHEMA",
            "$.stockcode",
            "AS",
            "stockcode",
            "TAG",
            "$.model",
            "AS",
            "model",
            "TEXT",
            "$.brand",
            "AS",
            "brand",
            "TAG",
            "$.price",
            "AS",
            "price",
            "NUMERIC",
            "SORTABLE",
            "$.type",
            "AS",
            "type",
            "TAG",
            "$.description",
            "AS",
            "description",
            "TEXT",
            "$.material",
            "AS",
            "material",
            "TAG",
            "$.weight",
            "AS",
            "weight",
            "NUMERIC",
            "SORTABLE",
        )
        print("\nCreated RediSearch index: bikes_idx")

    except ImportError:
        print("\nNote: redis-py not installed, skipping index creation")
        print("Install with: pip install redis")
        print(
            "Then run: python -c \"import redis; r = redis.from_url('redis://localhost:6379'); ...\""
        )


def main():
    parser = argparse.ArgumentParser(description="Load Redis Bike Co data into Redis")
    parser.add_argument(
        "--url",
        default="redis://localhost:6379",
        help="Redis connection URL (default: redis://localhost:6379)",
    )
    parser.add_argument(
        "--create-index",
        action="store_true",
        help="Create RediSearch index after loading data",
    )
    args = parser.parse_args()

    # Find the data directory
    script_dir = Path(__file__).parent
    data_dir = script_dir / "data"

    if not data_dir.exists():
        print(f"Error: Data directory not found: {data_dir}")
        return 1

    print("=" * 60)
    print("Redis Bike Co - Data Loader (polars-redis)")
    print("=" * 60)
    print(f"\nConnecting to: {args.url}")

    # Load data
    bikes_count = load_bikes(args.url, data_dir)
    stores_count = load_stores(args.url, data_dir)

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  Bikes loaded:  {bikes_count}")
    print(f"  Stores loaded: {stores_count}")

    # Optionally create search index
    if args.create_index:
        create_search_index(args.url)

    print("\nDone! You can now use polars-redis to query the data.")
    print("\nExample:")
    print("  import polars_redis as redis")
    print(
        '  df = redis.scan_json("redis://localhost:6379", pattern="redisbikeco:bike:*").collect()'
    )
    print("  print(df)")

    return 0


if __name__ == "__main__":
    exit(main())
