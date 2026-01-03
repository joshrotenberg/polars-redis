"""Example: automatic schema inference."""

import polars_redis as redis

URL = "redis://localhost:6379"


def main():
    # Infer schema from existing hashes
    schema = redis.infer_hash_schema(URL, pattern="user:*", sample_size=100)
    print("Inferred hash schema:")
    for field, dtype in schema.items():
        print(f"  {field}: {dtype}")

    # Use inferred schema to scan
    df = redis.read_hashes(URL, pattern="user:*", schema=schema)
    print(f"\nLoaded {len(df)} rows")
    print(df.head())

    # Infer JSON schema
    json_schema = redis.infer_json_schema(URL, pattern="product:*", sample_size=50)
    print("\nInferred JSON schema:")
    for field, dtype in json_schema.items():
        print(f"  {field}: {dtype}")


if __name__ == "__main__":
    main()
