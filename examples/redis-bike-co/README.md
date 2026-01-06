# Redis Bike Co Example

This example demonstrates how to use **polars-redis** with a realistic dataset from [Redis Bike Co](https://github.com/redis-developer/redis-bike-co), a fictional bicycle retail company.

## Dataset

The dataset includes:
- **111 bikes** with details like brand, model, price, type, material, and weight
- **5 stores** with location and amenity information

## Prerequisites

1. **Redis Stack** (for JSON and RediSearch support):
   ```bash
   docker run -d --name redis-stack -p 6379:6379 redis/redis-stack:latest
   ```

2. **polars-redis**:
   ```bash
   pip install polars-redis
   ```

3. **redis-py** (optional, for creating search indexes):
   ```bash
   pip install redis
   ```

## Quick Start

### Load Data

```bash
python load_data.py
```

This loads all bikes and stores into Redis with key patterns:
- `redisbikeco:bike:{stockcode}`
- `redisbikeco:store:{storecode}`

To also create a RediSearch index:
```bash
python load_data.py --create-index
```

### Query Data

```python
import polars_redis as redis

# Define schema
bike_schema = {
    "stockcode": pl.Utf8,
    "model": pl.Utf8,
    "brand": pl.Utf8,
    "price": pl.Int64,
    "type": pl.Utf8,
    "description": pl.Utf8,
    "material": pl.Utf8,
    "weight": pl.Float64,
}

# Scan all bikes
bikes = redis.scan_json(
    "redis://localhost:6379",
    pattern="redisbikeco:bike:*",
    schema=bike_schema,
).collect()

print(f"Total bikes: {len(bikes)}")
print(bikes.head())
```

### Search with Query Builder

```python
from polars_redis.query import col

# Find eBikes under a price threshold
ebikes = redis.search_json(
    "redis://localhost:6379",
    index="bikes_idx",
    query=(col("type") == "eBikes") & (col("price") < 200000),
    schema=bike_schema,
).collect()

print(ebikes)
```

## Files

| File | Description |
|------|-------------|
| `load_data.py` | Script to load JSON data into Redis |
| `redis_bike_co.ipynb` | Jupyter notebook with detailed examples |
| `data/bikes.json` | Bike product catalog (111 items) |
| `data/stores.json` | Store locations (5 stores) |

## Features Demonstrated

- **Writing data**: `write_json()` to store DataFrames as Redis JSON documents
- **Schema inference**: `infer_json_schema()` and `infer_json_schema_with_confidence()`
- **Lazy scanning**: `scan_json()` with batched fetching
- **RediSearch queries**: `search_json()` with the query builder
- **Aggregations**: `aggregate_json()` for server-side computations
- **Polars operations**: Filtering, grouping, sorting, and analytics

## Data Source

The bike and store data is from the [redis-developer/redis-bike-co](https://github.com/redis-developer/redis-bike-co) repository, used for Redis learning materials.
