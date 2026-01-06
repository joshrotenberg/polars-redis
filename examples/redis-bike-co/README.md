# Redis Bike Co Example

This example demonstrates how to use **polars-redis** with a realistic dataset from [Redis Bike Co](https://github.com/redis-developer/redis-bike-co), a fictional bicycle retail company.

## Why polars-redis?

The original Redis Bike Co example uses redis-py with manual JSON handling. This example shows how polars-redis simplifies the workflow:

| Task | Traditional (redis-py) | polars-redis |
|------|------------------------|--------------|
| Load 111 bikes | 112 Redis calls (scan + N gets) | Batched writes |
| Read all bikes | Loop + manual JSON parsing | `scan_json().collect()` |
| Filter by type | Client-side filtering | Server-side with query builder |
| Analytics | Manual aggregation | Native Polars operations |

## Dataset

- **111 bikes** with brand, model, price, type, material, weight, and description
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

3. **redis-py** (for creating search indexes):
   ```bash
   pip install redis
   ```

4. **numpy** (for vector search example):
   ```bash
   pip install numpy
   ```

## Quick Start

### Load Data

```bash
python load_data.py --create-index
```

This loads all bikes and stores into Redis and creates a RediSearch index.

### Run the Notebook

```bash
jupyter notebook redis_bike_co.ipynb
```

## What's Demonstrated

### 1. Schema Inference
Automatically detect field types with confidence scores:
```python
schema = redis.infer_json_schema_with_confidence(
    url, pattern="redisbikeco:bike:*", sample_size=20
)
# Returns: {field: (type, confidence), ...}
```

### 2. Pythonic Query Builder
Write queries in Python, compile to RediSearch syntax:
```python
from polars_redis.query import col

query = (
    (col("type") == "Mountain Bikes") &
    (col("price") < 200000) &
    (col("material") == "carbon")
)
# Compiles to: (@type:{Mountain Bikes}) (@price:[-inf 200000]) (@material:{carbon})
```

### 3. Vector Similarity Search
Find semantically similar items using KNN:
```python
knn_query = col("embedding").knn(k=5, vector_param="query_vec")
```

### 4. Real-World Analytics
Find underpriced inventory by comparing to category averages:
```python
# Join bikes with category stats, calculate z-scores
underpriced = (
    bikes.join(category_stats, on=["type", "material"])
    .with_columns(
        ((pl.col("price") - pl.col("avg_price")) / pl.col("std_price"))
        .alias("price_zscore")
    )
    .filter(pl.col("price_zscore") < -1)
)
```

### 5. Performance Comparison
Benchmark showing batched fetching vs individual calls:
```
Traditional approach: 45.2ms (112 Redis calls)
polars-redis:         12.1ms (batched)
```

## Files

| File | Description |
|------|-------------|
| `load_data.py` | CLI script to load data and create indexes |
| `redis_bike_co.ipynb` | Comprehensive Jupyter notebook |
| `data/bikes.json` | Bike catalog (111 items) |
| `data/stores.json` | Store locations (5 stores) |

## Data Source

Data from [redis-developer/redis-bike-co](https://github.com/redis-developer/redis-bike-co), used for Redis learning materials.
