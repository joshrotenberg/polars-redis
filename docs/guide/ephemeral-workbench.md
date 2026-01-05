# Ephemeral Data Workbench

Redis + polars-redis provides a compelling workflow for interactive data exploration
and transformation. Think of it as a fast, queryable staging area that you can spin
up instantly.

## Why Use Redis as a Data Workbench?

| Benefit | Description |
|---------|-------------|
| **Fast setup** | Just `docker run redis/redis-stack` - no database installation |
| **In-memory speed** | Sub-millisecond queries on indexed data |
| **Schema flexibility** | No upfront schema definition required |
| **Queryable** | RediSearch for server-side filtering and aggregation |
| **Ephemeral by default** | Data disappears when Redis stops (or use persistence) |
| **Python-friendly** | Natural Polars API for data manipulation |

## Quick Start

### 1. Start Redis

```bash
# Start Redis Stack (includes RediSearch and RedisJSON)
docker run -d --name redis-workbench -p 6379:6379 redis/redis-stack:latest
```

### 2. Load Your Data

```python
import polars as pl
import polars_redis as pr

# Load data from any source
df = pl.read_csv("sales_data.csv")
print(f"Loaded {len(df)} rows")

# Write to Redis as hashes
count = pr.write_hashes(
    df,
    "redis://localhost:6379",
    key_column="id",
    key_prefix="sale:"
)
print(f"Wrote {count} records to Redis")
```

### 3. Query Interactively

```python
# Infer schema from the data we just loaded
schema = pr.infer_hash_schema("redis://localhost:6379", "sale:*")
print(f"Inferred schema: {schema}")

# Scan and explore
lf = pr.scan_hashes(
    "redis://localhost:6379",
    "sale:*",
    schema=schema
)

# Preview
print(lf.head(10).collect())

# Filter and aggregate
result = (
    lf.filter(pl.col("status") == "completed")
    .group_by("region")
    .agg(pl.col("amount").sum().alias("total_sales"))
    .collect()
)
print(result)
```

## Workflow Examples

### Load, Explore, Filter, Export

A common pattern for one-off data analysis:

```python
import polars as pl
import polars_redis as pr

# Step 1: Load raw data into Redis
raw_df = pl.read_parquet("events.parquet")
pr.write_hashes(raw_df, "redis://localhost", key_column="event_id", key_prefix="event:")

# Step 2: Create a RediSearch index for fast queries
# Run this in redis-cli or via redis-py:
# FT.CREATE events_idx ON HASH PREFIX 1 event: SCHEMA
#   timestamp NUMERIC SORTABLE
#   user_id TAG
#   event_type TAG
#   value NUMERIC

# Step 3: Query with predicates pushed to Redis
from polars_redis import col, search_hashes

active_events = search_hashes(
    "redis://localhost",
    index="events_idx",
    query=(col("event_type") == "purchase") & (col("value") > 100),
    schema={"user_id": pl.Utf8, "event_type": pl.Utf8, "value": pl.Float64}
).collect()

# Step 4: Further analysis in Polars
top_users = (
    active_events
    .group_by("user_id")
    .agg(pl.col("value").sum().alias("total_spend"))
    .sort("total_spend", descending=True)
    .head(100)
)

# Step 5: Export results
top_users.write_csv("top_spenders.csv")
```

### ETL Staging Area

Use Redis as an intermediate store during ETL:

```python
import polars as pl
import polars_redis as pr

# Extract: Load from multiple sources
customers = pl.read_csv("customers.csv")
orders = pl.read_parquet("orders.parquet")
products = pl.read_json("products.json")

# Stage in Redis (fast access for multiple passes)
pr.write_hashes(customers, "redis://localhost", key_column="customer_id", key_prefix="cust:")
pr.write_hashes(orders, "redis://localhost", key_column="order_id", key_prefix="order:")
pr.write_hashes(products, "redis://localhost", key_column="product_id", key_prefix="prod:")

# Transform: Query and join as needed
# (Use scan_hashes for each, then join in Polars)

# Load: Write final results
final_df.write_parquet("warehouse/enriched_orders.parquet")
```

### Interactive Data Cleaning

Iteratively clean and validate data:

```python
import polars as pl
import polars_redis as pr

# Load messy data
df = pl.read_csv("user_data.csv")
pr.write_hashes(df, "redis://localhost", key_column="id", key_prefix="user:")

# Check for issues
schema = pr.infer_hash_schema_with_confidence("redis://localhost", "user:*")
print(f"Schema confidence: {schema.average_confidence:.1%}")

for field, conf in schema.low_confidence_fields():
    print(f"  Warning: {field} has {conf:.0%} confidence")

# Query and inspect problematic records
lf = pr.scan_hashes("redis://localhost", "user:*", schema=schema.schema)

# Find nulls
nulls = lf.filter(pl.col("email").is_null()).collect()
print(f"Found {len(nulls)} records with null email")

# Find duplicates
dupes = (
    lf.group_by("email")
    .agg(pl.count().alias("count"))
    .filter(pl.col("count") > 1)
    .collect()
)
print(f"Found {len(dupes)} duplicate emails")

# Clean and re-write
cleaned = lf.filter(pl.col("email").is_not_null()).unique("email").collect()
pr.write_hashes(cleaned, "redis://localhost", key_column="id", key_prefix="user_clean:")
```

### Session-Based Analytics

Analyze user sessions with fast lookups:

```python
import polars as pl
import polars_redis as pr

# Load session data
sessions = pl.read_parquet("sessions.parquet")
pr.write_hashes(
    sessions,
    "redis://localhost",
    key_column="session_id",
    key_prefix="sess:",
    ttl=3600  # Auto-expire after 1 hour
)

# Aggregate with RediSearch
stats = pr.aggregate_hashes(
    "redis://localhost",
    index="sessions_idx",
    query="*",
    group_by=["user_type"],
    reduce=[
        ("COUNT", [], "session_count"),
        ("AVG", ["@duration"], "avg_duration"),
        ("SUM", ["@page_views"], "total_views"),
    ]
)
print(stats)
```

## When to Use This Pattern

**Good for:**

- Prototyping before setting up a production database
- One-off data analysis tasks
- Development and testing environments
- Sharing datasets between processes
- Interactive exploration of large files
- Ad-hoc analytics that need to be fast

**Not ideal for:**

- Long-term data storage (use a proper database)
- Data larger than available RAM
- Mission-critical production workloads
- Complex relational queries (use PostgreSQL, etc.)

## Performance Tips

### Batch Size for Loading

```python
# For large datasets, write in batches
BATCH_SIZE = 10000
for i in range(0, len(df), BATCH_SIZE):
    batch = df.slice(i, BATCH_SIZE)
    pr.write_hashes(batch, url, key_column="id", key_prefix="row:")
```

### Create Indexes for Queries

If you'll query the same data multiple times, create a RediSearch index:

```bash
# In redis-cli
FT.CREATE mydata_idx ON HASH PREFIX 1 row: SCHEMA
  category TAG
  value NUMERIC SORTABLE
  name TEXT
```

Then use `search_hashes` instead of `scan_hashes` for filtered queries.

### Memory Considerations

```python
# Check Redis memory usage
import redis
r = redis.from_url("redis://localhost")
info = r.info("memory")
print(f"Used memory: {info['used_memory_human']}")
print(f"Peak memory: {info['used_memory_peak_human']}")
```

### Use TTL for Automatic Cleanup

```python
# Set TTL when writing (data expires automatically)
pr.write_hashes(
    df,
    "redis://localhost",
    key_column="id",
    key_prefix="temp:",
    ttl=3600  # Expires in 1 hour
)
```

Or set TTL in Redis directly:

```bash
# Expire all keys matching a pattern (requires a script or loop)
redis-cli KEYS "temp:*" | xargs -I {} redis-cli EXPIRE {} 3600
```

## Cleanup

When you're done:

```bash
# Stop and remove the container (data is deleted)
docker stop redis-workbench
docker rm redis-workbench

# Or just flush the database
redis-cli FLUSHDB
```
