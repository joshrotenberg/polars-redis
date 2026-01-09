# Data Staging Patterns

Redis can serve as a fast, ephemeral staging area for data processing workflows. This guide covers practical patterns for ETL staging, data cleaning, and interactive exploration.

## ETL Staging

Use Redis as an intermediate store when processing data from multiple sources:

```python
import polars as pl
import polars_redis as pr

# Extract: Load from multiple sources
customers = pl.read_csv("customers.csv")
orders = pl.read_parquet("orders.parquet")
products = pl.read_json("products.json")

# Stage in Redis for fast access
pr.write_hashes(customers, "redis://localhost", key_column="customer_id", key_prefix="cust:")
pr.write_hashes(orders, "redis://localhost", key_column="order_id", key_prefix="order:")
pr.write_hashes(products, "redis://localhost", key_column="product_id", key_prefix="prod:")

# Transform: Query and join as needed
customers_lf = pr.scan_hashes("redis://localhost", "cust:*", schema={...})
orders_lf = pr.scan_hashes("redis://localhost", "order:*", schema={...})

enriched = orders_lf.join(customers_lf, on="customer_id").collect()

# Load: Write final results
enriched.write_parquet("warehouse/enriched_orders.parquet")
```

## Data Cleaning Workflow

Iteratively clean and validate data with fast feedback:

```python
import polars as pl
import polars_redis as pr

# Load messy data into Redis
df = pl.read_csv("user_data.csv")
pr.write_hashes(df, "redis://localhost", key_column="id", key_prefix="user:")

# Check schema confidence
schema = pr.infer_hash_schema_with_confidence("redis://localhost", "user:*")
print(f"Schema confidence: {schema.average_confidence:.1%}")

for field, conf in schema.low_confidence_fields():
    print(f"  Warning: {field} has {conf:.0%} confidence")

# Query and inspect issues
lf = pr.scan_hashes("redis://localhost", "user:*", schema=schema.schema)

# Find nulls
nulls = lf.filter(pl.col("email").is_null()).collect()
print(f"Found {len(nulls)} records with null email")

# Find duplicates
dupes = (
    lf.group_by("email")
    .agg(pl.len().alias("count"))
    .filter(pl.col("count") > 1)
    .collect()
)
print(f"Found {len(dupes)} duplicate emails")

# Clean and write back
cleaned = lf.filter(pl.col("email").is_not_null()).unique("email").collect()
pr.write_hashes(cleaned, "redis://localhost", key_column="id", key_prefix="user_clean:")
```

## Interactive Exploration

Load data, explore with queries, export results:

```python
import polars as pl
import polars_redis as pr

# Load raw data
raw_df = pl.read_parquet("events.parquet")
pr.write_hashes(raw_df, "redis://localhost", key_column="event_id", key_prefix="event:")

# Infer schema
schema = pr.infer_hash_schema("redis://localhost", "event:*")

# Explore with Polars
lf = pr.scan_hashes("redis://localhost", "event:*", schema=schema)

# Filter and aggregate
result = (
    lf.filter(pl.col("status") == "completed")
    .group_by("region")
    .agg(pl.col("amount").sum().alias("total"))
    .collect()
)

# Export
result.write_csv("regional_totals.csv")
```

## With RediSearch

For repeated queries, create an index for server-side filtering:

```python
from polars_redis import col, search_hashes

# Create index (run once)
# FT.CREATE events_idx ON HASH PREFIX 1 event: SCHEMA
#   event_type TAG user_id TAG value NUMERIC SORTABLE

# Query with predicates pushed to Redis
active_events = search_hashes(
    "redis://localhost",
    index="events_idx",
    query=(col("event_type") == "purchase") & (col("value") > 100),
    schema={"user_id": pl.Utf8, "event_type": pl.Utf8, "value": pl.Float64},
).collect()
```

## Auto-Expiring Data

Use TTL for automatic cleanup:

```python
# Data expires automatically after 1 hour
pr.write_hashes(
    df,
    "redis://localhost",
    key_column="id",
    key_prefix="temp:",
    ttl=3600,
)
```

## When to Use This Pattern

**Good for:**

- Prototyping before setting up a production database
- One-off data analysis tasks
- Development and testing environments
- Interactive exploration of large files

**Not ideal for:**

- Long-term data storage
- Data larger than available RAM
- Complex relational queries

## Cleanup

```bash
# Flush the database
redis-cli FLUSHDB

# Or stop the container
docker stop redis && docker rm redis
```
