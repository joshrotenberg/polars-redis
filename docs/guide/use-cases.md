# Use Cases

polars-redis shines in scenarios where Redis holds data that needs analytical
processing, enrichment, or transformation. Here are common patterns and
examples.

## Customer Data Enrichment

Combine Redis session data with external sources for real-time customer
insights:

```python
import polars as pl
import polars_redis as redis

url = "redis://localhost:6379"

# Load active sessions from Redis
sessions = redis.scan_hashes(
    url,
    pattern="session:*",
    schema={"user_id": pl.Utf8, "started_at": pl.Datetime, "page_views": pl.Int64},
)

# Join with customer data from data warehouse
customers = pl.read_parquet("customers.parquet")

# Find high-engagement customers currently browsing
engaged = (
    sessions
    .join(customers, on="user_id")
    .filter(pl.col("page_views") > 10)
    .select(["user_id", "name", "email", "page_views"])
)

# Write to Redis for real-time targeting
redis.write_hashes(engaged.collect(), url, key_prefix="engaged:")
```

## Leaderboard Analytics

Analyze gaming or competition leaderboards stored in sorted sets:

```python
import polars as pl
import polars_redis as redis

url = "redis://localhost:6379"

# Load leaderboard scores
lf = redis.scan_zsets(
    url,
    pattern="leaderboard:weekly:*",
    include_rank=True,
)

# Find top performers across all leaderboards
top_players = (
    lf
    .group_by("member")
    .agg([
        pl.col("score").sum().alias("total_score"),
        pl.col("rank").mean().alias("avg_rank"),
        pl.len().alias("games_played"),
    ])
    .sort("total_score", descending=True)
    .head(100)
    .collect()
)

# Store aggregated rankings
redis.write_hashes(top_players, url, key_prefix="top100:")
```

## Event Stream Processing

Process Redis Streams for event analytics:

```python
import polars as pl
import polars_redis as redis
from datetime import datetime, timedelta

url = "redis://localhost:6379"

# Calculate timestamp for 24 hours ago
yesterday_ms = int((datetime.now() - timedelta(days=1)).timestamp() * 1000)

# Load recent events
events = redis.scan_streams(
    url,
    pattern="events:*",
    fields=["action", "user_id", "product_id"],
    start_id=f"{yesterday_ms}-0",
)

# Aggregate by action type
action_counts = (
    events
    .group_by(["action", pl.col("_ts").dt.hour().alias("hour")])
    .agg(pl.len().alias("count"))
    .sort(["hour", "count"], descending=[False, True])
    .collect()
)
```

## Time Series Downsampling

Aggregate high-frequency sensor data for dashboards:

```python
import polars as pl
import polars_redis as redis

url = "redis://localhost:6379"

# Server-side aggregation: 5-minute averages
aggregated = redis.scan_timeseries(
    url,
    pattern="sensor:temperature:*",
    aggregation="avg",
    bucket_size_ms=300000,  # 5 minutes
    label_columns=["location", "device_id"],
)

# Further analyze with Polars
hourly_stats = (
    aggregated
    .with_columns(pl.col("_ts").dt.truncate("1h").alias("hour"))
    .group_by(["location", "hour"])
    .agg([
        pl.col("value").mean().alias("avg_temp"),
        pl.col("value").min().alias("min_temp"),
        pl.col("value").max().alias("max_temp"),
    ])
    .collect()
)
```

## Cache Warming

Pre-compute and cache frequently accessed aggregations:

```python
import polars as pl
import polars_redis as redis

url = "redis://localhost:6379"

# Load order data
orders = pl.read_parquet("orders.parquet")

# Compute daily summaries
daily_summaries = (
    orders
    .group_by(pl.col("created_at").dt.date().alias("date"))
    .agg([
        pl.col("total").sum().alias("revenue"),
        pl.len().alias("order_count"),
        pl.col("total").mean().alias("avg_order_value"),
    ])
)

# Cache in Redis with 1-hour TTL
redis.write_hashes(
    daily_summaries,
    url,
    key_column="date",
    key_prefix="summary:daily:",
    ttl=3600,
)
```

## Session Analysis

Analyze user sessions for behavior patterns:

```python
import polars as pl
import polars_redis as redis

url = "redis://localhost:6379"

# Load all session data
sessions = redis.scan_hashes(
    url,
    pattern="session:*",
    schema={
        "user_id": pl.Utf8,
        "started_at": pl.Datetime,
        "last_activity": pl.Datetime,
        "page_views": pl.Int64,
        "cart_value": pl.Float64,
    },
    include_ttl=True,
)

# Find sessions likely to convert
likely_buyers = (
    sessions
    .filter(
        (pl.col("cart_value") > 50) & 
        (pl.col("page_views") > 5) &
        (pl.col("_ttl") > 300)  # Still active
    )
    .select(["user_id", "cart_value", "page_views"])
    .collect()
)
```

## RediSearch-Powered Analytics

Use server-side filtering for efficient queries on indexed data:

```python
import polars as pl
import polars_redis as redis
from polars_redis import col

url = "redis://localhost:6379"

# Server-side filtered search
premium_users = redis.search_hashes(
    url,
    index="users_idx",
    query=(col("tier") == "premium") & (col("lifetime_value") > 1000),
    schema={
        "user_id": pl.Utf8,
        "name": pl.Utf8,
        "tier": pl.Utf8,
        "lifetime_value": pl.Float64,
    },
)

# Combine with external data for campaign targeting
campaign_data = pl.read_csv("campaign_responses.csv")

targets = (
    premium_users
    .join(campaign_data.lazy(), on="user_id", how="left")
    .filter(pl.col("last_response").is_null())  # Haven't responded yet
    .collect()
)

# Write campaign targets back to Redis
redis.write_hashes(targets, url, key_prefix="campaign:target:")
```

## Inventory Synchronization

Keep Redis cache in sync with inventory systems:

```python
import polars as pl
import polars_redis as redis

url = "redis://localhost:6379"

# Current inventory from database/warehouse
current_inventory = pl.read_parquet("inventory.parquet")

# Cached inventory in Redis
cached = redis.scan_hashes(
    url,
    pattern="product:*",
    schema={"sku": pl.Utf8, "quantity": pl.Int64, "last_updated": pl.Datetime},
)

# Find products that need updates
needs_update = (
    current_inventory.lazy()
    .join(cached, on="sku", how="left", suffix="_cached")
    .filter(
        pl.col("quantity") != pl.col("quantity_cached")
    )
    .select(["sku", "quantity", "price", "name"])
    .collect()
)

# Update Redis cache
redis.write_hashes(
    needs_update,
    url,
    key_column="sku",
    key_prefix="product:",
    write_mode="replace",
)
```

## Tag-Based Filtering

Analyze data using Redis sets for tag membership:

```python
import polars as pl
import polars_redis as redis

url = "redis://localhost:6379"

# Load tag memberships
product_tags = redis.scan_sets(
    url,
    pattern="tags:*",
)

# Extract tag name from key
tagged_products = (
    product_tags
    .with_columns(
        pl.col("_key").str.replace("tags:", "").alias("tag")
    )
    .group_by("member")
    .agg(pl.col("tag").alias("tags"))
)

# Load product details
products = redis.scan_hashes(
    url,
    pattern="product:*",
    schema={"name": pl.Utf8, "price": pl.Float64, "category": pl.Utf8},
)

# Join and analyze
product_analysis = (
    products
    .join(tagged_products, left_on="_key", right_on="member", how="left")
    .with_columns(pl.col("tags").list.len().alias("tag_count"))
    .sort("tag_count", descending=True)
    .collect()
)
```

## Performance Considerations

### Use RediSearch When Available

For indexed data, `search_hashes()` is significantly faster than scanning
with client-side filtering:

```python
# Slow: scan all, filter client-side
result = (
    redis.scan_hashes(url, "user:*", schema)
    .filter(pl.col("age") > 30)
    .collect()
)

# Fast: filter server-side with RediSearch
result = redis.search_hashes(
    url,
    index="users_idx",
    query=col("age") > 30,
    schema=schema,
).collect()
```

### Projection Pushdown

Only select the columns you need to minimize data transfer:

```python
# Transfers all fields
df = redis.scan_hashes(url, "user:*", large_schema).collect()

# Transfers only selected fields (uses HMGET)
df = (
    redis.scan_hashes(url, "user:*", large_schema)
    .select(["name", "email"])
    .collect()
)
```

### Batch Size Tuning

Adjust batch size based on your data and memory constraints:

```python
# Large batches for small documents
lf = redis.scan_hashes(url, "user:*", schema, batch_size=5000)

# Smaller batches for large documents
lf = redis.scan_json(url, "doc:*", schema, batch_size=100)
```

### Parallel Fetching

Use parallel workers for large datasets:

```python
# 4 parallel workers for faster fetching
lf = redis.scan_hashes(url, "user:*", schema, parallel=4)
```
