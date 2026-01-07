# Building a Product Catalog with Search

This end-to-end example shows how to build a searchable product catalog using polars-redis. We'll cover:

1. Loading product data into Redis
2. Creating a RediSearch index
3. Querying products with filters
4. Aggregating for analytics
5. Updating inventory in bulk

## Setup

Start Redis Stack (includes RediSearch):

```bash
docker run -d -p 6379:6379 redis/redis-stack:latest
```

Install polars-redis:

```bash
pip install polars-redis
```

## Step 1: Load Product Data

Let's say you have product data in a CSV or from an API:

```python
import polars as pl
import polars_redis as redis

url = "redis://localhost:6379"

# Sample product data
products = pl.DataFrame({
    "sku": ["LAPTOP-001", "LAPTOP-002", "PHONE-001", "PHONE-002", "TABLET-001"],
    "name": ["Pro Laptop 15", "Budget Laptop 14", "Flagship Phone", "Mid-range Phone", "Pro Tablet 12"],
    "category": ["laptops", "laptops", "phones", "phones", "tablets"],
    "brand": ["TechCorp", "ValueBrand", "TechCorp", "ValueBrand", "TechCorp"],
    "price": [1299.99, 599.99, 999.99, 449.99, 799.99],
    "quantity": [50, 120, 200, 350, 75],
    "rating": [4.5, 4.0, 4.7, 4.2, 4.6],
})

# Write to Redis as hashes
result = redis.write_hashes(
    products,
    url,
    key_column="sku",
    key_prefix="product:",
)
print(f"Loaded {result} products")
```

Each product is now a Redis hash:
```
product:LAPTOP-001 -> {name: "Pro Laptop 15", category: "laptops", ...}
product:LAPTOP-002 -> {name: "Budget Laptop 14", category: "laptops", ...}
...
```

## Step 2: Create a Search Index

Create a RediSearch index for fast querying:

```python
from polars_redis import Index, TextField, NumericField, TagField

# Define the index schema
product_index = Index(
    name="products_idx",
    prefix="product:",
    schema=[
        TextField("name", sortable=True),
        TagField("category"),
        TagField("brand"),
        NumericField("price", sortable=True),
        NumericField("quantity", sortable=True),
        NumericField("rating", sortable=True),
    ]
)

# Create the index
product_index.create(url)
print("Index created")
```

## Step 3: Query Products

### Basic Scan (No Index Required)

```python
# Scan all products
all_products = redis.scan_hashes(
    url,
    "product:*",
    schema={
        "name": pl.Utf8,
        "category": pl.Utf8,
        "price": pl.Float64,
        "quantity": pl.Int64,
    }
).collect()

print(all_products)
```

### Search with Filters (Uses Index)

```python
from polars_redis import col

# Find laptops under $1000
affordable_laptops = redis.search_hashes(
    url,
    index="products_idx",
    query=(col("category") == "laptops") & (col("price") < 1000),
    schema={
        "name": pl.Utf8,
        "price": pl.Float64,
        "quantity": pl.Int64,
    }
).collect()

print(affordable_laptops)
# shape: (1, 4)
# +---------+------------------+--------+----------+
# | _key    | name             | price  | quantity |
# +---------+------------------+--------+----------+
# | product | Budget Laptop 14 | 599.99 | 120      |
# +---------+------------------+--------+----------+
```

### Text Search

```python
# Search by name
results = redis.search_hashes(
    url,
    index="products_idx",
    query=col("name").contains("Pro"),
    schema={"name": pl.Utf8, "price": pl.Float64},
).collect()

print(results)
# Matches: "Pro Laptop 15", "Pro Tablet 12"
```

### Complex Queries

```python
# High-rated TechCorp products with good stock
query = (
    (col("brand") == "TechCorp") &
    (col("rating") >= 4.5) &
    (col("quantity") > 50)
)

premium_products = redis.search_hashes(
    url,
    index="products_idx",
    query=query,
    schema={
        "name": pl.Utf8,
        "category": pl.Utf8,
        "price": pl.Float64,
        "rating": pl.Float64,
    },
    sort_by="price",
    sort_ascending=False,
).collect()

print(premium_products)
```

## Step 4: Analytics with Aggregation

### Category Statistics

```python
# Get stats by category
category_stats = redis.aggregate_hashes(
    url,
    index="products_idx",
    query="*",
    group_by=["@category"],
    reduce=[
        ("COUNT", [], "product_count"),
        ("AVG", ["@price"], "avg_price"),
        ("SUM", ["@quantity"], "total_inventory"),
        ("AVG", ["@rating"], "avg_rating"),
    ],
    sort_by=[("@avg_price", False)],
)

print(category_stats)
# shape: (3, 5)
# +----------+---------------+-----------+-----------------+------------+
# | category | product_count | avg_price | total_inventory | avg_rating |
# +----------+---------------+-----------+-----------------+------------+
# | laptops  | 2             | 949.99    | 170             | 4.25       |
# | phones   | 2             | 724.99    | 550             | 4.45       |
# | tablets  | 1             | 799.99    | 75              | 4.6        |
# +----------+---------------+-----------+-----------------+------------+
```

### Brand Performance

```python
# Revenue potential by brand (price * quantity)
brand_stats = redis.aggregate_hashes(
    url,
    index="products_idx",
    query="*",
    group_by=["@brand"],
    reduce=[
        ("COUNT", [], "products"),
        ("SUM", ["@quantity"], "total_units"),
    ],
    apply=[
        # Note: For complex calculations, you might need to do this in Polars
    ],
)

# For more complex analytics, combine with Polars
products_df = redis.scan_hashes(
    url, "product:*",
    schema={
        "brand": pl.Utf8,
        "price": pl.Float64,
        "quantity": pl.Int64,
    }
).collect()

brand_revenue = (
    products_df
    .with_columns((pl.col("price") * pl.col("quantity")).alias("potential_revenue"))
    .group_by("brand")
    .agg([
        pl.len().alias("products"),
        pl.col("potential_revenue").sum().alias("total_potential_revenue"),
    ])
    .sort("total_potential_revenue", descending=True)
)

print(brand_revenue)
```

## Step 5: Bulk Inventory Updates

When inventory changes, update Redis efficiently:

```python
# Simulate inventory update from warehouse system
inventory_updates = pl.DataFrame({
    "sku": ["LAPTOP-001", "PHONE-001", "TABLET-001"],
    "quantity": [45, 180, 100],  # New quantities
})

# Write updates (replace mode overwrites existing fields)
redis.write_hashes(
    inventory_updates,
    url,
    key_column="sku",
    key_prefix="product:",
    write_mode="replace",
)

print("Inventory updated")

# Verify
updated = redis.search_hashes(
    url,
    index="products_idx",
    query=col("sku").has_any_tag(["LAPTOP-001", "PHONE-001", "TABLET-001"]),
    schema={"name": pl.Utf8, "quantity": pl.Int64},
).collect()

print(updated)
```

## Step 6: Low Stock Alerts

Find products that need reordering:

```python
# Products with less than 100 units
low_stock = redis.search_hashes(
    url,
    index="products_idx",
    query=col("quantity") < 100,
    schema={
        "name": pl.Utf8,
        "category": pl.Utf8,
        "quantity": pl.Int64,
    },
    sort_by="quantity",
    sort_ascending=True,
).collect()

print("Low stock alerts:")
print(low_stock)

# Write alerts to a separate key for monitoring
if len(low_stock) > 0:
    redis.write_hashes(
        low_stock.with_columns(pl.lit("low_stock").alias("alert_type")),
        url,
        key_prefix="alert:stock:",
        ttl=3600,  # Expire after 1 hour
    )
```

## Step 7: Smart Scan (Auto Index Detection)

If you're not sure whether an index exists, use `smart_scan`:

```python
from polars_redis import smart_scan, explain_scan

# Check execution plan
plan = explain_scan(url, "product:*", schema={"name": pl.Utf8})
print(plan.explain())
# Strategy: SEARCH
# Index: products_idx
#   Prefixes: product:
#   Type: HASH

# Smart scan auto-uses the index
df = smart_scan(
    url,
    "product:*",
    schema={"name": pl.Utf8, "price": pl.Float64},
).filter(pl.col("price") > 500).collect()
```

## Step 8: Caching Expensive Queries

Cache aggregation results for dashboards:

```python
@redis.cache(url=url, ttl=300, compression="zstd")
def get_category_dashboard():
    """Expensive aggregation cached for 5 minutes."""
    return redis.aggregate_hashes(
        url,
        index="products_idx",
        query="*",
        group_by=["@category"],
        reduce=[
            ("COUNT", [], "products"),
            ("AVG", ["@price"], "avg_price"),
            ("SUM", ["@quantity"], "inventory"),
        ],
    )

# First call: computes and caches
dashboard = get_category_dashboard()

# Subsequent calls: instant cache hit
dashboard = get_category_dashboard()
```

## Complete Script

Here's the full example in one script:

```python
import polars as pl
import polars_redis as redis
from polars_redis import col, Index, TextField, NumericField, TagField

url = "redis://localhost:6379"

# 1. Load data
products = pl.DataFrame({
    "sku": ["LAPTOP-001", "LAPTOP-002", "PHONE-001", "PHONE-002", "TABLET-001"],
    "name": ["Pro Laptop 15", "Budget Laptop 14", "Flagship Phone", "Mid-range Phone", "Pro Tablet 12"],
    "category": ["laptops", "laptops", "phones", "phones", "tablets"],
    "brand": ["TechCorp", "ValueBrand", "TechCorp", "ValueBrand", "TechCorp"],
    "price": [1299.99, 599.99, 999.99, 449.99, 799.99],
    "quantity": [50, 120, 200, 350, 75],
    "rating": [4.5, 4.0, 4.7, 4.2, 4.6],
})

redis.write_hashes(products, url, key_column="sku", key_prefix="product:")

# 2. Create index
Index(
    name="products_idx",
    prefix="product:",
    schema=[
        TextField("name", sortable=True),
        TagField("category"),
        TagField("brand"),
        NumericField("price", sortable=True),
        NumericField("quantity", sortable=True),
        NumericField("rating", sortable=True),
    ]
).ensure_exists(url)

# 3. Query
affordable = redis.search_hashes(
    url,
    index="products_idx",
    query=(col("price") < 800) & (col("rating") >= 4.0),
    schema={"name": pl.Utf8, "price": pl.Float64, "rating": pl.Float64},
).collect()

print("Affordable highly-rated products:")
print(affordable)

# 4. Aggregate
stats = redis.aggregate_hashes(
    url,
    index="products_idx",
    query="*",
    group_by=["@category"],
    reduce=[("COUNT", [], "count"), ("AVG", ["@price"], "avg_price")],
)

print("\nCategory stats:")
print(stats)
```

## Key Takeaways

1. **Write once, query many ways** - Load data with `write_hashes`, query with `scan_hashes` or `search_hashes`

2. **Index for performance** - Create RediSearch indexes for server-side filtering on large datasets

3. **Use the query builder** - Polars-like syntax is more readable than raw RediSearch queries

4. **Aggregate server-side** - Use `aggregate_hashes` for analytics to minimize data transfer

5. **Cache expensive queries** - The `@cache` decorator memoizes results automatically

6. **Smart scan for flexibility** - Auto-detects indexes so code works with or without them

## Next Steps

- Add geospatial search for store locations: [Client Operations](../guide/client-operations.md)
- Set up real-time inventory updates: [Pub/Sub](../guide/pubsub.md)
- Learn more about indexing: [Index Management](../guide/index-management.md)
