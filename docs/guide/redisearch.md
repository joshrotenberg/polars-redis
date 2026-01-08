# RediSearch Overview

## Why RediSearch?

When you have thousands or millions of documents in Redis, scanning everything and filtering in Python is slow and wasteful. RediSearch changes this by filtering and aggregating **inside Redis** - only the data you need crosses the network.

With polars-redis + RediSearch, you can:

- **Filter server-side**: Query `@age > 30 AND @status = "active"` returns only matching documents
- **Aggregate server-side**: Get `COUNT`, `AVG`, `SUM` by group without transferring raw data
- **Search text**: Full-text search with stemming, fuzzy matching, and phrase queries
- **Query geospatially**: Find documents within a radius or polygon
- **Search vectors**: K-nearest neighbors for semantic similarity

The result? **90%+ reduction in data transfer** for selective queries.

## When to Use RediSearch

RediSearch shines when:

- You have **10K+ documents** and selective queries (filtering returns <50% of data)
- You need **server-side aggregation** (GROUP BY, COUNT, AVG, SUM)
- You want **full-text search** with stemming, fuzzy matching, or phrase queries
- You need **geo queries** (find within radius/polygon)
- You're doing **vector similarity search**

## When NOT to Use RediSearch

Skip RediSearch and use `scan_hashes()` + Polars filtering when:

- **Small datasets** (<10K documents) - scanning is fast enough
- **One-off exploratory queries** - creating an index isn't worth it
- **You need all or most of the data** - no filtering benefit
- **Complex transforms** that Polars handles better than RediSearch

```python
# For small datasets, this is often simpler and fast enough
df = scan_hashes(url, "user:*", schema).filter(
    pl.col("age") > 30
).collect()
```

## Setup: Test Data

To follow along with examples in this guide, set up sample data:

```python
import polars as pl
import polars_redis as pr
from polars_redis import Index, TextField, NumericField, TagField

url = "redis://localhost:6379"

# Create sample employees
df = pl.DataFrame({
    "id": [1, 2, 3, 4, 5, 6, 7, 8],
    "name": ["Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Henry"],
    "age": [32, 28, 45, 35, 29, 52, 38, 41],
    "department": ["engineering", "engineering", "product", "product", 
                   "marketing", "engineering", "marketing", "engineering"],
    "salary": [120000, 95000, 140000, 110000, 85000, 150000, 95000, 130000],
    "status": ["active", "active", "active", "inactive", "active", 
               "active", "active", "inactive"],
})

# Write to Redis
pr.write_hashes(df, url, key_column="id", key_prefix="employee:")

# Create index
idx = Index(
    name="employees_idx",
    prefix="employee:",
    schema=[
        TextField("name", sortable=True),
        NumericField("age", sortable=True),
        TagField("department"),
        NumericField("salary", sortable=True),
        TagField("status"),
    ]
)
idx.ensure_exists(url)

print("Created 8 employees and index")
```

## Prerequisites

RediSearch requires Redis Stack or Redis with the RediSearch module:

```bash
# Start Redis Stack (includes RediSearch)
docker run -d -p 6379:6379 redis/redis-stack:latest

# Verify RediSearch is available
redis-cli MODULE LIST
# Should include "search"
```

## Two Ways to Query

You can write queries using **native RediSearch syntax** or the **polars-redis query builder**:

=== "Query Builder (Recommended)"

    ```python
    from polars_redis import col, search_hashes
    
    # Polars-like syntax you already know
    query = (col("age") > 30) & (col("status") == "active")
    
    df = search_hashes(
        url,
        index="employees_idx",
        query=query,
        schema={"name": pl.Utf8, "age": pl.Int64},
    ).collect()
    ```

=== "Raw RediSearch Syntax"

    ```python
    # Native RediSearch query string
    df = search_hashes(
        url,
        index="employees_idx",
        query="@age:[(30 +inf] @status:{active}",
        schema={"name": pl.Utf8, "age": pl.Int64},
    ).collect()
    ```

The query builder generates valid RediSearch queries while giving you a familiar, composable API. Use `query.to_redis()` anytime to see the generated query string.

## Basic Search

`search_hashes()` uses RediSearch FT.SEARCH to filter data server-side:

```python
import polars as pl
import polars_redis as pr
from polars_redis import col

url = "redis://localhost:6379"

# Find active employees over 30
df = pr.search_hashes(
    url,
    index="employees_idx",
    query=(col("age") > 30) & (col("status") == "active"),
    schema={"name": pl.Utf8, "age": pl.Int64, "department": pl.Utf8},
).collect()

print(df)
# shape: (4, 3)
# +-------+-----+-------------+
# | name  | age | department  |
# | ---   | --- | ---         |
# | str   | i64 | str         |
# +=======+=====+=============+
# | Alice | 32  | engineering |
# | Carol | 45  | product     |
# | Frank | 52  | engineering |
# | Grace | 38  | marketing   |
# +-------+-----+-------------+
```

## Basic Aggregation

`aggregate_hashes()` uses FT.AGGREGATE for server-side grouping:

```python
# Salary statistics by department
df = pr.aggregate_hashes(
    url,
    index="employees_idx",
    query="@status:{active}",
    group_by=["@department"],
    reduce=[
        ("COUNT", [], "headcount"),
        ("AVG", ["@salary"], "avg_salary"),
        ("SUM", ["@salary"], "total_payroll"),
    ],
    sort_by=[("@avg_salary", False)],  # Descending
)

print(df)
# shape: (3, 4)
# +-------------+-----------+------------+--------------+
# | department  | headcount | avg_salary | total_payroll|
# | ---         | ---       | ---        | ---          |
# | str         | i64       | f64        | f64          |
# +=============+===========+============+==============+
# | engineering | 3         | 121666.67  | 365000.0     |
# | product     | 1         | 140000.0   | 140000.0     |
# | marketing   | 2         | 90000.0    | 180000.0     |
# +-------------+-----------+------------+--------------+
```

Only the aggregated results transfer - not the raw documents.

## Smart Scan: Automatic Optimization

Don't want to think about indexes? Use `smart_scan()`:

```python
from polars_redis import smart_scan

# Auto-detect index and use FT.SEARCH if available
df = smart_scan(
    url,
    "employee:*",
    schema={"name": pl.Utf8, "age": pl.Int64},
).filter(pl.col("age") > 30).collect()
```

If an index exists for the pattern, `smart_scan` uses FT.SEARCH automatically. Otherwise, it falls back to SCAN - same API, optimal execution.

## Debugging Queries

See what RediSearch query gets generated:

```python
query = (col("department") == "engineering") & (col("salary") > 100000)
print(query.to_redis())
# @department:{engineering} @salary:[(100000 +inf]
```

See how a query will execute:

```python
from polars_redis import explain_scan

plan = explain_scan(url, "employee:*", schema={"name": pl.Utf8})
print(plan.explain())
# Strategy: SEARCH
# Index: employees_idx
#   Prefixes: employee:
#   Type: HASH
```

## Next Steps

- **[Query Builder](redisearch-queries.md)** - Deep dive into all query operations
- **[Aggregation](redisearch-aggregation.md)** - FT.AGGREGATE details and examples
- **[Index Management](index-management.md)** - Creating and managing indexes
- **[Advanced Queries](../examples/advanced-queries.md)** - Real-world query patterns

## Performance Comparison

| Method | Data Transfer | Use Case |
|--------|---------------|----------|
| `scan_hashes()` | All matching keys | No index, small datasets |
| `scan_hashes()` + filter | All keys, filter client-side | No index available |
| `search_hashes()` | Only matching docs | Indexed data, selective queries |
| `smart_scan()` | Optimized automatically | Best default - auto-selects strategy |
| `aggregate_hashes()` | Only aggregated results | Analytics, reporting |
