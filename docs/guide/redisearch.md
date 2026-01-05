# RediSearch Integration

polars-redis integrates with RediSearch to enable server-side filtering (predicate pushdown) and aggregation. Instead of scanning all keys and filtering in Python, RediSearch filters data in Redis - only matching documents are transferred.

## Prerequisites

RediSearch requires:

- Redis Stack or Redis with RediSearch module
- An existing index on your data

```bash
# Start Redis Stack
docker run -d -p 6379:6379 redis/redis-stack:latest

# Verify RediSearch is available
redis-cli MODULE LIST
# Should include "search"
```

## Creating an Index

Before using `search_hashes()` or `aggregate_hashes()`, create an index:

```bash
# Create index on hash keys with prefix "user:"
FT.CREATE users_idx ON HASH PREFIX 1 user: SCHEMA \
    name TEXT SORTABLE \
    age NUMERIC SORTABLE \
    department TAG \
    salary NUMERIC \
    status TAG
```

## Searching with search_hashes()

`search_hashes()` uses RediSearch FT.SEARCH to filter data server-side:

```python
import polars as pl
import polars_redis as redis

# Basic search with raw query string
df = redis.search_hashes(
    "redis://localhost:6379",
    index="users_idx",
    query="@age:[30 +inf]",  # RediSearch query syntax
    schema={"name": pl.Utf8, "age": pl.Int64, "department": pl.Utf8},
).collect()
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | str | required | Redis connection URL |
| `index` | str | required | RediSearch index name |
| `query` | str or Expr | `"*"` | Query string or expression |
| `schema` | dict | required | Field names to Polars dtypes |
| `include_key` | bool | `True` | Include Redis key as column |
| `key_column_name` | str | `"_key"` | Name of key column |
| `include_ttl` | bool | `False` | Include TTL as column |
| `ttl_column_name` | str | `"_ttl"` | Name of TTL column |
| `batch_size` | int | `1000` | Documents per batch |
| `sort_by` | str | `None` | Field to sort by |
| `sort_ascending` | bool | `True` | Sort direction |

## Query Builder

polars-redis provides a Polars-like query builder that generates RediSearch queries:

```python
from polars_redis import col

# Simple comparison
query = col("age") > 30
# Generates: @age:[(30 +inf]

# Equality
query = col("status") == "active"
# Generates: @status:{active}

# Combining conditions
query = (col("age") > 30) & (col("department") == "engineering")
# Generates: (@age:[(30 +inf]) (@department:{engineering})

# Or conditions
query = (col("status") == "active") | (col("status") == "pending")
# Generates: (@status:{active}) | (@status:{pending})

# Negation
query = col("status") != "inactive"
# Generates: -@status:{inactive}

# Use in search
df = redis.search_hashes(
    "redis://localhost:6379",
    index="users_idx",
    query=query,
    schema={"name": pl.Utf8, "age": pl.Int64, "status": pl.Utf8},
).collect()
```

### Supported Operations

| Operation | Example | RediSearch Output |
|-----------|---------|-------------------|
| Equal | `col("status") == "active"` | `@status:{active}` |
| Not Equal | `col("status") != "active"` | `-@status:{active}` |
| Greater Than | `col("age") > 30` | `@age:[(30 +inf]` |
| Greater or Equal | `col("age") >= 30` | `@age:[30 +inf]` |
| Less Than | `col("age") < 30` | `@age:[-inf (30]` |
| Less or Equal | `col("age") <= 30` | `@age:[-inf 30]` |
| And | `expr1 & expr2` | `(expr1) (expr2)` |
| Or | `expr1 \| expr2` | `(expr1) \| (expr2)` |
| Negate | `expr.negate()` | `-(...expr...)` |

### Raw Queries

For complex queries, use `raw()`:

```python
from polars_redis import raw

# Full-text search
query = raw("@name:alice")

# Prefix search
query = raw("@name:ali*")

# Combine raw with builder
query = raw("@name:alice") & (col("age") > 25)
```

## Aggregation with aggregate_hashes()

`aggregate_hashes()` uses FT.AGGREGATE for server-side grouping and aggregation:

```python
# Group by department and calculate statistics
df = redis.aggregate_hashes(
    "redis://localhost:6379",
    index="users_idx",
    query="*",
    group_by=["@department"],
    reduce=[
        ("COUNT", [], "employee_count"),
        ("AVG", ["@salary"], "avg_salary"),
        ("SUM", ["@salary"], "total_payroll"),
    ],
)
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | str | required | Redis connection URL |
| `index` | str | required | RediSearch index name |
| `query` | str | `"*"` | Filter query before aggregation |
| `group_by` | list | `None` | Fields to group by (with @ prefix) |
| `reduce` | list | `None` | Reduce operations (function, args, alias) |
| `apply` | list | `None` | Computed expressions (expression, alias) |
| `filter_expr` | str | `None` | Post-aggregation filter |
| `sort_by` | list | `None` | Sort by (field, ascending) tuples |
| `limit` | int | `None` | Maximum results |
| `offset` | int | `0` | Skip first N results |
| `load` | list | `None` | Fields to load before aggregation |

### Reduce Functions

| Function | Args | Description |
|----------|------|-------------|
| `COUNT` | `[]` | Count documents |
| `SUM` | `["@field"]` | Sum of values |
| `AVG` | `["@field"]` | Average of values |
| `MIN` | `["@field"]` | Minimum value |
| `MAX` | `["@field"]` | Maximum value |
| `FIRST_VALUE` | `["@field"]` | First value |
| `TOLIST` | `["@field"]` | Collect as list |
| `QUANTILE` | `["@field", "0.5"]` | Quantile (e.g., median) |
| `STDDEV` | `["@field"]` | Standard deviation |

### Computed Fields with APPLY

```python
df = redis.aggregate_hashes(
    "redis://localhost:6379",
    index="sales_idx",
    query="*",
    group_by=["@region"],
    reduce=[
        ("SUM", ["@revenue"], "total_revenue"),
        ("COUNT", [], "order_count"),
    ],
    apply=[
        ("@total_revenue / @order_count", "avg_order_value"),
    ],
)
```

### Post-Aggregation Filtering

```python
df = redis.aggregate_hashes(
    "redis://localhost:6379",
    index="users_idx",
    query="*",
    group_by=["@department"],
    reduce=[("COUNT", [], "count")],
    filter_expr="@count > 10",  # Only departments with >10 employees
)
```

### Sorting and Pagination

```python
df = redis.aggregate_hashes(
    "redis://localhost:6379",
    index="users_idx",
    query="*",
    group_by=["@department"],
    reduce=[("AVG", ["@salary"], "avg_salary")],
    sort_by=[("@avg_salary", False)],  # Descending
    limit=10,
    offset=0,
)
```

## Example: Analytics Dashboard

```python
import polars as pl
import polars_redis as redis
from polars_redis import col

url = "redis://localhost:6379"

# Filter active users in engineering
engineers = redis.search_hashes(
    url,
    index="users_idx",
    query=(col("department") == "engineering") & (col("status") == "active"),
    schema={"name": pl.Utf8, "age": pl.Int64, "salary": pl.Float64},
).collect()

# Salary statistics by department
salary_stats = redis.aggregate_hashes(
    url,
    index="users_idx",
    query="@status:{active}",
    group_by=["@department"],
    reduce=[
        ("COUNT", [], "headcount"),
        ("AVG", ["@salary"], "avg_salary"),
        ("MIN", ["@salary"], "min_salary"),
        ("MAX", ["@salary"], "max_salary"),
    ],
    sort_by=[("@avg_salary", False)],
)

print(salary_stats)
```

## Performance Comparison

| Method | Data Transfer | Use Case |
|--------|---------------|----------|
| `scan_hashes()` | All matching keys | No index, small datasets |
| `scan_hashes()` + filter | All keys, filter client-side | No index available |
| `search_hashes()` | Only matching docs | Indexed data, selective queries |
| `aggregate_hashes()` | Only aggregated results | Analytics, reporting |

For large datasets with selective queries, RediSearch can reduce data transfer by 90%+ compared to scanning.
