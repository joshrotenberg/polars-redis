# Aggregation

`aggregate_hashes()` uses RediSearch's FT.AGGREGATE for server-side grouping and aggregation. Only aggregated results transfer - not raw documents.

!!! tip "Setup"
    Examples on this page use the test data from [RediSearch Overview](redisearch.md#setup-test-data).

## Basic Aggregation

```python
import polars_redis as pr

url = "redis://localhost:6379"

# Count employees by department
df = pr.aggregate_hashes(
    url,
    index="employees_idx",
    query="*",
    group_by=["@department"],
    reduce=[("COUNT", [], "employee_count")],
)

print(df)
# shape: (3, 2)
# +-------------+----------------+
# | department  | employee_count |
# | ---         | ---            |
# | str         | i64            |
# +=============+================+
# | engineering | 4              |
# | product     | 2              |
# | marketing   | 2              |
# +-------------+----------------+
```

## Multiple Reduce Functions

```python
# Salary statistics by department
df = pr.aggregate_hashes(
    url,
    index="employees_idx",
    query="@status:{active}",  # Filter before aggregating
    group_by=["@department"],
    reduce=[
        ("COUNT", [], "headcount"),
        ("AVG", ["@salary"], "avg_salary"),
        ("MIN", ["@salary"], "min_salary"),
        ("MAX", ["@salary"], "max_salary"),
        ("SUM", ["@salary"], "total_payroll"),
    ],
)
```

## Global Aggregation

Aggregate without grouping for overall statistics:

```python
# Company-wide stats
df = pr.aggregate_hashes(
    url,
    index="employees_idx",
    query="@status:{active}",
    reduce=[
        ("COUNT", [], "total_employees"),
        ("AVG", ["@salary"], "company_avg_salary"),
        ("AVG", ["@age"], "avg_age"),
    ],
)

print(df)
# shape: (1, 3)
# +-----------------+--------------------+---------+
# | total_employees | company_avg_salary | avg_age |
# | ---             | ---                | ---     |
# | i64             | f64                | f64     |
# +=================+====================+=========+
# | 6               | 114166.67          | 37.33   |
# +-----------------+--------------------+---------+
```

## Computed Fields with APPLY

Create derived fields using expressions:

```python
df = pr.aggregate_hashes(
    url,
    index="employees_idx",
    query="*",
    group_by=["@department"],
    reduce=[
        ("SUM", ["@salary"], "total_salary"),
        ("COUNT", [], "count"),
    ],
    apply=[
        ("@total_salary / @count", "calculated_avg"),
    ],
)
```

## Post-Aggregation Filtering

Filter results after aggregation (like SQL's HAVING):

```python
# Only departments with more than 2 employees
df = pr.aggregate_hashes(
    url,
    index="employees_idx",
    query="*",
    group_by=["@department"],
    reduce=[("COUNT", [], "count")],
    filter_expr="@count > 2",
)
```

## Sorting and Pagination

```python
# Top 3 departments by average salary
df = pr.aggregate_hashes(
    url,
    index="employees_idx",
    query="*",
    group_by=["@department"],
    reduce=[("AVG", ["@salary"], "avg_salary")],
    sort_by=[("@avg_salary", False)],  # False = descending
    limit=3,
    offset=0,
)
```

## Loading Additional Fields

Load specific fields before aggregation:

```python
df = pr.aggregate_hashes(
    url,
    index="employees_idx",
    query="*",
    load=["@name", "@salary"],  # Load these fields
    group_by=["@department"],
    reduce=[
        ("FIRST_VALUE", ["@name"], "sample_employee"),
        ("AVG", ["@salary"], "avg_salary"),
    ],
)
```

## Parameters Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | str | required | Redis connection URL |
| `index` | str | required | RediSearch index name |
| `query` | str | `"*"` | Filter query before aggregation |
| `group_by` | list | `None` | Fields to group by (with @ prefix) |
| `reduce` | list | `None` | Reduce operations as (function, args, alias) tuples |
| `apply` | list | `None` | Computed expressions as (expression, alias) tuples |
| `filter_expr` | str | `None` | Post-aggregation filter |
| `sort_by` | list | `None` | Sort by (field, ascending) tuples |
| `limit` | int | `None` | Maximum results |
| `offset` | int | `0` | Skip first N results |
| `load` | list | `None` | Fields to load before aggregation |

## Reduce Functions Reference

| Function | Args | Description |
|----------|------|-------------|
| `COUNT` | `[]` | Count documents in group |
| `SUM` | `["@field"]` | Sum of numeric field |
| `AVG` | `["@field"]` | Average of numeric field |
| `MIN` | `["@field"]` | Minimum value |
| `MAX` | `["@field"]` | Maximum value |
| `FIRST_VALUE` | `["@field"]` | First value in group |
| `TOLIST` | `["@field"]` | Collect all values as list |
| `QUANTILE` | `["@field", "0.5"]` | Quantile (0.5 = median) |
| `STDDEV` | `["@field"]` | Standard deviation |
| `COUNT_DISTINCT` | `["@field"]` | Count unique values |
| `COUNT_DISTINCTISH` | `["@field"]` | Approximate distinct count (faster) |

## Example: Analytics Dashboard

```python
import polars as pl
import polars_redis as pr
from polars_redis import col

url = "redis://localhost:6379"

# 1. Filter: active engineers
engineers = pr.search_hashes(
    url,
    index="employees_idx",
    query=(col("department") == "engineering") & (col("status") == "active"),
    schema={"name": pl.Utf8, "age": pl.Int64, "salary": pl.Float64},
).collect()

# 2. Aggregate: salary distribution by department
salary_stats = pr.aggregate_hashes(
    url,
    index="employees_idx",
    query="@status:{active}",
    group_by=["@department"],
    reduce=[
        ("COUNT", [], "headcount"),
        ("AVG", ["@salary"], "avg_salary"),
        ("STDDEV", ["@salary"], "salary_stddev"),
        ("MIN", ["@salary"], "min_salary"),
        ("MAX", ["@salary"], "max_salary"),
    ],
    sort_by=[("@headcount", False)],
)

# 3. Aggregate: age distribution
age_stats = pr.aggregate_hashes(
    url,
    index="employees_idx",
    query="*",
    reduce=[
        ("AVG", ["@age"], "avg_age"),
        ("MIN", ["@age"], "youngest"),
        ("MAX", ["@age"], "oldest"),
        ("QUANTILE", ["@age", "0.5"], "median_age"),
    ],
)

print("Engineers:", engineers)
print("Salary by dept:", salary_stats)
print("Age stats:", age_stats)
```
