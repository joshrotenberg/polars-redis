# RediSearch Integration

## Why RediSearch?

When you have thousands or millions of documents in Redis, scanning everything and filtering in Python is slow and wasteful. RediSearch changes this by filtering and aggregating **inside Redis** - only the data you need crosses the network.

With polars-redis + RediSearch, you can:

- **Filter server-side**: Query `@age > 30 AND @status = "active"` returns only matching documents
- **Aggregate server-side**: Get `COUNT`, `AVG`, `SUM` by group without transferring raw data
- **Search text**: Full-text search with stemming, fuzzy matching, and phrase queries
- **Query geospatially**: Find documents within a radius or polygon
- **Search vectors**: K-nearest neighbors for semantic similarity

The result? **90%+ reduction in data transfer** for selective queries.

## Two Ways to Query

You can write queries using **native RediSearch syntax** or the **polars-redis query builder**:

=== "Query Builder (Recommended)"

    ```python
    from polars_redis import col, search_hashes
    
    # Polars-like syntax you already know
    query = (col("age") > 30) & (col("status") == "active")
    
    df = search_hashes(
        "redis://localhost:6379",
        index="users_idx",
        query=query,
        schema={"name": pl.Utf8, "age": pl.Int64},
    ).collect()
    ```

=== "Raw RediSearch Syntax"

    ```python
    # Native RediSearch query string
    df = search_hashes(
        "redis://localhost:6379",
        index="users_idx",
        query="@age:[(30 +inf] @status:{active}",
        schema={"name": pl.Utf8, "age": pl.Int64},
    ).collect()
    ```

The query builder generates valid RediSearch queries while giving you a familiar, composable API. Use `query.to_redis()` anytime to see the generated query string.

## Prerequisites

RediSearch requires:

- Redis Stack or Redis with RediSearch module
- An existing index on your data (or use polars-redis to create one)

```bash
# Start Redis Stack
docker run -d -p 6379:6379 redis/redis-stack:latest

# Verify RediSearch is available
redis-cli MODULE LIST
# Should include "search"
```

## Creating an Index

Before using `search_hashes()` or `aggregate_hashes()`, you need an index. polars-redis provides typed helpers for creating indexes:

```python
from polars_redis import Index, TextField, NumericField, TagField

idx = Index(
    name="users_idx",
    prefix="user:",
    schema=[
        TextField("name", sortable=True),
        NumericField("age", sortable=True),
        TagField("department"),
        NumericField("salary"),
        TagField("status"),
    ]
)

# Create the index
idx.create("redis://localhost:6379")

# Or ensure it exists (idempotent)
idx.ensure_exists("redis://localhost:6379")
```

You can also pass the `Index` object directly to `search_hashes()` for auto-creation:

```python
df = search_hashes(
    "redis://localhost:6379",
    index=idx,  # Auto-creates if not exists
    query=col("age") > 30,
    schema={"name": pl.Utf8, "age": pl.Int64},
).collect()
```

!!! tip "Index Management"
    See [Index Management](index-management.md) for complete documentation on field types, schema inference, validation, and migration.

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

## Advanced Query Features

### Text Search

#### Full-text Search

```python
# Basic text search (with stemming)
query = col("title").contains("python")
# @title:python

# Prefix matching
query = col("name").starts_with("jo")
# @name:jo*

# Suffix matching
query = col("name").ends_with("son")
# @name:*son

# Substring/infix matching
query = col("description").contains_substring("data")
# @description:*data*
```

#### Fuzzy Matching

Match terms with typos using Levenshtein distance (1-3):

```python
# Allow 1 character difference
query = col("title").fuzzy("python", distance=1)
# @title:%python%

# Allow 2 character differences
query = col("title").fuzzy("algorithm", distance=2)
# @title:%%algorithm%%
```

#### Phrase Search

Search for phrases with optional slop and order control:

```python
# Exact phrase (words must appear consecutively)
query = col("title").phrase("hello", "world")
# @title:(hello world)

# Allow words between (slop = max intervening terms)
query = col("title").phrase("machine", "learning", slop=2)
# @title:(machine learning) => { $slop: 2; }

# Require in-order with slop
query = col("title").phrase("data", "science", slop=3, inorder=True)
# @title:(data science) => { $slop: 3; $inorder: true; }
```

#### Wildcard Matching

```python
# Simple wildcard
query = col("name").matches("j*n")
# @name:j*n

# Exact wildcard matching
query = col("code").matches_exact("FOO*BAR?")
# @code:"w'FOO*BAR?'"
```

### Multi-field Search

Search across multiple fields simultaneously:

```python
from polars_redis import cols

# Search title and body together
query = cols("title", "body").contains("python")
# @title|body:python

# Prefix search across fields
query = cols("first_name", "last_name").starts_with("john")
# @first_name|last_name:john*
```

### Tag Operations

```python
# Single tag match
query = col("category").has_tag("electronics")
# @category:{electronics}

# Match any of multiple tags
query = col("tags").has_any_tag(["urgent", "important"])
# @tags:{urgent|important}
```

### Geo Search

#### Radius Search

```python
# Find locations within 10km of a point
query = col("location").within_radius(-122.4194, 37.7749, 10, "km")
# @location:[-122.4194 37.7749 10 km]

# Supported units: m, km, mi, ft
query = col("location").within_radius(-73.9857, 40.7484, 5, "mi")
```

#### Polygon Search

Search within a polygon boundary:

```python
# Define polygon as list of (lon, lat) points
polygon = [
    (-122.5, 37.7),
    (-122.5, 37.8),
    (-122.3, 37.8),
    (-122.3, 37.7),
    (-122.5, 37.7),  # Close the polygon
]
query = col("location").within_polygon(polygon)
# @location:[WITHIN $poly]
```

!!! note
    Polygon queries require passing the polygon as a query parameter. The query builder generates a parameterized query.

### Vector Search

For semantic similarity search with vector embeddings:

#### K-Nearest Neighbors (KNN)

```python
# Find 10 most similar documents
query = col("embedding").knn(10, "query_vec")
# *=>[KNN 10 @embedding $query_vec]

# Use with search_hashes (pass vector as parameter)
df = redis.search_hashes(
    url,
    index="docs_idx",
    query=query,
    schema={"title": pl.Utf8, "embedding": pl.List(pl.Float32)},
    params={"query_vec": embedding_bytes},
)
```

#### Vector Range Search

Find all vectors within a distance threshold:

```python
# Find all documents within distance 0.5
query = col("embedding").vector_range(0.5, "query_vec")
# @embedding:[VECTOR_RANGE 0.5 $query_vec]
```

### Relevance Tuning

#### Boosting

Increase the relevance score contribution of specific terms:

```python
# Boost title matches 2x
query = col("title").contains("python").boost(2.0)
# (@title:python) => { $weight: 2.0; }

# Combine with other terms
title_query = col("title").contains("python").boost(2.0)
body_query = col("body").contains("python")
query = title_query | body_query
```

#### Optional Terms

Mark terms as optional for better ranking without requiring them:

```python
# Documents with "python" required, "tutorial" optional but preferred
required = col("title").contains("python")
optional = col("title").contains("tutorial").optional()
query = required & optional
# @title:python ~@title:tutorial
```

### Null Checks

```python
# Find documents missing a field
query = col("email").is_null()
# ismissing(@email)

# Find documents with a field present
query = col("email").is_not_null()
# -ismissing(@email)
```

### Debugging Queries

Use `to_redis()` to inspect the generated RediSearch query:

```python
query = (col("type") == "eBikes") & (col("price") < 1000)
print(query.to_redis())
# @type:{eBikes} @price:[-inf (1000]
```

!!! tip "More Examples"
    See [Advanced Query Examples](../examples/advanced-queries.md) for real-world use cases including vector search, geo filtering, fuzzy matching, and dynamic query building.

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

## Advanced Search Options

For fine-grained control over search behavior, use `SearchOptions`:

```python
from polars_redis.options import SearchOptions, HighlightOptions, SummarizeOptions

# Create options with advanced settings
opts = SearchOptions(
    index="articles_idx",
    query="python programming",
    verbatim=True,           # Disable stemming
    language="english",       # Stemming language
    scorer="BM25",           # Scoring algorithm
    dialect=4,               # RediSearch dialect
).with_highlight(
    open_tag="<mark>",
    close_tag="</mark>",
).with_summarize(
    len=50,
    separator="...",
)

df = redis.search_hashes(
    "redis://localhost:6379",
    options=opts,
    schema={"title": pl.Utf8, "body": pl.Utf8},
).collect()
```

### Query Modifiers

| Option | Description |
|--------|-------------|
| `verbatim` | Disable stemming for exact term matching |
| `no_stopwords` | Include stop words in the query |
| `language` | Language for stemming (e.g., "english", "spanish") |
| `scorer` | Scoring function: "BM25", "TFIDF", "DISMAX" |
| `expander` | Query expander: "SYNONYM" |
| `slop` | Default slop for phrase queries |
| `in_order` | Require phrase terms in order |
| `dialect` | RediSearch dialect version (1-4) |

### Filtering Options

| Option | Description |
|--------|-------------|
| `in_keys` | Limit search to specific document keys |
| `in_fields` | Limit search to specific fields |
| `timeout_ms` | Query timeout in milliseconds |

### Result Enrichment

#### Highlighting

Wrap matching terms in custom tags:

```python
opts = SearchOptions(
    index="articles_idx",
    query="python",
).with_highlight(
    fields=["title", "body"],
    open_tag="<em>",
    close_tag="</em>",
)
```

#### Summarization

Generate text snippets with matched terms:

```python
opts = SearchOptions(
    index="articles_idx",
    query="machine learning",
).with_summarize(
    fields=["body"],
    frags=3,      # Number of fragments
    len=30,       # Fragment length in words
    separator="...",
)
```

### Relevance Scores

Include relevance scores in results:

```python
opts = SearchOptions(
    index="articles_idx",
    query="python",
).with_score(True, "_relevance")

df = redis.search_hashes(
    "redis://localhost:6379",
    options=opts,
    schema={"title": pl.Utf8},
).collect()

# Results include _relevance column with scores
```

## Enhanced Client-Side Operations

Some operations can't be pushed to RediSearch but execute efficiently in Polars after fetching results. These enable powerful filtering beyond RediSearch's capabilities.

### Regex Matching

```python
# Match email patterns (runs in Polars)
query = col("email").matches_regex(r".*@gmail\.com$")

# Combine with server-side filter
query = (col("status") == "active") & col("email").matches_regex(r".*@company\.com")
```

### Case-Insensitive Matching

```python
# Case-insensitive contains
query = col("name").icontains("john")

# Case-insensitive equality
query = col("department").iequals("ENGINEERING")
```

### Multiple Substring Matching

```python
# Match any of multiple substrings
query = col("description").contains_any(["python", "rust", "go"])
```

### String Similarity

Match strings using Levenshtein distance:

```python
# Find names similar to "john" (80% similarity threshold)
query = col("name").similar_to("john", threshold=0.8)
```

### Date/Time Operations

Parse and filter by date fields:

```python
# Date comparisons
query = col("created_at").as_date() > "2024-01-01"
query = col("updated_at").as_datetime() >= "2024-06-15T12:00:00"

# Date part extraction
query = col("birth_date").as_date().year() == 1990
query = col("created_at").as_date().month().is_in([1, 2, 3])  # Q1
query = col("event_time").as_datetime().hour() >= 9

# Available date parts: year(), month(), day(), weekday(), hour(), minute()
```

### Array/JSON Operations

For JSON documents:

```python
# Check if array contains value
query = col("tags").array_contains("python")

# Filter by array length
query = col("items").array_len() > 5

# Extract nested JSON value
query = col("metadata").json_path("$.user.role") == "admin"
```

## Hybrid Query Execution

When queries combine server-side and client-side operations, polars-redis automatically splits execution:

```python
# This query has both server-pushable and client-side parts
query = (col("age") > 30) & col("email").matches_regex(r".*@gmail\.com")

# Check what runs where
print(query.is_client_side)  # True

# Extract server portion
server_filter = query.get_server_filter()
print(server_filter.to_redis())  # @age:[(30 +inf]

# Extract client portion
client_filter = query.get_client_filter()
print(client_filter._op)  # "regex"

# View execution plan
print(query.explain())
# RediSearch: @age:[(30 +inf]
# Polars filter: pl.col("email").str.contains(r".*@gmail\.com")
```

### Execution Strategy

1. **Pure server-side**: All predicates pushed to RediSearch
2. **Pure client-side**: Fetch all, filter in Polars
3. **Hybrid**: Push what we can, filter rest in Polars

The query builder automatically optimizes for minimum data transfer.

## Smart Scan: Automatic Index Detection

The `smart_scan()` function provides a higher-level API that automatically detects whether a RediSearch index exists for your key pattern and optimizes query execution accordingly. This enables "RediSearch without learning RediSearch" - you get the performance benefits of FT.SEARCH when indexes are available, with automatic fallback to SCAN when they're not.

### Basic Usage

```python
from polars_redis import smart_scan

# Auto-detect index and use FT.SEARCH if available
df = smart_scan(
    "redis://localhost:6379",
    "user:*",
    schema={"name": pl.Utf8, "age": pl.Int64, "status": pl.Utf8},
).filter(pl.col("age") > 30).collect()
```

If an index exists with prefix "user:", `smart_scan` uses FT.SEARCH automatically. Otherwise, it falls back to SCAN - same API, optimal execution.

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | str | required | Redis connection URL |
| `pattern` | str | `"*"` | Key pattern to match |
| `schema` | dict | required | Field names to Polars dtypes |
| `index` | str or Index | `None` | Force use of specific index |
| `include_key` | bool | `True` | Include Redis key as column |
| `key_column_name` | str | `"_key"` | Name of key column |
| `include_ttl` | bool | `False` | Include TTL as column |
| `ttl_column_name` | str | `"_ttl"` | Name of TTL column |
| `batch_size` | int | `1000` | Documents per batch |
| `auto_detect_index` | bool | `True` | Auto-detect matching indexes |

### Execution Strategies

`smart_scan` chooses from three strategies:

| Strategy | When Used | Description |
|----------|-----------|-------------|
| **SEARCH** | Index found for pattern | Uses FT.SEARCH for server-side filtering |
| **SCAN** | No matching index | Falls back to Redis SCAN |
| **HYBRID** | Partial predicate pushdown | FT.SEARCH + client-side filtering |

### Query Explanation

Use `explain_scan()` to see exactly how a query will execute before running it:

```python
from polars_redis import explain_scan

plan = explain_scan(
    "redis://localhost:6379",
    "user:*",
    schema={"name": pl.Utf8, "age": pl.Int64},
)

print(plan.explain())
# Strategy: SEARCH
# Index: users_idx
#   Prefixes: user:
#   Type: HASH
# Server Query: *
```

The `QueryPlan` object provides:

- `strategy`: ExecutionStrategy enum (SEARCH, SCAN, or HYBRID)
- `index`: DetectedIndex with name, prefixes, type, and fields
- `server_query`: RediSearch query that will run server-side
- `client_filters`: Filters that will run in Polars
- `warnings`: Any optimization warnings

### Index Discovery

Explore available indexes:

```python
from polars_redis import list_indexes, find_index_for_pattern

# List all RediSearch indexes
indexes = list_indexes("redis://localhost:6379")
for idx in indexes:
    print(f"{idx.name}: prefixes={idx.prefixes}, fields={idx.fields}")

# Find index for specific pattern
idx = find_index_for_pattern("redis://localhost:6379", "user:*")
if idx:
    print(f"Found index: {idx.name}")
else:
    print("No index covers this pattern")
```

### Explicit Index Control

Override auto-detection when needed:

```python
# Use a specific index by name
df = smart_scan(
    url,
    "user:*",
    schema={"name": pl.Utf8},
    index="users_idx",
    auto_detect_index=False,
)

# Use an Index object (auto-creates if needed)
from polars_redis import Index, TextField, NumericField

idx = Index(
    name="users_idx",
    prefix="user:",
    schema=[TextField("name"), NumericField("age")],
)

df = smart_scan(
    url,
    "user:*",
    schema={"name": pl.Utf8, "age": pl.Int64},
    index=idx,
).collect()
```

### Graceful Degradation

When RediSearch is unavailable (no module loaded), `smart_scan` falls back to SCAN without errors:

```python
# Works whether RediSearch is available or not
df = smart_scan(
    url,
    "user:*",
    schema={"name": pl.Utf8, "age": pl.Int64},
).collect()
```

This makes `smart_scan` the recommended default for new code - you get automatic optimization when indexes exist, with zero changes needed when they don't.

## Performance Comparison

| Method | Data Transfer | Use Case |
|--------|---------------|----------|
| `scan_hashes()` | All matching keys | No index, small datasets |
| `scan_hashes()` + filter | All keys, filter client-side | No index available |
| `search_hashes()` | Only matching docs | Indexed data, selective queries |
| `smart_scan()` | Optimized automatically | Best default - auto-selects strategy |
| `aggregate_hashes()` | Only aggregated results | Analytics, reporting |

For large datasets with selective queries, RediSearch can reduce data transfer by 90%+ compared to scanning. With `smart_scan`, you get this optimization automatically when indexes are available.
