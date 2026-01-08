# Query Builder

polars-redis provides a Polars-like query builder that generates RediSearch queries. This gives you a familiar, composable API without learning RediSearch query syntax.

!!! tip "Setup"
    Examples on this page use the test data from [RediSearch Overview](redisearch.md#setup-test-data).

## Basic Operations

### Comparisons

```python
from polars_redis import col

# Numeric comparisons
query = col("age") > 30       # @age:[(30 +inf]
query = col("age") >= 30      # @age:[30 +inf]
query = col("age") < 30       # @age:[-inf (30]
query = col("age") <= 30      # @age:[-inf 30]

# Equality (tags)
query = col("status") == "active"    # @status:{active}
query = col("status") != "inactive"  # -@status:{inactive}
```

### Combining Conditions

```python
# AND (both must match)
query = (col("age") > 30) & (col("status") == "active")
# (@age:[(30 +inf]) (@status:{active})

# OR (either matches)
query = (col("department") == "engineering") | (col("department") == "product")
# (@department:{engineering}) | (@department:{product})

# Negation
query = ~(col("status") == "inactive")
# -(@status:{inactive})
```

### Raw Queries

For RediSearch features not covered by the builder, use `raw()`:

```python
from polars_redis import raw

# Full-text search
query = raw("@name:alice")

# Prefix search
query = raw("@name:ali*")

# Combine raw with builder
query = raw("@name:alice") & (col("age") > 25)
```

## Text Search

### Full-text Search

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

### Fuzzy Matching

Match terms with typos using Levenshtein distance (1-3):

```python
# Allow 1 character difference
query = col("title").fuzzy("python", distance=1)
# @title:%python%

# Allow 2 character differences
query = col("title").fuzzy("algorithm", distance=2)
# @title:%%algorithm%%
```

### Phrase Search

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

### Wildcard Matching

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

## Tag Operations

```python
# Single tag match
query = col("category").has_tag("electronics")
# @category:{electronics}

# Match any of multiple tags
query = col("tags").has_any_tag(["urgent", "important"])
# @tags:{urgent|important}
```

## Geo Search

### Radius Search

```python
# Find locations within 10km of a point
query = col("location").within_radius(-122.4194, 37.7749, 10, "km")
# @location:[-122.4194 37.7749 10 km]

# Supported units: m, km, mi, ft
query = col("location").within_radius(-73.9857, 40.7484, 5, "mi")
```

### Polygon Search

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

## Vector Search

For semantic similarity search with vector embeddings:

### K-Nearest Neighbors (KNN)

```python
# Find 10 most similar documents
query = col("embedding").knn(10, "query_vec")
# *=>[KNN 10 @embedding $query_vec]

# Use with search_hashes (pass vector as parameter)
df = search_hashes(
    url,
    index="docs_idx",
    query=query,
    schema={"title": pl.Utf8, "embedding": pl.List(pl.Float32)},
    params={"query_vec": embedding_bytes},
)
```

### Vector Range Search

```python
# Find all documents within distance 0.5
query = col("embedding").vector_range(0.5, "query_vec")
# @embedding:[VECTOR_RANGE 0.5 $query_vec]
```

## Relevance Tuning

### Boosting

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

### Optional Terms

Mark terms as optional for better ranking without requiring them:

```python
# Documents with "python" required, "tutorial" optional but preferred
required = col("title").contains("python")
optional = col("title").contains("tutorial").optional()
query = required & optional
# @title:python ~@title:tutorial
```

## Null Checks

```python
# Find documents missing a field
query = col("email").is_null()
# ismissing(@email)

# Find documents with a field present
query = col("email").is_not_null()
# -ismissing(@email)
```

## Debugging

Use `to_redis()` to inspect the generated RediSearch query:

```python
query = (col("type") == "eBikes") & (col("price") < 1000)
print(query.to_redis())
# @type:{eBikes} @price:[-inf (1000]
```

## Operations Reference

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
| Negate | `~expr` or `expr.negate()` | `-(...expr...)` |
| Contains | `col("title").contains("word")` | `@title:word` |
| Starts With | `col("name").starts_with("jo")` | `@name:jo*` |
| Ends With | `col("name").ends_with("son")` | `@name:*son` |
| Fuzzy | `col("name").fuzzy("john", 1)` | `@name:%john%` |
| Tag Match | `col("cat").has_tag("x")` | `@cat:{x}` |
| Tag Any | `col("cat").has_any_tag(["x","y"])` | `@cat:{x\|y}` |
| Geo Radius | `col("loc").within_radius(lon, lat, r, "km")` | `@loc:[lon lat r km]` |
| KNN | `col("vec").knn(10, "param")` | `*=>[KNN 10 @vec $param]` |
| Boost | `expr.boost(2.0)` | `(expr) => { $weight: 2.0; }` |
| Optional | `expr.optional()` | `~(expr)` |
| Is Null | `col("field").is_null()` | `ismissing(@field)` |

## Client-Side Filters

Some operations can't be pushed to RediSearch. These fetch data first, then filter in Polars. Use them when RediSearch doesn't support the operation you need.

!!! warning "Performance"
    Client-side filters transfer all matching documents before filtering. Combine with server-side filters when possible to reduce data transfer.

### Regex Matching

```python
# Match email patterns (runs in Polars)
query = col("email").matches_regex(r".*@gmail\.com$")

# Combine with server-side filter for efficiency
query = (col("status") == "active") & col("email").matches_regex(r".*@company\.com")
# Server filters by status first, then Polars filters by regex
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

```python
# Find names similar to "john" (80% similarity threshold)
query = col("name").similar_to("john", threshold=0.8)
```

### Date/Time Operations

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

```python
# Check if array contains value
query = col("tags").array_contains("python")

# Filter by array length
query = col("items").array_len() > 5

# Extract nested JSON value
query = col("metadata").json_path("$.user.role") == "admin"
```

### Hybrid Execution

When queries combine server-side and client-side operations, polars-redis automatically splits execution:

```python
# This query has both server-pushable and client-side parts
query = (col("age") > 30) & col("email").matches_regex(r".*@gmail\.com")

# Check what runs where
print(query.is_client_side)  # True

# View execution plan
print(query.explain())
# RediSearch: @age:[(30 +inf]
# Polars filter: pl.col("email").str.contains(r".*@gmail\.com")
```

The query builder automatically optimizes for minimum data transfer by pushing what it can to RediSearch.
