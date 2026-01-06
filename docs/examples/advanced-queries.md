# Advanced Query Examples

This page demonstrates real-world use cases for the polars-redis query builder.

## Vector Similarity Search

Find similar items using vector embeddings with KNN search.

### Setup

```bash
# Create index with vector field
FT.CREATE products_idx ON HASH PREFIX 1 product: SCHEMA \
    name TEXT SORTABLE \
    description TEXT \
    category TAG \
    price NUMERIC SORTABLE \
    embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 384 DISTANCE_METRIC COSINE
```

### Find Similar Products

```python
import polars as pl
import polars_redis as redis
from polars_redis import col

# Assume you have a function to generate embeddings
# embedding = get_embedding("wireless bluetooth headphones")

# Find 10 most similar products
query = col("embedding").knn(10, "query_vec")

df = redis.search_hashes(
    "redis://localhost:6379",
    index="products_idx",
    query=query,
    schema={
        "name": pl.Utf8,
        "description": pl.Utf8,
        "category": pl.Utf8,
        "price": pl.Float64,
    },
    params={"query_vec": embedding_bytes},
).collect()

print(df)
```

### Hybrid Search: Vector + Filters

Combine vector similarity with traditional filters:

```python
from polars_redis import col, raw

# Find similar products under $100 in electronics category
filter_query = (col("category") == "electronics") & (col("price") < 100)
vector_query = col("embedding").knn(10, "query_vec")

# Combine: filter first, then vector search
query = raw(f"({filter_query.to_redis()})=>[KNN 10 @embedding $query_vec]")

df = redis.search_hashes(
    "redis://localhost:6379",
    index="products_idx",
    query=query,
    schema={"name": pl.Utf8, "price": pl.Float64},
    params={"query_vec": embedding_bytes},
).collect()
```

## Geo-Location Filtering

Filter data by geographic boundaries.

### Setup

```bash
# Create index with geo field
FT.CREATE stores_idx ON HASH PREFIX 1 store: SCHEMA \
    name TEXT SORTABLE \
    address TEXT \
    location GEO \
    type TAG
```

### Find Stores Within Radius

```python
from polars_redis import col

# Find stores within 5km of downtown
query = col("location").within_radius(-122.4194, 37.7749, 5, "km")

df = redis.search_hashes(
    "redis://localhost:6379",
    index="stores_idx",
    query=query,
    schema={"name": pl.Utf8, "address": pl.Utf8, "type": pl.Utf8},
).collect()

print(f"Found {len(df)} stores nearby")
```

### Filter by Delivery Zone (Polygon)

```python
from polars_redis import col

# Define delivery zone polygon (lon, lat pairs)
delivery_zone = [
    (-122.45, 37.75),
    (-122.45, 37.80),
    (-122.38, 37.80),
    (-122.38, 37.75),
    (-122.45, 37.75),  # Close the polygon
]

query = col("location").within_polygon(delivery_zone)

# Combine with other filters
query = query & (col("type") == "restaurant")

df = redis.search_hashes(
    "redis://localhost:6379",
    index="stores_idx",
    query=query,
    schema={"name": pl.Utf8, "address": pl.Utf8},
).collect()
```

## Fuzzy Name Matching

Handle typos and spelling variations in searches.

### Setup

```bash
FT.CREATE customers_idx ON HASH PREFIX 1 customer: SCHEMA \
    name TEXT SORTABLE \
    email TAG \
    company TEXT
```

### Search with Typo Tolerance

```python
from polars_redis import col

# Find "Johnson" even with typos like "Jonson" or "Johnsen"
query = col("name").fuzzy("johnson", distance=1)

df = redis.search_hashes(
    "redis://localhost:6379",
    index="customers_idx",
    query=query,
    schema={"name": pl.Utf8, "email": pl.Utf8, "company": pl.Utf8},
).collect()
```

### Deduplicate Records

Find potential duplicates using fuzzy matching:

```python
from polars_redis import col

def find_duplicates(name: str, distance: int = 2):
    """Find records with similar names."""
    query = col("name").fuzzy(name, distance=distance)
    
    return redis.search_hashes(
        "redis://localhost:6379",
        index="customers_idx",
        query=query,
        schema={"name": pl.Utf8, "email": pl.Utf8},
    ).collect()

# Check for duplicates of a new entry
duplicates = find_duplicates("Micheal Smith")  # Will find "Michael Smith"
if len(duplicates) > 0:
    print("Potential duplicates found:")
    print(duplicates)
```

## Full-Text Document Search

Search documents with phrase matching and relevance tuning.

### Setup

```bash
FT.CREATE articles_idx ON HASH PREFIX 1 article: SCHEMA \
    title TEXT WEIGHT 2.0 SORTABLE \
    body TEXT \
    author TAG \
    tags TAG \
    published NUMERIC SORTABLE
```

### Phrase Search with Proximity

```python
from polars_redis import col

# Exact phrase match
query = col("body").phrase("machine", "learning")

# Allow words between (slop = max intervening words)
query = col("body").phrase("data", "science", slop=3)

# Require in-order with slop
query = col("title").phrase("getting", "started", slop=2, inorder=True)

df = redis.search_hashes(
    "redis://localhost:6379",
    index="articles_idx",
    query=query,
    schema={"title": pl.Utf8, "body": pl.Utf8, "author": pl.Utf8},
).collect()
```

### Multi-Field Search

Search across multiple fields simultaneously:

```python
from polars_redis import cols

# Search both title and body
query = cols("title", "body").contains("python")

df = redis.search_hashes(
    "redis://localhost:6379",
    index="articles_idx",
    query=query,
    schema={"title": pl.Utf8, "body": pl.Utf8},
).collect()
```

### Relevance Boosting

Prioritize matches in important fields:

```python
from polars_redis import col

# Title matches are more important than body matches
title_query = col("title").contains("python").boost(2.0)
body_query = col("body").contains("python")

query = title_query | body_query

df = redis.search_hashes(
    "redis://localhost:6379",
    index="articles_idx",
    query=query,
    schema={"title": pl.Utf8, "body": pl.Utf8},
    sort_by="__score",  # Sort by relevance score
).collect()
```

### Optional Terms for Soft Preferences

Include optional terms that improve ranking but aren't required:

```python
from polars_redis import col

# Must have "python", prefer "tutorial" or "beginner"
required = col("title").contains("python")
optional1 = col("title").contains("tutorial").optional()
optional2 = col("tags").has_tag("beginner").optional()

query = required & optional1 & optional2

# Results: All have "python", but those with "tutorial" 
# or "beginner" tag rank higher
```

## Complex Filter Combinations

Build sophisticated queries combining multiple conditions.

### E-commerce Product Search

```python
from polars_redis import col

# Find electronics under $500, in stock, with good ratings
query = (
    (col("category") == "electronics")
    & (col("price").is_between(50, 500))
    & (col("in_stock") == True)
    & (col("rating") >= 4.0)
)

# Add optional preference for prime shipping
query = query & col("prime_eligible").has_tag("yes").optional()

df = redis.search_hashes(
    "redis://localhost:6379",
    index="products_idx",
    query=query,
    schema={
        "name": pl.Utf8,
        "price": pl.Float64,
        "rating": pl.Float64,
    },
    sort_by="rating",
    sort_ascending=False,
).collect()
```

### Dynamic Query Building

Build queries from user input:

```python
from polars_redis import col, match_all

def build_search_query(
    text: str | None = None,
    category: str | None = None,
    min_price: float | None = None,
    max_price: float | None = None,
    tags: list[str] | None = None,
):
    """Build a search query from user parameters."""
    conditions = []
    
    if text:
        conditions.append(col("title").contains(text))
    
    if category:
        conditions.append(col("category") == category)
    
    if min_price is not None and max_price is not None:
        conditions.append(col("price").is_between(min_price, max_price))
    elif min_price is not None:
        conditions.append(col("price") >= min_price)
    elif max_price is not None:
        conditions.append(col("price") <= max_price)
    
    if tags:
        conditions.append(col("tags").has_any_tag(tags))
    
    # Combine all conditions with AND
    if not conditions:
        return match_all()
    
    query = conditions[0]
    for condition in conditions[1:]:
        query = query & condition
    
    return query

# Example usage
query = build_search_query(
    text="laptop",
    category="electronics",
    max_price=1000,
    tags=["gaming", "portable"],
)

print(f"Generated query: {query.to_redis()}")
```

### Combining Numeric, Tag, Text, and Geo

```python
from polars_redis import col

# Find restaurants:
# - Within 2km of current location
# - Open now (hour check)
# - Rating >= 4.0
# - Cuisine matches preference
# - Has outdoor seating (optional bonus)

query = (
    col("location").within_radius(-122.4, 37.7, 2, "km")
    & col("rating") >= 4.0
    & col("cuisine").has_any_tag(["italian", "mediterranean"])
    & col("hours").contains("dinner")
)

# Prefer outdoor seating but don't require it
query = query & col("features").has_tag("outdoor").optional()

df = redis.search_hashes(
    "redis://localhost:6379",
    index="restaurants_idx",
    query=query,
    schema={
        "name": pl.Utf8,
        "rating": pl.Float64,
        "cuisine": pl.Utf8,
        "address": pl.Utf8,
    },
    sort_by="rating",
    sort_ascending=False,
).collect()
```

## Debugging Queries

Use `to_redis()` to inspect generated queries:

```python
from polars_redis import col

query = (
    (col("category") == "electronics")
    & (col("price") < 100)
    & col("title").fuzzy("wireless", distance=1)
)

# See the generated RediSearch query
print(query.to_redis())
# Output: @category:{electronics} @price:[-inf (100] @title:%wireless%

# Use this to:
# - Debug unexpected results
# - Copy to redis-cli for testing
# - Verify query syntax
```
