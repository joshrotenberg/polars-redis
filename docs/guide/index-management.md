# Index Management

polars-redis provides typed helpers for creating and managing RediSearch indexes, eliminating the need to write raw `FT.CREATE` commands.

## Overview

Instead of manually constructing FT.CREATE commands:

```bash
# Manual approach
FT.CREATE users_idx ON HASH PREFIX 1 user: SCHEMA \
    name TEXT SORTABLE \
    age NUMERIC SORTABLE \
    status TAG
```

Use the typed Python API:

```python
from polars_redis import Index, TextField, NumericField, TagField

idx = Index(
    name="users_idx",
    prefix="user:",
    schema=[
        TextField("name", sortable=True),
        NumericField("age", sortable=True),
        TagField("status"),
    ]
)
idx.create("redis://localhost:6379")
```

## Field Types

### TextField

Full-text search fields with stemming, phonetic matching, and relevance scoring.

```python
from polars_redis import TextField

# Basic text field
TextField("title")

# With options
TextField("title", sortable=True, weight=2.0)  # Higher relevance weight
TextField("body", nostem=True)  # Disable stemming
TextField("name", phonetic="dm:en")  # Double Metaphone English
TextField("content", withsuffixtrie=True)  # Enable suffix queries (*word)
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `sortable` | bool | False | Enable sorting on this field |
| `nostem` | bool | False | Disable stemming |
| `weight` | float | 1.0 | Relevance weight for scoring |
| `phonetic` | str | None | Phonetic algorithm (dm:en, dm:fr, dm:pt, dm:es) |
| `noindex` | bool | False | Store but don't index |
| `withsuffixtrie` | bool | False | Enable suffix queries |

### NumericField

Numeric fields for range queries.

```python
from polars_redis import NumericField

NumericField("age")
NumericField("price", sortable=True)
NumericField("score", noindex=True)  # Store only
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `sortable` | bool | False | Enable sorting |
| `noindex` | bool | False | Store but don't index |

### TagField

Exact-match fields for categories, tags, and statuses. Values are not tokenized or stemmed.

```python
from polars_redis import TagField

TagField("status")
TagField("tags", separator="|")  # Pipe-separated values
TagField("code", casesensitive=True)  # Case-sensitive matching
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `separator` | str | "," | Character separating multiple values |
| `casesensitive` | bool | False | Case-sensitive matching |
| `sortable` | bool | False | Enable sorting |
| `noindex` | bool | False | Store but don't index |
| `withsuffixtrie` | bool | False | Enable suffix queries |

### GeoField

Geographic fields for radius and polygon queries.

```python
from polars_redis import GeoField

GeoField("location")
# Store as: HSET key location "-122.4194,37.7749"
```

### VectorField

Vector fields for similarity search with embeddings.

```python
from polars_redis import VectorField

# For sentence-transformers (384 dim)
VectorField("embedding", algorithm="HNSW", dim=384, distance_metric="COSINE")

# For OpenAI embeddings (1536 dim)
VectorField("embedding", algorithm="HNSW", dim=1536, distance_metric="COSINE", m=32)

# Flat index (brute force, exact results)
VectorField("embedding", algorithm="FLAT", dim=384, distance_metric="L2")
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `algorithm` | str | "HNSW" | "HNSW" (approximate) or "FLAT" (exact) |
| `dim` | int | 384 | Vector dimension |
| `distance_metric` | str | "COSINE" | "COSINE", "L2", or "IP" (inner product) |
| `initial_cap` | int | None | Initial index capacity |
| `m` | int | None | HNSW: edges per node (default 16) |
| `ef_construction` | int | None | HNSW: construction-time search width |
| `ef_runtime` | int | None | HNSW: query-time search width |
| `block_size` | int | None | FLAT: block size |

### GeoShapeField

Polygon and complex geometry fields.

```python
from polars_redis import GeoShapeField

GeoShapeField("boundary")
GeoShapeField("region", coord_system="FLAT")  # Flat coordinate system
# Store as WKT: HSET key boundary "POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))"
```

## Creating Indexes

### Basic Creation

```python
from polars_redis import Index, TextField, NumericField, TagField

idx = Index(
    name="products_idx",
    prefix="product:",
    schema=[
        TextField("name", sortable=True, weight=2.0),
        TextField("description"),
        NumericField("price", sortable=True),
        TagField("category"),
    ]
)

# Create the index
idx.create("redis://localhost:6379")
```

### JSON Indexes

For RedisJSON documents:

```python
idx = Index(
    name="products_idx",
    prefix="product:",
    schema=[
        TextField("name"),
        NumericField("price"),
    ],
    on="JSON",  # Index JSON documents instead of hashes
)
```

### Index Options

```python
idx = Index(
    name="articles_idx",
    prefix="article:",
    schema=[TextField("content")],
    
    # Multiple prefixes
    # prefix=["article:", "post:"],
    
    # Language and stemming
    language="english",
    # language_field="lang",  # Per-document language
    
    # Stopwords
    stopwords=["the", "a", "an"],  # Custom stopwords
    # stopwords=[],  # Disable stopwords
    
    # Memory optimizations
    nooffsets=True,  # Save memory, disable exact phrase search
    nofreqs=True,  # Save memory, affects scoring
    
    # Skip scanning existing keys
    skipinitialscan=True,
)
```

## Idempotent Operations

### ensure_exists()

Create the index if it doesn't exist (safe for concurrent access):

```python
idx = Index(name="users_idx", prefix="user:", schema=[TextField("name")])

# Safe to call multiple times
idx.ensure_exists("redis://localhost:6379")
idx.ensure_exists("redis://localhost:6379")  # No-op
```

### create() with if_not_exists

```python
idx.create("redis://localhost:6379", if_not_exists=True)
```

### Check Existence

```python
if idx.exists("redis://localhost:6379"):
    print("Index already exists")
```

## Auto-Create with search_hashes()

Pass an `Index` object to `search_hashes()` for automatic index creation:

```python
from polars_redis import Index, TextField, NumericField, TagField, col, search_hashes
import polars as pl

idx = Index(
    name="users_idx",
    prefix="user:",
    schema=[
        TextField("name", sortable=True),
        NumericField("age", sortable=True),
        TagField("status"),
    ]
)

# Index is auto-created if it doesn't exist
df = search_hashes(
    "redis://localhost:6379",
    index=idx,  # Pass Index object instead of string
    query=col("age") > 30,
    schema={"name": pl.Utf8, "age": pl.Int64, "status": pl.Utf8},
).collect()
```

To disable auto-creation:

```python
df = search_hashes(
    url,
    index=idx,
    query=col("age") > 30,
    schema=schema,
    create_index=False,  # Don't auto-create
).collect()
```

## Schema Inference

### From DataFrame

Infer an index schema from an existing DataFrame:

```python
import polars as pl
from polars_redis import Index

df = pl.DataFrame({
    "name": ["Alice", "Bob"],
    "age": [30, 25],
    "department": ["engineering", "sales"],
    "salary": [100000.0, 85000.0],
})

# Infer schema (strings default to TAG)
idx = Index.from_frame(df, "employees_idx", "employee:")

# Specify which string fields should be TEXT (full-text search)
idx = Index.from_frame(
    df,
    "employees_idx",
    "employee:",
    text_fields=["name"],  # name becomes TEXT, department stays TAG
    sortable=["age", "salary"],
)
```

Type mapping:

| Polars Type | Default Field Type |
|-------------|-------------------|
| Int8-Int64, UInt8-UInt64, Float32, Float64 | NUMERIC |
| Utf8, String | TAG (or TEXT if in text_fields) |
| Boolean | TAG |

### From Schema Dict

Create an index from a Polars schema dictionary:

```python
schema = {"name": pl.Utf8, "age": pl.Int64, "active": pl.Boolean}

idx = Index.from_schema(
    schema,
    "users_idx",
    "user:",
    text_fields=["name"],
    sortable=["age"],
)
```

### From Existing Index

Load an existing index definition from Redis:

```python
idx = Index.from_redis("redis://localhost:6379", "existing_idx")
if idx:
    print(f"Index has {len(idx.schema)} fields")
    print(f"Prefix: {idx.prefix}")
```

## Validation and Migration

### Validate Schema

Check if a Polars schema matches an index:

```python
schema = {"name": pl.Utf8, "age": pl.Int64, "email": pl.Utf8}

warnings = idx.validate_schema(schema)
for warning in warnings:
    print(warning)
# Output:
# Field 'email' in schema but not in index
```

### Compare Schemas (diff)

Compare desired schema to existing index:

```python
idx = Index(
    name="users_idx",
    prefix="user:",
    schema=[
        TextField("name"),
        NumericField("age"),
        TagField("status"),  # New field
    ]
)

diff = idx.diff("redis://localhost:6379")
print(diff)
# + status (TAG)
# ~ age: TEXT -> NUMERIC  
#   name (unchanged)
```

### Migration

!!! warning "Destructive Operation"
    RediSearch doesn't support ALTER for most schema changes. Migration requires dropping and recreating the index, which re-indexes all documents.

```python
diff = idx.diff(url)
if diff.has_changes:
    print(f"Changes needed:\n{diff}")
    
    # Migrate (drop and recreate)
    idx.migrate(url, drop_existing=True)
```

## Dropping Indexes

```python
# Drop index only (keep data)
idx.drop("redis://localhost:6379")

# Drop index and delete all indexed documents
idx.drop("redis://localhost:6379", delete_docs=True)
```

## Index Information

Get details about an existing index:

```python
info = idx.info("redis://localhost:6379")
if info:
    print(f"Documents indexed: {info.num_docs}")
    print(f"Index type: {info.on_type}")
    print(f"Prefixes: {info.prefixes}")
    print(f"Fields: {len(info.fields)}")
```

## Debugging

View the generated FT.CREATE command:

```python
idx = Index(
    name="users_idx",
    prefix="user:",
    schema=[
        TextField("name", sortable=True),
        NumericField("age"),
    ]
)

print(str(idx))
# FT.CREATE users_idx ON HASH PREFIX 1 user: SCHEMA name TEXT SORTABLE age NUMERIC
```

## Complete Example

```python
import polars as pl
import polars_redis as pr
from polars_redis import Index, TextField, NumericField, TagField, col

url = "redis://localhost:6379"

# 1. Create sample data
df = pl.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice Smith", "Bob Jones", "Charlie Brown", "Diana Ross", "Eve Wilson"],
    "age": [30, 25, 35, 28, 32],
    "department": ["engineering", "sales", "engineering", "marketing", "engineering"],
    "salary": [120000, 85000, 140000, 95000, 130000],
})

# 2. Write to Redis
pr.write_hashes(df, url, key_column="id", key_prefix="employee:")

# 3. Define index
idx = Index(
    name="employees_idx",
    prefix="employee:",
    schema=[
        TextField("name", sortable=True),
        NumericField("age", sortable=True),
        TagField("department"),
        NumericField("salary", sortable=True),
    ]
)

# 4. Create index (idempotent)
idx.ensure_exists(url)

# 5. Query with predicate pushdown
engineers = pr.search_hashes(
    url,
    index=idx,
    query=(col("department") == "engineering") & (col("salary") > 125000),
    schema={"name": pl.Utf8, "age": pl.Int64, "department": pl.Utf8, "salary": pl.Float64},
).collect()

print(engineers)
# shape: (2, 4)
# +-----------------+-----+-------------+----------+
# | name            | age | department  | salary   |
# | ---             | --- | ---         | ---      |
# | str             | i64 | str         | f64      |
# +=================+=====+=============+==========+
# | Charlie Brown   | 35  | engineering | 140000.0 |
# | Eve Wilson      | 32  | engineering | 130000.0 |
# +-----------------+-----+-------------+----------+

# 6. Clean up
idx.drop(url)
```

## Rust API

The Rust API provides equivalent functionality with a builder pattern:

```rust
use polars_redis::{Index, TextField, NumericField, TagField};

let idx = Index::new("users_idx")
    .with_prefix("user:")
    .with_field(TextField::new("name").sortable())
    .with_field(NumericField::new("age").sortable())
    .with_field(TagField::new("status"));

// Create
idx.create("redis://localhost:6379")?;

// Or ensure exists
idx.ensure_exists("redis://localhost:6379")?;

// Check existence
if idx.exists("redis://localhost:6379")? {
    println!("Index exists");
}

// Drop
idx.drop("redis://localhost:6379")?;
```

See the [Rust API Reference](../api/rust.md) for full details.
