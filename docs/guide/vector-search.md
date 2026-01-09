# Vector Similarity Search

Redis Stack includes vector search capabilities that pair well with polars-redis for building semantic search, recommendation systems, and AI-powered retrieval.

## Overview

Vector search enables finding similar items based on embeddings - numerical representations of text, images, or other data. Common use cases:

- **Semantic search**: Find documents by meaning, not just keywords
- **Product recommendations**: "Customers who viewed X also liked Y"
- **Image similarity**: Find visually similar products
- **RAG retrieval**: Fetch relevant context for LLM prompts

## Quick Start

### Prerequisites

```bash
# Redis Stack (includes vector search)
docker run -d --name redis -p 6379:6379 redis/redis-stack

# Python dependencies
pip install polars-redis redis sentence-transformers
```

### Complete Example

```python
import struct
import polars as pl
import redis
from sentence_transformers import SentenceTransformer

# Sample data
products = [
    {"id": "1", "name": "Wireless Headphones", "category": "electronics"},
    {"id": "2", "name": "Bluetooth Speaker", "category": "electronics"},
    {"id": "3", "name": "Office Chair", "category": "furniture"},
]

# Generate embeddings
model = SentenceTransformer("all-MiniLM-L6-v2")
embeddings = model.encode([p["name"] for p in products])

# Store in Redis
client = redis.from_url("redis://localhost:6379")
for product, emb in zip(products, embeddings):
    client.hset(
        f"product:{product['id']}",
        mapping={
            "name": product["name"],
            "category": product["category"],
            "embedding": struct.pack(f"{len(emb)}f", *emb.tolist()),
        },
    )

# Create vector index
from redis.commands.search.field import TagField, TextField, VectorField
from redis.commands.search.index_definition import IndexDefinition, IndexType

client.ft("products_idx").create_index(
    [
        TextField("name"),
        TagField("category"),
        VectorField(
            "embedding",
            "HNSW",
            {"TYPE": "FLOAT32", "DIM": 384, "DISTANCE_METRIC": "COSINE"},
        ),
    ],
    definition=IndexDefinition(prefix=["product:"], index_type=IndexType.HASH),
)

# Query by similarity
from redis.commands.search.query import Query

query_emb = model.encode(["audio equipment"])[0]
query_bytes = struct.pack(f"{len(query_emb)}f", *query_emb.tolist())

results = client.ft("products_idx").search(
    Query("*=>[KNN 3 @embedding $vec AS score]")
    .return_fields("name", "category", "score")
    .sort_by("score")
    .dialect(2),
    query_params={"vec": query_bytes},
)

# Convert to DataFrame
rows = [{"name": doc.name, "category": doc.category, "score": float(doc.score)} 
        for doc in results.docs]
df = pl.DataFrame(rows)
print(df)
```

## Vector Index Configuration

### HNSW Algorithm

HNSW (Hierarchical Navigable Small World) is the recommended algorithm for most use cases:

```python
VectorField(
    "embedding",
    "HNSW",
    {
        "TYPE": "FLOAT32",           # or FLOAT64
        "DIM": 384,                  # Must match embedding dimension
        "DISTANCE_METRIC": "COSINE", # or L2, IP
        "M": 16,                     # Connections per node (higher = more accurate, more memory)
        "EF_CONSTRUCTION": 200,      # Build-time search width (higher = better quality)
    },
)
```

### Distance Metrics

| Metric | Use Case |
|--------|----------|
| `COSINE` | Text embeddings, normalized vectors |
| `L2` | Euclidean distance, unnormalized vectors |
| `IP` | Inner product, when vectors are pre-normalized |

## Hybrid Search

Combine vector similarity with metadata filters:

```python
# Vector + category filter
query = "(@category:{electronics})=>[KNN 5 @embedding $vec AS score]"

# Vector + price range
query = "(@price:[0 100])=>[KNN 5 @embedding $vec AS score]"

# Vector + text match + category
query = "(@name:wireless @category:{electronics})=>[KNN 5 @embedding $vec AS score]"
```

## Embedding Models

### sentence-transformers (Local)

No API key required, runs locally:

```python
from sentence_transformers import SentenceTransformer

# General purpose (384 dimensions)
model = SentenceTransformer("all-MiniLM-L6-v2")

# Higher quality (768 dimensions)
model = SentenceTransformer("all-mpnet-base-v2")

embeddings = model.encode(["your text here"])
```

### OpenAI (API)

Higher quality, requires API key:

```python
from openai import OpenAI

client = OpenAI()
response = client.embeddings.create(
    model="text-embedding-3-small",  # 1536 dimensions
    input=["your text here"],
)
embedding = response.data[0].embedding
```

## Integration with polars-redis

### Reading Vector Search Results

After a vector search, convert results to a DataFrame for further analysis:

```python
import polars as pl

results = client.ft("products_idx").search(query, query_params={"vec": query_bytes})

df = pl.DataFrame([
    {
        "id": doc.id,
        "name": doc.name,
        "score": float(doc.score),
    }
    for doc in results.docs
])

# Further analysis with Polars
top_results = df.filter(pl.col("score") < 0.5).sort("score")
```

### Batch Processing with polars-redis

For bulk operations, use polars-redis to read/write the non-vector fields:

```python
import polars_redis as pr

# Read products (without embeddings)
df = pr.scan_hashes(
    "redis://localhost:6379",
    "product:*",
    schema={"name": pl.Utf8, "category": pl.Utf8, "price": pl.Float64},
).collect()

# Process and write back
enriched = df.with_columns(pl.col("price") * 1.1)
pr.write_hashes(enriched, "redis://localhost:6379", key_column="_key")
```

## Performance Tips

### Index Tuning

| Parameter | Trade-off |
|-----------|-----------|
| `M` (16-64) | Higher = better recall, more memory |
| `EF_CONSTRUCTION` (100-500) | Higher = better quality, slower indexing |
| `EF_RUNTIME` (query param) | Higher = better recall, slower queries |

### Batch Embedding Generation

Generate embeddings in batches for better throughput:

```python
# Batch encode (much faster than one at a time)
texts = df["description"].to_list()
embeddings = model.encode(texts, batch_size=32, show_progress_bar=True)
```

### Pre-filter When Possible

Hybrid queries with filters are faster than post-filtering:

```python
# Good: filter in query (uses index)
"(@category:{electronics})=>[KNN 10 @embedding $vec]"

# Less efficient: fetch all, filter after
results = search("*=>[KNN 100 @embedding $vec]")
filtered = [r for r in results if r.category == "electronics"][:10]
```

## Example Script

A complete working example is available at:

```bash
python examples/python/vector_search.py
```

This demonstrates:
- Embedding generation with sentence-transformers
- Creating an HNSW vector index
- Semantic search queries
- Hybrid filtering (vector + category, vector + price range)

## See Also

- [RediSearch Queries](redisearch-queries.md) - Text search and filtering
- [RediSearch Aggregation](redisearch-aggregation.md) - Server-side aggregation
- [Scanning Data](scanning.md) - Reading data into DataFrames
