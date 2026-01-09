#!/usr/bin/env python3
"""Vector similarity search with polars-redis.

This example demonstrates:
1. Generating embeddings with sentence-transformers
2. Storing vectors in Redis hashes
3. Creating an HNSW vector index
4. Querying by semantic similarity
5. Hybrid filtering (vector + metadata)

Prerequisites:
    pip install sentence-transformers polars-redis redis

    docker run -d --name redis -p 6379:6379 redis/redis-stack
"""

from __future__ import annotations

import struct
import time

import polars as pl

# Sample product catalog - in practice, load from CSV/parquet/database
PRODUCTS = [
    {
        "id": "prod_001",
        "name": "Wireless Noise-Canceling Headphones",
        "category": "electronics",
        "price": 299.99,
    },
    {
        "id": "prod_002",
        "name": "Bluetooth Speaker with Deep Bass",
        "category": "electronics",
        "price": 79.99,
    },
    {
        "id": "prod_003",
        "name": "USB-C Fast Charging Cable",
        "category": "electronics",
        "price": 19.99,
    },
    {"id": "prod_004", "name": "Ergonomic Office Chair", "category": "furniture", "price": 449.99},
    {"id": "prod_005", "name": "Standing Desk Converter", "category": "furniture", "price": 299.99},
    {
        "id": "prod_006",
        "name": "Mechanical Keyboard with RGB",
        "category": "electronics",
        "price": 149.99,
    },
    {
        "id": "prod_007",
        "name": "Ultrawide Curved Monitor",
        "category": "electronics",
        "price": 599.99,
    },
    {"id": "prod_008", "name": "Laptop Stand Aluminum", "category": "accessories", "price": 49.99},
    {
        "id": "prod_009",
        "name": "Wireless Mouse Ergonomic",
        "category": "electronics",
        "price": 69.99,
    },
    {
        "id": "prod_010",
        "name": "Webcam HD 1080p with Microphone",
        "category": "electronics",
        "price": 89.99,
    },
    {"id": "prod_011", "name": "Desk Lamp LED Adjustable", "category": "furniture", "price": 39.99},
    {"id": "prod_012", "name": "Cable Management Kit", "category": "accessories", "price": 24.99},
    {
        "id": "prod_013",
        "name": "Monitor Arm Dual Mount",
        "category": "accessories",
        "price": 129.99,
    },
    {
        "id": "prod_014",
        "name": "Noise Machine White Sound",
        "category": "electronics",
        "price": 49.99,
    },
    {
        "id": "prod_015",
        "name": "Portable Charger Power Bank",
        "category": "electronics",
        "price": 59.99,
    },
]


def float_list_to_bytes(floats: list[float]) -> bytes:
    """Convert a list of floats to bytes for Redis vector storage."""
    return struct.pack(f"{len(floats)}f", *floats)


def generate_embeddings(
    texts: list[str], model_name: str = "all-MiniLM-L6-v2"
) -> list[list[float]]:
    """Generate embeddings using sentence-transformers."""
    from sentence_transformers import SentenceTransformer

    print(f"Loading model: {model_name}")
    model = SentenceTransformer(model_name)

    print(f"Generating embeddings for {len(texts)} texts...")
    embeddings = model.encode(texts, show_progress_bar=True)

    return [emb.tolist() for emb in embeddings]


def create_vector_index(client, index_name: str, prefix: str, vector_dim: int) -> None:
    """Create a RediSearch index with vector field."""
    from redis.commands.search.field import NumericField, TagField, TextField, VectorField
    from redis.commands.search.index_definition import IndexDefinition, IndexType

    # Drop existing index if present
    try:
        client.ft(index_name).dropindex(delete_documents=False)
        print(f"Dropped existing index: {index_name}")
    except Exception:
        pass

    # Define schema with vector field
    schema = [
        TextField("name"),
        TagField("category"),
        NumericField("price", sortable=True),
        VectorField(
            "embedding",
            "HNSW",
            {
                "TYPE": "FLOAT32",
                "DIM": vector_dim,
                "DISTANCE_METRIC": "COSINE",
                "M": 16,
                "EF_CONSTRUCTION": 200,
            },
        ),
    ]

    # Create index
    definition = IndexDefinition(prefix=[prefix], index_type=IndexType.HASH)
    client.ft(index_name).create_index(schema, definition=definition)
    print(f"Created index: {index_name}")


def store_products_with_embeddings(
    url: str,
    products: list[dict],
    embeddings: list[list[float]],
    key_prefix: str = "product:",
) -> int:
    """Store products with embeddings in Redis."""
    import redis

    client = redis.from_url(url)
    pipe = client.pipeline()

    for product, embedding in zip(products, embeddings):
        key = f"{key_prefix}{product['id']}"
        pipe.hset(
            key,
            mapping={
                "name": product["name"],
                "category": product["category"],
                "price": str(product["price"]),
                "embedding": float_list_to_bytes(embedding),
            },
        )

    results = pipe.execute()
    client.close()

    return sum(1 for r in results if r)


def vector_search(
    url: str,
    index_name: str,
    query_embedding: list[float],
    k: int = 5,
    filter_expr: str | None = None,
) -> pl.DataFrame:
    """Search for similar products using vector similarity."""
    import redis
    from redis.commands.search.query import Query

    client = redis.from_url(url)

    # Build KNN query
    if filter_expr:
        # Hybrid: filter + vector
        query_str = f"({filter_expr})=>[KNN {k} @embedding $vec AS score]"
    else:
        # Pure vector search
        query_str = f"*=>[KNN {k} @embedding $vec AS score]"

    query = (
        Query(query_str)
        .return_fields("name", "category", "price", "score")
        .sort_by("score")
        .dialect(2)
    )

    # Execute search
    results = client.ft(index_name).search(
        query,
        query_params={"vec": float_list_to_bytes(query_embedding)},
    )

    client.close()

    # Convert to DataFrame
    rows = []
    for doc in results.docs:
        rows.append(
            {
                "id": doc.id.replace("product:", ""),
                "name": doc.name,
                "category": doc.category,
                "price": float(doc.price),
                "score": float(doc.score),
            }
        )

    return pl.DataFrame(rows)


def main() -> None:
    """Run the vector search example."""
    import redis

    url = "redis://localhost:6379"
    index_name = "products_idx"
    key_prefix = "product:"

    # Check Redis connection
    client = redis.from_url(url)
    try:
        client.ping()
    except redis.ConnectionError:
        print("Error: Cannot connect to Redis. Make sure Redis Stack is running:")
        print("  docker run -d --name redis -p 6379:6379 redis/redis-stack")
        return
    finally:
        client.close()

    # Step 1: Generate embeddings for product names
    product_names = [p["name"] for p in PRODUCTS]
    embeddings = generate_embeddings(product_names)
    vector_dim = len(embeddings[0])
    print(f"Generated {len(embeddings)} embeddings of dimension {vector_dim}")

    # Step 2: Create vector index
    client = redis.from_url(url)
    create_vector_index(client, index_name, key_prefix, vector_dim)
    client.close()

    # Step 3: Store products with embeddings
    count = store_products_with_embeddings(url, PRODUCTS, embeddings, key_prefix)
    print(f"Stored {count} products in Redis")

    # Wait for indexing
    time.sleep(0.5)

    # Step 4: Semantic search - find products similar to a query
    print("\n" + "=" * 60)
    print("Semantic Search Examples")
    print("=" * 60)

    # Generate query embedding
    queries = [
        "audio equipment for music",
        "work from home desk setup",
        "charging my phone",
    ]

    for query in queries:
        print(f"\nQuery: '{query}'")
        print("-" * 40)

        query_embedding = generate_embeddings([query])[0]
        results = vector_search(url, index_name, query_embedding, k=3)

        print(results)

    # Step 5: Hybrid search - vector + metadata filter
    print("\n" + "=" * 60)
    print("Hybrid Search: Vector + Category Filter")
    print("=" * 60)

    query = "comfortable seating"
    print(f"\nQuery: '{query}' (filtered to furniture only)")
    print("-" * 40)

    query_embedding = generate_embeddings([query])[0]
    results = vector_search(
        url,
        index_name,
        query_embedding,
        k=3,
        filter_expr="@category:{furniture}",
    )
    print(results)

    # Step 6: Hybrid search - vector + price filter
    print(f"\nQuery: '{query}' (filtered to price < $100)")
    print("-" * 40)

    results = vector_search(
        url,
        index_name,
        query_embedding,
        k=5,
        filter_expr="@price:[0 100]",
    )
    print(results)

    # Cleanup hint
    print("\n" + "=" * 60)
    print("Cleanup")
    print("=" * 60)
    print("To remove test data:")
    print("  redis-cli FLUSHDB")
    print("Or stop the container:")
    print("  docker stop redis && docker rm redis")


if __name__ == "__main__":
    main()
