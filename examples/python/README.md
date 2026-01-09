# Examples

These examples demonstrate common polars-redis usage patterns.

## Prerequisites

- Redis Stack running on localhost:6379 (includes RediSearch and RedisJSON)
- `pip install polars-redis redis`

## Examples

| File | Description |
|------|-------------|
| `basic_hashes.py` | Scanning and writing Redis hashes |
| `json_documents.py` | Working with RedisJSON documents |
| `schema_inference.py` | Automatic schema detection |
| `strings_and_counters.py` | Redis strings and counters |
| `ttl_and_metadata.py` | TTL and row index columns |
| `write_modes.py` | Write modes: fail, replace, append |
| `search_example.py` | RediSearch: server-side filtering and aggregation |
| `vector_search.py` | Vector similarity search with embeddings |

## Running

```bash
# Start Redis Stack (includes RediSearch and RedisJSON)
docker run -d --name redis -p 6379:6379 redis/redis-stack

# Run an example
python examples/python/basic_hashes.py

# Run RediSearch example
python examples/python/search_example.py

# Run vector search example (requires sentence-transformers)
pip install sentence-transformers
python examples/python/vector_search.py
```
