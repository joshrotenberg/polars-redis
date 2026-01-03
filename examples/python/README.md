# Examples

These examples demonstrate common polars-redis usage patterns.

## Prerequisites

- Redis running on localhost:6379
- RedisJSON module for JSON examples
- `pip install polars-redis`

## Examples

| File | Description |
|------|-------------|
| `basic_hashes.py` | Scanning and writing Redis hashes |
| `json_documents.py` | Working with RedisJSON documents |
| `schema_inference.py` | Automatic schema detection |
| `strings_and_counters.py` | Redis strings and counters |
| `ttl_and_metadata.py` | TTL and row index columns |
| `write_modes.py` | Write modes: fail, replace, append |

## Running

```bash
# Start Redis (with RedisJSON for JSON examples)
docker run -d --name redis -p 6379:6379 redis/redis-stack

# Run an example
python examples/python/basic_hashes.py
```
