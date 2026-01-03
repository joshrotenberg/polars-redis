# Installation

## Python

Install from PyPI:

```bash
pip install polars-redis
```

Or with uv:

```bash
uv add polars-redis
```

### Requirements

- Python 3.9+
- Polars 1.0+

## Rust

Add to your `Cargo.toml`:

```toml
[dependencies]
polars-redis = "0.1"
```

## Redis Setup

polars-redis requires Redis 7.0 or later. For JSON support, you need the RedisJSON module.

### Docker (recommended for development)

```bash
# Redis Stack includes RedisJSON
docker run -d --name redis -p 6379:6379 redis/redis-stack:latest
```

### Verify installation

```python
import polars_redis as redis

# Check version
print(redis.__version__)

# Test connection (will raise if Redis is not available)
keys = redis.scan_keys("redis://localhost:6379", pattern="*", count=1)
print(f"Connected! Found {len(keys)} keys")
```
