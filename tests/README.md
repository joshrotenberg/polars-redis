# Testing polars-redis

This directory contains integration and stress tests for polars-redis.

## Test Categories

### Unit Tests (in `src/`)

Run with:
```bash
cargo test --lib --all-features
```

These tests don't require Redis and run automatically in CI.

### Integration Tests

These tests require a running Redis instance with the required modules.

| Test File | Description | Requirements |
|-----------|-------------|--------------|
| `integration_hash.rs` | Hash scanning operations | Redis with JSON module |
| `integration_search.rs` | RediSearch FT.SEARCH operations | Redis with Search module |
| `integration_write.rs` | Write operations (hashes, strings) | Redis with JSON module |
| `integration_cluster.rs` | Redis Cluster operations | Redis Cluster (3+ nodes) |

### Stress Tests

`stress_tests.rs` tests behavior under high load:
- Large datasets (10k, 100k, 1M keys)
- Memory efficiency
- Write throughput
- Field size variations

These are resource-intensive and run manually only.

## Running Tests Locally

### Option 1: Using docker-wrapper (Recommended)

The tests use docker-wrapper to manage Redis containers automatically. Just run:

```bash
# Integration tests (starts Redis 8 on port 16379)
cargo test --test integration_hash --features "json,search" -- --ignored

# All integration tests
cargo test --test 'integration_*' --features "json,search" -- --ignored
```

### Option 2: Using an Existing Redis

Set environment variables to point to your Redis instance:

```bash
export REDIS_URL=redis://localhost:6379
export REDIS_PORT=6379

cargo test --test integration_hash --features "json,search" -- --ignored
```

### Running Stress Tests

Stress tests require more resources and time:

```bash
# Start Redis (or use existing)
docker run -d --name redis-stress -p 16379:6379 redis:8

# Run stress tests
cargo test --test stress_tests --features "json,search" -- --ignored --nocapture
```

### Running Cluster Tests

Cluster tests require a multi-node Redis Cluster:

```bash
# Start Redis Cluster
docker run -d --name redis-cluster \
  -e IP=0.0.0.0 \
  -p 7000-7005:7000-7005 \
  grokzen/redis-cluster:latest

# Wait for cluster to be ready, then run tests
cargo test --test integration_cluster --features "json,search,cluster" -- --ignored
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_URL` | `redis://localhost:16379` | Redis connection URL |
| `REDIS_PORT` | `16379` | Redis port for CLI commands |
| `REDIS_CLUSTER_NODES` | (none) | Comma-separated cluster node URLs |

## CI Behavior

- **Unit tests**: Always run
- **Integration tests**: Run with GitHub Actions Redis service on port 6379
- **Cluster tests**: Run in separate job with Redis Cluster container
- **Stress tests**: Manual only (not in CI)

## Adding New Tests

1. For unit tests, add `#[test]` functions in `src/` modules
2. For integration tests requiring Redis:
   - Add `#[test]` and `#[ignore]` attributes
   - Use `common::redis_url()` for connection URL
   - Use `common::redis_available()` to skip if Redis unavailable
   - Clean up test data with `common::cleanup_keys()`
