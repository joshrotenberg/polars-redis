# polars-redis

A Polars IO plugin for Redis.

## Completed

### Core Features
- [x] `scan_hashes()` / `read_hashes()` - LazyFrame/DataFrame from Redis hashes
- [x] `scan_json()` / `read_json()` - LazyFrame/DataFrame from RedisJSON documents
- [x] `scan_strings()` / `read_strings()` - LazyFrame/DataFrame from Redis strings
- [x] `scan_sets()` / `read_sets()` - LazyFrame/DataFrame from Redis sets
- [x] `scan_lists()` / `read_lists()` - LazyFrame/DataFrame from Redis lists
- [x] `scan_zsets()` / `read_zsets()` - LazyFrame/DataFrame from Redis sorted sets
- [x] `write_hashes()` / `write_json()` / `write_strings()` - Write DataFrames to Redis
- [x] `write_sets()` / `write_lists()` / `write_zsets()` - Write DataFrames to Redis
- [x] `infer_hash_schema()` / `infer_json_schema()` - Schema inference from samples

### Optimizations
- [x] Projection pushdown (HMGET vs HGETALL, JSON.GET with paths)
- [x] n_rows pushdown (`.head()` / `.limit()` stops iteration early)
- [x] Batched iteration with configurable batch_size and count_hint
- [x] Pipelined writes for performance

### Options
- [x] Write modes: fail, replace, append
- [x] TTL support on write
- [x] Key prefix support
- [x] Metadata columns: key, TTL, row index
- [x] Configurable column names

### Infrastructure
- [x] CI/CD pipeline (GitHub Actions)
- [x] 114 Rust unit tests
- [x] 50+ Python integration tests
- [x] Documentation site (MkDocs Material)
- [x] Python and Rust examples

## Todo

### Phase 6: Additional Data Types
- [ ] Redis Streams (`scan_stream`) - event logs, CDC
- [ ] RedisTimeSeries (`scan_timeseries`) - metrics, sensor data

### Phase 7: Advanced Features
- [ ] RediSearch predicate pushdown
- [ ] Connection pooling
- [ ] Cluster support

### Phase 8: Release
- [ ] PyPI release
- [ ] crates.io release
- [ ] awesome-polars submission
