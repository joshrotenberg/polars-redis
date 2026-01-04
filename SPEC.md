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
- [x] `scan_streams()` / `read_streams()` - LazyFrame/DataFrame from Redis Streams
- [x] `scan_timeseries()` / `read_timeseries()` - LazyFrame/DataFrame from RedisTimeSeries
- [x] `write_hashes()` / `write_json()` / `write_strings()` - Write DataFrames to Redis
- [x] `write_sets()` / `write_lists()` / `write_zsets()` - Write DataFrames to Redis
- [x] `infer_hash_schema()` / `infer_json_schema()` - Schema inference from samples
- [x] `search_hashes()` - RediSearch FT.SEARCH for server-side filtering

### Optimizations
- [x] Projection pushdown (HMGET vs HGETALL, JSON.GET with paths)
- [x] n_rows pushdown (`.head()` / `.limit()` stops iteration early)
- [x] Batched iteration with configurable batch_size and count_hint
- [x] Pipelined writes for performance
- [x] Connection pooling via ConnectionManager (auto-reconnect)
- [x] RediSearch predicate pushdown (FT.SEARCH with LIMIT pagination)

### Options
- [x] Write modes: fail, replace, append
- [x] TTL support on write
- [x] Key prefix support
- [x] Metadata columns: key, TTL, row index
- [x] Configurable column names

### Infrastructure
- [x] CI/CD pipeline (GitHub Actions)
- [x] 145 Rust unit tests
- [x] 50+ Python integration tests
- [x] Documentation site (MkDocs Material)
- [x] Python and Rust examples

## Todo

### Phase 7: Advanced Features
- [x] RediSearch predicate pushdown (FT.SEARCH)
- [x] Connection pooling
- [ ] Cluster support
- [ ] RediSearch aggregation (FT.AGGREGATE)

### Phase 8: Release
- [ ] PyPI release
- [ ] crates.io release
- [ ] awesome-polars submission

## Research & Improvements

### Polars IO Study (tmp/polars) - COMPLETED

Key patterns learned from polars-io:

**1. Options Pattern**
- Separate `*ReadOptions` structs (e.g., `CsvReadOptions`, `ParquetOptions`)
- Builder pattern with `with_*` methods for fluent configuration
- `Default` impl with sensible defaults
- Serde support for serialization

**2. Reader Pattern**
- `SerReader<R>` trait with `new(reader: R)` and `finish() -> DataFrame`
- `SerWriter<W>` trait for writing
- Separate `CoreReader` for low-level implementation
- `ArrowReader` trait for batch iteration: `next_record_batch() -> Option<RecordBatch>`

**3. Schema Inference**
- `infer_schema_length` option (default ~100 samples)
- `schema_overwrite` for user-provided schema hints
- Deduplicate header names automatically
- Type inference with regex patterns for int/float/bool

**4. Predicate Pushdown**
- `PhysicalIoExpr` trait: `evaluate_io(&DataFrame) -> Series` (bool mask)
- `ColumnPredicateExpr` for column-specific predicates
- `SpecializedColumnPredicate` enum: Equal, Between, StartsWith, etc.
- Parquet uses row group statistics for filtering

**5. Row Index**
- `RowIndex { name, offset }` struct - consistent across all readers
- Added via `with_row_index()` method

**6. Parallel Strategies**
- `ParallelStrategy` enum: None, Columns, RowGroups, Prefiltered, Auto
- Concurrency tuning with semaphores
- Chunk size configuration via environment variables

**7. Cloud/Async**
- Tokio runtime management in `pl_async.rs`
- `CloudOptions` for credentials/retries
- Download chunk size tuning
- Semaphore-based concurrency budget

**Applicable improvements for polars-redis:**
- [ ] Consider `RedisReadOptions` struct instead of many function params
- [ ] Add `schema_overwrite` for partial schema hints
- [ ] Implement `PhysicalIoExpr` for RediSearch query translation
- [ ] Add parallel strategy option (keys vs batches)
- [ ] Environment variable config (batch size, timeout, etc.)

### Redis Cluster Support (tmp/redis-rs)
Deep dive into redis-rs cluster implementation:
- [ ] Study ClusterClient and ClusterConnection API
- [ ] Understand SCAN behavior across cluster nodes
- [ ] Review slot-aware routing patterns
- [ ] Examine connection management for clusters
- [ ] Plan cluster-aware iterator design

### RediSearch Aggregation (FT.AGGREGATE)
Server-side aggregation pipeline - complement to FT.SEARCH:
```
FT.AGGREGATE users_idx "*"
  GROUPBY 1 @city
  REDUCE COUNT 0 AS user_count
  REDUCE AVG 1 @age AS avg_age
  SORTBY 2 @user_count DESC
  LIMIT 0 10
```

**Implementation plan:**
- [ ] `AggregateConfig` struct (index, query, groupby, reduce, sortby, limit)
- [ ] `aggregate_hashes()` async function with response parsing
- [ ] `aggregate_hashes()` Python API
- [ ] Support REDUCE operations: COUNT, SUM, AVG, MIN, MAX, etc.
- [ ] Support APPLY for computed fields
- [ ] Integration tests

### Rust Integration Tests
Currently: 145 unit tests (in-module), Python integration tests
Missing: Rust integration tests against real Redis

**Plan:**
- [ ] Create `tests/` directory with integration tests
- [ ] Test full Rust API without Python layer
- [ ] Use `#[ignore]` or feature flag for CI (requires Redis)
- [ ] Cover: connection, scan, search, write, schema inference
- [ ] Test error conditions (connection failure, invalid data)
- [ ] Consider testcontainers-rs for local Redis in tests

### Testing Improvements
- [ ] Edge case coverage (empty results, malformed data, connection failures)
- [ ] Property-based testing with proptest/quickcheck
- [ ] Benchmark suite for performance regression testing
- [ ] Stress testing with large datasets
- [ ] Concurrent access testing

### Examples & Documentation
- [ ] Complete examples for all data types
- [ ] RediSearch examples with index creation
- [ ] Real-world use case examples (analytics, caching, ETL)
- [ ] Performance tuning guide
- [ ] Migration guide from other Redis clients
- [ ] API reference completeness check
