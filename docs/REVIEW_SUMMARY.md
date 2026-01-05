# polars-redis Project Review Summary

*Review Date: January 2026*

---

## Project Overview

**polars-redis** is a Rust library (~30k lines) with Python bindings that integrates Redis with the Polars DataFrame library. It enables querying, transforming, and writing Redis data using Polars' expressive API.

**Core Value Proposition:**
> "Query Redis like a database. Transform with Polars. Write back without ETL."

**Current Version:** 0.1.4
**License:** MIT OR Apache-2.0

---

## Architecture Highlights

```
┌─────────────────────────────────────────┐
│     Python/Rust API Layer (PyO3)        │
├─────────────────────────────────────────┤
│  Type Iterators (Hash, JSON, Set, etc)  │
├─────────────────────────────────────────┤
│   Schema, Inference, Conversion         │
├─────────────────────────────────────────┤
│  Connection, Parallel, RediSearch       │
├─────────────────────────────────────────┤
│    Arrow RecordBatches (IPC Format)     │
├─────────────────────────────────────────┤
│          Redis Client (redis-rs)        │
└─────────────────────────────────────────┘
```

**Key Design Principles:**
- Async-first (Tokio runtime)
- Memory-efficient batched iteration
- Zero-copy Python interop via Arrow IPC
- Strong typing through Rust's type system

---

## Feature Summary

### Reading (8 Data Types)
| Type | Function | Key Features |
|------|----------|--------------|
| Hash | `scan_hashes()` | Field projection pushdown, TTL |
| JSON | `scan_json()` | RedisJSON, JSONPath support |
| String | `scan_strings()` | Configurable value types |
| Set | `scan_sets()` | One row per member |
| List | `scan_lists()` | Position tracking |
| Sorted Set | `scan_zsets()` | Members with scores |
| Stream | `scan_streams()` | Timestamped entries |
| TimeSeries | `scan_timeseries()` | Aggregation support |

### Writing (6 Data Types)
- Conflict modes: `fail`, `replace`, `append`
- TTL support per key
- Batched pipelining for performance

### Advanced Features
- **RediSearch Integration:** Query builder with predicate pushdown
- **Parallel Processing:** `ParallelStrategy` for concurrent batch fetching
- **Schema Inference:** Sample-based type detection with override support

---

## Technical Review Findings

### Schema Inference
- **Default sample size:** 100 keys (user-configurable)
- Returns `sample_count` for transparency
- `with_overwrite()` method for type corrections without re-scanning

### Error Handling in Writes
- Batch-level granularity (entire batch succeeds or fails)
- Processing continues to next batch on failure
- Trade-off: Simpler/faster, but no partial success within batch

### Cluster Support
- Feature flag exists but not yet implemented
- Current target: Single-instance Redis / Redis Stack
- Architecture should translate well to cluster when implemented

### Backpressure / Memory Management
- Bounded mpsc channels (default size: 16)
- Natural flow control when consumer is slow
- Configurable via `ParallelConfig::with_channel_size()`

---

## Competitive Positioning

### The Sweet Spot

polars-redis is ideal when:
- Data already lives in Redis (or should)
- Need more than key-value lookups
- Need less than a full data warehouse
- Multiple processes need shared access
- Low-latency requirements (sub-100ms)
- DataFrame-style queries needed

### What It's NOT Competing With
- **Spark:** Petabyte scale, complex DAGs
- **Snowflake:** SQL interface, BI tools, historical queries
- **Kafka:** Event streaming, replay, exactly-once semantics

### What It IS Competing With
- Custom Python scripts that SCAN Redis and build DataFrames
- "Just dump it to Postgres" when Redis is already present
- Feature store SDKs for batch operations
- RediSearch for complex queries (with better ergonomics)

---

## Identified Use Cases

### 1. Feature Store "Last Mile"

**Problem:** Feature stores (DoorDash, Tubi, Swiggy) optimize for single-key lookup during inference, but struggle with batch inference, backfilling, and feature analysis.

**polars-redis solution:**
```python
# Batch inference on all active users
users = redis.scan_hashes(url, "user:*", schema)
features = users.filter(pl.col("active")).select(feature_cols).collect()
predictions = model.predict(features)
redis.write_hashes(predictions, url, key_prefix="prediction:", ttl=3600)
```

### 2. Kafka State Analytics

**Problem:** Kafka → Redis pipelines create materialized views, but analytics requires exporting to a warehouse.

**polars-redis solution:** Query the live state directly with full Polars expressiveness.

### 3. Medium Data Shared State

**Problem:** Spark is overkill for 10-100GB, but Parquet files lack multi-process coordination and TTL.

**polars-redis solution:** Shared, mutable, TTL-enabled state with DataFrame queries.

### 4. RDI Analytics Complement

**Problem:** Redis Data Integration syncs data INTO Redis, but analytics on that data requires export.

**polars-redis solution:** Analytics layer on RDI-synced data without additional infrastructure.

---

## Embedded Query Engine Positioning

polars-redis fits the emerging "embedded analytics" pattern alongside DuckDB and Polars:

| Engine | Data Source | Interface |
|--------|-------------|-----------|
| DuckDB | Files (Parquet, CSV) | SQL |
| Polars | Files (Parquet, CSV) | DataFrame |
| **polars-redis** | Redis (live state) | DataFrame |

### Composable Analytics Vision

```
┌─────────────────────────────────────────────────────────┐
│                   Your Application                       │
├───────────────┬───────────────┬─────────────────────────┤
│    Polars     │    DuckDB     │     polars-redis        │
│   (files)     │    (SQL)      │    (live state)         │
├───────────────┴───────────────┴─────────────────────────┤
│                  Arrow Memory Format                     │
├───────────────┬───────────────┬─────────────────────────┤
│   Parquet     │   CSV/JSON    │        Redis            │
└───────────────┴───────────────┴─────────────────────────┘
```

All engines can interoperate via Arrow, enabling:
- Scan Redis → Join with Parquet → Write back to Redis
- Combined queries across live and historical data

---

## Comparison: Data at Rest vs. Data in Motion

| Aspect | DuckDB/Polars + Files | polars-redis |
|--------|----------------------|--------------|
| Data freshness | Batch (minutes/hours) | Real-time |
| Multi-writer | File locking issues | Redis handles it |
| TTL/Expiry | Manual cleanup | Built-in |
| Point lookups | Full scan required | Redis native |
| Complex analytics | Full SQL/DataFrame | Full Polars |
| Infrastructure | Just files | Redis required |

---

## Reference Case Studies

### Redis Ecosystem
- [DoorDash Feature Store](https://careersatdoordash.com/blog/building-a-gigascale-ml-feature-store-with-redis/)
- [Tubi Redis Optimization](https://code.tubitv.com/optimizing-latency-in-redis-online-feature-store-a-case-study-1fd6a611bafa)
- [Redis + Kafka Time Series](https://redis.io/blog/processing-time-series-data-with-redis-and-apache-kafka/)
- [Redis Data Integration](https://redis.io/data-integration/)

### Polars Ecosystem
- [Double River Investments Case Study](https://pola.rs/posts/case-double-river/)
- [Spark to Polars Migration](https://dataengineeringcentral.substack.com/p/replace-databricks-spark-jobs-using)

### Embedded Analytics
- [Rill Data + DuckDB](https://www.rilldata.com/blog/why-we-built-rill-with-duckdb)
- [Composable Query Engines](https://thinhdanggroup.github.io/composable-query-engines-with-polars-and-datafusion/)

---

## Open Items

1. **Cluster Support:** Redis Cluster implementation pending
2. **Error Granularity:** Consider per-key error reporting for writes
3. **Schema Confidence:** Potential enhancement for type inference confidence scores
4. **Performance Guide:** Documentation gap for tuning recommendations

---

## Summary Statement

> **polars-redis** positions itself as an embedded query engine for Redis - not a database replacement, but a DataFrame interface to live operational state. It fills the gap between Redis point lookups and full data warehouse capabilities, enabling analytics on data that's already in Redis without additional infrastructure.

The project demonstrates strong engineering fundamentals with comprehensive async support, memory-efficient batching, and a clean API design. At v0.1.4, it's production-ready for single-instance Redis deployments with cluster support as a natural next step.
