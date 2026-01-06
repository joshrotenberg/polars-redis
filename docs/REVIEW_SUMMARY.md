# polars-redis Project Review Summary

*Review Date: January 2026*
*Updated: v0.1.5 Review*

---

## Project Overview

**polars-redis** is a Rust library (~30k lines) with Python bindings that integrates Redis with the Polars DataFrame library. It enables querying, transforming, and writing Redis data using Polars' expressive API.

**Core Value Proposition:**
> "Query Redis like a database. Transform with Polars. Write back without ETL."

**Current Version:** 0.1.5
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
│  Cluster Support (ClusterKeyScanner)    │  ← NEW in 0.1.5
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
- Cluster-aware key scanning across nodes

---

## Feature Summary

### Reading (8 Data Types)
| Type | Function | Cluster Support | Key Features |
|------|----------|-----------------|--------------|
| Hash | `scan_hashes()` | ✓ | Field projection pushdown, TTL |
| JSON | `scan_json()` | ✓ | RedisJSON, JSONPath support |
| String | `scan_strings()` | ✓ | Configurable value types, TTL |
| Set | `scan_sets()` | ✓ | One row per member |
| List | `scan_lists()` | ✓ | Position tracking |
| Sorted Set | `scan_zsets()` | ✓ | Members with scores, rank |
| Stream | `scan_streams()` | ✓ | Timestamped entries, ID filtering |
| TimeSeries | `scan_timeseries()` | ✓ | Aggregation support |

### Writing (6 Data Types) - Extended in 0.1.5
| Type | Function | Detailed Errors | Key Features |
|------|----------|-----------------|--------------|
| Hash | `write_hashes()` | ✓ | Field mapping, TTL |
| JSON | `write_json()` | ✓ | Document storage |
| String | `write_strings()` | ✓ | Simple values |
| Set | `write_sets()` | ✓ | **NEW** Member collections |
| List | `write_lists()` | ✓ | **NEW** Ordered elements |
| Sorted Set | `write_zsets()` | ✓ | **NEW** Scored members |

### RediSearch Integration - Extended in 0.1.5
| Function | Data Type | Features |
|----------|-----------|----------|
| `search_hashes()` | Hash | Predicate pushdown, sorting |
| `search_json()` | JSON | **NEW** JSONPath queries |
| `aggregate_hashes()` | Hash | Server-side aggregation |
| `aggregate_json()` | JSON | **NEW** JSON aggregation |

### Advanced Features
- **Redis Cluster:** Full support for all data types (NEW)
- **RediSearch Integration:** Query builder with predicate pushdown
- **Parallel Processing:** `ParallelStrategy` for concurrent batch fetching
- **Schema Inference:** Sample-based type detection with confidence scores
- **Per-Key Error Reporting:** Detailed write results with retry support

---

## New in v0.1.5

### 1. Redis Cluster Support

Full cluster support via `ClusterKeyScanner`:

```python
# Cluster-aware scanning
nodes = ["redis://node1:7000", "redis://node2:7001", "redis://node3:7002"]
df = redis.scan_hashes(nodes, "user:*", schema).collect()
```

**Implementation Details:**
- Discovers all master nodes automatically
- Executes SCAN on each node sequentially
- Keys are unique across nodes (no deduplication)
- Data fetching uses cluster routing

### 2. Per-Key Error Reporting

New `WriteResultDetailed` for granular error handling:

```python
result = redis.write_hashes_detailed(df, url, key_prefix="user:")

if not result.is_complete_success():
    for error in result.errors:
        print(f"Failed: {error.key} - {error.error}")

    # Retry failed keys
    failed_keys = result.failed_keys()
```

**Features:**
- `succeeded_keys`: List of successfully written keys
- `errors`: List of `KeyError` with key and error message
- `failed_keys()`: Get list of failed keys for retry
- `error_map()`: Get key → error message mapping

### 3. Schema Inference Confidence Scores

New `infer_*_with_confidence()` functions:

```python
result = redis.infer_hash_schema_with_confidence(url, "user:*", sample_size=100)

for field, info in result.field_info.items():
    if info.confidence < 0.9:
        print(f"Warning: {field} has {info.confidence:.0%} confidence")
        print(f"  Type candidates: {info.type_candidates}")
```

**FieldInferenceInfo includes:**
- `confidence`: 0.0-1.0 score (1.0 = all samples matched)
- `samples`: Total samples for this field
- `valid`: Samples that parsed successfully
- `nulls`: Count of null/missing values
- `type_candidates`: Map of types → match counts

### 4. RediSearch on JSON Documents

New `search_json()` and `aggregate_json()`:

```python
# Search JSON documents with RediSearch
lf = redis.search_json(
    url,
    index="products_idx",
    query=col("price") > 100,
    schema={"name": pl.Utf8, "price": pl.Float64}
)

# Aggregate JSON data server-side
agg = redis.aggregate_json(
    url,
    index="products_idx",
    group_by="category",
    aggregations={"avg_price": ("price", "avg")}
)
```

### 5. Extended Write Support

New write functions for collection types:

```python
# Write sets
redis.write_sets(df, url, key_col="user_id", members_col="tags")

# Write lists
redis.write_lists(df, url, key_col="id", elements_col="items")

# Write sorted sets
redis.write_zsets(df, url, key_col="leaderboard",
                  member_col="player", score_col="score")
```

### 6. Python Package Restructure

Modular imports for better organization:

```python
from polars_redis._scan import scan_hashes, scan_json
from polars_redis._write import write_hashes, WriteResult
from polars_redis._search import search_hashes, aggregate_hashes
from polars_redis._infer import infer_hash_schema_with_confidence
```

---

## Technical Review Findings

### Schema Inference
- **Default sample size:** 100 keys (user-configurable)
- Returns `sample_count` for transparency
- `with_overwrite()` method for type corrections without re-scanning
- **NEW:** Confidence scores with detailed type candidate analysis

### Error Handling in Writes
- ~~Batch-level granularity~~ **NOW:** Per-key error reporting available
- `write_*_detailed()` functions return `WriteResultDetailed`
- Enables retry logic for partial failures
- Original batch-level functions still available for simplicity

### Cluster Support
- ~~Feature flag exists but not yet implemented~~ **NOW:** Fully implemented
- All 8 data types support cluster mode
- `ClusterKeyScanner` handles multi-node SCAN
- Automatic master node discovery

### Backpressure / Memory Management
- Bounded mpsc channels (default size: 16)
- Natural flow control when consumer is slow
- Configurable via `ParallelConfig::with_channel_size()`

---

## New Documentation

| Document | Description |
|----------|-------------|
| `docs/guide/cluster.md` | Redis Cluster setup and usage |
| `docs/guide/performance.md` | Batch tuning, parallel fetching, benchmarks |
| `docs/guide/use-cases.md` | Customer enrichment, leaderboards, streams |
| `docs/guide/ephemeral-workbench.md` | Testing and development setup |
| `docs/guide/redisearch.md` | Query builder, FT.SEARCH, FT.AGGREGATE |

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
- **NEW:** Running on Redis Cluster

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

**Problem:** Feature stores optimize for single-key lookup during inference, but struggle with batch inference, backfilling, and feature analysis.

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

### 5. Leaderboard Analytics (NEW)

**Problem:** Gaming/competition leaderboards in sorted sets need cross-leaderboard analysis.

**polars-redis solution:**
```python
lf = redis.scan_zsets(url, "leaderboard:*", include_rank=True)
top_players = (
    lf.group_by("member")
    .agg([pl.col("score").sum(), pl.col("rank").mean()])
    .sort("score", descending=True)
    .head(100)
    .collect()
)
```

### 6. Event Stream Processing (NEW)

**Problem:** Redis Streams hold event data that needs time-windowed aggregation.

**polars-redis solution:**
```python
events = redis.scan_streams(url, "events:*",
                            fields=["action", "user_id"],
                            start_id=f"{yesterday_ms}-0")
hourly = events.group_by(pl.col("_ts").dt.hour()).agg(pl.count())
```

---

## Embedded Query Engine Positioning

polars-redis fits the emerging "embedded analytics" pattern alongside DuckDB and Polars:

| Engine | Data Source | Interface | Cluster |
|--------|-------------|-----------|---------|
| DuckDB | Files (Parquet, CSV) | SQL | No |
| Polars | Files (Parquet, CSV) | DataFrame | No |
| **polars-redis** | Redis (live state) | DataFrame | **Yes** |

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
│   Parquet     │   CSV/JSON    │   Redis / Cluster       │
└───────────────┴───────────────┴─────────────────────────┘
```

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
| **Horizontal scale** | **No** | **Yes (Cluster)** |

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

## Resolved Items (from v0.1.4 review)

| Item | Status | Implementation |
|------|--------|----------------|
| Cluster Support | ✅ Resolved | `ClusterKeyScanner`, cluster iterators for all types |
| Error Granularity | ✅ Resolved | `WriteResultDetailed` with per-key errors |
| Schema Confidence | ✅ Resolved | `FieldInferenceInfo` with confidence scores |
| Performance Guide | ✅ Resolved | `docs/guide/performance.md` |

## Remaining Open Items

1. **RediSearch on Cluster:** Verify FT.SEARCH behavior across cluster nodes
2. **Write Cluster Support:** Write operations on cluster (may need slot-aware batching)
3. **Streaming Writes:** Large dataset writes with backpressure

---

## Summary Statement

> **polars-redis** positions itself as an embedded query engine for Redis - not a database replacement, but a DataFrame interface to live operational state. It fills the gap between Redis point lookups and full data warehouse capabilities, enabling analytics on data that's already in Redis without additional infrastructure.

The project has matured significantly from v0.1.4 to v0.1.5, resolving all previously identified gaps:
- **Cluster support** is now production-ready
- **Per-key error reporting** enables robust retry logic
- **Schema confidence scores** provide type inference transparency
- **Comprehensive documentation** covers performance tuning and use cases

At v0.1.5, polars-redis is production-ready for both single-instance and clustered Redis deployments.
