# polars-redis Hardening Roadmap

*Created: January 2026*
*Version: 0.1.6*

---

## Project Evolution

polars-redis has evolved from a simple Redis-to-DataFrame connector into a multi-faceted tool with three distinct value layers:

### 1. DataFrame Engine (Core)
Query, transform, and aggregate Redis data using Polars:
- 8 Redis data types supported (Hash, JSON, String, Set, List, ZSet, Stream, TimeSeries)
- Cluster support for all types
- Projection pushdown, batched iteration, parallel fetching

### 2. ETL / Streaming Platform
- DataFrame caching with Arrow IPC / Parquet
- `@cached` decorator for distributed memoization
- Pub/Sub and Streams consumption into DataFrames
- Smart scan with automatic index detection

### 3. Semantic Redis Client
- RediSearch with Polars-like query builder
- Typed Index management (TextField, NumericField, TagField)
- Query plan explanation

---

## Target Use Case: Content-Based Routing Pipeline

A representative production use case involves:

```
Source Redis Cluster(s)
    │
    ▼ (Streams with change events)
┌─────────────────────────────────┐
│  polars-redis Processing Tier   │
│  - Consume from Streams         │
│  - Apply routing rules (Polars) │
│  - Track offsets                │
└─────────────────────────────────┘
    │
    ├──▶ Destination Cluster A (filtered subset)
    ├──▶ Destination Cluster B (filtered subset)
    └──▶ Analytics Cluster (everything)
```

**Key requirements:**
- Reliable stream consumption with resume capability
- Content-based filtering using Polars expressions
- Multi-cluster writes with error handling
- Offset tracking for CDC-like semantics
- Idempotency support for retries

---

## Current State Assessment

### Testing Coverage

| Category | Files | Lines | CI Status |
|----------|-------|-------|-----------|
| Rust unit tests | `src/**/*.rs` | Inline | ✅ Runs |
| Rust integration | `tests/integration_*.rs` | ~4,700 | ✅ Runs |
| Rust stress tests | `tests/stress_tests.rs` | ~740 | ⚠️ Manual only |
| Python tests | `tests/test_*.py` | ~7,300 | ✅ Runs |

### Feature Coverage by Test Depth

| Feature | Unit | Integration | Stress | Failure Injection |
|---------|------|-------------|--------|-------------------|
| Hash scan/write | ✅ | ✅ | ✅ | ❌ |
| JSON scan/write | ✅ | ✅ | ✅ | ❌ |
| RediSearch | ✅ | ✅ | ⚠️ | ❌ |
| Cluster | ✅ | ✅ | ❌ | ❌ |
| Caching | ✅ | ✅ | ❌ | ❌ |
| Pub/Sub | ✅ | ✅ | ❌ | ❌ |
| Streams | ✅ | ✅ | ❌ | ❌ |
| Smart scan | ✅ | ✅ | ❌ | ❌ |

---

## Hardening Tracks

### Track 1: Stream Reliability (P0)

**Goal:** Ensure Streams consumption is production-ready for CDC-like workloads.

#### 1.1 Consumer Group Semantics
- [ ] Test XREADGROUP with multiple consumers
- [ ] Verify XACK only after successful processing
- [ ] Test pending entry recovery (XPENDING, XCLAIM)
- [ ] Document consumer group best practices

#### 1.2 Offset Management
- [ ] Test resume from specific stream ID
- [ ] Verify no message loss on restart
- [ ] Test behavior when stream is trimmed (XTRIM)
- [ ] Consider built-in checkpoint helpers

#### 1.3 Failure Scenarios
- [ ] Connection drop mid-read
- [ ] Redis restart during consumption
- [ ] Consumer group rebalancing

### Track 2: Write Reliability (P0)

**Goal:** Ensure writes are reliable with proper error handling for production retry logic.

#### 2.1 Per-Key Error Handling
- [x] `WriteResultDetailed` with per-key errors ✅ Implemented
- [x] `failed_keys()` and `error_map()` helpers ✅ Implemented
- [ ] Integration tests with actual Redis errors (WRONGTYPE, OOM)
- [ ] Document retry patterns

#### 2.2 Partial Failure Recovery
- [ ] Test batch where some keys fail
- [ ] Verify successful keys are not retried
- [ ] Test pipeline failure vs individual key failure

#### 2.3 Multi-Cluster Writes
- [ ] Document patterns for writing to multiple clusters
- [ ] Consider transaction-like semantics (best-effort with reconciliation)
- [ ] Test connection failure to one cluster mid-pipeline

### Track 3: Memory & Performance Bounds (P1)

**Goal:** Ensure memory efficiency and predictable performance at scale.

#### 3.1 Large Batch Processing
- [ ] Test 10k, 100k, 1M row batches
- [ ] Verify memory stays bounded during iteration
- [ ] Profile memory usage in CI (not just manual)

#### 3.2 Caching Layer
- [ ] Test large DataFrame chunking (>512MB)
- [ ] Verify chunk reassembly on read
- [ ] Test concurrent read/write to same cache key
- [ ] Test Redis OOM response handling

#### 3.3 Parallel Fetching
- [ ] Test ParallelStrategy under contention
- [ ] Verify backpressure when consumer is slow
- [ ] Measure throughput vs worker count

### Track 4: Connection Resilience (P1)

**Goal:** Handle network and Redis failures gracefully.

#### 4.1 Connection Failures
- [ ] Test connection drop mid-operation
- [ ] Verify connection pool recovery
- [ ] Test timeout handling

#### 4.2 Cluster Resilience
- [ ] Test node failure during scan
- [ ] Test slot migration handling
- [ ] Test MOVED/ASK redirect handling

### Track 5: API Completeness (P2)

**Goal:** Fill gaps in the API surface.

#### 5.1 Missing Operations
- [ ] `delete_keys()` function for bulk deletes
- [ ] `exists_keys()` for existence checks
- [ ] `ttl_keys()` for TTL inspection

#### 5.2 Convenience Helpers
- [ ] Stream offset checkpoint helpers
- [ ] Retry decorator with exponential backoff
- [ ] Multi-URL write coordinator

---

## Testing Infrastructure Improvements

### CI Enhancements
- [ ] Add memory limit tests (container with constrained memory)
- [ ] Add Python integration tests to CI (if not already)
- [ ] Add periodic stress test runs (nightly/weekly)
- [ ] Add benchmark regression tracking

### Test Utilities
- [ ] Failure injection helpers (connection drops, timeouts)
- [ ] Redis error simulation (OOM, WRONGTYPE)
- [ ] Multi-cluster test fixtures

---

## Documentation Gaps

### Production Patterns
- [ ] CDC-like pipeline architecture guide
- [ ] Retry and idempotency patterns
- [ ] Multi-cluster write coordination
- [ ] Offset tracking best practices

### Troubleshooting
- [ ] Common error scenarios and solutions
- [ ] Performance tuning guide (exists, needs expansion)
- [ ] Memory troubleshooting

---

## Success Criteria

### Stream Reliability
- Consumer can restart and resume from exact position
- No message loss under normal operation
- Graceful handling of connection failures

### Write Reliability
- Per-key error reporting enables targeted retries
- Partial batch failures don't corrupt state
- Clear documentation of failure modes

### Performance
- Memory usage bounded regardless of dataset size
- Throughput scales with parallel workers
- No performance regression between releases

---

## Priority Summary

| Priority | Track | Key Deliverables |
|----------|-------|------------------|
| **P0** | Stream Reliability | Consumer group tests, offset resume, failure scenarios |
| **P0** | Write Reliability | Error injection tests, retry pattern docs |
| **P1** | Memory & Performance | Large batch tests in CI, caching stress tests |
| **P1** | Connection Resilience | Failure injection, cluster failover tests |
| **P2** | API Completeness | `delete_keys()`, checkpoint helpers |

---

## Positioning Statement

> **polars-redis** is an embedded query engine and data pipeline building block for Redis. It enables DataFrame-style analytics on Redis data and can serve as the processing tier in content-based routing and filtered replication architectures. While it does not replace native Redis replication or provide durable CDC guarantees, it offers a flexible, high-performance foundation for custom data pipelines.

**Best suited for:**
- Batch and micro-batch processing (not per-write inline routing)
- Content-based filtering and analytics
- Multi-cluster data distribution with application-level control
- Caching intermediate computation results

**Not a replacement for:**
- Native Redis replication
- Durable CDC systems (Kafka, Debezium)
- Per-write routing with single-digit ms SLAs
