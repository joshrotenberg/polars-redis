# polars-redis Specification

## Overview

polars-redis is a Polars IO plugin that enables scanning Redis data structures (hashes, JSON documents) as LazyFrames with support for projection pushdown, predicate filtering, and batched iteration.

**Positioning**: The first Redis plugin in the Polars ecosystem, filling a gap in the Import/Export category of [awesome-polars](https://github.com/ddotta/awesome-polars).

**Value Proposition**: "Bring your Redis operational data into Polars analytics pipelines without writing ETL"

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Python API                               │
│  scan_hashes() / scan_json() / read_hashes() / write_*()    │
└─────────────────────────┬───────────────────────────────────┘
                          │ register_io_source
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   Polars Query Engine                        │
│              (projection/predicate/n_rows pushdown)          │
└─────────────────────────┬───────────────────────────────────┘
                          │ with_columns, predicate, n_rows
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                    Rust Core (PyO3)                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │ HashBatch    │  │ JsonBatch    │  │ StreamBatch  │       │
│  │ Iterator     │  │ Iterator     │  │ Iterator     │       │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘       │
│         └─────────────────┼─────────────────┘               │
│                           ▼                                  │
│  ┌─────────────────────────────────────────────────────────┐│
│  │              Arrow RecordBatch Builder                  ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────┬───────────────────────────────────┘
                          │ Arrow IPC bytes
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                     Redis (redis-rs)                         │
│         SCAN + HGETALL/HMGET/JSON.GET + Pipeline            │
└─────────────────────────────────────────────────────────────┘
```

---

## Current Status

### Implemented (Phases 1-3)

| Feature | Status | Notes |
|---------|--------|-------|
| `scan_hashes()` | ✅ Done | LazyFrame from Redis hashes |
| `scan_json()` | ✅ Done | LazyFrame from RedisJSON documents |
| `read_hashes()` / `read_json()` | ✅ Done | Eager versions (call `.collect()`) |
| `write_hashes()` / `write_json()` | ✅ Done | Write DataFrames to Redis |
| `infer_hash_schema()` / `infer_json_schema()` | ✅ Done | Sample keys to detect schema |
| Projection pushdown | ✅ Done | HMGET vs HGETALL, JSON.GET with paths |
| n_rows pushdown | ✅ Done | `.head()` / `.limit()` stops iteration early |
| Client-side predicate filtering | ✅ Done | Filter applied after fetch |
| Schema-based type conversion | ✅ Done | Utf8, Int64, Float64, Boolean |
| Null/missing field handling | ✅ Done | Missing fields become null |
| Batched iteration | ✅ Done | Configurable batch_size and count_hint |
| Include/exclude key column | ✅ Done | `include_key`, `key_column_name` options |
| TTL column support | ✅ Done | `include_ttl`, `ttl_column_name` options |
| Row index column support | ✅ Done | `include_row_index`, `row_index_column_name` options |
| Python API (`register_io_source`) | ✅ Done | Full LazyFrame integration |
| `scan_strings()` / `read_strings()` | ✅ Done | LazyFrame/DataFrame from Redis strings |
| `write_strings()` | ✅ Done | Write DataFrames as Redis strings |
| Write modes (fail/replace/append) | ✅ Done | `if_exists` parameter on write functions |
| TTL on write | ✅ Done | `ttl` parameter on write functions |
| Key prefix support | ✅ Done | `key_prefix` parameter on write functions |
| Integration tests | ✅ Done | 50+ Python tests, 82 Rust tests |
| CI/CD pipeline | ✅ Done | GitHub Actions with Redis service |
| Rust examples | ✅ Done | IPC serialization, projection examples |
| Python examples | ✅ Done | 9 comprehensive examples |

### Not Yet Implemented

| Feature | Priority | Phase |
|---------|----------|-------|
| Batch pipelining for writes | Medium | 3 |
| Key generation from row index | Low | 3 |
| Sorted Sets (`scan_zset`) | Medium | 4 |
| Redis Streams (`scan_stream`) | Medium | 4 |
| RedisTimeSeries support | High | 4 |
| RediSearch predicate pushdown | Medium | 5 |
| Connection pooling | Low | 5 |
| Cluster support | Low | 5 |

---

## API Reference

### Phase 1 (Current)

#### `scan_hashes()`

```python
def scan_hashes(
    url: str,
    pattern: str = "*",
    schema: dict[str, pl.DataType],
    *,
    include_key: bool = True,
    key_column_name: str = "_key",
    batch_size: int = 1000,
    count_hint: int = 100,
) -> pl.LazyFrame
```

**Parameters:**
- `url`: Redis connection URL (`redis://`, `rediss://` for TLS)
- `pattern`: Key pattern for SCAN (e.g., `user:*`, `session:????`)
- `schema`: Dict mapping field names to Polars types
- `include_key`: Whether to include Redis key as a column
- `key_column_name`: Name of the key column (default: `_key`)
- `batch_size`: Number of keys to fetch per batch
- `count_hint`: SCAN COUNT hint (affects iteration chunk size)

**Behavior:**
- Returns LazyFrame immediately (no Redis calls until `.collect()`)
- On `.collect()`: SCAN keys → batch HGETALL/HMGET → Arrow → Polars
- Projection pushdown: `.select(["name", "age"])` uses HMGET not HGETALL
- n_rows pushdown: `.head(10)` stops iteration after 10 rows

**Supported Types:**

| Polars Type | Redis String Parsing |
|-------------|---------------------|
| `pl.Utf8` / `pl.String` | Pass-through |
| `pl.Int64` | `str.parse::<i64>()` |
| `pl.Float64` | `str.parse::<f64>()` |
| `pl.Boolean` | `true/false/1/0/yes/no/t/f/y/n` |
| `pl.Date` | ISO 8601 date (`YYYY-MM-DD`) or epoch days |
| `pl.Datetime` | ISO 8601 datetime or Unix timestamp (s/ms/us) |

#### `scan_json()`

```python
def scan_json(
    url: str,
    pattern: str = "*",
    schema: dict[str, pl.DataType],
    *,
    include_key: bool = True,
    key_column_name: str = "_key",
    batch_size: int = 1000,
    count_hint: int = 100,
) -> pl.LazyFrame
```

**Behavior:**
- Uses `JSON.GET key $` for full document fetch
- Projection pushdown: `JSON.GET key $.field1 $.field2`
- Handles both response formats:
  - Array-wrapped: `[{"name": "Alice"}]`
  - Path-keyed: `{"$.name": ["Alice"], "$.age": [30]}`

---

### Phase 2 (Planned)

#### Eager Read Functions

```python
def read_hashes(
    url: str,
    pattern: str = "*",
    schema: dict[str, pl.DataType] | None = None,
    **kwargs,
) -> pl.DataFrame
```

Implementation: `scan_hashes(...).collect()`

If `schema=None`, calls `infer_hash_schema()` first.

#### Schema Inference

```python
def infer_hash_schema(
    url: str,
    pattern: str = "*",
    sample_size: int = 100,
    type_inference: bool = True,
) -> dict[str, pl.DataType]

def infer_json_schema(
    url: str,
    pattern: str = "*",
    sample_size: int = 100,
) -> dict[str, pl.DataType]
```

**Algorithm:**
1. SCAN up to `sample_size` keys matching pattern
2. Fetch all fields from sampled keys (HGETALL / JSON.GET)
3. Union all field names seen across samples
4. For each field, attempt type inference:
   - All non-null values parse as int → `pl.Int64`
   - All non-null values parse as float → `pl.Float64`
   - All non-null values are boolean strings → `pl.Boolean`
   - Otherwise → `pl.Utf8`
5. Return schema dict

#### Metadata Columns

```python
def scan_hashes(
    ...,
    ttl_column: str | None = None,       # TTL in seconds (-1 = no expiry)
    memory_column: str | None = None,    # MEMORY USAGE in bytes
) -> pl.LazyFrame
```

---

### Phase 3 (Planned)

#### Write Functions

```python
def write_hashes(
    df: pl.DataFrame,
    url: str,
    *,
    key_column: str = "_key",
    key_prefix: str = "",
    if_exists: Literal["fail", "replace", "append"] = "replace",
    ttl: int | None = None,
    batch_size: int = 1000,
) -> int  # Returns rows written

def write_json(
    df: pl.DataFrame,
    url: str,
    *,
    key_column: str = "_key",
    key_prefix: str = "",
    if_exists: Literal["fail", "replace", "append"] = "replace",
    ttl: int | None = None,
    batch_size: int = 1000,
) -> int
```

**Key Generation Strategies:**

```python
# Strategy 1: Existing column as key
df = pl.DataFrame({"_key": ["user:1", "user:2"], "name": ["Alice", "Bob"]})
write_hashes(df, url, key_column="_key")
# Result: HSET user:1 name Alice; HSET user:2 name Bob

# Strategy 2: Prefix + column value
df = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
write_hashes(df, url, key_column="id", key_prefix="user:")
# Result: HSET user:1 name Alice; HSET user:2 name Bob

# Strategy 3: Row index (auto-generated)
df = pl.DataFrame({"name": ["Alice", "Bob"]})
write_hashes(df, url, key_column=None, key_prefix="user:")
# Result: HSET user:0 name Alice; HSET user:1 name Bob
```

**Write Modes:**
- `fail`: Error if any key already exists
- `replace`: Overwrite existing keys (DEL + HSET)
- `append`: Add/update fields in existing hashes (HSET only)

---

### Phase 4 (Planned)

#### RediSearch Predicate Pushdown

```python
def scan_hashes(
    ...,
    search_index: str | None = None,  # RediSearch index name
) -> pl.LazyFrame
```

When `search_index` is provided AND a filter is applied:

```python
lf = scan_hashes(
    url,
    pattern="user:*",
    schema={"name": pl.Utf8, "age": pl.Int64},
    search_index="idx:users",
)

# This filter gets pushed down to RediSearch
result = lf.filter(pl.col("age") > 30).collect()
```

**Translation:**
```
pl.col("age") > 30
    ↓
FT.SEARCH idx:users "@age:[30 +inf]" RETURN 2 name age
```

**Supported Filter Translations:**

| Polars Expression | RediSearch Query |
|-------------------|-----------------|
| `col("x") == 5` | `@x:[5 5]` |
| `col("x") > 5` | `@x:[(5 +inf]` |
| `col("x") < 5` | `@x:[-inf (5]` |
| `col("x").is_in([1,2,3])` | `@x:(1|2|3)` |
| `col("x") == "foo"` | `@x:{foo}` |
| `col("x").str.contains("bar")` | `@x:*bar*` |

**Limitations:**
- Requires RediSearch module installed
- Index must exist and cover queried fields
- Complex predicates may fall back to client-side filtering

#### RedisTimeSeries Support

RedisTimeSeries is a purpose-built time-series data structure with native aggregation, 
downsampling, and retention policies. This is the ideal target for metrics, IoT, and 
financial data analysis with Polars.

```python
def scan_timeseries(
    url: str,
    key: str,
    *,
    start: int | str | datetime = "-",      # "-" = oldest
    end: int | str | datetime = "+",        # "+" = newest
    count: int | None = None,
    aggregation: str | None = None,         # avg, sum, min, max, range, count, first, last, std.p, std.s, var.p, var.s, twa
    bucket_size_ms: int | None = None,      # Required if aggregation set
    align: str | None = None,               # start, end, or timestamp
    filter_by_value: tuple[float, float] | None = None,  # (min, max)
    include_timestamp: bool = True,
    timestamp_column_name: str = "_ts",
    value_column_name: str = "value",
) -> pl.LazyFrame
```

**Multi-key queries with label filtering:**

```python
def scan_timeseries_multi(
    url: str,
    filters: list[str],                     # Label filters: ["location=us", "unit=cm"]
    *,
    start: int | str | datetime = "-",
    end: int | str | datetime = "+",
    aggregation: str | None = None,
    bucket_size_ms: int | None = None,
    group_by: str | None = None,            # Label to group by
    reducer: str | None = None,             # avg, sum, min, max (for groupby)
    with_labels: bool = False,
    selected_labels: list[str] | None = None,
    include_key: bool = True,
    key_column_name: str = "_key",
) -> pl.LazyFrame
```

**Use Cases:**
- IoT sensor data (temperature, humidity, pressure)
- Application metrics (latency, throughput, error rates)
- Financial market data (OHLCV, tick data)
- Infrastructure monitoring (CPU, memory, network)
- Smart metering (energy, water, gas)

**Implementation:**
- Single key: `TS.RANGE key start end [AGGREGATION type bucket] [FILTER_BY_VALUE min max]`
- Multi key: `TS.MRANGE start end FILTER label=value [GROUPBY label REDUCE reducer]`
- Labels become additional columns when `with_labels=True`

**Aggregation Pushdown:**
Server-side aggregation is a killer feature - push downsampling to Redis:

```python
# Downsample 1-second data to 1-minute averages
lf = redis.scan_timeseries(
    url,
    key="sensor:temp:1",
    start=datetime.now() - timedelta(hours=24),
    aggregation="avg",
    bucket_size_ms=60_000,  # 1 minute
)

# Returns pre-aggregated data - minimal network transfer
hourly_temps = lf.collect()
```

**Multi-sensor analysis with groupby:**

```python
# Get max temperature per location, aggregated hourly
lf = redis.scan_timeseries_multi(
    url,
    filters=["type=temperature", "country=us"],
    aggregation="max",
    bucket_size_ms=3600_000,  # 1 hour
    group_by="location",
    reducer="max",
    selected_labels=["location", "unit"],
)

result = lf.collect()
# Columns: _ts, value, _key, location, unit
```

**Example - IoT Dashboard:**

```python
# Last 24 hours of sensor readings, 5-minute averages
sensors = redis.scan_timeseries_multi(
    url,
    filters=["facility=building_a"],
    start=datetime.now() - timedelta(hours=24),
    aggregation="avg",
    bucket_size_ms=300_000,  # 5 minutes
    with_labels=True,
)

# Pivot by sensor type for dashboard
dashboard_data = (
    sensors
    .with_columns(pl.from_epoch("_ts", time_unit="ms").alias("time"))
    .collect()
)
```

#### Redis Streams Support

```python
def scan_stream(
    url: str,
    key: str,
    schema: dict[str, pl.DataType],
    *,
    start_id: str = "-",           # "-" = beginning
    end_id: str = "+",             # "+" = end  
    start_time: datetime | None = None,  # Alternative to start_id
    end_time: datetime | None = None,    # Alternative to end_id
    count: int | None = None,
    include_id: bool = True,
    id_column_name: str = "_id",
    include_timestamp: bool = True,
    timestamp_column_name: str = "_ts",
) -> pl.LazyFrame
```

**Use Cases:**
- Event logs / audit trails
- CDC (Change Data Capture) streams
- IoT sensor data
- User activity streams

**Implementation:**
- Uses `XRANGE key start end [COUNT count]`
- Entry ID (`1234567890123-0`) parsed into timestamp column
- Schema applied to field values

**Example:**
```python
# Scan events from last hour
lf = redis.scan_stream(
    url,
    key="events:user:123",
    schema={"action": pl.Utf8, "value": pl.Int64},
    start_time=datetime.now() - timedelta(hours=1),
)

events_per_action = lf.group_by("action").len().collect()
```

#### Sorted Sets Support

```python
def scan_zset(
    url: str,
    pattern: str = "*",
    *,
    min_score: float = float("-inf"),
    max_score: float = float("+inf"),
    include_key: bool = True,
    key_column_name: str = "_key",
    member_column_name: str = "member",
    score_column_name: str = "score",
    count: int | None = None,
) -> pl.LazyFrame
```

**Use Cases:**
- Leaderboards / rankings
- Time-indexed data (score = timestamp)
- Rate limiting windows
- Priority queues

**Implementation:**
- Uses `ZRANGEBYSCORE key min max WITHSCORES [LIMIT offset count]`
- Returns DataFrame with member (Utf8) and score (Float64) columns

**Example:**
```python
# Get top 100 users by score
lf = redis.scan_zset(
    url,
    pattern="leaderboard:*",
    min_score=0,
)

top_100 = lf.sort("score", descending=True).head(100).collect()
```

#### Redis Strings Support

Redis Strings are the simplest data type - single key-value pairs. While less structured
than Hashes or JSON, they're commonly used for caching, counters, and simple values.

```python
def scan_strings(
    url: str,
    pattern: str = "*",
    *,
    value_type: pl.DataType = pl.Utf8,  # Type to parse values as
    include_key: bool = True,
    key_column_name: str = "_key",
    value_column_name: str = "value",
    batch_size: int = 1000,
    count_hint: int = 100,
) -> pl.LazyFrame
```

**Parameters:**
- `url`: Redis connection URL
- `pattern`: Key pattern for SCAN (e.g., `cache:*`, `counter:*`)
- `value_type`: Polars dtype for the value column (Utf8, Int64, Float64, Boolean, Date, Datetime)
- `include_key`: Whether to include Redis key as a column
- `key_column_name`: Name of the key column (default: `_key`)
- `value_column_name`: Name of the value column (default: `value`)
- `batch_size`: Number of keys to fetch per batch
- `count_hint`: SCAN COUNT hint

**Use Cases:**
- Cache hit/miss analysis
- Counter aggregation
- Session token auditing
- Feature flag states
- Simple key-value exports

**Implementation:**
- Uses `SCAN` to find keys matching pattern
- Batched `MGET` to fetch values efficiently
- Type conversion based on `value_type` parameter

**Examples:**

```python
# Export all cache entries as strings
lf = redis.scan_strings(
    url,
    pattern="cache:*",
    value_type=pl.Utf8,
)

cache_entries = lf.collect()
# Columns: _key (Utf8), value (Utf8)
```

```python
# Aggregate counters
lf = redis.scan_strings(
    url,
    pattern="counter:page_views:*",
    value_type=pl.Int64,
)

total_views = lf.select(pl.col("value").sum()).collect()

# Group by page (extract from key)
page_stats = (
    lf.with_columns(
        pl.col("_key").str.extract(r"counter:page_views:(.+)", 1).alias("page")
    )
    .group_by("page")
    .agg(pl.col("value").sum().alias("total_views"))
    .collect()
)
```

```python
# Session analysis - parse JSON strings
lf = redis.scan_strings(
    url,
    pattern="session:*",
    value_type=pl.Utf8,
)

# Parse JSON values client-side
sessions = (
    lf.with_columns(
        pl.col("value").str.json_decode().alias("data")
    )
    .unnest("data")
    .collect()
)
```

```python
# Feature flags as booleans
lf = redis.scan_strings(
    url,
    pattern="feature:*",
    value_type=pl.Boolean,
)

enabled_features = lf.filter(pl.col("value")).collect()
```

**Write Support:**

```python
def write_strings(
    df: pl.DataFrame,
    url: str,
    *,
    key_column: str = "_key",
    value_column: str = "value",
    key_prefix: str = "",
    ttl: int | None = None,
    batch_size: int = 1000,
) -> int  # Returns rows written
```

**Example:**
```python
# Write computed values back to Redis
df = pl.DataFrame({
    "_key": ["result:1", "result:2", "result:3"],
    "value": [100, 200, 300],
})

redis.write_strings(df, url, ttl=3600)  # 1 hour TTL
```

**Comparison with Hashes:**

| Aspect | Strings | Hashes |
|--------|---------|--------|
| **Structure** | Single value per key | Multiple fields per key |
| **Atomicity** | Single SET/GET | Field-level operations |
| **Memory** | Lower overhead per key | More efficient for multi-field |
| **Best for** | Counters, caches, flags | Entities, profiles, objects |

**When to use Strings:**
- Single value per key (counters, flags, simple caches)
- Need atomic increment/decrement (INCR, DECR)
- Storing serialized data (JSON strings, binary)
- Simple expiration patterns

**When to use Hashes instead:**
- Multiple related fields per entity
- Need partial updates (HSET single field)
- Memory efficiency for many small fields
- Field-level TTL not needed (Hash fields share key TTL)

---

## Gap Analysis vs. Ecosystem

### Compared to Other IO Plugins

| Feature | polars-avro | polars_readstat | polars-redis |
|---------|-------------|-----------------|--------------|
| Lazy scan | ✅ | ✅ | ✅ |
| Eager read | ✅ | ✅ | Phase 2 |
| Write | ✅ | ❌ | Phase 3 |
| Schema inference | ❌ | ✅ | Phase 2 |
| Metadata access | ❌ | ✅ | Phase 2 |
| Projection pushdown | N/A | N/A | ✅ |
| Predicate pushdown | N/A | N/A | Phase 4 |

### Unique to polars-redis

1. **Projection pushdown** - Genuine network optimization (HMGET vs HGETALL)
2. **RediSearch integration** - Predicate pushdown to search engine
3. **Operational data access** - Bridge between cache/app data and analytics

---

## Use Cases & Data Type Selection Guide

Choosing the right Redis data type for your Polars analytics workload depends on your data shape, 
query patterns, and performance requirements. This section helps you decide.

### Decision Matrix: Which Data Type to Use?

| If your data is... | Use | Why |
|-------------------|-----|-----|
| User profiles, product catalogs, config | **Hash** (`scan_hashes`) | Flexible schema, field-level access |
| Nested/hierarchical documents | **JSON** (`scan_json`) | Path queries, complex structures |
| Simple key-value pairs, counters, flags | **Strings** (`scan_strings`) | Simplest structure, atomic ops |
| Metrics, sensor readings, stock prices | **TimeSeries** (`scan_timeseries`) | Native aggregation, 90% compression |
| Event logs, CDC streams, audit trails | **Streams** (`scan_stream`) | Ordered events, consumer groups |
| Leaderboards, rankings, scored items | **Sorted Sets** (`scan_zset`) | Score-based ordering |
| Searchable entities with filters | **Hash + RediSearch** | Index-accelerated queries |

### Detailed Comparison

#### Hash vs JSON

| Aspect | Hash | JSON |
|--------|------|------|
| **Schema** | Flat key-value pairs | Nested, hierarchical |
| **Field types** | All strings (you parse) | Native types preserved |
| **Memory** | More efficient for flat data | Overhead for structure |
| **Partial reads** | HMGET specific fields | JSONPath projections |
| **Best for** | Simple entities, counters | Complex documents, APIs |

**Use Hash when:** Your data is naturally flat (user profiles, feature flags, session data).

**Use JSON when:** You have nested structures, arrays, or need to preserve types.

#### TimeSeries vs Hash/JSON for Time-Stamped Data

This is a critical decision. Many teams store metrics in Hashes or JSON and regret it.

| Aspect | TimeSeries | Hash/JSON |
|--------|------------|-----------|
| **Compression** | 90-94% (Gorilla algorithm) | None (raw storage) |
| **Memory for 1M samples** | ~1.6 MB compressed | ~16 MB uncompressed |
| **Aggregation** | Server-side (avg, min, max, sum) | Client-side (pull all data) |
| **Time range queries** | Native `TS.RANGE` | Manual key patterns |
| **Retention policies** | Built-in auto-expiry | Manual cleanup |
| **Downsampling** | Automatic compaction rules | Manual ETL |
| **Multi-series queries** | `TS.MRANGE` with label filters | Multiple SCAN operations |

**Example - 10K sensors, 1 reading/second, 24 hours:**

```
TimeSeries: 10K keys × 86,400 samples × ~1.6 bytes = ~1.4 GB
Hash approach: 10K keys × 86,400 fields × ~16 bytes = ~14 GB
```

**When to use TimeSeries:**
- Numeric measurements over time (metrics, sensors, prices)
- Need aggregations (hourly averages, daily max)
- High cardinality (many series)
- Retention requirements (keep 30 days, auto-delete older)

**When Hash/JSON is fine:**
- Infrequent updates (config that changes daily)
- Need non-numeric fields alongside timestamps
- Simple "latest value" patterns
- Already using RediSearch for other queries

#### TimeSeries vs RediSearch for Analytics

People sometimes use RediSearch numeric indexes for time-series-like queries. Here's when each wins:

| Aspect | TimeSeries | RediSearch |
|--------|------------|------------|
| **Optimized for** | Append-only numeric streams | Multi-field search & filter |
| **Aggregation** | Native (avg, sum, std, twa) | Via FT.AGGREGATE (more flexible) |
| **Compression** | Gorilla (90%+ reduction) | Standard index structures |
| **Time range** | First-class `TS.RANGE` | `@timestamp:[min max]` |
| **Multi-dimensional** | Labels (location=us) | Full query language |
| **Write throughput** | ~500K samples/sec | ~100K docs/sec |
| **Query latency** | Sub-ms for single series | Sub-30ms for complex queries |

**Use TimeSeries when:**
- Pure numeric time-series (one value per timestamp)
- Need built-in downsampling/compaction
- Memory efficiency is critical
- Simple label-based filtering is sufficient

**Use RediSearch when:**
- Complex multi-field queries (`@price:[100 500] @category:{electronics}`)
- Full-text search alongside numeric filters
- Need aggregations with grouping on non-time dimensions
- Data has rich text fields

**Hybrid approach:**
You can use both! Store raw metrics in TimeSeries for efficient storage and aggregation,
then periodically materialize aggregated summaries into Hashes indexed by RediSearch for
complex analytical queries.

#### Streams vs TimeSeries

| Aspect | Streams | TimeSeries |
|--------|---------|------------|
| **Data model** | Multi-field messages | Single numeric value |
| **Ordering** | By entry ID (time-based) | By timestamp |
| **Consumer groups** | Yes (pub/sub patterns) | No |
| **Aggregation** | Manual (XREAD + process) | Built-in (avg, sum, etc.) |
| **Retention** | MAXLEN or MINID | Time-based policies |
| **Best for** | Events, logs, CDC | Metrics, measurements |

**Use Streams when:**
- Events have multiple fields per entry
- Need consumer group semantics (work distribution)
- Building event sourcing / CDC pipelines
- Messages are processed then acknowledged

**Use TimeSeries when:**
- Single numeric measurement per timestamp
- Need server-side aggregation
- Memory efficiency matters (compression)
- Downsampling/compaction rules needed

### polars-redis Use Case Examples

#### 1. Real-Time Dashboard (TimeSeries)

```python
# 5-minute averages for last 24 hours, all US sensors
lf = redis.scan_timeseries_multi(
    url,
    filters=["region=us", "type=temperature"],
    start=datetime.now() - timedelta(hours=24),
    aggregation="avg",
    bucket_size_ms=300_000,  # 5 min
    group_by="facility",
    reducer="avg",
)

# Polars does the pivot/reshape
dashboard = (
    lf.with_columns(pl.from_epoch("_ts", time_unit="ms").alias("time"))
    .collect()
    .pivot(on="facility", index="time", values="value")
)
```

**Why TimeSeries:** Server-side aggregation means you transfer 288 points per facility 
instead of 17,280 raw samples. 60x less data over the wire.

#### 2. Customer Analytics (Hash + RediSearch)

```python
# High-value customers in California, with specific fields
lf = redis.scan_hashes(
    url,
    pattern="customer:*",
    schema={
        "name": pl.Utf8,
        "lifetime_value": pl.Float64,
        "state": pl.Utf8,
        "last_purchase": pl.Utf8,
    },
    search_index="idx:customers",  # Phase 4: predicate pushdown
)

ca_whales = (
    lf.filter(
        (pl.col("state") == "CA") & 
        (pl.col("lifetime_value") > 10000)
    )
    .sort("lifetime_value", descending=True)
    .head(100)
    .collect()
)
```

**Why Hash + RediSearch:** Complex filtering on multiple fields. RediSearch's numeric 
range tree makes `lifetime_value > 10000` fast without scanning all keys.

#### 3. Event Log Analysis (Streams)

```python
# Last hour of user activity events
lf = redis.scan_stream(
    url,
    key="events:user:12345",
    schema={"action": pl.Utf8, "page": pl.Utf8, "duration_ms": pl.Int64},
    start_time=datetime.now() - timedelta(hours=1),
)

session_analysis = (
    lf.group_by("action")
    .agg([
        pl.len().alias("count"),
        pl.col("duration_ms").mean().alias("avg_duration"),
    ])
    .collect()
)
```

**Why Streams:** Events have multiple fields, natural time ordering via entry IDs,
and you might want consumer groups for distributed processing.

#### 4. Leaderboard Export (Sorted Set)

```python
# Top 1000 players with their ranks
lf = redis.scan_zset(
    url,
    pattern="leaderboard:global",
    min_score=0,
)

top_players = (
    lf.sort("score", descending=True)
    .head(1000)
    .with_row_index("rank", offset=1)
    .collect()
)
```

**Why Sorted Set:** Score-based ordering is the native operation. No need to sort 
client-side for millions of members.

### Performance Expectations

| Operation | Expected Performance |
|-----------|---------------------|
| `scan_hashes` (10K keys, 10 fields) | ~200ms |
| `scan_hashes` + projection (10K keys, 2 fields) | ~80ms |
| `scan_json` (10K keys, nested) | ~300ms |
| `scan_timeseries` (1 series, 100K points) | ~50ms |
| `scan_timeseries` + aggregation (1 series, 100K→100 points) | ~10ms |
| `scan_timeseries_multi` (100 series, 1K points each) | ~100ms |
| `scan_hashes` + RediSearch predicate | ~30ms (index-accelerated) |

*Benchmarks on local Redis, single-threaded, will vary with network latency.*

### Anti-Patterns to Avoid

1. **Storing time-series in Hashes:** You lose compression, aggregation, retention policies.
   Use TimeSeries for numeric measurements.

2. **Using SCAN for searchable data:** If you're filtering on field values, add a 
   RediSearch index. SCAN touches every key; indexes don't.

3. **Pulling raw data for aggregation:** If you need `AVG(temperature) GROUP BY hour`,
   push that to TimeSeries with `aggregation="avg", bucket_size_ms=3600000`.

4. **Giant JSON documents:** If you have 1MB JSON docs, consider breaking into smaller
   keys or using Hash for frequently-accessed fields.

5. **Ignoring projection pushdown:** Always specify only the fields you need in your
   schema. `HMGET field1 field2` is faster than `HGETALL` + filter in Python.

---

## Roadmap

### Phase 1: Core (Done)
- [x] scan_hashes with projection pushdown
- [x] scan_json with path projection
- [x] n_rows pushdown
- [x] Schema-based type conversion
- [x] Python API via register_io_source
- [x] Integration tests with real Redis
- [x] CI/CD pipeline (GitHub Actions)
- [x] README with examples

### Phase 2: Enhanced Read (Done)
- [x] `read_hashes()` / `read_json()` eager functions
- [x] `infer_hash_schema()` / `infer_json_schema()`
- [x] TTL column support
- [x] Row index support
- [x] Better error messages

### Phase 3: Write Support (Done)
- [x] `write_hashes()` basic implementation
- [x] `write_json()` basic implementation
- [x] `write_strings()` basic implementation
- [x] Write modes (fail/replace/append)
- [x] TTL on write
- [x] Key prefix support
- [x] Batch pipelining for writes
- [x] Key generation from row index (auto-index)

### Phase 4: Additional Data Types
- [x] Redis Strings support (`scan_strings`, `read_strings`, `write_strings`)
- [ ] Sorted Sets support (`scan_zset`)
- [ ] Redis Streams support (`scan_stream`)
- [ ] RedisTimeSeries support (`scan_timeseries`, `scan_timeseries_multi`)

### Phase 5: Advanced Features
- [ ] RediSearch predicate pushdown
- [ ] Connection pooling
- [ ] Cluster support
- [ ] Lists support (`scan_list`)
- [ ] Performance benchmarks (batch sizes, pipelining, etc.)

### Phase 6: Documentation
- [ ] MkDocs Material site setup
- [ ] Getting started guide
- [ ] API reference (auto-generated from docstrings)
- [ ] Usage examples
- [ ] Data type selection guide
- [ ] GitHub Pages deployment

### Phase 7: Release
- [ ] PyPI release
- [ ] crates.io release
- [ ] awesome-polars submission

---

## Technical Details

### Dependencies

```toml
[dependencies]
redis = { version = "0.27", features = ["tokio-comp", "json", "cluster-async"] }
tokio = { version = "1", features = ["rt-multi-thread", "sync"] }
polars = { version = "0.46", features = ["dtype-struct", "strings", "lazy"] }
arrow = { version = "54", features = ["ipc"] }
pyo3 = { version = "0.23", features = ["extension-module"], optional = true }
thiserror = "2.0"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
```

### Build

```bash
# Development
maturin develop

# Release wheel
maturin build --release

# Install from wheel
pip install target/wheels/polars_redis-*.whl
```

### Testing

```bash
# Rust unit tests (no Redis needed)
cargo test --lib

# Integration tests (requires Redis on localhost:6379)
cargo test --test integration

# Python tests
pytest tests/ -v
```

---

## Marketing

### awesome-polars Entry

```markdown
* [polars-redis](https://github.com/joshrotenberg/polars-redis) - Lazily scan Redis hashes and JSON documents as Polars LazyFrames with projection pushdown by [@joshrotenberg](https://github.com/joshrotenberg).
```

### Blog Post Ideas

1. "Bridging Redis and Polars: Zero-Copy Analytics on Operational Data"
2. "Projection Pushdown in polars-redis: Why HMGET Beats HGETALL"
3. "From Cache to DataFrame: Real-Time Analytics with polars-redis"

---

## Open Questions

1. **Large scans**: Should we warn/require opt-in for scans > N keys?
2. **RediSearch auto-index**: Create index if missing, or require explicit?
3. **Cluster support**: Scan all nodes, or require explicit node selection?
4. **Connection sharing**: How to share pool between multiple scan calls?
5. **Streaming writes**: Is `sink_redis()` feasible with Polars sink API?
