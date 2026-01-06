# Redis Streams Consumption

polars-redis provides functions for consuming Redis Streams as streaming data sources with support for consumer groups, blocking reads, and continuous batch iteration.

## Overview

Redis Streams are append-only log data structures ideal for:

- Event sourcing and CDC (Change Data Capture)
- Real-time analytics pipelines
- IoT sensor data ingestion
- Log aggregation
- Message queuing with acknowledgment

**Key Features:**

- Single stream consumption with `read_stream()` and `scan_stream()`
- Consumer group support for distributed processing
- Blocking reads for real-time tailing
- Batch iteration with `iter_stream()` and `stream_batches()`
- Manual or automatic message acknowledgment

## Differences from `scan_streams`/`read_streams`

The existing `scan_streams()` and `read_streams()` functions scan **multiple streams** by key pattern. The new functions in this module focus on:

1. **Single stream consumption** - Work with one stream at a time
2. **Consumer groups** - Distributed message processing with XREADGROUP
3. **Blocking reads** - Real-time tailing of new entries
4. **Continuous streaming** - Batch iterators for ongoing processing

## Basic Usage

### Reading Stream Entries

Use `read_stream()` to read entries from a single stream:

```python
import polars as pl
import polars_redis as redis

# Read all entries from a stream
df = redis.read_stream(
    "redis://localhost",
    stream="events",
    schema={"user_id": pl.Utf8, "action": pl.Utf8, "value": pl.Int64},
)

print(df)
# shape: (100, 5)
# +-------------------+-------------+---------+--------+-------+
# | _id               | _ts         | user_id | action | value |
# | ---               | ---         | ---     | ---    | ---   |
# | str               | i64         | str     | str    | i64   |
# +-------------------+-------------+---------+--------+-------+
# | 1704067200000-0   | 17040672... | user_1  | click  | 10    |
# | 1704067200001-0   | 17040672... | user_2  | view   | 20    |
# +-------------------+-------------+---------+--------+-------+
```

### Entry ID Parsing

Stream entry IDs contain timestamp and sequence information:

- `_id`: Full entry ID (e.g., "1704067200000-0")
- `_ts`: Timestamp in milliseconds (first part of ID)
- `_seq`: Sequence number (second part of ID, optional)

```python
df = redis.read_stream(
    "redis://localhost",
    stream="events",
    include_id=True,
    include_timestamp=True,
    include_sequence=True,  # Enable sequence column
)
```

### Reading with Count Limit

```python
# Read only the first 100 entries
df = redis.read_stream(
    "redis://localhost",
    stream="events",
    count=100,
)
```

### Reading an ID Range

```python
# Read entries between specific IDs
df = redis.read_stream(
    "redis://localhost",
    stream="events",
    start_id="1704067200000-0",
    end_id="1704067300000-0",
)

# Read from oldest to newest (default)
df = redis.read_stream(url, stream="events", start_id="-", end_id="+")
```

## Consumer Groups

Consumer groups enable distributed stream processing where multiple consumers share the workload.

### Basic Consumer Group Usage

```python
# Read as part of a consumer group
df = redis.read_stream(
    "redis://localhost",
    stream="events",
    group="analytics",
    consumer="worker-1",
    auto_ack=True,  # Automatically acknowledge messages
)
```

### Manual Acknowledgment

For at-least-once processing, disable auto-ack and acknowledge after successful processing:

```python
# Read without auto-acknowledgment
df = redis.read_stream(
    "redis://localhost",
    stream="events",
    group="analytics",
    consumer="worker-1",
    auto_ack=False,
)

# Process entries
try:
    process(df)
    
    # Acknowledge after successful processing
    entry_ids = df["_id"].to_list()
    redis.ack_entries("redis://localhost", "events", "analytics", entry_ids)
except Exception as e:
    # Messages will be redelivered to another consumer
    log_error(e)
```

### Multiple Consumers

Distribute work across multiple consumers:

```python
# Worker 1
df1 = redis.read_stream(
    url, stream="events",
    group="analytics",
    consumer="worker-1",
    count=100,
    auto_ack=True,
)

# Worker 2 (in another process)
df2 = redis.read_stream(
    url, stream="events",
    group="analytics",
    consumer="worker-2",
    count=100,
    auto_ack=True,
)

# Each worker receives different entries
```

## Blocking Reads

Block waiting for new entries in real-time:

```python
# Wait up to 5 seconds for new entries
df = redis.read_stream(
    "redis://localhost",
    stream="events",
    start_id="$",  # Only new entries (after current latest)
    block_ms=5000,
    count=100,
)
```

## Batch Iteration

For continuous stream processing, use iterators that yield DataFrames.

### Synchronous Iterator

```python
for batch_df in redis.iter_stream(
    "redis://localhost",
    stream="events",
    batch_size=100,
    block_ms=1000,  # Wait up to 1 second for batch
):
    # Process each batch
    summary = batch_df.group_by("action").len()
    print(f"Batch: {len(batch_df)} entries")
    
    if should_stop():
        break
```

### With Consumer Group

```python
for batch_df in redis.iter_stream(
    "redis://localhost",
    stream="events",
    group="analytics",
    consumer="worker-1",
    batch_size=100,
    auto_ack=True,
):
    process_batch(batch_df)
```

### Async Iterator

```python
import asyncio

async def process_stream():
    async for batch_df in redis.stream_batches(
        "redis://localhost",
        stream="events",
        batch_size=100,
        batch_timeout_ms=1000,
    ):
        await process_async(batch_df)
        
        if should_stop():
            break

asyncio.run(process_stream())
```

## Lazy Scanning

Use `scan_stream()` for lazy evaluation:

```python
# Create LazyFrame (no execution yet)
lf = redis.scan_stream(
    "redis://localhost",
    stream="events",
    schema={"action": pl.Utf8, "value": pl.Float64},
)

# Apply lazy operations
result = (
    lf.filter(pl.col("value") > 100)
    .group_by("action")
    .agg(pl.col("value").mean())
    .collect()
)
```

Note: Consumer groups and blocking reads are not supported in lazy mode.

## Real-Time Analytics Example

```python
import polars as pl
import polars_redis as redis

# Continuous aggregation over stream
for batch_df in redis.iter_stream(
    "redis://localhost",
    stream="metrics",
    group="aggregator",
    consumer="worker-1",
    batch_size=1000,
    block_ms=5000,
    schema={
        "sensor_id": pl.Utf8,
        "value": pl.Float64,
    },
    auto_ack=True,
):
    # Aggregate by sensor
    summary = (
        batch_df.group_by("sensor_id")
        .agg([
            pl.col("value").mean().alias("avg"),
            pl.col("value").max().alias("max"),
            pl.len().alias("count"),
        ])
    )
    
    # Store aggregates
    redis.write_hashes(
        "redis://localhost",
        summary,
        key_column="sensor_id",
        key_prefix="sensor:stats:",
    )
```

## API Reference

### `read_stream()`

Read entries from a Redis Stream into a DataFrame.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | `str` | required | Redis connection URL |
| `stream` | `str` | required | Stream name |
| `schema` | `dict` | `None` | Field name to dtype mapping |
| `start_id` | `str` | `"-"` | Start entry ID |
| `end_id` | `str` | `"+"` | End entry ID |
| `count` | `int` | `None` | Max entries to read |
| `block_ms` | `int` | `None` | Block timeout (ms) |
| `group` | `str` | `None` | Consumer group name |
| `consumer` | `str` | `None` | Consumer name |
| `auto_ack` | `bool` | `False` | Auto-acknowledge messages |
| `create_group` | `bool` | `True` | Create group if missing |

### `scan_stream()`

Lazy version of `read_stream()`. Returns a LazyFrame.

### `iter_stream()`

Synchronous iterator yielding DataFrame batches.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batch_size` | `int` | `100` | Max entries per batch |
| `block_ms` | `int` | `1000` | Block timeout per batch |
| *(plus all `read_stream` params)* | | | |

### `stream_batches()`

Async iterator yielding DataFrame batches.

Same parameters as `iter_stream()`.

### `ack_entries()`

Acknowledge stream entries in a consumer group.

| Parameter | Type | Description |
|-----------|------|-------------|
| `url` | `str` | Redis connection URL |
| `stream` | `str` | Stream name |
| `group` | `str` | Consumer group name |
| `entry_ids` | `list[str]` | Entry IDs to acknowledge |

## See Also

- [Scanning Data](scanning.md) - Multi-stream scanning with `scan_streams()`
- [Pub/Sub Streaming](pubsub.md) - Real-time Pub/Sub messaging
- [Writing Data](writing.md) - Write DataFrames to Redis
