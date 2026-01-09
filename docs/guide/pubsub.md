# Pub/Sub DataFrame Streaming

polars-redis provides functions to collect Redis Pub/Sub messages into DataFrames for real-time analytics. This enables processing streaming data with Polars' powerful DataFrame operations.

## Overview

Redis Pub/Sub is a messaging system where publishers send messages to channels, and subscribers receive messages from those channels. polars-redis bridges this with Polars by collecting messages into DataFrames.

**Key Features:**

- Collect messages by count, timeout, or time window
- Support for raw text and JSON message formats
- Custom message parsers for complex formats
- Pattern subscriptions with `PSUBSCRIBE`
- Batch iteration for continuous streaming
- Both sync and async APIs

## Basic Usage

### Collecting Messages

Use `collect_pubsub()` to subscribe to channels and collect messages:

```python
import polars_redis as redis

# Collect 100 messages or timeout after 5 seconds
df = redis.collect_pubsub(
    "redis://localhost",
    channels=["events"],
    count=100,
    timeout_ms=5000,
)

print(df)
# shape: (100, 1)
# +---------------+
# | message       |
# | ---           |
# | str           |
# +---------------+
# | event_data_1  |
# | event_data_2  |
# | ...           |
# +---------------+
```

### Termination Conditions

You can specify multiple termination conditions - collection stops when any is met:

```python
# Stop after 100 messages
df = redis.collect_pubsub(url, ["channel"], count=100)

# Stop after 5 seconds
df = redis.collect_pubsub(url, ["channel"], timeout_ms=5000)

# Stop after 30 second window
df = redis.collect_pubsub(url, ["channel"], window_seconds=30.0)

# Combined: 1000 messages OR 60 seconds, whichever comes first
df = redis.collect_pubsub(
    url,
    ["channel"],
    count=1000,
    timeout_ms=60000,
)
```

## JSON Messages

For JSON-formatted messages, use `message_format="json"` with an optional schema:

```python
# Messages like: {"user_id": 123, "action": "click", "value": 45.5}
df = redis.collect_pubsub(
    "redis://localhost",
    channels=["json_events"],
    count=100,
    message_format="json",
    schema={
        "user_id": pl.Int64,
        "action": pl.Utf8,
        "value": pl.Float64,
    },
)

print(df)
# shape: (100, 3)
# +---------+--------+-------+
# | user_id | action | value |
# | ---     | ---    | ---   |
# | i64     | str    | f64   |
# +---------+--------+-------+
# | 123     | click  | 45.5  |
# | 456     | view   | 12.0  |
# | ...     | ...    | ...   |
# +---------+--------+-------+
```

## Custom Parsers

For complex message formats, provide a custom parser function:

```python
def parse_log_line(channel: str, payload: bytes) -> dict:
    """Parse log format: LEVEL|timestamp|message"""
    text = payload.decode("utf-8")
    parts = text.split("|", 2)
    return {
        "level": parts[0],
        "timestamp": parts[1] if len(parts) > 1 else "",
        "message": parts[2] if len(parts) > 2 else "",
    }

df = redis.collect_pubsub(
    "redis://localhost",
    channels=["logs"],
    count=100,
    parser=parse_log_line,
    schema={
        "level": pl.Utf8,
        "timestamp": pl.Utf8,
        "message": pl.Utf8,
    },
)
```

## Pattern Subscriptions

Subscribe to multiple channels using patterns with `pattern=True`:

```python
# Subscribe to all channels matching "sensor:*"
df = redis.collect_pubsub(
    "redis://localhost",
    channels=["sensor:*"],
    pattern=True,
    count=100,
    include_channel=True,  # Track which channel each message came from
)

print(df)
# shape: (100, 2)
# +---------------+---------------+
# | _channel      | message       |
# | ---           | ---           |
# | str           | str           |
# +---------------+---------------+
# | sensor:temp   | 23.5          |
# | sensor:humid  | 65            |
# | sensor:press  | 1013.25       |
# +---------------+---------------+
```

## Metadata Columns

Include channel names and timestamps as columns:

```python
df = redis.collect_pubsub(
    "redis://localhost",
    channels=["events"],
    count=100,
    include_channel=True,      # Add _channel column
    include_timestamp=True,    # Add _received_at column
)

# Custom column names
df = redis.collect_pubsub(
    "redis://localhost",
    channels=["events"],
    count=100,
    include_channel=True,
    include_timestamp=True,
    channel_column="source",
    message_column="payload",
    timestamp_column="received_ts",
)
```

## Batch Iteration

For continuous streaming, use iterators that yield batches of messages:

### Synchronous Iterator

```python
# Process batches of up to 100 messages or every second
for batch_df in redis.iter_pubsub(
    "redis://localhost",
    channels=["events"],
    batch_size=100,
    batch_timeout_ms=1000,
):
    # Process each batch
    summary = batch_df.group_by("type").len()
    print(f"Batch: {len(batch_df)} messages")
    
    # Break when done
    if should_stop():
        break
```

### Async Iterator

```python
import asyncio

async def process_events():
    async for batch_df in redis.iter_pubsub_async(
        "redis://localhost",
        channels=["events"],
        batch_size=100,
        batch_timeout_ms=1000,
        message_format="json",
        schema={"type": pl.Utf8, "value": pl.Float64},
    ):
        # Async processing
        result = batch_df.filter(pl.col("value") > 100)
        await save_to_database(result)

asyncio.run(process_events())
```

## Real-Time Analytics Example

Combine Pub/Sub with Polars for real-time analytics:

```python
import polars as pl
import polars_redis as redis

# Collect 5 minutes of clickstream data
df = redis.collect_pubsub(
    "redis://localhost",
    channels=["clickstream"],
    window_seconds=300,
    message_format="json",
    schema={
        "user_id": pl.Int64,
        "page": pl.Utf8,
        "duration_ms": pl.Int64,
        "timestamp": pl.Float64,
    },
    include_timestamp=True,
)

# Analyze with Polars
summary = (
    df.group_by("page")
    .agg([
        pl.len().alias("views"),
        pl.col("user_id").n_unique().alias("unique_users"),
        pl.col("duration_ms").mean().alias("avg_duration_ms"),
    ])
    .sort("views", descending=True)
)

print(summary)
```

## Rust API

The Rust library provides equivalent functionality:

```rust
use polars_redis::client::pubsub::{collect_pubsub, PubSubConfig};

// Configure collection
let config = PubSubConfig {
    channels: vec!["events".to_string()],
    count: Some(100),
    timeout: Some(Duration::from_secs(5)),
    pattern: false,
    ..Default::default()
};

// Collect messages into RecordBatch
let batch = collect_pubsub("redis://localhost", &config)?;
```

## API Reference

### `collect_pubsub()`

Collect messages into a DataFrame.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | `str` | required | Redis connection URL |
| `channels` | `list[str]` | required | Channel names or patterns |
| `count` | `int \| None` | `None` | Maximum messages to collect |
| `timeout_ms` | `int \| None` | `None` | Timeout in milliseconds |
| `window_seconds` | `float \| None` | `None` | Time window for collection |
| `pattern` | `bool` | `False` | Use pattern subscription |
| `message_format` | `"raw" \| "json"` | `"raw"` | Message format |
| `parser` | `Callable` | `None` | Custom parser function |
| `schema` | `dict` | `None` | Schema for parsed fields |
| `include_channel` | `bool` | `False` | Include channel column |
| `include_timestamp` | `bool` | `False` | Include timestamp column |

### `iter_pubsub()`

Synchronous iterator yielding DataFrame batches.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `batch_size` | `int` | `100` | Max messages per batch |
| `batch_timeout_ms` | `int` | `1000` | Timeout to yield partial batch |
| *(plus all `collect_pubsub` params)* | | | |

### `iter_pubsub_async()`

Async iterator yielding DataFrame batches.

Same parameters as `iter_pubsub()`.

## See Also

- [Redis Streams](streams.md) - For persistent, replayable message streams
- [Writing Data](writing.md) - Write DataFrames to Redis
- [Caching](caching.md) - Cache DataFrames in Redis
