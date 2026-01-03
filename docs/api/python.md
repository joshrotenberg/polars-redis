# Python API Reference

## Scan Functions

### scan_hashes

```python
def scan_hashes(
    url: str,
    pattern: str = "*",
    schema: dict | None = None,
    *,
    include_key: bool = True,
    key_column_name: str = "_key",
    include_ttl: bool = False,
    ttl_column_name: str = "_ttl",
    include_row_index: bool = False,
    row_index_column_name: str = "_index",
    batch_size: int = 1000,
    count_hint: int = 100,
) -> pl.LazyFrame
```

Scan Redis hashes matching a pattern and return a LazyFrame.

**Parameters:**

- `url`: Redis connection URL
- `pattern`: Key pattern to match (e.g., `"user:*"`)
- `schema`: Dictionary mapping field names to Polars dtypes
- `include_key`: Include Redis key as a column
- `key_column_name`: Name of the key column
- `include_ttl`: Include TTL as a column
- `ttl_column_name`: Name of the TTL column
- `include_row_index`: Include row index column
- `row_index_column_name`: Name of the index column
- `batch_size`: Keys per batch
- `count_hint`: Redis SCAN COUNT hint

**Returns:** `pl.LazyFrame`

---

### scan_json

```python
def scan_json(
    url: str,
    pattern: str = "*",
    schema: dict | None = None,
    *,
    include_key: bool = True,
    key_column_name: str = "_key",
    include_ttl: bool = False,
    ttl_column_name: str = "_ttl",
    include_row_index: bool = False,
    row_index_column_name: str = "_index",
    batch_size: int = 1000,
    count_hint: int = 100,
) -> pl.LazyFrame
```

Scan RedisJSON documents matching a pattern and return a LazyFrame.

Parameters are identical to `scan_hashes`.

---

### scan_strings

```python
def scan_strings(
    url: str,
    pattern: str = "*",
    *,
    value_type: type[pl.DataType] = pl.Utf8,
    include_key: bool = True,
    key_column_name: str = "_key",
    value_column_name: str = "value",
    batch_size: int = 1000,
    count_hint: int = 100,
) -> pl.LazyFrame
```

Scan Redis string values matching a pattern and return a LazyFrame.

**Parameters:**

- `url`: Redis connection URL
- `pattern`: Key pattern to match
- `value_type`: Polars dtype for value column (default: `pl.Utf8`)
- `include_key`: Include Redis key as a column
- `key_column_name`: Name of the key column
- `value_column_name`: Name of the value column
- `batch_size`: Keys per batch
- `count_hint`: Redis SCAN COUNT hint

**Returns:** `pl.LazyFrame`

---

### scan_sets

```python
def scan_sets(
    url: str,
    pattern: str = "*",
    *,
    include_key: bool = True,
    key_column_name: str = "_key",
    member_column_name: str = "member",
    include_row_index: bool = False,
    row_index_column_name: str = "_index",
    batch_size: int = 1000,
    count_hint: int = 100,
) -> pl.LazyFrame
```

Scan Redis sets matching a pattern and return a LazyFrame with one row per member.

**Parameters:**

- `url`: Redis connection URL
- `pattern`: Key pattern to match
- `include_key`: Include Redis key as a column
- `key_column_name`: Name of the key column
- `member_column_name`: Name of the member column
- `include_row_index`: Include row index column
- `row_index_column_name`: Name of the index column
- `batch_size`: Keys per batch
- `count_hint`: Redis SCAN COUNT hint

**Returns:** `pl.LazyFrame`

---

### scan_lists

```python
def scan_lists(
    url: str,
    pattern: str = "*",
    *,
    include_key: bool = True,
    key_column_name: str = "_key",
    element_column_name: str = "element",
    include_position: bool = False,
    position_column_name: str = "position",
    include_row_index: bool = False,
    row_index_column_name: str = "_index",
    batch_size: int = 1000,
    count_hint: int = 100,
) -> pl.LazyFrame
```

Scan Redis lists matching a pattern and return a LazyFrame with one row per element.

**Parameters:**

- `url`: Redis connection URL
- `pattern`: Key pattern to match
- `include_key`: Include Redis key as a column
- `key_column_name`: Name of the key column
- `element_column_name`: Name of the element column
- `include_position`: Include position index
- `position_column_name`: Name of the position column
- `include_row_index`: Include row index column
- `row_index_column_name`: Name of the index column
- `batch_size`: Keys per batch
- `count_hint`: Redis SCAN COUNT hint

**Returns:** `pl.LazyFrame`

---

### scan_zsets

```python
def scan_zsets(
    url: str,
    pattern: str = "*",
    *,
    include_key: bool = True,
    key_column_name: str = "_key",
    member_column_name: str = "member",
    score_column_name: str = "score",
    include_rank: bool = False,
    rank_column_name: str = "rank",
    include_row_index: bool = False,
    row_index_column_name: str = "_index",
    batch_size: int = 1000,
    count_hint: int = 100,
) -> pl.LazyFrame
```

Scan Redis sorted sets matching a pattern and return a LazyFrame with one row per member.

**Parameters:**

- `url`: Redis connection URL
- `pattern`: Key pattern to match
- `include_key`: Include Redis key as a column
- `key_column_name`: Name of the key column
- `member_column_name`: Name of the member column
- `score_column_name`: Name of the score column
- `include_rank`: Include rank index
- `rank_column_name`: Name of the rank column
- `include_row_index`: Include row index column
- `row_index_column_name`: Name of the index column
- `batch_size`: Keys per batch
- `count_hint`: Redis SCAN COUNT hint

**Returns:** `pl.LazyFrame`

---

### scan_streams

```python
def scan_streams(
    url: str,
    pattern: str = "*",
    fields: list[str] = [],
    *,
    start_id: str = "-",
    end_id: str = "+",
    count_per_stream: int | None = None,
    include_key: bool = True,
    key_column_name: str = "_key",
    include_id: bool = True,
    id_column_name: str = "_id",
    include_timestamp: bool = True,
    timestamp_column_name: str = "_ts",
    include_sequence: bool = False,
    sequence_column_name: str = "_seq",
    include_row_index: bool = False,
    row_index_column_name: str = "_index",
    batch_size: int = 1000,
    count_hint: int = 100,
) -> pl.LazyFrame
```

Scan Redis Streams matching a pattern and return a LazyFrame with one row per entry.

**Parameters:**

- `url`: Redis connection URL
- `pattern`: Key pattern to match
- `fields`: Field names to extract from entries
- `start_id`: Start entry ID (default: "-" for oldest)
- `end_id`: End entry ID (default: "+" for newest)
- `count_per_stream`: Maximum entries per stream (optional)
- `include_key`: Include Redis key as a column
- `key_column_name`: Name of the key column
- `include_id`: Include entry ID as a column
- `id_column_name`: Name of the entry ID column
- `include_timestamp`: Include timestamp as a column
- `timestamp_column_name`: Name of the timestamp column
- `include_sequence`: Include sequence number as a column
- `sequence_column_name`: Name of the sequence column
- `include_row_index`: Include row index column
- `row_index_column_name`: Name of the index column
- `batch_size`: Keys per batch
- `count_hint`: Redis SCAN COUNT hint

**Returns:** `pl.LazyFrame`

---

### scan_timeseries

```python
def scan_timeseries(
    url: str,
    pattern: str = "*",
    *,
    start: str = "-",
    end: str = "+",
    count_per_series: int | None = None,
    aggregation: str | None = None,
    bucket_size_ms: int | None = None,
    include_key: bool = True,
    key_column_name: str = "_key",
    include_timestamp: bool = True,
    timestamp_column_name: str = "_ts",
    value_column_name: str = "value",
    include_row_index: bool = False,
    row_index_column_name: str = "_index",
    label_columns: list[str] = [],
    batch_size: int = 1000,
    count_hint: int = 100,
) -> pl.LazyFrame
```

Scan RedisTimeSeries matching a pattern and return a LazyFrame with one row per sample.

**Parameters:**

- `url`: Redis connection URL
- `pattern`: Key pattern to match
- `start`: Start timestamp (default: "-" for oldest)
- `end`: End timestamp (default: "+" for newest)
- `count_per_series`: Maximum samples per time series (optional)
- `aggregation`: Aggregation type (avg, sum, min, max, range, count, first, last, std.p, std.s, var.p, var.s)
- `bucket_size_ms`: Bucket size in milliseconds (required with aggregation)
- `include_key`: Include Redis key as a column
- `key_column_name`: Name of the key column
- `include_timestamp`: Include timestamp as a column
- `timestamp_column_name`: Name of the timestamp column
- `value_column_name`: Name of the value column
- `include_row_index`: Include row index column
- `row_index_column_name`: Name of the index column
- `label_columns`: Label names to include as columns
- `batch_size`: Keys per batch
- `count_hint`: Redis SCAN COUNT hint

**Returns:** `pl.LazyFrame`

---

## Read Functions (Eager)

### read_hashes

```python
def read_hashes(...) -> pl.DataFrame
```

Eager version of `scan_hashes`. Parameters are identical.

### read_json

```python
def read_json(...) -> pl.DataFrame
```

Eager version of `scan_json`. Parameters are identical.

### read_strings

```python
def read_strings(...) -> pl.DataFrame
```

Eager version of `scan_strings`. Parameters are identical.

### read_sets

```python
def read_sets(...) -> pl.DataFrame
```

Eager version of `scan_sets`. Parameters are identical.

### read_lists

```python
def read_lists(...) -> pl.DataFrame
```

Eager version of `scan_lists`. Parameters are identical.

### read_zsets

```python
def read_zsets(...) -> pl.DataFrame
```

Eager version of `scan_zsets`. Parameters are identical.

### read_streams

```python
def read_streams(...) -> pl.DataFrame
```

Eager version of `scan_streams`. Parameters are identical.

### read_timeseries

```python
def read_timeseries(...) -> pl.DataFrame
```

Eager version of `scan_timeseries`. Parameters are identical.

---

## Write Functions

### write_hashes

```python
def write_hashes(
    df: pl.DataFrame,
    url: str,
    key_column: str | None = "_key",
    ttl: int | None = None,
    key_prefix: str = "",
    if_exists: str = "replace",
) -> int
```

Write a DataFrame to Redis as hashes.

**Parameters:**

- `df`: DataFrame to write
- `url`: Redis connection URL
- `key_column`: Column with Redis keys, or `None` for auto-generated
- `ttl`: TTL in seconds (optional)
- `key_prefix`: Prefix for all keys
- `if_exists`: `"fail"`, `"replace"`, or `"append"`

**Returns:** Number of keys written

---

### write_json

```python
def write_json(
    df: pl.DataFrame,
    url: str,
    key_column: str | None = "_key",
    ttl: int | None = None,
    key_prefix: str = "",
    if_exists: str = "replace",
) -> int
```

Write a DataFrame to Redis as JSON documents.

Parameters are identical to `write_hashes`.

---

### write_strings

```python
def write_strings(
    df: pl.DataFrame,
    url: str,
    key_column: str | None = "_key",
    value_column: str = "value",
    ttl: int | None = None,
    key_prefix: str = "",
    if_exists: str = "replace",
) -> int
```

Write a DataFrame to Redis as string values.

**Parameters:**

- `df`: DataFrame to write
- `url`: Redis connection URL
- `key_column`: Column with Redis keys, or `None` for auto-generated
- `value_column`: Column with values to write
- `ttl`: TTL in seconds (optional)
- `key_prefix`: Prefix for all keys
- `if_exists`: `"fail"`, `"replace"`, or `"append"`

**Returns:** Number of keys written

---

### write_sets

```python
def write_sets(
    df: pl.DataFrame,
    url: str,
    key_column: str | None = "_key",
    member_column: str = "member",
    ttl: int | None = None,
    key_prefix: str = "",
    if_exists: str = "replace",
) -> int
```

Write a DataFrame to Redis as sets.

**Parameters:**

- `df`: DataFrame to write
- `url`: Redis connection URL
- `key_column`: Column with Redis keys, or `None` for auto-generated
- `member_column`: Column with member values
- `ttl`: TTL in seconds (optional)
- `key_prefix`: Prefix for all keys
- `if_exists`: `"fail"`, `"replace"`, or `"append"`

**Returns:** Number of keys written

---

### write_lists

```python
def write_lists(
    df: pl.DataFrame,
    url: str,
    key_column: str | None = "_key",
    element_column: str = "element",
    ttl: int | None = None,
    key_prefix: str = "",
    if_exists: str = "replace",
) -> int
```

Write a DataFrame to Redis as lists.

**Parameters:**

- `df`: DataFrame to write
- `url`: Redis connection URL
- `key_column`: Column with Redis keys, or `None` for auto-generated
- `element_column`: Column with element values
- `ttl`: TTL in seconds (optional)
- `key_prefix`: Prefix for all keys
- `if_exists`: `"fail"`, `"replace"`, or `"append"`

**Returns:** Number of keys written

---

### write_zsets

```python
def write_zsets(
    df: pl.DataFrame,
    url: str,
    key_column: str | None = "_key",
    member_column: str = "member",
    score_column: str = "score",
    ttl: int | None = None,
    key_prefix: str = "",
    if_exists: str = "replace",
) -> int
```

Write a DataFrame to Redis as sorted sets.

**Parameters:**

- `df`: DataFrame to write
- `url`: Redis connection URL
- `key_column`: Column with Redis keys, or `None` for auto-generated
- `member_column`: Column with member values
- `score_column`: Column with score values
- `ttl`: TTL in seconds (optional)
- `key_prefix`: Prefix for all keys
- `if_exists`: `"fail"`, `"replace"`, or `"append"`

**Returns:** Number of keys written

---

## Schema Inference

### infer_hash_schema

```python
def infer_hash_schema(
    url: str,
    pattern: str = "*",
    *,
    sample_size: int = 100,
    type_inference: bool = True,
) -> dict[str, type[pl.DataType]]
```

Infer schema from Redis hashes by sampling keys.

**Parameters:**

- `url`: Redis connection URL
- `pattern`: Key pattern to sample
- `sample_size`: Maximum keys to sample
- `type_inference`: Infer types (vs all Utf8)

**Returns:** Dictionary mapping field names to Polars dtypes

---

### infer_json_schema

```python
def infer_json_schema(
    url: str,
    pattern: str = "*",
    *,
    sample_size: int = 100,
) -> dict[str, type[pl.DataType]]
```

Infer schema from RedisJSON documents by sampling keys.

**Parameters:**

- `url`: Redis connection URL
- `pattern`: Key pattern to sample
- `sample_size`: Maximum keys to sample

**Returns:** Dictionary mapping field names to Polars dtypes

---

## Utility Functions

### scan_keys

```python
def scan_keys(
    url: str,
    pattern: str = "*",
    count: int | None = None,
) -> list[str]
```

Scan Redis keys matching a pattern.

**Parameters:**

- `url`: Redis connection URL
- `pattern`: Key pattern to match
- `count`: Maximum keys to return (optional)

**Returns:** List of matching keys
