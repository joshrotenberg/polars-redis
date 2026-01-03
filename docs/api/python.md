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
