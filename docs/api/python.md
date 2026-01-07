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

## Detailed Write Functions

### write_hashes_detailed

```python
def write_hashes_detailed(
    df: pl.DataFrame,
    url: str,
    key_column: str | None = "_key",
    ttl: int | None = None,
    key_prefix: str = "",
    if_exists: str = "replace",
) -> WriteResult
```

Write a DataFrame to Redis as hashes with detailed error reporting.

**Parameters:** Same as `write_hashes`.

**Returns:** `WriteResult` object with per-key success/failure information.

---

### WriteResult

```python
class WriteResult:
    keys_written: int      # Number of keys successfully written
    keys_failed: int       # Number of keys that failed
    keys_skipped: int      # Number of keys skipped (if_exists="fail")
    succeeded_keys: list[str]  # List of successfully written keys
    failed_keys: list[str]     # List of keys that failed
    errors: dict[str, str]     # Map of failed keys to error messages

    def is_complete_success(self) -> bool:
        """Check if all keys were written successfully."""
```

---

## Query Builder

### col

```python
def col(name: str) -> Expr
```

Create a column expression for building RediSearch queries.

**Parameters:**

- `name`: Field name to query

**Returns:** `Expr` object

---

### cols

```python
def cols(*names: str) -> MultiFieldExpr
```

Create a multi-field expression for searching across multiple fields.

**Parameters:**

- `*names`: Field names to search across

**Returns:** `MultiFieldExpr` object

---

### raw

```python
def raw(query: str) -> Expr
```

Create a raw RediSearch query expression.

**Parameters:**

- `query`: Raw RediSearch query string

**Returns:** `Expr` object

---

### Expr Methods

#### Comparison Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `>` | Greater than | `col("age") > 30` |
| `>=` | Greater or equal | `col("age") >= 30` |
| `<` | Less than | `col("age") < 30` |
| `<=` | Less or equal | `col("age") <= 30` |
| `==` | Equal | `col("status") == "active"` |
| `!=` | Not equal | `col("status") != "deleted"` |

#### Logical Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `&` | AND | `(col("a") > 1) & (col("b") < 2)` |
| `\|` | OR | `(col("a") == 1) \| (col("a") == 2)` |
| `~` | NOT | `~(col("status") == "deleted")` |

#### Range and Membership

```python
Expr.is_between(lower, upper) -> Expr
Expr.is_in(values: list) -> Expr
```

#### Text Search

```python
Expr.contains(text: str) -> Expr           # Full-text search
Expr.starts_with(prefix: str) -> Expr      # Prefix match
Expr.ends_with(suffix: str) -> Expr        # Suffix match
Expr.contains_substring(text: str) -> Expr # Infix/substring match
Expr.matches(pattern: str) -> Expr         # Wildcard match
Expr.matches_exact(pattern: str) -> Expr   # Exact wildcard match
Expr.fuzzy(term: str, distance: int = 1) -> Expr  # Fuzzy match (distance 1-3)
Expr.phrase(*words: str, slop: int = None, inorder: bool = None) -> Expr
```

#### Tag Operations

```python
Expr.has_tag(tag: str) -> Expr
Expr.has_any_tag(tags: list[str]) -> Expr
```

#### Geo Operations

```python
Expr.within_radius(lon: float, lat: float, radius: float, unit: str = "km") -> Expr
Expr.within_polygon(points: list[tuple[float, float]]) -> Expr
```

#### Vector Search

```python
Expr.knn(k: int, vector_param: str = "query_vec") -> Expr
Expr.vector_range(radius: float, vector_param: str = "query_vec") -> Expr
```

#### Relevance

```python
Expr.boost(weight: float) -> Expr    # Increase relevance weight
Expr.optional() -> Expr              # Mark as optional (prefer but don't require)
```

#### Null Checks

```python
Expr.is_null() -> Expr
Expr.is_not_null() -> Expr
```

#### Query Output

```python
Expr.to_redis() -> str  # Convert to RediSearch query string
```

---

### MultiFieldExpr Methods

```python
MultiFieldExpr.contains(text: str) -> Expr      # Search across all fields
MultiFieldExpr.starts_with(prefix: str) -> Expr # Prefix match across fields
```

---

## DataFrame Caching

### cache_dataframe

```python
def cache_dataframe(
    df: pl.DataFrame,
    url: str,
    key: str,
    *,
    format: Literal["ipc", "parquet"] = "ipc",
    compression: str | None = None,
    compression_level: int | None = None,
    ttl: int | None = None,
) -> int
```

Cache a DataFrame in Redis using Arrow IPC or Parquet format.

**Parameters:**

- `df`: DataFrame to cache
- `url`: Redis connection URL
- `key`: Redis key for storage
- `format`: Serialization format (`"ipc"` or `"parquet"`)
- `compression`: Compression codec (IPC: lz4, zstd; Parquet: snappy, gzip, lz4, zstd)
- `compression_level`: Compression level (codec-specific)
- `ttl`: TTL in seconds (optional)

**Returns:** Number of bytes written

---

### get_cached_dataframe

```python
def get_cached_dataframe(
    url: str,
    key: str,
    *,
    format: Literal["ipc", "parquet"] = "ipc",
    columns: list[str] | None = None,
    n_rows: int | None = None,
) -> pl.DataFrame | None
```

Retrieve a cached DataFrame from Redis.

**Parameters:**

- `url`: Redis connection URL
- `key`: Redis key to retrieve
- `format`: Serialization format (must match cache_dataframe)
- `columns`: Columns to read (Parquet only)
- `n_rows`: Maximum rows to read (Parquet only)

**Returns:** DataFrame or None if key doesn't exist

---

### scan_cached

```python
def scan_cached(
    url: str,
    key: str,
    *,
    format: Literal["ipc", "parquet"] = "ipc",
) -> pl.LazyFrame | None
```

Retrieve cached data as a LazyFrame.

**Returns:** LazyFrame or None if key doesn't exist

---

### delete_cached

```python
def delete_cached(url: str, key: str) -> bool
```

Delete a cached DataFrame from Redis.

**Returns:** True if deleted, False if key didn't exist

---

### cache_exists

```python
def cache_exists(url: str, key: str) -> bool
```

Check if a cached DataFrame exists in Redis.

---

### cache_ttl

```python
def cache_ttl(url: str, key: str) -> int | None
```

Get the remaining TTL of a cached DataFrame.

**Returns:** TTL in seconds, or None if no TTL or key doesn't exist

---

## Index Management

### Index

```python
class Index:
    def __init__(
        self,
        name: str,
        prefix: str | list[str] = "",
        schema: list[Field] = [],
        on: Literal["HASH", "JSON"] = "HASH",
        stopwords: list[str] | None = None,
        language: str | None = None,
        language_field: str | None = None,
        score: float | None = None,
        score_field: str | None = None,
        payload_field: str | None = None,
        maxtextfields: bool = False,
        nooffsets: bool = False,
        nohl: bool = False,
        nofields: bool = False,
        nofreqs: bool = False,
        skipinitialscan: bool = False,
    )
```

RediSearch index definition.

**Parameters:**

- `name`: Index name
- `prefix`: Key prefix(es) to index
- `schema`: List of field definitions
- `on`: Data type (`"HASH"` or `"JSON"`)
- `stopwords`: Custom stopwords list (empty list disables)
- `language`: Default language for stemming
- `language_field`: Field containing per-document language
- `score`: Default document score
- `score_field`: Field containing per-document score
- `payload_field`: Field to use as document payload
- `maxtextfields`: Optimize for many TEXT fields
- `nooffsets`: Don't store term offsets (saves memory)
- `nohl`: Don't store data for highlighting
- `nofields`: Don't store field names
- `nofreqs`: Don't store term frequencies
- `skipinitialscan`: Don't scan existing keys when creating

**Methods:**

```python
Index.create(url: str, *, if_not_exists: bool = False) -> None
Index.drop(url: str, *, delete_docs: bool = False) -> None
Index.exists(url: str) -> bool
Index.ensure_exists(url: str, *, recreate: bool = False) -> Index
Index.info(url: str) -> IndexInfo | None
Index.diff(url: str) -> IndexDiff
Index.migrate(url: str, *, drop_existing: bool = False) -> bool
Index.validate_schema(schema: dict) -> list[str]

# Class methods
Index.from_frame(df, name, prefix, *, text_fields=None, sortable=None, on="HASH") -> Index
Index.from_schema(schema, name, prefix, *, text_fields=None, sortable=None, on="HASH") -> Index
Index.from_redis(url: str, name: str) -> Index | None
```

---

### Field Types

#### TextField

```python
class TextField(Field):
    def __init__(
        self,
        name: str,
        sortable: bool = False,
        nostem: bool = False,
        weight: float = 1.0,
        phonetic: str | None = None,
        noindex: bool = False,
        withsuffixtrie: bool = False,
    )
```

Full-text search field with stemming and scoring.

---

#### NumericField

```python
class NumericField(Field):
    def __init__(
        self,
        name: str,
        sortable: bool = False,
        noindex: bool = False,
    )
```

Numeric field for range queries.

---

#### TagField

```python
class TagField(Field):
    def __init__(
        self,
        name: str,
        separator: str = ",",
        casesensitive: bool = False,
        sortable: bool = False,
        noindex: bool = False,
        withsuffixtrie: bool = False,
    )
```

Exact-match field for categories and tags.

---

#### GeoField

```python
class GeoField(Field):
    def __init__(
        self,
        name: str,
        noindex: bool = False,
    )
```

Geographic field for radius queries.

---

#### VectorField

```python
class VectorField(Field):
    def __init__(
        self,
        name: str,
        algorithm: Literal["FLAT", "HNSW"] = "HNSW",
        dim: int = 384,
        distance_metric: Literal["COSINE", "L2", "IP"] = "COSINE",
        initial_cap: int | None = None,
        m: int | None = None,
        ef_construction: int | None = None,
        ef_runtime: int | None = None,
        block_size: int | None = None,
    )
```

Vector field for similarity search.

---

#### GeoShapeField

```python
class GeoShapeField(Field):
    def __init__(
        self,
        name: str,
        coord_system: Literal["SPHERICAL", "FLAT"] = "SPHERICAL",
    )
```

Polygon/geometry field for complex geo queries.

---

### IndexInfo

```python
class IndexInfo:
    name: str
    num_docs: int
    max_doc_id: int
    num_terms: int
    num_records: int
    inverted_sz_mb: float
    total_inverted_index_blocks: int
    offset_vectors_sz_mb: float
    doc_table_size_mb: float
    sortable_values_size_mb: float
    key_table_size_mb: float
    records_per_doc_avg: float
    bytes_per_record_avg: float
    offsets_per_term_avg: float
    offset_bits_per_record_avg: float
    hash_indexing_failures: int
    indexing: bool
    percent_indexed: float
    fields: list[dict]
    prefixes: list[str]
    on_type: str
```

Information about an existing RediSearch index.

---

### IndexDiff

```python
class IndexDiff:
    added: list[Field]              # Fields to be added
    removed: list[str]              # Fields to be removed
    changed: dict[str, tuple]       # Fields with type changes
    unchanged: list[str]            # Unchanged fields

    @property
    def has_changes(self) -> bool
```

Differences between desired and existing index schemas.

---

## Smart Scan (Auto-detection)

### smart_scan

```python
def smart_scan(
    url: str,
    pattern: str = "*",
    schema: dict | None = None,
    *,
    index: str | Index | None = None,
    include_key: bool = True,
    key_column_name: str = "_key",
    include_ttl: bool = False,
    ttl_column_name: str = "_ttl",
    batch_size: int = 1000,
    auto_detect_index: bool = True,
) -> pl.LazyFrame
```

Smart scan that automatically detects and uses RediSearch indexes when available.

**Parameters:**

- `url`: Redis connection URL
- `pattern`: Key pattern to match (e.g., `"user:*"`)
- `schema`: Dictionary mapping field names to Polars dtypes (required)
- `index`: Force use of specific index (name or Index object)
- `include_key`: Include Redis key as a column
- `key_column_name`: Name of the key column
- `include_ttl`: Include TTL as a column
- `ttl_column_name`: Name of the TTL column
- `batch_size`: Documents per batch
- `auto_detect_index`: Auto-detect matching indexes (default: True)

**Returns:** `pl.LazyFrame`

---

### explain_scan

```python
def explain_scan(
    url: str,
    pattern: str = "*",
    schema: dict | None = None,
    filter_expr: pl.Expr | None = None,
) -> QueryPlan
```

Explain how a scan would be executed without running it.

**Parameters:**

- `url`: Redis connection URL
- `pattern`: Key pattern to match
- `schema`: Schema dictionary
- `filter_expr`: Optional Polars filter expression

**Returns:** `QueryPlan` object

---

### find_index_for_pattern

```python
def find_index_for_pattern(url: str, pattern: str) -> DetectedIndex | None
```

Find a RediSearch index that covers the given key pattern.

**Parameters:**

- `url`: Redis connection URL
- `pattern`: Key pattern (e.g., `"user:*"`)

**Returns:** `DetectedIndex` if found, `None` otherwise

---

### list_indexes

```python
def list_indexes(url: str) -> list[DetectedIndex]
```

List all RediSearch indexes.

**Parameters:**

- `url`: Redis connection URL

**Returns:** List of `DetectedIndex` objects

---

### ExecutionStrategy

```python
class ExecutionStrategy(Enum):
    SEARCH = "search"   # Use FT.SEARCH with index
    SCAN = "scan"       # Use SCAN without index
    HYBRID = "hybrid"   # Use FT.SEARCH + client-side filtering
```

Enum representing query execution strategies.

---

### DetectedIndex

```python
@dataclass
class DetectedIndex:
    name: str           # Index name
    prefixes: list[str] # Key prefixes covered
    on_type: str        # "HASH" or "JSON"
    fields: list[str]   # Indexed field names
```

Information about an auto-detected index.

---

### QueryPlan

```python
@dataclass
class QueryPlan:
    strategy: ExecutionStrategy
    index: DetectedIndex | None = None
    server_query: str | None = None
    client_filters: list[str] = []
    warnings: list[str] = []

    def explain(self) -> str:
        """Return human-readable explanation of the query plan."""
```

Execution plan for a query.

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
