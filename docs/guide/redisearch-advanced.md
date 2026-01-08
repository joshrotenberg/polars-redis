# Advanced Search Options

For fine-grained control over search behavior, use `SearchOptions`. This is useful for full-text search applications that need highlighting, summarization, or custom scoring.

## SearchOptions

```python
from polars_redis.options import SearchOptions
import polars_redis as pr

opts = SearchOptions(
    index="articles_idx",
    query="python programming",
    verbatim=True,           # Disable stemming
    language="english",       # Stemming language
    scorer="BM25",           # Scoring algorithm
    dialect=4,               # RediSearch dialect
)

df = pr.search_hashes(
    "redis://localhost:6379",
    options=opts,
    schema={"title": pl.Utf8, "body": pl.Utf8},
).collect()
```

## Highlighting

Wrap matching terms in custom tags for display:

```python
opts = SearchOptions(
    index="articles_idx",
    query="python",
).with_highlight(
    fields=["title", "body"],
    open_tag="<em>",
    close_tag="</em>",
)

df = pr.search_hashes(url, options=opts, schema=schema).collect()
# Results have matching terms wrapped: "<em>Python</em> is a great language"
```

## Summarization

Generate text snippets with matched terms (useful for search result previews):

```python
opts = SearchOptions(
    index="articles_idx",
    query="machine learning",
).with_summarize(
    fields=["body"],
    frags=3,          # Number of fragments
    len=30,           # Fragment length in words
    separator="...",  # Between fragments
)
```

## Relevance Scores

Include relevance scores in results:

```python
opts = SearchOptions(
    index="articles_idx",
    query="python tutorial",
).with_score(True, "_relevance")

df = pr.search_hashes(url, options=opts, schema=schema).collect()
# Results include _relevance column with BM25 scores
```

## Query Modifiers Reference

| Option | Description |
|--------|-------------|
| `verbatim` | Disable stemming for exact term matching |
| `no_stopwords` | Include stop words in the query |
| `language` | Language for stemming (e.g., "english", "spanish", "french") |
| `scorer` | Scoring function: "BM25", "TFIDF", "DISMAX" |
| `expander` | Query expander: "SYNONYM" |
| `slop` | Default slop for phrase queries |
| `in_order` | Require phrase terms in order |
| `dialect` | RediSearch dialect version (1-4) |

## Filtering Options Reference

| Option | Description |
|--------|-------------|
| `in_keys` | Limit search to specific document keys |
| `in_fields` | Limit search to specific fields |
| `timeout_ms` | Query timeout in milliseconds |

## Smart Scan

`smart_scan()` automatically detects whether a RediSearch index exists and optimizes query execution accordingly.

### Basic Usage

```python
from polars_redis import smart_scan

# Auto-detect index and use FT.SEARCH if available
df = smart_scan(
    url,
    "user:*",
    schema={"name": pl.Utf8, "age": pl.Int64},
).filter(pl.col("age") > 30).collect()
```

If an index exists for the pattern, it uses FT.SEARCH. Otherwise, it falls back to SCAN.

### Query Explanation

See how a query will execute before running it:

```python
from polars_redis import explain_scan

plan = explain_scan(url, "user:*", schema={"name": pl.Utf8})
print(plan.explain())
# Strategy: SEARCH
# Index: users_idx
#   Prefixes: user:
#   Type: HASH
# Server Query: *
```

### Execution Strategies

| Strategy | When Used | Description |
|----------|-----------|-------------|
| **SEARCH** | Index found | Uses FT.SEARCH for server-side filtering |
| **SCAN** | No index | Falls back to Redis SCAN |
| **HYBRID** | Partial pushdown | FT.SEARCH + client-side filtering |

### Index Discovery

```python
from polars_redis import list_indexes, find_index_for_pattern

# List all RediSearch indexes
indexes = list_indexes(url)
for idx in indexes:
    print(f"{idx.name}: prefixes={idx.prefixes}")

# Find index for specific pattern
idx = find_index_for_pattern(url, "user:*")
if idx:
    print(f"Found index: {idx.name}")
```

### Explicit Index Control

```python
# Use a specific index by name
df = smart_scan(
    url, "user:*",
    schema={"name": pl.Utf8},
    index="users_idx",
    auto_detect_index=False,
)

# Use an Index object (auto-creates if needed)
from polars_redis import Index, TextField, NumericField

idx = Index(
    name="users_idx",
    prefix="user:",
    schema=[TextField("name"), NumericField("age")],
)

df = smart_scan(url, "user:*", schema=schema, index=idx).collect()
```

### Graceful Degradation

When RediSearch is unavailable, `smart_scan` falls back to SCAN without errors:

```python
# Works whether RediSearch is available or not
df = smart_scan(url, "user:*", schema=schema).collect()
```

## search_hashes Parameters Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | str | required | Redis connection URL |
| `index` | str | required | RediSearch index name |
| `query` | str or Expr | `"*"` | Query string or expression |
| `schema` | dict | required | Field names to Polars dtypes |
| `include_key` | bool | `True` | Include Redis key as column |
| `key_column_name` | str | `"_key"` | Name of key column |
| `include_ttl` | bool | `False` | Include TTL as column |
| `ttl_column_name` | str | `"_ttl"` | Name of TTL column |
| `batch_size` | int | `1000` | Documents per batch |
| `sort_by` | str | `None` | Field to sort by |
| `sort_ascending` | bool | `True` | Sort direction |
| `options` | SearchOptions | `None` | Advanced search options |

## smart_scan Parameters Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | str | required | Redis connection URL |
| `pattern` | str | `"*"` | Key pattern to match |
| `schema` | dict | required | Field names to Polars dtypes |
| `index` | str or Index | `None` | Force use of specific index |
| `include_key` | bool | `True` | Include Redis key as column |
| `key_column_name` | str | `"_key"` | Name of key column |
| `include_ttl` | bool | `False` | Include TTL as column |
| `ttl_column_name` | str | `"_ttl"` | Name of TTL column |
| `batch_size` | int | `1000` | Documents per batch |
| `auto_detect_index` | bool | `True` | Auto-detect matching indexes |
