# Schema Inference

polars-redis can automatically infer schemas from existing Redis data.

## Inferring Hash Schema

`infer_hash_schema` samples Redis hashes to detect field names and types:

```python
import polars_redis as redis

schema = redis.infer_hash_schema(
    "redis://localhost:6379",
    pattern="user:*",
    sample_size=100,
)
print(schema)
# {'name': Utf8, 'age': Int64, 'score': Float64, 'active': Boolean}
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | str | required | Redis connection URL |
| `pattern` | str | `"*"` | Key pattern to sample |
| `sample_size` | int | `100` | Maximum keys to sample |
| `type_inference` | bool | `True` | Infer types (vs all Utf8) |

### Type Detection

When `type_inference=True`, values are analyzed:

| Pattern | Detected Type |
|---------|---------------|
| Integer strings (`"123"`) | `Int64` |
| Float strings (`"3.14"`) | `Float64` |
| Boolean strings (`"true"`, `"false"`) | `Boolean` |
| ISO dates (`"2024-01-15"`) | `Date` |
| ISO datetimes (`"2024-01-15T10:30:00"`) | `Datetime` |
| Everything else | `Utf8` |

### Without Type Inference

Set `type_inference=False` to treat all fields as strings:

```python
schema = redis.infer_hash_schema(
    url,
    pattern="user:*",
    type_inference=False,
)
# All fields are Utf8
```

## Inferring JSON Schema

`infer_json_schema` samples RedisJSON documents:

```python
schema = redis.infer_json_schema(
    "redis://localhost:6379",
    pattern="doc:*",
    sample_size=100,
)
print(schema)
# {'title': Utf8, 'views': Int64, 'rating': Float64}
```

JSON type inference uses native JSON types (number, string, boolean) rather than parsing strings.

## Using Inferred Schemas

Pass the inferred schema directly to scan functions:

```python
# Infer
schema = redis.infer_hash_schema(url, pattern="user:*")

# Scan
lf = redis.scan_hashes(url, pattern="user:*", schema=schema)
df = lf.collect()
```

## Sampling Strategy

Schema inference uses Redis SCAN to sample keys:

1. Keys matching the pattern are scanned
2. Up to `sample_size` keys are fetched
3. All unique field names are collected
4. Field types are inferred from sampled values

!!! tip
    For heterogeneous data, increase `sample_size` to capture more field variations.

## Handling Missing Fields

If some hashes have fields that others don't:

- All discovered fields are included in the schema
- Missing values become `null` when scanning

```python
# user:1 has {name, age}
# user:2 has {name, age, email}

schema = redis.infer_hash_schema(url, pattern="user:*")
# {'name': Utf8, 'age': Int64, 'email': Utf8}

df = redis.read_hashes(url, pattern="user:*", schema=schema)
# user:1 will have email=null
```

## Confidence Scores

For production use, you may want to validate the quality of inferred schemas before processing large datasets. The `infer_hash_schema_with_confidence` function provides detailed confidence information:

```python
result = redis.infer_hash_schema_with_confidence(
    "redis://localhost:6379",
    pattern="user:*",
    sample_size=100
)

# Check overall confidence
print(f"Average confidence: {result.average_confidence:.1%}")
print(f"All fields confident: {result.all_confident}")

# Get the schema if confidence is high
if result.all_confident:
    df = redis.read_hashes(url, pattern="user:*", schema=result.schema)
else:
    # Investigate low-confidence fields
    for field, conf in result.low_confidence_fields(threshold=0.8):
        print(f"Warning: {field} has {conf:.0%} confidence")
```

### SchemaConfidence Properties

| Property | Type | Description |
|----------|------|-------------|
| `schema` | `dict` | The inferred schema |
| `sample_count` | `int` | Number of keys sampled |
| `average_confidence` | `float` | Average confidence across all fields (0.0-1.0) |
| `all_confident` | `bool` | Whether all fields have confidence >= 0.9 |
| `field_info` | `dict` | Detailed per-field inference information |

### Per-Field Information

The `field_info` property provides detailed statistics for each field:

```python
for name, info in result.field_info.items():
    print(f"{name}:")
    print(f"  Type: {info['type']}")
    print(f"  Confidence: {info['confidence']:.1%}")
    print(f"  Samples: {info['samples']}")
    print(f"  Valid: {info['valid']}")
    print(f"  Nulls: {info['nulls']} ({info['null_ratio']:.1%})")
    print(f"  Type candidates: {info['type_candidates']}")
```

### Low Confidence Fields

Use `low_confidence_fields()` to identify fields that may need manual type specification:

```python
# Get fields with confidence below 80%
low_conf = result.low_confidence_fields(threshold=0.8)
for field, confidence in low_conf:
    print(f"{field}: {confidence:.0%} confidence")
```

## Schema Overwrite

When schema inference gets a type wrong or you need to enforce specific types, use the overwrite functions:

```python
# Infer schema but override specific field types
schema = redis.infer_hash_schema_with_overwrite(
    "redis://localhost:6379",
    pattern="user:*",
    schema_overwrite={
        "age": pl.Int64,           # Force age to Int64
        "created_at": pl.Datetime, # Force timestamp field
        "score": pl.Float64,       # Ensure float precision
    }
)
```

### Use Cases for Overwrite

- **Fix incorrect inference**: When a field looks like one type but should be another
- **Add missing fields**: Fields not in sampled data will be added
- **Force timestamp parsing**: Override string fields to `Datetime` or `Date`
- **Ensure numeric precision**: Override ambiguous numeric fields to specific types

### JSON Schema Overwrite

The same pattern works for JSON documents:

```python
schema = redis.infer_json_schema_with_overwrite(
    "redis://localhost:6379",
    pattern="doc:*",
    schema_overwrite={
        "timestamp": pl.Datetime,
        "count": pl.Int64,
    }
)
```

## Limitations

- Sampling may miss rare fields (increase `sample_size`)
- Type inference is based on sampled values only
- Mixed types in the same field default to `Utf8`
- Nested JSON structures are not supported (top-level fields only)
