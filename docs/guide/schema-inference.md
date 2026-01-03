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

## Limitations

- Sampling may miss rare fields (increase `sample_size`)
- Type inference is based on sampled values only
- Mixed types in the same field default to `Utf8`
- Nested JSON structures are not supported (top-level fields only)
