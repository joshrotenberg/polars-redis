# Client Operations

Beyond DataFrame I/O, polars-redis provides ergonomic wrappers for common Redis operations. These live in the `client` module and are designed for bulk operations that complement your DataFrame workflows.

## Geospatial Operations

Work with Redis GEO commands for location-based data.

### Adding Locations

```python
import polars_redis as redis

url = "redis://localhost:6379"

# Add locations to a geo set
locations = [
    ("headquarters", -122.4194, 37.7749),  # San Francisco
    ("warehouse", -118.2437, 34.0522),      # Los Angeles
    ("office", -73.9857, 40.7484),          # New York
]

result = redis.geo_add(url, "locations", locations)
print(f"Added {result['added']} new, updated {result['updated']} existing")
```

### Radius Queries

Find locations within a radius:

```python
# Find locations within 500km of a point
nearby = redis.geo_radius(
    url,
    "locations",
    longitude=-122.4,
    latitude=37.8,
    radius=500,
    unit="km",
    count=10,       # Max results
    sort="ASC",     # Nearest first
)

for loc in nearby:
    print(f"{loc['name']}: {loc['distance']:.1f} km away")
```

Search from an existing member:

```python
# Find locations within 1000 miles of headquarters
nearby = redis.geo_radius_by_member(
    url,
    "locations",
    member="headquarters",
    radius=1000,
    unit="mi",
)
```

### Distance Calculations

```python
# Distance between two members
dist = redis.geo_dist(url, "locations", "headquarters", "warehouse", "km")
print(f"Distance: {dist:.1f} km")

# Distance matrix for multiple members
members = ["headquarters", "warehouse", "office"]
matrix = redis.geo_dist_matrix(url, "locations", members, "km")

# matrix[i][j] = distance from members[i] to members[j]
for i, m1 in enumerate(members):
    for j, m2 in enumerate(members):
        print(f"{m1} -> {m2}: {matrix[i][j]:.1f} km")
```

### Position and Geohash

```python
# Get coordinates
positions = redis.geo_pos(url, "locations", ["headquarters", "warehouse"])
for pos in positions:
    print(f"{pos['name']}: ({pos['longitude']}, {pos['latitude']})")

# Get geohashes
hashes = redis.geo_hash(url, "locations", ["headquarters", "warehouse"])
for name, hash in hashes:
    print(f"{name}: {hash}")
```

### Combining with DataFrames

```python
import polars as pl
import polars_redis as redis

url = "redis://localhost:6379"

# Get all stores within 50km of a customer
nearby_stores = redis.geo_radius(
    url, "stores", 
    longitude=customer_lon, 
    latitude=customer_lat,
    radius=50, unit="km"
)

# Convert to DataFrame for analysis
stores_df = pl.DataFrame(nearby_stores)

# Join with inventory data
inventory = redis.scan_hashes(url, "inventory:*", schema)

available = (
    stores_df
    .join(inventory.collect(), left_on="name", right_on="store_id")
    .filter(pl.col("quantity") > 0)
    .sort("distance")
)
```

## Key Management

Bulk operations for managing Redis keys.

### Key Information

```python
import polars_redis as redis

url = "redis://localhost:6379"

# Get info for all keys matching a pattern
info = redis.key_info(url, "user:*", include_memory=True)

for k in info:
    print(f"{k['key']}: type={k['key_type']}, ttl={k['ttl']}s, "
          f"memory={k['memory_usage']} bytes")
```

### Bulk TTL Operations

```python
# Set TTL for multiple keys
keys = ["session:1", "session:2", "session:3"]
result = redis.set_ttl(url, keys, ttl_seconds=3600)
print(f"Set TTL on {result['succeeded']} keys, {result['failed']} failed")

# Set different TTLs for each key
keys_and_ttls = [
    ("cache:hot", 300),      # 5 minutes
    ("cache:warm", 3600),    # 1 hour
    ("cache:cold", 86400),   # 1 day
]
result = redis.set_ttl_individual(url, keys_and_ttls)

# Remove TTL (make persistent)
result = redis.persist_keys(url, keys)

# Get current TTL
ttls = redis.get_ttl(url, keys)
for key, ttl in ttls:
    if ttl == -1:
        print(f"{key}: no expiry")
    elif ttl == -2:
        print(f"{key}: does not exist")
    else:
        print(f"{key}: expires in {ttl}s")
```

### Bulk Delete

```python
# Delete specific keys
result = redis.delete_keys(url, ["temp:1", "temp:2", "temp:3"])
print(f"Deleted {result['deleted']} keys")

# Delete by pattern (uses SCAN internally)
result = redis.delete_keys_pattern(url, "temp:*")
print(f"Deleted {result['deleted']} keys matching pattern")
```

### Bulk Rename

```python
# Rename multiple keys
renames = [
    ("old:user:1", "user:1"),
    ("old:user:2", "user:2"),
]
result = redis.rename_keys(url, renames)
print(f"Renamed {result['succeeded']} keys")

if result['errors']:
    for key, error in result['errors']:
        print(f"  Failed: {key} - {error}")
```

### Check Existence

```python
keys = ["user:1", "user:2", "user:999"]
exists = redis.exists_keys(url, keys)

for key, exists in exists:
    print(f"{key}: {'exists' if exists else 'missing'}")
```

## Pipelines and Transactions

Batch multiple operations for efficiency or atomicity.

### Pipelines

Pipelines send multiple commands in a single round-trip, reducing network latency:

```python
import polars_redis as redis

url = "redis://localhost:6379"

# Create a pipeline
pipe = redis.Pipeline(url)

# Queue operations (no network calls yet)
pipe.set("counter", "0")
pipe.incr("counter")
pipe.incr("counter")
pipe.hset("user:1", "name", "Alice")
pipe.hset("user:1", "age", "30")
pipe.get("counter")

# Execute all at once (single round-trip)
results = pipe.execute()

print(f"Executed {len(results)} commands")
print(f"Counter value: {results.get(5).value}")  # "2"
```

### Transactions

Transactions use MULTI/EXEC for atomic execution:

```python
# Create a transaction
tx = redis.Transaction(url)

# Queue atomic operations
tx.set("balance:alice", "100")
tx.set("balance:bob", "50")
tx.decrby("balance:alice", 25)
tx.incrby("balance:bob", 25)

# Execute atomically - all succeed or all fail
results = tx.execute()

if results.all_succeeded():
    print("Transfer complete")
else:
    print("Transfer failed")
```

### Available Commands

Both `Pipeline` and `Transaction` support:

**String operations:**
- `set(key, value)`, `set_ex(key, value, seconds)`
- `get(key)`, `mget(keys)`
- `incr(key)`, `incrby(key, amount)`, `decr(key)`

**Hash operations:**
- `hset(key, field, value)`, `hmset(key, fields_dict)`
- `hget(key, field)`, `hgetall(key)`
- `hdel(key, fields)`, `hincrby(key, field, amount)`

**List operations:**
- `lpush(key, values)`, `rpush(key, values)`
- `lrange(key, start, stop)`, `llen(key)`

**Set operations:**
- `sadd(key, members)`, `smembers(key)`
- `sismember(key, member)`, `scard(key)`

**Sorted set operations:**
- `zadd(key, members_with_scores)`
- `zrange(key, start, stop)`, `zscore(key, member)`, `zcard(key)`

**Key operations:**
- `delete(keys)`, `exists(keys)`
- `expire(key, seconds)`, `ttl(key)`
- `rename(key, new_key)`, `key_type(key)`

**Raw commands:**
- `raw(command, args)` - Execute any Redis command

### Pipeline with DataFrame Data

```python
import polars as pl
import polars_redis as redis

url = "redis://localhost:6379"

# DataFrame with updates
updates = pl.DataFrame({
    "user_id": ["user:1", "user:2", "user:3"],
    "score": [100, 85, 92],
})

# Batch update using pipeline
pipe = redis.Pipeline(url)

for row in updates.iter_rows(named=True):
    pipe.hset(row["user_id"], "score", str(row["score"]))

results = pipe.execute()
print(f"Updated {results.succeeded} users")
```

## Pub/Sub

Collect messages from Redis Pub/Sub channels into DataFrames.

### Basic Collection

```python
import polars_redis as redis

url = "redis://localhost:6379"

# Collect messages for 10 seconds
config = redis.PubSubConfig(
    channels=["events", "alerts"],
    timeout_seconds=10,
)

df = redis.collect_pubsub(url, config)
print(df)
# shape: (N, 3)
# +---------+---------+---------------------------+
# | channel | message | timestamp                 |
# +---------+---------+---------------------------+
# | events  | {...}   | 2024-01-01 12:00:00.123   |
# | alerts  | {...}   | 2024-01-01 12:00:00.456   |
# +---------+---------+---------------------------+
```

### Pattern Subscriptions

```python
# Subscribe to patterns
config = redis.PubSubConfig(
    patterns=["sensor:*", "device:*"],
    timeout_seconds=30,
    max_messages=1000,  # Stop after 1000 messages
)

df = redis.collect_pubsub(url, config)
```

### Processing Messages

```python
import polars as pl
import json

# Collect JSON messages
df = redis.collect_pubsub(url, config)

# Parse JSON payloads
parsed = df.with_columns(
    pl.col("message").str.json_decode().alias("data")
).unnest("data")

# Aggregate by channel
stats = (
    parsed
    .group_by("channel")
    .agg([
        pl.len().alias("message_count"),
        pl.col("value").mean().alias("avg_value"),
    ])
)
```

## When to Use What

| Task | Use |
|------|-----|
| Bulk location queries | `geo_radius()`, `geo_radius_by_member()` |
| Distance calculations | `geo_dist()`, `geo_dist_matrix()` |
| Key inventory/cleanup | `key_info()`, `delete_keys_pattern()` |
| Bulk TTL management | `set_ttl()`, `persist_keys()` |
| Batch operations (speed) | `Pipeline` |
| Atomic operations | `Transaction` |
| Real-time event collection | `collect_pubsub()` |

## Performance Tips

1. **Use pipelines for bulk operations** - A pipeline with 1000 commands is much faster than 1000 individual calls.

2. **Transactions have overhead** - Only use transactions when atomicity is required. Pipelines are faster for non-atomic batches.

3. **Geo queries are efficient** - Redis GEO uses sorted sets with geohashing. Radius queries are O(N+log(M)) where N is results and M is total members.

4. **Key info is expensive** - `key_info()` with `include_memory=True` runs MEMORY USAGE for each key. Use sparingly on large keyspaces.
