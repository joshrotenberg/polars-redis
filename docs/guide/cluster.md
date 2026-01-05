# Redis Cluster Support

polars-redis supports Redis Cluster deployments, allowing you to scan data
distributed across multiple nodes with automatic key discovery and routing.

## Overview

Redis Cluster distributes keys across multiple nodes using hash slots.
Each master node owns a subset of the 16,384 hash slots. When scanning
a cluster:

1. polars-redis discovers all master nodes
2. SCAN is executed on each node sequentially
3. Keys are unique across nodes (no deduplication needed)
4. Data fetching uses cluster routing automatically

## Cluster Iterators

polars-redis provides cluster-aware iterators that handle multi-node scanning:

### ClusterHashBatchIterator

Scan hashes across all cluster nodes:

```python
from polars_redis import ClusterHashBatchIterator, HashSchema, BatchConfig, RedisType

# Define schema
schema = HashSchema([
    ("name", RedisType.Utf8),
    ("age", RedisType.Int64),
    ("active", RedisType.Boolean),
]).with_key(True)

# Configure batch settings
config = BatchConfig("user:*").with_batch_size(1000).with_count_hint(100)

# Connect to cluster nodes
nodes = [
    "redis://node1:7000",
    "redis://node2:7001", 
    "redis://node3:7002",
]

iterator = ClusterHashBatchIterator(nodes, schema, config)

# Iterate batches
while True:
    batch = iterator.next_batch()
    if batch is None:
        break
    # Process Arrow RecordBatch
    print(f"Got {batch.num_rows()} rows")
```

### ClusterStringBatchIterator

Scan string keys across the cluster:

```python
from polars_redis import ClusterStringBatchIterator, StringSchema, BatchConfig
from arrow import DataType

schema = StringSchema(DataType.utf8()).with_key(True)
config = BatchConfig("cache:*").with_batch_size(500)

iterator = ClusterStringBatchIterator(nodes, schema, config)

while True:
    batch = iterator.next_batch()
    if batch is None:
        break
    print(f"Got {batch.num_rows()} rows")
```

## Rust API

For Rust applications, use the cluster iterators directly:

```rust
use polars_redis::{
    ClusterHashBatchIterator, HashSchema, BatchConfig, RedisType,
};

// Define schema
let schema = HashSchema::new(vec![
    ("name".to_string(), RedisType::Utf8),
    ("age".to_string(), RedisType::Int64),
])
.with_key(true);

// Configure batch
let config = BatchConfig::new("user:*".to_string())
    .with_batch_size(1000)
    .with_count_hint(100);

// Create cluster iterator
let nodes = &["redis://node1:7000", "redis://node2:7001", "redis://node3:7002"];
let mut iterator = ClusterHashBatchIterator::new(nodes, schema, config, None)?;

// Process batches
while let Some(batch) = iterator.next_batch()? {
    println!("Got {} rows", batch.num_rows());
}
```

## Connection URLs

Cluster nodes are specified as a list of Redis URLs:

```python
# Standard Redis URLs
nodes = [
    "redis://node1:7000",
    "redis://node2:7001",
    "redis://node3:7002",
]

# With authentication
nodes = [
    "redis://:password@node1:7000",
    "redis://:password@node2:7001",
    "redis://:password@node3:7002",
]
```

You only need to specify a subset of nodes - redis-rs will discover the full
cluster topology automatically.

## Key Distribution

In Redis Cluster, keys are distributed by hash slot. The pattern you scan
will match keys on all nodes:

```python
# Scans "user:*" across all nodes
config = BatchConfig("user:*")

# Each node returns only keys in its slots
# Results are combined automatically
```

## Limitations

### SCAN Ordering

Keys are returned grouped by node, not by key name. If you need sorted
results, sort after collection:

```python
import polars as pl

# Collect all batches
batches = []
while True:
    batch = iterator.next_batch()
    if batch is None:
        break
    batches.append(pl.from_arrow(batch))

# Combine and sort
df = pl.concat(batches).sort("_key")
```

### Topology Changes

If the cluster reshards during a scan (nodes added/removed, slots migrated),
some keys may be missed or duplicated. For consistent scans, avoid topology
changes during the operation.

### Module Availability

If using Redis modules (RedisJSON, RediSearch, RedisTimeSeries), ensure they
are installed on all cluster nodes.

### RediSearch in Cluster

RediSearch indexes are local to each node. For cluster-wide search:

1. Each node must have the index created
2. Use `FT.SEARCH` with cluster routing (handled by redis-rs)
3. Results are aggregated across nodes

## Best Practices

### Use Sufficient Initial Nodes

Provide at least 2-3 node URLs for redundancy:

```python
# Good: multiple initial nodes
nodes = ["redis://n1:7000", "redis://n2:7001", "redis://n3:7002"]

# Risky: single node (fails if node is down)
nodes = ["redis://n1:7000"]
```

### Batch Size Tuning

Cluster operations have more network overhead. Consider larger batch sizes:

```python
# Larger batches reduce round-trips across nodes
config = BatchConfig("user:*").with_batch_size(2000)
```

### Connection Pooling

ClusterConnection handles connection pooling internally. Creating multiple
iterators reuses connections efficiently.

### Monitor Node Health

Use cluster info commands to verify cluster health before large scans:

```bash
redis-cli -c -h node1 -p 7000 CLUSTER INFO
```

## Example: Full Cluster Scan

Complete example scanning and processing cluster data:

```python
import polars as pl
from polars_redis import (
    ClusterHashBatchIterator,
    HashSchema,
    BatchConfig,
    RedisType,
)

# Cluster configuration
CLUSTER_NODES = [
    "redis://redis-cluster-1:7000",
    "redis://redis-cluster-2:7001",
    "redis://redis-cluster-3:7002",
]

# Schema definition
schema = HashSchema([
    ("user_id", RedisType.Utf8),
    ("name", RedisType.Utf8),
    ("email", RedisType.Utf8),
    ("signup_date", RedisType.Date),
    ("purchases", RedisType.Int64),
]).with_key(True).with_ttl(True)

# Batch configuration
config = (
    BatchConfig("user:*")
    .with_batch_size(1000)
    .with_count_hint(100)
)

# Create iterator
iterator = ClusterHashBatchIterator(CLUSTER_NODES, schema, config)

# Collect all data
batches = []
while True:
    batch = iterator.next_batch()
    if batch is None:
        break
    batches.append(pl.from_arrow(batch))

# Combine into DataFrame
if batches:
    df = pl.concat(batches)
    
    # Analyze
    summary = (
        df
        .group_by(pl.col("signup_date").dt.month().alias("month"))
        .agg([
            pl.len().alias("user_count"),
            pl.col("purchases").sum().alias("total_purchases"),
            pl.col("purchases").mean().alias("avg_purchases"),
        ])
        .sort("month")
    )
    
    print(summary)
```

## Troubleshooting

### Connection Errors

If you see `ClusterConnectionNotFound`:

1. Verify all nodes are running: `redis-cli -c CLUSTER INFO`
2. Check node URLs are accessible from your application
3. Ensure firewall allows connections to cluster ports

### Missing Keys

If fewer keys than expected are returned:

1. Verify the pattern matches keys on all nodes
2. Check that SCAN completed on all nodes (not interrupted)
3. Ensure no cluster resharding occurred during scan

### Performance Issues

If cluster scans are slow:

1. Increase batch size to reduce round-trips
2. Ensure nodes are geographically close to application
3. Check cluster health for rebalancing operations
