"""Integration tests for polars-redis (require running Redis)."""

from __future__ import annotations

import os

import polars as pl
import polars_redis
import pytest


def redis_available() -> bool:
    """Check if Redis is available for testing."""
    try:
        keys = polars_redis.scan_keys("redis://localhost:6379", "*", count=1)
        return True
    except Exception:
        return False


# Skip all tests in this module if Redis is not available
pytestmark = pytest.mark.skipif(
    not redis_available(),
    reason="Redis not available at localhost:6379",
)


@pytest.fixture(scope="module")
def redis_url() -> str:
    """Get Redis URL from environment or use default."""
    return os.environ.get("REDIS_URL", "redis://localhost:6379")


@pytest.fixture(scope="module", autouse=True)
def setup_test_data(redis_url: str) -> None:
    """Set up test data in Redis before running tests."""
    import subprocess

    # Create test hashes using redis-cli with proper escaping
    for i in range(1, 51):
        active = "true" if i % 2 == 0 else "false"
        subprocess.run(
            [
                "redis-cli",
                "HSET",
                f"test:user:{i}",
                "name",
                f"TestUser{i}",
                "age",
                str(20 + i),
                "email",
                f"testuser{i}@example.com",
                "active",
                active,
            ],
            capture_output=True,
        )

    # Add some hashes with missing fields
    subprocess.run(
        ["redis-cli", "HSET", "test:partial:1", "name", "Partial1", "age", "30"],
        capture_output=True,
    )
    subprocess.run(
        ["redis-cli", "HSET", "test:partial:2", "name", "Partial2", "email", "partial@example.com"],
        capture_output=True,
    )

    yield

    # Cleanup - delete all test keys
    result = subprocess.run(
        ["redis-cli", "KEYS", "test:*"],
        capture_output=True,
        text=True,
    )
    for key in result.stdout.strip().split("\n"):
        if key:
            subprocess.run(["redis-cli", "DEL", key], capture_output=True)


class TestScanKeys:
    """Tests for scan_keys function."""

    def test_scan_keys_basic(self, redis_url: str) -> None:
        """Test basic key scanning."""
        keys = polars_redis.scan_keys(redis_url, "test:user:*", count=100)
        assert len(keys) == 50

    def test_scan_keys_with_limit(self, redis_url: str) -> None:
        """Test key scanning with a limit."""
        keys = polars_redis.scan_keys(redis_url, "test:user:*", count=10)
        assert len(keys) == 10

    def test_scan_keys_no_match(self, redis_url: str) -> None:
        """Test scanning with pattern that matches nothing."""
        keys = polars_redis.scan_keys(redis_url, "nonexistent:*", count=10)
        assert len(keys) == 0


class TestScanHashes:
    """Tests for scan_hashes function."""

    def test_scan_hashes_basic(self, redis_url: str) -> None:
        """Test basic hash scanning."""
        lf = polars_redis.scan_hashes(
            redis_url,
            pattern="test:user:*",
            schema={
                "name": pl.Utf8,
                "age": pl.Int64,
                "email": pl.Utf8,
                "active": pl.Boolean,
            },
        )

        df = lf.collect()
        assert len(df) == 50
        assert df.columns == ["_key", "name", "age", "email", "active"]

    def test_scan_hashes_schema_types(self, redis_url: str) -> None:
        """Test that schema types are correctly applied."""
        lf = polars_redis.scan_hashes(
            redis_url,
            pattern="test:user:*",
            schema={
                "name": pl.Utf8,
                "age": pl.Int64,
                "active": pl.Boolean,
            },
        )

        df = lf.collect()
        assert df.schema["name"] == pl.Utf8
        assert df.schema["age"] == pl.Int64
        assert df.schema["active"] == pl.Boolean

    def test_scan_hashes_without_key(self, redis_url: str) -> None:
        """Test scanning without the key column."""
        lf = polars_redis.scan_hashes(
            redis_url,
            pattern="test:user:*",
            schema={"name": pl.Utf8, "age": pl.Int64},
            include_key=False,
        )

        df = lf.collect()
        assert "_key" not in df.columns
        assert df.columns == ["name", "age"]

    def test_scan_hashes_custom_key_column(self, redis_url: str) -> None:
        """Test scanning with a custom key column name."""
        lf = polars_redis.scan_hashes(
            redis_url,
            pattern="test:user:*",
            schema={"name": pl.Utf8},
            key_column_name="redis_key",
        )

        df = lf.collect()
        assert "redis_key" in df.columns
        assert "_key" not in df.columns

    def test_scan_hashes_projection_pushdown(self, redis_url: str) -> None:
        """Test that projection pushdown works."""
        lf = polars_redis.scan_hashes(
            redis_url,
            pattern="test:user:*",
            schema={
                "name": pl.Utf8,
                "age": pl.Int64,
                "email": pl.Utf8,
            },
        )

        # Select only name and age
        df = lf.select(["name", "age"]).collect()
        assert df.columns == ["name", "age"]
        assert len(df) == 50

    def test_scan_hashes_filter(self, redis_url: str) -> None:
        """Test filtering (client-side)."""
        lf = polars_redis.scan_hashes(
            redis_url,
            pattern="test:user:*",
            schema={
                "name": pl.Utf8,
                "age": pl.Int64,
            },
        )

        # Filter to ages > 50
        # Ages are 21-70 (20 + i for i in 1..50), so > 50 means 51-70 = 20 users
        df = lf.filter(pl.col("age") > 50).collect()
        assert len(df) == 20
        assert all(age > 50 for age in df["age"].to_list())

    def test_scan_hashes_limit(self, redis_url: str) -> None:
        """Test row limit."""
        lf = polars_redis.scan_hashes(
            redis_url,
            pattern="test:user:*",
            schema={"name": pl.Utf8},
        )

        df = lf.head(10).collect()
        assert len(df) == 10

    def test_scan_hashes_sort(self, redis_url: str) -> None:
        """Test sorting results."""
        lf = polars_redis.scan_hashes(
            redis_url,
            pattern="test:user:*",
            schema={"name": pl.Utf8, "age": pl.Int64},
        )

        df = lf.sort("age").collect()
        ages = df["age"].to_list()
        assert ages == sorted(ages)

    def test_scan_hashes_aggregation(self, redis_url: str) -> None:
        """Test aggregation on scanned data."""
        lf = polars_redis.scan_hashes(
            redis_url,
            pattern="test:user:*",
            schema={
                "age": pl.Int64,
                "active": pl.Boolean,
            },
        )

        # Group by active status and get average age
        df = lf.group_by("active").agg(pl.col("age").mean().alias("avg_age")).collect()
        assert len(df) == 2
        assert "avg_age" in df.columns

    def test_scan_hashes_no_matches(self, redis_url: str) -> None:
        """Test scanning pattern with no matches."""
        lf = polars_redis.scan_hashes(
            redis_url,
            pattern="nonexistent:*",
            schema={"name": pl.Utf8},
        )

        df = lf.collect()
        assert len(df) == 0

    def test_scan_hashes_requires_schema(self, redis_url: str) -> None:
        """Test that schema is required."""
        with pytest.raises(ValueError, match="schema is required"):
            polars_redis.scan_hashes(redis_url, pattern="test:*")


class TestNullHandling:
    """Tests for null/missing field handling."""

    def test_missing_fields_as_null(self, redis_url: str) -> None:
        """Test that missing fields are represented as null."""
        lf = polars_redis.scan_hashes(
            redis_url,
            pattern="test:partial:*",
            schema={
                "name": pl.Utf8,
                "age": pl.Int64,
                "email": pl.Utf8,
            },
        )

        df = lf.collect()
        assert len(df) == 2

        # Check that we have nulls where expected
        partial1 = df.filter(pl.col("name") == "Partial1")
        partial2 = df.filter(pl.col("name") == "Partial2")

        # Partial1 has name and age, but no email
        assert partial1["age"][0] == 30
        assert partial1["email"][0] is None

        # Partial2 has name and email, but no age
        assert partial2["email"][0] == "partial@example.com"
        assert partial2["age"][0] is None

    def test_all_fields_missing(self, redis_url: str) -> None:
        """Test handling of hash with none of the requested fields."""
        import subprocess

        # Create a hash with different fields than what we're looking for
        subprocess.run(
            ["redis-cli", "HSET", "test:empty:1", "other_field", "value"],
            capture_output=True,
        )

        try:
            lf = polars_redis.scan_hashes(
                redis_url,
                pattern="test:empty:*",
                schema={
                    "name": pl.Utf8,
                    "age": pl.Int64,
                },
            )

            df = lf.collect()
            assert len(df) == 1
            assert df["name"][0] is None
            assert df["age"][0] is None
        finally:
            subprocess.run(["redis-cli", "DEL", "test:empty:1"], capture_output=True)


class TestScanJson:
    """Tests for scan_json function."""

    @pytest.fixture(autouse=True)
    def setup_json_data(self, redis_url: str) -> None:
        """Set up JSON test data in Redis."""
        import subprocess

        # Create test JSON documents using redis-cli
        for i in range(1, 21):
            active = "true" if i % 2 == 0 else "false"
            json_data = f'{{"title":"Document{i}","author":"Author{i}","views":{i * 100},"published":{active}}}'
            subprocess.run(
                ["redis-cli", "JSON.SET", f"test:doc:{i}", "$", json_data],
                capture_output=True,
            )

        # Add a document with missing fields
        subprocess.run(
            ["redis-cli", "JSON.SET", "test:doc:partial", "$", '{"title":"PartialDoc"}'],
            capture_output=True,
        )

        yield

        # Cleanup
        for i in range(1, 21):
            subprocess.run(["redis-cli", "DEL", f"test:doc:{i}"], capture_output=True)
        subprocess.run(["redis-cli", "DEL", "test:doc:partial"], capture_output=True)

    def test_scan_json_basic(self, redis_url: str) -> None:
        """Test basic JSON document scanning."""
        lf = polars_redis.scan_json(
            redis_url,
            pattern="test:doc:[0-9]*",
            schema={
                "title": pl.Utf8,
                "author": pl.Utf8,
                "views": pl.Int64,
                "published": pl.Boolean,
            },
        )

        df = lf.collect()
        assert len(df) == 20
        assert df.columns == ["_key", "title", "author", "views", "published"]

    def test_scan_json_schema_types(self, redis_url: str) -> None:
        """Test that schema types are correctly applied."""
        lf = polars_redis.scan_json(
            redis_url,
            pattern="test:doc:[0-9]*",
            schema={
                "title": pl.Utf8,
                "views": pl.Int64,
                "published": pl.Boolean,
            },
        )

        df = lf.collect()
        assert df.schema["title"] == pl.Utf8
        assert df.schema["views"] == pl.Int64
        assert df.schema["published"] == pl.Boolean

    def test_scan_json_without_key(self, redis_url: str) -> None:
        """Test scanning without the key column."""
        lf = polars_redis.scan_json(
            redis_url,
            pattern="test:doc:[0-9]*",
            schema={"title": pl.Utf8, "views": pl.Int64},
            include_key=False,
        )

        df = lf.collect()
        assert "_key" not in df.columns
        assert df.columns == ["title", "views"]

    def test_scan_json_filter(self, redis_url: str) -> None:
        """Test filtering (client-side)."""
        lf = polars_redis.scan_json(
            redis_url,
            pattern="test:doc:[0-9]*",
            schema={
                "title": pl.Utf8,
                "views": pl.Int64,
            },
        )

        # Filter to views > 1000 (docs 11-20 have views 1100-2000)
        df = lf.filter(pl.col("views") > 1000).collect()
        assert len(df) == 10
        assert all(v > 1000 for v in df["views"].to_list())

    def test_scan_json_projection(self, redis_url: str) -> None:
        """Test that projection pushdown works."""
        lf = polars_redis.scan_json(
            redis_url,
            pattern="test:doc:[0-9]*",
            schema={
                "title": pl.Utf8,
                "author": pl.Utf8,
                "views": pl.Int64,
            },
        )

        df = lf.select(["title", "views"]).collect()
        assert df.columns == ["title", "views"]

    def test_scan_json_missing_fields(self, redis_url: str) -> None:
        """Test that missing fields are represented as null."""
        lf = polars_redis.scan_json(
            redis_url,
            pattern="test:doc:partial",
            schema={
                "title": pl.Utf8,
                "author": pl.Utf8,
                "views": pl.Int64,
            },
        )

        df = lf.collect()
        assert len(df) == 1
        assert df["title"][0] == "PartialDoc"
        assert df["author"][0] is None
        assert df["views"][0] is None

    def test_scan_json_requires_schema(self, redis_url: str) -> None:
        """Test that schema is required."""
        with pytest.raises(ValueError, match="schema is required"):
            polars_redis.scan_json(redis_url, pattern="test:*")


class TestPyJsonBatchIterator:
    """Tests for the low-level PyJsonBatchIterator."""

    @pytest.fixture(autouse=True)
    def setup_json_data(self, redis_url: str) -> None:
        """Set up JSON test data in Redis."""
        import subprocess

        for i in range(1, 11):
            json_data = f'{{"name":"Item{i}","count":{i}}}'
            subprocess.run(
                ["redis-cli", "JSON.SET", f"test:item:{i}", "$", json_data],
                capture_output=True,
            )

        yield

        for i in range(1, 11):
            subprocess.run(["redis-cli", "DEL", f"test:item:{i}"], capture_output=True)

    def test_json_iterator_basic(self, redis_url: str) -> None:
        """Test basic JSON iterator functionality."""
        iterator = polars_redis.PyJsonBatchIterator(
            url=redis_url,
            pattern="test:item:*",
            schema=[("name", "utf8"), ("count", "int64")],
            batch_size=5,
        )

        assert not iterator.is_done()

        batches = []
        while not iterator.is_done():
            ipc_bytes = iterator.next_batch_ipc()
            if ipc_bytes is None:
                break
            batches.append(pl.read_ipc(ipc_bytes))

        total_rows = sum(len(b) for b in batches)
        assert total_rows == 10

    def test_json_iterator_max_rows(self, redis_url: str) -> None:
        """Test JSON iterator with max_rows limit."""
        iterator = polars_redis.PyJsonBatchIterator(
            url=redis_url,
            pattern="test:item:*",
            schema=[("name", "utf8")],
            max_rows=5,
        )

        total_rows = 0
        while not iterator.is_done():
            ipc_bytes = iterator.next_batch_ipc()
            if ipc_bytes is None:
                break
            total_rows += len(pl.read_ipc(ipc_bytes))

        assert total_rows == 5


class TestPyHashBatchIterator:
    """Tests for the low-level PyHashBatchIterator."""

    def test_iterator_basic(self, redis_url: str) -> None:
        """Test basic iterator functionality."""
        iterator = polars_redis.PyHashBatchIterator(
            url=redis_url,
            pattern="test:user:*",
            schema=[("name", "utf8"), ("age", "int64")],
            batch_size=10,
        )

        assert not iterator.is_done()

        batches = []
        while not iterator.is_done():
            ipc_bytes = iterator.next_batch_ipc()
            if ipc_bytes is None:
                break
            batches.append(pl.read_ipc(ipc_bytes))

        total_rows = sum(len(b) for b in batches)
        assert total_rows == 50

    def test_iterator_projection(self, redis_url: str) -> None:
        """Test iterator with projection."""
        iterator = polars_redis.PyHashBatchIterator(
            url=redis_url,
            pattern="test:user:*",
            schema=[("name", "utf8"), ("age", "int64"), ("email", "utf8")],
            projection=["name", "age"],
            include_key=False,
        )

        ipc_bytes = iterator.next_batch_ipc()
        df = pl.read_ipc(ipc_bytes)
        assert df.columns == ["name", "age"]

    def test_iterator_max_rows(self, redis_url: str) -> None:
        """Test iterator with max_rows limit."""
        iterator = polars_redis.PyHashBatchIterator(
            url=redis_url,
            pattern="test:user:*",
            schema=[("name", "utf8")],
            max_rows=15,
        )

        total_rows = 0
        while not iterator.is_done():
            ipc_bytes = iterator.next_batch_ipc()
            if ipc_bytes is None:
                break
            total_rows += len(pl.read_ipc(ipc_bytes))

        assert total_rows == 15


class TestInferSchema:
    """Tests for schema inference functions."""

    def test_infer_hash_schema_basic(self, redis_url: str) -> None:
        """Test basic hash schema inference."""
        schema = polars_redis.infer_hash_schema(
            redis_url,
            pattern="test:user:*",
            sample_size=10,
        )

        assert isinstance(schema, dict)
        assert "name" in schema
        assert "age" in schema
        assert "email" in schema
        assert "active" in schema

    def test_infer_hash_schema_types(self, redis_url: str) -> None:
        """Test that type inference works correctly."""
        schema = polars_redis.infer_hash_schema(
            redis_url,
            pattern="test:user:*",
            sample_size=10,
            type_inference=True,
        )

        # age should be inferred as Int64
        assert schema["age"] == pl.Int64
        # name and email should be Utf8
        assert schema["name"] == pl.Utf8
        assert schema["email"] == pl.Utf8
        # active should be Boolean (stored as "true"/"false")
        assert schema["active"] == pl.Boolean

    def test_infer_hash_schema_no_type_inference(self, redis_url: str) -> None:
        """Test schema inference without type inference."""
        schema = polars_redis.infer_hash_schema(
            redis_url,
            pattern="test:user:*",
            sample_size=10,
            type_inference=False,
        )

        # All fields should be Utf8 when type_inference=False
        assert schema["age"] == pl.Utf8
        assert schema["name"] == pl.Utf8
        assert schema["active"] == pl.Utf8

    def test_infer_hash_schema_empty_pattern(self, redis_url: str) -> None:
        """Test schema inference with pattern matching no keys."""
        schema = polars_redis.infer_hash_schema(
            redis_url,
            pattern="nonexistent:*",
            sample_size=10,
        )

        assert schema == {}

    def test_infer_hash_schema_use_with_scan(self, redis_url: str) -> None:
        """Test using inferred schema with scan_hashes."""
        # First infer the schema
        schema = polars_redis.infer_hash_schema(
            redis_url,
            pattern="test:user:*",
            sample_size=10,
        )

        # Then use it to scan
        df = polars_redis.read_hashes(
            redis_url,
            pattern="test:user:*",
            schema=schema,
        )

        assert len(df) == 50
        assert df.schema["age"] == pl.Int64


class TestInferJsonSchema:
    """Tests for JSON schema inference."""

    @pytest.fixture(autouse=True)
    def setup_json_data(self, redis_url: str) -> None:
        """Set up JSON test data in Redis."""
        import subprocess

        for i in range(1, 11):
            active = "true" if i % 2 == 0 else "false"
            rating = 3.5 + (i * 0.1)
            json_data = (
                f'{{"title":"Doc{i}","views":{i * 100},"rating":{rating},"featured":{active}}}'
            )
            subprocess.run(
                ["redis-cli", "JSON.SET", f"test:infer:doc:{i}", "$", json_data],
                capture_output=True,
            )

        yield

        for i in range(1, 11):
            subprocess.run(["redis-cli", "DEL", f"test:infer:doc:{i}"], capture_output=True)

    def test_infer_json_schema_basic(self, redis_url: str) -> None:
        """Test basic JSON schema inference."""
        schema = polars_redis.infer_json_schema(
            redis_url,
            pattern="test:infer:doc:*",
            sample_size=10,
        )

        assert isinstance(schema, dict)
        assert "title" in schema
        assert "views" in schema
        assert "rating" in schema
        assert "featured" in schema

    def test_infer_json_schema_types(self, redis_url: str) -> None:
        """Test that JSON type inference works correctly."""
        schema = polars_redis.infer_json_schema(
            redis_url,
            pattern="test:infer:doc:*",
            sample_size=10,
        )

        # title should be Utf8
        assert schema["title"] == pl.Utf8
        # views should be Int64
        assert schema["views"] == pl.Int64
        # rating should be Float64
        assert schema["rating"] == pl.Float64
        # featured should be Boolean
        assert schema["featured"] == pl.Boolean

    def test_infer_json_schema_empty_pattern(self, redis_url: str) -> None:
        """Test JSON schema inference with pattern matching no keys."""
        schema = polars_redis.infer_json_schema(
            redis_url,
            pattern="nonexistent:*",
            sample_size=10,
        )

        assert schema == {}

    def test_infer_json_schema_use_with_scan(self, redis_url: str) -> None:
        """Test using inferred schema with scan_json."""
        # First infer the schema
        schema = polars_redis.infer_json_schema(
            redis_url,
            pattern="test:infer:doc:*",
            sample_size=10,
        )

        # Then use it to scan
        df = polars_redis.read_json(
            redis_url,
            pattern="test:infer:doc:*",
            schema=schema,
        )

        assert len(df) == 10
        assert df.schema["views"] == pl.Int64
        assert df.schema["rating"] == pl.Float64


class TestReadEager:
    """Tests for eager read functions (read_hashes, read_json)."""

    def test_read_hashes_basic(self, redis_url: str) -> None:
        """Test basic eager hash reading."""
        df = polars_redis.read_hashes(
            redis_url,
            pattern="test:user:*",
            schema={
                "name": pl.Utf8,
                "age": pl.Int64,
            },
        )

        # Should return a DataFrame, not a LazyFrame
        assert isinstance(df, pl.DataFrame)
        assert len(df) == 50
        assert "_key" in df.columns
        assert "name" in df.columns
        assert "age" in df.columns

    def test_read_hashes_without_key(self, redis_url: str) -> None:
        """Test eager hash reading without key column."""
        df = polars_redis.read_hashes(
            redis_url,
            pattern="test:user:*",
            schema={"name": pl.Utf8},
            include_key=False,
        )

        assert isinstance(df, pl.DataFrame)
        assert "_key" not in df.columns
        assert df.columns == ["name"]

    def test_read_hashes_empty_result(self, redis_url: str) -> None:
        """Test eager hash reading with no matches."""
        df = polars_redis.read_hashes(
            redis_url,
            pattern="nonexistent:*",
            schema={"name": pl.Utf8},
        )

        assert isinstance(df, pl.DataFrame)
        assert len(df) == 0

    def test_read_hashes_requires_schema(self, redis_url: str) -> None:
        """Test that read_hashes requires a schema."""
        with pytest.raises(ValueError, match="schema is required"):
            polars_redis.read_hashes(redis_url, pattern="test:*")

    def test_read_json_basic(self, redis_url: str) -> None:
        """Test basic eager JSON reading."""
        # Set up test JSON data
        import subprocess

        for i in range(1, 6):
            json_data = f'{{"name":"Item{i}","value":{i * 10}}}'
            subprocess.run(
                ["redis-cli", "JSON.SET", f"test:eager:json:{i}", "$", json_data],
                capture_output=True,
            )

        try:
            df = polars_redis.read_json(
                redis_url,
                pattern="test:eager:json:*",
                schema={
                    "name": pl.Utf8,
                    "value": pl.Int64,
                },
            )

            assert isinstance(df, pl.DataFrame)
            assert len(df) == 5
            assert "_key" in df.columns
            assert "name" in df.columns
            assert "value" in df.columns
        finally:
            for i in range(1, 6):
                subprocess.run(
                    ["redis-cli", "DEL", f"test:eager:json:{i}"],
                    capture_output=True,
                )

    def test_read_json_requires_schema(self, redis_url: str) -> None:
        """Test that read_json requires a schema."""
        with pytest.raises(ValueError, match="schema is required"):
            polars_redis.read_json(redis_url, pattern="test:*")


class TestWriteHashes:
    """Tests for write_hashes function."""

    def test_write_hashes_basic(self, redis_url: str) -> None:
        """Test basic hash writing."""
        import subprocess

        df = pl.DataFrame(
            {
                "_key": ["test:write:hash:1", "test:write:hash:2"],
                "name": ["Alice", "Bob"],
                "age": [30, 25],
            }
        )

        try:
            count = polars_redis.write_hashes(df, redis_url)
            assert count == 2

            # Verify data was written
            result = subprocess.run(
                ["redis-cli", "HGETALL", "test:write:hash:1"],
                capture_output=True,
                text=True,
            )
            output = result.stdout.strip().split("\n")
            # Should have name and age fields
            assert "name" in output
            assert "Alice" in output
            assert "age" in output
            assert "30" in output
        finally:
            subprocess.run(["redis-cli", "DEL", "test:write:hash:1"], capture_output=True)
            subprocess.run(["redis-cli", "DEL", "test:write:hash:2"], capture_output=True)

    def test_write_hashes_custom_key_column(self, redis_url: str) -> None:
        """Test writing hashes with custom key column."""
        import subprocess

        df = pl.DataFrame(
            {
                "redis_key": ["test:write:custom:1"],
                "value": ["test_value"],
            }
        )

        try:
            count = polars_redis.write_hashes(df, redis_url, key_column="redis_key")
            assert count == 1

            result = subprocess.run(
                ["redis-cli", "HGET", "test:write:custom:1", "value"],
                capture_output=True,
                text=True,
            )
            assert result.stdout.strip() == "test_value"
        finally:
            subprocess.run(["redis-cli", "DEL", "test:write:custom:1"], capture_output=True)

    def test_write_hashes_missing_key_column(self, redis_url: str) -> None:
        """Test that missing key column raises error."""
        df = pl.DataFrame({"name": ["Alice"], "age": [30]})

        with pytest.raises(ValueError, match="Key column '_key' not found"):
            polars_redis.write_hashes(df, redis_url)

    def test_write_hashes_with_nulls(self, redis_url: str) -> None:
        """Test writing hashes with null values."""
        import subprocess

        df = pl.DataFrame(
            {
                "_key": ["test:write:null:1"],
                "name": ["Alice"],
                "optional": [None],
            }
        )

        try:
            count = polars_redis.write_hashes(df, redis_url)
            assert count == 1

            # Null fields should not be written
            result = subprocess.run(
                ["redis-cli", "HGET", "test:write:null:1", "optional"],
                capture_output=True,
                text=True,
            )
            # Empty string means field doesn't exist
            assert result.stdout.strip() == ""
        finally:
            subprocess.run(["redis-cli", "DEL", "test:write:null:1"], capture_output=True)

    def test_write_hashes_roundtrip(self, redis_url: str) -> None:
        """Test write then read roundtrip."""
        import subprocess

        original = pl.DataFrame(
            {
                "_key": ["test:roundtrip:hash:1", "test:roundtrip:hash:2"],
                "name": ["Alice", "Bob"],
                "score": [95, 87],
            }
        )

        try:
            # Write
            count = polars_redis.write_hashes(original, redis_url)
            assert count == 2

            # Read back
            result = polars_redis.read_hashes(
                redis_url,
                pattern="test:roundtrip:hash:*",
                schema={"name": pl.Utf8, "score": pl.Int64},
            )

            assert len(result) == 2
            # Sort to compare
            result = result.sort("_key")
            assert result["name"].to_list() == ["Alice", "Bob"]
            assert result["score"].to_list() == [95, 87]
        finally:
            subprocess.run(["redis-cli", "DEL", "test:roundtrip:hash:1"], capture_output=True)
            subprocess.run(["redis-cli", "DEL", "test:roundtrip:hash:2"], capture_output=True)


class TestScanStrings:
    """Tests for scan_strings function."""

    @pytest.fixture(autouse=True)
    def setup_string_data(self, redis_url: str) -> None:
        """Set up string test data in Redis."""
        import subprocess

        # Create test string values
        for i in range(1, 21):
            # String values
            subprocess.run(
                ["redis-cli", "SET", f"test:cache:{i}", f"cached_value_{i}"],
                capture_output=True,
            )
            # Counter values (integers)
            subprocess.run(
                ["redis-cli", "SET", f"test:counter:{i}", str(i * 10)],
                capture_output=True,
            )
            # Float values
            subprocess.run(
                ["redis-cli", "SET", f"test:price:{i}", f"{i * 1.5:.2f}"],
                capture_output=True,
            )
            # Boolean flags
            flag = "true" if i % 2 == 0 else "false"
            subprocess.run(
                ["redis-cli", "SET", f"test:flag:{i}", flag],
                capture_output=True,
            )

        yield

        # Cleanup
        for i in range(1, 21):
            subprocess.run(["redis-cli", "DEL", f"test:cache:{i}"], capture_output=True)
            subprocess.run(["redis-cli", "DEL", f"test:counter:{i}"], capture_output=True)
            subprocess.run(["redis-cli", "DEL", f"test:price:{i}"], capture_output=True)
            subprocess.run(["redis-cli", "DEL", f"test:flag:{i}"], capture_output=True)

    def test_scan_strings_basic(self, redis_url: str) -> None:
        """Test basic string scanning as UTF-8."""
        lf = polars_redis.scan_strings(
            redis_url,
            pattern="test:cache:*",
        )

        df = lf.collect()
        assert len(df) == 20
        assert df.columns == ["_key", "value"]
        assert df.schema["_key"] == pl.Utf8
        assert df.schema["value"] == pl.Utf8

    def test_scan_strings_as_int64(self, redis_url: str) -> None:
        """Test scanning string values as integers."""
        lf = polars_redis.scan_strings(
            redis_url,
            pattern="test:counter:*",
            value_type=pl.Int64,
        )

        df = lf.collect()
        assert len(df) == 20
        assert df.schema["value"] == pl.Int64
        # Sum should be 10 + 20 + ... + 200 = 10 * (1+2+...+20) = 10 * 210 = 2100
        assert df["value"].sum() == 2100

    def test_scan_strings_as_float64(self, redis_url: str) -> None:
        """Test scanning string values as floats."""
        lf = polars_redis.scan_strings(
            redis_url,
            pattern="test:price:*",
            value_type=pl.Float64,
        )

        df = lf.collect()
        assert len(df) == 20
        assert df.schema["value"] == pl.Float64
        # All values should be positive floats
        assert all(v > 0 for v in df["value"].to_list())

    def test_scan_strings_as_boolean(self, redis_url: str) -> None:
        """Test scanning string values as booleans."""
        lf = polars_redis.scan_strings(
            redis_url,
            pattern="test:flag:*",
            value_type=pl.Boolean,
        )

        df = lf.collect()
        assert len(df) == 20
        assert df.schema["value"] == pl.Boolean
        # Half should be true (even numbers), half false
        assert df["value"].sum() == 10

    def test_scan_strings_without_key(self, redis_url: str) -> None:
        """Test scanning without the key column."""
        lf = polars_redis.scan_strings(
            redis_url,
            pattern="test:cache:*",
            include_key=False,
        )

        df = lf.collect()
        assert "_key" not in df.columns
        assert df.columns == ["value"]

    def test_scan_strings_custom_column_names(self, redis_url: str) -> None:
        """Test scanning with custom column names."""
        lf = polars_redis.scan_strings(
            redis_url,
            pattern="test:counter:*",
            value_type=pl.Int64,
            key_column_name="redis_key",
            value_column_name="count",
        )

        df = lf.collect()
        assert "redis_key" in df.columns
        assert "count" in df.columns
        assert "_key" not in df.columns
        assert "value" not in df.columns

    def test_scan_strings_filter(self, redis_url: str) -> None:
        """Test filtering string values."""
        lf = polars_redis.scan_strings(
            redis_url,
            pattern="test:counter:*",
            value_type=pl.Int64,
        )

        # Filter to values > 100 (counters 11-20 have values 110-200)
        df = lf.filter(pl.col("value") > 100).collect()
        assert len(df) == 10
        assert all(v > 100 for v in df["value"].to_list())

    def test_scan_strings_limit(self, redis_url: str) -> None:
        """Test row limit."""
        lf = polars_redis.scan_strings(
            redis_url,
            pattern="test:cache:*",
        )

        df = lf.head(5).collect()
        assert len(df) == 5

    def test_scan_strings_aggregation(self, redis_url: str) -> None:
        """Test aggregation on scanned data."""
        lf = polars_redis.scan_strings(
            redis_url,
            pattern="test:counter:*",
            value_type=pl.Int64,
        )

        df = lf.select(
            [
                pl.col("value").sum().alias("total"),
                pl.col("value").mean().alias("average"),
                pl.col("value").max().alias("max_val"),
            ]
        ).collect()

        assert df["total"][0] == 2100
        assert df["average"][0] == 105.0
        assert df["max_val"][0] == 200

    def test_scan_strings_no_matches(self, redis_url: str) -> None:
        """Test scanning pattern with no matches."""
        lf = polars_redis.scan_strings(
            redis_url,
            pattern="nonexistent:*",
        )

        df = lf.collect()
        assert len(df) == 0


class TestReadStrings:
    """Tests for read_strings function."""

    @pytest.fixture(autouse=True)
    def setup_string_data(self, redis_url: str) -> None:
        """Set up string test data in Redis."""
        import subprocess

        for i in range(1, 11):
            subprocess.run(
                ["redis-cli", "SET", f"test:read:str:{i}", f"value_{i}"],
                capture_output=True,
            )

        yield

        for i in range(1, 11):
            subprocess.run(["redis-cli", "DEL", f"test:read:str:{i}"], capture_output=True)

    def test_read_strings_basic(self, redis_url: str) -> None:
        """Test basic eager string reading."""
        df = polars_redis.read_strings(
            redis_url,
            pattern="test:read:str:*",
        )

        assert isinstance(df, pl.DataFrame)
        assert len(df) == 10
        assert "_key" in df.columns
        assert "value" in df.columns

    def test_read_strings_empty_result(self, redis_url: str) -> None:
        """Test eager string reading with no matches."""
        df = polars_redis.read_strings(
            redis_url,
            pattern="nonexistent:*",
        )

        assert isinstance(df, pl.DataFrame)
        assert len(df) == 0


class TestPyStringBatchIterator:
    """Tests for the low-level PyStringBatchIterator."""

    @pytest.fixture(autouse=True)
    def setup_string_data(self, redis_url: str) -> None:
        """Set up string test data in Redis."""
        import subprocess

        for i in range(1, 26):
            subprocess.run(
                ["redis-cli", "SET", f"test:iter:str:{i}", str(i * 5)],
                capture_output=True,
            )

        yield

        for i in range(1, 26):
            subprocess.run(["redis-cli", "DEL", f"test:iter:str:{i}"], capture_output=True)

    def test_string_iterator_basic(self, redis_url: str) -> None:
        """Test basic string iterator functionality."""
        iterator = polars_redis.PyStringBatchIterator(
            url=redis_url,
            pattern="test:iter:str:*",
            value_type="int64",
            batch_size=10,
        )

        assert not iterator.is_done()

        batches = []
        while not iterator.is_done():
            ipc_bytes = iterator.next_batch_ipc()
            if ipc_bytes is None:
                break
            batches.append(pl.read_ipc(ipc_bytes))

        total_rows = sum(len(b) for b in batches)
        assert total_rows == 25

    def test_string_iterator_max_rows(self, redis_url: str) -> None:
        """Test string iterator with max_rows limit."""
        iterator = polars_redis.PyStringBatchIterator(
            url=redis_url,
            pattern="test:iter:str:*",
            value_type="utf8",
            max_rows=10,
        )

        total_rows = 0
        while not iterator.is_done():
            ipc_bytes = iterator.next_batch_ipc()
            if ipc_bytes is None:
                break
            total_rows += len(pl.read_ipc(ipc_bytes))

        assert total_rows == 10

    def test_string_iterator_custom_columns(self, redis_url: str) -> None:
        """Test string iterator with custom column names."""
        iterator = polars_redis.PyStringBatchIterator(
            url=redis_url,
            pattern="test:iter:str:*",
            value_type="int64",
            key_column_name="key",
            value_column_name="num",
            batch_size=5,
        )

        ipc_bytes = iterator.next_batch_ipc()
        df = pl.read_ipc(ipc_bytes)
        assert "key" in df.columns
        assert "num" in df.columns


class TestWriteJson:
    """Tests for write_json function."""

    def test_write_json_basic(self, redis_url: str) -> None:
        """Test basic JSON writing."""
        import subprocess

        df = pl.DataFrame(
            {
                "_key": ["test:write:json:1", "test:write:json:2"],
                "title": ["Hello", "World"],
                "views": [100, 200],
            }
        )

        try:
            count = polars_redis.write_json(df, redis_url)
            assert count == 2

            # Verify data was written
            result = subprocess.run(
                ["redis-cli", "JSON.GET", "test:write:json:1"],
                capture_output=True,
                text=True,
            )
            import json

            data = json.loads(result.stdout.strip())
            assert data["title"] == "Hello"
            assert data["views"] == 100
        finally:
            subprocess.run(["redis-cli", "DEL", "test:write:json:1"], capture_output=True)
            subprocess.run(["redis-cli", "DEL", "test:write:json:2"], capture_output=True)

    def test_write_json_custom_key_column(self, redis_url: str) -> None:
        """Test writing JSON with custom key column."""
        import subprocess

        df = pl.DataFrame(
            {
                "doc_key": ["test:write:json:custom:1"],
                "content": ["test_content"],
            }
        )

        try:
            count = polars_redis.write_json(df, redis_url, key_column="doc_key")
            assert count == 1

            result = subprocess.run(
                ["redis-cli", "JSON.GET", "test:write:json:custom:1", "$.content"],
                capture_output=True,
                text=True,
            )
            import json

            data = json.loads(result.stdout.strip())
            assert data[0] == "test_content"
        finally:
            subprocess.run(["redis-cli", "DEL", "test:write:json:custom:1"], capture_output=True)

    def test_write_json_missing_key_column(self, redis_url: str) -> None:
        """Test that missing key column raises error."""
        df = pl.DataFrame({"title": ["Hello"], "views": [100]})

        with pytest.raises(ValueError, match="Key column '_key' not found"):
            polars_redis.write_json(df, redis_url)

    def test_write_json_preserves_types(self, redis_url: str) -> None:
        """Test that JSON writing preserves native types."""
        import subprocess

        df = pl.DataFrame(
            {
                "_key": ["test:write:json:types:1"],
                "string_val": ["hello"],
                "int_val": [42],
                "float_val": [3.14],
                "bool_val": [True],
            }
        )

        try:
            count = polars_redis.write_json(df, redis_url)
            assert count == 1

            result = subprocess.run(
                ["redis-cli", "JSON.GET", "test:write:json:types:1"],
                capture_output=True,
                text=True,
            )
            import json

            data = json.loads(result.stdout.strip())
            assert data["string_val"] == "hello"
            assert data["int_val"] == 42
            assert data["float_val"] == 3.14
            assert data["bool_val"] is True
        finally:
            subprocess.run(["redis-cli", "DEL", "test:write:json:types:1"], capture_output=True)

    def test_write_json_roundtrip(self, redis_url: str) -> None:
        """Test write then read roundtrip."""
        import subprocess

        original = pl.DataFrame(
            {
                "_key": ["test:roundtrip:json:1", "test:roundtrip:json:2"],
                "title": ["Doc1", "Doc2"],
                "count": [10, 20],
            }
        )

        try:
            # Write
            count = polars_redis.write_json(original, redis_url)
            assert count == 2

            # Read back
            result = polars_redis.read_json(
                redis_url,
                pattern="test:roundtrip:json:*",
                schema={"title": pl.Utf8, "count": pl.Int64},
            )

            assert len(result) == 2
            # Sort to compare
            result = result.sort("_key")
            assert result["title"].to_list() == ["Doc1", "Doc2"]
            assert result["count"].to_list() == [10, 20]
        finally:
            subprocess.run(["redis-cli", "DEL", "test:roundtrip:json:1"], capture_output=True)
            subprocess.run(["redis-cli", "DEL", "test:roundtrip:json:2"], capture_output=True)


class TestWriteStrings:
    """Tests for write_strings function."""

    def test_write_strings_basic(self, redis_url: str) -> None:
        """Test basic string writing."""
        import subprocess

        df = pl.DataFrame(
            {
                "_key": ["test:write:str:1", "test:write:str:2"],
                "value": ["hello", "world"],
            }
        )

        try:
            count = polars_redis.write_strings(df, redis_url)
            assert count == 2

            # Verify data was written
            result = subprocess.run(
                ["redis-cli", "GET", "test:write:str:1"],
                capture_output=True,
                text=True,
            )
            assert result.stdout.strip() == "hello"

            result = subprocess.run(
                ["redis-cli", "GET", "test:write:str:2"],
                capture_output=True,
                text=True,
            )
            assert result.stdout.strip() == "world"
        finally:
            subprocess.run(["redis-cli", "DEL", "test:write:str:1"], capture_output=True)
            subprocess.run(["redis-cli", "DEL", "test:write:str:2"], capture_output=True)

    def test_write_strings_custom_columns(self, redis_url: str) -> None:
        """Test writing strings with custom column names."""
        import subprocess

        df = pl.DataFrame(
            {
                "redis_key": ["test:write:str:custom:1"],
                "data": ["custom_value"],
            }
        )

        try:
            count = polars_redis.write_strings(
                df, redis_url, key_column="redis_key", value_column="data"
            )
            assert count == 1

            result = subprocess.run(
                ["redis-cli", "GET", "test:write:str:custom:1"],
                capture_output=True,
                text=True,
            )
            assert result.stdout.strip() == "custom_value"
        finally:
            subprocess.run(["redis-cli", "DEL", "test:write:str:custom:1"], capture_output=True)

    def test_write_strings_missing_key_column(self, redis_url: str) -> None:
        """Test that missing key column raises error."""
        df = pl.DataFrame({"value": ["hello"]})

        with pytest.raises(ValueError, match="Key column '_key' not found"):
            polars_redis.write_strings(df, redis_url)

    def test_write_strings_missing_value_column(self, redis_url: str) -> None:
        """Test that missing value column raises error."""
        df = pl.DataFrame({"_key": ["test:key"]})

        with pytest.raises(ValueError, match="Value column 'value' not found"):
            polars_redis.write_strings(df, redis_url)

    def test_write_strings_with_nulls(self, redis_url: str) -> None:
        """Test that null values are skipped."""
        import subprocess

        df = pl.DataFrame(
            {
                "_key": ["test:write:str:null:1", "test:write:str:null:2"],
                "value": ["valid", None],
            }
        )

        try:
            count = polars_redis.write_strings(df, redis_url)
            # Only non-null values should be written
            assert count == 1

            # Verify the valid key was written
            result = subprocess.run(
                ["redis-cli", "GET", "test:write:str:null:1"],
                capture_output=True,
                text=True,
            )
            assert result.stdout.strip() == "valid"

            # Verify the null key was not written
            result = subprocess.run(
                ["redis-cli", "EXISTS", "test:write:str:null:2"],
                capture_output=True,
                text=True,
            )
            assert result.stdout.strip() == "0"
        finally:
            subprocess.run(["redis-cli", "DEL", "test:write:str:null:1"], capture_output=True)
            subprocess.run(["redis-cli", "DEL", "test:write:str:null:2"], capture_output=True)

    def test_write_strings_int_values(self, redis_url: str) -> None:
        """Test writing integer values as strings."""
        import subprocess

        df = pl.DataFrame(
            {
                "_key": ["test:write:str:int:1", "test:write:str:int:2"],
                "value": [42, 100],
            }
        )

        try:
            count = polars_redis.write_strings(df, redis_url)
            assert count == 2

            result = subprocess.run(
                ["redis-cli", "GET", "test:write:str:int:1"],
                capture_output=True,
                text=True,
            )
            assert result.stdout.strip() == "42"

            result = subprocess.run(
                ["redis-cli", "GET", "test:write:str:int:2"],
                capture_output=True,
                text=True,
            )
            assert result.stdout.strip() == "100"
        finally:
            subprocess.run(["redis-cli", "DEL", "test:write:str:int:1"], capture_output=True)
            subprocess.run(["redis-cli", "DEL", "test:write:str:int:2"], capture_output=True)

    def test_write_strings_float_values(self, redis_url: str) -> None:
        """Test writing float values as strings."""
        import subprocess

        df = pl.DataFrame(
            {
                "_key": ["test:write:str:float:1", "test:write:str:float:2"],
                "value": [3.14, 2.718],
            }
        )

        try:
            count = polars_redis.write_strings(df, redis_url)
            assert count == 2

            result = subprocess.run(
                ["redis-cli", "GET", "test:write:str:float:1"],
                capture_output=True,
                text=True,
            )
            assert result.stdout.strip() == "3.14"

            result = subprocess.run(
                ["redis-cli", "GET", "test:write:str:float:2"],
                capture_output=True,
                text=True,
            )
            assert result.stdout.strip() == "2.718"
        finally:
            subprocess.run(["redis-cli", "DEL", "test:write:str:float:1"], capture_output=True)
            subprocess.run(["redis-cli", "DEL", "test:write:str:float:2"], capture_output=True)


class TestWriteTTL:
    """Tests for TTL (time-to-live) support in write operations."""

    def test_write_hashes_with_ttl(self, redis_url: str) -> None:
        """Test writing hashes with TTL."""
        import subprocess

        df = pl.DataFrame(
            {
                "_key": ["test:ttl:hash:1", "test:ttl:hash:2"],
                "name": ["Alice", "Bob"],
                "age": [30, 25],
            }
        )

        try:
            count = polars_redis.write_hashes(df, redis_url, ttl=60)
            assert count == 2

            # Verify TTL was set (should be > 0 and <= 60)
            result = subprocess.run(
                ["redis-cli", "TTL", "test:ttl:hash:1"],
                capture_output=True,
                text=True,
            )
            ttl = int(result.stdout.strip())
            assert 0 < ttl <= 60

            result = subprocess.run(
                ["redis-cli", "TTL", "test:ttl:hash:2"],
                capture_output=True,
                text=True,
            )
            ttl = int(result.stdout.strip())
            assert 0 < ttl <= 60
        finally:
            subprocess.run(["redis-cli", "DEL", "test:ttl:hash:1"], capture_output=True)
            subprocess.run(["redis-cli", "DEL", "test:ttl:hash:2"], capture_output=True)

    def test_write_hashes_without_ttl(self, redis_url: str) -> None:
        """Test writing hashes without TTL (no expiration)."""
        import subprocess

        df = pl.DataFrame(
            {
                "_key": ["test:no:ttl:hash:1"],
                "name": ["Alice"],
            }
        )

        try:
            count = polars_redis.write_hashes(df, redis_url, ttl=None)
            assert count == 1

            # TTL should be -1 (no expiration)
            result = subprocess.run(
                ["redis-cli", "TTL", "test:no:ttl:hash:1"],
                capture_output=True,
                text=True,
            )
            ttl = int(result.stdout.strip())
            assert ttl == -1
        finally:
            subprocess.run(["redis-cli", "DEL", "test:no:ttl:hash:1"], capture_output=True)

    def test_write_json_with_ttl(self, redis_url: str) -> None:
        """Test writing JSON with TTL."""
        import subprocess

        df = pl.DataFrame(
            {
                "_key": ["test:ttl:json:1", "test:ttl:json:2"],
                "title": ["Hello", "World"],
                "views": [100, 200],
            }
        )

        try:
            count = polars_redis.write_json(df, redis_url, ttl=120)
            assert count == 2

            # Verify TTL was set
            result = subprocess.run(
                ["redis-cli", "TTL", "test:ttl:json:1"],
                capture_output=True,
                text=True,
            )
            ttl = int(result.stdout.strip())
            assert 0 < ttl <= 120

            result = subprocess.run(
                ["redis-cli", "TTL", "test:ttl:json:2"],
                capture_output=True,
                text=True,
            )
            ttl = int(result.stdout.strip())
            assert 0 < ttl <= 120
        finally:
            subprocess.run(["redis-cli", "DEL", "test:ttl:json:1"], capture_output=True)
            subprocess.run(["redis-cli", "DEL", "test:ttl:json:2"], capture_output=True)

    def test_write_strings_with_ttl(self, redis_url: str) -> None:
        """Test writing strings with TTL (uses SETEX)."""
        import subprocess

        df = pl.DataFrame(
            {
                "_key": ["test:ttl:str:1", "test:ttl:str:2"],
                "value": ["hello", "world"],
            }
        )

        try:
            count = polars_redis.write_strings(df, redis_url, ttl=90)
            assert count == 2

            # Verify TTL was set
            result = subprocess.run(
                ["redis-cli", "TTL", "test:ttl:str:1"],
                capture_output=True,
                text=True,
            )
            ttl = int(result.stdout.strip())
            assert 0 < ttl <= 90

            result = subprocess.run(
                ["redis-cli", "TTL", "test:ttl:str:2"],
                capture_output=True,
                text=True,
            )
            ttl = int(result.stdout.strip())
            assert 0 < ttl <= 90
        finally:
            subprocess.run(["redis-cli", "DEL", "test:ttl:str:1"], capture_output=True)
            subprocess.run(["redis-cli", "DEL", "test:ttl:str:2"], capture_output=True)

    def test_write_strings_without_ttl(self, redis_url: str) -> None:
        """Test writing strings without TTL (no expiration)."""
        import subprocess

        df = pl.DataFrame(
            {
                "_key": ["test:no:ttl:str:1"],
                "value": ["hello"],
            }
        )

        try:
            count = polars_redis.write_strings(df, redis_url, ttl=None)
            assert count == 1

            # TTL should be -1 (no expiration)
            result = subprocess.run(
                ["redis-cli", "TTL", "test:no:ttl:str:1"],
                capture_output=True,
                text=True,
            )
            ttl = int(result.stdout.strip())
            assert ttl == -1
        finally:
            subprocess.run(["redis-cli", "DEL", "test:no:ttl:str:1"], capture_output=True)


def redisearch_available() -> bool:
    """Check if RediSearch module is available."""
    import subprocess

    try:
        result = subprocess.run(
            ["redis-cli", "MODULE", "LIST"],
            capture_output=True,
            text=True,
        )
        return "search" in result.stdout.lower()
    except Exception:
        return False


@pytest.mark.skipif(
    not redisearch_available(),
    reason="RediSearch module not available",
)
class TestSearchHashes:
    """Tests for search_hashes function using RediSearch."""

    @pytest.fixture(scope="class", autouse=True)
    def setup_search_data(self, redis_url: str) -> None:
        """Set up test data and index for RediSearch tests."""
        import subprocess

        # Create test hashes for search
        for i in range(1, 21):
            age = 20 + i
            score = float(i * 10)
            active = "true" if i % 2 == 0 else "false"
            subprocess.run(
                [
                    "redis-cli",
                    "HSET",
                    f"search:user:{i}",
                    "name",
                    f"SearchUser{i}",
                    "age",
                    str(age),
                    "score",
                    str(score),
                    "active",
                    active,
                ],
                capture_output=True,
            )

        # Create RediSearch index on the hash prefix
        # Drop index if it exists (ignore errors)
        subprocess.run(
            ["redis-cli", "FT.DROPINDEX", "search_users_idx"],
            capture_output=True,
        )

        # Create new index
        subprocess.run(
            [
                "redis-cli",
                "FT.CREATE",
                "search_users_idx",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "search:user:",
                "SCHEMA",
                "name",
                "TEXT",
                "age",
                "NUMERIC",
                "SORTABLE",
                "score",
                "NUMERIC",
                "SORTABLE",
                "active",
                "TAG",
            ],
            capture_output=True,
        )

        # Wait for RediSearch to index all documents
        # RediSearch indexes in the background, so we need to wait
        import time

        for _ in range(50):  # Wait up to 5 seconds
            result = subprocess.run(
                ["redis-cli", "FT.INFO", "search_users_idx"],
                capture_output=True,
                text=True,
            )
            # Parse FT.INFO output to check num_docs
            output = result.stdout
            if "num_docs" in output:
                # Find num_docs value - format is "num_docs\n20\n"
                lines = output.strip().split("\n")
                for i, line in enumerate(lines):
                    if line == "num_docs" and i + 1 < len(lines):
                        num_docs = int(lines[i + 1])
                        if num_docs >= 20:
                            break
            time.sleep(0.1)
        else:
            # Final check - if we get here, indexing might not be complete
            time.sleep(0.5)

        yield

        # Cleanup
        subprocess.run(
            ["redis-cli", "FT.DROPINDEX", "search_users_idx"],
            capture_output=True,
        )
        for i in range(1, 21):
            subprocess.run(
                ["redis-cli", "DEL", f"search:user:{i}"],
                capture_output=True,
            )

    def test_search_hashes_all(self, redis_url: str) -> None:
        """Test searching all documents with wildcard query."""
        lf = polars_redis.search_hashes(
            redis_url,
            index="search_users_idx",
            query="*",
            schema={
                "name": pl.Utf8,
                "age": pl.Int64,
                "score": pl.Float64,
            },
        )

        df = lf.collect()
        assert len(df) == 20
        assert "_key" in df.columns
        assert "name" in df.columns
        assert "age" in df.columns
        assert "score" in df.columns

    def test_search_hashes_numeric_range(self, redis_url: str) -> None:
        """Test searching with numeric range filter."""
        # Search for users with age >= 30 (that's users 10-20 since age = 20 + i)
        lf = polars_redis.search_hashes(
            redis_url,
            index="search_users_idx",
            query="@age:[30 +inf]",
            schema={
                "name": pl.Utf8,
                "age": pl.Int64,
            },
        )

        df = lf.collect()
        assert len(df) == 11  # Users 10-20 have age 30-40
        assert all(df["age"] >= 30)

    def test_search_hashes_text_search(self, redis_url: str) -> None:
        """Test searching with text filter."""
        # Search for user with name containing "SearchUser1"
        # This should match SearchUser1, SearchUser10-19
        lf = polars_redis.search_hashes(
            redis_url,
            index="search_users_idx",
            query="@name:SearchUser1*",
            schema={
                "name": pl.Utf8,
                "age": pl.Int64,
            },
        )

        df = lf.collect()
        # Should match SearchUser1, SearchUser10, SearchUser11, ..., SearchUser19
        assert len(df) == 11

    def test_search_hashes_with_sort(self, redis_url: str) -> None:
        """Test searching with sort order."""
        lf = polars_redis.search_hashes(
            redis_url,
            index="search_users_idx",
            query="*",
            schema={
                "name": pl.Utf8,
                "age": pl.Int64,
                "score": pl.Float64,
            },
            sort_by="score",
            sort_ascending=False,
        )

        df = lf.collect()
        assert len(df) == 20
        # Check that scores are in descending order
        scores = df["score"].to_list()
        assert scores == sorted(scores, reverse=True)

    def test_search_hashes_with_sort_ascending(self, redis_url: str) -> None:
        """Test searching with ascending sort order."""
        lf = polars_redis.search_hashes(
            redis_url,
            index="search_users_idx",
            query="*",
            schema={
                "name": pl.Utf8,
                "age": pl.Int64,
            },
            sort_by="age",
            sort_ascending=True,
        )

        df = lf.collect()
        assert len(df) == 20
        # Check that ages are in ascending order
        ages = df["age"].to_list()
        assert ages == sorted(ages)

    def test_search_hashes_without_key(self, redis_url: str) -> None:
        """Test searching without the key column."""
        lf = polars_redis.search_hashes(
            redis_url,
            index="search_users_idx",
            query="*",
            schema={"name": pl.Utf8, "age": pl.Int64},
            include_key=False,
        )

        df = lf.collect()
        assert "_key" not in df.columns
        assert df.columns == ["name", "age"]

    def test_search_hashes_combined_query(self, redis_url: str) -> None:
        """Test searching with combined filters."""
        # Search for users with age >= 25 AND age <= 35
        lf = polars_redis.search_hashes(
            redis_url,
            index="search_users_idx",
            query="@age:[25 35]",
            schema={
                "name": pl.Utf8,
                "age": pl.Int64,
            },
        )

        df = lf.collect()
        # Users 5-15 have ages 25-35
        assert len(df) == 11
        assert all((df["age"] >= 25) & (df["age"] <= 35))

    def test_search_hashes_no_results(self, redis_url: str) -> None:
        """Test searching with query that returns no results."""
        lf = polars_redis.search_hashes(
            redis_url,
            index="search_users_idx",
            query="@age:[100 200]",
            schema={
                "name": pl.Utf8,
                "age": pl.Int64,
            },
        )

        df = lf.collect()
        assert len(df) == 0


@pytest.mark.skipif(
    not redisearch_available(),
    reason="RediSearch module not available",
)
class TestAggregateHashes:
    """Tests for aggregate_hashes function using RediSearch FT.AGGREGATE."""

    @pytest.fixture(scope="class", autouse=True)
    def setup_aggregate_data(self, redis_url: str) -> None:
        """Set up test data and index for FT.AGGREGATE tests."""
        import subprocess

        # Create test hashes for aggregation
        # Department distribution: Engineering (10), Sales (5), Marketing (5)
        departments = ["Engineering"] * 10 + ["Sales"] * 5 + ["Marketing"] * 5
        for i in range(1, 21):
            age = 25 + (i % 20)  # Ages 25-44
            salary = 50000 + (i * 5000)  # Salaries 55000-150000
            department = departments[i - 1]
            subprocess.run(
                [
                    "redis-cli",
                    "HSET",
                    f"agg:employee:{i}",
                    "name",
                    f"Employee{i}",
                    "age",
                    str(age),
                    "salary",
                    str(salary),
                    "department",
                    department,
                ],
                capture_output=True,
            )

        # Drop index if it exists (ignore errors)
        subprocess.run(
            ["redis-cli", "FT.DROPINDEX", "agg_employees_idx"],
            capture_output=True,
        )

        # Create new index
        subprocess.run(
            [
                "redis-cli",
                "FT.CREATE",
                "agg_employees_idx",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "agg:employee:",
                "SCHEMA",
                "name",
                "TEXT",
                "age",
                "NUMERIC",
                "SORTABLE",
                "salary",
                "NUMERIC",
                "SORTABLE",
                "department",
                "TAG",
                "SORTABLE",
            ],
            capture_output=True,
        )

        # Wait for RediSearch to index all documents
        import time

        for _ in range(50):  # Wait up to 5 seconds
            result = subprocess.run(
                ["redis-cli", "FT.INFO", "agg_employees_idx"],
                capture_output=True,
                text=True,
            )
            output = result.stdout
            if "num_docs" in output:
                lines = output.strip().split("\n")
                for idx, line in enumerate(lines):
                    if line == "num_docs" and idx + 1 < len(lines):
                        num_docs = int(lines[idx + 1])
                        if num_docs >= 20:
                            break
            time.sleep(0.1)
        else:
            time.sleep(0.5)

        yield

        # Cleanup
        subprocess.run(
            ["redis-cli", "FT.DROPINDEX", "agg_employees_idx"],
            capture_output=True,
        )
        for i in range(1, 21):
            subprocess.run(
                ["redis-cli", "DEL", f"agg:employee:{i}"],
                capture_output=True,
            )

    def test_aggregate_count_all(self, redis_url: str) -> None:
        """Test counting all documents."""
        df = polars_redis.aggregate_hashes(
            redis_url,
            index="agg_employees_idx",
            query="*",
            reduce=[("COUNT", [], "total")],
        )

        assert len(df) == 1
        assert "total" in df.columns
        assert int(df["total"][0]) == 20

    def test_aggregate_group_by_department(self, redis_url: str) -> None:
        """Test grouping by department with count."""
        df = polars_redis.aggregate_hashes(
            redis_url,
            index="agg_employees_idx",
            query="*",
            group_by=["@department"],
            reduce=[("COUNT", [], "count")],
        )

        assert len(df) == 3  # Engineering, Sales, Marketing
        assert "department" in df.columns
        assert "count" in df.columns

        # Convert to dict for easier checking
        # Note: RediSearch lowercases TAG values
        dept_counts = dict(zip(df["department"].to_list(), df["count"].to_list()))
        assert int(dept_counts.get("engineering", 0)) == 10
        assert int(dept_counts.get("sales", 0)) == 5
        assert int(dept_counts.get("marketing", 0)) == 5

    def test_aggregate_avg_salary_by_department(self, redis_url: str) -> None:
        """Test calculating average salary by department."""
        df = polars_redis.aggregate_hashes(
            redis_url,
            index="agg_employees_idx",
            query="*",
            group_by=["@department"],
            reduce=[
                ("COUNT", [], "count"),
                ("AVG", ["@salary"], "avg_salary"),
            ],
        )

        assert len(df) == 3
        assert "avg_salary" in df.columns
        # Just verify we got numeric values
        for val in df["avg_salary"].to_list():
            assert float(val) > 0

    def test_aggregate_sum_salary(self, redis_url: str) -> None:
        """Test summing salaries."""
        df = polars_redis.aggregate_hashes(
            redis_url,
            index="agg_employees_idx",
            query="*",
            reduce=[("SUM", ["@salary"], "total_salary")],
        )

        assert len(df) == 1
        assert "total_salary" in df.columns
        # Sum of salaries: 55000 + 60000 + ... + 150000 = sum of 55000 to 150000 step 5000
        # = 20 terms, average = (55000 + 150000) / 2 = 102500, total = 102500 * 20 = 2050000
        assert float(df["total_salary"][0]) == 2050000

    def test_aggregate_min_max(self, redis_url: str) -> None:
        """Test MIN and MAX aggregations."""
        df = polars_redis.aggregate_hashes(
            redis_url,
            index="agg_employees_idx",
            query="*",
            reduce=[
                ("MIN", ["@salary"], "min_salary"),
                ("MAX", ["@salary"], "max_salary"),
            ],
        )

        assert len(df) == 1
        assert float(df["min_salary"][0]) == 55000
        assert float(df["max_salary"][0]) == 150000

    def test_aggregate_with_sort(self, redis_url: str) -> None:
        """Test aggregation with sorting."""
        df = polars_redis.aggregate_hashes(
            redis_url,
            index="agg_employees_idx",
            query="*",
            group_by=["@department"],
            reduce=[("COUNT", [], "count")],
            sort_by=[("@count", False)],  # Descending by count
        )

        assert len(df) == 3
        counts = [int(c) for c in df["count"].to_list()]
        assert counts == sorted(counts, reverse=True)
        # Engineering should be first with 10 employees (lowercase due to TAG)
        assert df["department"][0] == "engineering"

    def test_aggregate_with_limit(self, redis_url: str) -> None:
        """Test aggregation with limit."""
        df = polars_redis.aggregate_hashes(
            redis_url,
            index="agg_employees_idx",
            query="*",
            group_by=["@department"],
            reduce=[("COUNT", [], "count")],
            sort_by=[("@count", False)],
            limit=2,
        )

        assert len(df) == 2

    def test_aggregate_with_filter_query(self, redis_url: str) -> None:
        """Test aggregation with filtered query."""
        df = polars_redis.aggregate_hashes(
            redis_url,
            index="agg_employees_idx",
            query="@department:{Engineering}",
            reduce=[("COUNT", [], "count")],
        )

        assert len(df) == 1
        assert int(df["count"][0]) == 10

    def test_aggregate_empty_result(self, redis_url: str) -> None:
        """Test aggregation that returns no results."""
        df = polars_redis.aggregate_hashes(
            redis_url,
            index="agg_employees_idx",
            query="@salary:[1000000 2000000]",  # No one earns this much
            reduce=[("COUNT", [], "count")],
        )

        # Empty result or count of 0
        assert len(df) == 0 or int(df["count"][0]) == 0
