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
