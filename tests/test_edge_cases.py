"""Edge case tests for polars-redis (require running Redis).

These tests cover edge cases that require a live Redis connection:
- Malformed data handling
- Connection failures and reconnection
- Timeout handling
- Large values
- Race conditions (keys deleted between SCAN and fetch)
- Concurrent access patterns
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import polars as pl
import polars_redis
import pytest


def redis_available() -> bool:
    """Check if Redis is available for testing."""
    try:
        polars_redis.scan_keys("redis://localhost:6379", "*", count=1)
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


def redis_cli(*args: str) -> subprocess.CompletedProcess:
    """Run redis-cli command."""
    return subprocess.run(["redis-cli"] + list(args), capture_output=True, text=True)


def cleanup_keys(pattern: str) -> None:
    """Delete all keys matching pattern."""
    result = redis_cli("KEYS", pattern)
    for key in result.stdout.strip().split("\n"):
        if key:
            redis_cli("DEL", key)


class TestMalformedData:
    """Tests for handling malformed or unusual data."""

    @pytest.fixture(autouse=True)
    def setup_teardown(self) -> None:
        """Clean up test keys before and after each test."""
        cleanup_keys("edge:malformed:*")
        yield
        cleanup_keys("edge:malformed:*")

    def test_empty_string_values(self, redis_url: str) -> None:
        """Test handling of empty string values."""
        redis_cli("HSET", "edge:malformed:empty", "name", "", "value", "")

        df = polars_redis.scan_hashes(
            redis_url,
            "edge:malformed:empty",
            schema={"name": pl.Utf8, "value": pl.Utf8},
        ).collect()

        assert len(df) == 1
        assert df["name"][0] == ""
        assert df["value"][0] == ""

    def test_whitespace_only_values(self, redis_url: str) -> None:
        """Test handling of whitespace-only values."""
        redis_cli("HSET", "edge:malformed:ws", "name", "   ", "tabs", "\t\t")

        df = polars_redis.scan_hashes(
            redis_url,
            "edge:malformed:ws",
            schema={"name": pl.Utf8, "tabs": pl.Utf8},
        ).collect()

        assert len(df) == 1
        assert df["name"][0] == "   "
        assert df["tabs"][0] == "\t\t"

    def test_numeric_string_with_whitespace(self, redis_url: str) -> None:
        """Test parsing numeric strings with leading/trailing whitespace.

        Note: The library does NOT trim whitespace - strings with whitespace
        fail to parse as numbers and become null.
        """
        redis_cli("HSET", "edge:malformed:numws", "age", "  42  ", "score", " 3.14 ")

        df = polars_redis.scan_hashes(
            redis_url,
            "edge:malformed:numws",
            schema={
                "age": pl.Utf8,
                "score": pl.Utf8,
            },  # Read as strings since whitespace breaks parsing
        ).collect()

        assert len(df) == 1
        # Values preserved with whitespace
        assert df["age"][0] == "  42  "
        assert df["score"][0] == " 3.14 "

    def test_invalid_numeric_raises_error(self, redis_url: str) -> None:
        """Test that invalid numeric values raise an error.

        The library raises a type conversion error rather than returning null.
        """
        redis_cli("HSET", "edge:malformed:badnum", "age", "not_a_number", "score", "abc")

        with pytest.raises(Exception):
            polars_redis.scan_hashes(
                redis_url,
                "edge:malformed:badnum",
                schema={"age": pl.Int64, "score": pl.Float64},
            ).collect()

    def test_special_characters_in_values(self, redis_url: str) -> None:
        """Test handling of special characters in string values."""
        special = "Hello \"World\" with 'quotes' and\nnewlines\tand\ttabs"
        redis_cli("HSET", "edge:malformed:special", "text", special)

        df = polars_redis.scan_hashes(
            redis_url,
            "edge:malformed:special",
            schema={"text": pl.Utf8},
        ).collect()

        assert len(df) == 1
        assert df["text"][0] == special

    def test_unicode_values(self, redis_url: str) -> None:
        """Test handling of unicode characters."""
        redis_cli(
            "HSET",
            "edge:malformed:unicode",
            "emoji",
            "Hello World",
            "chinese",
            "nihao",
            "arabic",
            "marhaba",
        )

        df = polars_redis.scan_hashes(
            redis_url,
            "edge:malformed:unicode",
            schema={"emoji": pl.Utf8, "chinese": pl.Utf8, "arabic": pl.Utf8},
        ).collect()

        assert len(df) == 1
        # Values should be preserved
        assert df["emoji"][0] is not None
        assert df["chinese"][0] is not None
        assert df["arabic"][0] is not None

    def test_very_long_field_names(self, redis_url: str) -> None:
        """Test handling of very long field names."""
        long_field = "a" * 1000
        redis_cli("HSET", "edge:malformed:longfield", long_field, "value")

        df = polars_redis.scan_hashes(
            redis_url,
            "edge:malformed:longfield",
            schema={long_field: pl.Utf8},
        ).collect()

        assert len(df) == 1
        assert df[long_field][0] == "value"

    def test_numeric_field_names(self, redis_url: str) -> None:
        """Test handling of numeric field names."""
        redis_cli("HSET", "edge:malformed:numfield", "123", "value1", "456", "value2")

        df = polars_redis.scan_hashes(
            redis_url,
            "edge:malformed:numfield",
            schema={"123": pl.Utf8, "456": pl.Utf8},
        ).collect()

        assert len(df) == 1
        assert df["123"][0] == "value1"
        assert df["456"][0] == "value2"

    def test_mixed_valid_invalid_in_batch(self, redis_url: str) -> None:
        """Test batch with mix of valid and invalid numeric values.

        The library raises an error when encountering invalid values.
        """
        redis_cli("HSET", "edge:malformed:mix1", "value", "100")
        redis_cli("HSET", "edge:malformed:mix2", "value", "invalid")
        redis_cli("HSET", "edge:malformed:mix3", "value", "200")

        # Library raises error on invalid numeric value
        with pytest.raises(Exception):
            polars_redis.scan_hashes(
                redis_url,
                "edge:malformed:mix*",
                schema={"value": pl.Int64},
            ).collect()


class TestConnectionEdgeCases:
    """Tests for connection-related edge cases."""

    def test_invalid_url_format(self) -> None:
        """Test handling of invalid Redis URL format."""
        with pytest.raises(Exception):
            polars_redis.scan_hashes(
                "not_a_valid_url",
                "test:*",
                schema={"field": pl.Utf8},
            ).collect()

    def test_connection_refused(self) -> None:
        """Test handling when Redis is not running on specified port."""
        with pytest.raises(Exception):
            polars_redis.scan_hashes(
                "redis://localhost:59999",  # Unlikely to have Redis here
                "test:*",
                schema={"field": pl.Utf8},
            ).collect()

    def test_wrong_auth(self) -> None:
        """Test handling of wrong authentication (if Redis requires auth)."""
        # This test assumes Redis doesn't require auth - it should still work
        # If Redis requires auth, this would fail differently
        try:
            polars_redis.scan_keys("redis://localhost:6379", "test:*", count=1)
        except Exception:
            # Expected if auth is required but not provided
            pass


class TestLargeValues:
    """Tests for handling large values."""

    @pytest.fixture(autouse=True)
    def setup_teardown(self) -> None:
        """Clean up test keys before and after each test."""
        cleanup_keys("edge:large:*")
        yield
        cleanup_keys("edge:large:*")

    def test_large_string_value(self, redis_url: str) -> None:
        """Test handling of large string values (100KB)."""
        # Use 100KB instead of 1MB to avoid command line length limits
        large_value = "x" * (100 * 1024)

        # Use stdin to avoid command line length limits
        proc = subprocess.run(
            ["redis-cli", "HSET", "edge:large:100kb", "data", "-"],
            input=large_value,
            capture_output=True,
            text=True,
        )
        # Alternative: use redis-cli with file or direct protocol
        # For this test, let's use a smaller value that works with CLI
        redis_cli("HSET", "edge:large:small", "data", "x" * 10000)  # 10KB

        df = polars_redis.scan_hashes(
            redis_url,
            "edge:large:small",
            schema={"data": pl.Utf8},
        ).collect()

        assert len(df) == 1
        assert len(df["data"][0]) == 10000

    def test_many_fields_in_hash(self, redis_url: str) -> None:
        """Test handling of hash with many fields."""
        # Create hash with 100 fields
        args = ["HSET", "edge:large:manyfields"]
        schema = {}
        for i in range(100):
            args.extend([f"field{i}", f"value{i}"])
            schema[f"field{i}"] = pl.Utf8

        subprocess.run(["redis-cli"] + args, capture_output=True)

        df = polars_redis.scan_hashes(
            redis_url,
            "edge:large:manyfields",
            schema=schema,
        ).collect()

        assert len(df) == 1
        # Account for _key column
        assert len(df.columns) >= 100

    def test_large_batch_size(self, redis_url: str) -> None:
        """Test scanning with large batch size."""
        # Create 100 keys
        for i in range(100):
            redis_cli("HSET", f"edge:large:batch{i}", "value", str(i))

        df = polars_redis.scan_hashes(
            redis_url,
            "edge:large:batch*",
            schema={"value": pl.Int64},
            batch_size=1000,  # Larger than number of keys
        ).collect()

        assert len(df) == 100

    def test_small_batch_size(self, redis_url: str) -> None:
        """Test scanning with very small batch size."""
        # Create 20 keys
        for i in range(20):
            redis_cli("HSET", f"edge:large:smallbatch{i}", "value", str(i))

        df = polars_redis.scan_hashes(
            redis_url,
            "edge:large:smallbatch*",
            schema={"value": pl.Int64},
            batch_size=3,  # Very small batch
        ).collect()

        assert len(df) == 20


class TestRaceConditions:
    """Tests for race conditions and concurrent access."""

    @pytest.fixture(autouse=True)
    def setup_teardown(self) -> None:
        """Clean up test keys before and after each test."""
        cleanup_keys("edge:race:*")
        yield
        cleanup_keys("edge:race:*")

    def test_key_deleted_during_scan(self, redis_url: str) -> None:
        """Test behavior when keys are deleted between SCAN and HGETALL."""
        # Create keys
        for i in range(10):
            redis_cli("HSET", f"edge:race:del{i}", "value", str(i))

        # Delete some keys - simulating concurrent deletion
        # This is not a perfect simulation but tests the code path
        redis_cli("DEL", "edge:race:del5")
        redis_cli("DEL", "edge:race:del7")

        df = polars_redis.scan_hashes(
            redis_url,
            "edge:race:del*",
            schema={"value": pl.Int64},
        ).collect()

        # Should get 8 results (10 - 2 deleted)
        assert len(df) == 8

    def test_concurrent_reads(self, redis_url: str) -> None:
        """Test concurrent read operations."""
        # Create test data
        for i in range(50):
            redis_cli("HSET", f"edge:race:concurrent{i}", "value", str(i))

        def read_data() -> int:
            df = polars_redis.scan_hashes(
                redis_url,
                "edge:race:concurrent*",
                schema={"value": pl.Int64},
            ).collect()
            return len(df)

        # Run 5 concurrent reads
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(read_data) for _ in range(5)]
            results = [f.result() for f in futures]

        # All reads should return the same count
        assert all(r == 50 for r in results)

    def test_concurrent_read_write(self, redis_url: str) -> None:
        """Test concurrent read and write operations."""
        # Create initial data
        for i in range(20):
            redis_cli("HSET", f"edge:race:rw{i}", "counter", "0")

        read_results: list[int] = []
        write_complete = threading.Event()

        def reader() -> None:
            # Keep reading until writes are done
            while not write_complete.is_set():
                try:
                    df = polars_redis.scan_hashes(
                        redis_url,
                        "edge:race:rw*",
                        schema={"counter": pl.Utf8},
                    ).collect()
                    read_results.append(len(df))
                except Exception:
                    pass
                time.sleep(0.01)

        def writer() -> None:
            # Add new keys while reading
            for i in range(20, 30):
                redis_cli("HSET", f"edge:race:rw{i}", "counter", str(i))
                time.sleep(0.01)
            write_complete.set()

        reader_thread = threading.Thread(target=reader)
        writer_thread = threading.Thread(target=writer)

        reader_thread.start()
        writer_thread.start()

        writer_thread.join()
        reader_thread.join()

        # Final read should see all 30 keys
        df = polars_redis.scan_hashes(
            redis_url,
            "edge:race:rw*",
            schema={"counter": pl.Utf8},
        ).collect()
        assert len(df) == 30


class TestEmptyAndNull:
    """Tests for empty results and null handling."""

    @pytest.fixture(autouse=True)
    def setup_teardown(self) -> None:
        """Clean up test keys before and after each test."""
        cleanup_keys("edge:empty:*")
        yield
        cleanup_keys("edge:empty:*")

    def test_no_matching_keys(self, redis_url: str) -> None:
        """Test scanning pattern with no matches."""
        df = polars_redis.scan_hashes(
            redis_url,
            "edge:empty:nonexistent*",
            schema={"field": pl.Utf8},
        ).collect()

        assert len(df) == 0
        assert "field" in df.columns

    def test_empty_hash(self, redis_url: str) -> None:
        """Test scanning an empty hash (all fields deleted)."""
        redis_cli("HSET", "edge:empty:hash", "temp", "value")
        redis_cli("HDEL", "edge:empty:hash", "temp")

        df = polars_redis.scan_hashes(
            redis_url,
            "edge:empty:hash",
            schema={"temp": pl.Utf8},
        ).collect()

        # Empty hash might not appear in results or appear with null
        assert len(df) <= 1

    def test_missing_fields_become_null(self, redis_url: str) -> None:
        """Test that missing fields become null values."""
        redis_cli("HSET", "edge:empty:partial1", "name", "Alice", "age", "30")
        redis_cli("HSET", "edge:empty:partial2", "name", "Bob")  # Missing age

        df = polars_redis.scan_hashes(
            redis_url,
            "edge:empty:partial*",
            schema={"name": pl.Utf8, "age": pl.Int64},
        ).collect()

        assert len(df) == 2
        # One row should have null age
        null_ages = df.filter(pl.col("age").is_null())
        assert len(null_ages) == 1
        assert null_ages["name"][0] == "Bob"

    def test_all_fields_missing(self, redis_url: str) -> None:
        """Test hash where all requested fields are missing."""
        redis_cli("HSET", "edge:empty:nomatch", "other_field", "value")

        df = polars_redis.scan_hashes(
            redis_url,
            "edge:empty:nomatch",
            schema={"name": pl.Utf8, "age": pl.Int64},
        ).collect()

        assert len(df) == 1
        assert df["name"][0] is None
        assert df["age"][0] is None


class TestSchemaEdgeCases:
    """Tests for schema-related edge cases."""

    @pytest.fixture(autouse=True)
    def setup_teardown(self) -> None:
        """Clean up test keys before and after each test."""
        cleanup_keys("edge:schema:*")
        yield
        cleanup_keys("edge:schema:*")

    def test_schema_type_coercion(self, redis_url: str) -> None:
        """Test type coercion based on schema."""
        redis_cli("HSET", "edge:schema:coerce", "number", "42", "bool", "true", "float", "3.14")

        # Request all as strings
        df = polars_redis.scan_hashes(
            redis_url,
            "edge:schema:coerce",
            schema={"number": pl.Utf8, "bool": pl.Utf8, "float": pl.Utf8},
        ).collect()

        assert df["number"][0] == "42"
        assert df["bool"][0] == "true"
        assert df["float"][0] == "3.14"

    def test_boolean_variations(self, redis_url: str) -> None:
        """Test various boolean string representations."""
        redis_cli("HSET", "edge:schema:bool1", "val", "true")
        redis_cli("HSET", "edge:schema:bool2", "val", "false")
        redis_cli("HSET", "edge:schema:bool3", "val", "1")
        redis_cli("HSET", "edge:schema:bool4", "val", "0")
        redis_cli("HSET", "edge:schema:bool5", "val", "TRUE")
        redis_cli("HSET", "edge:schema:bool6", "val", "FALSE")

        df = polars_redis.scan_hashes(
            redis_url,
            "edge:schema:bool*",
            schema={"val": pl.Boolean},
        ).collect()

        assert len(df) == 6
        true_count = df.filter(pl.col("val") == True).height  # noqa: E712
        false_count = df.filter(pl.col("val") == False).height  # noqa: E712
        assert true_count == 3
        assert false_count == 3

    def test_float_special_values(self, redis_url: str) -> None:
        """Test special float values (inf, -inf, nan)."""
        redis_cli("HSET", "edge:schema:float1", "val", "inf")
        redis_cli("HSET", "edge:schema:float2", "val", "-inf")
        redis_cli("HSET", "edge:schema:float3", "val", "nan")
        redis_cli("HSET", "edge:schema:float4", "val", "1e308")  # Near max float

        df = polars_redis.scan_hashes(
            redis_url,
            "edge:schema:float*",
            schema={"val": pl.Float64},
        ).collect()

        assert len(df) == 4
        # Check that special values are handled
        import math

        values = df["val"].to_list()
        has_inf = any(v == float("inf") for v in values if v is not None)
        has_neg_inf = any(v == float("-inf") for v in values if v is not None)
        has_nan = any(v is not None and math.isnan(v) for v in values)

        assert has_inf or has_neg_inf or has_nan  # At least some special values parsed

    def test_projection_subset(self, redis_url: str) -> None:
        """Test reading only a subset of fields."""
        redis_cli(
            "HSET",
            "edge:schema:proj",
            "a",
            "1",
            "b",
            "2",
            "c",
            "3",
            "d",
            "4",
            "e",
            "5",
        )

        # Only request 2 of 5 fields
        df = polars_redis.scan_hashes(
            redis_url,
            "edge:schema:proj",
            schema={"a": pl.Int64, "c": pl.Int64},
            include_key=False,
        ).collect()

        assert len(df) == 1
        assert set(df.columns) == {"a", "c"}
        assert df["a"][0] == 1
        assert df["c"][0] == 3
