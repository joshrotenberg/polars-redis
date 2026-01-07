"""Tests for DataFrame caching functionality."""

import polars as pl
import polars_redis as redis
import pytest
from polars.testing import assert_frame_equal

# Use the REDIS_URL environment variable or default to localhost
REDIS_URL = "redis://localhost:6379"


@pytest.fixture
def sample_df():
    """Create a sample DataFrame for testing."""
    return pl.DataFrame(
        {
            "id": list(range(100)),
            "name": [f"item_{i}" for i in range(100)],
            "value": [float(i) * 1.5 for i in range(100)],
            "active": [i % 2 == 0 for i in range(100)],
        }
    )


@pytest.fixture
def large_df():
    """Create a larger DataFrame for chunking tests."""
    n = 100_000
    return pl.DataFrame(
        {
            "id": list(range(n)),
            "data": [f"row_{i}_" + "x" * 100 for i in range(n)],  # ~100 bytes per row
            "value": [float(i) for i in range(n)],
        }
    )


@pytest.fixture(autouse=True)
def cleanup():
    """Clean up test keys before and after each test."""
    test_keys = [
        "test:cache:basic",
        "test:cache:ipc",
        "test:cache:parquet",
        "test:cache:compressed",
        "test:cache:ttl",
        "test:cache:chunked",
        "test:cache:delete",
        "test:cache:exists",
        "test:cache:scan",
        "test:cache:info",
        "test:cache:columns",
        "test:cache:types",
    ]

    def do_cleanup():
        for key in test_keys:
            try:
                redis.delete_cached(REDIS_URL, key)
            except Exception:
                pass

    # Clean before test
    do_cleanup()
    yield
    # Clean after test
    do_cleanup()


class TestCacheBasic:
    """Basic caching tests."""

    def test_cache_and_retrieve_ipc(self, sample_df):
        """Test basic cache and retrieve with IPC format."""
        key = "test:cache:ipc"

        # Cache
        bytes_written = redis.cache_dataframe(sample_df, REDIS_URL, key, format="ipc")
        assert bytes_written > 0

        # Retrieve
        result = redis.get_cached_dataframe(REDIS_URL, key, format="ipc")
        assert result is not None
        assert_frame_equal(result, sample_df)

    def test_cache_and_retrieve_parquet(self, sample_df):
        """Test basic cache and retrieve with Parquet format."""
        key = "test:cache:parquet"

        # Cache
        bytes_written = redis.cache_dataframe(sample_df, REDIS_URL, key, format="parquet")
        assert bytes_written > 0

        # Retrieve
        result = redis.get_cached_dataframe(REDIS_URL, key, format="parquet")
        assert result is not None
        assert_frame_equal(result, sample_df)

    def test_cache_nonexistent_key(self):
        """Test retrieving a nonexistent key returns None."""
        result = redis.get_cached_dataframe(REDIS_URL, "test:cache:nonexistent")
        assert result is None

    def test_cache_empty_dataframe(self):
        """Test caching an empty DataFrame."""
        key = "test:cache:basic"
        empty_df = pl.DataFrame({"a": [], "b": []}).cast({"a": pl.Int64, "b": pl.Utf8})

        redis.cache_dataframe(empty_df, REDIS_URL, key)
        result = redis.get_cached_dataframe(REDIS_URL, key)

        assert result is not None
        assert len(result) == 0
        assert result.columns == ["a", "b"]


class TestCacheCompression:
    """Compression tests."""

    def test_ipc_lz4_compression(self, sample_df):
        """Test IPC with LZ4 compression."""
        key = "test:cache:compressed"

        redis.cache_dataframe(sample_df, REDIS_URL, key, format="ipc", compression="lz4")
        result = redis.get_cached_dataframe(REDIS_URL, key, format="ipc")

        assert result is not None
        assert_frame_equal(result, sample_df)

    def test_ipc_zstd_compression(self, sample_df):
        """Test IPC with ZSTD compression."""
        key = "test:cache:compressed"

        redis.cache_dataframe(sample_df, REDIS_URL, key, format="ipc", compression="zstd")
        result = redis.get_cached_dataframe(REDIS_URL, key, format="ipc")

        assert result is not None
        assert_frame_equal(result, sample_df)

    def test_parquet_snappy_compression(self, sample_df):
        """Test Parquet with Snappy compression."""
        key = "test:cache:compressed"

        redis.cache_dataframe(sample_df, REDIS_URL, key, format="parquet", compression="snappy")
        result = redis.get_cached_dataframe(REDIS_URL, key, format="parquet")

        assert result is not None
        assert_frame_equal(result, sample_df)

    def test_parquet_gzip_compression(self, sample_df):
        """Test Parquet with GZIP compression."""
        key = "test:cache:compressed"

        redis.cache_dataframe(sample_df, REDIS_URL, key, format="parquet", compression="gzip")
        result = redis.get_cached_dataframe(REDIS_URL, key, format="parquet")

        assert result is not None
        assert_frame_equal(result, sample_df)

    def test_parquet_zstd_with_level(self, sample_df):
        """Test Parquet with ZSTD compression and custom level."""
        key = "test:cache:compressed"

        redis.cache_dataframe(
            sample_df, REDIS_URL, key, format="parquet", compression="zstd", compression_level=3
        )
        result = redis.get_cached_dataframe(REDIS_URL, key, format="parquet")

        assert result is not None
        assert_frame_equal(result, sample_df)

    def test_invalid_ipc_compression(self, sample_df):
        """Test invalid IPC compression raises error."""
        with pytest.raises(ValueError, match="Invalid IPC compression"):
            redis.cache_dataframe(
                sample_df, REDIS_URL, "test:cache:basic", format="ipc", compression="invalid"
            )

    def test_invalid_parquet_compression(self, sample_df):
        """Test invalid Parquet compression raises error."""
        with pytest.raises(ValueError, match="Invalid Parquet compression"):
            redis.cache_dataframe(
                sample_df, REDIS_URL, "test:cache:basic", format="parquet", compression="invalid"
            )


class TestCacheTTL:
    """TTL tests."""

    def test_cache_with_ttl(self, sample_df):
        """Test caching with TTL."""
        key = "test:cache:ttl"

        redis.cache_dataframe(sample_df, REDIS_URL, key, ttl=3600)

        ttl = redis.cache_ttl(REDIS_URL, key)
        assert ttl is not None
        assert 3590 <= ttl <= 3600  # Allow for some time passing

    def test_cache_without_ttl(self, sample_df):
        """Test caching without TTL."""
        key = "test:cache:ttl"

        redis.cache_dataframe(sample_df, REDIS_URL, key)

        ttl = redis.cache_ttl(REDIS_URL, key)
        assert ttl is None  # No TTL set

    def test_ttl_nonexistent_key(self):
        """Test TTL on nonexistent key returns None."""
        ttl = redis.cache_ttl(REDIS_URL, "test:cache:nonexistent")
        assert ttl is None


class TestCacheChunking:
    """Chunking tests."""

    def test_chunked_storage(self, sample_df):
        """Test chunked storage with small chunk size."""
        key = "test:cache:chunked"

        # Use very small chunk size to force chunking
        redis.cache_dataframe(sample_df, REDIS_URL, key, chunk_size_mb=0.001)  # 1KB chunks

        # Should still retrieve correctly
        result = redis.get_cached_dataframe(REDIS_URL, key)
        assert result is not None
        assert_frame_equal(result, sample_df)

        # Check info shows chunked
        info = redis.cache_info(REDIS_URL, key)
        assert info is not None
        assert info["is_chunked"] is True
        assert info["num_chunks"] > 1

    def test_chunked_with_parquet(self, sample_df):
        """Test chunked storage with Parquet format."""
        key = "test:cache:chunked"

        redis.cache_dataframe(sample_df, REDIS_URL, key, format="parquet", chunk_size_mb=0.001)

        result = redis.get_cached_dataframe(REDIS_URL, key)
        assert result is not None
        assert_frame_equal(result, sample_df)

    def test_chunked_delete(self, sample_df):
        """Test deleting chunked data removes all chunks."""
        key = "test:cache:chunked"

        redis.cache_dataframe(sample_df, REDIS_URL, key, chunk_size_mb=0.001)

        # Verify it exists
        assert redis.cache_exists(REDIS_URL, key)

        # Delete
        deleted = redis.delete_cached(REDIS_URL, key)
        assert deleted is True

        # Verify it's gone
        assert redis.cache_exists(REDIS_URL, key) is False

    def test_disable_chunking(self, sample_df):
        """Test disabling chunking with chunk_size_mb=0."""
        key = "test:cache:chunked"

        redis.cache_dataframe(sample_df, REDIS_URL, key, chunk_size_mb=0)

        # Should not be chunked
        info = redis.cache_info(REDIS_URL, key)
        assert info is not None
        assert info["is_chunked"] is False


class TestCacheOperations:
    """Cache operation tests."""

    def test_cache_exists(self, sample_df):
        """Test cache_exists function."""
        key = "test:cache:exists"

        # Should not exist initially
        assert redis.cache_exists(REDIS_URL, key) is False

        # Cache it
        redis.cache_dataframe(sample_df, REDIS_URL, key)

        # Should exist now
        assert redis.cache_exists(REDIS_URL, key) is True

    def test_delete_cached(self, sample_df):
        """Test delete_cached function."""
        key = "test:cache:delete"

        redis.cache_dataframe(sample_df, REDIS_URL, key)
        assert redis.cache_exists(REDIS_URL, key) is True

        # Delete
        deleted = redis.delete_cached(REDIS_URL, key)
        assert deleted is True

        # Verify deleted
        assert redis.cache_exists(REDIS_URL, key) is False

        # Delete again should return False
        deleted = redis.delete_cached(REDIS_URL, key)
        assert deleted is False

    def test_scan_cached(self, sample_df):
        """Test scan_cached returns LazyFrame."""
        key = "test:cache:scan"

        redis.cache_dataframe(sample_df, REDIS_URL, key)

        lf = redis.scan_cached(REDIS_URL, key)
        assert lf is not None
        assert isinstance(lf, pl.LazyFrame)

        # Can apply lazy operations
        result = lf.filter(pl.col("id") > 50).collect()
        expected = sample_df.filter(pl.col("id") > 50)
        assert_frame_equal(result, expected)

    def test_scan_cached_nonexistent(self):
        """Test scan_cached on nonexistent key returns None."""
        result = redis.scan_cached(REDIS_URL, "test:cache:nonexistent")
        assert result is None

    def test_cache_info(self, sample_df):
        """Test cache_info function."""
        key = "test:cache:info"

        redis.cache_dataframe(sample_df, REDIS_URL, key)

        info = redis.cache_info(REDIS_URL, key)
        assert info is not None
        assert info["size_bytes"] > 0
        assert info["is_chunked"] is False
        assert info["num_chunks"] == 1

    def test_cache_info_nonexistent(self):
        """Test cache_info on nonexistent key returns None."""
        info = redis.cache_info(REDIS_URL, "test:cache:nonexistent")
        assert info is None


class TestCacheDataTypes:
    """Test caching various data types."""

    def test_all_basic_types(self):
        """Test caching DataFrame with all basic types."""
        key = "test:cache:types"

        df = pl.DataFrame(
            {
                "int8": pl.Series([1, 2, 3], dtype=pl.Int8),
                "int16": pl.Series([100, 200, 300], dtype=pl.Int16),
                "int32": pl.Series([1000, 2000, 3000], dtype=pl.Int32),
                "int64": pl.Series([10000, 20000, 30000], dtype=pl.Int64),
                "float32": pl.Series([1.1, 2.2, 3.3], dtype=pl.Float32),
                "float64": pl.Series([1.11, 2.22, 3.33], dtype=pl.Float64),
                "bool": [True, False, True],
                "str": ["a", "b", "c"],
            }
        )

        redis.cache_dataframe(df, REDIS_URL, key)
        result = redis.get_cached_dataframe(REDIS_URL, key)

        assert result is not None
        assert_frame_equal(result, df)

    def test_null_values(self):
        """Test caching DataFrame with null values."""
        key = "test:cache:types"

        df = pl.DataFrame(
            {
                "a": [1, None, 3],
                "b": ["x", None, "z"],
                "c": [1.0, 2.0, None],
            }
        )

        redis.cache_dataframe(df, REDIS_URL, key)
        result = redis.get_cached_dataframe(REDIS_URL, key)

        assert result is not None
        assert_frame_equal(result, df)

    def test_date_datetime(self):
        """Test caching DataFrame with date and datetime."""
        key = "test:cache:types"
        from datetime import date, datetime

        df = pl.DataFrame(
            {
                "date": [date(2024, 1, 1), date(2024, 6, 15), date(2024, 12, 31)],
                "datetime": [
                    datetime(2024, 1, 1, 10, 30, 0),
                    datetime(2024, 6, 15, 12, 0, 0),
                    datetime(2024, 12, 31, 23, 59, 59),
                ],
            }
        )

        redis.cache_dataframe(df, REDIS_URL, key)
        result = redis.get_cached_dataframe(REDIS_URL, key)

        assert result is not None
        assert_frame_equal(result, df)

    def test_list_column(self):
        """Test caching DataFrame with list column."""
        key = "test:cache:types"

        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "values": [[1, 2, 3], [4, 5], [6, 7, 8, 9]],
            }
        )

        redis.cache_dataframe(df, REDIS_URL, key)
        result = redis.get_cached_dataframe(REDIS_URL, key)

        assert result is not None
        assert_frame_equal(result, df)


class TestParquetProjection:
    """Test Parquet-specific features."""

    def test_column_projection(self, sample_df):
        """Test reading only specific columns from Parquet."""
        key = "test:cache:columns"

        redis.cache_dataframe(sample_df, REDIS_URL, key, format="parquet")

        # Read only specific columns
        result = redis.get_cached_dataframe(
            REDIS_URL, key, format="parquet", columns=["id", "name"]
        )

        assert result is not None
        assert result.columns == ["id", "name"]
        assert len(result) == len(sample_df)

    def test_row_limit(self, sample_df):
        """Test reading limited rows from Parquet."""
        key = "test:cache:columns"

        redis.cache_dataframe(sample_df, REDIS_URL, key, format="parquet")

        # Read only first 10 rows
        result = redis.get_cached_dataframe(REDIS_URL, key, format="parquet", n_rows=10)

        assert result is not None
        assert len(result) == 10

    def test_columns_and_rows(self, sample_df):
        """Test reading specific columns and limited rows."""
        key = "test:cache:columns"

        redis.cache_dataframe(sample_df, REDIS_URL, key, format="parquet")

        result = redis.get_cached_dataframe(
            REDIS_URL, key, format="parquet", columns=["id", "value"], n_rows=5
        )

        assert result is not None
        assert result.columns == ["id", "value"]
        assert len(result) == 5


class TestInvalidInputs:
    """Test error handling for invalid inputs."""

    def test_invalid_format(self, sample_df):
        """Test invalid format raises error."""
        with pytest.raises(ValueError, match="Invalid format"):
            redis.cache_dataframe(sample_df, REDIS_URL, "test:cache:basic", format="invalid")  # type: ignore

    def test_invalid_format_get(self, sample_df):
        """Test invalid format on get raises error."""
        key = "test:cache:basic"
        redis.cache_dataframe(sample_df, REDIS_URL, key)

        with pytest.raises(ValueError, match="Invalid format"):
            redis.get_cached_dataframe(REDIS_URL, key, format="invalid")  # type: ignore
