"""Tests for DataFrame caching decorator."""

import polars as pl
import polars_redis as redis
import pytest
from polars.testing import assert_frame_equal

REDIS_URL = "redis://localhost:6379"


@pytest.fixture(autouse=True)
def cleanup():
    """Clean up test cache keys before and after each test."""
    # Keys are auto-generated, so we clean by pattern
    # For now, we rely on TTL or manual cleanup in tests
    yield


class TestCacheDecorator:
    """Tests for the @cache decorator."""

    def test_basic_caching(self):
        """Test that function results are cached."""
        call_count = 0

        @redis.cache(url=REDIS_URL, ttl=60, key_prefix="test:decorator")
        def compute(x: int) -> pl.DataFrame:
            nonlocal call_count
            call_count += 1
            return pl.DataFrame({"value": [x * 2]})

        # First call - should compute
        result1 = compute(5)
        assert call_count == 1
        assert result1["value"][0] == 10

        # Second call with same args - should use cache
        result2 = compute(5)
        assert call_count == 1  # Not incremented
        assert_frame_equal(result1, result2)

        # Different args - should compute again
        result3 = compute(10)
        assert call_count == 2
        assert result3["value"][0] == 20

        # Clean up
        compute.invalidate(5)
        compute.invalidate(10)

    def test_cache_refresh(self):
        """Test _cache_refresh forces recomputation."""
        call_count = 0

        @redis.cache(url=REDIS_URL, ttl=60, key_prefix="test:decorator:refresh")
        def compute(x: int) -> pl.DataFrame:
            nonlocal call_count
            call_count += 1
            return pl.DataFrame({"value": [x], "call": [call_count]})

        # First call
        result1 = compute(1)
        assert call_count == 1

        # Cached call
        result2 = compute(1)
        assert call_count == 1
        assert result2["call"][0] == 1

        # Force refresh
        result3 = compute(1, _cache_refresh=True)
        assert call_count == 2
        assert result3["call"][0] == 2

        # Clean up
        compute.invalidate(1)

    def test_cache_skip(self):
        """Test _cache_skip bypasses cache entirely."""
        call_count = 0

        @redis.cache(url=REDIS_URL, ttl=60, key_prefix="test:decorator:skip")
        def compute(x: int) -> pl.DataFrame:
            nonlocal call_count
            call_count += 1
            return pl.DataFrame({"value": [x]})

        # First call with skip - should not cache
        result1 = compute(1, _cache_skip=True)
        assert call_count == 1

        # Second call with skip - should compute again
        result2 = compute(1, _cache_skip=True)
        assert call_count == 2

        # Normal call - should also compute (nothing was cached)
        result3 = compute(1)
        assert call_count == 3

        # Now it's cached
        result4 = compute(1)
        assert call_count == 3

        # Clean up
        compute.invalidate(1)

    def test_invalidate(self):
        """Test cache invalidation."""
        call_count = 0

        @redis.cache(url=REDIS_URL, ttl=60, key_prefix="test:decorator:invalidate")
        def compute(x: int) -> pl.DataFrame:
            nonlocal call_count
            call_count += 1
            return pl.DataFrame({"value": [x]})

        # Cache a result
        compute(1)
        assert call_count == 1

        # Verify it's cached
        assert compute.is_cached(1)

        # Invalidate
        deleted = compute.invalidate(1)
        assert deleted is True

        # Verify it's gone
        assert not compute.is_cached(1)

        # Next call should recompute
        compute(1)
        assert call_count == 2

        # Clean up
        compute.invalidate(1)

    def test_is_cached(self):
        """Test is_cached method."""

        @redis.cache(url=REDIS_URL, ttl=60, key_prefix="test:decorator:is_cached")
        def compute(x: int) -> pl.DataFrame:
            return pl.DataFrame({"value": [x]})

        # Not cached initially
        assert not compute.is_cached(42)

        # Cache it
        compute(42)

        # Now it's cached
        assert compute.is_cached(42)

        # Different args are not cached
        assert not compute.is_cached(43)

        # Clean up
        compute.invalidate(42)

    def test_cache_key_for(self):
        """Test cache_key_for method."""

        @redis.cache(url=REDIS_URL, key_prefix="test:decorator:key")
        def compute(x: int, y: str) -> pl.DataFrame:
            return pl.DataFrame({"value": [x]})

        key = compute.cache_key_for(1, "a")
        assert key.startswith("test:decorator:key:")
        assert "compute" in key

        # Same args should give same key
        key2 = compute.cache_key_for(1, "a")
        assert key == key2

        # Different args should give different key
        key3 = compute.cache_key_for(1, "b")
        assert key != key3

    def test_custom_key_fn(self):
        """Test custom key function."""
        call_count = 0

        @redis.cache(
            url=REDIS_URL,
            ttl=60,
            key_prefix="test:decorator:custom_key",
            key_fn=lambda start, end: f"{start}_{end}",
        )
        def compute(start: str, end: str) -> pl.DataFrame:
            nonlocal call_count
            call_count += 1
            return pl.DataFrame({"start": [start], "end": [end]})

        # Check key format
        key = compute.cache_key_for("2024-01-01", "2024-12-31")
        assert "2024-01-01_2024-12-31" in key

        # Verify caching works
        compute("2024-01-01", "2024-12-31")
        assert call_count == 1

        compute("2024-01-01", "2024-12-31")
        assert call_count == 1  # Cached

        # Clean up
        compute.invalidate("2024-01-01", "2024-12-31")

    def test_multiple_args(self):
        """Test caching with multiple argument types."""

        @redis.cache(url=REDIS_URL, ttl=60, key_prefix="test:decorator:multi_args")
        def compute(a: int, b: str, c: list, d: dict) -> pl.DataFrame:
            return pl.DataFrame({"a": [a], "b": [b]})

        # Cache with complex args
        result1 = compute(1, "hello", [1, 2, 3], {"key": "value"})

        # Same args should hit cache
        result2 = compute(1, "hello", [1, 2, 3], {"key": "value"})
        assert_frame_equal(result1, result2)

        # Different args should miss
        result3 = compute(1, "hello", [1, 2, 4], {"key": "value"})  # Different list

        # Clean up
        compute.invalidate(1, "hello", [1, 2, 3], {"key": "value"})
        compute.invalidate(1, "hello", [1, 2, 4], {"key": "value"})

    def test_kwargs(self):
        """Test caching with keyword arguments."""
        call_count = 0

        @redis.cache(url=REDIS_URL, ttl=60, key_prefix="test:decorator:kwargs")
        def compute(x: int, multiplier: int = 2) -> pl.DataFrame:
            nonlocal call_count
            call_count += 1
            return pl.DataFrame({"value": [x * multiplier]})

        # Positional and keyword should be equivalent
        result1 = compute(5, multiplier=2)
        assert call_count == 1

        result2 = compute(5)  # Uses default
        assert call_count == 1  # Should hit cache

        # Different kwarg value
        result3 = compute(5, multiplier=3)
        assert call_count == 2

        # Clean up
        compute.invalidate(5, multiplier=2)
        compute.invalidate(5, multiplier=3)

    def test_compression(self):
        """Test caching with compression."""

        @redis.cache(
            url=REDIS_URL,
            ttl=60,
            key_prefix="test:decorator:compression",
            compression="zstd",
        )
        def compute(x: int) -> pl.DataFrame:
            return pl.DataFrame({"value": list(range(x))})

        result1 = compute(100)
        result2 = compute(100)
        assert_frame_equal(result1, result2)

        # Clean up
        compute.invalidate(100)

    def test_parquet_format(self):
        """Test caching with Parquet format."""

        @redis.cache(
            url=REDIS_URL,
            ttl=60,
            key_prefix="test:decorator:parquet",
            format="parquet",
        )
        def compute(x: int) -> pl.DataFrame:
            return pl.DataFrame({"value": list(range(x))})

        result1 = compute(50)
        result2 = compute(50)
        assert_frame_equal(result1, result2)

        # Clean up
        compute.invalidate(50)


class TestCacheLazyDecorator:
    """Tests for the @cache_lazy decorator."""

    def test_basic_lazy_caching(self):
        """Test that LazyFrame results are cached."""
        call_count = 0

        @redis.cache_lazy(url=REDIS_URL, ttl=60, key_prefix="test:lazy")
        def build_pipeline(x: int) -> pl.LazyFrame:
            nonlocal call_count
            call_count += 1
            return pl.DataFrame({"value": [x * 2]}).lazy()

        # First call - should compute
        lf1 = build_pipeline(5)
        assert call_count == 1
        assert isinstance(lf1, pl.LazyFrame)
        df1 = lf1.collect()
        assert df1["value"][0] == 10

        # Second call - should use cache
        lf2 = build_pipeline(5)
        assert call_count == 1  # Not incremented
        df2 = lf2.collect()
        assert_frame_equal(df1, df2)

        # Clean up
        build_pipeline.invalidate(5)

    def test_lazy_refresh(self):
        """Test _cache_refresh with LazyFrame."""
        call_count = 0

        @redis.cache_lazy(url=REDIS_URL, ttl=60, key_prefix="test:lazy:refresh")
        def build_pipeline(x: int) -> pl.LazyFrame:
            nonlocal call_count
            call_count += 1
            return pl.DataFrame({"value": [x], "call": [call_count]}).lazy()

        # First call
        lf1 = build_pipeline(1)
        assert call_count == 1

        # Cached call
        lf2 = build_pipeline(1)
        assert call_count == 1

        # Force refresh
        lf3 = build_pipeline(1, _cache_refresh=True)
        assert call_count == 2
        df3 = lf3.collect()
        assert df3["call"][0] == 2

        # Clean up
        build_pipeline.invalidate(1)

    def test_lazy_skip(self):
        """Test _cache_skip with LazyFrame."""
        call_count = 0

        @redis.cache_lazy(url=REDIS_URL, ttl=60, key_prefix="test:lazy:skip")
        def build_pipeline(x: int) -> pl.LazyFrame:
            nonlocal call_count
            call_count += 1
            return pl.DataFrame({"value": [x]}).lazy()

        # Skip cache
        lf1 = build_pipeline(1, _cache_skip=True)
        assert call_count == 1

        # Skip again
        lf2 = build_pipeline(1, _cache_skip=True)
        assert call_count == 2

    def test_lazy_invalidate(self):
        """Test cache invalidation for LazyFrame."""

        @redis.cache_lazy(url=REDIS_URL, ttl=60, key_prefix="test:lazy:invalidate")
        def build_pipeline(x: int) -> pl.LazyFrame:
            return pl.DataFrame({"value": [x]}).lazy()

        # Cache a result
        build_pipeline(1)
        assert build_pipeline.is_cached(1)

        # Invalidate
        build_pipeline.invalidate(1)
        assert not build_pipeline.is_cached(1)


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_dataframe(self):
        """Test caching empty DataFrames."""

        @redis.cache(url=REDIS_URL, ttl=60, key_prefix="test:empty")
        def compute() -> pl.DataFrame:
            return pl.DataFrame({"a": [], "b": []}).cast({"a": pl.Int64, "b": pl.Utf8})

        result1 = compute()
        result2 = compute()
        assert_frame_equal(result1, result2)
        assert len(result1) == 0

        # Clean up
        compute.invalidate()

    def test_large_dataframe(self):
        """Test caching larger DataFrames (triggers chunking)."""

        @redis.cache(
            url=REDIS_URL,
            ttl=60,
            key_prefix="test:large",
            chunk_size_mb=0.001,  # Force chunking with tiny chunks
        )
        def compute(n: int) -> pl.DataFrame:
            return pl.DataFrame(
                {
                    "id": list(range(n)),
                    "data": [f"row_{i}" for i in range(n)],
                }
            )

        result1 = compute(1000)
        result2 = compute(1000)
        assert_frame_equal(result1, result2)

        # Clean up
        compute.invalidate(1000)

    def test_function_metadata_preserved(self):
        """Test that function metadata is preserved."""

        @redis.cache(url=REDIS_URL, key_prefix="test:meta")
        def my_function(x: int) -> pl.DataFrame:
            """This is my docstring."""
            return pl.DataFrame({"value": [x]})

        assert my_function.__name__ == "my_function"
        assert "docstring" in my_function.__doc__

    def test_none_args(self):
        """Test caching with None arguments."""

        @redis.cache(url=REDIS_URL, ttl=60, key_prefix="test:none")
        def compute(x: int | None) -> pl.DataFrame:
            return pl.DataFrame({"value": [x if x else 0]})

        result1 = compute(None)
        result2 = compute(None)
        assert_frame_equal(result1, result2)

        # Clean up
        compute.invalidate(None)
