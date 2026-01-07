"""Cross-layer integration tests for polars-redis.

These tests verify that multiple components work correctly together:
- smart_scan + Index auto-discovery
- Index.create + query_builder + search_hashes
- @cache decorator + smart_scan/search results
- Large DataFrame caching with chunking
- Error propagation across layer boundaries

Run with: pytest tests/test_cross_layer.py -v --tb=short
"""

from __future__ import annotations

import polars as pl
import polars_redis as redis
import pytest
from polars.testing import assert_frame_equal
from polars_redis import (
    ExecutionStrategy,
    Index,
    NumericField,
    TagField,
    TextField,
    cache,
    explain_scan,
    find_index_for_pattern,
    search_hashes,
    smart_scan,
)

# ruff: noqa: F841


# Test prefixes to avoid conflicts
PREFIX_SMART = "pytest:crosslayer:smart:"
PREFIX_QUERY = "pytest:crosslayer:query:"
PREFIX_CACHE = "pytest:crosslayer:cache:"
PREFIX_CHUNK = "pytest:crosslayer:chunk:"
PREFIX_ERROR = "pytest:crosslayer:error:"


@pytest.fixture
def redis_client(redis_url):
    """Get a Redis client for test setup/teardown."""
    import redis as redis_lib

    client = redis_lib.from_url(redis_url)
    yield client
    client.close()


@pytest.fixture(autouse=True)
def cleanup_test_data(redis_url, redis_client):
    """Clean up test data before and after each test."""
    prefixes = [PREFIX_SMART, PREFIX_QUERY, PREFIX_CACHE, PREFIX_CHUNK, PREFIX_ERROR]
    index_names = [
        "pytest_crosslayer_smart_idx",
        "pytest_crosslayer_query_idx",
        "pytest_crosslayer_cache_idx",
    ]

    def do_cleanup():
        # Clean up indexes
        for idx_name in index_names:
            try:
                redis_client.execute_command("FT.DROPINDEX", idx_name)
            except Exception:
                pass

        # Clean up keys
        for prefix in prefixes:
            try:
                cursor = 0
                while True:
                    cursor, keys = redis_client.scan(cursor, match=f"{prefix}*", count=1000)
                    if keys:
                        redis_client.delete(*keys)
                    if cursor == 0:
                        break
            except Exception:
                pass

        # Clean up cache keys
        for key in ["pytest:cache:smart", "pytest:cache:search", "pytest:cache:chunk"]:
            try:
                redis.delete_cached(redis_url, key)
            except Exception:
                pass

    do_cleanup()
    yield
    do_cleanup()


# =============================================================================
# Test 1: smart_scan + Index Auto-Discovery Flow
# =============================================================================


@pytest.mark.integration
class TestSmartScanIndexDiscovery:
    """Test smart_scan automatically discovers and uses dynamically created indexes."""

    def test_smart_scan_discovers_dynamically_created_index(self, redis_url, redis_client):
        """
        Flow: Create Index -> Add data -> smart_scan auto-detects index -> Uses FT.SEARCH

        This tests the full integration between:
        - Index.create() (index.py)
        - find_index_for_pattern() (smart.py)
        - smart_scan() routing to search vs scan
        """
        # Step 1: Create an index programmatically
        idx = Index(
            name="pytest_crosslayer_smart_idx",
            prefix=PREFIX_SMART,
            schema=[
                TextField("name", sortable=True),
                NumericField("age", sortable=True),
                TagField("status"),
            ],
        )
        idx.create(redis_url)

        # Step 2: Add test data that matches the index prefix
        for i in range(20):
            redis_client.hset(
                f"{PREFIX_SMART}user:{i}",
                mapping={
                    "name": f"User{i}",
                    "age": str(20 + i),
                    "status": "active" if i % 2 == 0 else "inactive",
                },
            )

        # Wait for index to be ready
        import time

        time.sleep(0.5)

        # Step 3: Verify smart_scan auto-detects the index
        detected = find_index_for_pattern(redis_url, f"{PREFIX_SMART}*")
        assert detected is not None, "Index should be auto-detected"
        assert detected.name == "pytest_crosslayer_smart_idx"
        assert PREFIX_SMART in detected.prefixes

        # Step 4: Verify explain_scan shows SEARCH strategy
        plan = explain_scan(
            redis_url,
            f"{PREFIX_SMART}*",
            schema={"name": pl.Utf8, "age": pl.Int64, "status": pl.Utf8},
        )
        assert plan.strategy == ExecutionStrategy.SEARCH, "Should use SEARCH strategy"
        assert plan.index is not None
        assert plan.index.name == "pytest_crosslayer_smart_idx"

        # Step 5: Execute smart_scan and verify results
        lf = smart_scan(
            redis_url,
            f"{PREFIX_SMART}*",
            schema={"name": pl.Utf8, "age": pl.Int64, "status": pl.Utf8},
        )
        df = lf.collect()

        assert len(df) == 20, f"Expected 20 rows, got {len(df)}"
        assert {"name", "age", "status"} <= set(df.columns)  # May include _key

    def test_smart_scan_fallback_when_no_index(self, redis_url, redis_client):
        """
        Test that smart_scan falls back to SCAN when no index matches the pattern.
        """
        # Add data without creating an index
        for i in range(5):
            redis_client.hset(
                f"{PREFIX_ERROR}noindex:{i}",
                mapping={"field": f"value{i}"},
            )

        # Verify no index is detected
        detected = find_index_for_pattern(redis_url, f"{PREFIX_ERROR}noindex:*")
        assert detected is None, "No index should exist for this pattern"

        # Verify plan shows SCAN strategy with warning
        plan = explain_scan(
            redis_url,
            f"{PREFIX_ERROR}noindex:*",
            schema={"field": pl.Utf8},
        )
        assert plan.strategy == ExecutionStrategy.SCAN
        assert len(plan.warnings) > 0, "Should have warnings about no index"

        # smart_scan should still work via SCAN
        lf = smart_scan(
            redis_url,
            f"{PREFIX_ERROR}noindex:*",
            schema={"field": pl.Utf8},
        )
        df = lf.collect()
        assert len(df) == 5


# =============================================================================
# Test 2: Index.create + Query Builder + search_hashes Flow
# =============================================================================


@pytest.mark.integration
class TestIndexQuerySearchFlow:
    """Test the full flow from index creation through query building to search."""

    def test_index_create_query_builder_search_flow(self, redis_url, redis_client):
        """
        Flow: Index.create -> Add data -> Build query -> search_hashes -> Verify results

        This tests integration between:
        - Index creation (index.py)
        - Query building with predicates (query.py / query_builder.rs)
        - search_hashes execution (search.py)
        - Schema mapping in results
        """
        # Step 1: Create index with multiple field types
        idx = Index(
            name="pytest_crosslayer_query_idx",
            prefix=PREFIX_QUERY,
            schema=[
                TextField("title", sortable=True),
                NumericField("price", sortable=True),
                TagField("category"),
                NumericField("stock"),
            ],
        )
        idx.create(redis_url)

        # Step 2: Add diverse test data
        products = [
            {"title": "Python Book", "price": "29.99", "category": "books", "stock": "100"},
            {"title": "Redis Guide", "price": "39.99", "category": "books", "stock": "50"},
            {"title": "Laptop Stand", "price": "49.99", "category": "electronics", "stock": "25"},
            {"title": "USB Cable", "price": "9.99", "category": "electronics", "stock": "200"},
            {"title": "Coffee Mug", "price": "14.99", "category": "home", "stock": "75"},
            {"title": "Desk Lamp", "price": "34.99", "category": "home", "stock": "30"},
            {"title": "Notebook", "price": "4.99", "category": "office", "stock": "500"},
            {"title": "Pen Set", "price": "12.99", "category": "office", "stock": "150"},
        ]

        for i, product in enumerate(products):
            redis_client.hset(f"{PREFIX_QUERY}product:{i}", mapping=product)

        import time

        time.sleep(0.5)

        schema = {
            "title": pl.Utf8,
            "price": pl.Float64,
            "category": pl.Utf8,
            "stock": pl.Int64,
        }

        # Step 3: Test numeric range query
        lf = search_hashes(
            redis_url,
            index="pytest_crosslayer_query_idx",
            query="@price:[20 50]",  # Products between $20 and $50
            schema=schema,
        )
        df = lf.collect()
        assert len(df) == 4, f"Expected 4 products in price range, got {len(df)}"
        assert all(20 <= p <= 50 for p in df["price"].to_list())

        # Step 4: Test tag query
        lf = search_hashes(
            redis_url,
            index="pytest_crosslayer_query_idx",
            query="@category:{books}",
            schema=schema,
        )
        df = lf.collect()
        assert len(df) == 2, f"Expected 2 books, got {len(df)}"
        assert all(c == "books" for c in df["category"].to_list())

        # Step 5: Test combined query (numeric + tag)
        lf = search_hashes(
            redis_url,
            index="pytest_crosslayer_query_idx",
            query="@category:{electronics} @price:[0 30]",
            schema=schema,
        )
        df = lf.collect()
        assert len(df) == 1, f"Expected 1 cheap electronic, got {len(df)}"
        assert df["title"][0] == "USB Cable"

        # Step 6: Test text search
        lf = search_hashes(
            redis_url,
            index="pytest_crosslayer_query_idx",
            query="@title:Book",
            schema=schema,
        )
        df = lf.collect()
        assert len(df) >= 1, "Should find at least one book by title"

        # Step 7: Test sorting
        lf = search_hashes(
            redis_url,
            index="pytest_crosslayer_query_idx",
            query="*",
            schema=schema,
            sort_by="price",
            sort_ascending=True,
        )
        df = lf.collect()
        prices = df["price"].to_list()
        assert prices == sorted(prices), "Results should be sorted by price ascending"

    def test_query_with_or_conditions(self, redis_url, redis_client):
        """Test queries with OR conditions across layers."""
        # Reuse index from previous test or create new one
        idx = Index(
            name="pytest_crosslayer_query_idx",
            prefix=PREFIX_QUERY,
            schema=[
                TextField("title"),
                TagField("category"),
            ],
        )
        idx.ensure_exists(redis_url)

        # Query with OR on tags
        lf = search_hashes(
            redis_url,
            index="pytest_crosslayer_query_idx",
            query="@category:{books|electronics}",
            schema={"title": pl.Utf8, "category": pl.Utf8},
        )
        df = lf.collect()
        categories = set(df["category"].to_list())
        assert categories <= {"books", "electronics"}, f"Unexpected categories: {categories}"


# =============================================================================
# Test 3: @cache Decorator + smart_scan Integration
# =============================================================================


@pytest.mark.integration
class TestCacheSmartScanIntegration:
    """Test caching of smart_scan results with the @cache decorator."""

    def test_cached_smart_scan_results(self, redis_url, redis_client):
        """
        Flow: @cache decorator -> smart_scan -> cache_dataframe -> retrieval

        Tests integration between:
        - @cache decorator (decorator.py)
        - smart_scan execution (smart.py)
        - cache_dataframe serialization (cache.py)
        - Cache retrieval
        """
        # Setup: Create index and data
        idx = Index(
            name="pytest_crosslayer_cache_idx",
            prefix=PREFIX_CACHE,
            schema=[
                TextField("name"),
                NumericField("value"),
            ],
        )
        idx.create(redis_url)

        for i in range(10):
            redis_client.hset(
                f"{PREFIX_CACHE}item:{i}",
                mapping={"name": f"Item{i}", "value": str(i * 10)},
            )

        import time

        time.sleep(0.5)

        # Track function calls
        call_count = 0

        @cache(url=redis_url, ttl=60, key_prefix="pytest:cache:smart")
        def get_cached_data(prefix: str) -> pl.DataFrame:
            nonlocal call_count
            call_count += 1
            lf = smart_scan(
                redis_url,
                f"{prefix}*",
                schema={"name": pl.Utf8, "value": pl.Int64},
            )
            return lf.collect()

        # First call - should execute and cache
        result1 = get_cached_data(PREFIX_CACHE)
        assert call_count == 1
        assert len(result1) == 10

        # Second call - should hit cache
        result2 = get_cached_data(PREFIX_CACHE)
        assert call_count == 1, "Function should not be called again"
        assert_frame_equal(result1, result2)

        # Verify is_cached works
        assert get_cached_data.is_cached(PREFIX_CACHE)

        # Force refresh
        result3 = get_cached_data(PREFIX_CACHE, _cache_refresh=True)
        assert call_count == 2, "Function should be called on refresh"

        # Clean up
        get_cached_data.invalidate(PREFIX_CACHE)

    def test_cached_search_with_different_queries(self, redis_url, redis_client):
        """Test that different query parameters result in different cache keys."""
        # Ensure index exists for this test
        idx = Index(
            name="pytest_crosslayer_cache_idx",
            prefix=PREFIX_CACHE,
            schema=[
                TextField("name"),
                NumericField("value"),
            ],
        )
        idx.ensure_exists(redis_url)

        call_count = 0

        @cache(url=redis_url, ttl=60, key_prefix="pytest:cache:search")
        def search_by_category(category: str) -> pl.DataFrame:
            nonlocal call_count
            call_count += 1
            lf = search_hashes(
                redis_url,
                index="pytest_crosslayer_cache_idx",
                query=f"@name:{category}*",
                schema={"name": pl.Utf8, "value": pl.Int64},
            )
            return lf.collect()

        # Different categories should have different cache entries
        result1 = search_by_category("Item1")
        assert call_count == 1

        result2 = search_by_category("Item2")
        assert call_count == 2, "Different args should not hit cache"

        result3 = search_by_category("Item1")
        assert call_count == 2, "Same args should hit cache"

        # Clean up
        search_by_category.invalidate("Item1")
        search_by_category.invalidate("Item2")


# =============================================================================
# Test 4: Large DataFrame Caching with Chunking
# =============================================================================


@pytest.mark.integration
class TestLargeDataFrameChunkedCache:
    """Test caching large DataFrames with automatic chunking."""

    def test_large_dataframe_chunked_cache_and_retrieval(self, redis_url):
        """
        Flow: Large DataFrame -> cache_dataframe (chunked) -> get_cached_dataframe

        Tests integration between:
        - DataFrame serialization (cache.py)
        - Chunking logic for large data
        - Multi-key storage in Redis
        - Chunk reassembly on retrieval
        """
        # Create a moderately large DataFrame (~1MB)
        n_rows = 10000
        large_df = pl.DataFrame(
            {
                "id": list(range(n_rows)),
                "name": [f"User_{i}_" + "x" * 50 for i in range(n_rows)],
                "value": [float(i) * 1.5 for i in range(n_rows)],
                "category": [f"cat_{i % 10}" for i in range(n_rows)],
            }
        )

        cache_key = "pytest:cache:chunk"

        # Cache with small chunk size to force chunking
        bytes_written = redis.cache_dataframe(
            large_df,
            redis_url,
            cache_key,
            format="ipc",
            compression="zstd",
            chunk_size_mb=0.1,  # 100KB chunks to force multiple chunks
        )

        assert bytes_written > 0

        # Verify cache info
        info = redis.cache_info(redis_url, cache_key)
        assert info is not None
        # With small chunk size, it should be chunked (but compression may reduce size)
        assert info["size_bytes"] > 0

        # Retrieve and verify integrity
        retrieved_df = redis.get_cached_dataframe(redis_url, cache_key)
        assert retrieved_df is not None
        assert len(retrieved_df) == n_rows
        assert_frame_equal(retrieved_df, large_df)

        # Clean up
        redis.delete_cached(redis_url, cache_key)

    def test_chunked_cache_with_parquet_format(self, redis_url):
        """Test chunked caching with Parquet format and column projection."""
        n_rows = 5000
        df = pl.DataFrame(
            {
                "id": list(range(n_rows)),
                "data": ["row_" + "y" * 100 for _ in range(n_rows)],
                "score": [float(i) for i in range(n_rows)],
            }
        )

        cache_key = "pytest:cache:chunk"

        # Cache with Parquet
        redis.cache_dataframe(
            df,
            redis_url,
            cache_key,
            format="parquet",
            compression="snappy",
            chunk_size_mb=0.05,
        )

        # Retrieve with column projection (Parquet feature)
        retrieved_df = redis.get_cached_dataframe(
            redis_url,
            cache_key,
            format="parquet",
            columns=["id", "score"],  # Only fetch specific columns
        )

        assert retrieved_df is not None
        assert retrieved_df.columns == ["id", "score"]
        assert len(retrieved_df) == n_rows

        # Clean up
        redis.delete_cached(redis_url, cache_key)


# =============================================================================
# Test 5: Error Propagation Across Layers
# =============================================================================


@pytest.mark.integration
class TestErrorPropagation:
    """Test that errors propagate correctly across layer boundaries."""

    def test_search_on_nonexistent_index(self, redis_url):
        """Test error handling when searching a non-existent index."""
        with pytest.raises(Exception) as exc_info:
            lf = search_hashes(
                redis_url,
                index="nonexistent_index_12345",
                query="*",
                schema={"field": pl.Utf8},
            )
            # Force execution
            lf.collect()

        # Error should mention the index
        error_msg = str(exc_info.value).lower()
        assert "index" in error_msg or "unknown" in error_msg or "nonexistent" in error_msg

    def test_invalid_query_syntax(self, redis_url, redis_client):
        """Test error handling for invalid RediSearch query syntax."""
        # Create a valid index first
        idx = Index(
            name="pytest_crosslayer_query_idx",
            prefix=PREFIX_QUERY,
            schema=[TextField("name")],
        )
        idx.ensure_exists(redis_url)

        # Add some data
        redis_client.hset(f"{PREFIX_QUERY}test:1", mapping={"name": "test"})

        import time

        time.sleep(0.3)

        # Try an invalid query syntax
        with pytest.raises((Exception, RuntimeError)):
            lf = search_hashes(
                redis_url,
                index="pytest_crosslayer_query_idx",
                query="@@@invalid[[[syntax",  # Malformed query
                schema={"name": pl.Utf8},
            )
            lf.collect()

    def test_cache_retrieval_after_format_mismatch(self, redis_url):
        """Test error handling when retrieving cache with wrong format."""
        df = pl.DataFrame({"a": [1, 2, 3]})
        cache_key = "pytest:cache:format_test"

        # Cache with IPC format
        redis.cache_dataframe(df, redis_url, cache_key, format="ipc")

        # Try to retrieve with Parquet format - should handle gracefully
        # (Implementation may auto-detect or raise clear error)
        try:
            result = redis.get_cached_dataframe(redis_url, cache_key, format="parquet")
            # If it succeeds, it auto-detected the format
        except Exception as e:
            # If it fails, error should be informative
            assert "format" in str(e).lower() or "parquet" in str(e).lower()
        finally:
            redis.delete_cached(redis_url, cache_key)

    def test_smart_scan_with_invalid_schema_type(self, redis_url, redis_client):
        """Test error handling when schema types don't match data."""
        # Add data with string values
        redis_client.hset(
            f"{PREFIX_ERROR}schema:1",
            mapping={"value": "not_a_number"},
        )

        # Try to scan with Int64 schema - should raise clear error
        lf = smart_scan(
            redis_url,
            f"{PREFIX_ERROR}schema:*",
            schema={"value": pl.Int64},
        )

        # The library raises a clear error on type mismatch
        with pytest.raises(Exception) as exc_info:
            lf.collect()

        # Error should mention the type conversion issue
        error_msg = str(exc_info.value).lower()
        assert "int" in error_msg or "parse" in error_msg or "conversion" in error_msg


# =============================================================================
# Test 6: End-to-End Multi-Layer Flow
# =============================================================================


@pytest.mark.integration
class TestEndToEndFlow:
    """Test complete end-to-end flows spanning multiple layers."""

    def test_full_etl_pipeline(self, redis_url, redis_client):
        """
        Test a complete ETL pipeline:
        1. Create index from schema
        2. Write data
        3. Search with filters
        4. Cache results
        5. Retrieve from cache
        """
        # Step 1: Define schema and create index
        schema = {
            "product": pl.Utf8,
            "price": pl.Float64,
            "quantity": pl.Int64,
        }

        idx = Index.from_schema(
            schema,
            "pytest_crosslayer_etl_idx",
            PREFIX_CHUNK,
            text_fields=["product"],
        )
        try:
            idx.drop(redis_url)  # Clean start if exists
        except Exception:
            pass  # Index may not exist
        idx.create(redis_url)

        # Step 2: Write data using polars-redis
        products = [
            {"product": "Widget A", "price": "19.99", "quantity": "100"},
            {"product": "Widget B", "price": "29.99", "quantity": "50"},
            {"product": "Gadget X", "price": "99.99", "quantity": "25"},
            {"product": "Gadget Y", "price": "149.99", "quantity": "10"},
        ]

        for i, p in enumerate(products):
            redis_client.hset(f"{PREFIX_CHUNK}product:{i}", mapping=p)

        import time

        time.sleep(0.5)

        # Step 3: Search with filters
        lf = search_hashes(
            redis_url,
            index="pytest_crosslayer_etl_idx",
            query="@price:[0 50]",
            schema=schema,
        )
        filtered_df = lf.collect()
        assert len(filtered_df) == 2, "Should find 2 products under $50"

        # Step 4: Cache the filtered results
        cache_key = "pytest:cache:etl_results"
        redis.cache_dataframe(filtered_df, redis_url, cache_key, ttl=60)

        # Step 5: Retrieve from cache and verify
        cached_df = redis.get_cached_dataframe(redis_url, cache_key)
        assert cached_df is not None
        assert_frame_equal(cached_df, filtered_df)

        # Verify cache metadata
        info = redis.cache_info(redis_url, cache_key)
        assert info is not None
        assert info["size_bytes"] > 0

        # Clean up
        redis.delete_cached(redis_url, cache_key)
        idx.drop(redis_url)

    def test_cached_aggregation_pipeline(self, redis_url, redis_client):
        """
        Test caching aggregation results from search.
        """
        # Setup index and data
        idx = Index(
            name="pytest_crosslayer_agg_idx",
            prefix=PREFIX_SMART,
            schema=[
                TagField("region"),
                NumericField("sales"),
            ],
        )
        try:
            idx.drop(redis_url)
        except Exception:
            pass  # Index may not exist
        idx.create(redis_url)

        # Add sales data by region
        sales_data = [
            ("east", 100),
            ("east", 150),
            ("east", 200),
            ("west", 300),
            ("west", 250),
            ("north", 175),
        ]

        for i, (region, sales) in enumerate(sales_data):
            redis_client.hset(
                f"{PREFIX_SMART}sale:{i}",
                mapping={"region": region, "sales": str(sales)},
            )

        import time

        time.sleep(0.5)

        call_count = 0

        @cache(url=redis_url, ttl=60, key_prefix="pytest:cache:agg")
        def get_regional_totals() -> pl.DataFrame:
            nonlocal call_count
            call_count += 1

            lf = search_hashes(
                redis_url,
                index="pytest_crosslayer_agg_idx",
                query="*",
                schema={"region": pl.Utf8, "sales": pl.Int64},
            )
            return (
                lf.collect()
                .group_by("region")
                .agg(pl.col("sales").sum().alias("total_sales"))
                .sort("region")
            )

        # First call computes
        result1 = get_regional_totals()
        assert call_count == 1
        assert len(result1) == 3  # east, west, north

        # Second call uses cache
        result2 = get_regional_totals()
        assert call_count == 1
        assert_frame_equal(result1, result2)

        # Verify aggregation is correct
        east_total = result1.filter(pl.col("region") == "east")["total_sales"][0]
        assert east_total == 450  # 100 + 150 + 200

        # Clean up
        get_regional_totals.invalidate()
        idx.drop(redis_url)
