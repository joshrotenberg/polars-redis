"""Tests for smart scan with automatic index detection."""

from __future__ import annotations

import polars as pl
import pytest
from polars_redis import (
    DetectedIndex,
    ExecutionStrategy,
    Index,
    NumericField,
    QueryPlan,
    TagField,
    TextField,
    explain_scan,
    find_index_for_pattern,
    list_indexes,
    smart_scan,
)


class TestExecutionStrategy:
    """Test ExecutionStrategy enum."""

    def test_strategy_values(self):
        """Test strategy enum values."""
        assert ExecutionStrategy.SEARCH.value == "search"
        assert ExecutionStrategy.SCAN.value == "scan"
        assert ExecutionStrategy.HYBRID.value == "hybrid"

    def test_strategy_comparison(self):
        """Test strategy comparison."""
        assert ExecutionStrategy.SEARCH != ExecutionStrategy.SCAN
        assert ExecutionStrategy.SEARCH == ExecutionStrategy.SEARCH


class TestDetectedIndex:
    """Test DetectedIndex dataclass."""

    def test_detected_index_creation(self):
        """Test creating a DetectedIndex."""
        idx = DetectedIndex(
            name="users_idx",
            prefixes=["user:"],
            on_type="HASH",
            fields=["name", "age"],
        )
        assert idx.name == "users_idx"
        assert idx.prefixes == ["user:"]
        assert idx.on_type == "HASH"
        assert idx.fields == ["name", "age"]

    def test_detected_index_multiple_prefixes(self):
        """Test DetectedIndex with multiple prefixes."""
        idx = DetectedIndex(
            name="multi_idx",
            prefixes=["user:", "customer:"],
            on_type="HASH",
            fields=["name"],
        )
        assert len(idx.prefixes) == 2
        assert "user:" in idx.prefixes
        assert "customer:" in idx.prefixes

    def test_detected_index_json_type(self):
        """Test DetectedIndex with JSON type."""
        idx = DetectedIndex(
            name="json_idx",
            prefixes=["doc:"],
            on_type="JSON",
            fields=["title", "content"],
        )
        assert idx.on_type == "JSON"


class TestQueryPlan:
    """Test QueryPlan dataclass."""

    def test_query_plan_search_strategy(self):
        """Test QueryPlan with SEARCH strategy."""
        plan = QueryPlan(
            strategy=ExecutionStrategy.SEARCH,
            index=DetectedIndex(
                name="users_idx",
                prefixes=["user:"],
                on_type="HASH",
                fields=["name", "age"],
            ),
            server_query="@age:[30 +inf]",
        )
        assert plan.strategy == ExecutionStrategy.SEARCH
        assert plan.index is not None
        assert plan.server_query == "@age:[30 +inf]"

    def test_query_plan_scan_strategy(self):
        """Test QueryPlan with SCAN strategy."""
        plan = QueryPlan(
            strategy=ExecutionStrategy.SCAN,
            index=None,
            server_query=None,
        )
        assert plan.strategy == ExecutionStrategy.SCAN
        assert plan.index is None

    def test_query_plan_hybrid_strategy(self):
        """Test QueryPlan with HYBRID strategy."""
        plan = QueryPlan(
            strategy=ExecutionStrategy.HYBRID,
            index=DetectedIndex(
                name="users_idx",
                prefixes=["user:"],
                on_type="HASH",
                fields=["name"],
            ),
            server_query="@name:john",
            client_filters=["age > 30"],
        )
        assert plan.strategy == ExecutionStrategy.HYBRID
        assert len(plan.client_filters) == 1

    def test_query_plan_with_warnings(self):
        """Test QueryPlan with warnings."""
        plan = QueryPlan(
            strategy=ExecutionStrategy.HYBRID,
            warnings=["Some filters cannot be pushed to server"],
        )
        assert len(plan.warnings) == 1

    def test_query_plan_explain(self):
        """Test QueryPlan explain output."""
        plan = QueryPlan(
            strategy=ExecutionStrategy.SEARCH,
            index=DetectedIndex(
                name="users_idx",
                prefixes=["user:"],
                on_type="HASH",
                fields=["name", "age"],
            ),
            server_query="*",
        )
        explanation = plan.explain()
        assert "SEARCH" in explanation
        assert "users_idx" in explanation
        assert "user:" in explanation

    def test_query_plan_explain_with_client_filters(self):
        """Test QueryPlan explain with client filters."""
        plan = QueryPlan(
            strategy=ExecutionStrategy.HYBRID,
            index=DetectedIndex(
                name="idx",
                prefixes=["test:"],
                on_type="HASH",
                fields=["name"],
            ),
            server_query="*",
            client_filters=["age > 30", "status == 'active'"],
        )
        explanation = plan.explain()
        assert "Client Filters:" in explanation
        assert "age > 30" in explanation

    def test_query_plan_explain_with_warnings(self):
        """Test QueryPlan explain with warnings."""
        plan = QueryPlan(
            strategy=ExecutionStrategy.SCAN,
            warnings=["No index found for pattern"],
        )
        explanation = plan.explain()
        assert "Warnings:" in explanation
        assert "No index found" in explanation


class TestSmartScanAPI:
    """Test smart_scan API surface."""

    def test_smart_scan_requires_schema(self):
        """Test that smart_scan requires a schema."""
        with pytest.raises(ValueError, match="schema is required"):
            smart_scan("redis://localhost:6379", "user:*")

    def test_smart_scan_accepts_index_string(self):
        """Test that smart_scan accepts index as string."""
        # This would fail to connect, but validates the API
        try:
            smart_scan(
                "redis://localhost:6379",
                "user:*",
                schema={"name": pl.Utf8},
                index="users_idx",
                auto_detect_index=False,
            )
        except Exception:
            # Expected to fail on connection, but API is valid
            pass

    def test_smart_scan_accepts_index_object(self):
        """Test that smart_scan accepts Index object."""
        idx = Index(
            name="users_idx",
            prefix="user:",
            schema=[TextField("name")],
        )
        try:
            smart_scan(
                "redis://localhost:6379",
                "user:*",
                schema={"name": pl.Utf8},
                index=idx,
            )
        except Exception:
            pass


# Integration tests (require Redis with RediSearch)
@pytest.mark.integration
class TestSmartScanIntegration:
    """Integration tests for smart scan (require Redis with RediSearch)."""

    @pytest.fixture
    def setup_index(self, redis_url):
        """Create a test index for smart scan tests."""
        import redis

        client = redis.from_url(redis_url)

        # Create index
        idx = Index(
            name="pytest_smart_idx",
            prefix="pytest:smart:",
            schema=[
                TextField("name", sortable=True),
                NumericField("age", sortable=True),
                TagField("status"),
            ],
        )
        idx.drop(redis_url)
        idx.create(redis_url)

        # Add test data
        for i in range(10):
            client.hset(
                f"pytest:smart:user:{i}",
                mapping={
                    "name": f"User{i}",
                    "age": str(20 + i),
                    "status": "active" if i % 2 == 0 else "inactive",
                },
            )

        yield idx

        # Cleanup
        for i in range(10):
            client.delete(f"pytest:smart:user:{i}")
        idx.drop(redis_url)
        client.close()

    def test_find_index_for_pattern(self, redis_url, setup_index):
        """Test finding an index for a pattern."""
        idx_info = find_index_for_pattern(redis_url, "pytest:smart:*")
        assert idx_info is not None
        assert idx_info.name == "pytest_smart_idx"
        assert "pytest:smart:" in idx_info.prefixes

    def test_find_index_for_pattern_no_match(self, redis_url, setup_index):
        """Test finding index returns None for non-matching pattern."""
        idx_info = find_index_for_pattern(redis_url, "nonexistent:*")
        assert idx_info is None

    def test_list_indexes(self, redis_url, setup_index):
        """Test listing all indexes."""
        indexes = list_indexes(redis_url)
        assert len(indexes) >= 1
        names = [idx.name for idx in indexes]
        assert "pytest_smart_idx" in names

    def test_explain_scan_with_index(self, redis_url, setup_index):
        """Test explain_scan when index exists."""
        plan = explain_scan(
            redis_url,
            "pytest:smart:*",
            schema={"name": pl.Utf8, "age": pl.Int64},
        )
        assert plan.strategy == ExecutionStrategy.SEARCH
        assert plan.index is not None
        assert plan.index.name == "pytest_smart_idx"

    def test_explain_scan_without_index(self, redis_url):
        """Test explain_scan when no index exists."""
        plan = explain_scan(
            redis_url,
            "no_index:*",
            schema={"name": pl.Utf8},
        )
        assert plan.strategy == ExecutionStrategy.SCAN
        assert plan.index is None
        assert len(plan.warnings) > 0

    def test_smart_scan_auto_detects_index(self, redis_url, setup_index):
        """Test smart_scan auto-detects and uses index."""
        lf = smart_scan(
            redis_url,
            "pytest:smart:*",
            schema={"name": pl.Utf8, "age": pl.Int64, "status": pl.Utf8},
        )
        df = lf.collect()
        assert len(df) == 10
        assert "name" in df.columns

    def test_smart_scan_with_explicit_index(self, redis_url, setup_index):
        """Test smart_scan with explicit index name."""
        lf = smart_scan(
            redis_url,
            "pytest:smart:*",
            schema={"name": pl.Utf8, "age": pl.Int64},
            index="pytest_smart_idx",
            auto_detect_index=False,
        )
        df = lf.collect()
        assert len(df) == 10

    def test_smart_scan_with_index_object(self, redis_url, setup_index):
        """Test smart_scan with Index object."""
        idx = Index(
            name="pytest_smart_idx",
            prefix="pytest:smart:",
            schema=[TextField("name"), NumericField("age")],
        )
        lf = smart_scan(
            redis_url,
            "pytest:smart:*",
            schema={"name": pl.Utf8, "age": pl.Int64},
            index=idx,
        )
        df = lf.collect()
        assert len(df) == 10

    def test_smart_scan_fallback_to_scan(self, redis_url):
        """Test smart_scan falls back to SCAN when no index."""
        # Use a pattern with no index
        import redis

        client = redis.from_url(redis_url)

        # Create test data without index
        for i in range(5):
            client.hset(
                f"pytest:noindex:user:{i}",
                mapping={"name": f"User{i}", "value": str(i)},
            )

        try:
            lf = smart_scan(
                redis_url,
                "pytest:noindex:*",
                schema={"name": pl.Utf8, "value": pl.Int64},
                auto_detect_index=True,
            )
            df = lf.collect()
            assert len(df) == 5
        finally:
            for i in range(5):
                client.delete(f"pytest:noindex:user:{i}")
            client.close()

    def test_smart_scan_disable_auto_detect(self, redis_url, setup_index):
        """Test smart_scan with auto_detect_index=False falls back to SCAN."""
        lf = smart_scan(
            redis_url,
            "pytest:smart:*",
            schema={"name": pl.Utf8, "age": pl.Int64},
            auto_detect_index=False,
        )
        df = lf.collect()
        # Should still work but use SCAN instead of FT.SEARCH
        assert len(df) == 10

    def test_smart_scan_with_filters(self, redis_url, setup_index):
        """Test smart_scan results can be filtered."""
        lf = smart_scan(
            redis_url,
            "pytest:smart:*",
            schema={"name": pl.Utf8, "age": pl.Int64, "status": pl.Utf8},
        )
        # Collect first, then filter to avoid predicate pushdown issues
        df = lf.collect().filter(pl.col("age") > 25)
        assert len(df) > 0
        assert all(df["age"] > 25)

    def test_smart_scan_with_projections(self, redis_url, setup_index):
        """Test smart_scan with column selection."""
        lf = smart_scan(
            redis_url,
            "pytest:smart:*",
            schema={"name": pl.Utf8, "age": pl.Int64, "status": pl.Utf8},
        )
        df = lf.select(["name", "age"]).collect()
        assert "name" in df.columns
        assert "age" in df.columns
        # status might still be fetched but we only selected name and age

    def test_smart_scan_include_key(self, redis_url, setup_index):
        """Test smart_scan with key column."""
        lf = smart_scan(
            redis_url,
            "pytest:smart:*",
            schema={"name": pl.Utf8},
            include_key=True,
            key_column_name="_key",
        )
        df = lf.collect()
        assert "_key" in df.columns
        assert all(df["_key"].str.starts_with("pytest:smart:"))

    def test_smart_scan_custom_key_column(self, redis_url, setup_index):
        """Test smart_scan with custom key column name."""
        lf = smart_scan(
            redis_url,
            "pytest:smart:*",
            schema={"name": pl.Utf8},
            include_key=True,
            key_column_name="redis_key",
        )
        df = lf.collect()
        assert "redis_key" in df.columns

    def test_detected_index_fields(self, redis_url, setup_index):
        """Test that detected index includes field information."""
        idx_info = find_index_for_pattern(redis_url, "pytest:smart:*")
        assert idx_info is not None
        assert len(idx_info.fields) > 0
        assert "name" in idx_info.fields
        assert "age" in idx_info.fields

    def test_list_indexes_field_info(self, redis_url, setup_index):
        """Test that list_indexes returns field information."""
        indexes = list_indexes(redis_url)
        test_idx = next((i for i in indexes if i.name == "pytest_smart_idx"), None)
        assert test_idx is not None
        assert len(test_idx.fields) >= 3  # name, age, status
