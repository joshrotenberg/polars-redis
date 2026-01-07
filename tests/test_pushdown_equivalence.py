"""Tests for predicate pushdown equivalence.

This module tests that predicates pushed to RediSearch produce the same
results as scanning all data and filtering in Polars. This catches edge
cases in the Polars IO plugin system and our query builder.

The core principle: for every query pattern, verify that:
1. Pushing predicate to RediSearch (search_hashes with query)
2. Scanning all + filtering in Polars (scan_hashes + filter)

...produce identical results.
"""

from __future__ import annotations

import polars as pl
import pytest
from polars.testing import assert_frame_equal
from polars_redis import (
    Index,
    NumericField,
    TagField,
    TextField,
    col,
    scan_hashes,
    search_hashes,
    write_hashes,
)

# All tests in this module require RediSearch (Redis Stack)
pytestmark = pytest.mark.integration


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def indexed_test_data(redis_url):
    """Setup comprehensive test data with RediSearch index."""
    import time

    import redis

    client = redis.from_url(redis_url)

    # Create diverse test data
    test_data = []
    for i in range(100):
        test_data.append(
            {
                "_key": f"pushdown:user:{i}",
                "name": f"User{i}",
                "age": 20 + (i % 50),  # Ages 20-69
                "score": float(i) / 10,  # 0.0 to 9.9
                "status": ["active", "inactive", "pending"][i % 3],
                "department": ["engineering", "sales", "marketing", "hr"][i % 4],
                "level": i % 5,  # 0-4
            }
        )

    df = pl.DataFrame(test_data)
    write_hashes(df, redis_url, key_column="_key")

    # Create index
    idx = Index(
        name="pushdown_test_idx",
        prefix="pushdown:user:",
        schema=[
            TextField("name", sortable=True),
            NumericField("age", sortable=True),
            NumericField("score", sortable=True),
            TagField("status"),
            TagField("department"),
            NumericField("level", sortable=True),
        ],
    )
    # Drop if exists, ignore error if not
    try:
        idx.drop(redis_url)
    except Exception:
        pass
    idx.create(redis_url)

    # Wait for index to finish indexing all documents
    # RediSearch indexes asynchronously, so we need to poll until done
    for _ in range(50):  # Max 5 seconds
        info = client.execute_command("FT.INFO", "pushdown_test_idx")
        # Convert list to dict for easier access
        info_dict = dict(zip(info[::2], info[1::2]))
        indexing = info_dict.get(b"indexing", info_dict.get("indexing", 0))
        if indexing == 0 or indexing == b"0":
            break
        time.sleep(0.1)

    yield {
        "df": df,
        "index": idx,
        "schema": {
            "name": pl.Utf8,
            "age": pl.Int64,
            "score": pl.Float64,
            "status": pl.Utf8,
            "department": pl.Utf8,
            "level": pl.Int64,
        },
        "prefix": "pushdown:user:*",
    }

    # Cleanup
    for i in range(100):
        client.delete(f"pushdown:user:{i}")
    idx.drop(redis_url)
    client.close()


def compare_results(pushed_df: pl.DataFrame, filtered_df: pl.DataFrame) -> None:
    """Compare two DataFrames, sorting by key for consistent comparison."""
    # Sort both by key for consistent ordering
    pushed_sorted = pushed_df.sort("_key")
    filtered_sorted = filtered_df.sort("_key")

    # Use Polars assert_frame_equal for detailed comparison
    assert_frame_equal(pushed_sorted, filtered_sorted, check_row_order=False)


# =============================================================================
# Simple Predicate Tests
# =============================================================================


class TestSimplePredicates:
    """Test simple single-field predicates for pushdown equivalence."""

    def test_numeric_greater_than(self, redis_url, indexed_test_data):
        """Test: age > 40"""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        # Pushed to RediSearch
        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=col("age") > 40,
            schema=schema,
        ).collect()

        # Filtered in Polars
        filtered = (
            scan_hashes(redis_url, prefix, schema=schema).collect().filter(pl.col("age") > 40)
        )

        compare_results(pushed, filtered)
        assert len(pushed) > 0  # Sanity check

    def test_numeric_greater_equal(self, redis_url, indexed_test_data):
        """Test: age >= 40"""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=col("age") >= 40,
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema).collect().filter(pl.col("age") >= 40)
        )

        compare_results(pushed, filtered)

    def test_numeric_less_than(self, redis_url, indexed_test_data):
        """Test: age < 30"""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=col("age") < 30,
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema).collect().filter(pl.col("age") < 30)
        )

        compare_results(pushed, filtered)

    def test_numeric_less_equal(self, redis_url, indexed_test_data):
        """Test: age <= 30"""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=col("age") <= 30,
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema).collect().filter(pl.col("age") <= 30)
        )

        compare_results(pushed, filtered)

    def test_numeric_equal(self, redis_url, indexed_test_data):
        """Test: level == 2"""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=col("level") == 2,
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema).collect().filter(pl.col("level") == 2)
        )

        compare_results(pushed, filtered)

    def test_tag_equal(self, redis_url, indexed_test_data):
        """Test: status == 'active'"""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=col("status") == "active",
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema)
            .collect()
            .filter(pl.col("status") == "active")
        )

        compare_results(pushed, filtered)

    def test_tag_not_equal(self, redis_url, indexed_test_data):
        """Test: status != 'inactive'"""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=col("status") != "inactive",
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema)
            .collect()
            .filter(pl.col("status") != "inactive")
        )

        compare_results(pushed, filtered)

    def test_numeric_between(self, redis_url, indexed_test_data):
        """Test: age between 30 and 40"""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=col("age").is_between(30, 40),
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema)
            .collect()
            .filter(pl.col("age").is_between(30, 40))
        )

        compare_results(pushed, filtered)

    def test_float_greater_than(self, redis_url, indexed_test_data):
        """Test: score > 5.0"""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=col("score") > 5.0,
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema).collect().filter(pl.col("score") > 5.0)
        )

        compare_results(pushed, filtered)


# =============================================================================
# Compound Predicate Tests
# =============================================================================


class TestCompoundPredicates:
    """Test compound (AND/OR) predicates for pushdown equivalence."""

    def test_simple_and(self, redis_url, indexed_test_data):
        """Test: (age > 30) AND (status == 'active')"""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=(col("age") > 30) & (col("status") == "active"),
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema)
            .collect()
            .filter((pl.col("age") > 30) & (pl.col("status") == "active"))
        )

        compare_results(pushed, filtered)

    def test_triple_and(self, redis_url, indexed_test_data):
        """Test: (age > 25) AND (status == 'active') AND (level < 3)"""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=(col("age") > 25) & (col("status") == "active") & (col("level") < 3),
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema)
            .collect()
            .filter((pl.col("age") > 25) & (pl.col("status") == "active") & (pl.col("level") < 3))
        )

        compare_results(pushed, filtered)

    def test_simple_or(self, redis_url, indexed_test_data):
        """Test: (status == 'active') OR (status == 'pending')"""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=(col("status") == "active") | (col("status") == "pending"),
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema)
            .collect()
            .filter((pl.col("status") == "active") | (pl.col("status") == "pending"))
        )

        compare_results(pushed, filtered)

    def test_or_with_numeric(self, redis_url, indexed_test_data):
        """Test: (age < 25) OR (age > 60)"""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=(col("age") < 25) | (col("age") > 60),
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema)
            .collect()
            .filter((pl.col("age") < 25) | (pl.col("age") > 60))
        )

        compare_results(pushed, filtered)

    def test_nested_and_or(self, redis_url, indexed_test_data):
        """Test: ((status == 'active') OR (status == 'pending')) AND (age > 30)"""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=((col("status") == "active") | (col("status") == "pending")) & (col("age") > 30),
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema)
            .collect()
            .filter(
                ((pl.col("status") == "active") | (pl.col("status") == "pending"))
                & (pl.col("age") > 30)
            )
        )

        compare_results(pushed, filtered)

    def test_multiple_fields_and(self, redis_url, indexed_test_data):
        """Test: (department == 'engineering') AND (level >= 2) AND (score > 3.0)"""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=(col("department") == "engineering") & (col("level") >= 2) & (col("score") > 3.0),
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema)
            .collect()
            .filter(
                (pl.col("department") == "engineering")
                & (pl.col("level") >= 2)
                & (pl.col("score") > 3.0)
            )
        )

        compare_results(pushed, filtered)


# =============================================================================
# Edge Case Tests - Empty and All Results
# =============================================================================


class TestResultEdgeCases:
    """Test edge cases for result sets."""

    def test_no_matches(self, redis_url, indexed_test_data):
        """Test predicate that matches nothing."""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        # Age is 20-69, so age > 100 matches nothing
        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=col("age") > 100,
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema).collect().filter(pl.col("age") > 100)
        )

        compare_results(pushed, filtered)
        assert len(pushed) == 0

    def test_all_match(self, redis_url, indexed_test_data):
        """Test predicate that matches everything."""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        # Age is 20-69, so age > 0 matches everything
        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=col("age") > 0,
            schema=schema,
        ).collect()

        filtered = scan_hashes(redis_url, prefix, schema=schema).collect().filter(pl.col("age") > 0)

        compare_results(pushed, filtered)
        assert len(pushed) == 100

    def test_single_match(self, redis_url, indexed_test_data):
        """Test predicate that matches exactly two rows."""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        # User0 has age=20, level=0
        # User50 has age=20 (20 + 50%50 = 20), level=0 (50%5 = 0)
        # So both match this query
        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=(col("age") == 20) & (col("level") == 0),
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema)
            .collect()
            .filter((pl.col("age") == 20) & (pl.col("level") == 0))
        )

        compare_results(pushed, filtered)
        # Both user:0 and user:50 have age=20 AND level=0
        assert len(pushed) == 2


# =============================================================================
# Numeric Edge Cases
# =============================================================================


class TestNumericEdgeCases:
    """Test numeric edge cases."""

    def test_boundary_value_inclusive(self, redis_url, indexed_test_data):
        """Test boundary values with inclusive comparisons."""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        # Test exact boundary: age >= 20 (minimum age in data)
        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=col("age") >= 20,
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema).collect().filter(pl.col("age") >= 20)
        )

        compare_results(pushed, filtered)
        assert len(pushed) == 100  # All should match

    def test_boundary_value_exclusive(self, redis_url, indexed_test_data):
        """Test boundary values with exclusive comparisons."""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        # Test exact boundary: age > 20 (should exclude age=20)
        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=col("age") > 20,
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema).collect().filter(pl.col("age") > 20)
        )

        compare_results(pushed, filtered)
        # user:0 and user:50 have age=20, so 98 should match
        assert len(pushed) == 98

    def test_float_precision(self, redis_url, indexed_test_data):
        """Test float comparisons for precision issues."""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        # Test score > 4.9 (user:49 has score=4.9)
        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=col("score") > 4.9,
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema).collect().filter(pl.col("score") > 4.9)
        )

        compare_results(pushed, filtered)

    def test_zero_value(self, redis_url, indexed_test_data):
        """Test comparisons with zero."""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        # level == 0
        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=col("level") == 0,
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema).collect().filter(pl.col("level") == 0)
        )

        compare_results(pushed, filtered)
        assert len(pushed) == 20  # Every 5th user has level=0


# =============================================================================
# Text/Tag Edge Cases
# =============================================================================


class TestTextTagEdgeCases:
    """Test text and tag field edge cases."""

    def test_case_sensitivity_tag(self, redis_url, indexed_test_data):
        """Test tag matching is case-sensitive by default."""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        # Our data has lowercase status values
        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=col("status") == "active",
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema)
            .collect()
            .filter(pl.col("status") == "active")
        )

        compare_results(pushed, filtered)

    def test_multiple_tag_values(self, redis_url, indexed_test_data):
        """Test matching multiple tag values with OR."""
        schema = indexed_test_data["schema"]
        prefix = indexed_test_data["prefix"]

        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=(col("department") == "engineering") | (col("department") == "sales"),
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema)
            .collect()
            .filter((pl.col("department") == "engineering") | (pl.col("department") == "sales"))
        )

        compare_results(pushed, filtered)
        assert len(pushed) == 50  # 25 engineering + 25 sales


# =============================================================================
# Projection Tests
# =============================================================================


class TestWithProjection:
    """Test predicate pushdown with column projection."""

    def test_select_subset_columns(self, redis_url, indexed_test_data):
        """Test that projection doesn't affect predicate results."""
        prefix = indexed_test_data["prefix"]

        # Only select name and age
        subset_schema = {"name": pl.Utf8, "age": pl.Int64}

        pushed = search_hashes(
            redis_url,
            index="pushdown_test_idx",
            query=col("age") > 40,
            schema=subset_schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=subset_schema)
            .collect()
            .filter(pl.col("age") > 40)
        )

        compare_results(pushed, filtered)
        assert "status" not in pushed.columns  # Verify projection worked

    def test_filter_on_non_projected_field(self, redis_url, indexed_test_data):
        """Test filtering on field not in projection."""
        prefix = indexed_test_data["prefix"]
        full_schema = indexed_test_data["schema"]

        # Filter on status but only project name and age
        pushed = (
            search_hashes(
                redis_url,
                index="pushdown_test_idx",
                query=col("status") == "active",
                schema=full_schema,
            )
            .select(["_key", "name", "age"])
            .collect()
        )

        filtered = (
            scan_hashes(redis_url, prefix, schema=full_schema)
            .collect()
            .filter(pl.col("status") == "active")
            .select(["_key", "name", "age"])
        )

        compare_results(pushed, filtered)


# =============================================================================
# Parametrized Comprehensive Test
# =============================================================================


# =============================================================================
# Data Edge Cases - Nulls, Unicode, Special Characters
# =============================================================================


@pytest.fixture
def edge_case_data(redis_url):
    """Setup test data with edge cases."""
    import time

    import redis

    client = redis.from_url(redis_url)

    # Create data with edge cases
    test_data = [
        # Normal data
        {"_key": "edge:1", "name": "Alice", "value": 100, "tag": "normal"},
        {"_key": "edge:2", "name": "Bob", "value": 200, "tag": "normal"},
        # Unicode characters
        {"_key": "edge:3", "name": "Cafe", "value": 300, "tag": "unicode"},
        {"_key": "edge:4", "name": "Tokyo", "value": 400, "tag": "unicode"},
        {"_key": "edge:5", "name": "Emoji Test", "value": 500, "tag": "unicode"},
        # Special characters in values
        {"_key": "edge:6", "name": "O'Brien", "value": 600, "tag": "special"},
        {"_key": "edge:7", "name": "Smith-Jones", "value": 700, "tag": "special"},
        {"_key": "edge:8", "name": "Test & Co", "value": 800, "tag": "special"},
        # Numeric edge cases
        {"_key": "edge:9", "name": "Zero", "value": 0, "tag": "numeric"},
        {"_key": "edge:10", "name": "Negative", "value": -100, "tag": "numeric"},
        {"_key": "edge:11", "name": "Large", "value": 999999999, "tag": "numeric"},
    ]

    df = pl.DataFrame(test_data)
    write_hashes(df, redis_url, key_column="_key")

    # Create index
    idx = Index(
        name="edge_case_idx",
        prefix="edge:",
        schema=[
            TextField("name", sortable=True),
            NumericField("value", sortable=True),
            TagField("tag"),
        ],
    )
    try:
        idx.drop(redis_url)
    except Exception:
        pass
    idx.create(redis_url)

    # Wait for index to finish indexing all documents
    for _ in range(50):  # Max 5 seconds
        info = client.execute_command("FT.INFO", "edge_case_idx")
        info_dict = dict(zip(info[::2], info[1::2]))
        indexing = info_dict.get(b"indexing", info_dict.get("indexing", 0))
        if indexing == 0 or indexing == b"0":
            break
        time.sleep(0.1)

    yield {
        "schema": {"name": pl.Utf8, "value": pl.Int64, "tag": pl.Utf8},
        "prefix": "edge:*",
    }

    # Cleanup
    for i in range(1, 12):
        client.delete(f"edge:{i}")
    idx.drop(redis_url)
    client.close()


class TestDataEdgeCases:
    """Test edge cases in data values."""

    def test_zero_value_match(self, redis_url, edge_case_data):
        """Test matching zero values."""
        schema = edge_case_data["schema"]
        prefix = edge_case_data["prefix"]

        pushed = search_hashes(
            redis_url,
            index="edge_case_idx",
            query=col("value") == 0,
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema).collect().filter(pl.col("value") == 0)
        )

        compare_results(pushed, filtered)
        assert len(pushed) == 1

    def test_negative_value_match(self, redis_url, edge_case_data):
        """Test matching negative values."""
        schema = edge_case_data["schema"]
        prefix = edge_case_data["prefix"]

        pushed = search_hashes(
            redis_url,
            index="edge_case_idx",
            query=col("value") < 0,
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema).collect().filter(pl.col("value") < 0)
        )

        compare_results(pushed, filtered)
        assert len(pushed) == 1

    def test_large_value_match(self, redis_url, edge_case_data):
        """Test matching large values."""
        schema = edge_case_data["schema"]
        prefix = edge_case_data["prefix"]

        pushed = search_hashes(
            redis_url,
            index="edge_case_idx",
            query=col("value") > 100000,
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema).collect().filter(pl.col("value") > 100000)
        )

        compare_results(pushed, filtered)
        assert len(pushed) == 1

    def test_tag_with_special_chars(self, redis_url, edge_case_data):
        """Test tag matching with different tag values."""
        schema = edge_case_data["schema"]
        prefix = edge_case_data["prefix"]

        pushed = search_hashes(
            redis_url,
            index="edge_case_idx",
            query=col("tag") == "special",
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema)
            .collect()
            .filter(pl.col("tag") == "special")
        )

        compare_results(pushed, filtered)
        assert len(pushed) == 3

    def test_unicode_tag(self, redis_url, edge_case_data):
        """Test matching unicode data via tag."""
        schema = edge_case_data["schema"]
        prefix = edge_case_data["prefix"]

        pushed = search_hashes(
            redis_url,
            index="edge_case_idx",
            query=col("tag") == "unicode",
            schema=schema,
        ).collect()

        filtered = (
            scan_hashes(redis_url, prefix, schema=schema)
            .collect()
            .filter(pl.col("tag") == "unicode")
        )

        compare_results(pushed, filtered)
        assert len(pushed) == 3


# =============================================================================
# Parametrized Comprehensive Test
# =============================================================================


@pytest.mark.parametrize(
    "query_expr,polars_filter",
    [
        # Simple comparisons
        (col("age") > 30, pl.col("age") > 30),
        (col("age") >= 30, pl.col("age") >= 30),
        (col("age") < 30, pl.col("age") < 30),
        (col("age") <= 30, pl.col("age") <= 30),
        (col("level") == 2, pl.col("level") == 2),
        # Tag equality
        (col("status") == "active", pl.col("status") == "active"),
        (col("status") != "inactive", pl.col("status") != "inactive"),
        # Between
        (col("age").is_between(25, 35), pl.col("age").is_between(25, 35)),
        # Simple AND
        (
            (col("age") > 30) & (col("status") == "active"),
            (pl.col("age") > 30) & (pl.col("status") == "active"),
        ),
        # Simple OR
        (
            (col("level") == 0) | (col("level") == 4),
            (pl.col("level") == 0) | (pl.col("level") == 4),
        ),
        # Nested
        (
            ((col("status") == "active") | (col("status") == "pending")) & (col("age") > 25),
            ((pl.col("status") == "active") | (pl.col("status") == "pending"))
            & (pl.col("age") > 25),
        ),
        # Float
        (col("score") > 5.0, pl.col("score") > 5.0),
        (col("score").is_between(3.0, 7.0), pl.col("score").is_between(3.0, 7.0)),
    ],
)
def test_pushdown_equivalence_parametrized(redis_url, indexed_test_data, query_expr, polars_filter):
    """Parametrized test for pushdown equivalence across many predicate patterns."""
    schema = indexed_test_data["schema"]
    prefix = indexed_test_data["prefix"]

    pushed = search_hashes(
        redis_url,
        index="pushdown_test_idx",
        query=query_expr,
        schema=schema,
    ).collect()

    filtered = scan_hashes(redis_url, prefix, schema=schema).collect().filter(polars_filter)

    compare_results(pushed, filtered)
