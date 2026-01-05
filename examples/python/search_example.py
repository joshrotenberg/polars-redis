#!/usr/bin/env python3
"""RediSearch example: Server-side filtering and aggregation.

This example demonstrates:
- Creating a RediSearch index
- Using search_hashes() for server-side filtering
- Using the query builder (col, raw)
- Using aggregate_hashes() for server-side aggregation

Prerequisites:
- Redis Stack running on localhost:6379
- pip install polars-redis redis
"""

import polars as pl
import polars_redis as pr
import redis as redis_py
from polars_redis import col, raw

URL = "redis://localhost:6379"


def setup_sample_data():
    """Create sample data and RediSearch index."""
    r = redis_py.Redis()

    # Clear existing data
    for key in r.scan_iter("employee:*"):
        r.delete(key)

    # Create sample employees
    employees = [
        {
            "name": "Alice",
            "age": "32",
            "department": "engineering",
            "salary": "120000",
            "status": "active",
        },
        {
            "name": "Bob",
            "age": "28",
            "department": "engineering",
            "salary": "95000",
            "status": "active",
        },
        {
            "name": "Carol",
            "age": "45",
            "department": "product",
            "salary": "140000",
            "status": "active",
        },
        {
            "name": "Dave",
            "age": "35",
            "department": "product",
            "salary": "110000",
            "status": "inactive",
        },
        {
            "name": "Eve",
            "age": "29",
            "department": "marketing",
            "salary": "85000",
            "status": "active",
        },
        {
            "name": "Frank",
            "age": "52",
            "department": "engineering",
            "salary": "150000",
            "status": "active",
        },
        {
            "name": "Grace",
            "age": "38",
            "department": "marketing",
            "salary": "95000",
            "status": "active",
        },
        {
            "name": "Henry",
            "age": "41",
            "department": "engineering",
            "salary": "130000",
            "status": "inactive",
        },
    ]

    for i, emp in enumerate(employees, 1):
        r.hset(f"employee:{i}", mapping=emp)

    # Drop existing index if it exists
    try:
        r.execute_command("FT.DROPINDEX", "employees_idx")
    except redis_py.ResponseError:
        pass  # Index doesn't exist

    # Create RediSearch index
    r.execute_command(
        "FT.CREATE",
        "employees_idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "employee:",
        "SCHEMA",
        "name",
        "TEXT",
        "SORTABLE",
        "age",
        "NUMERIC",
        "SORTABLE",
        "department",
        "TAG",
        "salary",
        "NUMERIC",
        "SORTABLE",
        "status",
        "TAG",
    )

    print("Created 8 employees and RediSearch index")
    return r


def example_basic_search():
    """Basic search with raw query string."""
    print("\n=== Basic Search (age > 30) ===")

    df = pr.search_hashes(
        URL,
        index="employees_idx",
        query="@age:[30 +inf]",  # RediSearch query syntax
        schema={
            "name": pl.Utf8,
            "age": pl.Int64,
            "department": pl.Utf8,
            "salary": pl.Float64,
        },
    ).collect()

    print(df)


def example_query_builder():
    """Using the Polars-like query builder."""
    print("\n=== Query Builder (age > 30 AND status == active) ===")

    # Build query with Polars-like syntax
    query = (col("age") > 30) & (col("status") == "active")

    df = pr.search_hashes(
        URL,
        index="employees_idx",
        query=query,
        schema={
            "name": pl.Utf8,
            "age": pl.Int64,
            "department": pl.Utf8,
            "status": pl.Utf8,
        },
    ).collect()

    print(df)


def example_or_conditions():
    """Combining conditions with OR."""
    print("\n=== OR Conditions (engineering OR product) ===")

    query = (col("department") == "engineering") | (col("department") == "product")

    df = pr.search_hashes(
        URL,
        index="employees_idx",
        query=query,
        schema={"name": pl.Utf8, "department": pl.Utf8, "salary": pl.Float64},
    ).collect()

    print(df)


def example_negation():
    """Using negation."""
    print("\n=== Negation (NOT inactive) ===")

    query = col("status") != "inactive"

    df = pr.search_hashes(
        URL,
        index="employees_idx",
        query=query,
        schema={"name": pl.Utf8, "status": pl.Utf8},
    ).collect()

    print(df)


def example_raw_query():
    """Using raw() for complex queries."""
    print("\n=== Raw Query (name prefix search) ===")

    # Full-text prefix search
    query = raw("@name:A*")  # Names starting with A

    df = pr.search_hashes(
        URL,
        index="employees_idx",
        query=query,
        schema={"name": pl.Utf8, "department": pl.Utf8},
    ).collect()

    print(df)


def example_sorted_search():
    """Search with sorting."""
    print("\n=== Sorted Search (by salary descending) ===")

    df = pr.search_hashes(
        URL,
        index="employees_idx",
        query="@status:{active}",
        schema={"name": pl.Utf8, "salary": pl.Float64},
        sort_by="salary",
        sort_ascending=False,
    ).collect()

    print(df)


def example_basic_aggregation():
    """Basic aggregation with GROUP BY."""
    print("\n=== Aggregation (count by department) ===")

    df = pr.aggregate_hashes(
        URL,
        index="employees_idx",
        query="*",
        group_by=["@department"],
        reduce=[("COUNT", [], "employee_count")],
    )

    print(df)


def example_multi_aggregation():
    """Multiple aggregation functions."""
    print("\n=== Multi Aggregation (salary stats by department) ===")

    df = pr.aggregate_hashes(
        URL,
        index="employees_idx",
        query="@status:{active}",  # Only active employees
        group_by=["@department"],
        reduce=[
            ("COUNT", [], "headcount"),
            ("AVG", ["@salary"], "avg_salary"),
            ("MIN", ["@salary"], "min_salary"),
            ("MAX", ["@salary"], "max_salary"),
            ("SUM", ["@salary"], "total_payroll"),
        ],
        sort_by=[("@avg_salary", False)],  # Sort by avg salary descending
    )

    print(df)


def example_computed_fields():
    """Using APPLY for computed fields."""
    print("\n=== Computed Fields (avg order value) ===")

    df = pr.aggregate_hashes(
        URL,
        index="employees_idx",
        query="*",
        group_by=["@department"],
        reduce=[
            ("SUM", ["@salary"], "total_salary"),
            ("COUNT", [], "count"),
        ],
        apply=[
            ("@total_salary / @count", "calculated_avg"),
        ],
    )

    print(df)


def example_global_aggregation():
    """Aggregation without grouping (global stats)."""
    print("\n=== Global Aggregation (company-wide stats) ===")

    df = pr.aggregate_hashes(
        URL,
        index="employees_idx",
        query="@status:{active}",
        reduce=[
            ("COUNT", [], "total_employees"),
            ("AVG", ["@salary"], "company_avg_salary"),
            ("AVG", ["@age"], "avg_age"),
        ],
    )

    print(df)


def main():
    """Run all examples."""
    print("RediSearch Example")
    print("=" * 50)

    setup_sample_data()

    # Search examples
    example_basic_search()
    example_query_builder()
    example_or_conditions()
    example_negation()
    example_raw_query()
    example_sorted_search()

    # Aggregation examples
    example_basic_aggregation()
    example_multi_aggregation()
    example_computed_fields()
    example_global_aggregation()

    print("\n" + "=" * 50)
    print("All examples completed!")


if __name__ == "__main__":
    main()
