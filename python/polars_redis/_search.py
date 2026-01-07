"""RediSearch functions for polars-redis.

This module contains functions for searching and aggregating Redis data
using the RediSearch module's FT.SEARCH and FT.AGGREGATE commands.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING

import polars as pl
from polars.io.plugins import register_io_source

from polars_redis._utils import _polars_dtype_to_internal
from polars_redis.options import SearchOptions

# RediSearch support (optional - requires search feature)
try:
    from polars_redis._internal import PyHashSearchIterator, py_aggregate

    _HAS_SEARCH = True
except ImportError:
    _HAS_SEARCH = False
    PyHashSearchIterator = None  # type: ignore[misc, assignment]
    py_aggregate = None  # type: ignore[misc, assignment]

if TYPE_CHECKING:
    from polars import DataFrame, Expr
    from polars.type_aliases import SchemaDict

    from polars_redis._index import Index

from polars_redis.query import Expr as QueryExpr


def search_hashes(
    url: str,
    index: str | Index = "",
    query: str | QueryExpr = "*",
    schema: dict | None = None,
    *,
    options: SearchOptions | None = None,
    include_key: bool = True,
    key_column_name: str = "_key",
    include_ttl: bool = False,
    ttl_column_name: str = "_ttl",
    include_row_index: bool = False,
    row_index_column_name: str = "_index",
    batch_size: int = 1000,
    sort_by: str | None = None,
    sort_ascending: bool = True,
    create_index: bool = True,
) -> pl.LazyFrame:
    """Search Redis hashes using RediSearch and return a LazyFrame.

    This function uses RediSearch's FT.SEARCH command to perform server-side
    filtering (predicate pushdown), which is much more efficient than scanning
    all keys and filtering client-side.

    Requires:
        - RediSearch module installed on Redis server
        - An existing RediSearch index on the hash data (or pass an Index object)

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        index: RediSearch index name (e.g., "users_idx") or an Index object.
            When an Index object is passed, the index will be auto-created
            if it doesn't exist (controlled by create_index parameter).
        query: RediSearch query - either a string (e.g., "@age:[30 +inf]")
            or a query expression built with col() (e.g., col("age") > 30).
        schema: Dictionary mapping field names to Polars dtypes.
        options: SearchOptions object for configuration. If provided,
            individual keyword arguments are ignored.
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column (default: "_key").
        include_ttl: Whether to include the TTL as a column.
        ttl_column_name: Name of the TTL column (default: "_ttl").
        include_row_index: Whether to include the row index as a column.
        row_index_column_name: Name of the row index column (default: "_index").
        batch_size: Number of documents to fetch per batch.
        sort_by: Optional field name to sort results by.
        sort_ascending: Sort direction (default: True for ascending).
        create_index: When index is an Index object, automatically create
            the index if it doesn't exist (default: True).

    Returns:
        A Polars LazyFrame that will search Redis when collected.

    Example:
        >>> # Search with raw RediSearch query
        >>> lf = search_hashes(
        ...     "redis://localhost:6379",
        ...     index="users_idx",
        ...     query="@age:[30 +inf]",
        ...     schema={"name": pl.Utf8, "age": pl.Int64}
        ... )
        >>> df = lf.collect()

        >>> # Search with Polars-like syntax (predicate pushdown)
        >>> from polars_redis import col
        >>> lf = search_hashes(
        ...     "redis://localhost:6379",
        ...     index="users_idx",
        ...     query=(col("age") > 30) & (col("status") == "active"),
        ...     schema={"name": pl.Utf8, "age": pl.Int64, "status": pl.Utf8}
        ... )
        >>> df = lf.collect()

        >>> # Search with Index object (auto-creates index)
        >>> from polars_redis import Index, TextField, NumericField, TagField
        >>> idx = Index(
        ...     name="users_idx",
        ...     prefix="user:",
        ...     schema=[
        ...         TextField("name", sortable=True),
        ...         NumericField("age", sortable=True),
        ...         TagField("status"),
        ...     ]
        ... )
        >>> lf = search_hashes(
        ...     "redis://localhost:6379",
        ...     index=idx,  # Auto-creates if not exists
        ...     query=col("age") > 30,
        ...     schema={"name": pl.Utf8, "age": pl.Int64, "status": pl.Utf8}
        ... )
        >>> df = lf.collect()

        >>> # Search with sorting
        >>> lf = search_hashes(
        ...     "redis://localhost:6379",
        ...     index="users_idx",
        ...     query=col("status") == "active",
        ...     schema={"name": pl.Utf8, "score": pl.Float64},
        ...     sort_by="score",
        ...     sort_ascending=False
        ... )
        >>> top_users = lf.head(10).collect()

        >>> # Using options object
        >>> opts = SearchOptions(
        ...     index="users_idx",
        ...     query="@age:[30 +inf]",
        ...     sort_by="name",
        ...     sort_ascending=True,
        ... )
        >>> lf = search_hashes(
        ...     "redis://localhost:6379",
        ...     schema={"name": pl.Utf8, "age": pl.Int64},
        ...     options=opts,
        ... )
        >>> df = lf.collect()

    Raises:
        RuntimeError: If RediSearch support is not available.
    """
    # Import here to avoid circular import
    from polars_redis._index import Index as IndexClass

    # If options object provided, use its values
    if options is not None:
        index = options.index
        query = options.query
        include_key = options.include_key
        key_column_name = options.key_column_name
        include_ttl = options.include_ttl
        ttl_column_name = options.ttl_column_name
        include_row_index = options.include_row_index
        row_index_column_name = options.row_index_column_name
        batch_size = options.batch_size
        sort_by = options.sort_by
        sort_ascending = options.sort_ascending

    # Handle Index object
    index_name: str
    if isinstance(index, IndexClass):
        if create_index:
            index.ensure_exists(url)
        index_name = index.name
    else:
        index_name = index

    if not _HAS_SEARCH:
        raise RuntimeError(
            "RediSearch support is not available. "
            "Ensure the 'search' feature is enabled when building polars-redis."
        )

    if schema is None:
        raise ValueError("schema is required for search_hashes")

    # Convert Expr query to string if needed
    if isinstance(query, QueryExpr):
        query = query.to_redis()

    # Convert schema to internal format: list of (name, type_str) tuples
    internal_schema = [(name, _polars_dtype_to_internal(dtype)) for name, dtype in schema.items()]

    # Build the full Polars schema (for register_io_source)
    polars_schema: SchemaDict = {}
    if include_row_index:
        polars_schema[row_index_column_name] = pl.UInt64
    if include_key:
        polars_schema[key_column_name] = pl.Utf8
    if include_ttl:
        polars_schema[ttl_column_name] = pl.Int64
    for name, dtype in schema.items():
        polars_schema[name] = dtype

    def _search_source(
        with_columns: list[str] | None,
        predicate: Expr | None,
        n_rows: int | None,
        batch_size_hint: int | None,
    ) -> Iterator[DataFrame]:
        """Generator that yields DataFrames from RediSearch results."""
        # Determine projection
        projection = None
        if with_columns is not None:
            projection = [
                c
                for c in with_columns
                if c in schema
                or c == key_column_name
                or c == ttl_column_name
                or c == row_index_column_name
            ]

        # Use batch_size_hint if provided, otherwise use configured batch_size
        effective_batch_size = batch_size_hint if batch_size_hint is not None else batch_size

        # Create the iterator
        iterator = PyHashSearchIterator(
            url=url,
            index=index_name,
            query=query,
            schema=internal_schema,
            batch_size=effective_batch_size,
            projection=projection,
            include_key=include_key,
            key_column_name=key_column_name,
            include_ttl=include_ttl,
            ttl_column_name=ttl_column_name,
            include_row_index=include_row_index,
            row_index_column_name=row_index_column_name,
            max_rows=n_rows,
            sort_by=sort_by,
            sort_ascending=sort_ascending,
        )

        # Yield batches
        while not iterator.is_done():
            ipc_bytes = iterator.next_batch_ipc()
            if ipc_bytes is None:
                break

            df = pl.read_ipc(ipc_bytes)

            # Apply additional predicate filter if provided
            # Note: The query parameter already does server-side filtering,
            # but additional predicates can be applied client-side
            if predicate is not None:
                df = df.filter(predicate)

            # Apply column selection if needed
            if with_columns is not None:
                available = [c for c in with_columns if c in df.columns]
                if available:
                    df = df.select(available)

            # Don't yield empty batches
            if len(df) > 0:
                yield df

    return register_io_source(
        io_source=_search_source,
        schema=polars_schema,
    )


def search_json(
    url: str,
    index: str | Index = "",
    query: str | QueryExpr = "*",
    schema: dict | None = None,
    *,
    options: SearchOptions | None = None,
    include_key: bool = True,
    key_column_name: str = "_key",
    include_ttl: bool = False,
    ttl_column_name: str = "_ttl",
    include_row_index: bool = False,
    row_index_column_name: str = "_index",
    batch_size: int = 1000,
    sort_by: str | None = None,
    sort_ascending: bool = True,
    create_index: bool = True,
) -> pl.LazyFrame:
    """Search RedisJSON documents using RediSearch and return a LazyFrame.

    This function uses RediSearch's FT.SEARCH command on JSON indexes to perform
    server-side filtering (predicate pushdown), which is much more efficient than
    scanning all keys and filtering client-side.

    Requires:
        - RediSearch module installed on Redis server
        - RedisJSON module installed on Redis server
        - An existing RediSearch index created with ON JSON (or pass an Index object)

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        index: RediSearch index name (e.g., "products_idx") or an Index object.
            When an Index object is passed, the index will be auto-created
            if it doesn't exist (controlled by create_index parameter).
        query: RediSearch query - either a string (e.g., "@price:[0 100]")
            or a query expression built with col() (e.g., col("price") < 100).
        schema: Dictionary mapping field names to Polars dtypes.
            Field names should match the AS aliases defined in the index.
        options: SearchOptions object for configuration. If provided,
            individual keyword arguments are ignored.
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column (default: "_key").
        include_ttl: Whether to include the TTL as a column.
        ttl_column_name: Name of the TTL column (default: "_ttl").
        include_row_index: Whether to include the row index as a column.
        row_index_column_name: Name of the row index column (default: "_index").
        batch_size: Number of documents to fetch per batch.
        sort_by: Optional field name to sort results by.
        sort_ascending: Sort direction (default: True for ascending).
        create_index: When index is an Index object, automatically create
            the index if it doesn't exist (default: True).

    Returns:
        A Polars LazyFrame that will search Redis when collected.

    Example:
        >>> # Given a JSON index created like:
        >>> # FT.CREATE products_idx ON JSON PREFIX 1 product:
        >>> #   SCHEMA $.name AS name TEXT $.price AS price NUMERIC
        >>>
        >>> # Search with raw RediSearch query
        >>> lf = search_json(
        ...     "redis://localhost:6379",
        ...     index="products_idx",
        ...     query="@price:[0 100]",
        ...     schema={"name": pl.Utf8, "price": pl.Float64}
        ... )
        >>> df = lf.collect()

        >>> # Search with Polars-like syntax (predicate pushdown)
        >>> from polars_redis import col
        >>> lf = search_json(
        ...     "redis://localhost:6379",
        ...     index="products_idx",
        ...     query=(col("price") < 100) & (col("category") == "electronics"),
        ...     schema={"name": pl.Utf8, "price": pl.Float64, "category": pl.Utf8}
        ... )
        >>> df = lf.collect()

        >>> # Search with sorting
        >>> lf = search_json(
        ...     "redis://localhost:6379",
        ...     index="products_idx",
        ...     query="*",
        ...     schema={"name": pl.Utf8, "price": pl.Float64},
        ...     sort_by="price",
        ...     sort_ascending=True
        ... )
        >>> cheapest = lf.head(10).collect()

    Raises:
        RuntimeError: If RediSearch support is not available.

    Note:
        The schema field names should match the AS aliases defined when creating
        the index, not the JSON paths. For example, if your index was created with
        `$.product_name AS name`, use "name" in the schema.
    """
    # Import here to avoid circular import
    from polars_redis._index import Index as IndexClass

    # If options object provided, use its values
    if options is not None:
        index = options.index
        query = options.query
        include_key = options.include_key
        key_column_name = options.key_column_name
        include_ttl = options.include_ttl
        ttl_column_name = options.ttl_column_name
        include_row_index = options.include_row_index
        row_index_column_name = options.row_index_column_name
        batch_size = options.batch_size
        sort_by = options.sort_by
        sort_ascending = options.sort_ascending

    # Handle Index object
    index_name: str
    if isinstance(index, IndexClass):
        if create_index:
            index.ensure_exists(url)
        index_name = index.name
    else:
        index_name = index

    if not _HAS_SEARCH:
        raise RuntimeError(
            "RediSearch support is not available. "
            "Ensure the 'search' feature is enabled when building polars-redis."
        )

    if schema is None:
        raise ValueError("schema is required for search_json")

    # Convert Expr query to string if needed
    if isinstance(query, QueryExpr):
        query = query.to_redis()

    # Convert schema to internal format: list of (name, type_str) tuples
    internal_schema = [(name, _polars_dtype_to_internal(dtype)) for name, dtype in schema.items()]

    # Build the full Polars schema (for register_io_source)
    polars_schema: SchemaDict = {}
    if include_row_index:
        polars_schema[row_index_column_name] = pl.UInt64
    if include_key:
        polars_schema[key_column_name] = pl.Utf8
    if include_ttl:
        polars_schema[ttl_column_name] = pl.Int64
    for name, dtype in schema.items():
        polars_schema[name] = dtype

    # For JSON indexes, we MUST always use RETURN with explicit field names.
    # Without RETURN, RediSearch returns the entire JSON blob under "$" key,
    # but with RETURN <field_names>, it returns flat field-value pairs that
    # match the hash iterator's expected format.
    schema_fields = list(schema.keys())

    def _search_json_source(
        with_columns: list[str] | None,
        predicate: Expr | None,
        n_rows: int | None,
        batch_size_hint: int | None,
    ) -> Iterator[DataFrame]:
        """Generator that yields DataFrames from RediSearch JSON results."""
        # For JSON, always include schema fields in projection to get flat response
        # Start with schema fields (required for RETURN clause)
        projection = list(schema_fields)

        # Add special columns to projection so they're included in the output
        # (the iterator uses projection to filter output columns via schema.project())
        if include_key:
            projection.append(key_column_name)
        if include_ttl:
            projection.append(ttl_column_name)
        if include_row_index:
            projection.append(row_index_column_name)

        # If with_columns is specified, filter to requested columns
        # but always keep schema fields for the RETURN clause
        if with_columns is not None:
            # Filter projection to only requested columns, but schema fields
            # are still sent to Redis for RETURN (handled by Rust side)
            requested = set(with_columns)
            projection = [c for c in projection if c in requested or c in schema]

        # Use batch_size_hint if provided, otherwise use configured batch_size
        effective_batch_size = batch_size_hint if batch_size_hint is not None else batch_size

        # Create the iterator
        iterator = PyHashSearchIterator(
            url=url,
            index=index_name,
            query=query,
            schema=internal_schema,
            batch_size=effective_batch_size,
            projection=projection,
            include_key=include_key,
            key_column_name=key_column_name,
            include_ttl=include_ttl,
            ttl_column_name=ttl_column_name,
            include_row_index=include_row_index,
            row_index_column_name=row_index_column_name,
            max_rows=n_rows,
            sort_by=sort_by,
            sort_ascending=sort_ascending,
        )

        # Yield batches
        while not iterator.is_done():
            ipc_bytes = iterator.next_batch_ipc()
            if ipc_bytes is None:
                break

            df = pl.read_ipc(ipc_bytes)

            # Apply additional predicate filter if provided
            if predicate is not None:
                df = df.filter(predicate)

            # Apply column selection if needed
            if with_columns is not None:
                available = [c for c in with_columns if c in df.columns]
                if available:
                    df = df.select(available)

            # Don't yield empty batches
            if len(df) > 0:
                yield df

    return register_io_source(
        io_source=_search_json_source,
        schema=polars_schema,
    )


def aggregate_hashes(
    url: str,
    index: str,
    query: str = "*",
    *,
    group_by: list[str] | None = None,
    reduce: list[tuple[str, list[str], str]] | None = None,
    apply: list[tuple[str, str]] | None = None,
    filter_expr: str | None = None,
    sort_by: list[tuple[str, bool]] | None = None,
    limit: int | None = None,
    offset: int = 0,
    load: list[str] | None = None,
) -> pl.DataFrame:
    """Execute a RediSearch FT.AGGREGATE query and return results as a DataFrame.

    FT.AGGREGATE performs aggregations on indexed data, supporting grouping,
    reduce functions, computed fields, filtering, and sorting - all server-side.

    Requires:
        - RediSearch module installed on Redis server
        - An existing RediSearch index on the hash data

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        index: RediSearch index name (e.g., "users_idx").
        query: RediSearch query string (e.g., "@status:active", "*" for all).
        group_by: Fields to group by (e.g., ["@department", "@role"]).
        reduce: Reduce operations as (function, args, alias) tuples.
            Examples:
            - ("COUNT", [], "total")
            - ("SUM", ["@salary"], "total_salary")
            - ("AVG", ["@age"], "avg_age")
            - ("MIN", ["@score"], "min_score")
            - ("MAX", ["@score"], "max_score")
            - ("FIRST_VALUE", ["@name"], "first_name")
            - ("TOLIST", ["@name"], "names")
            - ("QUANTILE", ["@salary", "0.5"], "median_salary")
            - ("STDDEV", ["@score"], "score_stddev")
        apply: Computed expressions as (expression, alias) tuples.
            Example: ("@total_salary / @total", "avg_salary")
        filter_expr: Filter expression after aggregation.
            Example: "@total > 10"
        sort_by: Sort specifications as (field, ascending) tuples.
            Example: [("@total", False)] for descending by total.
        limit: Maximum number of results to return.
        offset: Number of results to skip (for pagination).
        load: Fields to load from the source documents (before aggregation).

    Returns:
        A Polars DataFrame containing the aggregation results.

    Example:
        >>> # Count users by department
        >>> df = aggregate_hashes(
        ...     "redis://localhost:6379",
        ...     index="users_idx",
        ...     query="*",
        ...     group_by=["@department"],
        ...     reduce=[("COUNT", [], "count")],
        ... )
        >>> print(df)

        >>> # Average salary by department, sorted by average
        >>> df = aggregate_hashes(
        ...     "redis://localhost:6379",
        ...     index="users_idx",
        ...     query="@status:active",
        ...     group_by=["@department"],
        ...     reduce=[
        ...         ("COUNT", [], "employee_count"),
        ...         ("AVG", ["@salary"], "avg_salary"),
        ...     ],
        ...     sort_by=[("@avg_salary", False)],
        ...     limit=10,
        ... )

        >>> # Calculate computed fields with APPLY
        >>> df = aggregate_hashes(
        ...     "redis://localhost:6379",
        ...     index="orders_idx",
        ...     query="*",
        ...     group_by=["@customer_id"],
        ...     reduce=[
        ...         ("SUM", ["@amount"], "total"),
        ...         ("COUNT", [], "order_count"),
        ...     ],
        ...     apply=[("@total / @order_count", "avg_order")],
        ...     filter_expr="@order_count > 5",
        ... )

    Raises:
        RuntimeError: If RediSearch support is not available.
    """
    if not _HAS_SEARCH:
        raise RuntimeError(
            "RediSearch support is not available. "
            "Ensure the 'search' feature is enabled when building polars-redis."
        )

    # Call the Rust implementation
    results = py_aggregate(
        url=url,
        index=index,
        query=query,
        group_by=group_by or [],
        reduce=reduce or [],
        apply=apply,
        filter=filter_expr,
        sort_by=sort_by,
        limit=limit,
        offset=offset,
        load=load,
    )

    # Convert list of dicts to DataFrame
    if not results:
        return pl.DataFrame()

    return pl.DataFrame(results)


def aggregate_json(
    url: str,
    index: str,
    query: str = "*",
    *,
    group_by: list[str] | None = None,
    reduce: list[tuple[str, list[str], str]] | None = None,
    apply: list[tuple[str, str]] | None = None,
    filter_expr: str | None = None,
    sort_by: list[tuple[str, bool]] | None = None,
    limit: int | None = None,
    offset: int = 0,
    load: list[str] | None = None,
) -> pl.DataFrame:
    """Execute a RediSearch FT.AGGREGATE query on JSON documents.

    FT.AGGREGATE performs aggregations on indexed JSON data, supporting grouping,
    reduce functions, computed fields, filtering, and sorting - all server-side.

    Requires:
        - RediSearch module installed on Redis server
        - RedisJSON module installed on Redis server
        - An existing RediSearch index created with ON JSON

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        index: RediSearch index name (e.g., "products_idx").
        query: RediSearch query string (e.g., "@category:electronics", "*" for all).
        group_by: Fields to group by (e.g., ["@category", "@brand"]).
        reduce: Reduce operations as (function, args, alias) tuples.
            Examples:
            - ("COUNT", [], "total")
            - ("SUM", ["@price"], "total_price")
            - ("AVG", ["@rating"], "avg_rating")
            - ("MIN", ["@price"], "min_price")
            - ("MAX", ["@price"], "max_price")
        apply: Computed expressions as (expression, alias) tuples.
            Example: ("@total_price / @total", "avg_price")
        filter_expr: Filter expression after aggregation.
            Example: "@total > 10"
        sort_by: Sort specifications as (field, ascending) tuples.
            Example: [("@total", False)] for descending by total.
        limit: Maximum number of results to return.
        offset: Number of results to skip (for pagination).
        load: Fields to load from the source documents (before aggregation).

    Returns:
        A Polars DataFrame containing the aggregation results.

    Example:
        >>> # Count products by category
        >>> df = aggregate_json(
        ...     "redis://localhost:6379",
        ...     index="products_idx",
        ...     query="*",
        ...     group_by=["@category"],
        ...     reduce=[("COUNT", [], "count")],
        ... )
        >>> print(df)

        >>> # Average price by category, sorted by average
        >>> df = aggregate_json(
        ...     "redis://localhost:6379",
        ...     index="products_idx",
        ...     query="@in_stock:true",
        ...     group_by=["@category"],
        ...     reduce=[
        ...         ("COUNT", [], "product_count"),
        ...         ("AVG", ["@price"], "avg_price"),
        ...     ],
        ...     sort_by=[("@avg_price", False)],
        ...     limit=10,
        ... )

    Raises:
        RuntimeError: If RediSearch support is not available.

    Note:
        The field names in group_by, reduce, and other parameters should use
        the AS aliases defined when creating the index, prefixed with @.
    """
    # aggregate_json uses the same underlying implementation as aggregate_hashes
    # because RediSearch FT.AGGREGATE works the same way for both hash and JSON indexes
    return aggregate_hashes(
        url=url,
        index=index,
        query=query,
        group_by=group_by,
        reduce=reduce,
        apply=apply,
        filter_expr=filter_expr,
        sort_by=sort_by,
        limit=limit,
        offset=offset,
        load=load,
    )
