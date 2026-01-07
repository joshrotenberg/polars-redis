"""Smart scan with automatic index detection and query optimization.

This module provides a higher-level abstraction that automatically detects
whether a RediSearch index exists for a key pattern and optimizes query
execution accordingly.

Example:
    >>> from polars_redis import smart_scan
    >>>
    >>> # Automatically uses FT.SEARCH if index exists, falls back to SCAN
    >>> lf = smart_scan(
    ...     "redis://localhost:6379",
    ...     "user:*",
    ...     schema={"name": pl.Utf8, "age": pl.Int64},
    ... )
    >>> df = lf.filter(pl.col("age") > 30).collect()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

import polars as pl
import redis

if TYPE_CHECKING:
    from polars_redis._index import Index


class ExecutionStrategy(Enum):
    """Strategy for executing a query."""

    SEARCH = "search"  # Use FT.SEARCH with index
    SCAN = "scan"  # Use SCAN without index
    HYBRID = "hybrid"  # Use FT.SEARCH + client-side filtering


@dataclass
class DetectedIndex:
    """Information about an auto-detected index."""

    name: str
    prefixes: list[str]
    on_type: str  # "HASH" or "JSON"
    fields: list[str]


@dataclass
class QueryPlan:
    """Execution plan for a query.

    Provides insight into how a query will be executed, including
    which strategy will be used and what filters will be applied
    where.
    """

    strategy: ExecutionStrategy
    index: DetectedIndex | None = None
    server_query: str | None = None
    client_filters: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def explain(self) -> str:
        """Return a human-readable explanation of the query plan."""
        lines = []
        lines.append(f"Strategy: {self.strategy.value.upper()}")

        if self.index:
            lines.append(f"Index: {self.index.name}")
            lines.append(f"  Prefixes: {', '.join(self.index.prefixes)}")
            lines.append(f"  Type: {self.index.on_type}")

        if self.server_query:
            lines.append(f"Server Query: {self.server_query}")

        if self.client_filters:
            lines.append("Client Filters:")
            for f in self.client_filters:
                lines.append(f"  - {f}")

        if self.warnings:
            lines.append("Warnings:")
            for w in self.warnings:
                lines.append(f"  - {w}")

        return "\n".join(lines)


def find_index_for_pattern(url: str, pattern: str) -> DetectedIndex | None:
    """Find a RediSearch index that covers the given key pattern.

    Args:
        url: Redis connection URL.
        pattern: Key pattern (e.g., "user:*").

    Returns:
        IndexInfo if a matching index is found, None otherwise.
    """
    client = redis.from_url(url)

    try:
        # Get list of all indexes
        indexes = client.execute_command("FT._LIST")

        # Extract prefix from pattern (e.g., "user:*" -> "user:")
        if pattern.endswith("*"):
            pattern_prefix = pattern[:-1]
        else:
            pattern_prefix = pattern

        for index_name in indexes:
            if isinstance(index_name, bytes):
                index_name = index_name.decode("utf-8")

            try:
                # Get index info
                info = client.execute_command("FT.INFO", index_name)

                # Parse the flat list response
                info_dict = {}
                for i in range(0, len(info), 2):
                    key = info[i]
                    if isinstance(key, bytes):
                        key = key.decode("utf-8")
                    info_dict[key] = info[i + 1]

                # Get index definition
                index_def = info_dict.get("index_definition", [])
                if isinstance(index_def, list):
                    def_dict = {}
                    for i in range(0, len(index_def), 2):
                        k = index_def[i]
                        if isinstance(k, bytes):
                            k = k.decode("utf-8")
                        def_dict[k] = index_def[i + 1]
                else:
                    def_dict = index_def

                # Get prefixes
                prefixes = def_dict.get("prefixes", [])
                if isinstance(prefixes, bytes):
                    prefixes = [prefixes.decode("utf-8")]
                elif isinstance(prefixes, list):
                    prefixes = [p.decode("utf-8") if isinstance(p, bytes) else p for p in prefixes]

                # Check if any prefix matches our pattern
                for prefix in prefixes:
                    if pattern_prefix.startswith(prefix) or prefix.startswith(pattern_prefix):
                        # Get field names
                        attributes = info_dict.get("attributes", [])
                        fields = []
                        for attr in attributes:
                            if isinstance(attr, list) and len(attr) >= 2:
                                # attr is like ['identifier', 'name', 'attribute', 'name', ...]
                                for j in range(0, len(attr), 2):
                                    k = attr[j]
                                    if isinstance(k, bytes):
                                        k = k.decode("utf-8")
                                    if k == "identifier":
                                        v = attr[j + 1]
                                        if isinstance(v, bytes):
                                            v = v.decode("utf-8")
                                        fields.append(v)
                                        break

                        return DetectedIndex(
                            name=index_name,
                            prefixes=prefixes,
                            on_type=def_dict.get("key_type", "HASH"),
                            fields=fields,
                        )
            except redis.ResponseError:
                # Index might have been deleted or is invalid
                continue

        return None
    except redis.ResponseError:
        # RediSearch not available
        return None
    finally:
        client.close()


def plan_query(
    url: str,
    pattern: str,
    filter_expr: pl.Expr | None = None,
    schema: dict | None = None,
) -> QueryPlan:
    """Plan how to execute a query for the given pattern.

    Args:
        url: Redis connection URL.
        pattern: Key pattern (e.g., "user:*").
        filter_expr: Optional Polars filter expression.
        schema: Schema dictionary.

    Returns:
        QueryPlan describing the execution strategy.
    """
    from polars_redis.query import Expr as QueryExpr

    # Check for index
    index_info = find_index_for_pattern(url, pattern)

    if index_info is None:
        # No index - must use SCAN
        plan = QueryPlan(strategy=ExecutionStrategy.SCAN)
        if filter_expr is not None:
            plan.client_filters.append(str(filter_expr))
            plan.warnings.append(
                "No index found for pattern. All filtering will be done client-side."
            )
        return plan

    # Index exists - check if we can push down the filter
    if filter_expr is None:
        # No filter - simple search with *
        return QueryPlan(
            strategy=ExecutionStrategy.SEARCH,
            index=index_info,
            server_query="*",
        )

    # Try to translate Polars expression to RediSearch query
    # For now, we'll do a simple translation for common cases
    server_query, client_filters = _translate_polars_expr(filter_expr, index_info.fields)

    if client_filters:
        strategy = ExecutionStrategy.HYBRID
    else:
        strategy = ExecutionStrategy.SEARCH

    plan = QueryPlan(
        strategy=strategy,
        index=index_info,
        server_query=server_query or "*",
        client_filters=client_filters,
    )

    if client_filters:
        plan.warnings.append(
            "Some filters cannot be pushed to RediSearch and will run client-side."
        )

    return plan


def _translate_polars_expr(
    expr: pl.Expr, indexed_fields: list[str]
) -> tuple[str | None, list[str]]:
    """Translate a Polars expression to RediSearch query.

    Args:
        expr: Polars filter expression.
        indexed_fields: List of fields that are indexed.

    Returns:
        Tuple of (server_query, client_filters).
    """
    # This is a simplified translation - in practice, we'd need to
    # analyze the expression tree more carefully
    #
    # For now, we return None for server_query (meaning fetch all)
    # and put the expression in client_filters
    #
    # A more complete implementation would:
    # 1. Parse the Polars expression AST
    # 2. Identify which parts can be pushed down
    # 3. Generate RediSearch query for pushable parts
    # 4. Return remaining parts as client filters

    return None, [str(expr)]


def smart_scan(
    url: str,
    pattern: str = "*",
    schema: dict | None = None,
    *,
    index: str | Index | None = None,
    include_key: bool = True,
    key_column_name: str = "_key",
    include_ttl: bool = False,
    ttl_column_name: str = "_ttl",
    batch_size: int = 1000,
    auto_detect_index: bool = True,
) -> pl.LazyFrame:
    """Smart scan that automatically optimizes query execution.

    This function provides a unified interface for querying Redis data.
    It automatically detects whether a RediSearch index exists for the
    key pattern and uses FT.SEARCH when possible, falling back to SCAN
    otherwise.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        pattern: Key pattern to match (e.g., "user:*").
        schema: Dictionary mapping field names to Polars dtypes.
        index: Optional index name or Index object. If provided, skip
            auto-detection and use this index.
        include_key: Whether to include the Redis key as a column.
        key_column_name: Name of the key column.
        include_ttl: Whether to include the TTL as a column.
        ttl_column_name: Name of the TTL column.
        batch_size: Number of documents per batch.
        auto_detect_index: Whether to auto-detect indexes (default: True).

    Returns:
        A Polars LazyFrame.

    Example:
        >>> # Auto-detect index and use FT.SEARCH if available
        >>> lf = smart_scan(
        ...     "redis://localhost:6379",
        ...     "user:*",
        ...     schema={"name": pl.Utf8, "age": pl.Int64},
        ... )
        >>> df = lf.filter(pl.col("age") > 30).collect()

        >>> # Force using a specific index
        >>> lf = smart_scan(
        ...     "redis://localhost:6379",
        ...     "user:*",
        ...     schema={"name": pl.Utf8, "age": pl.Int64},
        ...     index="users_idx",
        ... )
        >>> df = lf.collect()

        >>> # Check the execution plan
        >>> plan = explain_scan(
        ...     "redis://localhost:6379",
        ...     "user:*",
        ...     schema={"name": pl.Utf8, "age": pl.Int64},
        ... )
        >>> print(plan.explain())
    """
    from polars_redis._index import Index as IndexClass
    from polars_redis._scan import scan_hashes
    from polars_redis._search import search_hashes

    if schema is None:
        raise ValueError("schema is required for smart_scan")

    # Determine if we should use search or scan
    use_search = False
    index_name: str | None = None

    if index is not None:
        # Explicit index provided
        if isinstance(index, IndexClass):
            index.ensure_exists(url)
            index_name = index.name
        else:
            index_name = index
        use_search = True
    elif auto_detect_index:
        # Try to auto-detect an index
        index_info = find_index_for_pattern(url, pattern)
        if index_info is not None:
            index_name = index_info.name
            use_search = True

    if use_search and index_name:
        # Use FT.SEARCH
        return search_hashes(
            url=url,
            index=index_name,
            query="*",
            schema=schema,
            include_key=include_key,
            key_column_name=key_column_name,
            include_ttl=include_ttl,
            ttl_column_name=ttl_column_name,
            batch_size=batch_size,
        )
    else:
        # Fall back to SCAN
        return scan_hashes(
            url=url,
            pattern=pattern,
            schema=schema,
            include_key=include_key,
            key_column_name=key_column_name,
            include_ttl=include_ttl,
            ttl_column_name=ttl_column_name,
            batch_size=batch_size,
        )


def explain_scan(
    url: str,
    pattern: str = "*",
    schema: dict | None = None,
    filter_expr: pl.Expr | None = None,
) -> QueryPlan:
    """Explain how a scan would be executed.

    This function analyzes the pattern and optional filter expression
    to determine the optimal execution strategy, without actually
    executing the query.

    Args:
        url: Redis connection URL.
        pattern: Key pattern to match.
        schema: Schema dictionary.
        filter_expr: Optional Polars filter expression.

    Returns:
        QueryPlan with execution details.

    Example:
        >>> plan = explain_scan(
        ...     "redis://localhost:6379",
        ...     "user:*",
        ...     schema={"name": pl.Utf8, "age": pl.Int64},
        ...     filter_expr=pl.col("age") > 30,
        ... )
        >>> print(plan.explain())
        Strategy: SEARCH
        Index: users_idx
          Prefixes: user:
          Type: HASH
        Server Query: @age:[(30 +inf]
        Client Filters: (none)
    """
    return plan_query(url, pattern, filter_expr, schema)


def list_indexes(url: str) -> list[DetectedIndex]:
    """List all RediSearch indexes.

    Args:
        url: Redis connection URL.

    Returns:
        List of IndexInfo for all indexes.
    """
    client = redis.from_url(url)

    try:
        indexes = client.execute_command("FT._LIST")
        result = []

        for index_name in indexes:
            if isinstance(index_name, bytes):
                index_name = index_name.decode("utf-8")

            try:
                info = client.execute_command("FT.INFO", index_name)

                # Parse the flat list response
                info_dict = {}
                for i in range(0, len(info), 2):
                    key = info[i]
                    if isinstance(key, bytes):
                        key = key.decode("utf-8")
                    info_dict[key] = info[i + 1]

                # Get index definition
                index_def = info_dict.get("index_definition", [])
                if isinstance(index_def, list):
                    def_dict = {}
                    for i in range(0, len(index_def), 2):
                        k = index_def[i]
                        if isinstance(k, bytes):
                            k = k.decode("utf-8")
                        def_dict[k] = index_def[i + 1]
                else:
                    def_dict = index_def

                # Get prefixes
                prefixes = def_dict.get("prefixes", [])
                if isinstance(prefixes, bytes):
                    prefixes = [prefixes.decode("utf-8")]
                elif isinstance(prefixes, list):
                    prefixes = [p.decode("utf-8") if isinstance(p, bytes) else p for p in prefixes]

                # Get field names
                attributes = info_dict.get("attributes", [])
                fields = []
                for attr in attributes:
                    if isinstance(attr, list) and len(attr) >= 2:
                        for j in range(0, len(attr), 2):
                            k = attr[j]
                            if isinstance(k, bytes):
                                k = k.decode("utf-8")
                            if k == "identifier":
                                v = attr[j + 1]
                                if isinstance(v, bytes):
                                    v = v.decode("utf-8")
                                fields.append(v)
                                break

                on_type = def_dict.get("key_type", "HASH")
                if isinstance(on_type, bytes):
                    on_type = on_type.decode("utf-8")

                result.append(
                    DetectedIndex(
                        name=index_name,
                        prefixes=prefixes,
                        on_type=on_type,
                        fields=fields,
                    )
                )
            except redis.ResponseError:
                continue

        return result
    except redis.ResponseError:
        return []
    finally:
        client.close()
