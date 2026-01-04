"""Query builder for RediSearch predicate pushdown.

This module provides a Polars-like syntax for building RediSearch queries,
enabling automatic predicate pushdown when querying Redis.

Example:
    >>> from polars_redis.query import col
    >>>
    >>> # Build a query using familiar Polars-like syntax
    >>> query = (col("age") > 30) & (col("status") == "active")
    >>> print(query.to_redis())
    '@age:[(30 +inf] @status:{active}'
    >>>
    >>> # Use with search_hashes
    >>> from polars_redis import search_hashes
    >>> lf = search_hashes(
    ...     "redis://localhost",
    ...     index="users_idx",
    ...     query=query,  # Pass the query object directly
    ...     schema={"name": pl.Utf8, "age": pl.Int64}
    ... )
"""

from __future__ import annotations

from typing import Union

# Type alias for values that can be used in predicates
ValueType = Union[int, float, str, bool]


class Expr:
    """A query expression that can be translated to RediSearch syntax.

    This class mimics Polars' Expr interface for common filter operations.
    """

    def __init__(self, field: str):
        """Create an expression for a field.

        Args:
            field: The field name to query.
        """
        self._field = field
        self._op: str | None = None
        self._value: ValueType | None = None
        self._value2: ValueType | None = None  # For between
        self._left: Expr | None = None
        self._right: Expr | None = None

    def __gt__(self, value: ValueType) -> Expr:
        """Greater than comparison."""
        expr = Expr(self._field)
        expr._op = "gt"
        expr._value = value
        return expr

    def __ge__(self, value: ValueType) -> Expr:
        """Greater than or equal comparison."""
        expr = Expr(self._field)
        expr._op = "gte"
        expr._value = value
        return expr

    def __lt__(self, value: ValueType) -> Expr:
        """Less than comparison."""
        expr = Expr(self._field)
        expr._op = "lt"
        expr._value = value
        return expr

    def __le__(self, value: ValueType) -> Expr:
        """Less than or equal comparison."""
        expr = Expr(self._field)
        expr._op = "lte"
        expr._value = value
        return expr

    def __eq__(self, value: ValueType) -> Expr:  # type: ignore[override]
        """Equality comparison."""
        expr = Expr(self._field)
        expr._op = "eq"
        expr._value = value
        return expr

    def __ne__(self, value: ValueType) -> Expr:  # type: ignore[override]
        """Not equal comparison."""
        expr = Expr(self._field)
        expr._op = "ne"
        expr._value = value
        return expr

    def __and__(self, other: Expr) -> Expr:
        """Combine with AND."""
        expr = Expr("")
        expr._op = "and"
        expr._left = self
        expr._right = other
        return expr

    def __or__(self, other: Expr) -> Expr:
        """Combine with OR."""
        expr = Expr("")
        expr._op = "or"
        expr._left = self
        expr._right = other
        return expr

    def is_between(self, lower: ValueType, upper: ValueType) -> Expr:
        """Check if value is between lower and upper (inclusive).

        Args:
            lower: Lower bound (inclusive).
            upper: Upper bound (inclusive).

        Returns:
            A new expression representing the between condition.
        """
        expr = Expr(self._field)
        expr._op = "between"
        expr._value = lower
        expr._value2 = upper
        return expr

    def is_in(self, values: list[ValueType]) -> Expr:
        """Check if value is in a list of values.

        Args:
            values: List of values to check against.

        Returns:
            A new expression representing the IN condition.
        """
        if not values:
            # Empty list - nothing matches
            expr = Expr(self._field)
            expr._op = "raw"
            expr._value = "-*"  # Match nothing
            return expr

        # Build OR of equality checks
        result = Expr(self._field) == values[0]
        for v in values[1:]:
            result = result | (Expr(self._field) == v)
        return result

    def _format_value(self, value: ValueType) -> str:
        """Format a value for RediSearch query."""
        if isinstance(value, bool):
            return str(value).lower()
        elif isinstance(value, (int, float)):
            return str(value)
        else:
            # String - escape special characters for TAG fields
            return self._escape_tag(str(value))

    def _escape_tag(self, s: str) -> str:
        """Escape special characters in TAG values."""
        special = r",.<>{}[]\"':;!@#$%^&*()-+=~ "
        result = []
        for c in s:
            if c in special:
                result.append("\\")
            result.append(c)
        return "".join(result)

    def _is_numeric(self, value: ValueType) -> bool:
        """Check if a value should be treated as numeric."""
        return isinstance(value, (int, float)) and not isinstance(value, bool)

    def to_redis(self) -> str:
        """Convert the expression to a RediSearch query string.

        Returns:
            A RediSearch query string.

        Example:
            >>> (col("age") > 30).to_redis()
            '@age:[(30 +inf]'
        """
        if self._op == "gt":
            return f"@{self._field}:[({self._value} +inf]"
        elif self._op == "gte":
            return f"@{self._field}:[{self._value} +inf]"
        elif self._op == "lt":
            return f"@{self._field}:[-inf ({self._value}]"
        elif self._op == "lte":
            return f"@{self._field}:[-inf {self._value}]"
        elif self._op == "eq":
            if self._is_numeric(self._value):
                return f"@{self._field}:[{self._value} {self._value}]"
            else:
                return f"@{self._field}:{{{self._format_value(self._value)}}}"
        elif self._op == "ne":
            if self._is_numeric(self._value):
                return f"-@{self._field}:[{self._value} {self._value}]"
            else:
                return f"-@{self._field}:{{{self._format_value(self._value)}}}"
        elif self._op == "between":
            return f"@{self._field}:[{self._value} {self._value2}]"
        elif self._op == "and":
            left = self._left.to_redis() if self._left else "*"
            right = self._right.to_redis() if self._right else "*"
            # Wrap OR expressions in parentheses
            if self._left and self._left._op == "or":
                left = f"({left})"
            if self._right and self._right._op == "or":
                right = f"({right})"
            return f"{left} {right}"
        elif self._op == "or":
            left = self._left.to_redis() if self._left else "*"
            right = self._right.to_redis() if self._right else "*"
            # Wrap AND expressions in parentheses
            if self._left and self._left._op == "and":
                left = f"({left})"
            if self._right and self._right._op == "and":
                right = f"({right})"
            return f"{left} | {right}"
        elif self._op == "raw":
            return str(self._value)
        else:
            return "*"

    def __str__(self) -> str:
        """String representation (the RediSearch query)."""
        return self.to_redis()

    def __repr__(self) -> str:
        """Debug representation."""
        return f"Expr({self.to_redis()!r})"


def col(name: str) -> Expr:
    """Create a column expression.

    This is the main entry point for building RediSearch queries using
    a Polars-like syntax.

    Args:
        name: The field/column name.

    Returns:
        An Expr that can be used with comparison operators.

    Example:
        >>> from polars_redis.query import col
        >>>
        >>> # Simple comparison
        >>> query = col("age") > 30
        >>> print(query.to_redis())
        '@age:[(30 +inf]'
        >>>
        >>> # Combined conditions
        >>> query = (col("age") > 30) & (col("status") == "active")
        >>> print(query.to_redis())
        '@age:[(30 +inf] @status:{active}'
        >>>
        >>> # OR conditions
        >>> query = (col("status") == "active") | (col("status") == "pending")
        >>> print(query.to_redis())
        '@status:{active} | @status:{pending}'
        >>>
        >>> # Between
        >>> query = col("age").is_between(20, 40)
        >>> print(query.to_redis())
        '@age:[20 40]'
        >>>
        >>> # IN list
        >>> query = col("status").is_in(["active", "pending", "review"])
        >>> print(query.to_redis())
        '@status:{active} | @status:{pending} | @status:{review}'
    """
    return Expr(name)


def raw(query: str) -> Expr:
    """Create a raw RediSearch query expression.

    Use this as an escape hatch when you need RediSearch features
    not supported by the query builder.

    Args:
        query: A raw RediSearch query string.

    Returns:
        An Expr containing the raw query.

    Example:
        >>> from polars_redis.query import raw, col
        >>>
        >>> # Use raw query for full-text search
        >>> query = raw("@description:hello world")
        >>>
        >>> # Combine raw with builder
        >>> query = (col("age") > 30) & raw("@name:john*")
    """
    expr = Expr("")
    expr._op = "raw"
    expr._value = query
    return expr
