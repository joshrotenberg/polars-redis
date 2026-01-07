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

    Supported Operations:
        - Comparisons: >, >=, <, <=, ==, !=
        - Logical: & (AND), | (OR), ~ (NOT)
        - Range: is_between(a, b)
        - Membership: is_in([...])
        - Text: contains(), starts_with(), ends_with()
        - Phrase: phrase()
        - Tag: has_tag()
        - Geo: within_radius()
        - Null checks: is_null(), is_not_null()
    """

    def __init__(self, field: str):
        """Create an expression for a field.

        Args:
            field: The field name to query.
        """
        self._field = field
        self._op: str | None = None
        self._value: ValueType | tuple | list | None = None
        self._value2: ValueType | None = None  # For between, geo radius
        self._value3: ValueType | None = None  # For geo lat
        self._value4: str | None = None  # For geo unit
        self._left: Expr | None = None
        self._right: Expr | None = None
        self._client_side: bool = False  # True if this op runs in Polars, not Redis

    # =========================================================================
    # Comparison operators
    # =========================================================================

    def __gt__(self, value: ValueType) -> Expr:
        """Greater than comparison: col("age") > 30 -> @age:[(30 +inf]"""
        expr = Expr(self._field)
        expr._op = "gt"
        expr._value = value
        return expr

    def __ge__(self, value: ValueType) -> Expr:
        """Greater than or equal: col("age") >= 30 -> @age:[30 +inf]"""
        expr = Expr(self._field)
        expr._op = "gte"
        expr._value = value
        return expr

    def __lt__(self, value: ValueType) -> Expr:
        """Less than comparison: col("age") < 30 -> @age:[-inf (30]"""
        expr = Expr(self._field)
        expr._op = "lt"
        expr._value = value
        return expr

    def __le__(self, value: ValueType) -> Expr:
        """Less than or equal: col("age") <= 30 -> @age:[-inf 30]"""
        expr = Expr(self._field)
        expr._op = "lte"
        expr._value = value
        return expr

    def __eq__(self, value: ValueType) -> Expr:  # type: ignore[override]
        """Equality: col("status") == "active" -> @status:{active}"""
        expr = Expr(self._field)
        expr._op = "eq"
        expr._value = value
        return expr

    def __ne__(self, value: ValueType) -> Expr:  # type: ignore[override]
        """Not equal: col("status") != "deleted" -> -@status:{deleted}"""
        expr = Expr(self._field)
        expr._op = "ne"
        expr._value = value
        return expr

    # =========================================================================
    # Logical operators
    # =========================================================================

    def __and__(self, other: Expr) -> Expr:
        """AND: (expr1) & (expr2) -> query1 query2"""
        expr = Expr("")
        expr._op = "and"
        expr._left = self
        expr._right = other
        return expr

    def __or__(self, other: Expr) -> Expr:
        """OR: (expr1) | (expr2) -> query1 | query2"""
        expr = Expr("")
        expr._op = "or"
        expr._left = self
        expr._right = other
        return expr

    def __invert__(self) -> Expr:
        """NOT: ~expr -> -(query)"""
        expr = Expr("")
        expr._op = "not"
        expr._left = self
        return expr

    def not_(self) -> Expr:
        """NOT (alternative syntax): expr.not_() -> -(query)"""
        return ~self

    # =========================================================================
    # Range and membership
    # =========================================================================

    def is_between(self, lower: ValueType, upper: ValueType) -> Expr:
        """Range check (inclusive): col("age").is_between(20, 40) -> @age:[20 40]"""
        expr = Expr(self._field)
        expr._op = "between"
        expr._value = lower
        expr._value2 = upper
        return expr

    def is_in(self, values: list[ValueType]) -> Expr:
        """Membership check: col("status").is_in(["a", "b"]) -> @status:{a} | @status:{b}"""
        if not values:
            expr = Expr(self._field)
            expr._op = "raw"
            expr._value = "-*"
            return expr

        result = Expr(self._field) == values[0]
        for v in values[1:]:
            result = result | (Expr(self._field) == v)
        return result

    # =========================================================================
    # Text search operations
    # =========================================================================

    def contains(self, text: str) -> Expr:
        """Full-text search: col("title").contains("python") -> @title:python

        For TEXT fields, this performs full-text search with stemming.
        For exact substring matching, use starts_with/ends_with or TAG fields.
        """
        expr = Expr(self._field)
        expr._op = "text_search"
        expr._value = text
        return expr

    def starts_with(self, prefix: str) -> Expr:
        """Prefix match: col("name").starts_with("jo") -> @name:jo*"""
        expr = Expr(self._field)
        expr._op = "prefix"
        expr._value = prefix
        return expr

    def ends_with(self, suffix: str) -> Expr:
        """Suffix match: col("name").ends_with("son") -> @name:*son"""
        expr = Expr(self._field)
        expr._op = "suffix"
        expr._value = suffix
        return expr

    def contains_substring(self, substring: str) -> Expr:
        """Infix/contains match: col("name").contains_substring("sun") -> @name:*sun*

        Matches if the substring appears anywhere in the field.
        """
        expr = Expr(self._field)
        expr._op = "infix"
        expr._value = substring
        return expr

    def matches(self, pattern: str) -> Expr:
        """Wildcard match: col("name").matches("j*n") -> @name:j*n

        Supports * and ? wildcards.
        """
        expr = Expr(self._field)
        expr._op = "wildcard"
        expr._value = pattern
        return expr

    def matches_exact(self, pattern: str) -> Expr:
        """Exact wildcard match: col("name").matches_exact("foo*bar?") -> @name:"w'foo*bar?'"

        Supports * (any chars) and ? (single char) wildcards with exact matching.
        """
        expr = Expr(self._field)
        expr._op = "wildcard_exact"
        expr._value = pattern
        return expr

    def fuzzy(self, term: str, distance: int = 1) -> Expr:
        """Fuzzy match: col("name").fuzzy("john", 1) -> @name:%john%

        Args:
            term: The term to match
            distance: Levenshtein distance (1-3, default 1)
        """
        expr = Expr(self._field)
        expr._op = "fuzzy"
        expr._value = term
        expr._value2 = min(max(distance, 1), 3)  # Clamp to 1-3
        return expr

    def phrase(self, *words: str, slop: int | None = None, inorder: bool | None = None) -> Expr:
        """Phrase search: col("title").phrase("hello", "world") -> @title:(hello world)

        Args:
            *words: Words that must appear in order
            slop: Number of intervening terms allowed (None = exact match)
            inorder: Whether words must appear in order (None = any order)

        Example:
            >>> col("title").phrase("hello", "world")  # Exact phrase
            >>> col("title").phrase("hello", "world", slop=2)  # Allow 2 words between
            >>> col("title").phrase("hello", "world", slop=2, inorder=True)  # In order with slop
        """
        expr = Expr(self._field)
        expr._op = "phrase"
        expr._value = words
        expr._value2 = slop
        expr._value3 = inorder
        return expr

    # =========================================================================
    # Tag operations
    # =========================================================================

    def has_tag(self, tag: str) -> Expr:
        """Tag match: col("categories").has_tag("science") -> @categories:{science}

        Equivalent to == for TAG fields, but more explicit.
        """
        expr = Expr(self._field)
        expr._op = "tag"
        expr._value = tag
        return expr

    def has_any_tag(self, tags: list[str]) -> Expr:
        """Match any tag: col("tags").has_any_tag(["a", "b"]) -> @tags:{a|b}"""
        expr = Expr(self._field)
        expr._op = "tag_or"
        expr._value = tags
        return expr

    # =========================================================================
    # Geo operations
    # =========================================================================

    def within_radius(self, lon: float, lat: float, radius: float, unit: str = "km") -> Expr:
        """Geo radius search: col("location").within_radius(-122.4, 37.7, 10, "km")

        Args:
            lon: Longitude
            lat: Latitude
            radius: Search radius
            unit: Distance unit (m, km, mi, ft)
        """
        expr = Expr(self._field)
        expr._op = "geo_radius"
        expr._value = lon
        expr._value2 = lat
        expr._value3 = radius
        expr._value4 = unit
        return expr

    def within_polygon(self, points: list[tuple[float, float]]) -> Expr:
        """Geo polygon search: col("location").within_polygon([(0,0), (0,10), (10,10), (10,0), (0,0)])

        Args:
            points: List of (lon, lat) tuples forming a closed polygon.
                    First and last point should be the same.

        Note: This generates a query that requires PARAMS to be passed.
        The polygon WKT is stored and should be passed via PARAMS.
        """
        expr = Expr(self._field)
        expr._op = "geo_polygon"
        expr._value = points
        return expr

    # =========================================================================
    # Vector search operations
    # =========================================================================

    def knn(self, k: int, vector_param: str = "query_vec") -> Expr:
        """K-nearest neighbors vector search.

        Args:
            k: Number of nearest neighbors to return
            vector_param: Parameter name for the vector (passed via PARAMS)

        Example:
            >>> col("embedding").knn(10, "query_vec")
            >>> # Query: *=>[KNN 10 @embedding $query_vec]
        """
        expr = Expr(self._field)
        expr._op = "vector_knn"
        expr._value = k
        expr._value2 = vector_param
        return expr

    def vector_range(self, radius: float, vector_param: str = "query_vec") -> Expr:
        """Vector range search within a given radius.

        Args:
            radius: Search radius in vector space
            vector_param: Parameter name for the vector (passed via PARAMS)

        Example:
            >>> col("embedding").vector_range(0.5, "query_vec")
            >>> # Query: @embedding:[VECTOR_RANGE 0.5 $query_vec]
        """
        expr = Expr(self._field)
        expr._op = "vector_range"
        expr._value = radius
        expr._value2 = vector_param
        return expr

    # =========================================================================
    # Null checks
    # =========================================================================

    def is_null(self) -> Expr:
        """Check for missing field: col("email").is_null() -> -@email:[*]

        Note: RediSearch doesn't have a direct NULL check, this is approximate.
        """
        expr = Expr(self._field)
        expr._op = "is_null"
        return expr

    def is_not_null(self) -> Expr:
        """Check for existing field: col("email").is_not_null() -> @email:[*]"""
        expr = Expr(self._field)
        expr._op = "is_not_null"
        return expr

    # =========================================================================
    # Optional: Boosting/scoring
    # =========================================================================

    def boost(self, weight: float) -> Expr:
        """Boost relevance: col("title").contains("python").boost(2.0)

        Increases the relevance score contribution of this term.
        """
        expr = Expr(self._field)
        expr._op = "boost"
        expr._left = self
        expr._value = weight
        return expr

    def optional(self) -> Expr:
        """Mark as optional: col("title").contains("tutorial").optional()

        Documents with this term rank higher, but it's not required.
        Generates: ~(query)

        Example:
            >>> required = col("title").contains("python")
            >>> optional = col("title").contains("tutorial").optional()
            >>> query = required & optional
            >>> # Matches docs with "python", ranks "python tutorial" higher
        """
        expr = Expr(self._field)
        expr._op = "optional"
        expr._left = self
        return expr

    # =========================================================================
    # Enhanced operations (client-side in Polars)
    #
    # These operations cannot be pushed down to RediSearch and will be
    # executed client-side in Polars after fetching results. They are
    # marked with _client_side=True and provide capabilities beyond
    # RediSearch's query syntax.
    # =========================================================================

    def matches_regex(self, pattern: str) -> Expr:
        """Regex match (client-side): col("email").matches_regex(r".*@gmail\\.com$")

        This operation cannot be pushed to RediSearch and will execute
        in Polars after fetching results from Redis.

        Args:
            pattern: Regular expression pattern.

        Example:
            >>> # Find Gmail addresses
            >>> col("email").matches_regex(r".*@gmail\\.com$")
            >>> # Find phone numbers
            >>> col("phone").matches_regex(r"\\d{3}-\\d{3}-\\d{4}")
        """
        expr = Expr(self._field)
        expr._op = "regex"
        expr._value = pattern
        expr._client_side = True
        return expr

    def icontains(self, text: str) -> Expr:
        """Case-insensitive contains (client-side): col("name").icontains("john")

        RediSearch TAG fields are case-sensitive. This operation performs
        case-insensitive matching in Polars.

        Args:
            text: Text to search for (case-insensitive).
        """
        expr = Expr(self._field)
        expr._op = "icontains"
        expr._value = text
        expr._client_side = True
        return expr

    def iequals(self, value: str) -> Expr:
        """Case-insensitive equality (client-side): col("status").iequals("ACTIVE")

        Args:
            value: Value to compare (case-insensitive).
        """
        expr = Expr(self._field)
        expr._op = "iequals"
        expr._value = value
        expr._client_side = True
        return expr

    def contains_any(self, substrings: list[str]) -> Expr:
        """Match any substring (client-side): col("desc").contains_any(["python", "rust"])

        Args:
            substrings: List of substrings to match.
        """
        expr = Expr(self._field)
        expr._op = "contains_any"
        expr._value = substrings
        expr._client_side = True
        return expr

    def similar_to(self, text: str, threshold: float = 0.8) -> Expr:
        """String similarity match (client-side): col("name").similar_to("john", 0.8)

        Uses Levenshtein distance normalized to [0, 1] range.

        Args:
            text: Text to compare against.
            threshold: Minimum similarity score (0.0 to 1.0).
        """
        expr = Expr(self._field)
        expr._op = "similar_to"
        expr._value = text
        expr._value2 = threshold
        expr._client_side = True
        return expr

    # Date/time operations (client-side)

    def as_date(self) -> DateExpr:
        """Parse field as date for comparisons (client-side).

        Returns a DateExpr that supports date comparisons.

        Example:
            >>> col("created_at").as_date() > "2024-01-01"
            >>> col("birth_date").as_date().year() == 1990
        """
        return DateExpr(self._field)

    def as_datetime(self) -> DateExpr:
        """Parse field as datetime for comparisons (client-side).

        Returns a DateExpr that supports datetime comparisons.
        """
        return DateExpr(self._field, is_datetime=True)

    # Array/JSON operations (client-side)

    def array_contains(self, value: ValueType) -> Expr:
        """Check if array field contains value (client-side).

        For JSON arrays stored in Redis.

        Args:
            value: Value to search for in the array.
        """
        expr = Expr(self._field)
        expr._op = "array_contains"
        expr._value = value
        expr._client_side = True
        return expr

    def array_len(self) -> Expr:
        """Get array length for comparison (client-side).

        Returns an expression that can be compared.

        Example:
            >>> col("tags").array_len() > 5
        """
        expr = Expr(self._field)
        expr._op = "array_len"
        expr._client_side = True
        return expr

    def json_path(self, path: str) -> Expr:
        """Extract value at JSON path (client-side): col("data").json_path("$.user.name")

        Args:
            path: JSONPath expression.
        """
        expr = Expr(self._field)
        expr._op = "json_path"
        expr._value = path
        expr._client_side = True
        return expr

    # =========================================================================
    # Query metadata
    # =========================================================================

    @property
    def is_client_side(self) -> bool:
        """Check if this expression requires client-side evaluation."""
        if getattr(self, "_client_side", False):
            return True
        if self._left and self._left.is_client_side:
            return True
        if self._right and self._right.is_client_side:
            return True
        return False

    def get_server_filter(self) -> Expr | None:
        """Extract the server-side (RediSearch) portion of this expression.

        For hybrid queries, returns only the parts that can be pushed to Redis.
        """
        if self.is_client_side and self._op not in ("and", "or"):
            return None

        if self._op == "and":
            left = self._left.get_server_filter() if self._left else None
            right = self._right.get_server_filter() if self._right else None
            if left and right:
                return left & right
            return left or right

        if self._op == "or":
            # For OR, if either side is client-side, we can't push anything
            if (self._left and self._left.is_client_side) or (
                self._right and self._right.is_client_side
            ):
                return None
            return self

        return self if not self.is_client_side else None

    def get_client_filter(self) -> Expr | None:
        """Extract the client-side (Polars) portion of this expression.

        For hybrid queries, returns only the parts that must run in Polars.
        """
        if self.is_client_side and self._op not in ("and", "or"):
            return self

        if self._op == "and":
            left = self._left.get_client_filter() if self._left else None
            right = self._right.get_client_filter() if self._right else None
            if left and right:
                return left & right
            return left or right

        return None

    def to_polars(self) -> str:
        """Convert client-side expression to Polars filter expression.

        Returns a string that can be used with df.filter() via eval,
        or raises an error if the expression is server-side only.
        """
        import polars as pl

        if self._op == "regex":
            return f'pl.col("{self._field}").str.contains(r"{self._value}")'
        elif self._op == "icontains":
            return f'pl.col("{self._field}").str.to_lowercase().str.contains("{str(self._value).lower()}")'
        elif self._op == "iequals":
            return f'pl.col("{self._field}").str.to_lowercase() == "{str(self._value).lower()}"'
        elif self._op == "contains_any":
            patterns = "|".join(str(s) for s in self._value)
            return f'pl.col("{self._field}").str.contains(r"({patterns})")'
        elif self._op == "array_contains":
            return f'pl.col("{self._field}").list.contains({repr(self._value)})'
        elif self._op == "array_len":
            return f'pl.col("{self._field}").list.len()'
        else:
            raise ValueError(f"Cannot convert operation '{self._op}' to Polars expression")

    def explain(self) -> str:
        """Explain how this query will be executed.

        Returns a human-readable explanation of which parts will run
        on Redis vs. in Polars.

        Example:
            >>> query = (col("age") > 30) & col("email").matches_regex(r".*@gmail.com")
            >>> print(query.explain())
            RediSearch: @age:[(30 +inf]
            Polars filter: pl.col("email").str.contains(r".*@gmail.com")
        """
        lines = []

        server_part = self.get_server_filter()
        client_part = self.get_client_filter()

        if server_part:
            lines.append(f"RediSearch: {server_part.to_redis()}")
        else:
            lines.append("RediSearch: * (fetch all)")

        if client_part:
            lines.append(f"Polars filter: {client_part.to_polars()}")
        else:
            lines.append("Polars filter: none (all filtering done server-side)")

        return "\n".join(lines)

    # =========================================================================
    # Internal helpers
    # =========================================================================

    def _format_value(self, value: ValueType) -> str:
        """Format a value for RediSearch query."""
        if isinstance(value, bool):
            return str(value).lower()
        elif isinstance(value, (int, float)):
            return str(value)
        else:
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

    def _escape_text(self, s: str) -> str:
        """Escape special characters in TEXT search."""
        # For text search, escape fewer chars
        special = r"@{}[]()|-~"
        result = []
        for c in s:
            if c in special:
                result.append("\\")
            result.append(c)
        return "".join(result)

    def _is_numeric(self, value: ValueType) -> bool:
        """Check if a value should be treated as numeric."""
        return isinstance(value, (int, float)) and not isinstance(value, bool)

    def _is_tag_eq(self) -> bool:
        """Check if this expression is a tag equality (non-numeric eq)."""
        return self._op == "eq" and not self._is_numeric(self._value)

    def _collect_tag_or_values(self) -> tuple[str | None, list[str]]:
        """Collect all tag values from an OR tree on the same field.

        Returns:
            Tuple of (field_name, list of tag values) if this is a valid tag OR,
            or (None, []) if it's not optimizable.
        """
        if self._op == "or":
            # Recursively collect from both branches
            left_field, left_values = (
                self._left._collect_tag_or_values() if self._left else (None, [])
            )
            right_field, right_values = (
                self._right._collect_tag_or_values() if self._right else (None, [])
            )

            # Both sides must be valid and on the same field
            if left_field and right_field and left_field == right_field:
                return left_field, left_values + right_values
            elif left_field and not right_values:
                return None, []
            elif right_field and not left_values:
                return None, []
            else:
                return None, []
        elif self._is_tag_eq():
            return self._field, [str(self._value)]
        else:
            return None, []

    def _try_optimize_tag_or(self) -> str | None:
        """Try to optimize an OR of tag equality checks into @field:{tag1|tag2}.

        RediSearch requires the @field:{tag1|tag2} syntax for OR on tag fields.
        The general OR syntax (query1 | query2) doesn't work for tags.

        Returns:
            Optimized query string if applicable, None otherwise.
        """
        field, values = self._collect_tag_or_values()
        if field and len(values) >= 2:
            # Format as @field:{tag1|tag2|...}
            # RediSearch tag OR syntax uses pipe without spaces
            escaped_values = [self._format_value(v) for v in values]
            return f"@{field}:{{{'|'.join(escaped_values)}}}"
        return None

    # =========================================================================
    # Query generation
    # =========================================================================

    def to_redis(self) -> str:
        """Convert this expression to a RediSearch query string.

        Returns the query in RediSearch syntax that will be sent to Redis
        when using search_hashes() or search_json(). This is useful for:

        - Debugging queries to see the generated RediSearch syntax
        - Understanding what will be sent to Redis
        - Copying the query for use with redis-cli or other tools

        Returns:
            str: RediSearch query string in FT.SEARCH syntax.

        Example:
            >>> from polars_redis.query import col
            >>>
            >>> # Simple comparison
            >>> query = col("age") > 30
            >>> print(query.to_redis())
            '@age:[(30 +inf]'
            >>>
            >>> # Combined conditions
            >>> query = (col("type") == "eBikes") & (col("price") < 1000)
            >>> print(query.to_redis())
            '@type:{eBikes} @price:[-inf (1000]'
            >>>
            >>> # Text search with fuzzy matching
            >>> query = col("title").fuzzy("python", distance=1)
            >>> print(query.to_redis())
            '@title:%python%'
        """

        # Comparison operators
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

        # Logical operators
        elif self._op == "and":
            left = self._left.to_redis() if self._left else "*"
            right = self._right.to_redis() if self._right else "*"
            if self._left and self._left._op == "or":
                left = f"({left})"
            if self._right and self._right._op == "or":
                right = f"({right})"
            return f"{left} {right}"
        elif self._op == "or":
            # Check if this is an OR of tag equality checks on the same field
            # RediSearch requires @field:{tag1|tag2} syntax for tag ORs
            tag_or_result = self._try_optimize_tag_or()
            if tag_or_result:
                return tag_or_result

            left = self._left.to_redis() if self._left else "*"
            right = self._right.to_redis() if self._right else "*"
            # Wrap AND clauses in parentheses for proper precedence
            if self._left and self._left._op == "and":
                left = f"({left})"
            if self._right and self._right._op == "and":
                right = f"({right})"
            # Wrap the entire OR in parentheses for proper precedence
            return f"({left} | {right})"
        elif self._op == "not":
            inner = self._left.to_redis() if self._left else "*"
            return f"-({inner})"

        # Text search
        elif self._op == "text_search":
            return f"@{self._field}:{self._escape_text(str(self._value))}"
        elif self._op == "prefix":
            return f"@{self._field}:{self._escape_text(str(self._value))}*"
        elif self._op == "suffix":
            return f"@{self._field}:*{self._escape_text(str(self._value))}"
        elif self._op == "infix":
            return f"@{self._field}:*{self._escape_text(str(self._value))}*"
        elif self._op == "wildcard":
            return f"@{self._field}:{str(self._value)}"
        elif self._op == "wildcard_exact":
            return f"@{self._field}:\"w'{self._value}'\""
        elif self._op == "fuzzy":
            pct = "%" * int(self._value2)  # 1-3 percent signs
            return f"@{self._field}:{pct}{self._escape_text(str(self._value))}{pct}"
        elif self._op == "phrase":
            words = " ".join(str(w) for w in self._value)
            # Build query attributes for slop/inorder
            attrs = []
            if self._value2 is not None:  # slop
                attrs.append(f"$slop: {self._value2}")
            if self._value3 is not None:  # inorder
                attrs.append(f"$inorder: {str(self._value3).lower()}")
            if attrs:
                return f"@{self._field}:({words}) => {{ {'; '.join(attrs)}; }}"
            return f"@{self._field}:({words})"

        # Tag operations
        elif self._op == "tag":
            return f"@{self._field}:{{{self._format_value(self._value)}}}"
        elif self._op == "tag_or":
            tags = "|".join(self._format_value(t) for t in self._value)
            return f"@{self._field}:{{{tags}}}"

        # Multi-field search
        elif self._op == "multi_field_search":
            fields = "|".join(self._value)
            return f"@{fields}:{self._escape_text(str(self._value2))}"
        elif self._op == "multi_field_prefix":
            fields = "|".join(self._value)
            return f"@{fields}:{self._escape_text(str(self._value2))}*"

        # Geo operations
        elif self._op == "geo_radius":
            # @geo:[lon lat radius unit]
            return f"@{self._field}:[{self._value} {self._value2} {self._value3} {self._value4}]"
        elif self._op == "geo_polygon":
            # Polygon requires PARAMS - query just references the param
            return f"@{self._field}:[WITHIN $poly]"

        # Vector search
        elif self._op == "vector_knn":
            # *=>[KNN k @field $param]
            return f"*=>[KNN {self._value} @{self._field} ${self._value2}]"
        elif self._op == "vector_range":
            # @field:[VECTOR_RANGE radius $param]
            return f"@{self._field}:[VECTOR_RANGE {self._value} ${self._value2}]"

        # Null checks
        elif self._op == "is_null":
            # RediSearch: ismissing(@field) in DIALECT 4, or workaround
            return f"ismissing(@{self._field})"
        elif self._op == "is_not_null":
            return f"-ismissing(@{self._field})"

        # Boosting
        elif self._op == "boost":
            inner = self._left.to_redis() if self._left else "*"
            return f"({inner}) => {{ $weight: {self._value}; }}"

        # Optional terms
        elif self._op == "optional":
            inner = self._left.to_redis() if self._left else "*"
            return f"~{inner}"

        # Raw/fallback
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


# =============================================================================
# Factory functions
# =============================================================================


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
        >>> # Comparisons
        >>> col("age") > 30           # @age:[(30 +inf]
        >>> col("age").is_between(20, 40)  # @age:[20 40]
        >>>
        >>> # Logical operators
        >>> (col("age") > 30) & (col("status") == "active")
        >>> (col("x") == 1) | (col("x") == 2)
        >>> ~(col("status") == "deleted")  # NOT
        >>>
        >>> # Text search
        >>> col("title").contains("python")     # @title:python
        >>> col("name").starts_with("jo")       # @name:jo*
        >>> col("name").fuzzy("john", 1)        # @name:%john%
        >>> col("title").phrase("hello", "world")  # @title:(hello world)
        >>>
        >>> # Tags
        >>> col("tags").has_tag("urgent")       # @tags:{urgent}
        >>> col("tags").has_any_tag(["a", "b"]) # @tags:{a|b}
        >>>
        >>> # Geo
        >>> col("loc").within_radius(-122.4, 37.7, 10, "km")
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
        >>> # Complex query not supported by builder
        >>> raw("@title:python @year:[2020 2024]")
        >>>
        >>> # Combine raw with builder
        >>> (col("age") > 30) & raw("@name:john*")
    """
    expr = Expr("")
    expr._op = "raw"
    expr._value = query
    return expr


def match_all() -> Expr:
    """Match all documents: *"""
    return raw("*")


def match_none() -> Expr:
    """Match no documents."""
    return raw("-*")


def cols(*names: str) -> MultiFieldExpr:
    """Create a multi-field expression for searching across multiple fields.

    Args:
        *names: Field names to search across.

    Returns:
        A MultiFieldExpr that generates @field1|field2|...:term queries.

    Example:
        >>> from polars_redis.query import cols
        >>>
        >>> # Search across title and body
        >>> cols("title", "body").contains("python")
        >>> # Generates: @title|body:python
    """
    return MultiFieldExpr(list(names))


# =============================================================================
# Date expression helpers (client-side)
# =============================================================================


class DateExpr:
    """Expression for date/datetime comparisons (client-side).

    Provides date-aware comparisons that execute in Polars after
    fetching results from Redis.

    Example:
        >>> col("created_at").as_date() > "2024-01-01"
        >>> col("birth_date").as_date().year() == 1990
        >>> col("updated_at").as_datetime().month().is_in([1, 2, 3])
    """

    def __init__(self, field: str, is_datetime: bool = False):
        self._field = field
        self._is_datetime = is_datetime

    def __gt__(self, value: str) -> Expr:
        """Date greater than: col("date").as_date() > "2024-01-01" """
        expr = Expr(self._field)
        expr._op = "date_gt"
        expr._value = value
        expr._value2 = self._is_datetime
        expr._client_side = True
        return expr

    def __ge__(self, value: str) -> Expr:
        """Date greater than or equal."""
        expr = Expr(self._field)
        expr._op = "date_gte"
        expr._value = value
        expr._value2 = self._is_datetime
        expr._client_side = True
        return expr

    def __lt__(self, value: str) -> Expr:
        """Date less than."""
        expr = Expr(self._field)
        expr._op = "date_lt"
        expr._value = value
        expr._value2 = self._is_datetime
        expr._client_side = True
        return expr

    def __le__(self, value: str) -> Expr:
        """Date less than or equal."""
        expr = Expr(self._field)
        expr._op = "date_lte"
        expr._value = value
        expr._value2 = self._is_datetime
        expr._client_side = True
        return expr

    def __eq__(self, value: str) -> Expr:  # type: ignore[override]
        """Date equality."""
        expr = Expr(self._field)
        expr._op = "date_eq"
        expr._value = value
        expr._value2 = self._is_datetime
        expr._client_side = True
        return expr

    def year(self) -> DatePartExpr:
        """Extract year for comparison: col("date").as_date().year() == 2024"""
        return DatePartExpr(self._field, "year", self._is_datetime)

    def month(self) -> DatePartExpr:
        """Extract month for comparison: col("date").as_date().month() == 12"""
        return DatePartExpr(self._field, "month", self._is_datetime)

    def day(self) -> DatePartExpr:
        """Extract day for comparison: col("date").as_date().day() == 25"""
        return DatePartExpr(self._field, "day", self._is_datetime)

    def weekday(self) -> DatePartExpr:
        """Extract weekday (0=Monday, 6=Sunday)."""
        return DatePartExpr(self._field, "weekday", self._is_datetime)

    def hour(self) -> DatePartExpr:
        """Extract hour (for datetime)."""
        return DatePartExpr(self._field, "hour", self._is_datetime)

    def minute(self) -> DatePartExpr:
        """Extract minute (for datetime)."""
        return DatePartExpr(self._field, "minute", self._is_datetime)


class DatePartExpr:
    """Expression for date part comparisons (client-side)."""

    def __init__(self, field: str, part: str, is_datetime: bool):
        self._field = field
        self._part = part
        self._is_datetime = is_datetime

    def __eq__(self, value: int) -> Expr:  # type: ignore[override]
        expr = Expr(self._field)
        expr._op = f"date_part_{self._part}_eq"
        expr._value = value
        expr._value2 = self._is_datetime
        expr._client_side = True
        return expr

    def __gt__(self, value: int) -> Expr:
        expr = Expr(self._field)
        expr._op = f"date_part_{self._part}_gt"
        expr._value = value
        expr._value2 = self._is_datetime
        expr._client_side = True
        return expr

    def __lt__(self, value: int) -> Expr:
        expr = Expr(self._field)
        expr._op = f"date_part_{self._part}_lt"
        expr._value = value
        expr._value2 = self._is_datetime
        expr._client_side = True
        return expr

    def __ge__(self, value: int) -> Expr:
        expr = Expr(self._field)
        expr._op = f"date_part_{self._part}_gte"
        expr._value = value
        expr._value2 = self._is_datetime
        expr._client_side = True
        return expr

    def __le__(self, value: int) -> Expr:
        expr = Expr(self._field)
        expr._op = f"date_part_{self._part}_lte"
        expr._value = value
        expr._value2 = self._is_datetime
        expr._client_side = True
        return expr

    def is_in(self, values: list[int]) -> Expr:
        """Check if date part is in a list of values."""
        expr = Expr(self._field)
        expr._op = f"date_part_{self._part}_in"
        expr._value = values
        expr._value2 = self._is_datetime
        expr._client_side = True
        return expr


class MultiFieldExpr:
    """Expression for searching across multiple fields."""

    def __init__(self, fields: list[str]):
        self._fields = fields

    def contains(self, text: str) -> Expr:
        """Full-text search across all fields."""
        expr = Expr("")
        expr._op = "multi_field_search"
        expr._value = self._fields
        expr._value2 = text
        return expr

    def starts_with(self, prefix: str) -> Expr:
        """Prefix match across all fields."""
        expr = Expr("")
        expr._op = "multi_field_prefix"
        expr._value = self._fields
        expr._value2 = prefix
        return expr
