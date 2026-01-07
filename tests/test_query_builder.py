"""Tests for query builder enhancements."""

from __future__ import annotations

import pytest
from polars_redis.options import (
    HighlightOptions,
    SearchOptions,
    SummarizeOptions,
)
from polars_redis.query import (
    DateExpr,
    DatePartExpr,
    Expr,
    MultiFieldExpr,
    col,
    cols,
    match_all,
    match_none,
    raw,
)


class TestBasicQueries:
    """Test basic query builder operations."""

    def test_col_gt(self):
        """Test greater than."""
        query = col("age") > 30
        assert query.to_redis() == "@age:[(30 +inf]"

    def test_col_gte(self):
        """Test greater than or equal."""
        query = col("age") >= 30
        assert query.to_redis() == "@age:[30 +inf]"

    def test_col_lt(self):
        """Test less than."""
        query = col("age") < 30
        assert query.to_redis() == "@age:[-inf (30]"

    def test_col_lte(self):
        """Test less than or equal."""
        query = col("age") <= 30
        assert query.to_redis() == "@age:[-inf 30]"

    def test_col_eq_numeric(self):
        """Test equality for numeric."""
        query = col("age") == 30
        assert query.to_redis() == "@age:[30 30]"

    def test_col_eq_string(self):
        """Test equality for string (TAG)."""
        query = col("status") == "active"
        assert query.to_redis() == "@status:{active}"

    def test_col_ne_string(self):
        """Test not equal for string."""
        query = col("status") != "deleted"
        assert query.to_redis() == "-@status:{deleted}"

    def test_and(self):
        """Test AND combination."""
        query = (col("age") > 30) & (col("status") == "active")
        assert "@age:[(30 +inf]" in query.to_redis()
        assert "@status:{active}" in query.to_redis()

    def test_or(self):
        """Test OR combination."""
        query = (col("status") == "active") | (col("status") == "pending")
        assert "|" in query.to_redis()

    def test_not(self):
        """Test NOT."""
        query = ~(col("status") == "deleted")
        assert query.to_redis() == "-(@status:{deleted})"

    def test_is_between(self):
        """Test between range."""
        query = col("age").is_between(20, 40)
        assert query.to_redis() == "@age:[20 40]"

    def test_raw_query(self):
        """Test raw query escape hatch."""
        query = raw("@title:python @year:[2020 2024]")
        assert query.to_redis() == "@title:python @year:[2020 2024]"

    def test_match_all(self):
        """Test match all."""
        query = match_all()
        assert query.to_redis() == "*"

    def test_match_none(self):
        """Test match none."""
        query = match_none()
        assert query.to_redis() == "-*"


class TestTextSearch:
    """Test text search operations."""

    def test_contains(self):
        """Test full-text search."""
        query = col("title").contains("python")
        assert query.to_redis() == "@title:python"

    def test_starts_with(self):
        """Test prefix match."""
        query = col("name").starts_with("jo")
        assert query.to_redis() == "@name:jo*"

    def test_ends_with(self):
        """Test suffix match."""
        query = col("name").ends_with("son")
        assert query.to_redis() == "@name:*son"

    def test_contains_substring(self):
        """Test infix match."""
        query = col("name").contains_substring("sun")
        assert query.to_redis() == "@name:*sun*"

    def test_fuzzy(self):
        """Test fuzzy match."""
        query = col("name").fuzzy("john", 1)
        assert query.to_redis() == "@name:%john%"

        query2 = col("name").fuzzy("john", 2)
        assert query2.to_redis() == "@name:%%john%%"

    def test_phrase(self):
        """Test phrase search."""
        query = col("title").phrase("hello", "world")
        assert query.to_redis() == "@title:(hello world)"

    def test_phrase_with_slop(self):
        """Test phrase with slop."""
        query = col("title").phrase("hello", "world", slop=2)
        assert "$slop: 2" in query.to_redis()

    def test_phrase_with_inorder(self):
        """Test phrase with inorder."""
        query = col("title").phrase("hello", "world", inorder=True)
        assert "$inorder: true" in query.to_redis()


class TestTagOperations:
    """Test TAG field operations."""

    def test_has_tag(self):
        """Test single tag match."""
        query = col("category").has_tag("science")
        assert query.to_redis() == "@category:{science}"

    def test_has_any_tag(self):
        """Test multiple tag OR match."""
        query = col("tags").has_any_tag(["urgent", "important"])
        assert query.to_redis() == "@tags:{urgent|important}"


class TestGeoOperations:
    """Test geo search operations."""

    def test_within_radius(self):
        """Test geo radius search."""
        query = col("location").within_radius(-122.4, 37.7, 10, "km")
        assert query.to_redis() == "@location:[-122.4 37.7 10 km]"

    def test_within_polygon(self):
        """Test geo polygon search."""
        points = [(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)]
        query = col("location").within_polygon(points)
        assert "@location:[WITHIN $poly]" in query.to_redis()


class TestVectorSearch:
    """Test vector search operations."""

    def test_knn(self):
        """Test KNN vector search."""
        query = col("embedding").knn(10, "query_vec")
        assert query.to_redis() == "*=>[KNN 10 @embedding $query_vec]"

    def test_vector_range(self):
        """Test vector range search."""
        query = col("embedding").vector_range(0.5, "query_vec")
        assert query.to_redis() == "@embedding:[VECTOR_RANGE 0.5 $query_vec]"


class TestNullChecks:
    """Test null/missing field checks."""

    def test_is_null(self):
        """Test is_null check."""
        query = col("email").is_null()
        assert query.to_redis() == "ismissing(@email)"

    def test_is_not_null(self):
        """Test is_not_null check."""
        query = col("email").is_not_null()
        assert query.to_redis() == "-ismissing(@email)"


class TestScoring:
    """Test scoring operations."""

    def test_boost(self):
        """Test boost."""
        query = col("title").contains("python").boost(2.0)
        assert "$weight: 2.0" in query.to_redis()

    def test_optional(self):
        """Test optional terms."""
        query = col("title").contains("tutorial").optional()
        assert query.to_redis() == "~@title:tutorial"


class TestMultiField:
    """Test multi-field search."""

    def test_cols_contains(self):
        """Test multi-field contains."""
        query = cols("title", "body").contains("python")
        assert query.to_redis() == "@title|body:python"

    def test_cols_starts_with(self):
        """Test multi-field prefix."""
        query = cols("title", "body", "summary").starts_with("py")
        assert query.to_redis() == "@title|body|summary:py*"


class TestEnhancedClientSide:
    """Test enhanced client-side operations."""

    def test_matches_regex(self):
        """Test regex matching (client-side)."""
        query = col("email").matches_regex(r".*@gmail\.com$")
        assert query._op == "regex"
        assert query._client_side is True

    def test_icontains(self):
        """Test case-insensitive contains (client-side)."""
        query = col("name").icontains("john")
        assert query._op == "icontains"
        assert query._client_side is True

    def test_iequals(self):
        """Test case-insensitive equality (client-side)."""
        query = col("status").iequals("ACTIVE")
        assert query._op == "iequals"
        assert query._client_side is True

    def test_contains_any(self):
        """Test contains any substring (client-side)."""
        query = col("desc").contains_any(["python", "rust"])
        assert query._op == "contains_any"
        assert query._client_side is True

    def test_similar_to(self):
        """Test string similarity (client-side)."""
        query = col("name").similar_to("john", 0.8)
        assert query._op == "similar_to"
        assert query._value2 == 0.8
        assert query._client_side is True

    def test_array_contains(self):
        """Test array contains (client-side)."""
        query = col("tags").array_contains("python")
        assert query._op == "array_contains"
        assert query._client_side is True

    def test_array_len(self):
        """Test array length (client-side)."""
        query = col("tags").array_len()
        assert query._op == "array_len"
        assert query._client_side is True

    def test_json_path(self):
        """Test JSON path extraction (client-side)."""
        query = col("data").json_path("$.user.name")
        assert query._op == "json_path"
        assert query._value == "$.user.name"
        assert query._client_side is True


class TestDateOperations:
    """Test date/datetime operations (client-side)."""

    def test_as_date_gt(self):
        """Test date greater than."""
        query = col("created_at").as_date() > "2024-01-01"
        assert query._op == "date_gt"
        assert query._client_side is True

    def test_as_date_lt(self):
        """Test date less than."""
        query = col("created_at").as_date() < "2024-12-31"
        assert query._op == "date_lt"
        assert query._client_side is True

    def test_as_date_eq(self):
        """Test date equality."""
        query = col("birth_date").as_date() == "1990-01-15"
        assert query._op == "date_eq"
        assert query._client_side is True

    def test_as_datetime(self):
        """Test datetime parsing."""
        query = col("updated_at").as_datetime() > "2024-01-01T12:00:00"
        assert query._op == "date_gt"
        assert query._value2 is True  # is_datetime

    def test_date_part_year(self):
        """Test year extraction."""
        query = col("birth_date").as_date().year() == 1990
        assert query._op == "date_part_year_eq"
        assert query._client_side is True

    def test_date_part_month(self):
        """Test month extraction."""
        query = col("birth_date").as_date().month() == 12
        assert query._op == "date_part_month_eq"

    def test_date_part_day(self):
        """Test day extraction."""
        query = col("birth_date").as_date().day() > 15
        assert query._op == "date_part_day_gt"

    def test_date_part_is_in(self):
        """Test date part is_in."""
        query = col("created_at").as_date().month().is_in([1, 2, 3])
        assert query._op == "date_part_month_in"
        assert query._value == [1, 2, 3]


class TestHybridExecution:
    """Test hybrid query execution (server + client filtering)."""

    def test_is_client_side_simple(self):
        """Test is_client_side for simple server-side query."""
        query = col("age") > 30
        assert query.is_client_side is False

    def test_is_client_side_regex(self):
        """Test is_client_side for client-side query."""
        query = col("email").matches_regex(r".*@gmail\.com")
        assert query.is_client_side is True

    def test_is_client_side_combined(self):
        """Test is_client_side for combined query."""
        query = (col("age") > 30) & col("email").matches_regex(r".*@gmail\.com")
        assert query.is_client_side is True

    def test_get_server_filter_pure_server(self):
        """Test extracting server filter from pure server query."""
        query = col("age") > 30
        server = query.get_server_filter()
        assert server is not None
        assert server.to_redis() == "@age:[(30 +inf]"

    def test_get_server_filter_hybrid(self):
        """Test extracting server filter from hybrid query."""
        query = (col("age") > 30) & col("email").matches_regex(r".*@gmail\.com")
        server = query.get_server_filter()
        assert server is not None
        assert server.to_redis() == "@age:[(30 +inf]"

    def test_get_client_filter_hybrid(self):
        """Test extracting client filter from hybrid query."""
        query = (col("age") > 30) & col("email").matches_regex(r".*@gmail\.com")
        client = query.get_client_filter()
        assert client is not None
        assert client._op == "regex"

    def test_explain_pure_server(self):
        """Test explain for pure server query."""
        query = col("age") > 30
        explanation = query.explain()
        assert "RediSearch:" in explanation
        assert "@age:[(30 +inf]" in explanation
        assert "none (all filtering done server-side)" in explanation

    def test_explain_hybrid(self):
        """Test explain for hybrid query."""
        query = (col("age") > 30) & col("email").matches_regex(r".*@gmail\.com")
        explanation = query.explain()
        assert "RediSearch:" in explanation
        assert "Polars filter:" in explanation


class TestSearchOptions:
    """Test SearchOptions configuration."""

    def test_basic_options(self):
        """Test basic SearchOptions."""
        opts = SearchOptions(
            index="users_idx",
            query="@age:[30 +inf]",
            sort_by="score",
            sort_ascending=False,
        )
        assert opts.index == "users_idx"
        assert opts.query == "@age:[30 +inf]"
        assert opts.sort_by == "score"
        assert opts.sort_ascending is False

    def test_verbatim(self):
        """Test verbatim option."""
        opts = SearchOptions().with_verbatim(True)
        assert opts.verbatim is True

    def test_no_stopwords(self):
        """Test no_stopwords option."""
        opts = SearchOptions().with_no_stopwords(True)
        assert opts.no_stopwords is True

    def test_language(self):
        """Test language option."""
        opts = SearchOptions().with_language("english")
        assert opts.language == "english"

    def test_scorer(self):
        """Test scorer option."""
        opts = SearchOptions().with_scorer("BM25")
        assert opts.scorer == "BM25"

    def test_in_keys(self):
        """Test in_keys option."""
        opts = SearchOptions().with_in_keys(["key1", "key2"])
        assert opts.in_keys == ["key1", "key2"]

    def test_in_fields(self):
        """Test in_fields option."""
        opts = SearchOptions().with_in_fields(["title", "body"])
        assert opts.in_fields == ["title", "body"]

    def test_timeout(self):
        """Test timeout option."""
        opts = SearchOptions().with_timeout(5000)
        assert opts.timeout_ms == 5000

    def test_dialect(self):
        """Test dialect option."""
        opts = SearchOptions().with_dialect(4)
        assert opts.dialect == 4

    def test_include_score(self):
        """Test include_score option."""
        opts = SearchOptions().with_score(True, "_relevance")
        assert opts.include_score is True
        assert opts.score_column_name == "_relevance"

    def test_slop(self):
        """Test slop option."""
        opts = SearchOptions().with_slop(2)
        assert opts.slop == 2

    def test_in_order(self):
        """Test in_order option."""
        opts = SearchOptions().with_in_order(True)
        assert opts.in_order is True

    def test_expander(self):
        """Test expander option."""
        opts = SearchOptions().with_expander("SYNONYM")
        assert opts.expander == "SYNONYM"


class TestSummarizeOptions:
    """Test SummarizeOptions configuration."""

    def test_default(self):
        """Test default SummarizeOptions."""
        opts = SummarizeOptions()
        assert opts.fields is None
        assert opts.frags == 3
        assert opts.len == 20
        assert opts.separator == "..."

    def test_with_fields(self):
        """Test with specific fields."""
        opts = SummarizeOptions(fields=["title", "body"])
        assert opts.fields == ["title", "body"]

    def test_custom_options(self):
        """Test custom options."""
        opts = SummarizeOptions(frags=5, len=50, separator=" | ")
        assert opts.frags == 5
        assert opts.len == 50
        assert opts.separator == " | "


class TestHighlightOptions:
    """Test HighlightOptions configuration."""

    def test_default(self):
        """Test default HighlightOptions."""
        opts = HighlightOptions()
        assert opts.fields is None
        assert opts.open_tag == "<b>"
        assert opts.close_tag == "</b>"

    def test_custom_tags(self):
        """Test custom tags."""
        opts = HighlightOptions(open_tag="<em>", close_tag="</em>")
        assert opts.open_tag == "<em>"
        assert opts.close_tag == "</em>"

    def test_with_fields(self):
        """Test with specific fields."""
        opts = HighlightOptions(fields=["title"])
        assert opts.fields == ["title"]


class TestSearchOptionsBuilder:
    """Test SearchOptions builder pattern."""

    def test_with_summarize(self):
        """Test with_summarize builder method."""
        opts = SearchOptions().with_summarize(fields=["title"], len=50)
        assert opts.summarize is not None
        assert opts.summarize.fields == ["title"]
        assert opts.summarize.len == 50

    def test_with_highlight(self):
        """Test with_highlight builder method."""
        opts = SearchOptions().with_highlight(open_tag="<mark>", close_tag="</mark>")
        assert opts.highlight is not None
        assert opts.highlight.open_tag == "<mark>"
        assert opts.highlight.close_tag == "</mark>"

    def test_chained_builders(self):
        """Test chained builder methods."""
        opts = (
            SearchOptions()
            .with_index("articles_idx")
            .with_query("python programming")
            .with_verbatim(True)
            .with_language("english")
            .with_summarize(len=50)
            .with_highlight(open_tag="<em>", close_tag="</em>")
            .with_sort("score", ascending=False)
            .with_n_rows(100)
        )
        assert opts.index == "articles_idx"
        assert opts.query == "python programming"
        assert opts.verbatim is True
        assert opts.language == "english"
        assert opts.summarize is not None
        assert opts.highlight is not None
        assert opts.sort_by == "score"
        assert opts.sort_ascending is False
        assert opts.n_rows == 100
