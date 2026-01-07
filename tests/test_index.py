"""Tests for RediSearch index management."""

from __future__ import annotations

import polars as pl
import pytest
from polars_redis import (
    GeoField,
    GeoShapeField,
    Index,
    IndexDiff,
    NumericField,
    TagField,
    TextField,
    VectorField,
)


class TestFieldTypes:
    """Test field type classes."""

    def test_text_field_basic(self):
        """Test basic TextField creation."""
        field = TextField("name")
        assert field.name == "name"
        assert field.field_type == "TEXT"
        args = field.to_args()
        assert "name" in args
        assert "TEXT" in args

    def test_text_field_with_options(self):
        """Test TextField with options."""
        field = TextField("title", sortable=True, weight=2.0, nostem=True)
        args = field.to_args()
        assert "SORTABLE" in args
        assert "WEIGHT" in args
        assert "2.0" in args
        assert "NOSTEM" in args

    def test_text_field_phonetic(self):
        """Test TextField with phonetic matching."""
        field = TextField("name", phonetic="dm:en")
        args = field.to_args()
        assert "PHONETIC" in args
        assert "dm:en" in args

    def test_numeric_field_basic(self):
        """Test basic NumericField creation."""
        field = NumericField("age")
        assert field.name == "age"
        assert field.field_type == "NUMERIC"
        args = field.to_args()
        assert args == ["age", "NUMERIC"]

    def test_numeric_field_sortable(self):
        """Test NumericField with sortable."""
        field = NumericField("price", sortable=True)
        args = field.to_args()
        assert "SORTABLE" in args

    def test_tag_field_basic(self):
        """Test basic TagField creation."""
        field = TagField("status")
        assert field.name == "status"
        assert field.field_type == "TAG"
        args = field.to_args()
        assert "TAG" in args

    def test_tag_field_with_separator(self):
        """Test TagField with custom separator."""
        field = TagField("tags", separator="|")
        args = field.to_args()
        assert "SEPARATOR" in args
        assert "|" in args

    def test_tag_field_casesensitive(self):
        """Test TagField with case sensitivity."""
        field = TagField("code", casesensitive=True)
        args = field.to_args()
        assert "CASESENSITIVE" in args

    def test_geo_field(self):
        """Test GeoField creation."""
        field = GeoField("location")
        assert field.name == "location"
        assert field.field_type == "GEO"
        args = field.to_args()
        assert args == ["location", "GEO"]

    def test_vector_field_basic(self):
        """Test basic VectorField creation."""
        field = VectorField("embedding")
        assert field.name == "embedding"
        assert field.field_type == "VECTOR"

    def test_vector_field_with_options(self):
        """Test VectorField with options."""
        field = VectorField(
            "embedding",
            algorithm="HNSW",
            dim=384,
            distance_metric="COSINE",
            m=16,
        )
        args = field.to_args()
        assert "VECTOR" in args
        assert "HNSW" in args
        assert "384" in str(args)
        assert "COSINE" in args

    def test_geoshape_field(self):
        """Test GeoShapeField creation."""
        field = GeoShapeField("boundary")
        assert field.name == "boundary"
        assert field.field_type == "GEOSHAPE"


class TestIndex:
    """Test Index class."""

    def test_index_basic(self):
        """Test basic Index creation."""
        idx = Index(
            name="users_idx",
            prefix="user:",
            schema=[
                TextField("name"),
                NumericField("age"),
            ],
        )
        assert idx.name == "users_idx"
        assert idx.prefix == "user:"
        assert len(idx.schema) == 2

    def test_index_str_representation(self):
        """Test Index string representation (FT.CREATE command)."""
        idx = Index(
            name="test_idx",
            prefix="test:",
            schema=[
                TextField("name", sortable=True),
                NumericField("age"),
            ],
        )
        cmd = str(idx)
        assert "FT.CREATE" in cmd
        assert "test_idx" in cmd
        assert "ON HASH" in cmd
        assert "PREFIX 1 test:" in cmd
        assert "SCHEMA" in cmd
        assert "name TEXT SORTABLE" in cmd
        assert "age NUMERIC" in cmd

    def test_index_json_type(self):
        """Test Index with JSON type."""
        idx = Index(
            name="products_idx",
            prefix="product:",
            schema=[TextField("name")],
            on="JSON",
        )
        cmd = str(idx)
        assert "ON JSON" in cmd

    def test_index_with_stopwords(self):
        """Test Index with custom stopwords."""
        idx = Index(
            name="test_idx",
            prefix="test:",
            schema=[TextField("content")],
            stopwords=["the", "a", "an"],
        )
        cmd = str(idx)
        assert "STOPWORDS 3" in cmd

    def test_index_no_stopwords(self):
        """Test Index with disabled stopwords."""
        idx = Index(
            name="test_idx",
            prefix="test:",
            schema=[TextField("content")],
            stopwords=[],  # Empty list disables stopwords
        )
        cmd = str(idx)
        assert "STOPWORDS 0" in cmd

    def test_index_with_options(self):
        """Test Index with various options."""
        idx = Index(
            name="test_idx",
            prefix="test:",
            schema=[TextField("name")],
            language="english",
            nooffsets=True,
            nofreqs=True,
        )
        cmd = str(idx)
        assert "LANGUAGE english" in cmd
        assert "NOOFFSETS" in cmd
        assert "NOFREQS" in cmd


class TestIndexFromFrame:
    """Test Index.from_frame schema inference."""

    def test_from_frame_basic(self):
        """Test basic schema inference from DataFrame."""
        df = pl.DataFrame(
            {
                "name": ["Alice", "Bob"],
                "age": [30, 25],
                "score": [95.5, 88.0],
                "active": [True, False],
            }
        )
        idx = Index.from_frame(df, "test_idx", "test:")

        # Check field types are inferred correctly
        field_types = {f.name: f.field_type for f in idx.schema}
        assert field_types["name"] == "TAG"  # String defaults to TAG
        assert field_types["age"] == "NUMERIC"
        assert field_types["score"] == "NUMERIC"
        assert field_types["active"] == "TAG"  # Boolean as TAG

    def test_from_frame_text_fields(self):
        """Test specifying TEXT fields."""
        df = pl.DataFrame(
            {
                "title": ["Hello World"],
                "description": ["A test"],
                "category": ["test"],
            }
        )
        idx = Index.from_frame(
            df,
            "test_idx",
            "test:",
            text_fields=["title", "description"],
        )

        field_types = {f.name: f.field_type for f in idx.schema}
        assert field_types["title"] == "TEXT"
        assert field_types["description"] == "TEXT"
        assert field_types["category"] == "TAG"

    def test_from_frame_sortable_fields(self):
        """Test specifying sortable fields."""
        df = pl.DataFrame(
            {
                "name": ["Alice"],
                "age": [30],
            }
        )
        idx = Index.from_frame(
            df,
            "test_idx",
            "test:",
            sortable=["age"],
        )

        # Find age field and check sortable
        age_field = next(f for f in idx.schema if f.name == "age")
        assert age_field.sortable is True


class TestIndexFromSchema:
    """Test Index.from_schema derivation."""

    def test_from_schema_basic(self):
        """Test creating index from schema dict."""
        schema = {
            "name": pl.Utf8,
            "age": pl.Int64,
            "price": pl.Float64,
        }
        idx = Index.from_schema(schema, "test_idx", "test:")

        field_types = {f.name: f.field_type for f in idx.schema}
        assert field_types["name"] == "TAG"
        assert field_types["age"] == "NUMERIC"
        assert field_types["price"] == "NUMERIC"

    def test_from_schema_with_text_fields(self):
        """Test from_schema with TEXT field specification."""
        schema = {
            "title": pl.Utf8,
            "content": pl.Utf8,
            "author": pl.Utf8,
        }
        idx = Index.from_schema(
            schema,
            "test_idx",
            "test:",
            text_fields=["title", "content"],
        )

        field_types = {f.name: f.field_type for f in idx.schema}
        assert field_types["title"] == "TEXT"
        assert field_types["content"] == "TEXT"
        assert field_types["author"] == "TAG"

    def test_from_schema_skips_special_columns(self):
        """Test that special columns (_key, _ttl) are skipped."""
        schema = {
            "_key": pl.Utf8,
            "_ttl": pl.Int64,
            "name": pl.Utf8,
        }
        idx = Index.from_schema(schema, "test_idx", "test:")

        field_names = {f.name for f in idx.schema}
        assert "_key" not in field_names
        assert "_ttl" not in field_names
        assert "name" in field_names


class TestIndexDiff:
    """Test IndexDiff class."""

    def test_diff_no_changes(self):
        """Test diff with no changes."""
        diff = IndexDiff()
        assert not diff.has_changes

    def test_diff_with_added(self):
        """Test diff with added fields."""
        diff = IndexDiff(added=[TextField("new_field")])
        assert diff.has_changes
        assert len(diff.added) == 1

    def test_diff_with_removed(self):
        """Test diff with removed fields."""
        diff = IndexDiff(removed=["old_field"])
        assert diff.has_changes
        assert "old_field" in diff.removed

    def test_diff_with_changed(self):
        """Test diff with changed fields."""
        diff = IndexDiff(changed={"field": ("TEXT", "TAG")})
        assert diff.has_changes
        assert "field" in diff.changed

    def test_diff_str_representation(self):
        """Test diff string representation."""
        diff = IndexDiff(
            added=[TextField("new")],
            removed=["old"],
            changed={"modified": ("TEXT", "NUMERIC")},
            unchanged=["same"],
        )
        s = str(diff)
        assert "+ new (TEXT)" in s
        assert "- old" in s
        assert "~ modified" in s
        assert "same (unchanged)" in s


class TestIndexValidation:
    """Test Index validation methods."""

    def test_validate_schema_match(self):
        """Test validation with matching schema."""
        idx = Index(
            name="test_idx",
            prefix="test:",
            schema=[
                TextField("name"),
                NumericField("age"),
            ],
        )
        schema = {"name": pl.Utf8, "age": pl.Int64}
        warnings = idx.validate_schema(schema)
        assert len(warnings) == 0

    def test_validate_schema_missing_in_index(self):
        """Test validation with field in schema but not index."""
        idx = Index(name="test_idx", prefix="test:", schema=[TextField("name")])
        schema = {"name": pl.Utf8, "age": pl.Int64}
        warnings = idx.validate_schema(schema)
        assert any("age" in w for w in warnings)

    def test_validate_schema_missing_in_schema(self):
        """Test validation with field in index but not schema."""
        idx = Index(
            name="test_idx",
            prefix="test:",
            schema=[
                TextField("name"),
                NumericField("age"),
            ],
        )
        schema = {"name": pl.Utf8}
        warnings = idx.validate_schema(schema)
        assert any("age" in w for w in warnings)

    def test_validate_schema_skips_special(self):
        """Test validation skips special columns."""
        idx = Index(name="test_idx", prefix="test:", schema=[TextField("name")])
        schema = {"_key": pl.Utf8, "_ttl": pl.Int64, "name": pl.Utf8}
        warnings = idx.validate_schema(schema)
        assert len(warnings) == 0


# Integration tests (require Redis with RediSearch)
@pytest.mark.integration
class TestIndexIntegration:
    """Integration tests for Index operations (require Redis)."""

    def test_create_and_drop(self, redis_url):
        """Test creating and dropping an index."""
        idx = Index(
            name="pytest_test_idx",
            prefix="pytest:test:",
            schema=[
                TextField("name", sortable=True),
                NumericField("age"),
            ],
        )

        # Clean up first
        idx.drop(redis_url)

        # Create
        idx.create(redis_url)
        assert idx.exists(redis_url)

        # Drop
        idx.drop(redis_url)
        assert not idx.exists(redis_url)

    def test_ensure_exists_idempotent(self, redis_url):
        """Test ensure_exists is idempotent."""
        idx = Index(name="pytest_ensure_idx", prefix="pytest:ensure:", schema=[TextField("name")])

        # Clean up
        idx.drop(redis_url)

        # First call creates
        idx.ensure_exists(redis_url)
        assert idx.exists(redis_url)

        # Second call is no-op
        idx.ensure_exists(redis_url)
        assert idx.exists(redis_url)

        # Clean up
        idx.drop(redis_url)

    def test_create_if_not_exists(self, redis_url):
        """Test create with if_not_exists flag."""
        idx = Index(name="pytest_ifne_idx", prefix="pytest:ifne:", schema=[TextField("name")])

        # Clean up
        idx.drop(redis_url)

        # Create first time
        idx.create(redis_url, if_not_exists=True)
        assert idx.exists(redis_url)

        # Create again with if_not_exists=True should not raise
        idx.create(redis_url, if_not_exists=True)

        # Clean up
        idx.drop(redis_url)

    def test_from_redis(self, redis_url):
        """Test loading existing index from Redis."""
        idx = Index(
            name="pytest_load_idx",
            prefix="pytest:load:",
            schema=[
                TextField("name"),
                NumericField("age"),
            ],
        )

        # Clean up and create
        idx.drop(redis_url)
        idx.create(redis_url)

        # Load from Redis
        loaded = Index.from_redis(redis_url, "pytest_load_idx")
        assert loaded is not None
        assert loaded.name == "pytest_load_idx"

        # Clean up
        idx.drop(redis_url)

    def test_info(self, redis_url):
        """Test getting index info."""
        idx = Index(name="pytest_info_idx", prefix="pytest:info:", schema=[TextField("name")])

        # Clean up and create
        idx.drop(redis_url)
        idx.create(redis_url)

        # Get info
        info = idx.info(redis_url)
        assert info is not None
        assert info.name == "pytest_info_idx"

        # Clean up
        idx.drop(redis_url)
