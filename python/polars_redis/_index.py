"""RediSearch Index management for polars-redis.

This module provides typed helpers for creating and managing RediSearch indexes,
eliminating the need to write raw FT.CREATE commands.

Example:
    >>> from polars_redis import Index, TextField, NumericField, TagField
    >>>
    >>> # Define an index
    >>> idx = Index(
    ...     name="users_idx",
    ...     prefix="user:",
    ...     schema=[
    ...         TextField("name", sortable=True),
    ...         NumericField("age", sortable=True),
    ...         TagField("status"),
    ...     ]
    ... )
    >>>
    >>> # Create the index
    >>> idx.create("redis://localhost:6379")
    >>>
    >>> # Or use with search_hashes for auto-create
    >>> from polars_redis import search_hashes, col
    >>> df = search_hashes(
    ...     "redis://localhost:6379",
    ...     index=idx,  # Auto-creates if not exists
    ...     query=col("age") > 30,
    ...     schema={"name": pl.Utf8, "age": pl.Int64, "status": pl.Utf8},
    ... ).collect()
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

import redis

if TYPE_CHECKING:
    import polars as pl
    from polars import DataFrame


# =============================================================================
# Field Types
# =============================================================================


class Field(ABC):
    """Base class for RediSearch field types."""

    @abstractmethod
    def to_args(self) -> list[str]:
        """Convert field to FT.CREATE arguments."""
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """Field name."""
        ...

    @property
    @abstractmethod
    def field_type(self) -> str:
        """RediSearch field type name."""
        ...


@dataclass
class TextField(Field):
    """A TEXT field for full-text search.

    TEXT fields support full-text search with stemming, phonetic matching,
    and relevance scoring.

    Args:
        name: Field name in the hash/JSON document.
        sortable: Enable sorting on this field (uses additional memory).
        nostem: Disable stemming for this field.
        weight: Relevance weight for scoring (default 1.0).
        phonetic: Phonetic algorithm for fuzzy matching (dm:en, dm:fr, dm:pt, dm:es).
        noindex: Store field but don't index it (can still be returned/sorted).
        withsuffixtrie: Enable suffix queries (*word) - uses more memory.

    Example:
        >>> TextField("title", sortable=True, weight=2.0)
        >>> TextField("body", nostem=True)
        >>> TextField("name", phonetic="dm:en")  # Double Metaphone English
    """

    _name: str
    sortable: bool = False
    nostem: bool = False
    weight: float = 1.0
    phonetic: str | None = None
    noindex: bool = False
    withsuffixtrie: bool = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def field_type(self) -> str:
        return "TEXT"

    def to_args(self) -> list[str]:
        args = [self._name, "TEXT"]
        if self.nostem:
            args.append("NOSTEM")
        if self.weight != 1.0:
            args.extend(["WEIGHT", str(self.weight)])
        if self.phonetic:
            args.extend(["PHONETIC", self.phonetic])
        if self.sortable:
            args.append("SORTABLE")
        if self.noindex:
            args.append("NOINDEX")
        if self.withsuffixtrie:
            args.append("WITHSUFFIXTRIE")
        return args


@dataclass
class NumericField(Field):
    """A NUMERIC field for numeric range queries.

    NUMERIC fields support range queries like @price:[100 500].

    Args:
        name: Field name in the hash/JSON document.
        sortable: Enable sorting on this field.
        noindex: Store field but don't index it.

    Example:
        >>> NumericField("price", sortable=True)
        >>> NumericField("age")
    """

    _name: str
    sortable: bool = False
    noindex: bool = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def field_type(self) -> str:
        return "NUMERIC"

    def to_args(self) -> list[str]:
        args = [self._name, "NUMERIC"]
        if self.sortable:
            args.append("SORTABLE")
        if self.noindex:
            args.append("NOINDEX")
        return args


@dataclass
class TagField(Field):
    """A TAG field for exact-match filtering.

    TAG fields are optimized for exact matches and multi-value fields
    (like categories, tags, statuses). Values are not tokenized or stemmed.

    Args:
        name: Field name in the hash/JSON document.
        separator: Character separating multiple values (default ",").
        casesensitive: Enable case-sensitive matching (default False).
        sortable: Enable sorting on this field.
        noindex: Store field but don't index it.
        withsuffixtrie: Enable suffix queries.

    Example:
        >>> TagField("status")  # Single value
        >>> TagField("tags", separator=",")  # Comma-separated values
        >>> TagField("category", casesensitive=True)
    """

    _name: str
    separator: str = ","
    casesensitive: bool = False
    sortable: bool = False
    noindex: bool = False
    withsuffixtrie: bool = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def field_type(self) -> str:
        return "TAG"

    def to_args(self) -> list[str]:
        args = [self._name, "TAG"]
        if self.separator != ",":
            args.extend(["SEPARATOR", self.separator])
        if self.casesensitive:
            args.append("CASESENSITIVE")
        if self.sortable:
            args.append("SORTABLE")
        if self.noindex:
            args.append("NOINDEX")
        if self.withsuffixtrie:
            args.append("WITHSUFFIXTRIE")
        return args


@dataclass
class GeoField(Field):
    """A GEO field for geographic queries.

    GEO fields support radius and polygon queries. Values should be
    stored as "longitude,latitude" strings.

    Args:
        name: Field name in the hash/JSON document.
        noindex: Store field but don't index it.

    Example:
        >>> GeoField("location")
        >>> # Store as: HSET key location "-122.4194,37.7749"
    """

    _name: str
    noindex: bool = False

    @property
    def name(self) -> str:
        return self._name

    @property
    def field_type(self) -> str:
        return "GEO"

    def to_args(self) -> list[str]:
        args = [self._name, "GEO"]
        if self.noindex:
            args.append("NOINDEX")
        return args


@dataclass
class VectorField(Field):
    """A VECTOR field for similarity search.

    VECTOR fields enable k-nearest-neighbor (KNN) and range queries
    on embedding vectors.

    Args:
        name: Field name in the hash/JSON document.
        algorithm: Indexing algorithm - "FLAT" (brute force) or "HNSW" (approximate).
        dim: Vector dimension (must match your embeddings).
        distance_metric: Distance function - "COSINE", "L2", "IP" (inner product).
        initial_cap: Initial index capacity (HNSW only).
        m: Number of edges per node (HNSW only, default 16).
        ef_construction: Construction-time search width (HNSW only, default 200).
        ef_runtime: Query-time search width (HNSW only, default 10).
        block_size: Block size for FLAT algorithm.

    Example:
        >>> # For OpenAI embeddings (1536 dim)
        >>> VectorField("embedding", algorithm="HNSW", dim=1536, distance_metric="COSINE")
        >>>
        >>> # For sentence-transformers (384 dim)
        >>> VectorField("embedding", algorithm="HNSW", dim=384, distance_metric="COSINE", m=32)
    """

    _name: str
    algorithm: Literal["FLAT", "HNSW"] = "HNSW"
    dim: int = 384
    distance_metric: Literal["COSINE", "L2", "IP"] = "COSINE"
    initial_cap: int | None = None
    m: int | None = None
    ef_construction: int | None = None
    ef_runtime: int | None = None
    block_size: int | None = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def field_type(self) -> str:
        return "VECTOR"

    def to_args(self) -> list[str]:
        args = [self._name, "VECTOR", self.algorithm]

        # Count attributes for the attribute count
        attrs: list[str] = []
        attrs.extend(["TYPE", "FLOAT32"])
        attrs.extend(["DIM", str(self.dim)])
        attrs.extend(["DISTANCE_METRIC", self.distance_metric])

        if self.initial_cap is not None:
            attrs.extend(["INITIAL_CAP", str(self.initial_cap)])

        if self.algorithm == "HNSW":
            if self.m is not None:
                attrs.extend(["M", str(self.m)])
            if self.ef_construction is not None:
                attrs.extend(["EF_CONSTRUCTION", str(self.ef_construction)])
            if self.ef_runtime is not None:
                attrs.extend(["EF_RUNTIME", str(self.ef_runtime)])
        elif self.algorithm == "FLAT":
            if self.block_size is not None:
                attrs.extend(["BLOCK_SIZE", str(self.block_size)])

        args.append(str(len(attrs)))
        args.extend(attrs)
        return args


@dataclass
class GeoShapeField(Field):
    """A GEOSHAPE field for polygon and complex geometry queries.

    GEOSHAPE fields support polygon containment and intersection queries
    using WKT (Well-Known Text) format.

    Args:
        name: Field name in the hash/JSON document.
        coord_system: Coordinate system - "SPHERICAL" (default) or "FLAT".

    Example:
        >>> GeoShapeField("boundary")
        >>> # Store as WKT: HSET key boundary "POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))"
    """

    _name: str
    coord_system: Literal["SPHERICAL", "FLAT"] = "SPHERICAL"

    @property
    def name(self) -> str:
        return self._name

    @property
    def field_type(self) -> str:
        return "GEOSHAPE"

    def to_args(self) -> list[str]:
        args = [self._name, "GEOSHAPE"]
        if self.coord_system != "SPHERICAL":
            args.extend(["COORD_SYSTEM", self.coord_system])
        return args


# =============================================================================
# Index Class
# =============================================================================


@dataclass
class IndexDiff:
    """Represents differences between desired and existing index schemas.

    Attributes:
        added: Fields that will be added.
        removed: Fields that will be removed.
        changed: Fields that have changed (field_name -> (old, new)).
        unchanged: Fields that are the same.
    """

    added: list[Field] = field(default_factory=list)
    removed: list[str] = field(default_factory=list)
    changed: dict[str, tuple[str, str]] = field(default_factory=dict)
    unchanged: list[str] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        """True if there are any differences."""
        return bool(self.added or self.removed or self.changed)

    def __str__(self) -> str:
        lines = []
        for f in self.added:
            lines.append(f"+ {f.name} ({f.field_type})")
        for name in self.removed:
            lines.append(f"- {name}")
        for name, (old, new) in self.changed.items():
            lines.append(f"~ {name}: {old} -> {new}")
        for name in self.unchanged:
            lines.append(f"  {name} (unchanged)")
        return "\n".join(lines) if lines else "(no differences)"


@dataclass
class IndexInfo:
    """Information about an existing RediSearch index."""

    name: str
    num_docs: int
    max_doc_id: int
    num_terms: int
    num_records: int
    inverted_sz_mb: float
    total_inverted_index_blocks: int
    offset_vectors_sz_mb: float
    doc_table_size_mb: float
    sortable_values_size_mb: float
    key_table_size_mb: float
    records_per_doc_avg: float
    bytes_per_record_avg: float
    offsets_per_term_avg: float
    offset_bits_per_record_avg: float
    hash_indexing_failures: int
    indexing: bool
    percent_indexed: float
    fields: list[dict[str, Any]]
    prefixes: list[str]
    on_type: str  # HASH or JSON

    @classmethod
    def from_redis_info(cls, info: dict[str, Any]) -> IndexInfo:
        """Create IndexInfo from FT.INFO response."""
        # Parse index_definition which may be a list of key-value pairs or a dict
        index_def = info.get("index_definition", {})
        if isinstance(index_def, list):
            # Convert flat list [key1, val1, key2, val2, ...] to dict
            index_def_dict: dict[str, Any] = {}
            for i in range(0, len(index_def), 2):
                if i + 1 < len(index_def):
                    index_def_dict[index_def[i]] = index_def[i + 1]
            index_def = index_def_dict

        prefixes = index_def.get("prefixes", [])
        on_type = index_def.get("key_type", "HASH")

        # Handle bytes_per_record_avg which may be 'nan'
        bytes_per_record = info.get("bytes_per_record_avg", 0)
        if bytes_per_record == "nan" or bytes_per_record == "-nan":
            bytes_per_record = 0.0

        return cls(
            name=info.get("index_name", ""),
            num_docs=int(info.get("num_docs", 0)),
            max_doc_id=int(info.get("max_doc_id", 0)),
            num_terms=int(info.get("num_terms", 0)),
            num_records=int(info.get("num_records", 0)),
            inverted_sz_mb=float(info.get("inverted_sz_mb", 0)),
            total_inverted_index_blocks=int(info.get("total_inverted_index_blocks", 0)),
            offset_vectors_sz_mb=float(info.get("offset_vectors_sz_mb", 0)),
            doc_table_size_mb=float(info.get("doc_table_size_mb", 0)),
            sortable_values_size_mb=float(info.get("sortable_values_size_mb", 0)),
            key_table_size_mb=float(info.get("key_table_size_mb", 0)),
            records_per_doc_avg=float(info.get("records_per_doc_avg", 0)),
            bytes_per_record_avg=float(bytes_per_record),
            offsets_per_term_avg=float(info.get("offsets_per_term_avg", 0)),
            offset_bits_per_record_avg=float(info.get("offset_bits_per_record_avg", 0)),
            hash_indexing_failures=int(info.get("hash_indexing_failures", 0)),
            indexing=info.get("indexing", "0") == "1",
            percent_indexed=float(info.get("percent_indexed", 1.0)),
            fields=info.get("attributes", []),
            prefixes=prefixes,
            on_type=on_type,
        )


@dataclass
class Index:
    """A RediSearch index definition.

    This class provides a typed interface for creating and managing RediSearch
    indexes, eliminating the need to write raw FT.CREATE commands.

    Args:
        name: Index name (e.g., "users_idx").
        prefix: Key prefix to index (e.g., "user:"). Can be a list for multiple prefixes.
        schema: List of field definitions.
        on: Data type to index - "HASH" (default) or "JSON".
        stopwords: Custom stopwords list. Use empty list to disable stopwords.
        language: Default language for stemming.
        language_field: Field containing per-document language.
        score: Default score for documents.
        score_field: Field containing per-document score.
        payload_field: Field to use as document payload.
        maxtextfields: Optimize for many TEXT fields.
        nooffsets: Don't store term offsets (saves memory, disables exact phrase search).
        nohl: Don't store data for highlighting.
        nofields: Don't store field names (saves memory).
        nofreqs: Don't store term frequencies (saves memory, affects scoring).
        skipinitialscan: Don't scan existing keys when creating index.

    Example:
        >>> idx = Index(
        ...     name="products_idx",
        ...     prefix="product:",
        ...     schema=[
        ...         TextField("name", sortable=True, weight=2.0),
        ...         NumericField("price", sortable=True),
        ...         TagField("category"),
        ...         VectorField("embedding", dim=384),
        ...     ]
        ... )
        >>> idx.create("redis://localhost:6379")
    """

    name: str
    prefix: str | list[str] = ""
    schema: list[Field] = field(default_factory=list)
    on: Literal["HASH", "JSON"] = "HASH"
    stopwords: list[str] | None = None
    language: str | None = None
    language_field: str | None = None
    score: float | None = None
    score_field: str | None = None
    payload_field: str | None = None
    maxtextfields: bool = False
    nooffsets: bool = False
    nohl: bool = False
    nofields: bool = False
    nofreqs: bool = False
    skipinitialscan: bool = False

    def _get_client(self, url: str) -> redis.Redis:
        """Get a Redis client from URL."""
        return redis.from_url(url, decode_responses=True)

    def _build_create_args(self) -> list[str]:
        """Build the FT.CREATE command arguments."""
        args = [self.name]

        # ON HASH/JSON
        args.extend(["ON", self.on])

        # PREFIX
        prefixes = [self.prefix] if isinstance(self.prefix, str) else self.prefix
        if prefixes and prefixes[0]:
            args.append("PREFIX")
            args.append(str(len(prefixes)))
            args.extend(prefixes)

        # Index options
        if self.language:
            args.extend(["LANGUAGE", self.language])
        if self.language_field:
            args.extend(["LANGUAGE_FIELD", self.language_field])
        if self.score is not None:
            args.extend(["SCORE", str(self.score)])
        if self.score_field:
            args.extend(["SCORE_FIELD", self.score_field])
        if self.payload_field:
            args.extend(["PAYLOAD_FIELD", self.payload_field])
        if self.maxtextfields:
            args.append("MAXTEXTFIELDS")
        if self.nooffsets:
            args.append("NOOFFSETS")
        if self.nohl:
            args.append("NOHL")
        if self.nofields:
            args.append("NOFIELDS")
        if self.nofreqs:
            args.append("NOFREQS")
        if self.skipinitialscan:
            args.append("SKIPINITIALSCAN")
        if self.stopwords is not None:
            args.append("STOPWORDS")
            args.append(str(len(self.stopwords)))
            args.extend(self.stopwords)

        # SCHEMA
        args.append("SCHEMA")
        for f in self.schema:
            args.extend(f.to_args())

        return args

    def create(self, url: str, *, if_not_exists: bool = False) -> None:
        """Create the index in Redis.

        Args:
            url: Redis connection URL.
            if_not_exists: If True, don't raise error if index exists.

        Raises:
            redis.ResponseError: If index already exists (unless if_not_exists=True).
        """
        client = self._get_client(url)
        args = self._build_create_args()

        try:
            client.execute_command("FT.CREATE", *args)
        except redis.ResponseError as e:
            if "Index already exists" in str(e) and if_not_exists:
                return
            raise

    def drop(self, url: str, *, delete_docs: bool = False) -> None:
        """Drop the index.

        Args:
            url: Redis connection URL.
            delete_docs: If True, also delete the indexed documents.
        """
        client = self._get_client(url)
        args = [self.name]
        if delete_docs:
            args.append("DD")
        try:
            client.execute_command("FT.DROPINDEX", *args)
        except redis.ResponseError as e:
            if "unknown index name" not in str(e).lower():
                raise

    def exists(self, url: str) -> bool:
        """Check if the index exists.

        Args:
            url: Redis connection URL.

        Returns:
            True if the index exists, False otherwise.
        """
        client = self._get_client(url)
        try:
            client.execute_command("FT.INFO", self.name)
            return True
        except redis.ResponseError:
            return False

    def ensure_exists(self, url: str, *, recreate: bool = False) -> Index:
        """Ensure the index exists, creating it if necessary.

        This is idempotent and safe for concurrent access.

        Args:
            url: Redis connection URL.
            recreate: If True, drop and recreate the index.

        Returns:
            Self, for method chaining.

        Example:
            >>> idx.ensure_exists(url)
            >>> # Or chain with search
            >>> df = search_hashes(url, index=idx.ensure_exists(url), ...)
        """
        if recreate:
            self.drop(url)
            self.create(url)
        else:
            self.create(url, if_not_exists=True)
        return self

    def info(self, url: str) -> IndexInfo | None:
        """Get information about the index.

        Args:
            url: Redis connection URL.

        Returns:
            IndexInfo object, or None if index doesn't exist.
        """
        client = self._get_client(url)
        try:
            # FT.INFO returns alternating key-value pairs
            result = client.execute_command("FT.INFO", self.name)
            # Convert list to dict
            info_dict = {}
            if isinstance(result, list):
                it = iter(result)
                for key in it:
                    value = next(it, None)
                    if isinstance(key, (str, bytes)):
                        key_str = key.decode() if isinstance(key, bytes) else key
                        info_dict[key_str] = value
            return IndexInfo.from_redis_info(info_dict)
        except redis.ResponseError:
            return None

    def diff(self, url: str) -> IndexDiff:
        """Compare this index definition to an existing index.

        Args:
            url: Redis connection URL.

        Returns:
            IndexDiff showing differences between desired and existing schema.
        """
        info = self.info(url)
        if info is None:
            # Index doesn't exist, all fields are "added"
            return IndexDiff(added=list(self.schema))

        # Build map of existing fields
        existing: dict[str, str] = {}
        for field_info in info.fields:
            if isinstance(field_info, (list, tuple)):
                # Format: [name, type, ...]
                name = field_info[0] if field_info else ""
                ftype = field_info[2] if len(field_info) > 2 else ""
                existing[name] = ftype
            elif isinstance(field_info, dict):
                existing[field_info.get("identifier", "")] = field_info.get("type", "")

        # Compare
        diff = IndexDiff()
        desired_names = {f.name for f in self.schema}

        for f in self.schema:
            if f.name not in existing:
                diff.added.append(f)
            elif existing[f.name] != f.field_type:
                diff.changed[f.name] = (existing[f.name], f.field_type)
            else:
                diff.unchanged.append(f.name)

        for name in existing:
            if name not in desired_names:
                diff.removed.append(name)

        return diff

    def migrate(self, url: str, *, drop_existing: bool = False) -> bool:
        """Migrate the index to match the desired schema.

        Note: RediSearch doesn't support ALTER for most changes, so migration
        typically requires dropping and recreating the index. This will
        re-index all documents.

        Args:
            url: Redis connection URL.
            drop_existing: Must be True to perform destructive migration.

        Returns:
            True if migration was performed, False if no changes needed.

        Raises:
            ValueError: If changes are needed but drop_existing is False.
        """
        diff = self.diff(url)
        if not diff.has_changes:
            return False

        if not drop_existing:
            raise ValueError(
                f"Index migration requires dropping the existing index. "
                f"Changes needed:\n{diff}\n"
                f"Pass drop_existing=True to proceed."
            )

        self.drop(url)
        self.create(url)
        return True

    def validate_schema(self, schema: dict[str, Any]) -> list[str]:
        """Validate a Polars schema dict against this index.

        Args:
            schema: Dictionary mapping field names to Polars dtypes.

        Returns:
            List of validation warnings/errors (empty if valid).
        """
        warnings = []
        index_fields = {f.name for f in self.schema}

        for field_name in schema:
            if field_name.startswith("_"):
                # Skip special columns like _key, _ttl
                continue
            if field_name not in index_fields:
                warnings.append(f"Field '{field_name}' in schema but not in index")

        for f in self.schema:
            if f.name not in schema:
                warnings.append(f"Index field '{f.name}' not in schema")

        return warnings

    @classmethod
    def from_frame(
        cls,
        df: DataFrame,
        name: str,
        prefix: str = "",
        *,
        text_fields: list[str] | None = None,
        sortable: list[str] | None = None,
        on: Literal["HASH", "JSON"] = "HASH",
    ) -> Index:
        """Infer an index schema from a DataFrame.

        By default, string columns become TAG fields (exact match).
        Use text_fields to specify which should be TEXT fields (full-text search).

        Args:
            df: DataFrame to infer schema from.
            name: Index name.
            prefix: Key prefix.
            text_fields: Column names that should be TEXT fields (default: TAG).
            sortable: Column names that should be sortable.
            on: Data type - "HASH" or "JSON".

        Returns:
            An Index with inferred schema.

        Example:
            >>> df = pl.DataFrame({
            ...     "name": ["Alice", "Bob"],
            ...     "age": [30, 25],
            ...     "status": ["active", "inactive"],
            ... })
            >>> idx = Index.from_frame(df, "users_idx", "user:", text_fields=["name"])
        """
        import polars as pl

        text_fields = set(text_fields or [])
        sortable = set(sortable or [])
        schema: list[Field] = []

        for col_name, dtype in df.schema.items():
            is_sortable = col_name in sortable

            if dtype in (
                pl.Int8,
                pl.Int16,
                pl.Int32,
                pl.Int64,
                pl.UInt8,
                pl.UInt16,
                pl.UInt32,
                pl.UInt64,
                pl.Float32,
                pl.Float64,
            ):
                schema.append(NumericField(col_name, sortable=is_sortable))
            elif dtype in (pl.Utf8, pl.String):
                if col_name in text_fields:
                    schema.append(TextField(col_name, sortable=is_sortable))
                else:
                    schema.append(TagField(col_name, sortable=is_sortable))
            elif dtype == pl.Boolean:
                schema.append(TagField(col_name, sortable=is_sortable))
            # Skip other types (Date, Datetime, List, etc.) - not directly indexable

        return cls(name=name, prefix=prefix, schema=schema, on=on)

    @classmethod
    def from_schema(
        cls,
        schema: dict[str, Any],
        name: str,
        prefix: str = "",
        *,
        text_fields: list[str] | None = None,
        sortable: list[str] | None = None,
        on: Literal["HASH", "JSON"] = "HASH",
    ) -> Index:
        """Create an Index from a Polars schema dictionary.

        This is useful when you already have a schema dict (e.g., from scan_hashes)
        and want to create a matching index.

        Args:
            schema: Dictionary mapping field names to Polars dtypes.
            name: Index name.
            prefix: Key prefix.
            text_fields: Column names that should be TEXT fields.
            sortable: Column names that should be sortable.
            on: Data type - "HASH" or "JSON".

        Returns:
            An Index with the specified schema.

        Example:
            >>> schema = {"name": pl.Utf8, "age": pl.Int64, "status": pl.Utf8}
            >>> idx = Index.from_schema(schema, "users_idx", "user:", text_fields=["name"])
        """
        import polars as pl

        text_fields_set = set(text_fields or [])
        sortable_set = set(sortable or [])
        fields: list[Field] = []

        for col_name, dtype in schema.items():
            # Skip special columns
            if col_name.startswith("_"):
                continue

            is_sortable = col_name in sortable_set

            # Handle dtype comparison
            if dtype in (
                pl.Int8,
                pl.Int16,
                pl.Int32,
                pl.Int64,
                pl.UInt8,
                pl.UInt16,
                pl.UInt32,
                pl.UInt64,
                pl.Float32,
                pl.Float64,
            ):
                fields.append(NumericField(col_name, sortable=is_sortable))
            elif dtype in (pl.Utf8, pl.String):
                if col_name in text_fields_set:
                    fields.append(TextField(col_name, sortable=is_sortable))
                else:
                    fields.append(TagField(col_name, sortable=is_sortable))
            elif dtype == pl.Boolean:
                fields.append(TagField(col_name, sortable=is_sortable))

        return cls(name=name, prefix=prefix, schema=fields, on=on)

    @classmethod
    def from_redis(cls, url: str, name: str) -> Index | None:
        """Load an existing index definition from Redis.

        Args:
            url: Redis connection URL.
            name: Index name.

        Returns:
            Index object, or None if index doesn't exist.
        """
        client = redis.from_url(url, decode_responses=True)
        try:
            result = client.execute_command("FT.INFO", name)
        except redis.ResponseError:
            return None

        # Parse FT.INFO result
        info_dict: dict[str, Any] = {}
        if isinstance(result, list):
            it = iter(result)
            for key in it:
                value = next(it, None)
                if isinstance(key, (str, bytes)):
                    key_str = key.decode() if isinstance(key, bytes) else key
                    info_dict[key_str] = value

        # Extract prefix
        index_def = info_dict.get("index_definition", {})
        if isinstance(index_def, list):
            # Convert list to dict
            def_dict: dict[str, Any] = {}
            it = iter(index_def)
            for key in it:
                value = next(it, None)
                if isinstance(key, (str, bytes)):
                    key_str = key.decode() if isinstance(key, bytes) else key
                    def_dict[key_str] = value
            index_def = def_dict

        prefixes = index_def.get("prefixes", [])
        prefix = prefixes[0] if prefixes else ""
        on_type = index_def.get("key_type", "HASH")

        # Parse fields
        fields: list[Field] = []
        attributes = info_dict.get("attributes", [])
        for attr in attributes:
            if isinstance(attr, (list, tuple)) and len(attr) >= 3:
                field_name = attr[0]
                # attr[1] is usually "identifier" or "attribute"
                field_type = attr[2] if len(attr) > 2 else ""

                if field_type == "TEXT":
                    fields.append(TextField(field_name))
                elif field_type == "NUMERIC":
                    fields.append(NumericField(field_name))
                elif field_type == "TAG":
                    fields.append(TagField(field_name))
                elif field_type == "GEO":
                    fields.append(GeoField(field_name))
                elif field_type == "VECTOR":
                    fields.append(VectorField(field_name))
                elif field_type == "GEOSHAPE":
                    fields.append(GeoShapeField(field_name))

        return cls(
            name=name,
            prefix=prefix,
            schema=fields,
            on=on_type,
        )

    def __str__(self) -> str:
        """String representation showing the FT.CREATE command."""
        args = self._build_create_args()
        return f"FT.CREATE {' '.join(args)}"

    def __repr__(self) -> str:
        """Debug representation."""
        return f"Index(name={self.name!r}, prefix={self.prefix!r}, fields={len(self.schema)})"
