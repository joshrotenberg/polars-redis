"""Schema inference functions for polars-redis.

This module contains functions for inferring schema from Redis data
by sampling keys and analyzing their structure.
"""

from __future__ import annotations

import polars as pl

from polars_redis._internal import (
    py_infer_hash_schema,
    py_infer_hash_schema_with_confidence,
    py_infer_hash_schema_with_overwrite,
    py_infer_json_schema,
    py_infer_json_schema_with_overwrite,
)
from polars_redis._utils import _fields_to_schema, _schema_to_overwrite


class SchemaConfidence:
    """Schema inference result with confidence information.

    This class wraps the result of schema inference with detailed
    confidence scores for each field, allowing you to assess the
    quality of type inference before using the schema.

    Attributes:
        schema: The inferred schema as a dict of field names to Polars types.
        sample_count: Number of keys that were sampled.
        field_info: Dict of field names to detailed inference info.
        average_confidence: Average confidence score across all fields.
        all_confident: Whether all fields have confidence >= 0.9.
    """

    def __init__(self, result: dict):
        """Initialize from the raw result dict."""
        self._result = result
        self._schema = _fields_to_schema(result["fields"])

    @property
    def schema(self) -> dict[str, type[pl.DataType]]:
        """Get the inferred schema."""
        return self._schema

    @property
    def sample_count(self) -> int:
        """Get the number of keys sampled."""
        return self._result["sample_count"]

    @property
    def field_info(self) -> dict:
        """Get detailed inference info for each field.

        Returns a dict mapping field names to info dicts containing:
        - type: Inferred type name
        - confidence: Score from 0.0 to 1.0
        - samples: Total samples for this field
        - valid: Number matching the inferred type
        - nulls: Number of null/missing values
        - null_ratio: Percentage of nulls
        - type_candidates: Dict of type names to match counts
        """
        return self._result["field_info"]

    @property
    def average_confidence(self) -> float:
        """Get the average confidence score across all fields."""
        return self._result["average_confidence"]

    @property
    def all_confident(self) -> bool:
        """Check if all fields have confidence >= 0.9."""
        return self._result["all_confident"]

    def low_confidence_fields(self, threshold: float = 0.9) -> list[tuple[str, float]]:
        """Get fields with confidence below a threshold.

        Args:
            threshold: Confidence threshold (default: 0.9).

        Returns:
            List of (field_name, confidence) tuples.
        """
        return [
            (name, info["confidence"])
            for name, info in self.field_info.items()
            if info["confidence"] < threshold
        ]

    def __repr__(self) -> str:
        """Return a string representation."""
        return (
            f"SchemaConfidence(fields={len(self._schema)}, "
            f"samples={self.sample_count}, "
            f"avg_confidence={self.average_confidence:.2%})"
        )


def infer_hash_schema(
    url: str,
    pattern: str = "*",
    *,
    sample_size: int = 100,
    type_inference: bool = True,
) -> dict[str, type[pl.DataType]]:
    """Infer schema from Redis hashes by sampling keys.

    Samples keys matching the pattern and infers field names and types
    from the hash values. This is useful for discovering the schema
    when you don't know it ahead of time.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        pattern: Key pattern to match (e.g., "user:*").
        sample_size: Maximum number of keys to sample (default: 100).
        type_inference: Whether to infer types (default: True).
            If False, all fields will be Utf8.

    Returns:
        A dictionary mapping field names to Polars dtypes, suitable
        for passing to scan_hashes() or read_hashes().

    Example:
        >>> schema = infer_hash_schema(
        ...     "redis://localhost:6379",
        ...     pattern="user:*",
        ...     sample_size=50
        ... )
        >>> print(schema)
        {'name': Utf8, 'age': Int64, 'email': Utf8}
        >>> df = read_hashes(
        ...     "redis://localhost:6379",
        ...     pattern="user:*",
        ...     schema=schema
        ... )
    """
    fields, _ = py_infer_hash_schema(url, pattern, sample_size, type_inference)
    return _fields_to_schema(fields)


def infer_json_schema(
    url: str,
    pattern: str = "*",
    *,
    sample_size: int = 100,
) -> dict[str, type[pl.DataType]]:
    """Infer schema from RedisJSON documents by sampling keys.

    Samples keys matching the pattern and infers field names and types
    from the JSON document structure. This is useful for discovering
    the schema when you don't know it ahead of time.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        pattern: Key pattern to match (e.g., "doc:*").
        sample_size: Maximum number of keys to sample (default: 100).

    Returns:
        A dictionary mapping field names to Polars dtypes, suitable
        for passing to scan_json() or read_json().

    Example:
        >>> schema = infer_json_schema(
        ...     "redis://localhost:6379",
        ...     pattern="doc:*",
        ...     sample_size=50
        ... )
        >>> print(schema)
        {'title': Utf8, 'views': Int64, 'rating': Float64}
        >>> df = read_json(
        ...     "redis://localhost:6379",
        ...     pattern="doc:*",
        ...     schema=schema
        ... )
    """
    fields, _ = py_infer_json_schema(url, pattern, sample_size)
    return _fields_to_schema(fields)


def infer_hash_schema_with_confidence(
    url: str,
    pattern: str = "*",
    *,
    sample_size: int = 100,
) -> SchemaConfidence:
    """Infer schema from Redis hashes with detailed confidence information.

    This function returns confidence scores for each field, indicating how
    reliably the type was inferred. Use this when you need to validate
    schema quality before processing large datasets.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        pattern: Key pattern to match (e.g., "user:*").
        sample_size: Maximum number of keys to sample (default: 100).

    Returns:
        A SchemaConfidence object with the inferred schema and confidence data.

    Example:
        >>> result = infer_hash_schema_with_confidence(
        ...     "redis://localhost:6379",
        ...     pattern="user:*",
        ...     sample_size=100
        ... )
        >>> print(result.average_confidence)
        0.95
        >>> if result.all_confident:
        ...     df = read_hashes(url, pattern, schema=result.schema)
        ... else:
        ...     # Check low confidence fields
        ...     for field, conf in result.low_confidence_fields():
        ...         print(f"Warning: {field} has {conf:.0%} confidence")
    """
    result = py_infer_hash_schema_with_confidence(url, pattern, sample_size)
    return SchemaConfidence(result)


def infer_hash_schema_with_overwrite(
    url: str,
    pattern: str = "*",
    *,
    schema_overwrite: dict | None = None,
    sample_size: int = 100,
    type_inference: bool = True,
) -> dict[str, type[pl.DataType]]:
    """Infer schema from Redis hashes with optional type overrides.

    This function infers a schema by sampling Redis hashes, then applies
    user-specified type overrides. This is useful when you want to infer
    most fields but explicitly set types for specific ones.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        pattern: Key pattern to match (e.g., "user:*").
        schema_overwrite: Dictionary mapping field names to Polars dtypes
            that override inferred types. Fields not in inferred schema
            will be added.
        sample_size: Maximum number of keys to sample (default: 100).
        type_inference: Whether to infer types (default: True).
            If False, all fields will be Utf8 before overwrites are applied.

    Returns:
        A dictionary mapping field names to Polars dtypes, suitable
        for passing to scan_hashes() or read_hashes().

    Example:
        >>> # Infer schema but force 'age' to Int64 and 'created_at' to Datetime
        >>> schema = infer_hash_schema_with_overwrite(
        ...     "redis://localhost:6379",
        ...     pattern="user:*",
        ...     schema_overwrite={"age": pl.Int64, "created_at": pl.Datetime}
        ... )
        >>> print(schema)
        {'age': Int64, 'created_at': Datetime, 'email': Utf8, 'name': Utf8}
        >>> df = read_hashes(
        ...     "redis://localhost:6379",
        ...     pattern="user:*",
        ...     schema=schema
        ... )
    """
    overwrite_list = None
    if schema_overwrite is not None:
        overwrite_list = _schema_to_overwrite(schema_overwrite)

    fields, _ = py_infer_hash_schema_with_overwrite(
        url, pattern, overwrite_list, sample_size, type_inference
    )
    return _fields_to_schema(fields)


def infer_json_schema_with_overwrite(
    url: str,
    pattern: str = "*",
    *,
    schema_overwrite: dict | None = None,
    sample_size: int = 100,
) -> dict[str, type[pl.DataType]]:
    """Infer schema from RedisJSON documents with optional type overrides.

    This function infers a schema by sampling RedisJSON documents, then applies
    user-specified type overrides. This is useful when you want to infer
    most fields but explicitly set types for specific ones.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        pattern: Key pattern to match (e.g., "doc:*").
        schema_overwrite: Dictionary mapping field names to Polars dtypes
            that override inferred types. Fields not in inferred schema
            will be added.
        sample_size: Maximum number of keys to sample (default: 100).

    Returns:
        A dictionary mapping field names to Polars dtypes, suitable
        for passing to scan_json() or read_json().

    Example:
        >>> # Infer schema but force 'timestamp' to Datetime
        >>> schema = infer_json_schema_with_overwrite(
        ...     "redis://localhost:6379",
        ...     pattern="doc:*",
        ...     schema_overwrite={"timestamp": pl.Datetime}
        ... )
        >>> df = read_json(
        ...     "redis://localhost:6379",
        ...     pattern="doc:*",
        ...     schema=schema
        ... )
    """
    overwrite_list = None
    if schema_overwrite is not None:
        overwrite_list = _schema_to_overwrite(schema_overwrite)

    fields, _ = py_infer_json_schema_with_overwrite(url, pattern, overwrite_list, sample_size)
    return _fields_to_schema(fields)
