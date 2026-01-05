"""Internal utility functions for polars-redis.

This module contains internal helper functions for type conversion and
schema manipulation. These functions are not part of the public API.
"""

from __future__ import annotations

import polars as pl

# Mapping from Polars dtype to our internal type names
_DTYPE_MAP = {
    pl.Utf8: "utf8",
    pl.String: "utf8",
    pl.Int64: "int64",
    pl.Float64: "float64",
    pl.Boolean: "bool",
    pl.Date: "date",
    pl.Datetime: "datetime",
}

# Reverse mapping from internal type names to Polars dtypes
_TYPE_MAP = {
    "utf8": pl.Utf8,
    "int64": pl.Int64,
    "float64": pl.Float64,
    "bool": pl.Boolean,
    "date": pl.Date,
    "datetime": pl.Datetime,
}


def _polars_dtype_to_internal(dtype: pl.DataType) -> str:
    """Convert a Polars dtype to our internal type string."""
    # Handle both class and instance
    dtype_key = dtype if isinstance(dtype, type) else type(dtype)
    if dtype_key in _DTYPE_MAP:
        return _DTYPE_MAP[dtype_key]
    # Try matching by name for robustness
    dtype_name = str(dtype).lower()
    if "utf8" in dtype_name or "string" in dtype_name:
        return "utf8"
    if "int64" in dtype_name:
        return "int64"
    if "float64" in dtype_name:
        return "float64"
    if "bool" in dtype_name:
        return "bool"
    if "datetime" in dtype_name:
        return "datetime"
    if "date" in dtype_name:
        return "date"
    raise ValueError(f"Unsupported dtype: {dtype}")


def _fields_to_schema(fields: list[tuple[str, str]]) -> dict[str, type[pl.DataType]]:
    """Convert internal field list to Polars schema dict."""
    return {name: _TYPE_MAP.get(type_str, pl.Utf8) for name, type_str in fields}


def _schema_to_overwrite(schema: dict) -> list[tuple[str, str]]:
    """Convert Polars schema dict to overwrite format."""
    result = []
    for name, dtype in schema.items():
        type_str = _polars_dtype_to_internal(dtype)
        result.append((name, type_str))
    return result
