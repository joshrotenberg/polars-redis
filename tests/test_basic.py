"""Basic tests for polars-redis."""

from __future__ import annotations

import polars_redis
import pytest


def test_version():
    """Test that version is defined."""
    assert polars_redis.__version__ == "0.1.0"


def test_scanner_creation():
    """Test RedisScanner can be created."""
    scanner = polars_redis.RedisScanner(
        connection_url="redis://localhost:6379",
        pattern="test:*",
        batch_size=500,
        count_hint=50,
    )
    assert scanner.connection_url == "redis://localhost:6379"
    assert scanner.pattern == "test:*"
    assert scanner.batch_size == 500
    assert scanner.count_hint == 50


def test_scanner_defaults():
    """Test RedisScanner default values."""
    scanner = polars_redis.RedisScanner(
        connection_url="redis://localhost:6379",
        pattern="*",
    )
    assert scanner.batch_size == 1000
    assert scanner.count_hint == 100


def test_scan_hashes_requires_schema():
    """Test that scan_hashes requires a schema."""
    with pytest.raises(ValueError, match="schema is required"):
        polars_redis.scan_hashes("redis://localhost:6379", pattern="test:*")


def test_scan_json_requires_schema():
    """Test that scan_json requires a schema."""
    with pytest.raises(ValueError, match="schema is required"):
        polars_redis.scan_json("redis://localhost:6379", pattern="test:*")


def test_scan_strings_returns_lazyframe():
    """Test that scan_strings returns a LazyFrame."""
    lf = polars_redis.scan_strings("redis://localhost:6379", pattern="test:*")
    # Should return a LazyFrame (collection won't work without Redis)
    import polars as pl

    assert isinstance(lf, pl.LazyFrame)


def test_scan_keys_with_redis(redis_url: str, redis_available: bool):
    """Test scan_keys with a real Redis connection."""
    if not redis_available:
        pytest.skip("Redis not available")
    keys = polars_redis.scan_keys(redis_url, "*", count=5)
    assert isinstance(keys, list)
