"""Pytest configuration and fixtures for polars-redis tests."""

from __future__ import annotations

import os

import pytest


@pytest.fixture
def redis_url() -> str:
    """Get Redis URL from environment or use default."""
    return os.environ.get("REDIS_URL", "redis://localhost:6379")


@pytest.fixture
def redis_available(redis_url: str) -> bool:
    """Check if Redis is available for testing."""
    try:
        import polars_redis

        keys = polars_redis.scan_keys(redis_url, "*", count=1)
        return True
    except Exception:
        return False
