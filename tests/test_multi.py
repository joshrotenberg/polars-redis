"""Tests for multi-cluster write support."""

from __future__ import annotations

import asyncio

import polars as pl
import pytest


class TestDestination:
    """Tests for Destination dataclass."""

    def test_destination_defaults(self) -> None:
        """Test default values for Destination."""
        from polars_redis import Destination

        dest = Destination(url="redis://localhost:6379")

        assert dest.url == "redis://localhost:6379"
        assert dest.name == "localhost:6379"  # Extracted from URL
        assert dest.key_prefix == ""
        assert dest.transform is None
        assert dest.timeout_ms == 30000

    def test_destination_with_name(self) -> None:
        """Test Destination with explicit name."""
        from polars_redis import Destination

        dest = Destination(url="redis://cluster-a:6379", name="primary")

        assert dest.name == "primary"

    def test_destination_with_transform(self) -> None:
        """Test Destination with transform function."""
        from polars_redis import Destination

        def drop_internal(df: pl.DataFrame) -> pl.DataFrame:
            return df.drop("internal_field")

        dest = Destination(
            url="redis://localhost:6379",
            transform=drop_internal,
        )

        assert dest.transform is not None

        # Test transform works
        df = pl.DataFrame({"id": [1], "name": ["Alice"], "internal_field": ["secret"]})
        result = dest.transform(df)
        assert "internal_field" not in result.columns


class TestDestinationResult:
    """Tests for DestinationResult dataclass."""

    def test_destination_result_repr(self) -> None:
        """Test DestinationResult string representation."""
        from polars_redis import Destination, DestinationResult

        dest = Destination(url="redis://localhost:6379", name="primary")
        result = DestinationResult(
            destination=dest,
            keys_written=100,
            keys_failed=5,
            success=True,
        )

        repr_str = repr(result)
        assert "primary" in repr_str
        assert "100" in repr_str
        assert "5" in repr_str


class TestMultiWriteResult:
    """Tests for MultiWriteResult dataclass."""

    def test_multi_write_result_aggregation(self) -> None:
        """Test MultiWriteResult aggregates correctly."""
        from polars_redis import Destination, DestinationResult, MultiWriteResult

        dest1 = Destination(url="redis://a:6379", name="a")
        dest2 = Destination(url="redis://b:6379", name="b")

        result = MultiWriteResult(
            results=[
                DestinationResult(destination=dest1, keys_written=100, keys_failed=0, success=True),
                DestinationResult(destination=dest2, keys_written=80, keys_failed=20, success=True),
            ]
        )

        assert result.total_keys_written == 180
        assert result.total_keys_failed == 20
        assert result.all_succeeded is True
        assert len(result.failed_destinations) == 0

    def test_multi_write_result_with_failure(self) -> None:
        """Test MultiWriteResult with failed destination."""
        from polars_redis import Destination, DestinationResult, MultiWriteResult

        dest1 = Destination(url="redis://a:6379", name="a")
        dest2 = Destination(url="redis://b:6379", name="b")

        result = MultiWriteResult(
            results=[
                DestinationResult(destination=dest1, keys_written=100, success=True),
                DestinationResult(
                    destination=dest2, keys_written=0, success=False, error="Connection refused"
                ),
            ]
        )

        assert result.all_succeeded is False
        assert result.failed_destinations == ["b"]


class TestWriteHashesMulti:
    """Tests for write_hashes_multi function."""

    @pytest.mark.asyncio
    async def test_write_hashes_multi_single_destination(
        self, redis_url: str, redis_available: bool
    ) -> None:
        """Test writing to a single destination."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        df = pl.DataFrame(
            {
                "_key": ["multi:hash:1", "multi:hash:2", "multi:hash:3"],
                "name": ["Alice", "Bob", "Charlie"],
                "score": [100, 200, 300],
            }
        )

        result = await pr.write_hashes_multi(
            df,
            destinations=[pr.Destination(url=redis_url, name="test")],
        )

        assert result.all_succeeded
        assert result.total_keys_written == 3

        # Verify data was written
        client = redis_client.from_url(redis_url)
        try:
            assert client.hget("multi:hash:1", "name") == b"Alice"
            assert client.hget("multi:hash:2", "score") == b"200"
        finally:
            client.delete("multi:hash:1", "multi:hash:2", "multi:hash:3")
            client.close()

    @pytest.mark.asyncio
    async def test_write_hashes_multi_with_key_prefix(
        self, redis_url: str, redis_available: bool
    ) -> None:
        """Test writing with different key prefixes per destination."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        df = pl.DataFrame(
            {
                "_key": ["1", "2"],
                "value": ["a", "b"],
            }
        )

        result = await pr.write_hashes_multi(
            df,
            destinations=[
                pr.Destination(url=redis_url, name="prefix-a", key_prefix="prefix_a:"),
                pr.Destination(url=redis_url, name="prefix-b", key_prefix="prefix_b:"),
            ],
        )

        assert result.all_succeeded
        assert result.total_keys_written == 4  # 2 keys x 2 destinations

        # Verify both prefixes exist
        client = redis_client.from_url(redis_url)
        try:
            assert client.hget("prefix_a:1", "value") == b"a"
            assert client.hget("prefix_b:1", "value") == b"a"
        finally:
            client.delete("prefix_a:1", "prefix_a:2", "prefix_b:1", "prefix_b:2")
            client.close()

    @pytest.mark.asyncio
    async def test_write_hashes_multi_with_transform(
        self, redis_url: str, redis_available: bool
    ) -> None:
        """Test writing with per-destination transform."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        df = pl.DataFrame(
            {
                "_key": ["transform:1"],
                "name": ["Alice"],
                "internal": ["secret"],
            }
        )

        def drop_internal(df: pl.DataFrame) -> pl.DataFrame:
            return df.drop("internal")

        result = await pr.write_hashes_multi(
            df,
            destinations=[
                pr.Destination(url=redis_url, name="full", key_prefix="full:"),
                pr.Destination(
                    url=redis_url, name="filtered", key_prefix="filtered:", transform=drop_internal
                ),
            ],
        )

        assert result.all_succeeded

        client = redis_client.from_url(redis_url)
        try:
            # Full destination has all fields
            assert client.hget("full:transform:1", "internal") == b"secret"
            # Filtered destination doesn't have internal field
            assert client.hget("filtered:transform:1", "internal") is None
            assert client.hget("filtered:transform:1", "name") == b"Alice"
        finally:
            client.delete("full:transform:1", "filtered:transform:1")
            client.close()

    @pytest.mark.asyncio
    async def test_write_hashes_multi_dict_destinations(
        self, redis_url: str, redis_available: bool
    ) -> None:
        """Test writing with dict-style destination config."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        df = pl.DataFrame(
            {
                "_key": ["dict:1"],
                "value": ["test"],
            }
        )

        result = await pr.write_hashes_multi(
            df,
            destinations=[
                {"url": redis_url, "name": "dict-dest", "key_prefix": "dict:"},
            ],
        )

        assert result.all_succeeded

        client = redis_client.from_url(redis_url)
        try:
            assert client.hget("dict:dict:1", "value") == b"test"
        finally:
            client.delete("dict:dict:1")
            client.close()

    @pytest.mark.asyncio
    async def test_write_hashes_multi_failed_destination(
        self, redis_url: str, redis_available: bool
    ) -> None:
        """Test handling of failed destination."""
        if not redis_available:
            pytest.skip("Redis not available")

        import polars_redis as pr
        import redis as redis_client

        df = pl.DataFrame(
            {
                "_key": ["fail:1"],
                "value": ["test"],
            }
        )

        result = await pr.write_hashes_multi(
            df,
            destinations=[
                pr.Destination(url=redis_url, name="good"),
                pr.Destination(url="redis://nonexistent-host:6379", name="bad", timeout_ms=1000),
            ],
        )

        # Good destination should succeed
        assert result.results[0].success is True
        assert result.results[0].keys_written == 1

        # Bad destination should fail
        assert result.results[1].success is False
        assert result.results[1].error is not None
        assert "bad" in result.failed_destinations

        assert result.all_succeeded is False

        # Cleanup
        client = redis_client.from_url(redis_url)
        try:
            client.delete("fail:1")
        finally:
            client.close()


class TestWriteJsonMulti:
    """Tests for write_json_multi function."""

    @pytest.mark.asyncio
    async def test_write_json_multi_basic(self, redis_url: str, redis_available: bool) -> None:
        """Test basic JSON multi-write."""
        if not redis_available:
            pytest.skip("Redis not available")

        import json

        import polars_redis as pr
        import redis as redis_client

        df = pl.DataFrame(
            {
                "_key": ["json:multi:1", "json:multi:2"],
                "title": ["Hello", "World"],
                "count": [10, 20],
            }
        )

        result = await pr.write_json_multi(
            df,
            destinations=[pr.Destination(url=redis_url, name="json-test")],
        )

        assert result.all_succeeded
        assert result.total_keys_written == 2

        client = redis_client.from_url(redis_url)
        try:
            doc1 = client.json().get("json:multi:1")
            assert doc1["title"] == "Hello"
            assert doc1["count"] == 10
        finally:
            client.delete("json:multi:1", "json:multi:2")
            client.close()


class TestMultiUnit:
    """Unit tests that don't require Redis."""

    def test_imports(self) -> None:
        """Test that multi-cluster functions are importable."""
        from polars_redis import (
            Destination,
            DestinationResult,
            MultiWriteResult,
            write_hashes_multi,
            write_json_multi,
        )

        assert callable(write_hashes_multi)
        assert callable(write_json_multi)
        assert Destination is not None
        assert DestinationResult is not None
        assert MultiWriteResult is not None

    def test_destination_url_parsing(self) -> None:
        """Test URL parsing for default name."""
        from polars_redis import Destination

        # Standard URL
        dest = Destination(url="redis://my-cluster.example.com:6379")
        assert dest.name == "my-cluster.example.com:6379"

        # With path
        dest = Destination(url="redis://localhost:6379/0")
        assert dest.name == "localhost:6379"

        # With auth (should still extract host)
        dest = Destination(url="redis://:password@host:6379")
        assert "host:6379" in dest.name
