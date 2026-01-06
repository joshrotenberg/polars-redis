"""DataFrame caching decorator for polars-redis.

This module provides decorators that automatically cache function results
(DataFrames) in Redis, similar to functools.lru_cache but distributed.
"""

from __future__ import annotations

import functools
import hashlib
import inspect
import json
from typing import Any, Callable, Literal, TypeVar

import polars as pl

from polars_redis._cache import (
    cache_dataframe,
    cache_exists,
    delete_cached,
    get_cached_dataframe,
)

F = TypeVar("F", bound=Callable[..., pl.DataFrame])
LF = TypeVar("LF", bound=Callable[..., pl.LazyFrame])


def _make_hashable(obj: Any) -> Any:
    """Convert an object to a hashable representation."""
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    elif isinstance(obj, (list, tuple)):
        return tuple(_make_hashable(item) for item in obj)
    elif isinstance(obj, dict):
        return tuple(sorted((k, _make_hashable(v)) for k, v in obj.items()))
    elif isinstance(obj, set):
        return tuple(sorted(_make_hashable(item) for item in obj))
    elif isinstance(obj, (pl.DataFrame, pl.LazyFrame)):
        # DataFrames are not hashable - use schema + shape as approximation
        if isinstance(obj, pl.LazyFrame):
            schema = obj.collect_schema()
            return ("LazyFrame", tuple(schema.items()))
        return ("DataFrame", obj.shape, tuple(obj.schema.items()))
    else:
        # Try to convert to string representation
        try:
            return repr(obj)
        except Exception:
            raise TypeError(f"Unhashable argument type: {type(obj).__name__}")


def _generate_cache_key(
    prefix: str,
    func: Callable,
    args: tuple,
    kwargs: dict,
    key_fn: Callable[..., str] | None = None,
) -> str:
    """Generate a cache key for a function call."""
    if key_fn is not None:
        # Use custom key function
        custom_key = key_fn(*args, **kwargs)
        return f"{prefix}:{func.__module__}.{func.__qualname__}:{custom_key}"

    # Build key from function signature and arguments
    sig = inspect.signature(func)
    bound = sig.bind(*args, **kwargs)
    bound.apply_defaults()

    # Create a hashable representation of arguments
    args_repr = _make_hashable(dict(bound.arguments))
    args_hash = hashlib.sha256(json.dumps(args_repr, sort_keys=True).encode()).hexdigest()[:16]

    return f"{prefix}:{func.__module__}.{func.__qualname__}:{args_hash}"


class CachedFunction:
    """Wrapper for a cached function with cache control methods."""

    def __init__(
        self,
        func: Callable[..., pl.DataFrame],
        url: str,
        ttl: int | None = None,
        key_prefix: str = "polars_redis:cache",
        key_fn: Callable[..., str] | None = None,
        format: Literal["ipc", "parquet"] = "ipc",
        compression: str | None = None,
        chunk_size_mb: int | None = None,
    ):
        self._func = func
        self._url = url
        self._ttl = ttl
        self._key_prefix = key_prefix
        self._key_fn = key_fn
        self._format = format
        self._compression = compression
        self._chunk_size_mb = chunk_size_mb

        # Preserve function metadata
        functools.update_wrapper(self, func)

    def __call__(
        self,
        *args,
        _cache_refresh: bool = False,
        _cache_skip: bool = False,
        **kwargs,
    ) -> pl.DataFrame:
        """Call the function, using cache if available.

        Args:
            *args: Positional arguments for the wrapped function.
            _cache_refresh: If True, ignore cache and recompute (but still cache result).
            _cache_skip: If True, skip cache entirely (don't read or write).
            **kwargs: Keyword arguments for the wrapped function.

        Returns:
            The function result (DataFrame).
        """
        if _cache_skip:
            return self._func(*args, **kwargs)

        cache_key = _generate_cache_key(self._key_prefix, self._func, args, kwargs, self._key_fn)

        # Check cache unless refreshing
        if not _cache_refresh:
            cached = get_cached_dataframe(self._url, cache_key, format=self._format)
            if cached is not None:
                return cached

        # Compute result
        result = self._func(*args, **kwargs)

        # Cache the result
        cache_dataframe(
            result,
            self._url,
            cache_key,
            format=self._format,
            compression=self._compression,
            ttl=self._ttl,
            chunk_size_mb=self._chunk_size_mb,
        )

        return result

    def invalidate(self, *args, **kwargs) -> bool:
        """Invalidate the cache for specific arguments.

        Args:
            *args: Positional arguments that were used.
            **kwargs: Keyword arguments that were used.

        Returns:
            True if the cache entry was deleted, False if it didn't exist.
        """
        cache_key = _generate_cache_key(self._key_prefix, self._func, args, kwargs, self._key_fn)
        return delete_cached(self._url, cache_key)

    def is_cached(self, *args, **kwargs) -> bool:
        """Check if a result is cached for specific arguments.

        Args:
            *args: Positional arguments to check.
            **kwargs: Keyword arguments to check.

        Returns:
            True if a cached result exists.
        """
        cache_key = _generate_cache_key(self._key_prefix, self._func, args, kwargs, self._key_fn)
        return cache_exists(self._url, cache_key)

    def cache_key_for(self, *args, **kwargs) -> str:
        """Get the cache key that would be used for specific arguments.

        Useful for debugging or manual cache management.

        Args:
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            The cache key string.
        """
        return _generate_cache_key(self._key_prefix, self._func, args, kwargs, self._key_fn)


class CachedLazyFunction:
    """Wrapper for a cached function that returns LazyFrame."""

    def __init__(
        self,
        func: Callable[..., pl.LazyFrame],
        url: str,
        ttl: int | None = None,
        key_prefix: str = "polars_redis:cache",
        key_fn: Callable[..., str] | None = None,
        format: Literal["ipc", "parquet"] = "ipc",
        compression: str | None = None,
        chunk_size_mb: int | None = None,
    ):
        self._func = func
        self._url = url
        self._ttl = ttl
        self._key_prefix = key_prefix
        self._key_fn = key_fn
        self._format = format
        self._compression = compression
        self._chunk_size_mb = chunk_size_mb

        functools.update_wrapper(self, func)

    def __call__(
        self,
        *args,
        _cache_refresh: bool = False,
        _cache_skip: bool = False,
        **kwargs,
    ) -> pl.LazyFrame:
        """Call the function, using cache if available.

        The LazyFrame is collected before caching. On cache hit, returns
        a LazyFrame wrapping the cached DataFrame.

        Args:
            *args: Positional arguments for the wrapped function.
            _cache_refresh: If True, ignore cache and recompute.
            _cache_skip: If True, skip cache entirely.
            **kwargs: Keyword arguments for the wrapped function.

        Returns:
            The function result (LazyFrame).
        """
        if _cache_skip:
            return self._func(*args, **kwargs)

        cache_key = _generate_cache_key(self._key_prefix, self._func, args, kwargs, self._key_fn)

        # Check cache unless refreshing
        if not _cache_refresh:
            cached = get_cached_dataframe(self._url, cache_key, format=self._format)
            if cached is not None:
                return cached.lazy()

        # Compute result - collect the LazyFrame
        lazy_result = self._func(*args, **kwargs)
        result = lazy_result.collect()

        # Cache the collected result
        cache_dataframe(
            result,
            self._url,
            cache_key,
            format=self._format,
            compression=self._compression,
            ttl=self._ttl,
            chunk_size_mb=self._chunk_size_mb,
        )

        return result.lazy()

    def invalidate(self, *args, **kwargs) -> bool:
        """Invalidate the cache for specific arguments."""
        cache_key = _generate_cache_key(self._key_prefix, self._func, args, kwargs, self._key_fn)
        return delete_cached(self._url, cache_key)

    def is_cached(self, *args, **kwargs) -> bool:
        """Check if a result is cached for specific arguments."""
        cache_key = _generate_cache_key(self._key_prefix, self._func, args, kwargs, self._key_fn)
        return cache_exists(self._url, cache_key)

    def cache_key_for(self, *args, **kwargs) -> str:
        """Get the cache key that would be used for specific arguments."""
        return _generate_cache_key(self._key_prefix, self._func, args, kwargs, self._key_fn)


def cache(
    url: str,
    *,
    ttl: int | None = None,
    key_prefix: str = "polars_redis:cache",
    key_fn: Callable[..., str] | None = None,
    format: Literal["ipc", "parquet"] = "ipc",
    compression: str | None = None,
    chunk_size_mb: int | None = None,
) -> Callable[[F], CachedFunction]:
    """Decorator to cache DataFrame-returning function results in Redis.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        ttl: Time-to-live in seconds. If None, cache never expires.
        key_prefix: Prefix for all cache keys.
        key_fn: Custom function to generate cache key from arguments.
            If None, key is generated from function name and argument hash.
        format: Serialization format ("ipc" or "parquet").
        compression: Compression codec (e.g., "lz4", "zstd").
        chunk_size_mb: Chunk size for large DataFrames. Set to 0 to disable.

    Returns:
        A decorator that wraps the function with caching.

    Example:
        >>> import polars as pl
        >>> import polars_redis as redis
        >>>
        >>> @redis.cache(url="redis://localhost", ttl=3600)
        ... def expensive_transform(start: str, end: str) -> pl.DataFrame:
        ...     # Complex computation...
        ...     return df
        >>>
        >>> # First call: computes and caches
        >>> result = expensive_transform("2024-01-01", "2024-12-31")
        >>>
        >>> # Second call: returns from cache
        >>> result = expensive_transform("2024-01-01", "2024-12-31")
        >>>
        >>> # Force refresh
        >>> result = expensive_transform("2024-01-01", "2024-12-31", _cache_refresh=True)
        >>>
        >>> # Invalidate cache for specific args
        >>> expensive_transform.invalidate("2024-01-01", "2024-12-31")
    """

    def decorator(func: F) -> CachedFunction:
        return CachedFunction(
            func,
            url=url,
            ttl=ttl,
            key_prefix=key_prefix,
            key_fn=key_fn,
            format=format,
            compression=compression,
            chunk_size_mb=chunk_size_mb,
        )

    return decorator


def cache_lazy(
    url: str,
    *,
    ttl: int | None = None,
    key_prefix: str = "polars_redis:cache",
    key_fn: Callable[..., str] | None = None,
    format: Literal["ipc", "parquet"] = "ipc",
    compression: str | None = None,
    chunk_size_mb: int | None = None,
) -> Callable[[LF], CachedLazyFunction]:
    """Decorator to cache LazyFrame-returning function results in Redis.

    The LazyFrame is collected before caching. On cache hit, returns
    a LazyFrame wrapping the cached DataFrame.

    Args:
        url: Redis connection URL (e.g., "redis://localhost:6379").
        ttl: Time-to-live in seconds. If None, cache never expires.
        key_prefix: Prefix for all cache keys.
        key_fn: Custom function to generate cache key from arguments.
        format: Serialization format ("ipc" or "parquet").
        compression: Compression codec (e.g., "lz4", "zstd").
        chunk_size_mb: Chunk size for large DataFrames.

    Returns:
        A decorator that wraps the function with caching.

    Example:
        >>> import polars as pl
        >>> import polars_redis as redis
        >>>
        >>> @redis.cache_lazy(url="redis://localhost", ttl=3600)
        ... def build_pipeline(config: dict) -> pl.LazyFrame:
        ...     return pl.scan_parquet("data.parquet").filter(...)
        >>>
        >>> # First call: executes pipeline and caches result
        >>> lf = build_pipeline({"filter": "active"})
        >>> df = lf.collect()  # Already collected internally
        >>>
        >>> # Second call: returns cached result as LazyFrame
        >>> lf = build_pipeline({"filter": "active"})
    """

    def decorator(func: LF) -> CachedLazyFunction:
        return CachedLazyFunction(
            func,
            url=url,
            ttl=ttl,
            key_prefix=key_prefix,
            key_fn=key_fn,
            format=format,
            compression=compression,
            chunk_size_mb=chunk_size_mb,
        )

    return decorator
