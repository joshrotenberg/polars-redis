"""Pipeline and transaction support for batched Redis operations.

This module provides Pipeline and Transaction classes for efficient
batched Redis operations.

Example:
    >>> import polars_redis as pr
    >>>
    >>> # Pipeline - batch commands for efficiency
    >>> pipe = pr.Pipeline("redis://localhost:6379")
    >>> pipe.set("key1", "value1")
    >>> pipe.set("key2", "value2")
    >>> pipe.get("key1")
    >>> result = pipe.execute()
    >>> print(result.succeeded)  # 3
    >>>
    >>> # Transaction - atomic operations
    >>> tx = pr.Transaction("redis://localhost:6379")
    >>> tx.set("counter", "0")
    >>> tx.incr("counter")
    >>> result = tx.execute()  # Atomic
"""

from __future__ import annotations

from typing import Any

from polars_redis._internal import (
    PyCommandResult,
    PyPipeline,
    PyPipelineResult,
    PyTransaction,
)


class CommandResult:
    """Result of a single command in a pipeline or transaction.

    Attributes:
        result_type: Type of result (ok, string, int, bulk, array, nil, error)
        value: The value returned by the command
    """

    def __init__(self, inner: PyCommandResult) -> None:
        self._inner = inner

    @property
    def result_type(self) -> str:
        """Get the result type."""
        return self._inner.result_type

    @property
    def value(self) -> Any:
        """Get the value."""
        return self._inner.value

    def is_ok(self) -> bool:
        """Check if the result is OK."""
        return self._inner.is_ok()

    def is_error(self) -> bool:
        """Check if the result is an error."""
        return self._inner.is_error()

    def is_nil(self) -> bool:
        """Check if the result is nil."""
        return self._inner.is_nil()

    def __repr__(self) -> str:
        return f"CommandResult({self.result_type}, {self.value})"


class PipelineResult:
    """Result of executing a pipeline or transaction.

    Attributes:
        results: List of CommandResult objects
        succeeded: Number of commands that succeeded
        failed: Number of commands that failed
    """

    def __init__(self, inner: PyPipelineResult) -> None:
        self._inner = inner

    @property
    def results(self) -> list[CommandResult]:
        """Get the list of results."""
        return [CommandResult(r) for r in self._inner.results]

    @property
    def succeeded(self) -> int:
        """Get the number of successful commands."""
        return self._inner.succeeded

    @property
    def failed(self) -> int:
        """Get the number of failed commands."""
        return self._inner.failed

    def all_succeeded(self) -> bool:
        """Check if all commands succeeded."""
        return self._inner.all_succeeded()

    def get(self, index: int) -> CommandResult | None:
        """Get the result at a specific index."""
        inner = self._inner.get(index)
        return CommandResult(inner) if inner is not None else None

    def __len__(self) -> int:
        return len(self._inner)

    def __repr__(self) -> str:
        return f"PipelineResult(commands={len(self)}, succeeded={self.succeeded}, failed={self.failed})"


class Pipeline:
    """Pipeline for batching Redis commands.

    Pipelines reduce network round-trips by sending multiple commands
    at once and receiving all responses together.

    Example:
        >>> pipe = Pipeline("redis://localhost:6379")
        >>> pipe.set("key1", "value1")
        >>> pipe.set("key2", "value2")
        >>> pipe.get("key1")
        >>> result = pipe.execute()
        >>> print(result.succeeded)  # 3
    """

    def __init__(self, url: str) -> None:
        """Create a new pipeline.

        Args:
            url: Redis connection URL
        """
        self._inner = PyPipeline(url)

    def __len__(self) -> int:
        return len(self._inner)

    def is_empty(self) -> bool:
        """Check if the pipeline is empty."""
        return self._inner.is_empty()

    def clear(self) -> None:
        """Clear all queued commands."""
        self._inner.clear()

    # String commands
    def set(self, key: str, value: str) -> "Pipeline":
        """Queue a SET command."""
        self._inner.set(key, value)
        return self

    def set_ex(self, key: str, value: str, seconds: int) -> "Pipeline":
        """Queue a SET command with expiration."""
        self._inner.set_ex(key, value, seconds)
        return self

    def get(self, key: str) -> "Pipeline":
        """Queue a GET command."""
        self._inner.get(key)
        return self

    def mget(self, keys: list[str]) -> "Pipeline":
        """Queue an MGET command."""
        self._inner.mget(keys)
        return self

    def incr(self, key: str) -> "Pipeline":
        """Queue an INCR command."""
        self._inner.incr(key)
        return self

    def incrby(self, key: str, increment: int) -> "Pipeline":
        """Queue an INCRBY command."""
        self._inner.incrby(key, increment)
        return self

    def decr(self, key: str) -> "Pipeline":
        """Queue a DECR command."""
        self._inner.decr(key)
        return self

    # Hash commands
    def hset(self, key: str, field: str, value: str) -> "Pipeline":
        """Queue an HSET command."""
        self._inner.hset(key, field, value)
        return self

    def hmset(self, key: str, fields: dict[str, str]) -> "Pipeline":
        """Queue an HMSET command for multiple fields."""
        self._inner.hmset(key, fields)
        return self

    def hget(self, key: str, field: str) -> "Pipeline":
        """Queue an HGET command."""
        self._inner.hget(key, field)
        return self

    def hgetall(self, key: str) -> "Pipeline":
        """Queue an HGETALL command."""
        self._inner.hgetall(key)
        return self

    def hdel(self, key: str, fields: list[str]) -> "Pipeline":
        """Queue an HDEL command."""
        self._inner.hdel(key, fields)
        return self

    def hincrby(self, key: str, field: str, increment: int) -> "Pipeline":
        """Queue an HINCRBY command."""
        self._inner.hincrby(key, field, increment)
        return self

    # List commands
    def lpush(self, key: str, values: list[str]) -> "Pipeline":
        """Queue an LPUSH command."""
        self._inner.lpush(key, values)
        return self

    def rpush(self, key: str, values: list[str]) -> "Pipeline":
        """Queue an RPUSH command."""
        self._inner.rpush(key, values)
        return self

    def lrange(self, key: str, start: int, stop: int) -> "Pipeline":
        """Queue an LRANGE command."""
        self._inner.lrange(key, start, stop)
        return self

    def llen(self, key: str) -> "Pipeline":
        """Queue an LLEN command."""
        self._inner.llen(key)
        return self

    # Set commands
    def sadd(self, key: str, members: list[str]) -> "Pipeline":
        """Queue an SADD command."""
        self._inner.sadd(key, members)
        return self

    def smembers(self, key: str) -> "Pipeline":
        """Queue an SMEMBERS command."""
        self._inner.smembers(key)
        return self

    def sismember(self, key: str, member: str) -> "Pipeline":
        """Queue an SISMEMBER command."""
        self._inner.sismember(key, member)
        return self

    def scard(self, key: str) -> "Pipeline":
        """Queue an SCARD command."""
        self._inner.scard(key)
        return self

    # Sorted set commands
    def zadd(self, key: str, members: list[tuple[float, str]]) -> "Pipeline":
        """Queue a ZADD command."""
        self._inner.zadd(key, members)
        return self

    def zrange(self, key: str, start: int, stop: int) -> "Pipeline":
        """Queue a ZRANGE command."""
        self._inner.zrange(key, start, stop)
        return self

    def zscore(self, key: str, member: str) -> "Pipeline":
        """Queue a ZSCORE command."""
        self._inner.zscore(key, member)
        return self

    def zcard(self, key: str) -> "Pipeline":
        """Queue a ZCARD command."""
        self._inner.zcard(key)
        return self

    # Key commands
    def delete(self, keys: list[str]) -> "Pipeline":
        """Queue a DEL command."""
        self._inner.delete(keys)
        return self

    def exists(self, keys: list[str]) -> "Pipeline":
        """Queue an EXISTS command."""
        self._inner.exists(keys)
        return self

    def expire(self, key: str, seconds: int) -> "Pipeline":
        """Queue an EXPIRE command."""
        self._inner.expire(key, seconds)
        return self

    def ttl(self, key: str) -> "Pipeline":
        """Queue a TTL command."""
        self._inner.ttl(key)
        return self

    def rename(self, key: str, new_key: str) -> "Pipeline":
        """Queue a RENAME command."""
        self._inner.rename(key, new_key)
        return self

    def key_type(self, key: str) -> "Pipeline":
        """Queue a TYPE command."""
        self._inner.key_type(key)
        return self

    def raw(self, command: str, args: list[str]) -> "Pipeline":
        """Queue a raw command with arguments."""
        self._inner.raw(command, args)
        return self

    def execute(self) -> PipelineResult:
        """Execute all queued commands and return results."""
        return PipelineResult(self._inner.execute())


class Transaction:
    """Transaction for atomic Redis operations.

    Transactions use MULTI/EXEC to execute commands atomically.
    Either all commands succeed or none do.

    Example:
        >>> tx = Transaction("redis://localhost:6379")
        >>> tx.set("counter", "0")
        >>> tx.incr("counter")
        >>> result = tx.execute()  # Atomic
    """

    def __init__(self, url: str) -> None:
        """Create a new transaction.

        Args:
            url: Redis connection URL
        """
        self._inner = PyTransaction(url)

    def __len__(self) -> int:
        return len(self._inner)

    def is_empty(self) -> bool:
        """Check if the transaction is empty."""
        return self._inner.is_empty()

    def discard(self) -> None:
        """Discard the transaction (clear all queued commands)."""
        self._inner.discard()

    # String commands
    def set(self, key: str, value: str) -> "Transaction":
        """Queue a SET command."""
        self._inner.set(key, value)
        return self

    def set_ex(self, key: str, value: str, seconds: int) -> "Transaction":
        """Queue a SET command with expiration."""
        self._inner.set_ex(key, value, seconds)
        return self

    def get(self, key: str) -> "Transaction":
        """Queue a GET command."""
        self._inner.get(key)
        return self

    def mget(self, keys: list[str]) -> "Transaction":
        """Queue an MGET command."""
        self._inner.mget(keys)
        return self

    def incr(self, key: str) -> "Transaction":
        """Queue an INCR command."""
        self._inner.incr(key)
        return self

    def incrby(self, key: str, increment: int) -> "Transaction":
        """Queue an INCRBY command."""
        self._inner.incrby(key, increment)
        return self

    def decr(self, key: str) -> "Transaction":
        """Queue a DECR command."""
        self._inner.decr(key)
        return self

    # Hash commands
    def hset(self, key: str, field: str, value: str) -> "Transaction":
        """Queue an HSET command."""
        self._inner.hset(key, field, value)
        return self

    def hmset(self, key: str, fields: dict[str, str]) -> "Transaction":
        """Queue an HMSET command for multiple fields."""
        self._inner.hmset(key, fields)
        return self

    def hget(self, key: str, field: str) -> "Transaction":
        """Queue an HGET command."""
        self._inner.hget(key, field)
        return self

    def hgetall(self, key: str) -> "Transaction":
        """Queue an HGETALL command."""
        self._inner.hgetall(key)
        return self

    def hdel(self, key: str, fields: list[str]) -> "Transaction":
        """Queue an HDEL command."""
        self._inner.hdel(key, fields)
        return self

    def hincrby(self, key: str, field: str, increment: int) -> "Transaction":
        """Queue an HINCRBY command."""
        self._inner.hincrby(key, field, increment)
        return self

    # List commands
    def lpush(self, key: str, values: list[str]) -> "Transaction":
        """Queue an LPUSH command."""
        self._inner.lpush(key, values)
        return self

    def rpush(self, key: str, values: list[str]) -> "Transaction":
        """Queue an RPUSH command."""
        self._inner.rpush(key, values)
        return self

    def lrange(self, key: str, start: int, stop: int) -> "Transaction":
        """Queue an LRANGE command."""
        self._inner.lrange(key, start, stop)
        return self

    def llen(self, key: str) -> "Transaction":
        """Queue an LLEN command."""
        self._inner.llen(key)
        return self

    # Set commands
    def sadd(self, key: str, members: list[str]) -> "Transaction":
        """Queue an SADD command."""
        self._inner.sadd(key, members)
        return self

    def smembers(self, key: str) -> "Transaction":
        """Queue an SMEMBERS command."""
        self._inner.smembers(key)
        return self

    def sismember(self, key: str, member: str) -> "Transaction":
        """Queue an SISMEMBER command."""
        self._inner.sismember(key, member)
        return self

    def scard(self, key: str) -> "Transaction":
        """Queue an SCARD command."""
        self._inner.scard(key)
        return self

    # Sorted set commands
    def zadd(self, key: str, members: list[tuple[float, str]]) -> "Transaction":
        """Queue a ZADD command."""
        self._inner.zadd(key, members)
        return self

    def zrange(self, key: str, start: int, stop: int) -> "Transaction":
        """Queue a ZRANGE command."""
        self._inner.zrange(key, start, stop)
        return self

    def zscore(self, key: str, member: str) -> "Transaction":
        """Queue a ZSCORE command."""
        self._inner.zscore(key, member)
        return self

    def zcard(self, key: str) -> "Transaction":
        """Queue a ZCARD command."""
        self._inner.zcard(key)
        return self

    # Key commands
    def delete(self, keys: list[str]) -> "Transaction":
        """Queue a DEL command."""
        self._inner.delete(keys)
        return self

    def exists(self, keys: list[str]) -> "Transaction":
        """Queue an EXISTS command."""
        self._inner.exists(keys)
        return self

    def expire(self, key: str, seconds: int) -> "Transaction":
        """Queue an EXPIRE command."""
        self._inner.expire(key, seconds)
        return self

    def ttl(self, key: str) -> "Transaction":
        """Queue a TTL command."""
        self._inner.ttl(key)
        return self

    def rename(self, key: str, new_key: str) -> "Transaction":
        """Queue a RENAME command."""
        self._inner.rename(key, new_key)
        return self

    def key_type(self, key: str) -> "Transaction":
        """Queue a TYPE command."""
        self._inner.key_type(key)
        return self

    def raw(self, command: str, args: list[str]) -> "Transaction":
        """Queue a raw command with arguments."""
        self._inner.raw(command, args)
        return self

    def execute(self) -> PipelineResult:
        """Execute the transaction atomically."""
        return PipelineResult(self._inner.execute())
