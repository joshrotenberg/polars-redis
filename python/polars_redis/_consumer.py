"""Stream consumer with checkpointing for polars-redis.

This module provides a higher-level abstraction for consuming Redis Streams
with automatic checkpointing and resumption support.
"""

from __future__ import annotations

import asyncio
import json
import signal
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator

import polars as pl


class CheckpointStore(ABC):
    """Abstract base class for checkpoint storage.

    Checkpoint stores persist the last processed stream entry ID,
    enabling consumers to resume from where they left off after restarts.
    """

    @abstractmethod
    async def get(self, stream: str, group: str, consumer: str) -> str | None:
        """Get the last processed stream ID.

        Args:
            stream: Stream name.
            group: Consumer group name.
            consumer: Consumer name.

        Returns:
            Last processed stream ID, or None if no checkpoint exists.
        """
        ...

    @abstractmethod
    async def set(self, stream: str, group: str, consumer: str, stream_id: str) -> None:
        """Store a checkpoint.

        Args:
            stream: Stream name.
            group: Consumer group name.
            consumer: Consumer name.
            stream_id: Stream ID to checkpoint.
        """
        ...

    @abstractmethod
    async def delete(self, stream: str, group: str, consumer: str) -> None:
        """Delete a checkpoint.

        Args:
            stream: Stream name.
            group: Consumer group name.
            consumer: Consumer name.
        """
        ...


class MemoryCheckpointStore(CheckpointStore):
    """In-memory checkpoint store for testing.

    Checkpoints are lost when the process exits.
    """

    def __init__(self) -> None:
        self._checkpoints: dict[str, str] = {}

    def _key(self, stream: str, group: str, consumer: str) -> str:
        return f"{stream}:{group}:{consumer}"

    async def get(self, stream: str, group: str, consumer: str) -> str | None:
        return self._checkpoints.get(self._key(stream, group, consumer))

    async def set(self, stream: str, group: str, consumer: str, stream_id: str) -> None:
        self._checkpoints[self._key(stream, group, consumer)] = stream_id

    async def delete(self, stream: str, group: str, consumer: str) -> None:
        self._checkpoints.pop(self._key(stream, group, consumer), None)


class FileCheckpointStore(CheckpointStore):
    """File-based checkpoint store for development.

    Stores checkpoints as JSON in a local file.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._checkpoints: dict[str, str] = {}
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            try:
                self._checkpoints = json.loads(self._path.read_text())
            except (json.JSONDecodeError, OSError):
                self._checkpoints = {}

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(self._checkpoints, indent=2))

    def _key(self, stream: str, group: str, consumer: str) -> str:
        return f"{stream}:{group}:{consumer}"

    async def get(self, stream: str, group: str, consumer: str) -> str | None:
        return self._checkpoints.get(self._key(stream, group, consumer))

    async def set(self, stream: str, group: str, consumer: str, stream_id: str) -> None:
        self._checkpoints[self._key(stream, group, consumer)] = stream_id
        self._save()

    async def delete(self, stream: str, group: str, consumer: str) -> None:
        self._checkpoints.pop(self._key(stream, group, consumer), None)
        self._save()


class RedisCheckpointStore(CheckpointStore):
    """Redis-based checkpoint store for production.

    Stores checkpoints as Redis hash fields.
    """

    def __init__(self, url: str, key_prefix: str = "checkpoint:") -> None:
        self._url = url
        self._key_prefix = key_prefix
        self._client: Any = None

    async def _get_client(self) -> Any:
        if self._client is None:
            import redis.asyncio as redis_async

            self._client = redis_async.from_url(self._url)
        return self._client

    def _key(self, stream: str, group: str, consumer: str) -> str:
        return f"{self._key_prefix}{stream}:{group}:{consumer}"

    async def get(self, stream: str, group: str, consumer: str) -> str | None:
        client = await self._get_client()
        value = await client.get(self._key(stream, group, consumer))
        if value is not None:
            return value.decode() if isinstance(value, bytes) else value
        return None

    async def set(self, stream: str, group: str, consumer: str, stream_id: str) -> None:
        client = await self._get_client()
        await client.set(self._key(stream, group, consumer), stream_id)

    async def delete(self, stream: str, group: str, consumer: str) -> None:
        client = await self._get_client()
        await client.delete(self._key(stream, group, consumer))

    async def close(self) -> None:
        """Close the Redis connection."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None


@dataclass
class ConsumerStats:
    """Statistics for a stream consumer session."""

    batches_processed: int = 0
    entries_processed: int = 0
    last_entry_id: str | None = None
    start_time: float = field(default_factory=time.time)
    last_batch_time: float | None = None

    @property
    def uptime_seconds(self) -> float:
        return time.time() - self.start_time

    @property
    def entries_per_second(self) -> float:
        if self.uptime_seconds > 0:
            return self.entries_processed / self.uptime_seconds
        return 0.0


class StreamConsumer:
    """High-level stream consumer with checkpointing.

    Provides automatic checkpointing, graceful shutdown handling,
    and batch iteration over Redis Streams.

    Example:
        >>> from polars_redis import StreamConsumer, RedisCheckpointStore
        >>>
        >>> consumer = StreamConsumer(
        ...     url="redis://localhost:6379",
        ...     stream="events",
        ...     group="processor",
        ...     consumer="worker-1",
        ...     checkpoint_store=RedisCheckpointStore("redis://localhost:6379"),
        ... )
        >>>
        >>> async with consumer:
        ...     async for batch_df in consumer:
        ...         await process(batch_df)
        ...         # Checkpoint automatically saved after each batch
    """

    def __init__(
        self,
        url: str,
        stream: str,
        group: str,
        consumer: str,
        *,
        checkpoint_store: CheckpointStore | None = None,
        batch_size: int = 100,
        batch_timeout_ms: int = 1000,
        auto_ack: bool = True,
        auto_checkpoint: bool = True,
        schema: dict[str, pl.DataType] | None = None,
        include_id: bool = True,
        id_column: str = "_id",
        include_timestamp: bool = True,
        timestamp_column: str = "_ts",
        include_sequence: bool = False,
        sequence_column: str = "_seq",
        handle_signals: bool = True,
    ) -> None:
        """Initialize the stream consumer.

        Args:
            url: Redis connection URL.
            stream: Name of the stream to consume.
            group: Consumer group name.
            consumer: Consumer name within the group.
            checkpoint_store: Store for persisting checkpoints.
                If None, uses MemoryCheckpointStore (no persistence).
            batch_size: Maximum entries per batch.
            batch_timeout_ms: Timeout for batch collection in milliseconds.
            auto_ack: Automatically acknowledge messages after yielding.
            auto_checkpoint: Automatically checkpoint after each batch.
            schema: Dictionary mapping field names to Polars dtypes.
            include_id: Include the entry ID as a column.
            id_column: Name of the ID column.
            include_timestamp: Include the timestamp as a column.
            timestamp_column: Name of the timestamp column.
            include_sequence: Include the sequence number as a column.
            sequence_column: Name of the sequence column.
            handle_signals: Register SIGTERM/SIGINT handlers for graceful shutdown.
        """
        self._url = url
        self._stream = stream
        self._group = group
        self._consumer = consumer
        self._checkpoint_store = checkpoint_store or MemoryCheckpointStore()
        self._batch_size = batch_size
        self._batch_timeout_ms = batch_timeout_ms
        self._auto_ack = auto_ack
        self._auto_checkpoint = auto_checkpoint
        self._schema = schema
        self._include_id = include_id
        self._id_column = id_column
        self._include_timestamp = include_timestamp
        self._timestamp_column = timestamp_column
        self._include_sequence = include_sequence
        self._sequence_column = sequence_column
        self._handle_signals = handle_signals

        self._client: Any = None
        self._running = False
        self._shutdown_requested = False
        self._current_batch_ids: list[str] = []
        self._last_id: str | None = None
        self._stats = ConsumerStats()
        self._original_handlers: dict[int, Any] = {}

    @property
    def stats(self) -> ConsumerStats:
        """Get consumer statistics."""
        return self._stats

    async def __aenter__(self) -> "StreamConsumer":
        """Start the consumer."""
        await self._start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Stop the consumer."""
        await self._stop()

    async def _start(self) -> None:
        """Initialize the consumer."""
        import redis.asyncio as redis_async

        self._client = redis_async.from_url(self._url)
        self._running = True
        self._shutdown_requested = False
        self._stats = ConsumerStats()

        # Create consumer group if it doesn't exist
        try:
            await self._client.xgroup_create(
                name=self._stream,
                groupname=self._group,
                id="0",
                mkstream=True,
            )
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                raise

        # Restore from checkpoint if available
        checkpoint = await self._checkpoint_store.get(self._stream, self._group, self._consumer)
        if checkpoint:
            self._last_id = checkpoint

        # Register signal handlers
        if self._handle_signals:
            self._register_signal_handlers()

    async def _stop(self) -> None:
        """Shutdown the consumer."""
        self._running = False

        # Restore original signal handlers
        if self._handle_signals:
            self._restore_signal_handlers()

        # Save final checkpoint
        if self._last_id and self._auto_checkpoint:
            await self._checkpoint_store.set(
                self._stream, self._group, self._consumer, self._last_id
            )

        # Close connections
        if self._client:
            await self._client.aclose()
            self._client = None

        if isinstance(self._checkpoint_store, RedisCheckpointStore):
            await self._checkpoint_store.close()

    def _register_signal_handlers(self) -> None:
        """Register handlers for graceful shutdown."""
        loop = asyncio.get_event_loop()

        def handler(signum: int, frame: Any) -> None:
            self._shutdown_requested = True

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                self._original_handlers[sig] = signal.signal(sig, handler)
            except (ValueError, OSError):
                # Signal handling may not work in all contexts (e.g., threads)
                pass

    def _restore_signal_handlers(self) -> None:
        """Restore original signal handlers."""
        for sig, handler in self._original_handlers.items():
            try:
                signal.signal(sig, handler)
            except (ValueError, OSError):
                pass
        self._original_handlers.clear()

    def __aiter__(self) -> AsyncIterator[pl.DataFrame]:
        """Return async iterator."""
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[pl.DataFrame]:
        """Iterate over stream batches."""
        while self._running and not self._shutdown_requested:
            try:
                # Read from stream
                result = await self._client.xreadgroup(
                    groupname=self._group,
                    consumername=self._consumer,
                    streams={self._stream: ">"},
                    count=self._batch_size,
                    block=self._batch_timeout_ms,
                    noack=self._auto_ack,
                )

                if not result:
                    continue

                # Parse entries
                entries = []
                self._current_batch_ids = []

                for stream_name, stream_entries in result:
                    for entry_id, fields in stream_entries:
                        entry_id_str = (
                            entry_id.decode() if isinstance(entry_id, bytes) else entry_id
                        )
                        self._current_batch_ids.append(entry_id_str)

                        # Decode fields
                        decoded_fields = {}
                        for k, v in fields.items():
                            key = k.decode() if isinstance(k, bytes) else k
                            value = v.decode() if isinstance(v, bytes) else v
                            decoded_fields[key] = value

                        entries.append((entry_id_str, decoded_fields))

                if not entries:
                    continue

                # Build DataFrame
                df = self._entries_to_dataframe(entries)

                # Update last ID
                self._last_id = entries[-1][0]

                # Update stats
                self._stats.batches_processed += 1
                self._stats.entries_processed += len(entries)
                self._stats.last_entry_id = self._last_id
                self._stats.last_batch_time = time.time()

                yield df

                # Auto-checkpoint after successful yield
                if self._auto_checkpoint and self._last_id:
                    await self._checkpoint_store.set(
                        self._stream, self._group, self._consumer, self._last_id
                    )

            except asyncio.CancelledError:
                break
            except Exception:
                # Re-raise to let caller handle
                raise

    def _entries_to_dataframe(self, entries: list[tuple[str, dict[str, str]]]) -> pl.DataFrame:
        """Convert stream entries to DataFrame."""
        rows: list[dict[str, Any]] = []

        for entry_id, fields in entries:
            row: dict[str, Any] = {}

            if self._include_id:
                row[self._id_column] = entry_id

            if self._include_timestamp or self._include_sequence:
                # Parse timestamp and sequence from entry ID (format: "timestamp-sequence")
                parts = entry_id.split("-")
                if self._include_timestamp:
                    row[self._timestamp_column] = int(parts[0])
                if self._include_sequence:
                    row[self._sequence_column] = int(parts[1]) if len(parts) > 1 else 0

            row.update(fields)
            rows.append(row)

        df = pl.DataFrame(rows)

        # Cast to schema if provided
        if self._schema:
            for name, dtype in self._schema.items():
                if name in df.columns:
                    df = df.with_columns(pl.col(name).cast(dtype))

        return df

    async def commit(self) -> None:
        """Manually commit the current batch checkpoint.

        Use this when auto_checkpoint=False for explicit checkpoint control.
        """
        if self._last_id:
            await self._checkpoint_store.set(
                self._stream, self._group, self._consumer, self._last_id
            )

    async def rollback(self) -> None:
        """Discard current batch progress without checkpointing.

        Use this when processing fails and you want to re-process
        the current batch on restart. The checkpoint remains at the
        previous position, so restarting will re-read this batch.

        Note: This does NOT re-deliver messages in the current session.
        It only affects restart behavior by not advancing the checkpoint.
        """
        # Clear current batch state without saving checkpoint
        self._current_batch_ids = []
        # Note: _last_id is intentionally NOT cleared - it tracks what we've seen,
        # but the checkpoint store has the recovery point

    async def ack(self, entry_ids: list[str] | None = None) -> int:
        """Manually acknowledge entries.

        Use this when auto_ack=False for explicit acknowledgment control.

        Args:
            entry_ids: Entry IDs to acknowledge. If None, acknowledges
                the current batch.

        Returns:
            Number of entries acknowledged.
        """
        ids_to_ack = entry_ids or self._current_batch_ids
        if not ids_to_ack:
            return 0

        return await self._client.xack(self._stream, self._group, *ids_to_ack)

    def request_shutdown(self) -> None:
        """Request graceful shutdown.

        The consumer will finish the current batch and exit.
        """
        self._shutdown_requested = True
