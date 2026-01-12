"""Filtered CDC replication pipeline for polars-redis.

This module provides a high-level abstraction for consuming a source Redis Stream,
applying per-destination Polars filters, and writing to multiple destination clusters.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import polars as pl

from polars_redis._consumer import (
    CheckpointStore,
    ConsumerStats,
    MemoryCheckpointStore,
    StreamConsumer,
)
from polars_redis._multi import (
    Destination,
    DestinationResult,
    MultiWriteResult,
)


@dataclass
class ReplicationDestination:
    """Configuration for a replication destination with filtering.

    Attributes:
        url: Redis connection URL.
        name: Name for this destination (for logging/metrics).
        filter: Polars expression to filter records for this destination.
            If None, all records are replicated.
        key_column: Column containing the Redis key (default: "key").
        key_prefix: Prefix to prepend to all keys.
        transform: Optional function to transform the DataFrame before writing.
        write_type: Type of Redis data to write ("hash" or "json").
        timeout_ms: Timeout for writes in milliseconds.
    """

    url: str
    name: str | None = None
    filter: pl.Expr | None = None
    key_column: str = "key"
    key_prefix: str = ""
    transform: Callable[[pl.DataFrame], pl.DataFrame] | None = None
    write_type: str = "hash"
    timeout_ms: int = 30000

    def __post_init__(self) -> None:
        if self.name is None:
            self.name = self.url.split("://")[-1].split("/")[0]


@dataclass
class ReplicationStats:
    """Statistics for a replication pipeline session."""

    batches_processed: int = 0
    source_entries_read: int = 0
    total_keys_written: int = 0
    total_keys_failed: int = 0
    per_destination_written: dict[str, int] = field(default_factory=dict)
    per_destination_failed: dict[str, int] = field(default_factory=dict)
    start_time: float = field(default_factory=time.time)
    last_batch_time: float | None = None

    @property
    def uptime_seconds(self) -> float:
        return time.time() - self.start_time

    @property
    def entries_per_second(self) -> float:
        if self.uptime_seconds > 0:
            return self.source_entries_read / self.uptime_seconds
        return 0.0


@dataclass
class BatchResult:
    """Result of processing a single batch."""

    source_entries: int
    destination_results: list[DestinationResult]
    duration_ms: float

    @property
    def all_succeeded(self) -> bool:
        return all(r.success for r in self.destination_results)


class ReplicationPipeline:
    """High-level CDC replication pipeline.

    Consumes a source Redis Stream, applies per-destination filters,
    and writes to multiple destination clusters in parallel.

    Example:
        >>> from polars_redis import ReplicationPipeline, ReplicationDestination
        >>> import polars as pl
        >>>
        >>> pipeline = ReplicationPipeline(
        ...     source_url="redis://source:6379",
        ...     stream="changes:user",
        ...     group="replicator",
        ...     consumer="worker-1",
        ... )
        >>>
        >>> # EU cluster gets EU records
        >>> pipeline.add_destination(ReplicationDestination(
        ...     url="redis://eu:6379",
        ...     name="eu-cluster",
        ...     filter=pl.col("region") == "EU",
        ...     key_prefix="user:",
        ... ))
        >>>
        >>> # US cluster gets premium US records
        >>> pipeline.add_destination(ReplicationDestination(
        ...     url="redis://us:6379",
        ...     name="us-premium",
        ...     filter=(pl.col("region") == "US") & (pl.col("tier") == "premium"),
        ... ))
        >>>
        >>> # Run continuously
        >>> await pipeline.run()
    """

    def __init__(
        self,
        source_url: str,
        stream: str,
        group: str,
        consumer: str,
        *,
        checkpoint_store: CheckpointStore | None = None,
        batch_size: int = 1000,
        batch_timeout_ms: int = 100,
        auto_ack: bool = True,
        handle_signals: bool = True,
        schema: dict[str, pl.DataType] | None = None,
    ) -> None:
        """Initialize the replication pipeline.

        Args:
            source_url: Redis URL for the source stream.
            stream: Name of the source stream.
            group: Consumer group name.
            consumer: Consumer name within the group.
            checkpoint_store: Store for persisting checkpoints.
            batch_size: Maximum entries per batch.
            batch_timeout_ms: Timeout for batch collection.
            auto_ack: Automatically acknowledge source messages after successful replication.
            handle_signals: Register SIGTERM/SIGINT handlers for graceful shutdown.
            schema: Schema for casting stream fields.
        """
        self._source_url = source_url
        self._stream = stream
        self._group = group
        self._consumer_name = consumer
        self._checkpoint_store = checkpoint_store or MemoryCheckpointStore()
        self._batch_size = batch_size
        self._batch_timeout_ms = batch_timeout_ms
        self._auto_ack = auto_ack
        self._handle_signals = handle_signals
        self._schema = schema

        self._destinations: list[ReplicationDestination] = []
        self._stats = ReplicationStats()
        self._running = False
        self._shutdown_requested = False

    @property
    def stats(self) -> ReplicationStats:
        """Get replication statistics."""
        return self._stats

    def add_destination(self, destination: ReplicationDestination) -> "ReplicationPipeline":
        """Add a replication destination.

        Args:
            destination: Destination configuration with optional filter.

        Returns:
            Self for method chaining.
        """
        self._destinations.append(destination)
        if destination.name:
            self._stats.per_destination_written[destination.name] = 0
            self._stats.per_destination_failed[destination.name] = 0
        return self

    def request_shutdown(self) -> None:
        """Request graceful shutdown."""
        self._shutdown_requested = True

    async def run(self) -> ReplicationStats:
        """Run the replication pipeline continuously.

        Consumes from the source stream, applies filters, and writes
        to destinations until shutdown is requested.

        Returns:
            Final replication statistics.
        """
        if not self._destinations:
            raise ValueError("No destinations configured. Use add_destination() first.")

        self._running = True
        self._shutdown_requested = False
        self._stats = ReplicationStats()

        # Initialize per-destination stats
        for dest in self._destinations:
            if dest.name:
                self._stats.per_destination_written[dest.name] = 0
                self._stats.per_destination_failed[dest.name] = 0

        consumer = StreamConsumer(
            url=self._source_url,
            stream=self._stream,
            group=self._group,
            consumer=self._consumer_name,
            checkpoint_store=self._checkpoint_store,
            batch_size=self._batch_size,
            batch_timeout_ms=self._batch_timeout_ms,
            auto_ack=False,  # We ACK after successful replication
            auto_checkpoint=False,  # We checkpoint after successful replication
            schema=self._schema,
            handle_signals=self._handle_signals,
        )

        async with consumer:
            async for batch_df in consumer:
                if self._shutdown_requested:
                    break

                batch_result = await self._process_batch(batch_df)

                # Update stats
                self._stats.batches_processed += 1
                self._stats.source_entries_read += batch_result.source_entries
                self._stats.last_batch_time = time.time()

                for dest_result in batch_result.destination_results:
                    name = dest_result.destination.name or "unknown"
                    self._stats.total_keys_written += dest_result.keys_written
                    self._stats.total_keys_failed += dest_result.keys_failed
                    self._stats.per_destination_written[name] = (
                        self._stats.per_destination_written.get(name, 0) + dest_result.keys_written
                    )
                    self._stats.per_destination_failed[name] = (
                        self._stats.per_destination_failed.get(name, 0) + dest_result.keys_failed
                    )

                # ACK and checkpoint if all destinations succeeded
                if batch_result.all_succeeded and self._auto_ack:
                    await consumer.ack()
                    await consumer.commit()

                if self._shutdown_requested:
                    break

        self._running = False
        return self._stats

    async def run_once(self) -> BatchResult | None:
        """Process a single batch and return.

        Useful for testing or controlled batch processing.

        Returns:
            BatchResult if a batch was processed, None if no data available.
        """
        if not self._destinations:
            raise ValueError("No destinations configured. Use add_destination() first.")

        consumer = StreamConsumer(
            url=self._source_url,
            stream=self._stream,
            group=self._group,
            consumer=self._consumer_name,
            checkpoint_store=self._checkpoint_store,
            batch_size=self._batch_size,
            batch_timeout_ms=self._batch_timeout_ms,
            auto_ack=False,
            auto_checkpoint=False,
            schema=self._schema,
            handle_signals=False,
        )

        async with consumer:
            async for batch_df in consumer:
                batch_result = await self._process_batch(batch_df)

                if batch_result.all_succeeded and self._auto_ack:
                    await consumer.ack()
                    await consumer.commit()

                return batch_result

        return None

    async def _process_batch(self, batch_df: pl.DataFrame) -> BatchResult:
        """Process a single batch through all destinations."""
        start = time.perf_counter()
        source_entries = len(batch_df)

        # Process each destination in parallel
        tasks = [self._write_to_destination(batch_df, dest) for dest in self._destinations]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        duration_ms = (time.perf_counter() - start) * 1000
        return BatchResult(
            source_entries=source_entries,
            destination_results=list(results),
            duration_ms=duration_ms,
        )

    async def _write_to_destination(
        self, batch_df: pl.DataFrame, dest: ReplicationDestination
    ) -> DestinationResult:
        """Write filtered batch to a single destination."""
        start = time.perf_counter()

        # Create a Destination object for the result
        multi_dest = Destination(
            url=dest.url,
            name=dest.name,
            key_prefix=dest.key_prefix,
            transform=dest.transform,
            timeout_ms=dest.timeout_ms,
        )

        result = DestinationResult(destination=multi_dest)

        try:
            # Apply filter if specified
            filtered_df = batch_df
            if dest.filter is not None:
                filtered_df = batch_df.filter(dest.filter)

            if len(filtered_df) == 0:
                # No records match filter - success with zero writes
                result.success = True
                result.keys_written = 0
                result.duration_ms = (time.perf_counter() - start) * 1000
                return result

            # Apply transform if specified
            if dest.transform is not None:
                filtered_df = dest.transform(filtered_df)

            # Write to destination
            if dest.write_type == "hash":
                write_result = await self._write_hashes(filtered_df, dest)
            elif dest.write_type == "json":
                write_result = await self._write_json(filtered_df, dest)
            else:
                raise ValueError(f"Unknown write_type: {dest.write_type}")

            result.keys_written = write_result[0]
            result.keys_failed = write_result[1]
            result.success = True

        except asyncio.TimeoutError:
            result.success = False
            result.error = f"Timeout after {dest.timeout_ms}ms"
        except Exception as e:
            result.success = False
            result.error = str(e)

        result.duration_ms = (time.perf_counter() - start) * 1000
        return result

    async def _write_hashes(
        self, df: pl.DataFrame, dest: ReplicationDestination
    ) -> tuple[int, int, list[Any]]:
        """Write DataFrame as Redis hashes."""
        from polars_redis._internal import py_write_hashes

        # Prepare keys
        if dest.key_column not in df.columns:
            raise ValueError(f"Key column '{dest.key_column}' not found in DataFrame")

        keys = [f"{dest.key_prefix}{k}" for k in df[dest.key_column].to_list()]
        field_columns = [c for c in df.columns if c != dest.key_column]

        # Convert values to strings
        values = []
        for i in range(len(df)):
            row_values = []
            for col in field_columns:
                val = df[col][i]
                row_values.append(None if val is None else str(val))
            values.append(row_values)

        # Run in thread pool
        loop = asyncio.get_event_loop()
        return await asyncio.wait_for(
            loop.run_in_executor(
                None,
                lambda: py_write_hashes(dest.url, keys, field_columns, values, None, "replace"),
            ),
            timeout=dest.timeout_ms / 1000,
        )

    async def _write_json(
        self, df: pl.DataFrame, dest: ReplicationDestination
    ) -> tuple[int, int, list[Any]]:
        """Write DataFrame as Redis JSON documents."""
        import json

        from polars_redis._internal import py_write_json

        # Prepare keys
        if dest.key_column not in df.columns:
            raise ValueError(f"Key column '{dest.key_column}' not found in DataFrame")

        keys = [f"{dest.key_prefix}{k}" for k in df[dest.key_column].to_list()]
        field_columns = [c for c in df.columns if c != dest.key_column]

        # Build JSON strings
        json_strings = []
        for i in range(len(df)):
            doc = {}
            for col in field_columns:
                val = df[col][i]
                if val is not None:
                    doc[col] = val
            json_strings.append(json.dumps(doc))

        # Run in thread pool
        loop = asyncio.get_event_loop()
        return await asyncio.wait_for(
            loop.run_in_executor(
                None,
                lambda: py_write_json(dest.url, keys, json_strings, None, "replace"),
            ),
            timeout=dest.timeout_ms / 1000,
        )
