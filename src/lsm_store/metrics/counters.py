"""Fixed mutable counters owned by one handle, never global instrumentation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import uuid4

from lsm_store.errors import ArgumentError

from .purpose import IoPurpose
from .snapshot import IoStats, LogicalStats, MaintenanceStats, MetricsSnapshot, ReadCacheStats

if TYPE_CHECKING:
    from lsm_store.config import CaptureMode


@dataclass(slots=True)
class LogicalCounters:
    """Count successful public operations and their logical payload bytes."""

    puts: int = 0
    deletes: int = 0
    scans: int = 0
    hits: int = 0
    misses: int = 0
    scan_rows: int = 0
    mutation_bytes: int = 0
    returned_bytes: int = 0


@dataclass(slots=True)
class MaintenanceCounters:
    """Count completed maintenance, not merely selected plans."""

    flushes: int = 0
    compactions: int = 0
    input_entries: int = 0
    output_entries: int = 0
    input_bytes: int = 0
    output_bytes: int = 0


@dataclass(slots=True)
class IoCounters:
    """Count completed filesystem primitives; bytes are not device traffic."""

    read_calls: int = 0
    read_bytes: int = 0
    write_calls: int = 0
    write_bytes: int = 0
    fsync_calls: int = 0
    removals: int = 0


@dataclass(slots=True)
class ReadCacheCounters:
    """Count cache work without per-access clocks or callbacks."""

    block_decodes: int = 0
    block_reuses: int = 0
    metadata_decodes: int = 0
    metadata_reuses: int = 0
    admissions: int = 0
    rejections: int = 0
    evictions: int = 0
    invalidations: int = 0


class Metrics:
    """Allocate counters once; callers guard direct updates with ``enabled``."""

    __slots__ = ("_enabled", "_origin", "io", "logical", "maintenance", "read_cache")

    def __init__(self, mode: CaptureMode = "counters") -> None:
        """Choose collection mode for the entire handle lifetime."""
        if mode not in ("off", "counters"):
            message = f"unsupported instrumentation mode: {mode}"
            raise ArgumentError(message)
        self._enabled = mode == "counters"
        self._origin = uuid4()
        self.logical = LogicalCounters()
        self.maintenance = MaintenanceCounters()
        self.read_cache = ReadCacheCounters()
        self.io = tuple(IoCounters() for _ in IoPurpose)

    @property
    def enabled(self) -> bool:
        """Whether effect/operation owners should update their counters."""
        return self._enabled

    def snapshot(self) -> MetricsSnapshot:
        """Copy fixed counters without clocks, storage effects, or shared mutation."""
        logical = self.logical
        maintenance = self.maintenance
        return MetricsSnapshot(
            origin=self._origin,
            mode="counters" if self._enabled else "off",
            logical=LogicalStats(
                puts=logical.puts,
                deletes=logical.deletes,
                # Every completed get is exactly one hit or miss. Derive the
                # total here instead of maintaining redundant hot-path state.
                gets=logical.hits + logical.misses,
                scans=logical.scans,
                hits=logical.hits,
                misses=logical.misses,
                scan_rows=logical.scan_rows,
                mutation_bytes=logical.mutation_bytes,
                returned_bytes=logical.returned_bytes,
            ),
            maintenance=MaintenanceStats(
                flushes=maintenance.flushes,
                compactions=maintenance.compactions,
                input_entries=maintenance.input_entries,
                output_entries=maintenance.output_entries,
                input_bytes=maintenance.input_bytes,
                output_bytes=maintenance.output_bytes,
            ),
            read_cache=ReadCacheStats(
                block_decodes=self.read_cache.block_decodes,
                block_reuses=self.read_cache.block_reuses,
                metadata_decodes=self.read_cache.metadata_decodes,
                metadata_reuses=self.read_cache.metadata_reuses,
                admissions=self.read_cache.admissions,
                rejections=self.read_cache.rejections,
                evictions=self.read_cache.evictions,
                invalidations=self.read_cache.invalidations,
            ),
            io=tuple(
                IoStats(
                    read_calls=counter.read_calls,
                    read_bytes=counter.read_bytes,
                    write_calls=counter.write_calls,
                    write_bytes=counter.write_bytes,
                    fsync_calls=counter.fsync_calls,
                    removals=counter.removals,
                )
                for counter in self.io
            ),
        )
