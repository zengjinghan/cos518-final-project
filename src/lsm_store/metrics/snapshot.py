"""Immutable measurement windows retain units, provenance and unavailable ratios."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID  # noqa: TC003 -- Pydantic resolves result dataclass fields at runtime.

from lsm_store.config import CaptureMode  # noqa: TC001 -- Used by nested Pydantic result schemas.
from lsm_store.errors import ArgumentError

from .purpose import IoPurpose


def _difference(later: int, earlier: int) -> int:
    difference = later - earlier
    if difference < 0:
        message = "metric window must not subtract a later or larger counter snapshot"
        raise ArgumentError(message)
    return difference


@dataclass(frozen=True, slots=True)
class LogicalStats:
    """Successful logical operations at a snapshot or during one window."""

    puts: int = 0
    deletes: int = 0
    gets: int = 0
    scans: int = 0
    hits: int = 0
    misses: int = 0
    scan_rows: int = 0
    mutation_bytes: int = 0
    returned_bytes: int = 0

    def since(self, earlier: LogicalStats) -> LogicalStats:
        """Subtract an earlier immutable logical observation."""
        return LogicalStats(
            puts=_difference(self.puts, earlier.puts),
            deletes=_difference(self.deletes, earlier.deletes),
            gets=_difference(self.gets, earlier.gets),
            scans=_difference(self.scans, earlier.scans),
            hits=_difference(self.hits, earlier.hits),
            misses=_difference(self.misses, earlier.misses),
            scan_rows=_difference(self.scan_rows, earlier.scan_rows),
            mutation_bytes=_difference(self.mutation_bytes, earlier.mutation_bytes),
            returned_bytes=_difference(self.returned_bytes, earlier.returned_bytes),
        )


@dataclass(frozen=True, slots=True)
class MaintenanceStats:
    """Completed flush/compaction work, separated from physical I/O purposes."""

    flushes: int = 0
    compactions: int = 0
    input_entries: int = 0
    output_entries: int = 0
    input_bytes: int = 0
    output_bytes: int = 0

    def since(self, earlier: MaintenanceStats) -> MaintenanceStats:
        """Subtract an earlier immutable maintenance observation."""
        return MaintenanceStats(
            flushes=_difference(self.flushes, earlier.flushes),
            compactions=_difference(self.compactions, earlier.compactions),
            input_entries=_difference(self.input_entries, earlier.input_entries),
            output_entries=_difference(self.output_entries, earlier.output_entries),
            input_bytes=_difference(self.input_bytes, earlier.input_bytes),
            output_bytes=_difference(self.output_bytes, earlier.output_bytes),
        )


@dataclass(frozen=True, slots=True)
class IoStats:
    """Completed primitive calls and actual transferred bytes for one purpose."""

    read_calls: int = 0
    read_bytes: int = 0
    write_calls: int = 0
    write_bytes: int = 0
    fsync_calls: int = 0
    removals: int = 0

    def since(self, earlier: IoStats) -> IoStats:
        """Subtract an earlier immutable physical observation."""
        return IoStats(
            read_calls=_difference(self.read_calls, earlier.read_calls),
            read_bytes=_difference(self.read_bytes, earlier.read_bytes),
            write_calls=_difference(self.write_calls, earlier.write_calls),
            write_bytes=_difference(self.write_bytes, earlier.write_bytes),
            fsync_calls=_difference(self.fsync_calls, earlier.fsync_calls),
            removals=_difference(self.removals, earlier.removals),
        )


@dataclass(frozen=True, slots=True)
class ReadCacheStats:
    """Successful admissions/reuses and bounded-cache lifecycle work."""

    block_decodes: int = 0
    block_reuses: int = 0
    metadata_decodes: int = 0
    metadata_reuses: int = 0
    admissions: int = 0
    rejections: int = 0
    evictions: int = 0
    invalidations: int = 0

    def since(self, earlier: ReadCacheStats) -> ReadCacheStats:
        """Subtract cumulative work, never a decreasing occupancy gauge."""
        return ReadCacheStats(
            block_decodes=_difference(self.block_decodes, earlier.block_decodes),
            block_reuses=_difference(self.block_reuses, earlier.block_reuses),
            metadata_decodes=_difference(self.metadata_decodes, earlier.metadata_decodes),
            metadata_reuses=_difference(self.metadata_reuses, earlier.metadata_reuses),
            admissions=_difference(self.admissions, earlier.admissions),
            rejections=_difference(self.rejections, earlier.rejections),
            evictions=_difference(self.evictions, earlier.evictions),
            invalidations=_difference(self.invalidations, earlier.invalidations),
        )


@dataclass(frozen=True, slots=True)
class Ratio:
    """Preserve exact nonnegative counts and explicit zero-denominator absence."""

    numerator: int
    denominator: int

    def __post_init__(self) -> None:
        """Reject invalid dimensions, including bool masquerading as an integer."""
        if (
            type(self.numerator) is not int
            or type(self.denominator) is not int
            or self.numerator < 0
            or self.denominator < 0
        ):
            message = "ratio numerator and denominator must be nonnegative integers"
            raise ArgumentError(message)

    @property
    def value(self) -> float | None:
        """Return the ratio, or genuine absence when no denominator was observed."""
        return self.numerator / self.denominator if self.denominator else None


@dataclass(frozen=True, slots=True)
class MetricsWindow:
    """One explicit phase; off mode means uncollected, not zero physical work."""

    mode: CaptureMode
    logical: LogicalStats
    maintenance: MaintenanceStats
    read_cache: ReadCacheStats
    io: tuple[IoStats, ...]

    @property
    def write_amplification(self) -> Ratio:
        """All physical written bytes per successful logical mutation byte."""
        return Ratio(sum(part.write_bytes for part in self.io), self.logical.mutation_bytes)

    @property
    def table_write_amplification(self) -> Ratio:
        """Flush plus compaction table bytes per logical mutation byte."""
        written = self.io[IoPurpose.FLUSH].write_bytes + self.io[IoPurpose.COMPACTION].write_bytes
        return Ratio(written, self.logical.mutation_bytes)

    @property
    def read_amplification(self) -> Ratio:
        """Foreground physical read bytes per logical key/value byte returned."""
        read = self.io[IoPurpose.POINT_READ].read_bytes + self.io[IoPurpose.SCAN].read_bytes
        return Ratio(read, self.logical.returned_bytes)

    @property
    def point_read_calls_per_get(self) -> Ratio:
        """Point-read primitives per get; scans never inflate this numerator."""
        return Ratio(self.io[IoPurpose.POINT_READ].read_calls, self.logical.gets)


@dataclass(frozen=True, slots=True)
class MetricsSnapshot:
    """An immutable cumulative observation tied to one handle's metric origin."""

    origin: UUID
    mode: CaptureMode
    logical: LogicalStats
    maintenance: MaintenanceStats
    read_cache: ReadCacheStats
    io: tuple[IoStats, ...]

    def since(self, earlier: MetricsSnapshot) -> MetricsWindow:
        """Select a phase without global reset/pause state or cross-handle mixing."""
        if self.origin != earlier.origin or self.mode != earlier.mode:
            message = "metric snapshots must belong to the same handle and capture mode"
            raise ArgumentError(message)
        if len(self.io) != len(earlier.io):
            message = "metric snapshots have different physical counter dimensions"
            raise ArgumentError(message)
        return MetricsWindow(
            mode=self.mode,
            logical=self.logical.since(earlier.logical),
            maintenance=self.maintenance.since(earlier.maintenance),
            read_cache=self.read_cache.since(earlier.read_cache),
            io=tuple(
                later.since(previous) for later, previous in zip(self.io, earlier.io, strict=True)
            ),
        )
