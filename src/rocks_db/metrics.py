"""Metrics types for the LSM engine.

These metrics are collected by the LSMDatabase and by the Disk implementation
to support benchmarking and analysis. They are deliberately simple numeric
counters that can be sampled at the end of a run to compute amplification and
throughput statistics.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import Field

from .disk import DiskMetrics
from .lsm import LSMMetrics

if TYPE_CHECKING:
    from .lsm import LSMDatabase


def compute_read_amplification(lsm: LSMDatabase) -> float:
    """Compute logical read amplification: physical_bytes_read / logical_bytes_read."""
    physical_read = float(lsm.disk.metrics.bytes_read)
    logical_read = float(lsm.metrics.logical_bytes_read) or 1.0
    return physical_read / logical_read


def compute_write_amplification(lsm: LSMDatabase) -> float:
    """Compute logical write amplification: physical_bytes_written / logical_bytes_written."""
    physical_written = float(lsm.disk.metrics.bytes_written)
    logical_written = float(lsm.metrics.logical_bytes_written) or 1.0
    return physical_written / logical_written


def compute_total_sstable_bytes(lsm: LSMDatabase) -> int:
    """Sum of all SSTable file sizes across levels (excludes WAL and memtable)."""
    return sum(meta.size_bytes for level in lsm.levels for meta in level)


def compute_space_amplification(physical_sstable_bytes: int, live_data_bytes: int) -> float:
    """Compute space amplification as physical SSTable bytes / live logical bytes.

    Live logical bytes are maintained incrementally by LSMDatabase, so this
    calculation is O(1) and requires no SSTable scans.
    """
    logical = float(live_data_bytes) or 1.0
    return float(physical_sstable_bytes) / logical


class MetricsSnapshot(LSMMetrics, DiskMetrics):
    """Immutable snapshot of engine + disk metrics and derived amplifications."""

    level_sizes: list[int] = Field(default_factory=list)
    level_sizes_bytes: list[int] = Field(default_factory=list)
    total_sstable_bytes: int = 0
    live_data_bytes: int = 0
    memtable_active_bytes: int = 0
    read_amplification_logical: float = 0.0
    write_amplification_logical: float = 0.0
    space_amplification: float = 0.0

    sstable_num_keys: int = 0
    sstable_num_unique_keys: int = 0


def get_metrics_snapshot(lsm: LSMDatabase) -> MetricsSnapshot:
    """Compute a unified metrics snapshot for an LSMDatabase instance."""
    total_sstable_bytes = compute_total_sstable_bytes(lsm)
    live_data_bytes = lsm.live_data_bytes

    return MetricsSnapshot(
        **lsm.metrics.to_dict(),
        **lsm.disk.metrics.to_dict(),
        level_sizes=[len(level) for level in lsm.levels],
        level_sizes_bytes=[sum(meta.size_bytes for meta in level) for level in lsm.levels],
        total_sstable_bytes=total_sstable_bytes,
        live_data_bytes=live_data_bytes,
        memtable_active_bytes=lsm.memtable_bytes,
        read_amplification_logical=compute_read_amplification(lsm),
        write_amplification_logical=compute_write_amplification(lsm),
        space_amplification=compute_space_amplification(total_sstable_bytes, live_data_bytes),
        sstable_num_keys=lsm.sstable_num_keys,
        sstable_num_unique_keys=lsm.sstable_num_unique_keys,
    )
