"""Explicit, potentially expensive space inspection outside foreground metrics."""

from dataclasses import dataclass

from .memtable import MemTable
from .metrics import IoPurpose, Ratio
from .record import Put, logical_bytes
from .version import VersionSet


@dataclass(frozen=True, slots=True)
class LevelInspection:
    """Committed physical shape of one level."""

    level: int
    tables: int
    bytes: int


@dataclass(frozen=True, slots=True)
class StoreInspection:
    """Literal sizes, with undefined ratios retained rather than invented."""

    levels: tuple[LevelInspection, ...]
    committed_table_bytes: int
    journal_bytes: int
    total_owned_bytes: int
    live_logical_bytes: int
    live_keys: int
    memtable_bytes: int

    @property
    def table_space_amplification(self) -> Ratio:
        """Compare committed table bytes with current live logical bytes."""
        return Ratio(self.committed_table_bytes, self.live_logical_bytes)

    @property
    def total_space_amplification(self) -> Ratio:
        """Include metadata and WAL in physical storage occupancy."""
        return Ratio(self.total_owned_bytes, self.live_logical_bytes)


def inspect_store(version: VersionSet, memtable: MemTable) -> StoreInspection:
    """Scan visible records deliberately; do not retain a second full-key map."""
    live_bytes = 0
    live_keys = 0
    with version.merged(
        version.tables,
        IoPurpose.INSPECTION,
        additional=(memtable.records(),),
    ) as records:
        for record in records:
            if isinstance(record, Put):
                live_bytes += logical_bytes(record)
                live_keys += 1
    owned_bytes = 0
    journal_bytes = 0
    for name in version.store.names():
        with version.store.open_reader(name) as reader:
            owned_bytes += reader.size_bytes
            if name == "WAL":
                journal_bytes = reader.size_bytes
    levels = tuple(
        LevelInspection(
            level,
            sum(table.level == level for table in version.tables),
            sum(table.size_bytes for table in version.tables if table.level == level),
        )
        for level in sorted({table.level for table in version.tables})
    )
    return StoreInspection(
        levels,
        sum(table.size_bytes for table in version.tables),
        journal_bytes,
        owned_bytes,
        live_bytes,
        live_keys,
        memtable.size_bytes,
    )
