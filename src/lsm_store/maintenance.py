"""Shared flush/compaction execution; policies only choose immutable input plans."""

from .compaction import select_compaction
from .memtable import MemTable
from .metrics import IoPurpose, Metrics
from .storage.journal import Journal
from .table.writer import write_tables
from .version import VersionSet


def flush_memtable(
    version: VersionSet,
    memtable: MemTable,
    journal: Journal,
    sequence: int,
    metrics: Metrics,
) -> None:
    """Checkpoint tables before resetting WAL, then run synchronous maintenance."""
    if len(memtable):
        input_count = len(memtable)
        input_bytes = memtable.size_bytes
        outputs = write_tables(
            version.store,
            memtable.records(),
            0,
            version.allocate_id,
            version.config,
            purpose=IoPurpose.FLUSH,
        )
        version.install((*version.tables, *outputs), sequence, IoPurpose.FLUSH)
        memtable.clear()
        journal.reset()
        if metrics.enabled:
            metrics.maintenance.flushes += 1
            metrics.maintenance.input_entries += input_count
            metrics.maintenance.output_entries += sum(table.entry_count for table in outputs)
            metrics.maintenance.input_bytes += input_bytes
            metrics.maintenance.output_bytes += sum(table.size_bytes for table in outputs)
    while (plan := select_compaction(version.tables, version.config.compaction)) is not None:
        with version.merged(plan.inputs, IoPurpose.COMPACTION) as records:
            outputs = write_tables(
                version.store,
                records,
                plan.destination,
                version.allocate_id,
                version.config,
                purpose=IoPurpose.COMPACTION,
            )
        input_ids = {table.table_id for table in plan.inputs}
        retained = tuple(table for table in version.tables if table.table_id not in input_ids)
        version.install(
            (*retained, *outputs), version.manifest.checkpoint_sequence, IoPurpose.COMPACTION
        )
        if metrics.enabled:
            metrics.maintenance.compactions += 1
            metrics.maintenance.input_entries += sum(table.entry_count for table in plan.inputs)
            metrics.maintenance.output_entries += sum(table.entry_count for table in outputs)
            metrics.maintenance.input_bytes += sum(table.size_bytes for table in plan.inputs)
            metrics.maintenance.output_bytes += sum(table.size_bytes for table in outputs)
