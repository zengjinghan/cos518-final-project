"""Immutable compaction decisions over immutable table descriptions."""

from dataclasses import dataclass

from lsm_store.config import CompactionConfig
from lsm_store.table.metadata import TableInfo

from .leveled import leveled_inputs


@dataclass(frozen=True, slots=True)
class CompactionPlan:
    """Replace all inputs with their merged contents at the destination level."""

    inputs: tuple[TableInfo, ...]
    destination: int


def select_compaction(
    tables: tuple[TableInfo, ...],
    config: CompactionConfig,
) -> CompactionPlan | None:
    """Select the shallowest over-budget level, with deterministic tie breaking."""
    for level in sorted({table.level for table in tables}):
        current = tuple(table for table in tables if table.level == level)
        over_budget = (
            len(current) > config.max_l0_tables
            if level == 0
            else sum(table.size_bytes for table in current)
            > config.level_bytes * config.multiplier ** (level - 1)
        )
        if not over_budget:
            continue
        inputs = leveled_inputs(tables, current, level) if config.kind == "leveled" else current
        return CompactionPlan(inputs, level + 1)
    return None
