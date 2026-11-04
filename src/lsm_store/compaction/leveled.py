"""Range closure for leveled compaction, without storage effects."""

from lsm_store.table.metadata import TableInfo


def leveled_inputs(
    tables: tuple[TableInfo, ...],
    current: tuple[TableInfo, ...],
    level: int,
) -> tuple[TableInfo, ...]:
    """Choose the oldest table and all ranges required for a disjoint output."""
    selected = {min(current, key=lambda table: table.table_id)}
    if level == 0:
        while True:
            minimum = min(table.min_key for table in selected)
            maximum = max(table.max_key for table in selected)
            closure = {
                table for table in current if table.min_key <= maximum and table.max_key >= minimum
            }
            if closure <= selected:
                break
            selected.update(closure)
    minimum = min(table.min_key for table in selected)
    maximum = max(table.max_key for table in selected)
    selected.update(
        table
        for table in tables
        if table.level == level + 1 and table.min_key <= maximum and table.max_key >= minimum
    )
    return tuple(sorted(selected, key=lambda table: table.table_id))
