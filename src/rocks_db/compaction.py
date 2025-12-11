"""Pluggable compaction strategies for LSMDatabase.

High-level picture
------------------

Compaction is what keeps an LSM tree from degenerating into a pile of
overlapping files. Conceptually, a compaction strategy answers two questions:

1) When should we compact?   (trigger condition, usually "too many files")
2) What should we compact?   (which levels / files and how to rewrite them)

This module defines:

- LevelLayout:
    A small enum that tells the rest of the engine how levels are laid out
    (tiered, leveled, fifo). The LSMDatabase uses this to optimize reads
    (for example, one candidate file per leveled level).

- CompactionStrategy (Protocol):
    An interface that all compaction strategies implement, so the engine
    can call `maybe_compact(db)` without knowing the policy.

- Concrete strategies:
    * NoOpCompaction    : never compacts (for testing).
    * LeveledCompaction : approximate RocksDB-style leveled compaction.
    * TieredCompaction  : size-ratio-based tiered compaction, inspired by
                            RocksDB's Universal style.
    * FifoCompaction    : time-based retention, drop oldest files.
    * HybridCompaction  : placeholder; currently just wraps leveled.

- CompactionType + create_compactor:
    An enum + registry-based factory to instantiate a strategy from a
    configuration.

All strategies operate in-place on an LSMDatabase instance:

- They read SSTables via SSTableReader.
- They create new SSTables with SSTableBuilder.
- They modify `db._levels` and update `db.metrics`.
"""

from __future__ import annotations

import bisect
import heapq
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from enum import Enum
from operator import attrgetter
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from .bloom_filter import BloomFilterConfig
from .data_types import ORDER_BY_KEY_SEQ
from .sstable import SSTableBuilder, SSTableMeta, SSTableReader

if TYPE_CHECKING:
    from .data_types import KeyValueEntry
    from .disk import Disk
    from .lsm import LSMDatabase


__all__ = (
    "LevelLayout",
    "CompactionStrategy",
    "NoOpCompaction",
    "LeveledCompaction",
    "TieredCompaction",
    "FifoCompaction",
    "HybridCompaction",
    "CompactionType",
    "create_compactor",
)


class LevelLayout(Enum):
    """High-level description of how SSTables are organized within a level.

    This is *not* the compaction policy itself, but a summary of its effect
    on the layout of SSTables. The LSMDatabase uses this to decide how to
    search levels during reads:

    - LEVELED:
        For levels L >= 1, files are mostly non-overlapping and sorted by
        min_key. A read can often probe at most one SSTable per level.

    - TIERED:
        Many overlapping files per level, typically unsorted by key range.
        Reads may need to consult several SSTables per level.

    - FIFO:
        Files are ordered by age; compaction drops the oldest ones rather
        than merging. Layout is not about key ranges but about time.
    """

    LEVELED = "leveled"
    """L >= 1: non-overlapping files sorted by min_key (leveled layout)."""

    TIERED = "tiered"
    """Many overlapping files per level; no ordering guarantees."""

    FIFO = "fifo"
    """Log-style retention; oldest files dropped by age, not key layout."""


@runtime_checkable
class CompactionStrategy(Protocol):
    """Strategy interface for pluggable compaction policies.

    CompactionStrategy is intentionally small:

    - level_layout:
        Tells the LSMDatabase how levels will look (tiered, leveled, fifo).

    - maybe_compact(db):
        Called by the engine after memtable flushes or whenever it wants
        to give the strategy a chance to compact.

    The strategy is responsible for:
    - deciding whether compaction is needed, and
    - performing it by rewriting db._levels and updating db.metrics.
    """

    @property
    def level_layout(self) -> LevelLayout:
        """Describe how this strategy organizes SSTables within levels."""
        ...

    def maybe_compact(self, db: LSMDatabase) -> None:
        """Perform compaction work if needed."""
        ...


@dataclass(slots=True)
class NoOpCompaction:
    """A dummy compaction strategy that never compacts.

    This is useful for:
    - unit tests (you can inspect raw flush behavior),
    - benchmarking baseline write performance without compaction noise, or
    - understanding how bad read amplification gets without compaction.
    """

    @property
    def level_layout(self) -> LevelLayout:
        # Whatever layout currently exists; safest to treat as overlapping.
        return LevelLayout.TIERED

    def maybe_compact(self, db: LSMDatabase) -> None:
        # Intentionally does nothing.
        return


def _build_input_iterators_for_run(
    disk: Disk,
    metas: list[SSTableMeta],
) -> list[Iterator[KeyValueEntry]]:
    """Create per-SSTable iterators over sorted entries for a compaction run.

    Filters out empty tables by num_entries / size_bytes.
    """
    return [SSTableReader.iter_entries(disk, meta) for meta in metas if meta.num_entries > 0 and meta.size_bytes > 0]


def _stream_and_deduplicate_entries(
    merged_stream: Iterator[KeyValueEntry],
) -> Iterator[KeyValueEntry]:
    """Deduplicate multiple versions of the same key in a streaming fashion.

    Precondition:
        `merged_stream` is globally sorted by (key ASC, seq ASC).

    For each user key:
        Emit only the entry with the highest sequence number, which is
        guaranteed to be the last entry seen for that key.
    """
    last_key: bytes | None = None
    last_entry: KeyValueEntry | None = None

    for entry in merged_stream:
        if last_key is None:
            last_key = entry.key
            last_entry = entry
            continue

        if entry.key != last_key:
            if last_entry is not None:
                yield last_entry
            last_key = entry.key
            last_entry = entry
        else:
            # Same key; because seq is increasing within a key, the
            # current entry is always the newest version we have seen.
            last_entry = entry

    if last_entry is not None:
        yield last_entry


def _write_output_sstables_shared(
    disk: Disk,
    level: int,
    deduplicated_stream: Iterator[KeyValueEntry],
    target_file_size_base: int,
    bloom_filter_config: BloomFilterConfig,
) -> tuple[list[SSTableMeta], int]:
    """Write a deduplicated stream into SSTables at the given level.

    The stream is written into one or more SSTables, each approximately
    bounded by `target_file_size_base` in size (bytes).

    Returns:
        (new_files, total_output_bytes)
    """
    if target_file_size_base <= 0:
        # Fallback to a large but finite default instead of exploding.
        target_file_size_base = 1 << 30

    new_files: list[SSTableMeta] = []
    current_chunk: list[KeyValueEntry] = []
    current_bytes = 0

    for entry in deduplicated_stream:
        estimated_size = SSTableBuilder.estimate_entry_size(entry)

        # Flush current chunk if it would exceed the target size.
        if current_chunk and (current_bytes + estimated_size > target_file_size_base):
            meta = SSTableBuilder.build(
                disk=disk,
                level=level,
                entries=current_chunk,
                bloom_filter_config=bloom_filter_config,
                entries_are_sorted=True,
            )
            new_files.append(meta)
            current_chunk = []
            current_bytes = 0

        current_chunk.append(entry)
        current_bytes += estimated_size

    # Flush final partial chunk, if any.
    if current_chunk:
        meta = SSTableBuilder.build(
            disk=disk,
            level=level,
            entries=current_chunk,
            bloom_filter_config=bloom_filter_config,
            entries_are_sorted=True,
        )
        new_files.append(meta)

    total_output_bytes = sum(meta.size_bytes for meta in new_files) if new_files else 0
    return new_files, total_output_bytes


@dataclass(slots=True)
class LeveledCompaction:
    """Leveled compaction strategy.

    Responsibilities:

    - Select compaction input files at a given level (L0 vs L1+ policy).
    - Stream input SSTables using block-based iterators.
    - Merge and deduplicate entries in key/sequence order.
    - Write outputs in size-bounded SSTables based on target_file_size_base.
    - Atomically replace input files with outputs and maintain metrics.
    """

    @property
    def level_layout(self) -> LevelLayout:
        return LevelLayout.LEVELED

    def maybe_compact(self, db: LSMDatabase) -> None:
        """Trigger compaction starting from level 0 if over capacity."""
        if db._level_needs_compaction(0):
            self._compact_level_leveled(db, level=0)

    def _compact_level_leveled(self, db: LSMDatabase, level: int) -> None:
        """Orchestrate the compaction pipeline: select -> stream -> write -> install."""
        if not db._levels[level]:
            return

        next_level = level + 1
        db._ensure_level(next_level)

        # Phase 1: selection of compaction input files
        compaction_inputs = self._select_compaction_inputs(db, level, next_level)
        if not compaction_inputs:
            return

        total_input_bytes = sum(meta.size_bytes for meta in compaction_inputs)

        # Phase 2: streaming read and deduplication.
        input_iterators = _build_input_iterators_for_run(db.disk, compaction_inputs)
        if not input_iterators:
            # No live entries at all; drop files and account for logical bytes read.
            self._atomic_install(
                disk=db.disk,
                current_level_files=db._levels[level],
                next_level_files=db._levels[next_level],
                old_files=compaction_inputs,
                new_files=[],
            )
            db.metrics.compactions += 1
            db.metrics.compaction_bytes_read += total_input_bytes
            return

        merged_stream = heapq.merge(*input_iterators, key=ORDER_BY_KEY_SEQ)
        deduplicated_stream = _stream_and_deduplicate_entries(merged_stream)

        # Phase 3: materialize outputs as one or more SSTables.
        new_files, total_output_bytes = _write_output_sstables_shared(
            disk=db.disk,
            level=next_level,
            deduplicated_stream=deduplicated_stream,
            target_file_size_base=db.config.target_file_size_base,
            bloom_filter_config=db.config.bloom_filter,
        )

        # Phase 4: atomic installation (remove inputs, install outputs).
        self._atomic_install(
            disk=db.disk,
            current_level_files=db._levels[level],
            next_level_files=db._levels[next_level],
            old_files=compaction_inputs,
            new_files=new_files,
        )

        # Metrics.
        db.metrics.compactions += 1
        db.metrics.compaction_bytes_read += total_input_bytes
        db.metrics.compaction_bytes_written += total_output_bytes

        # Recursive compaction if the next level now exceeds capacity.
        if new_files and db._level_needs_compaction(next_level):
            self._compact_level_leveled(db, next_level)

    def _select_compaction_inputs(
        self,
        db: LSMDatabase,
        level: int,
        next_level: int,
    ) -> list[SSTableMeta]:
        """Select compaction input files at `level` and overlaps at `next_level`.

        Returns:
            A list of SSTableMeta objects that form the compaction input set.
        """
        level_files = db._levels[level]
        if not level_files:
            return []

        next_level_files = db._levels[next_level]

        # Ensure sorted views for efficient overlap checks.
        level_files.sort(key=attrgetter("min_key"))
        next_level_files.sort(key=attrgetter("min_key"))

        if level == 0:
            primary_inputs = self._select_l0_compaction_inputs(level_files)
        else:
            primary_inputs = self._select_level_compaction_input(level_files, next_level_files)

        if not primary_inputs:
            return []

        # Find overlapping files in the next level for the chosen primary inputs.
        global_min = min(meta.min_key for meta in primary_inputs)
        global_max = max(meta.max_key for meta in primary_inputs)
        overlapping = self._find_overlaps(next_level_files, global_min, global_max)

        return primary_inputs + overlapping

    def _select_l0_compaction_inputs(
        self,
        l0_files: list[SSTableMeta],
    ) -> list[SSTableMeta]:
        """Select compaction input files in level 0.

        Policy:

        - Choose a primary input file (approximate "oldest" via path).
        - Expand to the transitive closure of L0 files whose key ranges
            overlap the current input group.
        """
        primary_file = min(l0_files, key=attrgetter("path"))
        current_group = {primary_file}

        while True:
            group_min = min(meta.min_key for meta in current_group)
            group_max = max(meta.max_key for meta in current_group)

            added_new = False
            for file_meta in l0_files:
                if file_meta in current_group:
                    continue
                if file_meta.overlaps_range(group_min, group_max):
                    current_group.add(file_meta)
                    added_new = True

            if not added_new:
                break

        return list(current_group)

    def _select_level_compaction_input(
        self,
        level_files: list[SSTableMeta],
        next_level_files: list[SSTableMeta],
    ) -> list[SSTableMeta]:
        """Select compaction input files in levels L >= 1.

        Policy:

        - Choose a single primary input file at `level` that minimizes the
            total size (in bytes) of overlapping files at `next_level`.
        """
        selected_file: SSTableMeta | None = None
        best_overlap_bytes = float("inf")

        for candidate_file in level_files:
            overlaps = self._find_overlaps(
                next_level_files,
                candidate_file.min_key,
                candidate_file.max_key,
            )
            overlap_bytes = sum(meta.size_bytes for meta in overlaps)
            if overlap_bytes < best_overlap_bytes:
                best_overlap_bytes = overlap_bytes
                selected_file = candidate_file

        return [selected_file] if selected_file is not None else []

    def _find_overlaps(
        self,
        sorted_files: list[SSTableMeta],
        min_key: bytes,
        max_key: bytes,
    ) -> list[SSTableMeta]:
        """Find files whose key ranges overlap [min_key, max_key] in a sorted level.

        Precondition:
            `sorted_files` is sorted by meta.min_key in ascending order.
        """
        if not sorted_files:
            return []

        key_fn = attrgetter("min_key")

        # Find the first file whose min_key >= min_key (with a one-element lookback).
        idx = bisect.bisect_left(sorted_files, min_key, key=key_fn)
        left = max(0, idx - 1)

        # Find the first file whose min_key > max_key, starting from `left`.
        right = bisect.bisect_right(sorted_files, max_key, lo=left, key=key_fn)

        candidates = sorted_files[left:right]
        return [meta for meta in candidates if meta.overlaps_range(min_key, max_key)]

    def _atomic_install(
        self,
        disk: Disk,
        current_level_files: list[SSTableMeta],
        next_level_files: list[SSTableMeta],
        old_files: list[SSTableMeta],
        new_files: list[SSTableMeta],
    ) -> None:
        """Atomically remove input files and install output files in the next level.

        Args:
            disk:
                The backing Disk used to delete obsolete SSTable files.
            current_level_files:
                The list object representing the current level in the manifest
                (for example, db._levels[level]); this method mutates it in place.
            next_level_files:
                The list object representing the next level in the manifest
                (for example, db._levels[next_level]); also mutated in place.
            old_files:
                SSTables that participated in the compaction run (both the
                primary inputs and any overlapping files from the next level).
            new_files:
                Newly created SSTables that should be installed at the next level.
        """
        # Remove old files from disk and from the two level lists.
        for meta in old_files:
            disk.delete_file(meta.path)

            # Remove from current level if present.
            try:
                current_level_files.remove(meta)
            except ValueError:
                pass

            # Remove from next level if present (for overlapping inputs).
            try:
                next_level_files.remove(meta)
            except ValueError:
                pass

        # Install new files, if any.
        if new_files:
            next_level_files.extend(new_files)
            next_level_files.sort(key=attrgetter("min_key"))


@dataclass(slots=True)
class TieredCompaction:
    """Tiered compaction strategy.

    Intuition:
    ---------
    - Each level can accumulate many SSTables.
    - When a level has "too many" files, we merge *all* of them into
        a new sequence of SSTables in the next level.
    - This can create large write amplification, but is very simple and
        mirrors the original LSM-tree description.

    Layout:
    -------
    - In a pure tiered design, levels are not guaranteed to have
        non-overlapping files or to be sorted by min_key.
    - We therefore report LevelLayout.TIERED.
    """

    @property
    def level_layout(self) -> LevelLayout:
        return LevelLayout.TIERED

    def maybe_compact(self, db: LSMDatabase) -> None:
        """Trigger compaction starting from level 0 if over capacity.

        We check only level 0 here. The helper `_compact_level_tiered`
        recursively propagates compaction to deeper levels as needed.
        """
        if db._level_needs_compaction(0):
            self._compact_level_tiered(db, level=0)

    def _compact_level_tiered(self, db: LSMDatabase, level: int) -> None:
        """Perform a tiered compaction at the given level.

        Algorithm sketch:
        -----------------
        1. Gather all SSTables at `level`.
        2. Merge their entries in global (key, seq) order using block-based
            iterators.
        3. Deduplicate, keeping only the latest version per key.
        4. Write out new SSTables at `level + 1`, splitting as needed
            according to `target_file_size_base`.
        5. Delete old SSTables and install new ones.
        6. Update metrics and recursively compact the next level if needed.
        """
        level_files = db._levels[level]
        if not level_files:
            return

        next_level = level + 1
        db._ensure_level(next_level)

        # We merge all files from this level
        total_input_bytes = sum(meta.size_bytes for meta in level_files)

        # Phase 1: build per-SSTable iterators (block-based).
        input_iterators = _build_input_iterators_for_run(db.disk, level_files)
        if not input_iterators:
            # Nothing live to read. Clear the level and drop on-disk files.
            for meta in level_files:
                db.disk.delete_file(meta.path)
            db._levels[level].clear()
            return

        # Phase 2: merge streams in global (key, seq) order and deduplicate.
        merged_stream = heapq.merge(*input_iterators, key=ORDER_BY_KEY_SEQ)
        deduplicated_stream = _stream_and_deduplicate_entries(merged_stream)

        # Phase 3: write deduplicated entries into SSTables at the next level.
        new_files, total_output_bytes = _write_output_sstables_shared(
            disk=db.disk,
            level=next_level,
            deduplicated_stream=deduplicated_stream,
            target_file_size_base=db.config.target_file_size_base,
            bloom_filter_config=db.config.bloom_filter,
        )

        # Phase 4: remove old SSTables and install new ones in the next level.
        # At this point, the new_files are fully written. We now switch the
        # logical view (db._levels) to point to them.
        db._levels[level] = []
        db._levels[next_level].extend(new_files)

        # Now that the manifest points to the new files, it is safe
        # to delete the old SSTable files from disk.
        for meta in level_files:
            db.disk.delete_file(meta.path)

        # Phase 5: update compaction metrics.
        db.metrics.compactions += 1
        db.metrics.compaction_bytes_read += total_input_bytes
        db.metrics.compaction_bytes_written += total_output_bytes

        # Phase 6: recursively compact next level if over capacity.
        if db._level_needs_compaction(next_level):
            self._compact_level_tiered(db, next_level)


@dataclass(slots=True)
class FifoCompaction:
    """Simplified FIFO compaction strategy.

    Intuition:
    ---------
    FIFO compaction is used for log-like or TTL workloads where we care
    more about "keep only recent data" than about merging old versions.

    Policy:
    -------
    - Only level 0 is used.
    - When level 0 has more files than allowed:
        delete the oldest ones by some ordering (here: path lexicographic
        order, which usually encodes creation order on our Disk).
    - No merging or deduplication is performed.
    """

    @property
    def level_layout(self) -> LevelLayout:
        return LevelLayout.FIFO

    def maybe_compact(self, db: LSMDatabase) -> None:
        level = 0
        metas = db._levels[level]
        max_files = db.config.max_l0_files

        # If we are within capacity, nothing to do.
        if len(metas) <= max_files:
            return

        # Sort by path name (standing in for creation time) and drop oldest.
        ssts = sorted(metas, key=attrgetter("path"))
        to_delete = ssts[:-max_files]

        for meta in to_delete:
            db.disk.delete_file(meta.path)
            db._levels[level].remove(meta)

        db.metrics.compactions += 1
        # FIFO compactions do not merge files, so we do not update
        # compaction_bytes_read / compaction_bytes_written here.


@dataclass(slots=True)
class HybridCompaction:
    """Hybrid compaction strategy (placeholder).

    Right now this simply delegates to LeveledCompaction and reports a
    leveled layout. In a real system this could be extended to:

    - use tiered compaction in upper levels and leveled in deeper levels, or
    - switch policies dynamically based on workload characteristics.
    """

    _leveled: LeveledCompaction = field(default_factory=LeveledCompaction)

    @property
    def level_layout(self) -> LevelLayout:
        # For now we treat hybrid as leveled-style for lookup purposes.
        return LevelLayout.LEVELED

    def maybe_compact(self, db: LSMDatabase) -> None:
        self._leveled.maybe_compact(db)


class CompactionType(str, Enum):
    """Name of a compaction strategy."""

    LEVELED = "leveled"
    """Leveled compaction: maintain a small number of SSTables per level with disjoint key ranges."""

    TIERED = "tiered"
    """Tiered compaction: accumulate many SSTables per level, then merge them."""

    FIFO = "fifo"
    """FIFO compaction: delete oldest files first, used for TTL / log-style workloads."""

    HYBRID = "hybrid"
    """Hybrid strategies that combine aspects of tiered and leveled compaction."""

    NO_OP = "no_op"
    """Disable compaction entirely (for testing / debugging)."""


_COMPACTION_REGISTRY: Mapping[CompactionType, Callable[..., CompactionStrategy]] = {
    CompactionType.LEVELED: LeveledCompaction,
    CompactionType.TIERED: TieredCompaction,
    CompactionType.FIFO: FifoCompaction,
    CompactionType.HYBRID: HybridCompaction,
    CompactionType.NO_OP: NoOpCompaction,
}


def create_compactor(
    type: CompactionType,
    args: dict[str, Any] | None = None,
) -> CompactionStrategy:
    """Instantiate a CompactionStrategy from a CompactionType and kwargs.

    This is the single entry point the benchmark / config layer uses to
    materialize a compaction strategy:

        compactor = create_compactor(cfg.compaction.type, cfg.compaction.args)

    Args:
        type: High-level compaction type (e.g., CompactionType.LEVELED).
        args: Optional keyword arguments forwarded to the strategy constructor.

    Returns:
        A concrete CompactionStrategy instance.
    """
    if args is None:
        args = {}

    try:
        compaction_class = _COMPACTION_REGISTRY[type]
    except KeyError as exc:
        raise ValueError(f"Unknown compaction type: {type!r}") from exc

    return compaction_class(**args)
