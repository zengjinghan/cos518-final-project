"""Core LSM tree implementation.

This module ties together:

- MemTable: in-memory write buffer
- SSTableBuilder / SSTableReader: on-disk sorted runs
- Disk: storage abstraction
- LSMConfig: engine geometry and basic knobs
- LSMMetrics: simple numeric counters
- CompactionStrategy: pluggable compaction policy

High-level flow (RocksDB-style, simplified):

- Writes go to a memtable and optionally a WAL.
- When the memtable is full, it is flushed to level-0 SSTables.
- A CompactionStrategy decides when and how to compact SSTables.
- Reads consult the memtable first, then SSTables from newer to older
    levels. For leveled layouts we can use a single candidate file per
    level + binary search; for tiered layouts we scan overlapping files.
"""

from __future__ import annotations

import bisect
from collections.abc import Iterable
from dataclasses import dataclass, field
from operator import attrgetter

from pydantic import BaseModel, ConfigDict, Field

from .bloom_filter import BloomFilterConfig
from .compaction import CompactionStrategy, LevelLayout
from .data_types import ORDER_BY_KEY_SEQ, KeyValueEntry
from .disk import Disk
from .memtable import MemTable
from .metrics_toggle import MetricsAware, MetricsToggle
from .sstable import SSTableBuilder, SSTableMeta, SSTableReader
from .write_ahead_log import WriteAheadLogWriter

__all__ = (
    "LSMConfig",
    "LSMMetrics",
    "LSMDatabase",
)


class LSMConfig(BaseModel):
    """Configuration for an LSMDatabase instance.

    This model captures only engine geometry and basic knobs.
    Disk and compaction strategy are injected separately via
    dependency injection.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    write_buffer_size: int = Field(..., gt=0)
    """Maximum number of bytes stored in the memtable before a flush."""

    target_file_size_base: int = Field(..., gt=0)
    """Target size in bytes for SSTables produced by flush or compaction."""

    max_l0_files: int = Field(..., gt=0)
    """Maximum number of SSTables allowed in level 0 before triggering compaction."""

    max_bytes_for_level_base: int = Field(..., gt=0)
    """Byte budget for level 1. Deeper levels scale from this base."""

    max_bytes_for_level_multiplier: int = Field(..., gt=1)
    """Growth factor for capacity (in bytes) from one level to the next."""

    write_ahead_log_path: str | None = None
    """
    Logical path used for the write-ahead log (WAL) file on the Disk,
    or None if write-ahead logging is disabled.

    Different LSMDatabase instances sharing the same Disk should use
    distinct write-ahead log paths to avoid interference. In simple
    setups with a single database per Disk, a single path such as
    "wal.log" is sufficient.
    """

    bloom_filter: BloomFilterConfig
    """Whether to build per-SSTable Bloom filters for point lookups."""


class LSMMetrics(BaseModel):
    """Aggregated counters for an LSMDatabase instance."""

    model_config = ConfigDict(extra="forbid")

    user_puts: int = 0
    user_deletes: int = 0
    user_gets: int = 0

    logical_bytes_written: int = 0
    logical_bytes_read: int = 0

    memtable_flushes: int = 0
    compactions: int = 0

    flush_bytes_written: int = 0
    compaction_bytes_read: int = 0
    compaction_bytes_written: int = 0

    write_ahead_log_bytes_written: int = 0

    @property
    def total_ops(self) -> int:
        return self.user_puts + self.user_deletes + self.user_gets

    def reset(self) -> None:
        self.user_puts = 0
        self.user_deletes = 0
        self.user_gets = 0
        self.logical_bytes_written = 0
        self.logical_bytes_read = 0
        self.memtable_flushes = 0
        self.compactions = 0
        self.flush_bytes_written = 0
        self.compaction_bytes_read = 0
        self.compaction_bytes_written = 0
        self.write_ahead_log_bytes_written = 0

    def to_dict(self) -> dict[str, int]:
        return self.model_dump()


@dataclass(slots=True)
class LSMDatabase(MetricsAware[LSMMetrics]):
    """Log-Structured Merge-tree key-value store.

    The LSMDatabase maintains:
        - a mutable memtable (in-memory sorted map),
        - multiple levels of immutable SSTables on disk,
        - optional WAL for durability,
        - a CompactionStrategy that reshapes SSTables over time.

    Writes:
        - go to the memtable (and optionally WAL),
        - trigger a flush to a new level-0 SSTable when the memtable is full,
        - may cause the compaction strategy to run.

    Reads:
        - consult the memtable first (latest state),
        - then walk levels from newest (0) to oldest,
        - for leveled layouts: at most one SSTable per level (binary search),
        - for tiered layouts: potentially many overlapping SSTables per level.

    Keys and values are raw bytes; callers are responsible for encoding.
    """

    config: LSMConfig
    """Engine configuration (memtable size, SSTable size, etc.)."""

    disk: Disk
    """Storage backend used for all SSTables and WAL files."""

    compactor: CompactionStrategy
    """Pluggable compaction policy injected via dependency injection."""

    _memtable: MemTable = field(init=False)
    """Current mutable memtable holding latest in-memory entries."""

    _levels: list[list[SSTableMeta]] = field(init=False)
    """Vector of levels; each level is a list of SSTableMeta objects.

    Invariants (by convention, not strictly enforced everywhere):

    - Level 0 ("L0") behaves like a tiered level with overlapping ranges.
    - Levels L >= 1 may be:
        * leveled: key ranges mostly non-overlapping, sorted by min_key, or
        * tiered-like: many overlapping runs.

        The CompactionStrategy advertises which layout it maintains via
        its `level_layout` attribute.
    """

    _next_seq: int = field(init=False, default=0)
    """Monotonically increasing sequence number for versioning writes."""

    _write_ahead_log: WriteAheadLogWriter | None = field(init=False, default=None)
    """Active WAL writer for the current memtable, if WAL is enabled."""

    _metrics: MetricsToggle[LSMMetrics] = field(init=False, default_factory=lambda: MetricsToggle(LSMMetrics))
    """Metrics manager (counters, enable/disable, live-data bytes)."""

    # This is engine state, not a metric, so it should not be togglable.
    _live_bytes_by_key: dict[bytes, int] = field(init=False, default_factory=dict)
    _live_data_bytes: int = field(init=False, default=0)

    # ------------------------------------------------------------------
    # Construction and basic helpers
    # ------------------------------------------------------------------
    def __post_init__(self) -> None:
        """Initialize internal structures after dataclass construction.

        - Create an empty memtable with the configured capacity.
        - Initialize an empty level-0 (no SSTables yet).
        - If WAL is enabled, attach a WriteAheadLogWriter to this Disk.
        """
        # Start with an empty memtable and an empty level-0.
        self._memtable = MemTable(write_buffer_size=self.config.write_buffer_size)
        self._levels = [[]]

        # Create or open the write-ahead log if a path is configured.
        if self.config.write_ahead_log_path is not None:
            self._write_ahead_log = WriteAheadLogWriter(
                disk=self.disk,
                path=self.config.write_ahead_log_path,
            )

    def _allocate_seq(self) -> int:
        """Allocate a new, monotonically increasing sequence number.

        In RocksDB, the sequence number space is global and strictly
        increasing. Newer writes always receive larger sequence numbers,
        and compaction preserves this order across SSTables.
        """
        self._next_seq += 1
        return self._next_seq

    # ------------------------------------------------------------------
    # Flush + WAL helpers
    # ------------------------------------------------------------------
    def _flush_memtable(self) -> None:
        """Flush the current memtable to a new SSTable in level 0.

        This operation:

        1) Converts all memtable entries into a sorted sequence of
            KeyValueEntry records.
        2) Calls SSTableBuilder to create an immutable SSTable on Disk.
        3) Appends the resulting SSTableMeta to level 0.
        4) Clears the memtable.
        5) Updates flush-related metrics.
        6) Truncates the WAL (if present).
        7) Invokes the compaction strategy to potentially compact levels.
        """
        entries = list(self._memtable.iter_sorted())
        if not entries:
            # Nothing to flush (no user-visible writes yet).
            return

        total_flush_bytes = 0
        chunks = SSTableBuilder.chunk_entries_by_size(entries, self.config.target_file_size_base)

        for chunk in chunks:
            meta = SSTableBuilder.build(
                disk=self.disk,
                level=0,
                entries=chunk,
                bloom_filter_config=self.config.bloom_filter,
            )
            self._levels[0].append(meta)
            total_flush_bytes += meta.size_bytes

        # Clear memtable after a successful flush.
        self._memtable.clear()

        # Update metrics reflecting the flush.
        self.metrics.memtable_flushes += 1
        self.metrics.flush_bytes_written += total_flush_bytes

        # After the memtable is flushed and the corresponding updates are
        # safely stored in SSTables, we can reset the write-ahead log.
        self._reset_write_ahead_log()

        # Let the compaction strategy decide what to do next.
        self.compactor.maybe_compact(self)

    def _log_entry(self, entry: KeyValueEntry) -> None:
        """Append an internal KeyValueEntry to the WAL if enabled.

        Logging must happen before the corresponding update is made to the
        memtable so that a crash after logging but before updating in-memory
        state can be recovered by replaying the log.
        """
        if self._write_ahead_log is not None:
            self._write_ahead_log.append(entry)

    def _reset_write_ahead_log(self) -> None:
        """Reset (truncate) the write-ahead log after a successful flush.

        Once the memtable has been flushed to SSTables, all entries that were
        present in the write-ahead log are now durable. Truncating the log
        keeps it bounded and ensures it only contains updates that have not
        yet been flushed.
        """
        if self._write_ahead_log is not None:
            self._write_ahead_log.truncate()

    # ------------------------------------------------------------------
    # Level management helpers
    # ------------------------------------------------------------------
    def _ensure_level(self, level: int) -> None:
        """Ensure that `_levels` has at least (level + 1) entries.

        This is a small utility used by compaction strategies when they
        want to create or write into deeper levels that may not exist yet.
        """
        while len(self._levels) <= level:
            self._levels.append([])

    def _level_size_bytes(self, level: int) -> int:
        """Return the total SSTable size in bytes for a level."""
        return sum(meta.size_bytes for meta in self._levels[level])

    def _level_max_bytes(self, level: int) -> int:
        """Return the byte budget for a level (L1 and deeper)."""
        if level == 0:
            return 0

        multiplier = self.config.max_bytes_for_level_multiplier
        return self.config.max_bytes_for_level_base * (multiplier ** (level - 1))

    def _level_needs_compaction(self, level: int) -> bool:
        """Return True if the level exceeds RocksDB-like size thresholds."""
        if level == 0:
            return len(self._levels[0]) > self.config.max_l0_files

        return self._level_size_bytes(level) > self._level_max_bytes(level)

    # ------------------------------------------------------------------
    # Leveled lookup helper
    # ------------------------------------------------------------------
    @staticmethod
    def _find_sstable_for_key_sorted(
        metas: list[SSTableMeta],
        key: bytes,
    ) -> SSTableMeta | None:
        """Find the SSTable in a leveled, sorted level that could contain key.

        Intended for leveled layouts (L >= 1), where:
        - SSTable key ranges are mostly disjoint within a level.
        - `metas` can be treated as sorted by `min_key`.

        Algorithm:
        - Build a parallel list of min_keys.
        - Use `bisect_right` to find the last file whose min_key <= key.
        - Check that the candidate's range actually covers `key`.

        Note:
            For clarity, this function re-sorts the list by min_key on every call.
            In a higher-performance version, you would maintain the invariant
            that metas are already sorted and avoid this sort.
        """
        if not metas:
            return None

        # Keep a sorted view by min_key. This is O(n log n), but simple.
        metas_sorted = sorted(metas, key=attrgetter("min_key"))
        min_keys = [meta.min_key for meta in metas_sorted]

        # Find rightmost min_key <= key.
        idx = bisect.bisect_right(min_keys, key) - 1
        if idx < 0:
            return None

        candidate = metas_sorted[idx]
        if candidate.min_key <= key <= candidate.max_key:
            return candidate

        return None

    # ------------------------------------------------------------------
    # SSTable probing helper
    # ------------------------------------------------------------------
    def _probe_sstables_for_key(
        self,
        metas: Iterable[SSTableMeta],
        key: bytes,
    ) -> KeyValueEntry | None:
        """Try a sequence of SSTables for a key and return the first hit.

        For each SSTable:

        - SSTableReader.point_lookup() performs:
            * range checks (min_key / max_key),
            * Bloom filter checks (if enabled),
            * an in-table search to find the newest version of the key.

        We simply iterate in the desired priority order and return the
        first non-None result.
        """
        for meta in metas:
            found = SSTableReader.point_lookup(self.disk, meta, key)
            if found is not None:
                return found

        return None

    # ------------------------------------------------------------------
    # Public write API
    # ------------------------------------------------------------------
    def put(self, key: bytes, value: bytes) -> None:
        """Insert or update a key-value pair.

        - Assigns a new global sequence number.
        - Logs the write to the WAL (if enabled).
        - Inserts into the memtable.
        - Triggers a memtable flush if it is full.
        """
        seq = self._allocate_seq()
        entry = KeyValueEntry(key=key, seq=seq, tombstone=False, value=value)

        # Log first, then apply to the memtable.
        self._log_entry(entry)
        self._memtable.put(key=key, value=value, seq=seq)
        self._update_live_bytes(key=key, value=value, tombstone=False)

        # Update metrics for the logical write.
        self.metrics.user_puts += 1
        self.metrics.logical_bytes_written += len(key) + len(value)

        # Flush when the memtable hits its capacity.
        if self._memtable.is_full():
            self._flush_memtable()

    def delete(self, key: bytes) -> None:
        """Logically delete a key (via tombstone).

        Deletes are represented by tombstone entries both in the memtable
        and in SSTables after a flush or compaction. Readers interpret a
        tombstone as "key not present" even if older values exist in lower
        levels.

        Args:
            key: Encoded user key.
        """
        seq = self._allocate_seq()
        entry = KeyValueEntry(key=key, seq=seq, tombstone=True, value=None)

        self._log_entry(entry)
        self._memtable.delete(key=key, seq=seq)
        self._update_live_bytes(key=key, value=None, tombstone=True)

        self.metrics.user_deletes += 1
        self.metrics.logical_bytes_written += len(key)

        if self._memtable.is_full():
            self._flush_memtable()

    # ------------------------------------------------------------------
    # Public read API: point lookup
    # ------------------------------------------------------------------
    def get(self, key: bytes) -> bytes | None:
        """Retrieve the latest value for a key.

        Lookup order:

        1) Memtable (newest in-memory state).
        2) Level 0 SSTables (many overlapping files, searched newest -> oldest).
        3) Deeper levels (L >= 1):
            - If the compaction strategy exposes a LEVELED layout:
                at most one candidate file per level, found via binary
                search on min_key.
            - Otherwise:
                fall back to scanning all SSTables in that level
                (again newest -> oldest).

        Args:
            key: Encoded user key.
        """
        # ------------------------------------------------------------------
        # 1) Memtable: always check here first.
        #
        # The memtable holds the latest writes that have not yet been flushed
        # to SSTables, so if a key is present here, it is the freshest version.
        # ------------------------------------------------------------------
        entry = self._memtable.get(key)
        if entry is not None:
            self.metrics.user_gets += 1

            # Tombstone in memtable means "logically deleted" even if older
            # versions exist on disk.
            if entry.tombstone:
                return None

            # Count logical bytes returned to the caller for read amplification.
            if entry.value is not None:
                self.metrics.logical_bytes_read += len(entry.value)

            return entry.value

        # ------------------------------------------------------------------
        # 2) Walk SSTable levels from newest (L0) to oldest (deepest level).
        #
        # Level 0:
        #   - Many overlapping files (tiered-style), so we must examine
        #     multiple SSTables, typically newest first.
        #
        # Levels L >= 1:
        #   - If the compaction strategy maintains a leveled layout, key ranges
        #     are mostly disjoint within each level. We can then binary-search
        #     the per-level SSTable list by min_key and probe at most one file.
        #   - If not leveled, we fall back to the tiered-style "scan all"
        #     behavior.
        # ------------------------------------------------------------------
        for level, metas in enumerate(self._levels):
            if not metas:
                # Skip empty levels quickly.
                continue

            if level >= 1 and self.compactor.level_layout is LevelLayout.LEVELED:
                # Leveled layout in deeper levels: single candidate file per level.
                # _find_sstable_for_key_sorted assumes `metas` is sorted by min_key.
                meta = self._find_sstable_for_key_sorted(metas, key)
                if meta is None:
                    # No file in this level can contain the key; move to the next level.
                    continue

                candidates: Iterable[SSTableMeta] = (meta,)
            else:
                # Level 0 (always overlapping) or non-leveled strategies:
                # many potentially overlapping files. We search newest first
                # by walking the list in reverse.
                candidates = reversed(metas)

            # Probe the chosen SSTable(s) in this level for the key.
            # This helper encapsulates:
            #   - Bloom-filter checks (inside point_lookup),
            #   - On-disk table reads,
            #   - Version resolution within a single SSTable.
            found = self._probe_sstables_for_key(candidates, key)
            if found is None:
                # Nothing in this level; try older levels.
                continue

            # ------------------------------------------------------------------
            # 3) Common success path: we found the newest visible version.
            # ------------------------------------------------------------------
            self.metrics.user_gets += 1

            if found.tombstone:
                # Latest version is a delete; key is logically absent.
                return None

            if found.value is not None:
                self.metrics.logical_bytes_read += len(found.value)

            return found.value

        # ----------------------------------------------------------------------
        # 4) Key was not found in memtable or any SSTable.
        #
        # We still count this as a user_get so that the total logical read
        # count matches the number of `get` calls issued by the workload.
        # ----------------------------------------------------------------------
        self.metrics.user_gets += 1
        return None

    # ------------------------------------------------------------------
    # Public read API: range scan
    # ------------------------------------------------------------------
    def scan(self, start: bytes, end: bytes) -> list[tuple[bytes, bytes]]:
        """Range scan over [start, end], returning latest visible key-values.

        Semantics:

        - Returns (key, value) pairs in key order.
        - For each key in [start, end], the *latest* visible version is chosen.
        - Keys whose latest version is a tombstone are omitted.

        Implementation (teaching version):

        1) Collect candidate KeyValueEntry records from:
            - memtable entries in [start, end],
            - all SSTables whose key ranges overlap [start, end].
        2) Sort candidates by (key, seq).
        3) For each key, keep only the record with the highest seq.
        4) Drop keys whose latest record is a tombstone.

        Note:
            This is correct but not IO-optimal. Production systems use
            multi-way merged iterators that stream from memtable + SSTables
            in key order without materializing everything in memory.
        """
        if start > end:
            # Empty range by definition.
            return []

        # 1) Collect candidates from the memtable.
        #
        # The memtable stores at most one KeyValueEntry per user key,
        # so there is no need for version resolution within this subset.
        candidates: list[KeyValueEntry] = list(self._memtable.iter_range(start, end))

        # 2) Collect candidates from SSTables whose key ranges overlap
        #    the requested scan interval.
        #
        # We reuse min_key / max_key metadata to skip tables that cannot
        # contain relevant keys. For simplicity, we load each matching
        # SSTable fully and filter in memory.
        for level_metas in self._levels:
            for meta in level_metas:
                # Fast reject by range.
                if not meta.overlaps_range(start, end):
                    continue

                _, entries = SSTableReader.load_all(self.disk, meta)
                candidates.extend(entry for entry in entries if start <= entry.key <= end)

        if not candidates:
            # Nothing matched across memtable + all SSTables.
            return []

        # 3) Sort by (key, seq) so that all versions of a key appear
        # together in increasing sequence order. This lets us keep only
        # the latest version per key (the one with the highest seq).
        candidates.sort(key=ORDER_BY_KEY_SEQ)

        # 4) Walk the sorted list and, per key, remember the entry with
        # the highest seq. Then emit its value if the latest entry is not
        # a tombstone.
        results: list[tuple[bytes, bytes]] = []
        last_key: bytes | None = None
        last_entry: KeyValueEntry | None = None

        for entry in candidates:
            if last_key is None or entry.key != last_key:
                # New key: flush the previous key's latest entry, if any.
                if last_entry is not None and not last_entry.tombstone:
                    assert last_entry.value is not None
                    results.append((last_entry.key, last_entry.value))

                last_key = entry.key
                last_entry = entry
            else:
                # Same key: keep whichever entry has the larger seq.
                #
                # Because we sorted by (key, seq) ascending, this branch
                # effectively just tracks the last entry seen for this key.
                assert last_entry is not None
                if entry.seq > last_entry.seq:
                    last_entry = entry

        # Flush the final key's latest entry.
        if last_entry is not None and not last_entry.tombstone:
            assert last_entry.value is not None
            results.append((last_entry.key, last_entry.value))

        # 5) Update logical read metrics to reflect the amount of data
        # returned to the caller. Disk-level counters (bytes_read, read_ops)
        # are tracked by the Disk implementation.
        total_value_bytes = sum(len(v) for _, v in results)
        self.metrics.logical_bytes_read += total_value_bytes

        return results

    # ------------------------------------------------------------------
    # Introspection helpers (useful for tests / benchmarks)
    # ------------------------------------------------------------------
    @property
    def levels(self) -> tuple[tuple[SSTableMeta, ...], ...]:
        """Return the current SSTable layout for all levels.

        The return type is fully immutable (tuple of tuples) so that callers
        cannot mutate internal state accidentally.

        Typical uses:

        - test assertions about number of files per level,
        - plotting how compaction changes the level shape,
        - debugging compaction strategies.
        """
        return tuple(tuple(level) for level in self._levels)

    @property
    def memtable_size(self) -> int:
        """Return the number of distinct keys currently stored in the memtable."""
        return len(self._memtable)

    @property
    def memtable_bytes(self) -> int:
        """Approximate current memtable usage in bytes."""
        return self._memtable.approximate_size_bytes

    def iter_memtable_latest(self) -> Iterable[KeyValueEntry]:
        """Iterate over the latest version of every key in the memtable."""
        return self._memtable.iter_latest_entries()

    @property
    def sstable_num_keys(self) -> int:
        """Total number of KeyValueEntry records stored across all SSTables.

        This counts every on-disk version, including tombstones and older
        versions of a key that have not yet been compacted away.
        """
        return sum(meta.num_entries for level in self._levels for meta in level)

    @property
    def sstable_num_unique_keys(self) -> int:
        """Total number of distinct user keys stored across all SSTables.

        This performs a full scan of all SSTables and should be used only
        for offline diagnostics or benchmarking, not on the hot path.
        """
        unique_keys: set[bytes] = set()
        with self.manage_metrics(False), self.disk.manage_metrics(False):
            assert not self.disk.metrics_enabled
            for level_metas in self._levels:
                for meta in level_metas:
                    _, entries = SSTableReader.load_all(self.disk, meta)
                    unique_keys.update(e.key for e in entries)

        return len(unique_keys)

    def _update_live_bytes(self, key: bytes, value: bytes | None, tombstone: bool) -> None:
        """Update live logical byte counters for the latest version of a key."""
        # Operates on self._live_bytes_by_key directly.
        # This ensures we track DB size even if 'metrics' are paused.
        old = self._live_bytes_by_key.get(key, 0)
        if tombstone:
            new = 0
            self._live_bytes_by_key.pop(key, None)
        else:
            assert value is not None
            new = len(key) + len(value)
            self._live_bytes_by_key[key] = new

        self._live_data_bytes += new - old

    @property
    def live_data_bytes(self) -> int:
        """Total logical bytes across the newest version of each key."""
        return self._live_data_bytes
