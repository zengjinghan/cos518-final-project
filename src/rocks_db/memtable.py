"""MemTable implementation for the LSM engine.

The memtable is the in-memory write buffer where new writes (puts and deletes)
are first recorded before being flushed as SSTables.

This implementation follows the core ideas used in RocksDB:

- Every write produces a new *version* of a key (PUT or DELETE).
- All versions are retained in memory until the memtable flushes.
- A fast index maps each key to its latest version for point lookups.
- When the memtable is flushed, all versions are sorted and written to SSTables,
    and later compaction is responsible for collapsing old versions.

This preserves correct multi-version semantics while keeping the structure simple.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field

from .data_types import ORDER_BY_KEY_SEQ, KeyValueEntry

__all__ = ("MemTable",)


@dataclass(slots=True)
class MemTable:
    """Multi-version in-memory write buffer for the LSM tree.

    Structure:
        _entries : append-only list of ALL KeyValueEntry objects
        _latest  : mapping key -> most recent KeyValueEntry

    This supports:
    - fast Get operations through `_latest`
    - correct flush behavior through `_entries` (all versions)
    """

    write_buffer_size: int
    """Maximum number of bytes allowed before flushing."""

    _entries: list[KeyValueEntry] = field(default_factory=list, init=False)
    """Append-only list containing every version of every written key."""

    _latest: dict[bytes, KeyValueEntry] = field(default_factory=dict, init=False)
    """Index mapping each key to its newest KeyValueEntry."""

    _current_size_bytes: int = field(default=0, init=False)
    """Approximate memory usage of the memtable."""

    # ------------------------------------------------------------------ #
    # Write path
    # ------------------------------------------------------------------ #

    def put(self, key: bytes, value: bytes, seq: int) -> None:
        """Insert a new version of `key` into the memtable.

        A KeyValueEntry is appended to `_entries`, and `_latest` is updated to
        point to this newest version. Older versions remain in `_entries` until
        the next flush.
        """
        entry = KeyValueEntry(key=key, seq=seq, tombstone=False, value=value)
        self._entries.append(entry)
        self._latest[key] = entry
        # Rough size accounting (key + value + small per-entry overhead).
        self._current_size_bytes += len(key) + len(value) + 16

    def delete(self, key: bytes, seq: int) -> None:
        """Insert a delete tombstone for `key`.

        Deletes are represented as KeyValueEntry objects with tombstone=True.
        This tombstone supersedes earlier values in `_latest` and is flushed
        alongside all other versions.
        """
        entry = KeyValueEntry(key=key, seq=seq, tombstone=True, value=None)
        self._entries.append(entry)
        self._latest[key] = entry
        # Tombstones still consume space (key + fixed overhead).
        self._current_size_bytes += len(key) + 16

    # ------------------------------------------------------------------ #
    # Read path
    # ------------------------------------------------------------------ #

    def get(self, key: bytes) -> KeyValueEntry | None:
        """Return the newest version of `key`, or None if absent.

        Lookup is O(1) using the `_latest` index.
        """
        return self._latest.get(key)

    # ------------------------------------------------------------------ #
    # Capacity and reset
    # ------------------------------------------------------------------ #

    def is_full(self) -> bool:
        """Return True if the memtable has reached its capacity."""
        return self._current_size_bytes >= self.write_buffer_size

    def clear(self) -> None:
        """Clear all entries.

        Called after the memtable is flushed into an SSTable.
        """
        self._entries.clear()
        self._latest.clear()
        self._current_size_bytes = 0

    # ------------------------------------------------------------------ #
    # Iteration: used by SSTableBuilder and range scans
    # ------------------------------------------------------------------ #

    def iter_sorted(self) -> Iterable[KeyValueEntry]:
        """Iterate over all stored versions in sorted (key, seq) order.

        This produces the exact input expected by the SSTableBuilder: a fully
        sorted list of all KeyValueEntry objects currently in the memtable.
        """
        return iter(sorted(self._entries, key=ORDER_BY_KEY_SEQ))

    def iter_range(self, start: bytes, end: bytes) -> Iterator[KeyValueEntry]:
        """Iterate over the *latest* versions of all keys in [start, end].

        Although `_entries` contains multiple versions per key, range scans
        over the memtable should reflect the newest visible state, so only
        `_latest[key]` is returned for each matching key.
        """
        keys_in_range = [key for key in self._latest if start <= key <= end]
        for key in sorted(keys_in_range):
            yield self._latest[key]

    def iter_latest_entries(self) -> Iterator[KeyValueEntry]:
        """Iterate over the latest version of every key currently buffered."""
        return iter(self._latest.values())

    @property
    def approximate_size_bytes(self) -> int:
        """Return the current estimated size in bytes."""
        return self._current_size_bytes

    def __len__(self) -> int:
        """Return the number of distinct keys with at least one version stored."""
        return len(self._latest)
