"""A latest-only write buffer: old versions are unnecessary without snapshots."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .framing import HEADER_BYTES
from .merge import newest_record
from .record import RECORD_HEADER_BYTES, Record, logical_bytes

if TYPE_CHECKING:
    from collections.abc import Iterator


class MemTable:
    """Own one latest mutation per key and its exact framed-size accounting."""

    __slots__ = ("_records", "_size_bytes")

    def __init__(self) -> None:
        """Start an empty buffer with owned records and byte accounting."""
        self._records: dict[bytes, Record] = {}
        self._size_bytes = 0

    @property
    def size_bytes(self) -> int:
        """Return latest-record encoded bytes, not an estimate of Python heap use."""
        return self._size_bytes

    def apply(self, record: Record) -> None:
        """Install a newer mutation, accepting identical replay duplicates only."""
        previous = self._records.get(record.key)
        if previous is not None:
            if newest_record(previous, record) is previous:
                return
            self._size_bytes -= logical_bytes(previous) + RECORD_HEADER_BYTES + HEADER_BYTES
        self._records[record.key] = record
        self._size_bytes += logical_bytes(record) + RECORD_HEADER_BYTES + HEADER_BYTES

    def get(self, key: bytes) -> Record | None:
        """Return the latest mutation, preserving deletion separately from absence."""
        return self._records.get(key)

    def records(self, start: bytes | None = None, end: bytes | None = None) -> Iterator[Record]:
        """Yield unique keys in increasing order within inclusive optional bounds."""
        for key in sorted(self._records):
            if start is not None and key < start:
                continue
            if end is not None and key > end:
                break
            yield self._records[key]

    def clear(self) -> None:
        """Release checkpointed mutations together with their accounting."""
        self._records.clear()
        self._size_bytes = 0

    def __len__(self) -> int:
        """Return the number of distinct buffered keys, including deletions."""
        return len(self._records)
