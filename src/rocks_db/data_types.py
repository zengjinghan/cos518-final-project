"""Shared data structures used across the LSM engine.

These types are intentionally kept free of dependencies on specific storage
components (memtable, SSTable, WAL) so that they can be reused in multiple
layers of the system.
"""

from __future__ import annotations

from operator import attrgetter

import msgspec

__all__ = (
    "ORDER_BY_KEY_SEQ",
    "ORDER_BY_KEY",
    "KeyValueEntry",
)


ORDER_BY_KEY_SEQ = attrgetter("key", "seq")
ORDER_BY_KEY = attrgetter("key")


class KeyValueEntry(msgspec.Struct, frozen=True):
    """A single versioned key-value entry stored in an SSTable.

    Keys and values are raw bytes. Higher-level encoding (e.g. ints, strings)
    is handled by the caller. Sequence numbers enforce version ordering.
    """

    key: bytes
    """Raw byte key. Must be sortable lexicographically."""

    seq: int
    """Sequence number used for versioning. Higher seq replaces older ones."""

    tombstone: bool
    """Whether this entry represents a logical deletion."""

    value: bytes | None
    """Value bytes if not a tombstone."""
