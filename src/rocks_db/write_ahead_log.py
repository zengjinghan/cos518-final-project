"""Write-ahead log (WAL) for the LSM engine.

The write-ahead log provides durability for recent writes that have not yet
been flushed from the memtable to SSTables. On every put or delete, a record
is appended to the WAL file before the memtable is updated. On recovery, the
write-ahead log can be replayed to rebuild the memtable.

This implementation uses a very simple binary format:

    [len][record][len][record]...

where:
- len is a 4-byte big-endian unsigned integer
- record is a msgpack-encoded KeyValueEntry

This format is easy to decode sequentially.
"""

from __future__ import annotations

import struct
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Final

from msgspec.msgpack import decode, encode

from .data_types import KeyValueEntry
from .disk import Disk

__all__ = (
    "WriteAheadLogWriter",
    "WriteAheadLogReader",
)

WRITE_AHEAD_LOG_LEN_PREFIX_SIZE: Final[int] = 4
"""Number of bytes used to store the length prefix of each WAL record."""

WRITE_AHEAD_LOG_LEN_PREFIX_FORMAT: Final[str] = "!I"
"""Struct format string for encoding and decoding the length prefix."""


@dataclass(slots=True)
class WriteAheadLogWriter:
    """Append-only writer for a single write-ahead log (WAL) file.

    Each writer instance manages one log file. The LSMDatabase typically
    uses one write-ahead log per active memtable. Once the memtable is
    flushed to SSTables, the corresponding write-ahead log can be truncated
    or discarded.

    The writer is not thread-safe; external synchronization is assumed.
    """

    disk: Disk
    """Disk backend used to persist the write-ahead log file."""

    path: str
    """Logical path of the write-ahead log file on the Disk."""

    def append(self, entry: KeyValueEntry) -> None:
        """Append a single KeyValueEntry to the write-ahead log.

        The record is encoded as msgpack bytes, prefixed with a 4-byte
        big-endian length, and appended to the write-ahead log file.

        Args:
            entry: The versioned key-value record to log. This is the same
                structure used in SSTables, which simplifies recovery logic.
        """
        record_bytes = encode(entry)
        prefix = struct.pack(WRITE_AHEAD_LOG_LEN_PREFIX_FORMAT, len(record_bytes))
        payload = prefix + record_bytes
        self.disk.append_file(self.path, payload)

    def truncate(self) -> None:
        """Truncate the write-ahead log (WAL) file to zero length.

        This is called after a successful memtable flush: once all logged
        updates have been persisted to SSTables, the existing log contents
        are no longer needed and the file can be cleared.
        """
        self.disk.write_file(self.path, b"")

    def sync(self) -> None:
        """Force the write-ahead log to be durable.

        For the in-memory Disk implementation, sync is effectively a no-op.
        For a real filesystem-backed Disk, this would issue an fsync or
        equivalent call. The method is provided to match designs like
        RocksDB, where users can choose sync versus async writes.
        """
        # Placeholder: a FileSystemDisk implementation could perform fsync here.
        return


@dataclass(slots=True)
class WriteAheadLogReader:
    """Sequential reader for a write-ahead log (WAL) file.

    The reader decodes a log file written by WriteAheadLogWriter by repeatedly
    reading a 4-byte length prefix followed by a msgpack-encoded
    KeyValueEntry. This is intended for recovery and debugging, not for
    hot-path operations.
    """

    disk: Disk
    """Disk backend used to read the write-ahead log file."""

    path: str
    """Logical path of the write-ahead log file on the Disk."""

    def iter_entries(self) -> Iterator[KeyValueEntry]:
        """Iterate over all KeyValueEntry records in the write-ahead log.

        Returns:
            An iterator of KeyValueEntry objects in the order they were written.
        """
        raw = self.disk.read_file(self.path)
        offset = 0
        n = len(raw)

        while offset + WRITE_AHEAD_LOG_LEN_PREFIX_SIZE <= n:
            # Read the length prefix.
            (length,) = struct.unpack(
                WRITE_AHEAD_LOG_LEN_PREFIX_FORMAT,
                raw[offset : offset + WRITE_AHEAD_LOG_LEN_PREFIX_SIZE],
            )
            offset += WRITE_AHEAD_LOG_LEN_PREFIX_SIZE

            if length < 0 or offset + length > n:
                # Truncated or corrupted write-ahead log; stop reading to
                # avoid going out of bounds.
                break

            record_bytes = raw[offset : offset + length]
            offset += length

            entry = decode(record_bytes, type=KeyValueEntry)
            yield entry

    __iter__ = iter_entries
