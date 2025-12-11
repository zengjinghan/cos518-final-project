"""SSTable (Sorted String Table) implementation for the LSM engine.

High level picture
------------------

An SSTable is an immutable, sorted run of key/value entries stored on a Disk.
It is the basic on-disk building block of an LSM tree, just like in LevelDB
and RocksDB.

This version uses a simple block-based layout inspired by RocksDB:

    [header][data blocks][block index][footer]

Blocks store contiguous slices of sorted KeyValueEntry records. A compact
block index records the range and byte offsets of each block so point
lookups can read only the relevant block. The footer stores lengths for the
preceding regions, letting the reader find header/index without scanning.

Bloom filters are maintained in memory only (one per SSTable). They provide
a quick "probably not present" check before we touch table bytes.
"""

from __future__ import annotations

import bisect
import struct
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Final

import msgspec
from msgspec.msgpack import decode, encode

from .bloom_filter import BloomFilterConfig, BloomFilterLike, create_bloom_filter
from .data_types import ORDER_BY_KEY, ORDER_BY_KEY_SEQ, KeyValueEntry
from .disk import Disk

__all__ = (
    "SSTableHeader",
    "BlockIndexEntry",
    "SSTableMeta",
    "SSTableBuilder",
    "SSTableReader",
)


# ---------------------------------------------------------------------------
# Block-level primitives
# ---------------------------------------------------------------------------


class BlockIndexEntry(msgspec.Struct, frozen=True):
    """Describes a single data block inside an SSTable.

    Fields are intentionally small and msgpack-friendly, mirroring RocksDB's
    block index idea but with a minimal footprint:

    - min_key / max_key: inclusive key range covered by this block.
    - offset / length: byte range of the block payload within the data
        blocks region (not counting header/index/footer).
    - num_entries: number of KeyValueEntry records stored in the block.

    Offsets are relative to the *start* of the data blocks region (i.e. right
    after the header). This makes absolute addressing trivial once the header
    length is known.
    """

    min_key: bytes
    max_key: bytes
    offset: int
    length: int
    num_entries: int

    def contains_key(self, key: bytes) -> bool:
        """Return True if key lies within this block's inclusive range."""
        return self.min_key <= key <= self.max_key

    def overlaps_range(self, start: bytes, end: bytes) -> bool:
        """Return True if this block intersects [start, end]."""
        return start <= self.max_key and end >= self.min_key

    @property
    def end_offset(self) -> int:
        return self.offset + self.length


@dataclass(slots=True)
class SSTableHeader:
    """Small structure serialized at the beginning of every SSTable."""

    min_key: bytes
    """Smallest key in the SSTable."""

    max_key: bytes
    """Largest key in the SSTable."""

    num_entries: int
    """Number of KeyValueEntry records in the SSTable."""

    num_blocks: int
    """Number of data blocks stored in this SSTable."""


@dataclass(frozen=True, slots=True)
class SSTableMeta:
    """Lightweight in-memory metadata tracked by the LSM engine.

    SSTables are immutable, so once created this metadata never changes.
    This is similar in spirit to RocksDB's FileMetaData:

    - path / level: locate the file in the LSM layout.
    - min_key / max_key: fast range and overlap checks.
    - entries / size_bytes: size based decisions for compaction.
    - bloom: in memory Bloom filter for point lookups.

    The LSMDatabase keeps only SSTableMeta objects in memory, never raw
    table bytes. All access to the actual file goes through SSTableReader.
    """

    path: str
    """Path of the SSTable file on Disk."""

    level: int
    """LSM level where this SSTable resides."""

    min_key: bytes
    """Smallest key stored in the SSTable."""

    max_key: bytes
    """Largest key stored in the SSTable."""

    num_entries: int
    """Number of KeyValueEntry elements in the SSTable."""

    num_blocks: int
    """Number of data blocks in the SSTable (layout detail)."""

    size_bytes: int
    """Total size of the SSTable file in bytes (header + data + footer)."""

    bloom: BloomFilterLike
    """Bloom filter used to quickly reject keys missing from this SSTable."""

    def contains_key(self, key: bytes) -> bool:
        """Return True if key lies in this table's inclusive key range.

        This is a pure in memory check on min_key / max_key and is the
        first line of defense before touching any table bytes.
        """
        return self.min_key <= key <= self.max_key

    def overlaps_range(self, start: bytes, end: bytes) -> bool:
        """Return True if this table's [min_key, max_key] intersects [start, end].

        This is used by range scans and compaction to decide whether
        an SSTable can contribute any keys in the requested interval.
        """
        return start <= self.max_key and end >= self.min_key

    def __hash__(self) -> int:
        """Generate a hash of the metadata for fast comparisons, excluding bloom filter."""
        # Combine the relevant fields (excluding bloom) to form a string representation of the metadata
        metadata_tuple = (
            self.path,
            self.level,
            self.min_key,
            self.max_key,
            self.num_entries,
            self.num_blocks,
            self.size_bytes,
        )

        # Return the hash of the tuple (this will combine the hash of all fields)
        return hash(metadata_tuple)


class SSTableBuilder:
    """Responsible for constructing SSTables from sorted KeyValueEntry streams."""

    # Heuristic default: ~128 entries per block.
    _DEFAULT_BLOCK_TARGET_ENTRIES: Final[int] = 128

    @staticmethod
    def estimate_entry_size(entry: KeyValueEntry) -> int:
        """Estimate the on-disk size of an entry (key + value + small overhead)."""
        return len(entry.key) + len(entry.value or b"") + 16

    @staticmethod
    def chunk_entries_by_size(
        entries: list[KeyValueEntry],
        target_file_size: int,
    ) -> list[list[KeyValueEntry]]:
        """Split entries into chunks targeting a given file size in bytes."""
        if target_file_size <= 0:
            raise ValueError("target_file_size must be positive")

        chunks: list[list[KeyValueEntry]] = []
        current: list[KeyValueEntry] = []
        current_size = 0

        for entry in entries:
            est = SSTableBuilder.estimate_entry_size(entry)
            if current and current_size + est > target_file_size:
                chunks.append(current)
                current = []
                current_size = 0

            current.append(entry)
            current_size += est

        if current:
            chunks.append(current)

        return chunks

    @staticmethod
    def build(
        disk: Disk,
        level: int,
        entries: list[KeyValueEntry],
        bloom_filter_config: BloomFilterConfig,
        block_target_entries: int = _DEFAULT_BLOCK_TARGET_ENTRIES,
        *,
        entries_are_sorted: bool = False,
    ) -> SSTableMeta:
        """Build and write an SSTable, returning its SSTableMeta.

        Args:
            disk: Disk to write the SSTable bytes to.
            level: LSM level where this SSTable will reside.
            entries: In-memory list of KeyValueEntry records to store.
            enable_bloom_filter: Whether to construct an in-memory Bloom filter.
            block_target_entries: Approximate number of entries per block.
            entries_are_sorted: If True, entries are assumed already sorted by
                ORDER_BY_KEY_SEQ and will not be resorted. This avoids an extra
                sort during compaction.
        """
        if not entries:
            raise ValueError("Cannot build an empty SSTable")

        entries_sorted = entries if entries_are_sorted else sorted(entries, key=ORDER_BY_KEY_SEQ)
        min_key = entries_sorted[0].key
        max_key = entries_sorted[-1].key

        if block_target_entries <= 0:
            raise ValueError("block_target_entries must be positive")

        blocks_bytes = bytearray()
        block_index: list[BlockIndexEntry] = []
        offset = 0

        # Partition sorted entries into blocks.
        block_start = 0

        while block_start < len(entries_sorted):
            block_end = min(block_start + block_target_entries, len(entries_sorted))
            block_entries = entries_sorted[block_start:block_end]

            block_blob = encode(block_entries)
            block_len = len(block_blob)

            blocks_bytes.extend(block_blob)

            block_index.append(
                BlockIndexEntry(
                    min_key=block_entries[0].key,
                    max_key=block_entries[-1].key,
                    offset=offset,
                    length=block_len,
                    num_entries=len(block_entries),
                )
            )

            offset += block_len
            block_start = block_end

        header = SSTableHeader(
            min_key=min_key,
            max_key=max_key,
            num_entries=len(entries_sorted),
            num_blocks=len(block_index),
        )
        header_bytes = encode(header)
        blocks_bytes = bytes(blocks_bytes)
        index_bytes = encode(block_index)

        footer = struct.pack("!QQQ", len(header_bytes), len(blocks_bytes), len(index_bytes))
        full_bytes = header_bytes + blocks_bytes + index_bytes + footer

        path = disk.allocate_path(f"sst_l{level}_")
        disk.write_file(path, full_bytes)

        bloom = create_bloom_filter(bloom_filter_config.type, bloom_filter_config.args)
        bloom.update(e.key for e in entries_sorted)

        return SSTableMeta(
            path=path,
            level=level,
            min_key=min_key,
            max_key=max_key,
            num_entries=len(entries_sorted),
            num_blocks=len(block_index),
            size_bytes=len(full_bytes),
            bloom=bloom,
        )


class SSTableReader:
    """Provides read primitives on top of the raw SSTable bytes."""

    @staticmethod
    def _read_footer(disk: Disk, meta: SSTableMeta) -> tuple[int, int, int]:
        """Read and decode the footer, returning (header_len, blocks_len, index_len)."""
        footer_size = struct.calcsize("!QQQ")
        if meta.size_bytes < footer_size:
            raise ValueError(f"Corrupt SSTable: size too small ({meta.size_bytes})")

        footer_bytes = disk.read_at(meta.path, meta.size_bytes - footer_size, footer_size)
        if len(footer_bytes) != footer_size:
            raise ValueError("Failed to read full SSTable footer")

        header_len, blocks_len, index_len = struct.unpack("!QQQ", footer_bytes)
        return header_len, blocks_len, index_len

    @staticmethod
    def _read_header_blocks_index(disk: Disk, meta: SSTableMeta) -> tuple[bytes, bytes, bytes]:
        """Read header, data blocks, and index slices using footer lengths."""
        header_len, blocks_len, index_len = SSTableReader._read_footer(disk, meta)

        header_bytes = disk.read_at(meta.path, 0, header_len)
        blocks_offset = header_len
        blocks_bytes = disk.read_at(meta.path, blocks_offset, blocks_len)

        index_offset = header_len + blocks_len
        index_bytes = disk.read_at(meta.path, index_offset, index_len)

        return header_bytes, blocks_bytes, index_bytes

    @staticmethod
    def iter_entries(disk: Disk, meta: SSTableMeta) -> Iterator[KeyValueEntry]:
        """Stream all entries in key/seq order, block by block."""
        # This is a generator function; calling it always returns an iterator.
        if meta.size_bytes == 0 or meta.num_entries == 0:
            return

        header_len, blocks_len, index_len = SSTableReader._read_footer(disk, meta)
        index_offset = header_len + blocks_len
        index_bytes = disk.read_at(meta.path, index_offset, index_len)
        block_index = decode(index_bytes, type=list[BlockIndexEntry])

        if not block_index:
            return

        for block in block_index:
            block_offset = header_len + block.offset
            block_bytes = disk.read_at(meta.path, block_offset, block.length)
            if not block_bytes:
                continue
            entries = decode(block_bytes, type=list[KeyValueEntry])
            yield from entries

    @staticmethod
    def load_all(disk: Disk, meta: SSTableMeta) -> tuple[SSTableHeader, list[KeyValueEntry]]:
        """Load the entire SSTable, decoding header and all entries."""
        if meta.size_bytes == 0:
            return SSTableHeader(min_key=b"", max_key=b"", num_entries=0, num_blocks=0), []

        header_bytes, blocks_bytes, index_bytes = SSTableReader._read_header_blocks_index(disk, meta)
        header = decode(header_bytes, type=SSTableHeader)
        block_index = decode(index_bytes, type=list[BlockIndexEntry])

        entries: list[KeyValueEntry] = []
        for block in block_index:
            blob = blocks_bytes[block.offset : block.end_offset]
            block_entries = decode(blob, type=list[KeyValueEntry])
            entries.extend(block_entries)

        return header, entries

    @staticmethod
    def point_lookup(
        disk: Disk,
        meta: SSTableMeta,
        key: bytes,
    ) -> KeyValueEntry | None:
        """Perform a point lookup for a specific key in this SSTable."""
        # 1) Fast range check
        if not meta.contains_key(key):
            return None

        # 2) Bloom filter check
        if key not in meta.bloom:
            return None

        if meta.num_entries == 0:
            return None

        # 3) Use block index to locate the relevant block
        header_len, blocks_len, index_len = SSTableReader._read_footer(disk, meta)
        index_offset = header_len + blocks_len
        index_bytes = disk.read_at(meta.path, index_offset, index_len)
        block_index = decode(index_bytes, type=list[BlockIndexEntry])
        if not block_index:
            return None

        min_keys = [b.min_key for b in block_index]
        idx = bisect.bisect_right(min_keys, key) - 1
        if idx < 0:
            return None

        block = block_index[idx]
        if not block.contains_key(key):
            return None

        # 4) Decode just this block
        block_offset = header_len + block.offset
        block_bytes = disk.read_at(meta.path, block_offset, block.length)
        if not block_bytes:
            return None

        block_entries = decode(block_bytes, type=list[KeyValueEntry])
        if not block_entries:
            return None

        # 5) Binary search within the block by key, then directly pick the last
        #    occurrence (highest seq).
        pos = bisect.bisect_right(block_entries, key, key=ORDER_BY_KEY)

        if pos == 0:
            return None

        candidate = block_entries[pos - 1]
        if candidate.key != key:
            return None

        # Because block_entries are sorted by (key ASC, seq ASC), the last
        # entry for this key in the block is the one with the highest seq.
        return candidate
