"""Indexed, lazy block reads with a cached immutable table envelope."""

from __future__ import annotations

from bisect import bisect_right
from itertools import islice
from typing import TYPE_CHECKING, Self

from lsm_store import framing
from lsm_store.errors import CorruptionError, LifecycleError, UnsupportedFormatError
from lsm_store.metrics import IoPurpose
from lsm_store.record import RECORD_HEADER_BYTES
from lsm_store.storage.filesystem import table_filename
from lsm_store.table.block import Block, admit_block
from lsm_store.table.codec import FOOTER, TABLE_MAGIC, decode_footer
from lsm_store.table.metadata import (
    BlockInfo,
    TableInfo,
    TableMetadata,
    decode_metadata,
    validate_index,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import TracebackType

    from lsm_store.config import StoreConfig
    from lsm_store.record import Record
    from lsm_store.storage.file import ReadFile
    from lsm_store.storage.filesystem import FileStore
    from lsm_store.table.cache import ReadCache


class TableReader:
    """Own one file handle and index; validate data blocks only when accessed."""

    def __init__(
        self,
        store: FileStore,
        info: TableInfo,
        config: StoreConfig,
        *,
        purpose: IoPurpose = IoPurpose.RECOVERY,
        cache: ReadCache | None = None,
    ) -> None:
        """Admit the complete metadata envelope before it can exclude any keys."""
        self._cache = cache
        if cache is not None:
            cache.check_config(config)
        self._info = info
        self._config = config
        self._max_payload = max(config.block_bytes, config.max_record_bytes + RECORD_HEADER_BYTES)
        self._closed = False
        self._file: ReadFile = store.open_reader(table_filename(info.table_id))
        try:
            self._metadata = self._load_metadata(purpose)
        except BaseException as error:
            self._file.close()
            self._closed = True
            if isinstance(error, CorruptionError) and not isinstance(error, UnsupportedFormatError):
                message = f"table {info.table_id}: {error}"
                raise CorruptionError(message) from error
            raise
        self._minimum_keys = tuple(block.min_key for block in self._metadata.blocks)

    def _load_metadata(self, purpose: IoPurpose) -> TableMetadata:
        if self._file.size_bytes != self._info.size_bytes:
            message = f"table {self._info.table_id}: physical size disagrees with manifest"
            raise CorruptionError(message)
        magic = self._file.read_at(0, len(TABLE_MAGIC), purpose)
        if len(magic) != len(TABLE_MAGIC):
            message = f"table {self._info.table_id}: truncated magic"
            raise CorruptionError(message)
        if magic != TABLE_MAGIC:
            message = f"table {self._info.table_id}: unsupported format magic {magic!r}"
            raise UnsupportedFormatError(message)
        footer_offset = self._info.size_bytes - FOOTER.size
        if footer_offset < len(TABLE_MAGIC):
            message = f"table {self._info.table_id}: truncated table envelope"
            raise CorruptionError(message)
        footer = self._file.read_at(footer_offset, FOOTER.size, purpose)
        length, expected_crc = decode_footer(footer, self._info.size_bytes)
        metadata_offset = footer_offset - length
        payload = self._file.read_at(metadata_offset, length, purpose)
        if len(payload) != length:
            message = f"table {self._info.table_id}: metadata checksum or length mismatch"
            raise CorruptionError(message)
        framing.check_payload(payload, expected_crc)
        metadata = (
            None if self._cache is None else self._cache.metadata(self._info.table_id, payload)
        )
        decoded = metadata is None
        if metadata is None:
            metadata = decode_metadata(payload, self._config.max_record_bytes)
        validate_index(metadata, self._info, metadata_offset, self._max_payload)
        if (metadata.bloom is None) != (self._config.bloom.kind == "off"):
            message = (
                f"table {self._info.table_id}: filter kind disagrees with stored configuration"
            )
            raise CorruptionError(message)
        if decoded and self._cache is not None:
            self._cache.remember_metadata(self._info.table_id, payload, metadata, purpose)
        return metadata

    def _check_open(self) -> None:
        if self._closed:
            message = "table reader is closed"
            raise LifecycleError(message)

    def _read_block(self, block: BlockInfo, purpose: IoPurpose) -> Block:
        self._check_open()
        frame = self._file.read_at(block.offset, block.length, purpose)
        if self._cache is not None:
            cached = self._cache.block(self._info.table_id, block, self._info.max_sequence, frame)
            if cached is not None:
                return cached
        try:
            admitted = admit_block(
                frame,
                block,
                max_sequence=self._info.max_sequence,
                max_payload_bytes=self._max_payload,
                max_record_bytes=self._config.max_record_bytes,
            )
        except CorruptionError as error:
            message = f"table {self._info.table_id}, block offset {block.offset}: {error}"
            raise CorruptionError(message) from error
        if self._cache is not None:
            self._cache.remember_block(self._info.table_id, block.offset, admitted, purpose)
        return admitted

    def get(self, key: bytes, purpose: IoPurpose) -> Record | None:
        """Read at most one relevant block; a Bloom positive still needs lookup."""
        self._check_open()
        if key < self._info.min_key or key > self._info.max_key:
            return None
        bloom = self._metadata.bloom
        if bloom is not None and not bloom.may_contain(key):
            return None
        index = bisect_right(self._minimum_keys, key) - 1
        block = self._metadata.blocks[index]
        if key > block.max_key:
            return None
        return self._read_block(block, purpose).get(key)

    def iter_range(self, start: bytes, end: bytes, purpose: IoPurpose) -> Iterator[Record]:
        """Stream matching blocks only, including tombstones for shared merging."""
        self._check_open()
        if start > end or end < self._info.min_key or start > self._info.max_key:
            return
        first = max(0, bisect_right(self._minimum_keys, start) - 1)
        for block in islice(self._metadata.blocks, first, None):
            if block.min_key > end:
                break
            if block.max_key >= start:
                yield from self._read_block(block, purpose).iter_range(start, end)

    def iter_all(self, purpose: IoPurpose) -> Iterator[Record]:
        """Stream the complete sorted run, checking each accessed block."""
        self._check_open()
        for block in self._metadata.blocks:
            yield from self._read_block(block, purpose).records

    def close(self) -> None:
        """Release the file handle once."""
        if not self._closed:
            self._file.close()
            self._closed = True

    def __enter__(self) -> Self:
        """Return this live reader."""
        self._check_open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close without swallowing a caller's failure."""
        self.close()
