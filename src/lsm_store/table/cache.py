"""One handle-owned, byte-bounded LRU of exact-image validation results."""

from collections import OrderedDict
from dataclasses import dataclass
from sys import getsizeof

from lsm_store.config import StoreConfig
from lsm_store.errors import ArgumentError
from lsm_store.metrics import IoPurpose, Metrics
from lsm_store.record import Put
from lsm_store.table.block import Block
from lsm_store.table.metadata import BlockInfo, TableMetadata


@dataclass(frozen=True, slots=True)
class ReadCacheInfo:
    """Conservative retained-object charges, not process RSS or transient memory."""

    capacity_bytes: int
    retained_bytes: int
    entries: int


@dataclass(frozen=True, slots=True)
class _MetadataImage:
    image: bytes
    metadata: TableMetadata


type _Key = int | tuple[int, int]
type _Image = Block | _MetadataImage


@dataclass(frozen=True, slots=True)
class _Entry:
    value: _Image
    charge_bytes: int


def _metadata_bytes(value: _MetadataImage) -> int:
    metadata = value.metadata
    size = getsizeof(value) + getsizeof(value.image) + getsizeof(metadata)
    size += sum(
        map(
            getsizeof,
            (
                metadata.table_id,
                metadata.level,
                metadata.min_key,
                metadata.max_key,
                metadata.max_sequence,
                metadata.entry_count,
                metadata.blocks,
            ),
        )
    )
    for block in metadata.blocks:
        size += getsizeof(block) + sum(
            map(
                getsizeof,
                (block.min_key, block.max_key, block.offset, block.length, block.entry_count),
            )
        )
    if metadata.bloom is not None:
        size += getsizeof(metadata.bloom)
        size += getsizeof(metadata.bloom.bits) + getsizeof(metadata.bloom.hash_count)
    return size


def _block_bytes(block: Block) -> int:
    size = getsizeof(block) + getsizeof(block.image)
    size += getsizeof(block.records) + getsizeof(block.keys)
    for record in block.records:
        # keys aliases record.key: charge its bytes once, but both tuple slots.
        size += getsizeof(record) + getsizeof(record.key) + getsizeof(record.sequence)
        if isinstance(record, Put):
            size += getsizeof(record.value)
    index = block.index
    size += getsizeof(index) + getsizeof(block.max_sequence)
    size += sum(
        map(
            getsizeof, (index.min_key, index.max_key, index.offset, index.length, index.entry_count)
        )
    )
    return size


class ReadCache:
    """Reuse only equal current images; never replace reads with cached answers.

    One cache is scoped to one version set and its immutable configuration.
    Metadata uses table IDs; blocks use (table ID, offset). Table replacement
    invalidates both. Shared objects are conservatively charged more than once.
    Fixed empty-owner overhead and temporary parsing allocations are excluded.
    """

    def __init__(self, config: StoreConfig, metrics: Metrics) -> None:
        """Choose the validated runtime budget once for this handle."""
        self._limits = (config.block_bytes, config.max_record_bytes)
        self._capacity = 0 if config.read_cache.kind == "off" else config.read_cache.capacity_bytes
        self._metrics = metrics
        self._entries: OrderedDict[_Key, _Entry] = OrderedDict()
        self._empty_bytes = getsizeof(self._entries)
        self._charged = 0

    def check_config(self, config: StoreConfig) -> None:
        """Forbid reusing an admission proof under different decoding limits."""
        if self._limits != (config.block_bytes, config.max_record_bytes):
            raise ArgumentError("read cache decoding limits disagree with its owner")

    def info(self) -> ReadCacheInfo:
        """Observe cache state without walking entries or performing I/O."""
        return ReadCacheInfo(self._capacity, self._retained_bytes(), len(self._entries))

    def metadata(self, table_id: int, image: bytes) -> TableMetadata | None:
        """Reuse admitted metadata only after the caller checks its fresh envelope."""
        value = self._lookup(table_id, image)
        if value is None:
            return None
        if not isinstance(value, _MetadataImage):
            raise TypeError("metadata cache key refers to a block")
        if self._metrics.enabled:
            self._metrics.read_cache.metadata_reuses += 1
        return value.metadata

    def remember_metadata(
        self, table_id: int, image: bytes, metadata: TableMetadata, purpose: IoPurpose
    ) -> None:
        """Retain a completely admitted metadata image if it fits the budget."""
        if self._metrics.enabled:
            self._metrics.read_cache.metadata_decodes += 1
        if self._retains(purpose):
            value = _MetadataImage(image, metadata)
            self._remember(table_id, value, _metadata_bytes(value))

    def block(
        self, table_id: int, index: BlockInfo, max_sequence: int, image: bytes
    ) -> Block | None:
        """Require exact bytes, not only a checksum or a cache key, on every reuse."""
        value = self._lookup((table_id, index.offset), image)
        if value is None:
            return None
        if not isinstance(value, Block):
            raise TypeError("block cache key refers to metadata")
        if value.index != index or value.max_sequence != max_sequence:
            self._drop((table_id, index.offset))
            if self._metrics.enabled:
                self._metrics.read_cache.invalidations += 1
            return None
        if self._metrics.enabled:
            self._metrics.read_cache.block_reuses += 1
        return value

    def remember_block(self, table_id: int, offset: int, block: Block, purpose: IoPurpose) -> None:
        """Account for the complete retained image and decoded records, not payload alone."""
        if self._metrics.enabled:
            self._metrics.read_cache.block_decodes += 1
        if self._retains(purpose):
            self._remember((table_id, offset), block, _block_bytes(block))

    def invalidate(self, table_id: int) -> None:
        """Release all images belonging to a reclaimed immutable table."""
        keys = [
            key for key in self._entries if (key if isinstance(key, int) else key[0]) == table_id
        ]
        for key in keys:
            self._drop(key)
            if self._metrics.enabled:
                self._metrics.read_cache.invalidations += 1

    def clear(self) -> None:
        """Release all retained objects at handle close."""
        self._entries.clear()
        self._charged = 0

    def _retains(self, purpose: IoPurpose) -> bool:
        # Successful compaction reclaims every input table; failure faults its
        # owner. Reuse existing admissions, but never retain one-pass input data.
        return self._capacity > 0 and purpose != IoPurpose.COMPACTION

    def _lookup(self, key: _Key, image: bytes) -> _Image | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        if entry.value.image != image:
            self._drop(key)
            if self._metrics.enabled:
                self._metrics.read_cache.invalidations += 1
            return None
        self._entries.move_to_end(key)
        return entry.value

    def _remember(self, key: _Key, value: _Image, size: int) -> None:
        # Include the key, its scalar fields, entry, and retained charge integer.
        size += getsizeof(key) + getsizeof(_Entry(value, 0)) + getsizeof(size)
        if isinstance(key, tuple):
            size += sum(map(getsizeof, key))
        if size > self._capacity:
            if self._metrics.enabled:
                self._metrics.read_cache.rejections += 1
            return
        if key in self._entries:
            self._drop(key)
        self._entries[key] = _Entry(value, size)
        self._charged += size
        while self._retained_bytes() > self._capacity:
            oldest = next(iter(self._entries))
            self._drop(oldest)
            if self._metrics.enabled:
                self._metrics.read_cache.evictions += 1
        if self._metrics.enabled:
            if key in self._entries:
                self._metrics.read_cache.admissions += 1
            else:
                self._metrics.read_cache.rejections += 1

    def _drop(self, key: _Key) -> None:
        self._charged -= self._entries.pop(key).charge_bytes
        if not self._entries:
            self._entries.clear()  # Release the empty OrderedDict's retained capacity.

    def _retained_bytes(self) -> int:
        return self._charged + getsizeof(self._entries) - self._empty_bytes
