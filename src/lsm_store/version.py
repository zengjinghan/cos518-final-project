"""The sole owner of live-table versions and durable manifest publication."""

from bisect import bisect_right
from collections import OrderedDict
from collections.abc import Generator, Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import replace
from itertools import pairwise
from typing import Final

from .config import StoreConfig
from .errors import CorruptionError, StorageError
from .merge import merge_latest, newest_record
from .metrics import IoPurpose, Metrics
from .record import MAX_SEQUENCE, Record
from .storage.filesystem import FileStore, parse_table_id, table_filename
from .storage.manifest import Manifest, save_manifest
from .table.cache import ReadCache
from .table.metadata import TableInfo
from .table.reader import TableReader

_POINT_CACHE_TABLES: Final = 32
_MAX_LEVEL: Final = 64


class VersionSet:
    """Publish versions and scope separate reader-handle and validation caches."""

    def __init__(
        self, store: FileStore, manifest: Manifest, config: StoreConfig, metrics: Metrics
    ) -> None:
        """Validate an opened version before it becomes available to callers."""
        self.store = store
        self.manifest = manifest
        self.config = config
        self.read_cache = ReadCache(config, metrics)
        self._next_file_id = manifest.next_file_id
        self._readers: OrderedDict[int, TableReader] = OrderedDict()
        self._levels: tuple[tuple[TableInfo, ...], ...] = ()
        self._minimums: tuple[tuple[bytes, ...], ...] = ()
        self._index_layout()

    @property
    def tables(self) -> tuple[TableInfo, ...]:
        """Return an immutable live-set description."""
        return self.manifest.tables

    def validate_and_clean(self) -> None:
        """Check committed table envelopes before deleting any recognized orphans."""
        for info in self.tables:
            with TableReader(
                self.store, info, self.config, purpose=IoPurpose.RECOVERY, cache=self.read_cache
            ):
                pass
        names = self.store.names()
        known_ids = [identifier for name in names if (identifier := parse_table_id(name))]
        next_id = max(self._next_file_id, max(known_ids, default=0) + 1)
        if next_id != self._next_file_id:
            self._next_file_id = next_id
            updated = replace(self.manifest, next_file_id=next_id)
            save_manifest(self.store, updated)
            self.manifest = updated
        live_names = {table_filename(info.table_id) for info in self.tables}
        obsolete = [
            name
            for name in names
            if name.startswith(".pending-")
            or (parse_table_id(name) is not None and name not in live_names)
        ]
        for name in obsolete:
            self.store.remove(name, IoPurpose.RECOVERY)
        if obsolete:
            self.store.sync_directory(IoPurpose.RECOVERY)

    def allocate_id(self) -> int:
        """Reserve a fresh ID; an uncommitted output remains a discoverable orphan."""
        if self._next_file_id > MAX_SEQUENCE:
            raise StorageError("table identifier space exhausted")
        identifier = self._next_file_id
        self._next_file_id += 1
        return identifier

    def install(
        self,
        tables: tuple[TableInfo, ...],
        checkpoint: int,
        purpose: IoPurpose,
    ) -> None:
        """Durably publish one complete live set, then reclaim unreferenced files."""
        updated = Manifest(self.manifest.config, tables, checkpoint, self._next_file_id)
        self._validate_layout(tables, checkpoint)
        self.store.sync_directory(purpose)
        save_manifest(self.store, updated)
        old_tables = self.tables
        self.manifest = updated
        self._index_layout()
        live_ids = {table.table_id for table in tables}
        obsolete = [table for table in old_tables if table.table_id not in live_ids]
        for info in obsolete:
            self.read_cache.invalidate(info.table_id)
            cached = self._readers.pop(info.table_id, None)
            if cached is not None:
                cached.close()
            self.store.remove(table_filename(info.table_id), purpose)
        if obsolete:
            self.store.sync_directory(purpose)

    def get(self, key: bytes) -> Record | None:
        """Resolve candidates by sequence; physical level order is not precedence."""
        latest: Record | None = None
        for level, tables in enumerate(self._levels):
            candidates = tables
            if level > 0 and self.config.compaction.kind == "leveled":
                index = bisect_right(self._minimums[level], key) - 1
                candidates = tables[index : index + 1] if index >= 0 else ()
            for info in candidates:
                if not info.min_key <= key <= info.max_key:
                    continue
                if latest is not None and info.max_sequence < latest.sequence:
                    continue
                found = self._reader(info).get(key, IoPurpose.POINT_READ)
                if found is not None:
                    latest = found if latest is None else newest_record(latest, found)
        return latest

    @contextmanager
    def merged(
        self,
        tables: tuple[TableInfo, ...],
        purpose: IoPurpose,
        start: bytes | None = None,
        end: bytes | None = None,
        *,
        additional: tuple[Iterator[Record], ...] = (),
    ) -> Generator[Iterator[Record], None, None]:
        """Scope every merge reader, including exceptional exits and empty ranges."""
        with ExitStack() as stack:
            streams: list[Iterator[Record]] = list(additional)
            for info in tables:
                if start is not None and info.max_key < start:
                    continue
                if end is not None and info.min_key > end:
                    continue
                reader = stack.enter_context(
                    TableReader(
                        self.store, info, self.config, purpose=purpose, cache=self.read_cache
                    ),
                )
                streams.append(
                    reader.iter_all(purpose)
                    if start is None or end is None
                    else reader.iter_range(start, end, purpose)
                )
            yield merge_latest(streams)

    def close(self) -> None:
        """Release cached readers; store locking belongs to FileStore."""
        with ExitStack() as cleanup:
            cleanup.callback(self.read_cache.clear)
            for reader in self._readers.values():
                cleanup.callback(reader.close)
            self._readers.clear()

    def _reader(self, info: TableInfo) -> TableReader:
        cached = self._readers.pop(info.table_id, None)
        if cached is None:
            cached = TableReader(
                self.store, info, self.config, purpose=IoPurpose.POINT_READ, cache=self.read_cache
            )
        self._readers[info.table_id] = cached
        if len(self._readers) > _POINT_CACHE_TABLES:
            _, evicted = self._readers.popitem(last=False)
            evicted.close()
        return cached

    def _index_layout(self) -> None:
        self._validate_layout(self.tables, self.manifest.checkpoint_sequence)
        maximum = max((table.level for table in self.tables), default=0)
        self._levels = tuple(
            tuple(
                sorted(
                    (table for table in self.tables if table.level == level),
                    key=lambda table: table.min_key,
                )
            )
            for level in range(maximum + 1)
        )
        self._minimums = tuple(tuple(table.min_key for table in level) for level in self._levels)

    def _validate_layout(self, tables: tuple[TableInfo, ...], checkpoint: int) -> None:
        if any(not 0 <= table.level <= _MAX_LEVEL for table in tables):
            raise CorruptionError("table level is outside supported range 0..64")
        if any(table.max_sequence > checkpoint for table in tables):
            raise CorruptionError("table sequence exceeds committed checkpoint")
        if self.config.compaction.kind != "leveled":
            return
        for level in {table.level for table in tables if table.level > 0}:
            ordered = sorted(
                (table for table in tables if table.level == level), key=lambda t: t.min_key
            )
            if any(left.max_key >= right.min_key for left, right in pairwise(ordered)):
                raise CorruptionError(f"overlapping ranges in leveled level {level}")
