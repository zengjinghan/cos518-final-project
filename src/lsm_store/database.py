"""Sequential public API: validation, durable mutation order and resource ownership."""

import os
from contextlib import ExitStack
from enum import Enum, auto
from pathlib import Path
from threading import current_thread
from types import TracebackType
from typing import Self

from .config import CaptureMode, StoreConfig
from .errors import ArgumentError, ConfigurationError, CorruptionError, LifecycleError, StoreError
from .inspection import StoreInspection, inspect_store
from .maintenance import flush_memtable
from .memtable import MemTable
from .metrics import IoPurpose, Metrics, MetricsSnapshot
from .record import MAX_KEY_BYTES, MAX_SEQUENCE, Delete, Put, Record, logical_bytes
from .storage.filesystem import FileStore
from .storage.journal import WAL_MAGIC, Journal
from .storage.manifest import Manifest, load_manifest, save_manifest
from .table.cache import ReadCacheInfo
from .version import VersionSet


class _State(Enum):
    OPEN = auto()
    FAULTED = auto()
    CLOSED = auto()


class Database:
    """An exclusively owned persistent map; keys and values have no engine encoding."""

    def __init__(
        self,
        version: VersionSet,
        journal: Journal,
        memtable: MemTable,
        metrics: Metrics,
        resources: ExitStack,
    ) -> None:
        """Use open() to establish validated storage and ownership."""
        self._version = version
        self._journal = journal
        self._memtable = memtable
        self._metrics = metrics
        # Configuration is immutable; resolve these admission bounds once.
        self._max_record_bytes = version.config.max_record_bytes
        self._max_key_bytes = min(MAX_KEY_BYTES, self._max_record_bytes)
        self._sequence = max(
            (record.sequence for record in memtable.records()),
            default=version.manifest.checkpoint_sequence,
        )
        self._resources = resources
        self._state = _State.OPEN
        self._pid = os.getpid()
        self._thread = current_thread()

    @classmethod
    def open(
        cls,
        path: Path,
        config: StoreConfig | None = None,
        *,
        capture: CaptureMode | None = None,
        create: bool = True,
    ) -> "Database":
        """Create or recover a store; omitted config adopts persisted engine settings."""
        if capture is not None and capture not in ("off", "counters"):
            raise ConfigurationError(f"unsupported instrumentation mode: {capture!r}")
        mode = (
            capture
            if capture is not None
            else (config.instrumentation if config is not None else "counters")
        )
        metrics = Metrics(mode)
        with ExitStack() as resources:
            store = FileStore.open(path, metrics, create=create)
            resources.callback(store.close)
            names = store.names()
            if "MANIFEST" in names:
                manifest = load_manifest(store)
                if config is not None and config.engine_dict() != manifest.config.engine_dict():
                    raise ConfigurationError(
                        "explicit engine configuration differs from persisted configuration"
                    )
                effective = manifest.config if config is None else config
            else:
                if not create or set(names) - {"LOCK"}:
                    raise CorruptionError(
                        "missing MANIFEST; directory is uninitialized or creation was interrupted"
                    )
                effective = StoreConfig() if config is None else config
                store.atomic_write("WAL", WAL_MAGIC, IoPurpose.JOURNAL)
                manifest = Manifest(effective, (), 0, 1)
                save_manifest(store, manifest)
            journal = Journal(store, effective.max_record_bytes)
            resources.callback(journal.close)
            records = journal.recover(manifest.checkpoint_sequence)
            memtable = MemTable()
            for record in records:
                memtable.apply(record)
            version = VersionSet(store, manifest, effective, metrics)
            resources.callback(version.close)
            version.validate_and_clean()
            return cls(version, journal, memtable, metrics, resources.pop_all())

    @property
    def config(self) -> StoreConfig:
        """Return immutable engine settings; capture is independently handle-scoped."""
        self._check_open()
        return self._version.config

    def put(self, key: bytes, value: bytes) -> None:
        """Acknowledge only after journal sync and any triggered maintenance succeed."""
        self._check_open()
        self._validate_bytes(key, "key")
        self._validate_bytes(value, "value")
        if len(key) + len(value) > self._max_record_bytes:
            raise ArgumentError("combined key/value length exceeds max_record_bytes")
        record = Put(key, self._next_sequence(), value)
        self._mutate(record)

    def delete(self, key: bytes) -> None:
        """Persist a tombstone, even when the key currently does not exist."""
        self._check_open()
        self._validate_bytes(key, "key")
        record = Delete(key, self._next_sequence())
        self._mutate(record)

    def get(self, key: bytes) -> bytes | None:
        """Read the newest record; a tombstone and absence both produce None."""
        self._check_open()
        self._validate_bytes(key, "key")
        try:
            record = self._memtable.get(key)
            if record is None:
                record = self._version.get(key)
        except StoreError:
            self._state = _State.FAULTED
            raise
        value = record.value if isinstance(record, Put) else None
        if self._metrics.enabled:
            logical = self._metrics.logical
            if value is None:
                logical.misses += 1
            else:
                logical.hits += 1
                logical.returned_bytes += len(key) + len(value)
        return value

    def scan(self, start: bytes, end: bytes) -> list[tuple[bytes, bytes]]:
        """Return sorted unique live keys in [start,end], merging only overlapping tables."""
        self._check_open()
        self._validate_bytes(start, "start")
        self._validate_bytes(end, "end")
        result: list[tuple[bytes, bytes]] = []
        try:
            if start <= end:
                with self._version.merged(
                    self._version.tables,
                    IoPurpose.SCAN,
                    start,
                    end,
                    additional=(self._memtable.records(start, end),),
                ) as records:
                    result = [
                        (record.key, record.value) for record in records if isinstance(record, Put)
                    ]
        except StoreError:
            self._state = _State.FAULTED
            raise
        if self._metrics.enabled:
            self._metrics.logical.scans += 1
            self._metrics.logical.scan_rows += len(result)
            self._metrics.logical.returned_bytes += sum(
                len(key) + len(value) for key, value in result
            )
        return result

    def flush(self) -> None:
        """Checkpoint and maintain the store without changing its visible contents."""
        self._check_open()
        try:
            flush_memtable(
                self._version,
                self._memtable,
                self._journal,
                self._sequence,
                self._metrics,
            )
        except BaseException:
            # Interruption after durable effects is just as uncertain as an I/O error.
            self._state = _State.FAULTED
            raise

    def read_cache_info(self) -> ReadCacheInfo:
        """Observe retained validation memory without I/O or changing cache order."""
        self._check_open()
        return self._version.read_cache.info()

    def snapshot(self) -> MetricsSnapshot:
        """Capture fixed counters only, without any table traversal or I/O."""
        self._check_open()
        return self._metrics.snapshot()

    def inspect(self) -> StoreInspection:
        """Explicitly scan logical contents and report literal physical occupancy."""
        self._check_open()
        try:
            return inspect_store(self._version, self._memtable)
        except StoreError:
            self._state = _State.FAULTED
            raise

    def close(self) -> None:
        """Release every resource without requiring a flush; closing is idempotent."""
        if self._state is _State.CLOSED:
            return
        if os.getpid() == self._pid and current_thread() is not self._thread:
            raise LifecycleError("database belongs to another thread")
        self._state = _State.CLOSED
        self._resources.close()

    def __enter__(self) -> Self:
        """Return this exclusively owned open handle."""
        self._check_open()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Release resources even when the context body failed."""
        self.close()

    def _check_open(self) -> None:
        if os.getpid() != self._pid or current_thread() is not self._thread:
            raise LifecycleError("database handle belongs to another process or thread")
        if self._state is not _State.OPEN:
            raise LifecycleError(f"database is {self._state.name.lower()}; close and reopen it")

    def _validate_bytes(self, value: bytes, name: str) -> None:
        if type(value) is not bytes:
            raise ArgumentError(f"{name} must be bytes")
        maximum = self._max_record_bytes if name == "value" else self._max_key_bytes
        if len(value) > maximum:
            raise ArgumentError(f"{name} exceeds max_record_bytes")

    def _next_sequence(self) -> int:
        if self._sequence == MAX_SEQUENCE:
            raise ArgumentError("mutation sequence space exhausted")
        return self._sequence + 1

    def _mutate(self, record: Record) -> None:
        try:
            self._journal.append(record)
            self._memtable.apply(record)
            self._sequence = record.sequence
            budget = self._version.config.memtable_bytes
            if (
                self._memtable.size_bytes >= budget
                or self._journal.bytes_since_checkpoint >= budget
            ):
                self.flush()
            if self._metrics.enabled:
                if isinstance(record, Put):
                    self._metrics.logical.puts += 1
                else:
                    self._metrics.logical.deletes += 1
                self._metrics.logical.mutation_bytes += logical_bytes(record)
        except BaseException:
            # Do not permit a caught interrupt to retry with stale sequence state.
            self._state = _State.FAULTED
            raise
