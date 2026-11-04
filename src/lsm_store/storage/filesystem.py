"""Own directory namespace checks, locking and synchronized publication.

Each operation reports completed OS primitives to its fixed-purpose counter.
Closing is not a durability boundary: publication explicitly synchronizes files
and their containing directory in the order required by the storage protocol.
"""

from __future__ import annotations

import errno
import fcntl
import os
import re
import stat
from pathlib import Path
from typing import TYPE_CHECKING, Self
from uuid import uuid4

from lsm_store.errors import ArgumentError, DatabaseLockedError, LifecycleError, StorageError
from lsm_store.metrics import IoPurpose
from lsm_store.storage.file import ReadFile, WriteFile, open_regular, operation_error

if TYPE_CHECKING:
    from types import TracebackType

    from lsm_store.metrics import Metrics

_TABLE_NAME = re.compile(r"table-([0-9a-f]{16})\.sst\Z")
_PENDING_NAME = re.compile(r"\.pending-[0-9a-f]{32}\Z")
_FIXED_NAMES = frozenset({"LOCK", "MANIFEST", "WAL"})
_MAX_TABLE_ID = (1 << 64) - 1


def table_filename(table_id: int) -> str:
    """Return the canonical basename for an admitted positive u64 table ID."""
    if type(table_id) is not int or not 1 <= table_id <= _MAX_TABLE_ID:
        msg = "table ID must be a positive unsigned 64-bit integer"
        raise ArgumentError(msg)
    return f"table-{table_id:016x}.sst"


def parse_table_id(name: str) -> int | None:
    """Recognize a canonical table basename without admitting zero as an ID."""
    match = _TABLE_NAME.fullmatch(name)
    if match is None:
        return None
    table_id = int(match.group(1), 16)
    return table_id or None


def _is_owned_name(name: str) -> bool:
    return (
        name in _FIXED_NAMES
        or parse_table_id(name) is not None
        or _PENDING_NAME.fullmatch(name) is not None
    )


def _owned_names(root: Path) -> tuple[str, ...]:
    try:
        with os.scandir(root) as entries:
            names: list[str] = []
            for entry in entries:
                if not _is_owned_name(entry.name) or not entry.is_file(follow_symlinks=False):
                    msg = f"unexpected file, directory or symlink in store: {root / entry.name}"
                    raise StorageError(msg)
                names.append(entry.name)
    except OSError as exc:
        raise operation_error("list directory", root, exc) from exc
    return tuple(sorted(names))


def _require_initialized(root: Path, names: tuple[str, ...]) -> None:
    if not {"LOCK", "MANIFEST", "WAL"}.issubset(names):
        msg = f"store is not initialized: {root}; LOCK, MANIFEST and WAL are required"
        raise StorageError(msg)


def _require_original_lock(root: Path, names: tuple[str, ...]) -> None:
    if names and "LOCK" not in names:
        msg = f"nonempty store is missing its original LOCK file: {root}"
        raise StorageError(msg)


class FileStore:
    """Exclusive owner of the store namespace and filesystem effects."""

    __slots__ = ("_closed", "_lock_descriptor", "_metrics", "_pid", "_root")

    def __init__(self, root: Path, metrics: Metrics, lock_descriptor: int) -> None:
        """Adopt a locked descriptor; callers normally construct through open."""
        self._root = root
        self._metrics = metrics
        self._lock_descriptor = lock_descriptor
        self._pid = os.getpid()
        self._closed = False

    @property
    def root(self) -> Path:
        """Return the immutable root associated with the acquired directory lock."""
        return self._root

    @classmethod
    def open(cls, root: Path, metrics: Metrics, *, create: bool = True) -> Self:
        """Acquire an exclusive lock without adopting foreign directory contents."""
        # resolve() would erase the final symlink before the required check.
        root = Path(os.path.abspath(root))  # noqa: PTH100
        created = False
        try:
            if not root.exists() and not root.is_symlink():
                if not create:
                    msg = f"store directory does not exist: {root}"
                    raise StorageError(msg)
                root.mkdir(mode=0o700)
                created = True
            if root.is_symlink() or not stat.S_ISDIR(root.stat().st_mode):
                msg = f"store root must be a real directory, not a symlink: {root}"
                raise StorageError(msg)
            if created:
                cls._sync_path(root.parent, metrics, IoPurpose.METADATA)
            names = _owned_names(root)
            if not create:
                _require_initialized(root, names)
            _require_original_lock(root, names)
            descriptor = open_regular(root / "LOCK", os.O_CREAT | os.O_RDWR)
        except OSError as exc:
            raise operation_error("open store", root, exc) from exc
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(descriptor)
            if exc.errno in {errno.EACCES, errno.EAGAIN}:
                msg = f"database directory is already locked: {root}"
                raise DatabaseLockedError(msg) from exc
            raise operation_error("lock", root / "LOCK", exc) from exc
        store = cls(root, metrics, descriptor)
        try:
            store.names()
        except BaseException:
            store.close()
            raise
        return store

    def _check(self) -> None:
        if self._closed:
            msg = f"store is closed: {self.root}"
            raise LifecycleError(msg)
        if os.getpid() != self._pid:
            msg = f"store belongs to another process: {self.root}"
            raise LifecycleError(msg)

    def _path(self, name: str) -> Path:
        self._check()
        if not _is_owned_name(name):
            msg = f"invalid owned filename: {name!r}"
            raise ArgumentError(msg)
        return self.root / name

    def names(self) -> tuple[str, ...]:
        """List admitted regular owned files; reject unknown files and symlinks."""
        self._check()
        return _owned_names(self.root)

    def open_reader(self, name: str) -> ReadFile:
        """Open one existing owned file without following symlinks."""
        path = self._path(name)
        try:
            descriptor = open_regular(path, os.O_RDONLY)
        except OSError as exc:
            raise operation_error("open reader", path, exc) from exc
        return ReadFile(path, descriptor, self._metrics)

    def create_table(self, table_id: int, purpose: IoPurpose) -> WriteFile:
        """Exclusively create a table, preserving any previous file on collision."""
        path = self._path(table_filename(table_id))
        try:
            descriptor = open_regular(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except OSError as exc:
            raise operation_error("create table", path, exc) from exc
        return WriteFile(path, descriptor, self._metrics, purpose)

    def open_append(self, name: str, purpose: IoPurpose) -> WriteFile:
        """Open an existing journal with kernel-enforced append positioning."""
        if name != "WAL":
            msg = "only WAL can be opened for append"
            raise ArgumentError(msg)
        path = self._path(name)
        try:
            descriptor = open_regular(path, os.O_WRONLY | os.O_APPEND)
        except OSError as exc:
            raise operation_error("open append", path, exc) from exc
        return WriteFile(path, descriptor, self._metrics, purpose)

    def atomic_write(self, name: str, data: bytes, purpose: IoPurpose) -> None:
        """Synchronize a new file, replace metadata/journal and sync its directory."""
        if name not in {"MANIFEST", "WAL"}:
            msg = "only MANIFEST and WAL support atomic replacement"
            raise ArgumentError(msg)
        path = self._path(name)
        pending = self._path(f".pending-{uuid4().hex}")
        try:
            descriptor = open_regular(pending, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except OSError as exc:
            raise operation_error("create pending file", pending, exc) from exc
        with WriteFile(pending, descriptor, self._metrics, purpose) as writer:
            writer.write(data)
            writer.sync()
        try:
            if path.is_symlink():
                msg = f"refusing to replace a symlink: {path}"
                raise StorageError(msg)
            pending.replace(path)
        except OSError as exc:
            raise operation_error("replace", path, exc) from exc
        self.sync_directory(purpose)

    def truncate(self, name: str, size: int, purpose: IoPurpose) -> None:
        """Durably remove an interrupted journal tail, never extend the file."""
        if name != "WAL" or type(size) is not int or size < 0:
            msg = "only WAL supports truncation to a nonnegative integer size"
            raise ArgumentError(msg)
        path = self._path(name)
        try:
            descriptor = open_regular(path, os.O_WRONLY)
        except OSError as exc:
            raise operation_error("open truncate", path, exc) from exc
        with WriteFile(path, descriptor, self._metrics, purpose) as writer:
            if size > writer.size_bytes:
                msg = f"refusing to extend WAL during recovery: {size}"
                raise ArgumentError(msg)
            try:
                os.ftruncate(descriptor, size)
            except OSError as exc:
                raise operation_error("truncate", path, exc) from exc
            writer.sync()

    def remove(self, name: str, purpose: IoPurpose) -> None:
        """Remove one known obsolete file; callers synchronize removal groups."""
        if name in _FIXED_NAMES:
            msg = f"cannot remove live protocol file: {name}"
            raise ArgumentError(msg)
        path = self._path(name)
        try:
            if path.is_symlink():
                msg = f"refusing to remove a symlink: {path}"
                raise StorageError(msg)
            path.unlink()
        except OSError as exc:
            raise operation_error("remove", path, exc) from exc
        if self._metrics.enabled:
            self._metrics.io[purpose].removals += 1

    @staticmethod
    def _sync_path(path: Path, metrics: Metrics, purpose: IoPurpose) -> None:
        try:
            descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
            try:
                os.fsync(descriptor)
                if metrics.enabled:
                    metrics.io[purpose].fsync_calls += 1
            finally:
                os.close(descriptor)
        except OSError as exc:
            raise operation_error("directory fsync", path, exc) from exc

    def sync_directory(self, purpose: IoPurpose) -> None:
        """Synchronize namespace changes independently from file contents."""
        self._check()
        self._sync_path(self.root, self._metrics, purpose)

    def close(self) -> None:
        """Release this lock reference without explicitly unlocking across fork."""
        if self._closed:
            return
        self._closed = True
        try:
            os.close(self._lock_descriptor)
        except OSError as exc:
            raise operation_error("close lock", self.root / "LOCK", exc) from exc

    def __enter__(self) -> Self:
        """Enter the directory owner's explicit lifetime."""
        self._check()
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        """Release the directory lock reference on every exit."""
        self.close()
