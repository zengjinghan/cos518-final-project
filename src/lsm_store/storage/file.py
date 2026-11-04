"""Own regular-file descriptors, positioned reads and synchronized writes.

Directory locking and durable namespace publication belong to ``filesystem``.
Each handle accounts for completed OS primitives, and close is not a sync boundary.
"""

from __future__ import annotations

import errno
import os
import stat
from typing import TYPE_CHECKING, Self

from lsm_store.errors import ArgumentError, LifecycleError, StorageError

if TYPE_CHECKING:
    from pathlib import Path
    from types import TracebackType

    from lsm_store.metrics import IoPurpose, Metrics


def operation_error(operation: str, path: Path, cause: OSError) -> StorageError:
    """Build a contextual error; callers preserve its cause with raise-from."""
    return StorageError(f"{operation} failed for {path}: {cause.strerror or cause}")


def open_regular(path: Path, flags: int, *, mode: int = 0o600) -> int:
    """Open without following a final symlink and reject special files."""
    descriptor = os.open(path, flags | os.O_NOFOLLOW | os.O_CLOEXEC, mode)
    try:
        _require_regular(descriptor, path)
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _require_regular(descriptor: int, path: Path) -> None:
    if not stat.S_ISREG(os.fstat(descriptor).st_mode):
        msg = f"owned path must be a regular file: {path}"
        raise StorageError(msg)


class _File:
    """Common lifetime for a file descriptor; handles cannot cross processes."""

    __slots__ = ("_closed", "_descriptor", "_metrics", "_path", "_pid")

    def __init__(self, path: Path, descriptor: int, metrics: Metrics) -> None:
        self._path = path
        self._descriptor = descriptor
        self._metrics = metrics
        self._pid = os.getpid()
        self._closed = False

    def _check(self) -> None:
        if self._closed:
            msg = f"file handle is closed: {self._path}"
            raise LifecycleError(msg)
        if os.getpid() != self._pid:
            msg = f"file handle belongs to another process: {self._path}"
            raise LifecycleError(msg)

    @property
    def size_bytes(self) -> int:
        """Return current file length without counting it as a byte read."""
        self._check()
        try:
            return os.fstat(self._descriptor).st_size
        except OSError as exc:
            raise operation_error("stat", self._path, exc) from exc

    def close(self) -> None:
        """Close once, including a forked process's reference, without syncing."""
        if self._closed:
            return
        self._closed = True
        try:
            os.close(self._descriptor)
        except OSError as exc:
            raise operation_error("close", self._path, exc) from exc

    def __enter__(self) -> Self:
        """Enter the file's explicit resource lifetime."""
        self._check()
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        """Release the descriptor on normal and exceptional exits."""
        self.close()


class ReadFile(_File):
    """A positioned reader whose descriptor offset is not shared by reads."""

    __slots__ = ()

    def read_at(self, offset: int, length: int, purpose: IoPurpose) -> bytes:
        """Complete short syscalls; return fewer bytes only at the file's EOF."""
        self._check()
        if type(offset) is not int or type(length) is not int or offset < 0 or length < 0:
            msg = "read offset and length must be nonnegative integers"
            raise ArgumentError(msg)
        first = self._read_chunk(offset, length, purpose)
        if not first or len(first) == length:
            return first
        chunks = [first]
        received = len(first)
        while received < length:
            chunk = self._read_chunk(offset + received, length - received, purpose)
            if not chunk:
                break
            chunks.append(chunk)
            received += len(chunk)
        return b"".join(chunks)

    def _read_chunk(self, offset: int, length: int, purpose: IoPurpose) -> bytes:
        try:
            data = os.pread(self._descriptor, length, offset)
        except OSError as exc:
            raise operation_error("read", self._path, exc) from exc
        if self._metrics.enabled:
            counters = self._metrics.io[purpose]
            counters.read_calls += 1
            counters.read_bytes += len(data)
        return data


class WriteFile(_File):
    """A sequential writer with an immutable instrumentation purpose."""

    __slots__ = ("_purpose",)

    def __init__(self, path: Path, descriptor: int, metrics: Metrics, purpose: IoPurpose) -> None:
        """Bind an already-owned descriptor to one physical I/O purpose."""
        super().__init__(path, descriptor, metrics)
        self._purpose = purpose

    def write(self, data: bytes) -> None:
        """Write all bytes, accounting for each successful short-write syscall."""
        self._check()
        remaining = memoryview(data)
        while remaining:
            try:
                written = os.write(self._descriptor, remaining)
            except OSError as exc:
                raise operation_error("write", self._path, exc) from exc
            if written == 0:
                cause = OSError(errno.EIO, "write returned zero bytes")
                raise operation_error("write", self._path, cause) from cause
            if self._metrics.enabled:
                counters = self._metrics.io[self._purpose]
                counters.write_calls += 1
                counters.write_bytes += written
            remaining = remaining[written:]

    def sync(self) -> None:
        """Synchronize this file; its directory is a separate publication step."""
        self._check()
        try:
            os.fsync(self._descriptor)
        except OSError as exc:
            raise operation_error("fsync", self._path, exc) from exc
        if self._metrics.enabled:
            self._metrics.io[self._purpose].fsync_calls += 1
