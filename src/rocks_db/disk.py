"""Disk API for the LSM storage engine.

RocksDB isolates all file IO behind an abstraction layer (Env / FileSystem).
This keeps the LSM internals independent of the underlying storage medium
and makes testing, simulation, and portability easier.

The same idea is used here: the LSM tree interacts only with the Disk
interface. Two concrete implementations are provided:

- InMemoryDisk : fully in-memory simulation
- FileSystemDisk : backed by the OS filesystem

Both backends behave identically at the API level. SSTables, WAL, and
metadata files are written and read through Disk.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

from .metrics_toggle import MetricsAware, MetricsToggle

__all__ = (
    "DiskMetrics",
    "Disk",
    "InMemoryDisk",
    "FileSystemDisk",
    "DiskType",
    "create_disk",
)


class DiskMetrics(BaseModel):
    """Tracks cumulative I/O statistics for a Disk implementation.

    These counters are used by the LSM engine to compute:
    - Read amplification
    - Write amplification
    - Total physical IO cost
    - Performance diagnostics during compaction

    All values are monotonically increasing for the lifetime of the Disk.
    Different Disk backends (InMemoryDisk, FileSystemDisk, etc.) update these
    fields consistently, which allows the upper layers of the LSM tree to
    reason about storage behavior independently of the storage backend.
    """

    model_config = ConfigDict(extra="forbid")

    read_ops: int = 0
    """Number of file read operations performed."""

    write_ops: int = 0
    """Number of file write operations performed."""

    bytes_read: int = 0
    """Total bytes returned by `read_file()` calls."""

    bytes_written: int = 0
    """Total bytes passed to `write_file()` calls."""

    def reset(self) -> None:
        """Reset all disk IO counters to zero."""
        self.read_ops = 0
        self.write_ops = 0
        self.bytes_read = 0
        self.bytes_written = 0

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dict view (for snapshots / serialization)."""
        return self.model_dump()


@dataclass(slots=True, kw_only=True)
class Disk(MetricsAware[DiskMetrics], ABC):
    """Abstract byte-addressable storage interface.

    The LSM tree interacts with storage only through this interface.
    Implementations differ in where data is stored (memory or filesystem)
    but present an identical API.
    """

    _metrics: MetricsToggle[DiskMetrics] = field(init=False, default_factory=lambda: MetricsToggle(DiskMetrics))
    """Accumulated IO statistics with on/off control."""

    @abstractmethod
    def allocate_path(self, prefix: str) -> str:
        """Generate a unique new file path starting with the given prefix.

        Args:
            prefix: A prefix describing the type of file (e.g., "sst_l0_", "wal_").

        Returns:
            A unique path string suitable for creating a new file.
        """
        ...

    @abstractmethod
    def read_at(self, path: str, offset: int, length: int) -> bytes:
        """Read a specific chunk of a file.

        Args:
            path: File to read.
            offset: Byte offset to start reading.
            length: Number of bytes to read.
        """
        ...

    @abstractmethod
    def write_file(self, path: str, data: bytes) -> None:
        """Write an entire file atomically.

        Args:
            path: The location where the file should be written.
            data: Full contents of the file. Existing contents are replaced.
        """
        ...

    @abstractmethod
    def read_file(self, path: str) -> bytes:
        """Read an entire file.

        Args:
            path: File to read.

        Returns:
            bytes: The file contents, or empty bytes if not found.
        """
        ...

    @abstractmethod
    def delete_file(self, path: str) -> None:
        """Remove a file if present.

        Args:
            path: File to delete. Missing files are ignored.
        """
        ...

    @abstractmethod
    def list_files(self, prefix: str | None = None) -> list[str]:
        """List stored files.

        Args:
            prefix: Optional prefix filter.

        Returns:
            Names of stored files.
        """
        ...

    @abstractmethod
    def append_file(self, path: str, data: bytes) -> None:
        """Append raw bytes to an existing file.

        Implementations must:
        - Increase write_ops by 1.
        - Increase bytes_written by len(data).
        - Ensure that subsequent calls to read_file(path) see the appended data.

        Args:
            path: File identifier on this Disk. If the file does not exist yet,
                append_file creates it as if it were empty and then appends data.
            data: Bytes to append to the file.
        """
        ...

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Return True if a file with the given logical path exists in memory."""
        ...

    @property
    @abstractmethod
    def total_bytes(self) -> int:
        """Compute total stored bytes.

        Returns:
            Sum of all file sizes.
        """
        ...


@dataclass(slots=True, kw_only=True)
class InMemoryDisk(Disk):
    """In-memory storage backend.

    All files are stored in a dictionary mapping path strings to bytes.
    This backend is very fast and ideal for testing and simulation.
    It provides the same semantics as FileSystemDisk but without persistence.
    """

    _files: dict[str, bytes] = field(init=False, default_factory=dict)
    """Mapping of file paths to their byte contents."""

    _next_id: int = field(init=False, default=0)
    """Monotonic counter used to generate unique file names."""

    def allocate_path(self, prefix: str) -> str:
        self._next_id += 1
        return f"{prefix}{self._next_id:06d}"

    def read_at(self, path: str, offset: int, length: int) -> bytes:
        data = self._files.get(path, b"")
        # Simulate IO cost: we strictly slice the data
        chunk = data[offset : offset + length]
        self.metrics.read_ops += 1
        self.metrics.bytes_read += len(chunk)
        return chunk

    def write_file(self, path: str, data: bytes) -> None:
        self._files[path] = data
        self.metrics.write_ops += 1
        self.metrics.bytes_written += len(data)

    def read_file(self, path: str) -> bytes:
        data = self._files.get(path, b"")
        self.metrics.read_ops += 1
        self.metrics.bytes_read += len(data)
        return data

    def delete_file(self, path: str) -> None:
        self._files.pop(path, None)

    def list_files(self, prefix: str | None = None) -> list[str]:
        if prefix is None:
            return list(self._files)

        return [path for path in self._files if path.startswith(prefix)]

    def append_file(self, path: str, data: bytes) -> None:
        existing = self._files.get(path, b"")
        self._files[path] = existing + data
        self.metrics.write_ops += 1
        self.metrics.bytes_written += len(data)

    def exists(self, path: str) -> bool:
        """Return True if a file with the given logical path exists in memory."""
        return path in self._files

    @property
    def total_bytes(self) -> int:
        return sum(len(data) for data in self._files.values())


@dataclass(slots=True, kw_only=True)
class FileSystemDisk(Disk):
    """Filesystem-backed storage backend.

    Files are stored on disk inside a root directory. The backend provides
    the same semantics as InMemoryDisk but with persistence using the OS filesystem.
    """

    root: str
    """Directory that will store all files managed by this Disk."""

    _root: Path = field(init=False)
    """Resolved Path object for the root directory."""

    _next_id: int = field(init=False, default=0)
    """Monotonic ID used to generate unique file paths."""

    def __post_init__(self) -> None:
        """Initialize the filesystem environment after dataclass construction."""
        # Resolve the root path and ensure the directory exists.
        self._root = Path(self.root)
        self._root.mkdir(parents=True, exist_ok=True)

    def _resolve_path(self, path: str) -> Path:
        # Compute the absolute on-disk path given a logical file name.
        return self._root / path

    def allocate_path(self, prefix: str) -> str:
        self._next_id += 1
        return f"{prefix}{self._next_id:06d}"

    def read_at(self, path: str, offset: int, length: int) -> bytes:
        p = self._resolve_path(path)
        if not p.exists():
            return b""

        # Open, seek, read, close
        with open(p, "rb") as f:
            f.seek(offset)
            data = f.read(length)

        self.metrics.read_ops += 1
        self.metrics.bytes_read += len(data)
        return data

    def write_file(self, path: str, data: bytes) -> None:
        p = self._resolve_path(path)
        with open(p, "wb") as f:
            f.write(data)

        self.metrics.write_ops += 1
        self.metrics.bytes_written += len(data)

    def read_file(self, path: str) -> bytes:
        p = self._resolve_path(path)
        if not p.exists():
            self.metrics.read_ops += 1
            return b""

        data = p.read_bytes()
        self.metrics.read_ops += 1
        self.metrics.bytes_read += len(data)
        return data

    def delete_file(self, path: str) -> None:
        p = self._resolve_path(path)
        if p.exists():
            p.unlink()

    def list_files(self, prefix: str | None = None) -> list[str]:
        names = [f.name for f in self._root.iterdir() if f.is_file()]
        if prefix is None:
            return names

        return [name for name in names if name.startswith(prefix)]

    def append_file(self, path: str, data: bytes) -> None:
        """Append data to a file on the real filesystem.

        If the file does not exist, it is created. The implementation uses
        binary append mode so that only the new data is written.
        """
        p = self._resolve_path(path)
        with open(p, "ab") as f:
            f.write(data)

        self.metrics.write_ops += 1
        self.metrics.bytes_written += len(data)

    def exists(self, path: str) -> bool:
        """Return True if a file with the given logical path exists on disk."""
        p = self._resolve_path(path)
        return p.exists()

    @property
    def total_bytes(self) -> int:
        return sum(f.stat().st_size for f in self._root.iterdir() if f.is_file())


class DiskType(str, Enum):
    """Enumeration of available disk backends for benchmarking."""

    IN_MEMORY = "in_memory"
    FILE_SYSTEM = "file_system"


_DISK_REGISTRY: Mapping[DiskType, Callable[..., Disk]] = {
    DiskType.IN_MEMORY: InMemoryDisk,
    DiskType.FILE_SYSTEM: FileSystemDisk,
}


def create_disk(
    type: DiskType,
    args: dict[str, Any] | None = None,
) -> Disk:
    """Instantiate a Disk from a DiskSpec (type + args)."""
    if args is None:
        args = {}

    try:
        compaction_class = _DISK_REGISTRY[type]
    except KeyError as exc:
        raise ValueError(f"Unknown disk type: {type!r}") from exc

    return compaction_class(**args)
