"""Crash-recovery helpers for the LSM engine.

This module focuses on replaying the write-ahead log (WAL) to rebuild
memtable state after a crash. It does not attempt to reconstruct the full
LSM metadata (levels and SSTables) from disk; in a more complete design,
that would require a manifest similar to RocksDB's.

We assume:
- SSTables and level metadata are managed by the current process.
- WAL recovery is mainly used to restore recent, not-yet-flushed writes.
"""

from __future__ import annotations

from .compaction import CompactionStrategy
from .disk import Disk
from .lsm import LSMConfig, LSMDatabase
from .write_ahead_log import WriteAheadLogReader

__all__ = ("recover_from_write_ahead_log",)


def recover_from_write_ahead_log(
    config: LSMConfig,
    disk: Disk,
    compactor: CompactionStrategy,
) -> LSMDatabase:
    """Construct an LSMDatabase and replay the write-ahead log (WAL) into its
    memtable.

    If `config.write_ahead_log_path` is None, write-ahead logging is
    considered disabled and this function returns a fresh, empty database.

    Args:
        config: Engine configuration.
        disk: Disk instance pointing at the same underlying storage that the
            original engine used.

    Returns:
        LSMDatabase instance whose memtable reflects all updates present in
        the WAL file. The sequence counter is advanced so that future writes
        receive sequence numbers larger than any recovered entry.

    Limitations:
        This helper does not reconstruct the LSM levels or SSTable metadata
        from disk. It is intended for scenarios where:
        - SSTables and level state are already known to the process (e.g.
            in-memory experiments), or
        - you combine this function with a separate mechanism that restores
            level metadata from a manifest.

        In a full RocksDB-style system, recovery would:
        - read a MANIFEST file describing existing SSTables and levels,
        - rebuild the version metadata, and
        - replay any remaining WAL segments on top of that.
    """
    db = LSMDatabase(config=config, disk=disk, compactor=compactor)

    log_path = config.write_ahead_log_path
    if log_path is None:
        return db

    # If there is no WAL file, there is nothing to replay.
    if not disk.exists(log_path):
        return db

    reader = WriteAheadLogReader(disk, log_path)

    for entry in reader:
        if entry.tombstone:
            db._memtable.delete(key=entry.key, seq=entry.seq)
            db._update_live_bytes(entry.key, None, tombstone=True)
        else:
            assert entry.value is not None
            db._memtable.put(key=entry.key, value=entry.value, seq=entry.seq)
            db._update_live_bytes(entry.key, entry.value, tombstone=False)

        if entry.seq > db._next_seq:
            db._next_seq = entry.seq

    return db
