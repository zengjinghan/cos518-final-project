"""Check storage effects and malformed durable bytes independently of the engine."""

from __future__ import annotations

import errno
import json
import os
import struct
from pathlib import Path
from zlib import crc32

import pytest

from lsm_store.config import StoreConfig
from lsm_store.errors import (
    ArgumentError,
    CorruptionError,
    DatabaseLockedError,
    LifecycleError,
    StorageError,
    UnsupportedFormatError,
)
from lsm_store.metrics import IoPurpose, Metrics
from lsm_store.record import Delete, Put, encode_frame
from lsm_store.storage.filesystem import FileStore, parse_table_id, table_filename
from lsm_store.storage.journal import WAL_MAGIC, Journal
from lsm_store.storage.manifest import MANIFEST_MAGIC, Manifest, load_manifest, save_manifest
from lsm_store.table.metadata import TableInfo


def _wal(store: FileStore, data: bytes = WAL_MAGIC) -> Journal:
    store.atomic_write("WAL", data, IoPurpose.JOURNAL)
    return Journal(store, max_record_bytes=1024)


def _protected(payload: bytes) -> bytes:
    length = struct.pack("!I", len(payload))
    return struct.pack("!III", len(payload), crc32(length), crc32(payload)) + payload


def test_foreign_directory_rejection_preserves_contents(tmp_path: Path) -> None:
    foreign = tmp_path / "notes.txt"
    foreign.write_text("unrelated", encoding="utf-8")
    with pytest.raises(StorageError, match="unexpected"):
        FileStore.open(tmp_path, Metrics())
    assert tuple(tmp_path.iterdir()) == (foreign,)
    assert foreign.read_text(encoding="utf-8") == "unrelated"


def test_store_lock_has_one_owner_and_releases_after_close(tmp_path: Path) -> None:
    first = FileStore.open(tmp_path / "db", Metrics())
    with pytest.raises(DatabaseLockedError, match="already locked"):
        FileStore.open(tmp_path / "db", Metrics())
    first.close()
    first.close()
    with pytest.raises(LifecycleError, match="closed"):
        first.names()
    with FileStore.open(tmp_path / "db", Metrics()) as reopened:
        assert reopened.names() == ("LOCK",)


def test_open_existing_does_not_create_or_initialize(tmp_path: Path) -> None:
    missing = tmp_path / "missing"
    with pytest.raises(StorageError, match="does not exist"):
        FileStore.open(missing, Metrics(), create=False)
    assert not missing.exists()
    with pytest.raises(StorageError, match="not initialized"):
        FileStore.open(tmp_path, Metrics(), create=False)
    assert not tuple(tmp_path.iterdir())


def test_nonempty_store_cannot_recreate_a_missing_lock(tmp_path: Path) -> None:
    (tmp_path / "WAL").write_bytes(WAL_MAGIC)
    with pytest.raises(StorageError, match="original LOCK"):
        FileStore.open(tmp_path, Metrics())
    assert not (tmp_path / "LOCK").exists()


def test_owned_symlinks_and_symlink_roots_are_rejected(tmp_path: Path) -> None:
    root = tmp_path / "real"
    root.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(root, target_is_directory=True)
    with pytest.raises(StorageError, match="symlink"):
        FileStore.open(alias, Metrics())
    (root / "WAL").symlink_to(tmp_path / "outside")
    with pytest.raises(StorageError, match="symlink"):
        FileStore.open(root, Metrics())


def test_table_collision_does_not_overwrite_and_io_is_purpose_scoped(tmp_path: Path) -> None:
    metrics = Metrics()
    with FileStore.open(tmp_path / "db", metrics) as store:
        before = metrics.snapshot()
        with store.create_table(1, IoPurpose.FLUSH) as writer:
            writer.write(b"abcdef")
            writer.sync()
        store.sync_directory(IoPurpose.FLUSH)
        with pytest.raises(StorageError, match="create table"):
            store.create_table(1, IoPurpose.FLUSH)
        with store.open_reader(table_filename(1)) as reader:
            assert reader.read_at(2, 3, IoPurpose.POINT_READ) == b"cde"
            assert reader.read_at(5, 4, IoPurpose.INSPECTION) == b"f"
        delta = metrics.snapshot().since(before)
        assert delta.io[IoPurpose.FLUSH].write_bytes == 6
        assert delta.io[IoPurpose.FLUSH].fsync_calls == 2
        assert delta.io[IoPurpose.POINT_READ].read_bytes == 3
        assert delta.io[IoPurpose.INSPECTION].read_bytes == 1


@pytest.mark.parametrize("table_id", [0, -1, 1 << 64, True])
def test_invalid_table_identifiers_are_not_paths(table_id: int) -> None:
    with pytest.raises(ArgumentError, match="table ID"):
        table_filename(table_id)
    assert parse_table_id("../WAL") is None
    assert parse_table_id("table-0000000000000000.sst") is None
    assert parse_table_id("table-000000000000000a.sst") == 10


def test_atomic_replacement_failure_preserves_previous_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with FileStore.open(tmp_path / "db", Metrics()) as store:
        store.atomic_write("WAL", b"old", IoPurpose.JOURNAL)

        def fail_replace(_self: Path, _target: Path) -> Path:
            raise OSError(errno.EIO, "injected replacement failure")

        monkeypatch.setattr(Path, "replace", fail_replace)
        with pytest.raises(StorageError, match="replace") as error:
            store.atomic_write("WAL", b"new", IoPurpose.JOURNAL)
        assert isinstance(error.value.__cause__, OSError)
        assert (store.root / "WAL").read_bytes() == b"old"
        assert any(name.startswith(".pending-") for name in store.names())


def test_short_writes_complete_and_zero_writes_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    native_write = os.write
    with FileStore.open(tmp_path / "db", Metrics()) as store:
        with store.create_table(1, IoPurpose.FLUSH) as writer:

            def short_write(descriptor: int, data: memoryview[bytes]) -> int:
                return native_write(descriptor, data[:2])

            monkeypatch.setattr(os, "write", short_write)
            writer.write(b"abcdefg")
            assert writer.size_bytes == 7

            def zero_write(_descriptor: int, _data: memoryview[bytes]) -> int:
                return 0

            monkeypatch.setattr(os, "write", zero_write)
            with pytest.raises(StorageError, match="zero bytes"):
                writer.write(b"h")
        assert (store.root / table_filename(1)).read_bytes() == b"abcdefg"


def test_journal_acknowledges_fsynced_frames_and_replays_after_checkpoint(tmp_path: Path) -> None:
    with FileStore.open(tmp_path / "db", Metrics()) as store:
        journal = _wal(store)
        first = Put(b"key", 1, b"old")
        second = Delete(b"key", 2)
        journal.append(first)
        journal.append(second)
        assert journal.recover(1) == (second,)
        assert journal.bytes_since_checkpoint == len(encode_frame(second))
        assert (store.root / "WAL").read_bytes() == WAL_MAGIC + encode_frame(first) + encode_frame(
            second
        )
        journal.reset()
        assert journal.size_bytes == len(WAL_MAGIC)
        assert journal.bytes_since_checkpoint == 0
        assert journal.recover(2) == ()
        journal.close()


def test_short_read_syscalls_cannot_truncate_acknowledged_frames(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    native_pread = os.pread
    record = Put(b"key", 1, b"acknowledged")
    original = WAL_MAGIC + encode_frame(record)
    with FileStore.open(tmp_path / "db", Metrics()) as store:
        journal = _wal(store, original)

        def short_pread(descriptor: int, length: int, offset: int) -> bytes:
            return native_pread(descriptor, min(length, 2), offset)

        monkeypatch.setattr(os, "pread", short_pread)
        assert journal.recover(0) == (record,)
        assert (store.root / "WAL").read_bytes() == original
        journal.close()


@pytest.mark.parametrize("tail_bytes", [1, 7, 11, 12, 20, 30])
def test_interrupted_terminal_append_is_removed_before_new_append(
    tmp_path: Path, tail_bytes: int
) -> None:
    first = Put(b"a", 1, b"acknowledged")
    interrupted = encode_frame(Put(b"b", 2, b"unacknowledged"))
    prefix = WAL_MAGIC + encode_frame(first)
    with FileStore.open(tmp_path / "db", Metrics()) as store:
        journal = _wal(store, prefix + interrupted[:tail_bytes])
        assert journal.recover(0) == (first,)
        assert (store.root / "WAL").read_bytes() == prefix
        replacement = Put(b"c", 2, b"replacement")
        journal.append(replacement)
        assert journal.recover(0) == (first, replacement)
        journal.close()


@pytest.mark.parametrize("damaged_offset", [0, 4, 8, 12, 30])
def test_complete_damaged_journal_frames_are_not_silently_truncated(
    tmp_path: Path, damaged_offset: int
) -> None:
    frame = bytearray(encode_frame(Put(b"key", 1, b"value")))
    frame[damaged_offset] ^= 1
    original = WAL_MAGIC + frame
    with FileStore.open(tmp_path / "db", Metrics()) as store:
        journal = _wal(store, original)
        with pytest.raises(CorruptionError, match="checksum"):
            journal.recover(0)
        assert (store.root / "WAL").read_bytes() == original
        journal.close()


@pytest.mark.parametrize("length", [0, 16, 1042, 0xFFFFFFFF])
def test_validly_checksummed_invalid_journal_length_is_corruption(
    tmp_path: Path, length: int
) -> None:
    header = struct.pack("!III", length, crc32(struct.pack("!I", length)), 0)
    with FileStore.open(tmp_path / "db", Metrics()) as store:
        journal = _wal(store, WAL_MAGIC + header)
        with pytest.raises(CorruptionError, match="length"):
            journal.recover(0)
        journal.close()


def test_duplicate_journal_sequences_are_rejected_even_below_checkpoint(tmp_path: Path) -> None:
    data = WAL_MAGIC + encode_frame(Put(b"a", 1, b"v")) + encode_frame(Delete(b"b", 1))
    with FileStore.open(tmp_path / "db", Metrics()) as store:
        journal = _wal(store, data)
        with pytest.raises(CorruptionError, match="does not increase"):
            journal.recover(2)
        journal.close()


@pytest.mark.parametrize("data", [b"", WAL_MAGIC[:4], b"LSMWAL02"])
def test_journal_magic_cannot_be_repaired_as_an_interrupted_frame(
    tmp_path: Path, data: bytes
) -> None:
    with FileStore.open(tmp_path / "db", Metrics()) as store:
        journal = _wal(store, data)
        with pytest.raises(CorruptionError, match="magic"):
            journal.recover(0)
        journal.close()


def test_manifest_round_trip_uses_canonical_protected_bytes(tmp_path: Path) -> None:
    config = StoreConfig(instrumentation="off")
    manifest = Manifest(config, (), 0, 1)
    payload = json.dumps(
        {
            "config": config.engine_dict(),
            "tables": [],
            "checkpoint_sequence": 0,
            "next_file_id": 1,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    with FileStore.open(tmp_path / "db", Metrics()) as store:
        save_manifest(store, manifest)
        assert (store.root / "MANIFEST").read_bytes() == MANIFEST_MAGIC + _protected(payload)
        restored = load_manifest(store)
        assert restored.config.engine_dict() == config.engine_dict()
        assert restored.config.instrumentation == "counters"
        assert restored.tables == ()
        assert restored.checkpoint_sequence == 0
        assert restored.next_file_id == 1


@pytest.mark.parametrize("damage", ["header", "payload", "trailing", "magic", "truncated"])
def test_manifest_corruption_never_becomes_empty_state(tmp_path: Path, damage: str) -> None:
    with FileStore.open(tmp_path / "db", Metrics()) as store:
        save_manifest(store, Manifest(StoreConfig(), (), 0, 1))
        data = bytearray((store.root / "MANIFEST").read_bytes())
        if damage == "header":
            data[8] ^= 1
        elif damage == "payload":
            data[-1] ^= 1
        elif damage == "trailing":
            data.extend(b"x")
        elif damage == "magic":
            data[7] = ord("2")
        else:
            data = data[:15]
        store.atomic_write("MANIFEST", bytes(data), IoPurpose.METADATA)
        with pytest.raises(CorruptionError):
            load_manifest(store)


@pytest.mark.parametrize(
    "table",
    [
        TableInfo(2, 0, b"a", b"b", 1, 1, 100),
        TableInfo(1, 0, b"z", b"b", 1, 1, 100),
        TableInfo(1, 65, b"a", b"b", 1, 1, 100),
        TableInfo(1, 0, b"a", b"b", 2, 1, 100),
    ],
)
def test_invalid_manifest_descriptors_fail_before_publication(
    tmp_path: Path, table: TableInfo
) -> None:
    with FileStore.open(tmp_path / "db", Metrics()) as store:
        with pytest.raises(CorruptionError):
            save_manifest(store, Manifest(StoreConfig(), (table,), 1, 2))
        assert "MANIFEST" not in store.names()


def test_manifest_rejects_duplicate_ids_and_noncanonical_json(tmp_path: Path) -> None:
    table = TableInfo(1, 0, b"a", b"b", 1, 1, 100)
    with FileStore.open(tmp_path / "db", Metrics()) as store:
        with pytest.raises(CorruptionError, match="inconsistent"):
            save_manifest(store, Manifest(StoreConfig(), (table, table), 1, 2))
        payload = json.dumps(
            {
                "config": StoreConfig().engine_dict(),
                "tables": [],
                "checkpoint_sequence": 0,
                "next_file_id": 1,
            },
            indent=2,
        ).encode()
        store.atomic_write("MANIFEST", MANIFEST_MAGIC + _protected(payload), IoPurpose.METADATA)
        with pytest.raises(CorruptionError, match="canonical"):
            load_manifest(store)


def test_unsupported_manifest_identifier_has_distinct_error(tmp_path: Path) -> None:
    with FileStore.open(tmp_path / "db", Metrics()) as store:
        save_manifest(store, Manifest(StoreConfig(), (), 0, 1))
        data = (store.root / "MANIFEST").read_bytes()
        store.atomic_write("MANIFEST", b"LSMMAN02" + data[8:], IoPurpose.METADATA)
        with pytest.raises(UnsupportedFormatError):
            load_manifest(store)
