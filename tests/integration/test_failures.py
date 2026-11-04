from pathlib import Path

import pytest

from lsm_store import Database, StoreConfig
from lsm_store.errors import CorruptionError, LifecycleError, StorageError
from lsm_store.metrics import IoPurpose
from lsm_store.storage.filesystem import FileStore


@pytest.mark.parametrize("published", [False, True])
def test_failed_manifest_publication_recovers_without_data_loss(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    published: bool,
) -> None:
    path = tmp_path / "store"
    database = Database.open(path, StoreConfig(memtable_bytes=1))
    original = FileStore.atomic_write

    def interrupt(store: FileStore, name: str, data: bytes, purpose: IoPurpose) -> None:
        if name == "MANIFEST":
            if published:
                original(store, name, data, purpose)
            raise StorageError("injected manifest publication failure")
        original(store, name, data, purpose)

    with monkeypatch.context() as patch:
        patch.setattr(FileStore, "atomic_write", interrupt)
        with pytest.raises(StorageError):
            database.put(b"key", b"persisted but unacknowledged")
        with pytest.raises(LifecycleError):
            database.get(b"key")
    database.close()
    with Database.open(path) as recovered:
        assert recovered.get(b"key") == b"persisted but unacknowledged"
        recovered.put(b"next", b"value")
    with Database.open(path) as recovered:
        assert recovered.get(b"next") == b"value"


def test_failed_wal_reset_after_checkpoint_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "store"
    database = Database.open(path, StoreConfig(memtable_bytes=1))
    original = FileStore.atomic_write

    def interrupt(store: FileStore, name: str, data: bytes, purpose: IoPurpose) -> None:
        if name == "WAL":
            raise StorageError("injected reset failure after manifest checkpoint")
        original(store, name, data, purpose)

    with monkeypatch.context() as patch:
        patch.setattr(FileStore, "atomic_write", interrupt)
        with pytest.raises(StorageError):
            database.put(b"key", b"value")
    database.close()
    with Database.open(path) as recovered:
        assert recovered.get(b"key") == b"value"
        recovered.put(b"key", b"new")
        assert recovered.get(b"key") == b"new"


def test_missing_committed_table_is_not_treated_as_absence(tmp_path: Path) -> None:
    path = tmp_path / "store"
    with Database.open(path) as database:
        database.put(b"key", b"value")
        database.flush()
    next(path.glob("*.sst")).unlink()
    with pytest.raises(StorageError, match="table-"):
        Database.open(path)


def test_unrelated_directory_is_preserved_and_initialization_is_explicit(tmp_path: Path) -> None:
    path = tmp_path / "unrelated"
    path.mkdir()
    evidence = path / "notes.txt"
    evidence.write_text("preserve me", encoding="utf-8")
    with pytest.raises((CorruptionError, StorageError)):
        Database.open(path)
    assert evidence.read_text(encoding="utf-8") == "preserve me"
