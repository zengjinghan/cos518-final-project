"""Independent adversarial checks of publication, interruption and process ownership."""

from __future__ import annotations

import errno
import os
import subprocess
import sys
from pathlib import Path
from threading import Thread

import pytest

from lsm_store import Database, StoreConfig
from lsm_store import database as database_module
from lsm_store.errors import CorruptionError, LifecycleError, StorageError
from lsm_store.metrics import Metrics
from lsm_store.storage.file import WriteFile
from lsm_store.storage.filesystem import FileStore, table_filename
from lsm_store.storage.manifest import load_manifest


def _run(script: str, root: Path) -> subprocess.CompletedProcess[str]:
    # Fixed interpreter and test-owned script; no shell or interpolated code.
    return subprocess.run(  # noqa: S603
        [sys.executable, "-c", script, str(root)],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_handle_compares_thread_objects_not_reusable_numeric_ids(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = Thread()
    other = Thread()
    assert owner.ident == other.ident  # Equal IDs do not establish the same owner.
    monkeypatch.setattr(database_module, "current_thread", lambda: owner)
    with Database.open(tmp_path / "store") as database:
        monkeypatch.setattr(database_module, "current_thread", lambda: other)
        with pytest.raises(LifecycleError):
            database.get(b"key")
        with pytest.raises(LifecycleError):
            database.close()
        monkeypatch.setattr(database_module, "current_thread", lambda: owner)


def test_ungraceful_process_exit_preserves_acknowledged_mutations(tmp_path: Path) -> None:
    root = tmp_path / "db"
    result = _run(
        """
import os
import sys
from pathlib import Path
from lsm_store import Database, StoreConfig
from lsm_store.config import TieredConfig

config = StoreConfig(
    memtable_bytes=128, table_bytes=128, block_bytes=64,
    compaction=TieredConfig(max_l0_tables=1, level_bytes=512, multiplier=2),
)
database = Database.open(Path(sys.argv[1]), config)
for number in range(40):
    database.put(number.to_bytes(2, 'big'), b'first')
for number in range(0, 40, 2):
    database.delete(number.to_bytes(2, 'big'))
database.put(b'last', b'unflushed')
print('acknowledged', flush=True)
os._exit(17)
""",
        root,
    )
    assert result.returncode == 17, result.stderr
    assert result.stdout.strip() == "acknowledged"
    with Database.open(root) as recovered:
        assert recovered.get(b"last") == b"unflushed"
        for number in range(40):
            assert recovered.get(number.to_bytes(2, "big")) == (
                None if number % 2 == 0 else b"first"
            )
        recovered.put(b"after-recovery", b"new")


def test_forked_handle_cannot_write_and_child_close_retains_parent_lock(tmp_path: Path) -> None:
    result = _run(
        """
import os
import sys
from pathlib import Path
from lsm_store import Database
from lsm_store.errors import DatabaseLockedError, LifecycleError

path = Path(sys.argv[1])
database = Database.open(path)
database.put(b'key', b'parent')
database.flush()
assert database.get(b'key') == b'parent'
child = os.fork()
if child == 0:
    try:
        database.put(b'key', b'child')
    except LifecycleError:
        pass
    else:
        os._exit(21)
    database.close()
    os._exit(0)
_, status = os.waitpid(child, 0)
assert os.waitstatus_to_exitcode(status) == 0
try:
    Database.open(path)
except DatabaseLockedError:
    pass
else:
    raise AssertionError('child close released parent lock')
database.put(b'key', b'parent-after-child')
database.close()
with Database.open(path) as reopened:
    assert reopened.get(b'key') == b'parent-after-child'
print('parent lock retained')
""",
        tmp_path / "db",
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "parent lock retained"


def test_interruption_after_wal_sync_requires_reopen(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "db"
    database = Database.open(root)
    native_sync = WriteFile.sync

    def interrupt_after_sync(writer: WriteFile) -> None:
        native_sync(writer)
        raise KeyboardInterrupt

    with monkeypatch.context() as patch:
        patch.setattr(WriteFile, "sync", interrupt_after_sync)
        with pytest.raises(KeyboardInterrupt):
            database.put(b"key", b"uncertain")
    try:
        with pytest.raises(LifecycleError, match="faulted"):
            database.put(b"key", b"retry-with-stale-sequence")
    finally:
        database.close()
    with Database.open(root) as recovered:
        assert recovered.get(b"key") == b"uncertain"
        recovered.put(b"key", b"safe-retry")


@pytest.mark.parametrize("target_name", ["MANIFEST", "WAL"])
def test_failure_after_replacement_faults_handle_and_recovers_committed_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, target_name: str
) -> None:
    root = tmp_path / "db"
    database = Database.open(root)
    database.put(b"first", b"acknowledged")
    database.flush()
    database.put(b"second", b"also-acknowledged")
    native_replace = Path.replace

    def replace_then_fail(path: Path, target: Path) -> Path:
        result = native_replace(path, target)
        if target.name == target_name:
            raise OSError(errno.EIO, "injected failure after replacement")
        return result

    with monkeypatch.context() as patch:
        patch.setattr(Path, "replace", replace_then_fail)
        with pytest.raises(StorageError, match="after replacement"):
            database.flush()
    try:
        with pytest.raises(LifecycleError, match="faulted"):
            database.get(b"first")
    finally:
        database.close()
    with Database.open(root) as recovered:
        assert recovered.scan(b"", b"z") == [
            (b"first", b"acknowledged"),
            (b"second", b"also-acknowledged"),
        ]
        recovered.put(b"third", b"continues")
        recovered.flush()


def test_corrupt_committed_table_prevents_orphan_cleanup(tmp_path: Path) -> None:
    root = tmp_path / "db"
    with Database.open(root) as database:
        database.put(b"key", b"value")
        database.flush()
    live = next(root.glob("*.sst"))
    original = live.read_bytes()
    damaged = bytearray(original)
    damaged[-12] ^= 1
    live.write_bytes(damaged)
    orphan = root / table_filename(999)
    orphan.write_bytes(b"uncommitted-output")
    pending = root / (".pending-" + "a" * 32)
    pending.write_bytes(b"uncommitted-metadata")
    with pytest.raises(CorruptionError):
        Database.open(root)
    assert orphan.read_bytes() == b"uncommitted-output"
    assert pending.read_bytes() == b"uncommitted-metadata"
    live.write_bytes(original)
    with Database.open(root) as recovered:
        assert recovered.get(b"key") == b"value"
    assert not orphan.exists()
    assert not pending.exists()
    with FileStore.open(root, Metrics(), create=False) as store:
        assert load_manifest(store).next_file_id >= 1000


def test_recovery_truncation_failure_releases_resources_without_losing_acknowledged_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "db"
    with Database.open(root, StoreConfig(memtable_bytes=1024)) as database:
        database.put(b"key", b"acknowledged")
    wal = root / "WAL"
    acknowledged = wal.read_bytes()
    with wal.open("ab") as stream:
        stream.write(b"partial")

    def fail_fsync(_descriptor: int) -> None:
        raise OSError(errno.EIO, "injected recovery sync failure")

    with monkeypatch.context() as patch:
        patch.setattr(os, "fsync", fail_fsync)
        with pytest.raises(StorageError, match="recovery sync"):
            Database.open(root)
    assert wal.read_bytes() == acknowledged
    with Database.open(root) as recovered:
        assert recovered.get(b"key") == b"acknowledged"
