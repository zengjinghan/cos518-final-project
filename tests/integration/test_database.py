from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from lsm_store import Database, StoreConfig
from lsm_store.config import BloomOffConfig, CaptureMode, LeveledConfig, TieredConfig
from lsm_store.errors import ArgumentError, ConfigurationError, DatabaseLockedError, LifecycleError
from lsm_store.metrics import IoPurpose


def small_config(policy: str = "leveled", capture: CaptureMode = "counters") -> StoreConfig:
    return StoreConfig(
        memtable_bytes=160,
        table_bytes=160,
        block_bytes=64,
        compaction=LeveledConfig(max_l0_tables=2, level_bytes=4096, multiplier=4)
        if policy == "leveled"
        else TieredConfig(max_l0_tables=2, level_bytes=4096, multiplier=4),
        instrumentation=capture,
    )


@pytest.mark.parametrize("policy", ["leveled", "tiered"])
@given(
    st.lists(
        st.tuples(st.integers(0, 5), st.binary(max_size=2), st.binary(max_size=6)), max_size=60
    )
)
@settings(max_examples=15, deadline=None)
def test_histories_match_independent_map(
    policy: str, history: list[tuple[int, bytes, bytes]]
) -> None:
    with TemporaryDirectory(prefix="lsm-model-") as directory:
        path = Path(directory) / "store"
        expected: dict[bytes, bytes] = {}
        database = Database.open(path, small_config(policy))
        try:
            for operation, key, value in history:
                if operation == 0:
                    database.put(key, value)
                    expected[key] = value
                elif operation == 1:
                    database.delete(key)
                    expected.pop(key, None)
                elif operation == 2:
                    assert database.get(key) == expected.get(key)
                elif operation == 3:
                    database.flush()
                elif operation == 4:
                    database.close()
                    database = Database.open(path)
                else:
                    assert database.scan(b"", b"\xff\xff") == sorted(expected.items())
            database.flush()
            assert database.scan(b"", b"\xff\xff") == sorted(expected.items())
        finally:
            database.close()
        with Database.open(path) as reopened:
            assert reopened.scan(b"", b"\xff\xff") == sorted(expected.items())


def test_flush_recovery_and_unique_reopened_file_ids(tmp_path: Path) -> None:
    path = tmp_path / "store"
    with Database.open(path, small_config()) as database:
        database.put(b"key", b"first")
        database.flush()
    old_names = {file.name for file in path.glob("*.sst")}
    with Database.open(path) as database:
        assert database.get(b"key") == b"first"
        database.put(b"key", b"second")
        database.flush()
        assert old_names < {file.name for file in path.glob("*.sst")}
    with Database.open(path) as database:
        assert database.get(b"key") == b"second"


def test_l0_key_order_never_becomes_version_order(tmp_path: Path) -> None:
    config = StoreConfig(
        memtable_bytes=1 << 20,
        table_bytes=1 << 20,
        compaction=LeveledConfig(max_l0_tables=3),
        bloom=BloomOffConfig(),
    )
    with Database.open(tmp_path / "store", config) as database:
        for batch in (
            [(b"0", b"isolated")],
            [(b"b", b"boundary"), (b"m", b"old"), (b"z", b"boundary")],
            [(b"a", b"boundary"), (b"m", b"new"), (b"y", b"boundary")],
            [(b"~", b"trigger")],
        ):
            for key, value in batch:
                database.put(key, value)
            database.flush()
        assert database.get(b"m") == b"new"
        assert database.scan(b"m", b"m") == [(b"m", b"new")]


def test_latest_only_memtable_still_bounds_overwrite_journal(tmp_path: Path) -> None:
    with Database.open(tmp_path / "store", small_config()) as database:
        for _ in range(30):
            database.put(b"key", b"value")
        assert database.snapshot().maintenance.flushes > 0
        assert database.inspect().journal_bytes < 160 + 8


def test_counters_and_inspection_have_separate_ownership(tmp_path: Path) -> None:
    with Database.open(tmp_path / "store") as database:
        before = database.snapshot()
        database.put(b"a", b"")
        database.put(b"b", b"value")
        database.delete(b"b")
        assert database.get(b"a") == b""
        assert database.get(b"b") is None
        assert database.scan(b"", b"z") == [(b"a", b"")]
        assert database.scan(b"z", b"a") == []
        database.flush()
        after = database.snapshot()
        window = after.since(before)
        assert (window.logical.puts, window.logical.deletes) == (2, 1)
        assert (window.logical.gets, window.logical.hits, window.logical.misses) == (2, 1, 1)
        assert (window.logical.scans, window.logical.scan_rows) == (2, 1)
        assert window.logical.mutation_bytes == 8
        assert window.logical.returned_bytes == 2
        inspection = database.inspect()
        delta = database.snapshot().since(after)
        assert delta.io[IoPurpose.POINT_READ].read_bytes == 0
        assert delta.io[IoPurpose.SCAN].read_bytes == 0
        assert delta.io[IoPurpose.INSPECTION].read_bytes > 0
        assert inspection.live_keys == 1
        assert inspection.live_logical_bytes == 1


@pytest.mark.parametrize("capture", ["off", "counters"])
def test_capture_modes_preserve_contents(tmp_path: Path, capture: CaptureMode) -> None:
    with Database.open(tmp_path / capture, small_config(capture=capture)) as database:
        for number in range(20):
            database.put(bytes([number]), bytes([number]))
        database.delete(b"\x02")
        database.flush()
        assert database.scan(b"", b"\xff") == [
            (bytes([n]), bytes([n])) for n in range(20) if n != 2
        ]
        snapshot = database.snapshot()
        assert snapshot.mode == capture
        assert snapshot.logical.puts == (0 if capture == "off" else 20)


def test_record_limits_preserve_admission_after_reopen(tmp_path: Path) -> None:
    path = tmp_path / "store"
    with Database.open(path, StoreConfig(max_record_bytes=4)) as database:
        database.put(b"abcd", b"")
        with pytest.raises(ArgumentError, match="combined"):
            database.put(b"abcd", b"x")
    with Database.open(path) as database:
        assert database.get(b"abcd") == b""
        before = database.snapshot()
        with pytest.raises(ArgumentError, match="key exceeds"):
            database.get(b"abcde")
        with pytest.raises(ArgumentError, match="value exceeds"):
            database.put(b"", b"abcde")
        assert database.snapshot() == before
        database.put(b"", b"abcd")
        assert database.scan(b"", b"abcd") == [(b"", b"abcd"), (b"abcd", b"")]


def test_lifecycle_lock_and_explicit_config_mismatch(tmp_path: Path) -> None:
    path = tmp_path / "store"
    database = Database.open(path, small_config())
    with pytest.raises(DatabaseLockedError):
        Database.open(path)
    with pytest.raises(ArgumentError):
        database.put(b"x" * 65537, b"value")
    database.put(b"valid", b"still usable")
    database.close()
    database.close()
    with pytest.raises(LifecycleError):
        database.get(b"valid")
    with pytest.raises(ConfigurationError):
        Database.open(path, StoreConfig())
    with Database.open(path) as reopened:
        assert reopened.get(b"valid") == b"still usable"
