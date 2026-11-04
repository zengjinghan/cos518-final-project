"""Cache invariants across real fresh reads, changed bytes, and bounded ownership."""

from __future__ import annotations

from dataclasses import replace
from itertools import count
from struct import pack
from typing import TYPE_CHECKING
from zlib import crc32

import pytest

from lsm_store import Database
from lsm_store.config import (
    BloomOffConfig,
    LeveledConfig,
    ReadCacheOffConfig,
    StoreConfig,
    ValidatedCacheConfig,
)
from lsm_store.errors import ArgumentError, CorruptionError
from lsm_store.metrics import IoPurpose, Metrics
from lsm_store.record import Delete, Put
from lsm_store.storage.filesystem import FileStore
from lsm_store.table.cache import ReadCache
from lsm_store.table.reader import TableReader
from lsm_store.table.writer import write_tables

if TYPE_CHECKING:
    from pathlib import Path


def _config(capacity: int = 1 << 20) -> StoreConfig:
    return StoreConfig(
        block_bytes=1024,
        bloom=BloomOffConfig(),
        read_cache=ValidatedCacheConfig(capacity_bytes=capacity),
    )


@pytest.mark.parametrize("capacity", [1, 3000, 1 << 20])
def test_budget_rejection_eviction_and_retirement(tmp_path: Path, capacity: int) -> None:
    config = _config(capacity)
    metrics = Metrics()
    cache = ReadCache(config, metrics)
    with FileStore.open(tmp_path / "store", metrics) as store:
        records = [Put(bytes([key]), key, b"v" * 800) for key in range(1, 5)]
        (info,) = write_tables(store, records, 0, count(1).__next__, config)
        with TableReader(store, info, config, cache=cache) as reader:
            for record in records * 2:
                assert reader.get(record.key, IoPurpose.POINT_READ) == record
                state = cache.info()
                assert 0 <= state.retained_bytes <= capacity
        if capacity == 1:
            assert cache.info().entries == 0
            assert metrics.snapshot().read_cache.rejections > 0
        elif capacity == 3000:
            assert metrics.snapshot().read_cache.evictions > 0
        else:
            assert metrics.snapshot().read_cache.block_reuses == len(records)
        cache.invalidate(info.table_id)
        assert cache.info().entries == 0
        assert cache.info().retained_bytes == 0
        cache.clear()
        assert cache.info().retained_bytes == 0


def test_hits_still_read_current_bytes_and_do_not_decode_again(tmp_path: Path) -> None:
    config = _config()
    metrics = Metrics()
    cache = ReadCache(config, metrics)
    with FileStore.open(tmp_path / "store", metrics) as store:
        records = [Put(b"a", 1, b""), Delete(b"b", 2), Put(b"c", 3, b"v")]
        (info,) = write_tables(store, records, 0, count(1).__next__, config)
        with TableReader(store, info, config, cache=cache) as reader:
            assert list(reader.iter_all(IoPurpose.SCAN)) == records
            before = metrics.snapshot()
            assert reader.get(b"a", IoPurpose.POINT_READ) == records[0]
            assert list(reader.iter_range(b"b", b"b", IoPurpose.SCAN)) == records[1:2]
            window = metrics.snapshot().since(before)
            assert window.read_cache.block_decodes == 0
            assert window.read_cache.block_reuses == 2
            assert window.io[IoPurpose.POINT_READ].read_calls == 1
            assert window.io[IoPurpose.SCAN].read_calls == 1
        before = metrics.snapshot()
        with TableReader(store, info, config, cache=cache):
            pass
        window = metrics.snapshot().since(before)
        assert window.read_cache.metadata_reuses == 1
        assert window.read_cache.metadata_decodes == 0
        assert window.io[IoPurpose.RECOVERY].read_calls == 3


@pytest.mark.parametrize("damage", ["valid_change", "checksum", "sequence", "order", "duplicate"])
def test_cached_block_never_hides_changed_bytes(tmp_path: Path, damage: str) -> None:
    config = _config()
    metrics = Metrics()
    cache = ReadCache(config, metrics)
    with FileStore.open(tmp_path / "store", metrics) as store:
        records = [Put(b"a", 1, b"v"), Put(b"b", 2, b"v"), Put(b"c", 3, b"v")]
        (info,) = write_tables(store, records, 0, count(1).__next__, config)
        path = store.root / "table-0000000000000001.sst"
        with TableReader(store, info, config, cache=cache) as reader:
            assert reader.get(b"a", IoPurpose.POINT_READ) == records[0]
            image = bytearray(path.read_bytes())
            payload_start = 20
            if damage in {"valid_change", "checksum"}:
                image[payload_start + 18] = ord("w")
            elif damage == "sequence":
                image[payload_start + 38 : payload_start + 46] = pack("!Q", 4)
            else:
                image[payload_start + 19 + 17] = ord("z" if damage == "order" else "c")
            if damage != "checksum":
                image[16:20] = pack("!I", crc32(image[payload_start : payload_start + 57]))
            path.write_bytes(image)
            if damage == "valid_change":
                assert reader.get(b"a", IoPurpose.POINT_READ) == Put(b"a", 1, b"w")
                assert metrics.snapshot().read_cache.block_decodes == 2
            else:
                with pytest.raises(CorruptionError):
                    reader.get(b"a", IoPurpose.POINT_READ)
            assert metrics.snapshot().read_cache.invalidations == 1


def test_reused_metadata_still_checks_current_descriptor_and_envelope(tmp_path: Path) -> None:
    config = _config()
    metrics = Metrics()
    cache = ReadCache(config, metrics)
    with FileStore.open(tmp_path / "store", metrics) as store:
        (info,) = write_tables(store, [Put(b"a", 1, b"v")], 0, count(1).__next__, config)
        with TableReader(store, info, config, cache=cache):
            pass
        with pytest.raises(CorruptionError, match="manifest descriptor"):
            TableReader(store, replace(info, max_sequence=2), config, cache=cache)
        changed_limits = config.model_copy(update={"max_record_bytes": 64})
        with pytest.raises(ArgumentError, match="decoding limits"):
            TableReader(store, info, changed_limits, cache=cache)
        path = store.root / "table-0000000000000001.sst"
        image = bytearray(path.read_bytes())
        image[-21] ^= 1
        path.write_bytes(image)
        with pytest.raises(CorruptionError, match="checksum"):
            TableReader(store, info, config, cache=cache)


@pytest.mark.parametrize("capture", ["off", "counters"])
def test_runtime_policy_does_not_change_persistent_identity(tmp_path: Path, capture: str) -> None:
    config = _config()
    path = tmp_path / "store"
    with Database.open(path, config) as database:
        database.put(b"", b"")
        database.flush()
        assert database.get(b"") == b""
        assert database.read_cache_info().entries > 0
    off = StoreConfig.model_validate(
        {**config.model_dump(), "read_cache": {"kind": "off"}, "instrumentation": capture}
    )
    assert off.engine_dict() == config.engine_dict()
    assert isinstance(off.read_cache, ReadCacheOffConfig)
    with Database.open(path, off, create=False) as database:
        assert database.get(b"") == b""
        assert database.read_cache_info().retained_bytes == 0
        assert database.read_cache_info().capacity_bytes == 0
        stats = database.snapshot().read_cache
        assert stats.block_decodes == (1 if capture == "counters" else 0)
        assert stats.block_reuses == 0


def test_merge_fan_in_exceeds_point_reader_capacity(tmp_path: Path) -> None:
    config = StoreConfig(
        memtable_bytes=1,
        compaction=LeveledConfig(max_l0_tables=64),
    )
    rows = [(bytes([key]), b"value") for key in range(40)]
    with Database.open(tmp_path / "store", config) as database:
        for key, value in rows:
            database.put(key, value)
        for key, value in rows:
            assert database.get(key) == value
        assert database.scan(b"", b"z") == rows
        assert database.read_cache_info().retained_bytes <= 1 << 20


def test_compaction_reuses_existing_entries_without_admitting_one_pass_inputs(
    tmp_path: Path,
) -> None:
    config = _config()
    metrics = Metrics()
    cache = ReadCache(config, metrics)
    with FileStore.open(tmp_path / "store", metrics) as store:
        record = Put(b"a", 1, b"v")
        (info,) = write_tables(store, [record], 0, count(1).__next__, config)
        with TableReader(store, info, config, cache=cache, purpose=IoPurpose.COMPACTION) as reader:
            assert list(reader.iter_all(IoPurpose.COMPACTION)) == [record]
        assert cache.info().entries == 0
        assert metrics.snapshot().read_cache.block_decodes == 1
        with TableReader(store, info, config, cache=cache) as reader:
            assert reader.get(b"a", IoPurpose.POINT_READ) == record
        state = cache.info()
        before = metrics.snapshot()
        with TableReader(store, info, config, cache=cache, purpose=IoPurpose.COMPACTION) as reader:
            assert list(reader.iter_all(IoPurpose.COMPACTION)) == [record]
        window = metrics.snapshot().since(before)
        assert window.read_cache.block_reuses == window.read_cache.metadata_reuses == 1
        assert window.read_cache.admissions == 0
        assert cache.info() == state
