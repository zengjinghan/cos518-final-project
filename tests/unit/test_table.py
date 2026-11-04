"""Independent format vectors and table semantics across real filesystem I/O."""

from __future__ import annotations

from dataclasses import replace
from itertools import count
from struct import pack, unpack
from typing import TYPE_CHECKING
from zlib import crc32

import pytest
from hypothesis import given
from hypothesis import strategies as st

from lsm_store.config import BloomConfig, BloomOffConfig, StoreConfig
from lsm_store.errors import ArgumentError, CorruptionError, LifecycleError
from lsm_store.metrics import IoPurpose, Metrics
from lsm_store.record import MAX_KEY_BYTES, Delete, Put
from lsm_store.storage.filesystem import FileStore
from lsm_store.table.bloom import BloomFilter, build_filter
from lsm_store.table.codec import decode_block, encode_block
from lsm_store.table.reader import TableReader
from lsm_store.table.writer import write_tables

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

    from lsm_store.record import Record
    from lsm_store.table.metadata import TableInfo


@pytest.fixture
def store(tmp_path: Path) -> Iterator[FileStore]:
    owner = FileStore.open(tmp_path / "store", Metrics())
    try:
        yield owner
    finally:
        owner.close()


def _plain_config(*, block_bytes: int = 32, table_bytes: int = 128) -> StoreConfig:
    return StoreConfig(
        block_bytes=block_bytes,
        table_bytes=table_bytes,
        bloom=BloomOffConfig(),
    )


def _write(store: FileStore, records: list[Record], config: StoreConfig) -> tuple[TableInfo, ...]:
    return write_tables(store, records, 0, count(1).__next__, config)


def _table_path(store: FileStore, info: TableInfo) -> Path:
    return store.root / f"table-{info.table_id:016x}.sst"


def _replace_metadata(path: Path, old: bytes, new: bytes) -> int:
    data = path.read_bytes()
    length, _, _, magic = unpack("!III8s", data[-20:])
    metadata_start = len(data) - 20 - length
    metadata = data[metadata_start:-20].replace(old, new)
    prefix = pack("!II", len(metadata), crc32(metadata))
    rewritten = (
        data[:metadata_start] + metadata + prefix + pack("!I", crc32(prefix + magic)) + magic
    )
    path.write_bytes(rewritten)
    return len(rewritten)


def test_one_key_table_matches_independent_golden_bytes(store: FileStore) -> None:
    config = _plain_config()
    (info,) = _write(store, [Put(b"k", 1, b"v")], config)
    metadata = (
        b'{"blocks":[{"entry_count":1,"length":31,"max_key":"6b","min_key":"6b","offset":8}],'
        b'"bloom":{"kind":"off"},"entry_count":1,"level":0,"max_key":"6b",'
        b'"max_sequence":1,"min_key":"6b","table_id":1,"version":1}'
    )
    golden_block = bytes.fromhex("00000013a5fa9ec218f95d2600000000000000010100000001000000016b76")
    golden_footer = bytes.fromhex("000000cce52a3984bda6cbd04c534d454e443031")
    assert (
        _table_path(store, info).read_bytes()
        == b"LSMSST01" + golden_block + metadata + golden_footer
    )
    with TableReader(store, info, config) as reader:
        assert reader.get(b"k", IoPurpose.POINT_READ) == Put(b"k", 1, b"v")


def test_bloom_hashing_matches_independent_golden_bits() -> None:
    bloom = build_filter([b"", b"a", b"abc", b"\xff"], 4, BloomConfig())
    assert bloom == BloomFilter(bytes.fromhex("a63248fe9f444319"), 11)
    assert bloom.bit_count == 64


@given(st.lists(st.binary(max_size=12), min_size=1, max_size=40, unique=True))
def test_bloom_never_excludes_an_inserted_key(keys: list[bytes]) -> None:
    bloom = build_filter(keys, len(keys), BloomConfig())
    assert all(bloom.may_contain(key) for key in keys)


def test_bloom_rejects_empty_entry_population() -> None:
    with pytest.raises(ArgumentError, match="positive"):
        build_filter([], 0, BloomConfig())


def test_point_and_scan_preserve_empty_values_and_tombstones(store: FileStore) -> None:
    config = _plain_config(block_bytes=35)
    records: list[Record] = [Put(b"", 3, b""), Delete(b"a", 5), Put(b"b", 2, b"value")]
    (info,) = _write(store, records, config)
    with TableReader(store, info, config) as reader:
        assert list(reader.iter_all(IoPurpose.SCAN)) == records
        assert list(reader.iter_range(b"a", b"b", IoPurpose.SCAN)) == records[1:]
        assert list(reader.iter_range(b"b", b"a", IoPurpose.SCAN)) == []
        assert reader.get(b"", IoPurpose.POINT_READ) == records[0]
        assert reader.get(b"a", IoPurpose.POINT_READ) == records[1]
        assert reader.get(b"ab", IoPurpose.POINT_READ) is None
        assert reader.get(b"z", IoPurpose.POINT_READ) is None


def test_block_boundaries_and_narrow_scans_do_not_load_other_blocks(tmp_path: Path) -> None:
    metrics = Metrics()
    store = FileStore.open(tmp_path / "store", metrics)
    config = _plain_config(block_bytes=19)
    records: list[Record] = [Put(bytes([key]), key, b"v") for key in range(1, 5)]
    try:
        (info,) = _write(store, records, config)
        with TableReader(store, info, config) as reader:
            before = metrics.snapshot()
            assert reader.get(b"\x02", IoPurpose.POINT_READ) == records[1]
            assert list(reader.iter_range(b"\x03", b"\x03", IoPurpose.SCAN)) == records[2:3]
            window = metrics.snapshot().since(before)
            assert window.io[IoPurpose.POINT_READ].read_calls == 1
            assert window.io[IoPurpose.SCAN].read_calls == 1
    finally:
        store.close()


def test_table_target_is_soft_but_partitioning_is_shared(store: FileStore) -> None:
    config = _plain_config(table_bytes=20)
    records: list[Record] = [Put(b"a", 1, b"a"), Put(b"b", 2, b"x" * 50), Delete(b"c", 3)]
    tables = _write(store, records, config)
    assert len(tables) == 3
    actual: list[Record] = []
    for info in tables:
        with TableReader(store, info, config) as reader:
            actual.extend(reader.iter_all(IoPurpose.COMPACTION))
    assert actual == records


def test_filters_are_persisted_and_read_after_the_writer_is_gone(store: FileStore) -> None:
    config = StoreConfig(block_bytes=30)
    records: list[Record] = [Put(b"a", 1, b""), Delete(b"b", 2), Put(b"c", 3, b"last")]
    (info,) = _write(store, records, config)
    with TableReader(store, info, config) as reader:
        assert [reader.get(record.key, IoPurpose.POINT_READ) for record in records] == records
        assert reader.get(b"absent", IoPurpose.POINT_READ) is None


def test_large_admitted_key_roundtrips_without_exceeding_metadata_budget(store: FileStore) -> None:
    config = _plain_config()
    record = Put(b"k" * MAX_KEY_BYTES, 1, b"value")
    (info,) = _write(store, [record], config)
    with TableReader(store, info, config) as reader:
        assert reader.get(record.key, IoPurpose.POINT_READ) == record


def test_writer_rejects_key_beyond_durable_format_bound(store: FileStore) -> None:
    with pytest.raises(ArgumentError, match="key exceeds"):
        _write(store, [Delete(b"k" * (MAX_KEY_BYTES + 1), 1)], _plain_config())
    assert not any(name.endswith(".sst") for name in store.names())


def test_metadata_budget_is_a_second_soft_table_partition_limit(
    store: FileStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("lsm_store.table.writer.MAX_METADATA_BYTES", 6000)
    config = _plain_config(table_bytes=100_000)
    records: list[Record] = [Put(bytes([key]), key, b"v") for key in range(1, 4)]
    tables = _write(store, records, config)
    assert len(tables) == 3
    actual: list[Record] = []
    for info in tables:
        with TableReader(store, info, config) as reader:
            actual.extend(reader.iter_all(IoPurpose.SCAN))
    assert actual == records


@pytest.mark.parametrize(
    "records",
    [[Put(b"a", 1, b"one"), Put(b"a", 2, b"two")], [Delete(b"b", 1), Delete(b"a", 2)]],
)
def test_writer_rejects_duplicate_and_unsorted_input(
    store: FileStore, records: list[Record]
) -> None:
    with pytest.raises(ArgumentError, match="strictly increasing"):
        _write(store, records, _plain_config())


def test_writer_does_not_create_an_empty_table(store: FileStore) -> None:
    assert _write(store, [], _plain_config()) == ()
    assert not any(name.endswith(".sst") for name in store.names())


def test_reader_detects_corrupt_accessed_block_lazily(store: FileStore) -> None:
    config = _plain_config(block_bytes=19)
    records: list[Record] = [Put(b"a", 1, b"v"), Put(b"b", 2, b"v")]
    (info,) = _write(store, records, config)
    path = _table_path(store, info)
    data = bytearray(path.read_bytes())
    data[8 + 31 + 12 + 17] ^= 1
    path.write_bytes(data)
    with TableReader(store, info, config) as reader:
        assert reader.get(b"a", IoPurpose.POINT_READ) == records[0]
        with pytest.raises(CorruptionError, match="checksum"):
            reader.get(b"b", IoPurpose.POINT_READ)


@pytest.mark.parametrize("damage", ["sequence", "order", "duplicate"])
def test_point_read_revalidates_entire_accessed_block(store: FileStore, damage: str) -> None:
    config = _plain_config(block_bytes=1024)
    records: list[Record] = [Put(b"a", 1, b"v"), Put(b"b", 2, b"v"), Put(b"c", 3, b"v")]
    (info,) = _write(store, records, config)
    path = _table_path(store, info)
    with TableReader(store, info, config) as reader:
        assert reader.get(b"a", IoPurpose.POINT_READ) == records[0]
        data = bytearray(path.read_bytes())
        # Independent wire offsets: 8-byte magic, 12-byte frame, 19-byte records.
        payload_start = 8 + 12
        if damage == "sequence":
            last = payload_start + 2 * 19
            data[last : last + 8] = pack("!Q", 4)
        else:
            middle_key = payload_start + 19 + 17
            data[middle_key] = ord("z" if damage == "order" else "c")
        # Repair the payload CRC so semantic checks, not checksum failure, reject it.
        data[16:20] = pack("!I", crc32(data[payload_start : payload_start + 3 * 19]))
        path.write_bytes(data)
        with pytest.raises(CorruptionError, match="contents disagree"):
            reader.get(b"a", IoPurpose.POINT_READ)


@pytest.mark.parametrize("position", [8, 12, 16])
def test_block_frame_protects_length_and_payload_independently(position: int) -> None:
    golden = bytearray.fromhex("00000013a5fa9ec218f95d2600000000000000010100000001000000016b76")
    golden[position] ^= 1
    with pytest.raises(CorruptionError, match="checksum"):
        decode_block(bytes(golden), 100, 100)


def test_block_rejects_trailing_partial_mutation() -> None:
    golden = bytes.fromhex("00000000000000010100000001000000016b76")
    with pytest.raises(CorruptionError, match="truncated mutation"):
        decode_block(encode_block(golden + b"\x00"), 100, 100)


def test_footer_checksum_is_checked_before_metadata_length(store: FileStore) -> None:
    config = _plain_config()
    (info,) = _write(store, [Put(b"k", 1, b"v")], config)
    path = _table_path(store, info)
    data = bytearray(path.read_bytes())
    data[-20] = 0xFF
    path.write_bytes(data)
    with pytest.raises(CorruptionError, match="footer"):
        TableReader(store, info, config)


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        (b'"offset":8', b'"offset":9', "block index"),
        (b'"length":31', b'"length":3100000000', "block index"),
        (b'{"blocks"', b'{"version":1,"blocks"', "canonical"),
        (b'"min_key":"6b"', b'"min_key":"6B"', "hexadecimal"),
    ],
)
def test_reader_rejects_inconsistent_or_noncanonical_metadata(
    store: FileStore, old: bytes, new: bytes, message: str
) -> None:
    config = _plain_config()
    (info,) = _write(store, [Put(b"k", 1, b"v")], config)
    size = _replace_metadata(_table_path(store, info), old, new)
    with pytest.raises(CorruptionError, match=message):
        TableReader(store, replace(info, size_bytes=size), config)


def test_block_index_and_contents_cannot_disagree(store: FileStore) -> None:
    config = _plain_config()
    (info,) = _write(store, [Put(b"k", 1, b"v")], config)
    path = _table_path(store, info)
    size = _replace_metadata(path, b'"max_sequence":1', b'"max_sequence":2')
    with pytest.raises(CorruptionError, match="manifest descriptor"):
        TableReader(store, replace(info, size_bytes=size), config)


def test_closed_reader_cannot_be_reused(store: FileStore) -> None:
    config = _plain_config()
    (info,) = _write(store, [Put(b"k", 1, b"v")], config)
    reader = TableReader(store, info, config)
    reader.close()
    reader.close()
    with pytest.raises(LifecycleError, match="closed"):
        reader.get(b"k", IoPurpose.POINT_READ)
