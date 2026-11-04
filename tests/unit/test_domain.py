from __future__ import annotations

import struct
from typing import TYPE_CHECKING

import pytest
from hypothesis import given
from hypothesis import strategies as st

from lsm_store.config import BloomConfig, StoreConfig, TieredConfig, load_config, parse_config
from lsm_store.errors import ArgumentError, ConfigurationError, CorruptionError
from lsm_store.framing import HEADER_BYTES, Header, check_payload, decode_header, encode
from lsm_store.memtable import MemTable
from lsm_store.merge import merge_latest
from lsm_store.record import (
    MAX_KEY_BYTES,
    MAX_SEQUENCE,
    RECORD_HEADER_BYTES,
    Delete,
    Put,
    Record,
    decode_record,
    decode_records,
    encode_frame,
    encode_record,
)

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.parametrize(
    "values",
    [
        {"memtable_bytes": True},
        {"memtable_bytes": "32"},
        {"memtable_bytes": 0},
        {"table_bytes": (1 << 30) + 1},
        {"max_record_bytes": (16 << 20) + 1},
        {"unknown": 1},
        {"compaction": {"kind": "leveled", "multiplier": 1}},
        {"compaction": {"kind": "tiered", "max_l0_tables": 1025}},
        {"compaction": {"kind": "hybrid"}},
        {"bloom": {"kind": "off", "false_positive_rate": 0.1}},
        {"bloom": {"kind": "bloom", "false_positive_rate": True}},
        {"bloom": {"kind": "bloom", "false_positive_rate": "0.1"}},
        {"bloom": {"kind": "bloom", "false_positive_rate": float("nan")}},
        {"bloom": {"kind": "bloom", "false_positive_rate": float("inf")}},
        {"bloom": {"kind": "bloom", "false_positive_rate": 0.0000009}},
        {"bloom": {"kind": "bloom", "false_positive_rate": 0.51}},
        {"instrumentation": "sometimes"},
    ],
)
def test_configuration_rejects_unadmitted_values(values: dict[str, object]) -> None:
    with pytest.raises(ConfigurationError, match="invalid configuration"):
        parse_config(values, source="test.toml")


def test_configuration_defaults_round_trip_and_capture_is_not_identity() -> None:
    config = StoreConfig()
    assert StoreConfig.model_validate(config.model_dump()) == config
    assert config.engine_dict() == StoreConfig(instrumentation="off").engine_dict()
    assert "instrumentation" not in config.engine_dict()
    assert BloomConfig(false_positive_rate=1e-6).false_positive_rate == 1e-6
    assert BloomConfig(false_positive_rate=0.5).false_positive_rate == 0.5


def test_configuration_admits_filter_capacity_before_table_writes() -> None:
    assert BloomConfig(false_positive_rate=0.5).dimensions(1) == (64, 32)
    assert BloomConfig(false_positive_rate=0.01).dimensions(100) == (960, 7)
    with pytest.raises(ConfigurationError, match="16 MiB"):
        parse_config(
            {"table_bytes": 1 << 30, "bloom": {"kind": "bloom", "false_positive_rate": 1e-6}}
        )
    assert parse_config({"table_bytes": 1 << 30, "bloom": {"kind": "off"}}).table_bytes == 1 << 30
    with pytest.raises(ArgumentError, match="positive integer"):
        BloomConfig().dimensions(0)
    with pytest.raises(ArgumentError, match="16 MiB"):
        BloomConfig().dimensions(10**1000)


def test_toml_override_replaces_tagged_value_and_reports_source(tmp_path: Path) -> None:
    path = tmp_path / "store.toml"
    path.write_text('[compaction]\nkind = "leveled"\nmax_l0_tables = 7\n', encoding="utf-8")
    config = load_config(path, {"compaction": {"kind": "tiered", "max_l0_tables": 2}})
    assert config.compaction == TieredConfig(max_l0_tables=2)
    path.write_text("memtable_bytes = true\n", encoding="utf-8")
    with pytest.raises(ConfigurationError, match=r"store\.toml"):
        load_config(path)
    path.write_text("[broken\n", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="cannot read configuration"):
        load_config(path)


@given(
    key=st.binary(max_size=64),
    value=st.binary(max_size=64),
    sequence=st.integers(min_value=1, max_value=MAX_SEQUENCE),
    deleted=st.booleans(),
)
def test_record_codec_round_trip(key: bytes, value: bytes, sequence: int, *, deleted: bool) -> None:
    record = Delete(key, sequence) if deleted else Put(key, sequence, value)
    assert decode_record(encode_record(record), 128) == record


def test_record_wire_vector_and_frame_size() -> None:
    payload = bytes.fromhex("00000000000000010100000001000000016b76")
    assert encode_record(Put(b"k", 1, b"v")) == payload
    assert decode_record(payload, 2) == Put(b"k", 1, b"v")
    assert encode_frame(Put(b"k", 1, b"v")) == bytes.fromhex(
        "00000013a5fa9ec218f95d2600000000000000010100000001000000016b76",
    )
    assert len(encode_frame(Put(b"k", 1, b"v"))) == 2 + RECORD_HEADER_BYTES + HEADER_BYTES
    assert decode_record(encode_record(Put(b"", 1, b"")), 0) == Put(b"", 1, b"")
    assert decode_record(encode_record(Delete(b"", 1)), 0) == Delete(b"", 1)


def test_shared_frame_header_has_independent_length_and_payload_checksums() -> None:
    payload = bytes.fromhex("00000000000000010100000001000000016b76")
    frame = bytes.fromhex("00000013a5fa9ec218f95d2600000000000000010100000001000000016b76")
    assert encode(payload) == frame
    header = decode_header(frame[:HEADER_BYTES], minimum=17, maximum=32)
    assert header == Header(19, 0x18F95D26)
    check_payload(frame[HEADER_BYTES:], header.payload_crc)
    check_payload(b"123456789", 0xCBF43926)
    with pytest.raises(CorruptionError, match="length checksum"):
        decode_header(bytes.fromhex("00000012a5fa9ec218f95d26"), minimum=17, maximum=32)
    with pytest.raises(CorruptionError, match="inadmissible payload length"):
        decode_header(frame[:HEADER_BYTES], minimum=20, maximum=32)
    with pytest.raises(CorruptionError, match="protected header"):
        decode_header(frame[: HEADER_BYTES - 1], minimum=17, maximum=32)
    with pytest.raises(CorruptionError, match="payload checksum"):
        check_payload(b"wrong payload", header.payload_crc)


@pytest.mark.parametrize(
    "payload",
    [
        b"",
        b"\x00" * 16,
        struct.pack("!QBII", 0, 1, 0, 0),
        struct.pack("!QBII", 1, 3, 0, 0),
        struct.pack("!QBII", 1, 2, 0, 1) + b"v",
        struct.pack("!QBII", 1, 1, 2, 1) + b"kv",
        struct.pack("!QBII", 1, 1, 0, 0) + b"extra",
        struct.pack("!QBII", 1, 1, 0xFFFFFFFF, 0),
    ],
)
def test_record_decoder_rejects_corrupt_payloads(payload: bytes) -> None:
    with pytest.raises(CorruptionError):
        decode_record(payload, 32)


def test_record_limit_is_combined_key_and_value() -> None:
    with pytest.raises(CorruptionError, match="record limit"):
        decode_record(encode_record(Put(b"key", 1, b"value")), 7)


def test_record_key_bound_is_separate_from_value_bound() -> None:
    boundary = Put(b"k" * MAX_KEY_BYTES, 1, b"")
    assert decode_record(encode_record(boundary), MAX_KEY_BYTES) == boundary
    with pytest.raises(CorruptionError, match="key length"):
        decode_record(encode_record(Put(b"k" * (MAX_KEY_BYTES + 1), 1, b"")), MAX_KEY_BYTES + 1)
    large_value = Put(b"k", 1, b"v" * (MAX_KEY_BYTES + 1))
    assert decode_record(encode_record(large_value), MAX_KEY_BYTES + 2) == large_value


def test_concatenated_records_share_one_exact_header_decoder() -> None:
    records = (Put(b"", 1, b""), Delete(b"a", 2), Put(b"z", 3, b"payload"))
    payload = b"".join(encode_record(record) for record in records)
    assert decode_records(payload, 32) == records
    assert decode_records(b"", 32) == ()
    with pytest.raises(CorruptionError, match="truncated mutation header"):
        decode_records(payload + b"\x00", 32)
    with pytest.raises(CorruptionError, match="payload length"):
        decode_records(payload[:-1], 32)


def test_memtable_replacement_owns_latest_and_exact_size() -> None:
    table = MemTable()
    table.apply(Put(b"k", 1, b"old"))
    table.apply(Put(b"k", 3, b""))
    table.apply(Put(b"k", 2, b"ignored"))
    assert table.get(b"k") == Put(b"k", 3, b"")
    assert table.size_bytes == 1 + RECORD_HEADER_BYTES + HEADER_BYTES
    table.apply(Delete(b"k", 4))
    table.apply(Delete(b"k", 4))
    assert list(table.records()) == [Delete(b"k", 4)]
    assert len(table) == 1
    with pytest.raises(CorruptionError, match="equal-sequence"):
        table.apply(Put(b"k", 4, b"conflict"))
    table.clear()
    assert len(table) == 0
    assert table.size_bytes == 0


def test_memtable_inclusive_and_unbounded_ranges() -> None:
    table = MemTable()
    for index, key in enumerate((b"c", b"", b"b", b"a"), start=1):
        table.apply(Put(key, index, key))
    assert [record.key for record in table.records(b"a", b"b")] == [b"a", b"b"]
    assert [record.key for record in table.records(end=b"a")] == [b"", b"a"]
    assert [record.key for record in table.records(start=b"b")] == [b"b", b"c"]
    assert list(table.records(b"z", b"a")) == []


@given(
    st.lists(st.tuples(st.binary(max_size=3), st.binary(max_size=3), st.booleans()), max_size=100)
)
def test_merge_latest_agrees_with_independent_chronological_model(
    mutations: list[tuple[bytes, bytes, bool]],
) -> None:
    expected: dict[bytes, Record] = {}
    partitions: tuple[dict[bytes, Record], ...] = ({}, {}, {})
    for sequence, (key, value, deleted) in enumerate(mutations, start=1):
        record = Delete(key, sequence) if deleted else Put(key, sequence, value)
        expected[key] = record
        partitions[sequence % len(partitions)][key] = record
    streams = [
        iter(sorted(partition.values(), key=lambda record: record.key)) for partition in partitions
    ]
    assert list(merge_latest(streams)) == [expected[key] for key in sorted(expected)]


def test_merge_rejects_equal_sequence_conflicts_even_below_latest() -> None:
    streams = [iter([Put(b"k", 9, b"latest")]), iter([Put(b"k", 1, b"a")]), iter([Delete(b"k", 1)])]
    with pytest.raises(CorruptionError, match="equal-sequence"):
        list(merge_latest(streams))
    assert list(merge_latest([iter([Put(b"k", 1, b"")]), iter([Put(b"k", 1, b"")])])) == [
        Put(b"k", 1, b""),
    ]
