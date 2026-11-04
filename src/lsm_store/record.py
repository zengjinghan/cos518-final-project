"""Owned mutation values and their single version-1 binary codec."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Final

from . import framing
from .errors import CorruptionError

MAX_SEQUENCE: Final = (1 << 64) - 1
MAX_KEY_BYTES: Final = 64 << 10
RECORD_HEADER_BYTES: Final = 17
_RECORD_HEADER: Final = struct.Struct("!QBII")
_PUT_TAG: Final = 1
_DELETE_TAG: Final = 2


@dataclass(frozen=True, slots=True)
class Put:
    """Replace a key with a byte value at a globally ordered sequence."""

    key: bytes
    sequence: int
    value: bytes


@dataclass(frozen=True, slots=True)
class Delete:
    """Hide all older values for a key without an optional value field."""

    key: bytes
    sequence: int


type Record = Put | Delete


def logical_bytes(record: Record) -> int:
    """Count user key/value bytes, independent of encoded storage overhead."""
    return len(record.key) + (len(record.value) if isinstance(record, Put) else 0)


def encode_record(record: Record) -> bytes:
    """Encode an already admitted mutation using unsigned big-endian fields."""
    if isinstance(record, Put):
        header = _RECORD_HEADER.pack(record.sequence, _PUT_TAG, len(record.key), len(record.value))
        return header + record.key + record.value
    return _RECORD_HEADER.pack(record.sequence, _DELETE_TAG, len(record.key), 0) + record.key


def _decode_at(payload: bytes, offset: int, max_record_bytes: int) -> tuple[Record, int]:
    payload_length = len(payload)
    if payload_length - offset < RECORD_HEADER_BYTES:
        message = "corrupt record: truncated mutation header"
        raise CorruptionError(message)
    sequence, tag, key_length, value_length = _RECORD_HEADER.unpack_from(payload, offset)
    if sequence == 0:
        message = "corrupt record: sequence must be positive"
        raise CorruptionError(message)
    if key_length > MAX_KEY_BYTES:
        message = "corrupt record: key length exceeds 64 KiB limit"
        raise CorruptionError(message)
    if key_length + value_length > max_record_bytes:
        message = "corrupt record: key/value bytes exceed configured record limit"
        raise CorruptionError(message)
    record_end = offset + RECORD_HEADER_BYTES + key_length + value_length
    if record_end > payload_length:
        message = "corrupt record: payload length does not match key/value lengths"
        raise CorruptionError(message)
    key_start = offset + RECORD_HEADER_BYTES
    key_end = key_start + key_length
    key = payload[key_start:key_end]
    if tag == _PUT_TAG:
        return Put(key, sequence, payload[key_end:record_end]), record_end
    if tag == _DELETE_TAG and value_length == 0:
        return Delete(key, sequence), record_end
    message = f"corrupt record: unsupported tag {tag} or nonempty delete value"
    raise CorruptionError(message)


def decode_record(payload: bytes, max_record_bytes: int) -> Record:
    """Validate one exact mutation payload before exposing a trusted value."""
    record, end = _decode_at(payload, 0, max_record_bytes)
    if end != len(payload):
        message = "corrupt record: trailing bytes after mutation"
        raise CorruptionError(message)
    return record


def decode_records(payload: bytes, max_record_bytes: int) -> tuple[Record, ...]:
    """Decode concatenated mutation payloads without a second header grammar."""
    records: list[Record] = []
    offset = 0
    payload_length = len(payload)
    while offset < payload_length:
        record, offset = _decode_at(payload, offset, max_record_bytes)
        records.append(record)
    return tuple(records)


def encode_frame(record: Record) -> bytes:
    """Protect payload bytes and their length independently for crash-safe parsing."""
    return framing.encode(encode_record(record))
