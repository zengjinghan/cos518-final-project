"""One protected-frame grammar shared by journal, manifest and table blocks."""

from __future__ import annotations

from dataclasses import dataclass
from struct import Struct
from typing import Final
from zlib import crc32

from .errors import CorruptionError

HEADER_BYTES: Final = 12
_HEADER: Final = Struct("!III")
_LENGTH: Final = Struct("!I")


@dataclass(frozen=True, slots=True)
class Header:
    """A checked length and the expected checksum of that frame's payload."""

    length: int
    payload_crc: int


def encode(payload: bytes) -> bytes:
    """Protect the payload length separately from the payload's checksum."""
    length = _LENGTH.pack(len(payload))
    return _HEADER.pack(len(payload), crc32(length), crc32(payload)) + payload


def decode_header(header: bytes, *, minimum: int, maximum: int) -> Header:
    """Check the complete header before its length can control reads/allocations."""
    if len(header) != HEADER_BYTES:
        message = "corrupt frame: truncated or oversized protected header"
        raise CorruptionError(message)
    length, length_crc, payload_crc = _HEADER.unpack(header)
    if crc32(header[: _LENGTH.size]) != length_crc:
        message = "corrupt frame: length checksum mismatch"
        raise CorruptionError(message)
    if not minimum <= length <= maximum:
        message = f"corrupt frame: inadmissible payload length {length}"
        raise CorruptionError(message)
    return Header(length, payload_crc)


def check_payload(payload: bytes, expected_crc: int) -> None:
    """Verify complete payload bytes; the caller separately checks exact length."""
    if crc32(payload) != expected_crc:
        message = "corrupt frame: payload checksum mismatch"
        raise CorruptionError(message)
