"""Binary table framing; no filesystem effects or version-selection rules."""

from struct import Struct
from typing import Final
from zlib import crc32

from lsm_store import framing
from lsm_store.errors import CorruptionError
from lsm_store.record import RECORD_HEADER_BYTES, Record, decode_records

TABLE_MAGIC: Final = b"LSMSST01"
FOOTER_MAGIC: Final = b"LSMEND01"
MAX_METADATA_BYTES: Final = 64 * 1024 * 1024
FOOTER: Final = Struct("!III8s")
_LENGTH: Final = Struct("!I")


def encode_block(payload: bytes) -> bytes:
    """Protect a complete nonempty block's length and payload separately."""
    return framing.encode(payload)


def decode_block(frame: bytes, max_payload_bytes: int, max_record_bytes: int) -> tuple[Record, ...]:
    """Admit framed block bytes, then validate every contained mutation."""
    header = framing.decode_header(
        frame[: framing.HEADER_BYTES], minimum=RECORD_HEADER_BYTES, maximum=max_payload_bytes
    )
    if len(frame) != framing.HEADER_BYTES + header.length:
        message = "invalid table block payload length"
        raise CorruptionError(message)
    payload = frame[framing.HEADER_BYTES :]
    framing.check_payload(payload, header.payload_crc)
    return decode_records(payload, max_record_bytes)


def encode_footer(metadata: bytes) -> bytes:
    """Protect metadata contents and footer dimensions independently."""
    prefix = _LENGTH.pack(len(metadata)) + _LENGTH.pack(crc32(metadata))
    return FOOTER.pack(len(metadata), crc32(metadata), crc32(prefix + FOOTER_MAGIC), FOOTER_MAGIC)


def decode_footer(footer: bytes, file_size: int) -> tuple[int, int]:
    """Return bounded metadata length/checksum without trusting unchecked offsets."""
    if len(footer) != FOOTER.size:
        message = "truncated table footer"
        raise CorruptionError(message)
    length, metadata_crc, footer_crc, magic = FOOTER.unpack(footer)
    if magic != FOOTER_MAGIC or crc32(footer[:8] + magic) != footer_crc:
        message = "invalid table footer magic or checksum"
        raise CorruptionError(message)
    minimum_prefix = len(TABLE_MAGIC) + framing.HEADER_BYTES + RECORD_HEADER_BYTES
    if not 0 < length <= MAX_METADATA_BYTES or length > file_size - FOOTER.size - minimum_prefix:
        message = "invalid table metadata length"
        raise CorruptionError(message)
    return length, metadata_crc
