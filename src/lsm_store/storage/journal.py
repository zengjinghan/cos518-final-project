"""Append/fsync mutations and recover only a validated journal prefix."""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from lsm_store.errors import ArgumentError, CorruptionError, LifecycleError, UnsupportedFormatError
from lsm_store.framing import HEADER_BYTES, check_payload, decode_header
from lsm_store.metrics import IoPurpose
from lsm_store.record import (
    RECORD_HEADER_BYTES,
    Record,
    decode_record,
    encode_frame,
)

if TYPE_CHECKING:
    from lsm_store.storage.file import ReadFile
    from lsm_store.storage.filesystem import FileStore

WAL_MAGIC: Final = b"LSMWAL01"


def _read_frame(reader: ReadFile, offset: int, max_record_bytes: int) -> tuple[Record, int] | None:
    """Distinguish a protected complete frame from one interrupted terminal append."""
    header_bytes = reader.read_at(offset, HEADER_BYTES, IoPurpose.RECOVERY)
    if len(header_bytes) < HEADER_BYTES:
        return None
    try:
        header = decode_header(
            header_bytes,
            minimum=RECORD_HEADER_BYTES,
            maximum=RECORD_HEADER_BYTES + max_record_bytes,
        )
    except CorruptionError as exc:
        msg = f"corrupt WAL frame header at offset {offset}: {exc}"
        raise CorruptionError(msg) from exc
    payload = reader.read_at(offset + HEADER_BYTES, header.length, IoPurpose.RECOVERY)
    if len(payload) < header.length:
        return None
    try:
        check_payload(payload, header.payload_crc)
        record = decode_record(payload, max_record_bytes)
    except CorruptionError as exc:
        msg = f"corrupt WAL frame payload at offset {offset}: {exc}"
        raise CorruptionError(msg) from exc
    return record, HEADER_BYTES + header.length


class Journal:
    """Own one append handle and the durable prefix acknowledged by its writes."""

    __slots__ = (
        "_bytes_since_checkpoint",
        "_closed",
        "_last_sequence",
        "_max_record_bytes",
        "_size_bytes",
        "_store",
        "_writer",
    )

    def __init__(self, store: FileStore, max_record_bytes: int) -> None:
        """Open a previously created WAL; recovery runs before coordinator writes."""
        self._store = store
        self._max_record_bytes = max_record_bytes
        self._writer = store.open_append("WAL", IoPurpose.JOURNAL)
        try:
            self._size_bytes = self._writer.size_bytes
        except BaseException:
            self._writer.close()
            raise
        self._bytes_since_checkpoint = 0
        self._last_sequence = 0
        self._closed = False

    def _check(self) -> None:
        if self._closed:
            msg = "journal is closed"
            raise LifecycleError(msg)

    @property
    def size_bytes(self) -> int:
        """Return cached total length without adding an fstat to each mutation."""
        self._check()
        return self._size_bytes

    @property
    def bytes_since_checkpoint(self) -> int:
        """Return encoded frame bytes not covered by the recovered checkpoint."""
        self._check()
        return self._bytes_since_checkpoint

    def recover(self, checkpoint_sequence: int) -> tuple[Record, ...]:
        """Validate all frames and durably discard only an incomplete final append."""
        self._check()
        recovered: list[Record] = []
        previous_sequence = 0
        bytes_since_checkpoint = 0
        with self._store.open_reader("WAL") as reader:
            size = reader.size_bytes
            magic = reader.read_at(0, len(WAL_MAGIC), IoPurpose.RECOVERY)
            if len(magic) != len(WAL_MAGIC):
                msg = "corrupt WAL: missing or truncated magic"
                raise CorruptionError(msg)
            if magic != WAL_MAGIC:
                msg = f"unsupported WAL magic: {magic!r}"
                raise UnsupportedFormatError(msg)
            offset = len(WAL_MAGIC)
            while offset < size:
                frame = _read_frame(reader, offset, self._max_record_bytes)
                if frame is None:
                    break
                record, frame_bytes = frame
                if record.sequence <= previous_sequence:
                    msg = (
                        f"corrupt WAL: sequence {record.sequence} does not increase "
                        f"after {previous_sequence}"
                    )
                    raise CorruptionError(msg)
                previous_sequence = record.sequence
                if record.sequence > checkpoint_sequence:
                    recovered.append(record)
                    bytes_since_checkpoint += frame_bytes
                offset += frame_bytes
        if offset < size:
            self._store.truncate("WAL", offset, IoPurpose.RECOVERY)
        self._last_sequence = previous_sequence
        self._size_bytes = offset
        self._bytes_since_checkpoint = bytes_since_checkpoint
        return tuple(recovered)

    def append(self, record: Record) -> None:
        """Append one mutation and synchronize it before acknowledging success."""
        self._check()
        if record.sequence <= self._last_sequence:
            msg = f"journal sequence must increase beyond {self._last_sequence}"
            raise ArgumentError(msg)
        frame = encode_frame(record)
        self._writer.write(frame)
        self._writer.sync()
        self._last_sequence = record.sequence
        self._size_bytes += len(frame)
        self._bytes_since_checkpoint += len(frame)

    def reset(self) -> None:
        """Replace a checkpointed WAL only after the caller has committed metadata."""
        self._check()
        self._writer.close()
        self._store.atomic_write("WAL", WAL_MAGIC, IoPurpose.JOURNAL)
        self._writer = self._store.open_append("WAL", IoPurpose.JOURNAL)
        self._size_bytes = len(WAL_MAGIC)
        self._bytes_since_checkpoint = 0
        self._last_sequence = 0

    def close(self) -> None:
        """Release the writer without treating close as a durability operation."""
        if self._closed:
            return
        self._closed = True
        self._writer.close()
