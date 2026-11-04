"""Encode the single authoritative live-file set in a protected JSON envelope."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from lsm_store.config import StoreConfig
from lsm_store.errors import CorruptionError, UnsupportedFormatError
from lsm_store.framing import HEADER_BYTES, check_payload, decode_header, encode
from lsm_store.metrics import IoPurpose
from lsm_store.record import MAX_KEY_BYTES, MAX_SEQUENCE
from lsm_store.table.metadata import TableInfo

if TYPE_CHECKING:
    from lsm_store.storage.filesystem import FileStore

MANIFEST_MAGIC: Final = b"LSMMAN01"
MAX_MANIFEST_BYTES: Final = 64 << 20


@dataclass(frozen=True, slots=True)
class Manifest:
    """Immutable persistent authority, updated only by the version owner."""

    config: StoreConfig
    tables: tuple[TableInfo, ...]
    checkpoint_sequence: int
    next_file_id: int


class _MetadataModel(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class _TableModel(_MetadataModel):
    table_id: int = Field(gt=0, le=MAX_SEQUENCE)
    level: int = Field(ge=0, le=64)
    min_key: str = Field(max_length=2 * MAX_KEY_BYTES)
    max_key: str = Field(max_length=2 * MAX_KEY_BYTES)
    max_sequence: int = Field(gt=0, le=MAX_SEQUENCE)
    entry_count: int = Field(gt=0)
    size_bytes: int = Field(ge=57)


class _ManifestModel(_MetadataModel):
    config: dict[str, object]
    tables: tuple[_TableModel, ...]
    checkpoint_sequence: int = Field(ge=0, le=MAX_SEQUENCE)
    next_file_id: int = Field(gt=0, le=MAX_SEQUENCE + 1)


def _hex_key(value: str, limit: int) -> bytes:
    if len(value) % 2 or len(value) > 2 * limit or re.fullmatch("[0-9a-f]*", value) is None:
        msg = "corrupt manifest: invalid or oversized lowercase hexadecimal key"
        raise CorruptionError(msg)
    return bytes.fromhex(value)


def _validate(manifest: Manifest) -> None:
    ids: set[int] = set()
    greatest_sequence = 0
    key_limit = min(manifest.config.max_record_bytes, MAX_KEY_BYTES)
    for table in manifest.tables:
        if (
            table.table_id in ids
            or table.table_id >= manifest.next_file_id
            or table.min_key > table.max_key
            or len(table.min_key) > key_limit
            or len(table.max_key) > key_limit
            or table.entry_count > table.size_bytes // 17
        ):
            msg = f"corrupt manifest: inconsistent table descriptor {table.table_id}"
            raise CorruptionError(msg)
        ids.add(table.table_id)
        greatest_sequence = max(greatest_sequence, table.max_sequence)
    if greatest_sequence != manifest.checkpoint_sequence:
        msg = "corrupt manifest: checkpoint must equal the greatest persisted table sequence"
        raise CorruptionError(msg)


def _encode_payload(manifest: Manifest) -> bytes:
    try:
        model = _ManifestModel(
            config=manifest.config.engine_dict(),
            tables=tuple(
                _TableModel(
                    table_id=table.table_id,
                    level=table.level,
                    min_key=table.min_key.hex(),
                    max_key=table.max_key.hex(),
                    max_sequence=table.max_sequence,
                    entry_count=table.entry_count,
                    size_bytes=table.size_bytes,
                )
                for table in manifest.tables
            ),
            checkpoint_sequence=manifest.checkpoint_sequence,
            next_file_id=manifest.next_file_id,
        )
    except ValidationError as exc:
        msg = f"invalid manifest values: {exc}"
        raise CorruptionError(msg) from exc
    _validate(manifest)
    payload = json.dumps(
        model.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
    ).encode("ascii")
    if len(payload) > MAX_MANIFEST_BYTES:
        msg = "manifest exceeds the 64 MiB metadata limit"
        raise CorruptionError(msg)
    return payload


def _decode_payload(payload: bytes) -> Manifest:
    try:
        model = _ManifestModel.model_validate_json(payload)
        if {"instrumentation", "read_cache"}.intersection(model.config):
            msg = "corrupt manifest: instrumentation/read_cache are not persistent engine settings"
            raise CorruptionError(msg)
        config = StoreConfig.model_validate(model.config)
    except ValidationError as exc:
        msg = f"corrupt manifest metadata: {exc}"
        raise CorruptionError(msg) from exc
    manifest = Manifest(
        config=config,
        tables=tuple(
            TableInfo(
                table_id=table.table_id,
                level=table.level,
                min_key=_hex_key(table.min_key, min(config.max_record_bytes, MAX_KEY_BYTES)),
                max_key=_hex_key(table.max_key, min(config.max_record_bytes, MAX_KEY_BYTES)),
                max_sequence=table.max_sequence,
                entry_count=table.entry_count,
                size_bytes=table.size_bytes,
            )
            for table in model.tables
        ),
        checkpoint_sequence=model.checkpoint_sequence,
        next_file_id=model.next_file_id,
    )
    if _encode_payload(manifest) != payload:
        msg = "corrupt manifest: metadata is not canonical JSON"
        raise CorruptionError(msg)
    return manifest


def load_manifest(store: FileStore) -> Manifest:
    """Validate the envelope and schema before exposing trusted manifest values."""
    with store.open_reader("MANIFEST") as reader:
        size = reader.size_bytes
        if size < len(MANIFEST_MAGIC) + HEADER_BYTES:
            msg = "corrupt manifest: truncated envelope"
            raise CorruptionError(msg)
        magic = reader.read_at(0, len(MANIFEST_MAGIC), IoPurpose.RECOVERY)
        if magic != MANIFEST_MAGIC:
            msg = f"unsupported manifest magic: {magic!r}"
            raise UnsupportedFormatError(msg)
        header_bytes = reader.read_at(len(MANIFEST_MAGIC), HEADER_BYTES, IoPurpose.RECOVERY)
        try:
            header = decode_header(header_bytes, minimum=1, maximum=MAX_MANIFEST_BYTES)
        except CorruptionError as exc:
            msg = f"corrupt manifest frame header: {exc}"
            raise CorruptionError(msg) from exc
        if size != len(MANIFEST_MAGIC) + HEADER_BYTES + header.length:
            msg = "corrupt manifest: invalid payload length or trailing bytes"
            raise CorruptionError(msg)
        payload = reader.read_at(
            len(MANIFEST_MAGIC) + HEADER_BYTES, header.length, IoPurpose.RECOVERY
        )
        if len(payload) != header.length:
            msg = "corrupt manifest: missing payload bytes"
            raise CorruptionError(msg)
        try:
            check_payload(payload, header.payload_crc)
        except CorruptionError as exc:
            msg = f"corrupt manifest frame payload: {exc}"
            raise CorruptionError(msg) from exc
    return _decode_payload(payload)


def save_manifest(store: FileStore, manifest: Manifest) -> None:
    """Publish one validated canonical manifest with synchronized replacement."""
    payload = _encode_payload(manifest)
    store.atomic_write("MANIFEST", MANIFEST_MAGIC + encode(payload), IoPurpose.METADATA)
