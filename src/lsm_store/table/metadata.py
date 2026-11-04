"""Immutable table descriptors and strict on-disk metadata admission."""

import json
import re
from dataclasses import dataclass
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from lsm_store import framing
from lsm_store.config import MAX_FILTER_BYTES
from lsm_store.errors import CorruptionError
from lsm_store.record import MAX_KEY_BYTES, RECORD_HEADER_BYTES
from lsm_store.table.bloom import BloomFilter
from lsm_store.table.codec import TABLE_MAGIC


@dataclass(frozen=True, slots=True)
class TableInfo:
    """A manifest-owned description of one committed immutable table."""

    table_id: int
    level: int
    min_key: bytes
    max_key: bytes
    max_sequence: int
    entry_count: int
    size_bytes: int


@dataclass(frozen=True, slots=True)
class BlockInfo:
    """A nonempty key interval and its absolute framed byte range."""

    min_key: bytes
    max_key: bytes
    offset: int
    length: int
    entry_count: int


@dataclass(frozen=True, slots=True)
class TableMetadata:
    """Validated per-reader data, without mutable DTOs or raw JSON."""

    table_id: int
    level: int
    min_key: bytes
    max_key: bytes
    max_sequence: int
    entry_count: int
    blocks: tuple[BlockInfo, ...]
    bloom: BloomFilter | None


class _MetadataModel(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class _BlockModel(_MetadataModel):
    min_key: str
    max_key: str
    offset: int = Field(ge=len(TABLE_MAGIC))
    length: int = Field(ge=framing.HEADER_BYTES + RECORD_HEADER_BYTES)
    entry_count: int = Field(gt=0)


class _BloomOffModel(_MetadataModel):
    kind: Literal["off"]


class _BloomModel(_MetadataModel):
    kind: Literal["bloom"]
    bit_count: int = Field(ge=64, le=MAX_FILTER_BYTES * 8)
    hash_count: int = Field(ge=1, le=32)
    bits: str


class _TableModel(_MetadataModel):
    version: int = Field(ge=1, le=1)
    table_id: int = Field(gt=0, lt=2**64)
    level: int = Field(ge=0, le=64)
    min_key: str
    max_key: str
    max_sequence: int = Field(gt=0, lt=2**64)
    entry_count: int = Field(gt=0)
    blocks: tuple[_BlockModel, ...] = Field(min_length=1)
    bloom: Annotated[_BloomOffModel | _BloomModel, Field(discriminator="kind")]


def _hex_bytes(value: str, limit: int) -> bytes:
    if len(value) % 2 or len(value) > 2 * limit or re.fullmatch("[0-9a-f]*", value) is None:
        message = "table metadata contains invalid or oversized lowercase hexadecimal bytes"
        raise CorruptionError(message)
    return bytes.fromhex(value)


def encode_metadata(metadata: TableMetadata) -> bytes:
    """Encode exact canonical JSON without coupling records to Pydantic."""
    bloom: _BloomModel | _BloomOffModel
    if metadata.bloom is None:
        bloom = _BloomOffModel(kind="off")
    else:
        bloom = _BloomModel(
            kind="bloom",
            bit_count=metadata.bloom.bit_count,
            hash_count=metadata.bloom.hash_count,
            bits=metadata.bloom.bits.hex(),
        )
    model = _TableModel(
        version=1,
        table_id=metadata.table_id,
        level=metadata.level,
        min_key=metadata.min_key.hex(),
        max_key=metadata.max_key.hex(),
        max_sequence=metadata.max_sequence,
        entry_count=metadata.entry_count,
        blocks=tuple(
            _BlockModel(
                min_key=block.min_key.hex(),
                max_key=block.max_key.hex(),
                offset=block.offset,
                length=block.length,
                entry_count=block.entry_count,
            )
            for block in metadata.blocks
        ),
        bloom=bloom,
    )
    return json.dumps(model.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode(
        "ascii"
    )


def decode_metadata(payload: bytes, max_record_bytes: int) -> TableMetadata:
    """Validate one bounded metadata region and reject noncanonical encodings."""
    try:
        model = _TableModel.model_validate_json(payload)
    except ValidationError as error:
        detail = error.errors(include_url=False, include_input=False)[0]
        location = ".".join(str(part) for part in detail["loc"]) or "root"
        message = f"invalid table metadata at {location}: {detail['msg']}"
        raise CorruptionError(message) from error
    bloom = None
    if isinstance(model.bloom, _BloomModel):
        bits = _hex_bytes(model.bloom.bits, MAX_FILTER_BYTES)
        if model.bloom.bit_count != len(bits) * 8:
            message = "Bloom bit count does not match its persisted bytes"
            raise CorruptionError(message)
        bloom = BloomFilter(bits, model.bloom.hash_count)
    key_limit = min(max_record_bytes, MAX_KEY_BYTES)
    metadata = TableMetadata(
        table_id=model.table_id,
        level=model.level,
        min_key=_hex_bytes(model.min_key, key_limit),
        max_key=_hex_bytes(model.max_key, key_limit),
        max_sequence=model.max_sequence,
        entry_count=model.entry_count,
        blocks=tuple(
            BlockInfo(
                min_key=_hex_bytes(block.min_key, key_limit),
                max_key=_hex_bytes(block.max_key, key_limit),
                offset=block.offset,
                length=block.length,
                entry_count=block.entry_count,
            )
            for block in model.blocks
        ),
        bloom=bloom,
    )
    if encode_metadata(metadata) != payload:
        message = "table metadata is not canonical JSON"
        raise CorruptionError(message)
    return metadata


def validate_index(
    metadata: TableMetadata,
    info: TableInfo,
    metadata_offset: int,
    max_payload_bytes: int,
) -> None:
    """Establish descriptor equality and contiguous, strictly ordered block ranges."""
    if (
        metadata.table_id != info.table_id
        or metadata.level != info.level
        or metadata.min_key != info.min_key
        or metadata.max_key != info.max_key
        or metadata.max_sequence != info.max_sequence
        or metadata.entry_count != info.entry_count
    ):
        message = f"table {info.table_id}: metadata disagrees with its manifest descriptor"
        raise CorruptionError(message)
    offset = len(TABLE_MAGIC)
    previous_max: bytes | None = None
    entries = 0
    for block in metadata.blocks:
        if (
            block.offset != offset
            or block.length > max_payload_bytes + framing.HEADER_BYTES
            or block.length > metadata_offset - offset
            or block.entry_count > (block.length - framing.HEADER_BYTES) // RECORD_HEADER_BYTES
            or block.min_key > block.max_key
            or (previous_max is not None and block.min_key <= previous_max)
        ):
            message = f"table {info.table_id}: invalid block index range or dimensions"
            raise CorruptionError(message)
        offset += block.length
        entries += block.entry_count
        previous_max = block.max_key
    if (
        offset != metadata_offset
        or entries != metadata.entry_count
        or metadata.blocks[0].min_key != metadata.min_key
        or metadata.blocks[-1].max_key != metadata.max_key
    ):
        message = f"table {info.table_id}: inconsistent table endpoints, byte ranges or entry count"
        raise CorruptionError(message)
