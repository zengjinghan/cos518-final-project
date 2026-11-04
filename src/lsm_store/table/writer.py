"""One bounded partitioning and publication-preparation path for all tables."""

from __future__ import annotations

from typing import TYPE_CHECKING

from lsm_store.errors import ArgumentError
from lsm_store.metrics import IoPurpose
from lsm_store.record import MAX_KEY_BYTES, RECORD_HEADER_BYTES, encode_record, logical_bytes
from lsm_store.table.bloom import build_filter
from lsm_store.table.codec import MAX_METADATA_BYTES, TABLE_MAGIC, encode_block, encode_footer
from lsm_store.table.metadata import BlockInfo, TableInfo, TableMetadata, encode_metadata

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Iterator

    from lsm_store.config import StoreConfig
    from lsm_store.record import Record
    from lsm_store.storage.file import WriteFile
    from lsm_store.storage.filesystem import FileStore


def _table_chunks(
    records: Iterable[Record], target_bytes: int, max_record_bytes: int
) -> Iterator[list[Record]]:
    chunk: list[Record] = []
    size = 0
    metadata_budget = 4096
    previous_key: bytes | None = None
    for record in records:
        if previous_key is not None and record.key <= previous_key:
            message = "table writer requires strictly increasing unique keys"
            raise ArgumentError(message)
        previous_key = record.key
        if len(record.key) > MAX_KEY_BYTES:
            message = f"table key exceeds the {MAX_KEY_BYTES}-byte format limit"
            raise ArgumentError(message)
        payload_bytes = logical_bytes(record)
        if payload_bytes > max_record_bytes:
            message = "table record exceeds the configured key/value byte limit"
            raise ArgumentError(message)
        encoded_bytes = payload_bytes + RECORD_HEADER_BYTES
        # This bounds repeated hex endpoints, per-block integers and Bloom bits,
        # even if every record needs its own block. It is deliberately conservative.
        metadata_bytes = 8 * len(record.key) + 1024
        if chunk and (
            size + encoded_bytes > target_bytes
            or metadata_budget + metadata_bytes > MAX_METADATA_BYTES
        ):
            yield chunk
            chunk = []
            size = 0
            metadata_budget = 4096
        chunk.append(record)
        size += encoded_bytes
        metadata_budget += metadata_bytes
    if chunk:
        yield chunk


def _write_blocks(
    writer: WriteFile, records: list[Record], target_bytes: int
) -> tuple[BlockInfo, ...]:
    blocks: list[BlockInfo] = []
    payload = bytearray()
    block_records: list[Record] = []
    for record in records:
        encoded = encode_record(record)
        if block_records and len(payload) + len(encoded) > target_bytes:
            blocks.append(_write_block(writer, payload, block_records))
            payload = bytearray()
            block_records = []
        payload.extend(encoded)
        block_records.append(record)
    if block_records:
        blocks.append(_write_block(writer, payload, block_records))
    return tuple(blocks)


def _write_block(writer: WriteFile, payload: bytearray, records: list[Record]) -> BlockInfo:
    frame = encode_block(bytes(payload))
    block = BlockInfo(records[0].key, records[-1].key, writer.size_bytes, len(frame), len(records))
    writer.write(frame)
    return block


def _write_table(  # noqa: PLR0913 - Effects, input, identity and format are explicit owners.
    store: FileStore,
    records: list[Record],
    config: StoreConfig,
    *,
    table_id: int,
    level: int,
    purpose: IoPurpose,
) -> TableInfo:
    bloom = None
    if config.bloom.kind == "bloom":
        bloom = build_filter((record.key for record in records), len(records), config.bloom)
    max_sequence = max(record.sequence for record in records)
    with store.create_table(table_id, purpose) as writer:
        writer.write(TABLE_MAGIC)
        blocks = _write_blocks(writer, records, config.block_bytes)
        metadata = TableMetadata(
            table_id=table_id,
            level=level,
            min_key=records[0].key,
            max_key=records[-1].key,
            max_sequence=max_sequence,
            entry_count=len(records),
            blocks=blocks,
            bloom=bloom,
        )
        payload = encode_metadata(metadata)
        if len(payload) > MAX_METADATA_BYTES:
            message = "table metadata exceeds the 64-MiB format limit"
            raise ArgumentError(message)
        writer.write(payload)
        writer.write(encode_footer(payload))
        writer.sync()
        size_bytes = writer.size_bytes
    return TableInfo(
        table_id, level, records[0].key, records[-1].key, max_sequence, len(records), size_bytes
    )


def write_tables(  # noqa: PLR0913 - One shared flush/compaction boundary, no kwargs registry.
    store: FileStore,
    records: Iterable[Record],
    level: int,
    allocate_id: Callable[[], int],
    config: StoreConfig,
    *,
    purpose: IoPurpose = IoPurpose.FLUSH,
) -> tuple[TableInfo, ...]:
    """Write/sync sorted runs; the version owner alone publishes their live set."""
    tables = tuple(
        _write_table(store, chunk, config, table_id=allocate_id(), level=level, purpose=purpose)
        for chunk in _table_chunks(records, config.table_bytes, config.max_record_bytes)
    )
    if tables:
        store.sync_directory(purpose)
    return tables
