"""Bind completely validated records and search keys to their exact byte image."""

from bisect import bisect_left, bisect_right
from collections.abc import Iterator
from dataclasses import dataclass
from itertools import islice

from lsm_store.errors import CorruptionError
from lsm_store.record import Record
from lsm_store.table.codec import decode_block
from lsm_store.table.metadata import BlockInfo


@dataclass(frozen=True, slots=True)
class Block:
    """An admitted immutable image; results own bytes independently of readers."""

    image: bytes
    records: tuple[Record, ...]
    keys: tuple[bytes, ...]
    index: BlockInfo
    max_sequence: int

    def get(self, key: bytes) -> Record | None:
        """Find a record without allocating a comparison callback."""
        position = bisect_left(self.keys, key)
        if position < len(self.keys) and self.keys[position] == key:
            return self.records[position]
        return None

    def iter_range(self, start: bytes, end: bytes) -> Iterator[Record]:
        """Iterate the inclusive selected interval without copying records."""
        first = bisect_left(self.keys, start)
        last = bisect_right(self.keys, end)
        return islice(self.records, first, max(first, last))


def admit_block(
    image: bytes,
    block: BlockInfo,
    *,
    max_sequence: int,
    max_payload_bytes: int,
    max_record_bytes: int,
) -> Block:
    """Check the entire frame and every record before exposing even one record."""
    records = decode_block(image, max_payload_bytes, max_record_bytes)
    if (
        len(records) != block.entry_count
        or records[0].key != block.min_key
        or records[-1].key != block.max_key
    ):
        raise CorruptionError("block contents disagree with its index")
    keys: list[bytes] = []
    previous_key: bytes | None = None
    for record in records:
        if record.sequence > max_sequence or (
            previous_key is not None and previous_key >= record.key
        ):
            raise CorruptionError("block contents disagree with its index")
        keys.append(record.key)
        previous_key = record.key
    return Block(image, records, tuple(keys), block, max_sequence)
