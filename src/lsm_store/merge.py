"""Resolve versions once for reads, inspection and compaction."""

from __future__ import annotations

from heapq import merge
from operator import attrgetter
from typing import TYPE_CHECKING

from .errors import CorruptionError

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from .record import Record


def newest_record(left: Record, right: Record) -> Record:
    """Choose between versions of one key, rejecting conflicting sequence ties."""
    if left.sequence == right.sequence and left != right:
        message = "corrupt version input: conflicting equal-sequence records"
        raise CorruptionError(message)
    return left if left.sequence >= right.sequence else right


def merge_latest(streams: Iterable[Iterator[Record]]) -> Iterator[Record]:
    """Merge sorted unique-key streams, retaining each key's newest mutation.

    Each input owner guarantees increasing keys. Tombstones remain records here;
    only a caller producing visible values may omit them. Equal-sequence conflicts
    are corruption, not an opportunity to choose an incidental input order.
    """
    latest: Record | None = None
    for record in merge(*streams, key=attrgetter("key", "sequence")):
        if latest is None:
            latest = record
        elif record.key != latest.key:
            yield latest
            latest = record
        else:
            latest = newest_record(latest, record)
    if latest is not None:
        yield latest
