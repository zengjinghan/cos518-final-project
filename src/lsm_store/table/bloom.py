"""A persisted Bloom filter with a stable, language-independent hash contract."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import blake2b
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from lsm_store.config import BloomConfig

_PERSONALIZATION: Final = b"lsm-store-bloom"


def _hash_pair(key: bytes, bit_count: int) -> tuple[int, int]:
    digest = blake2b(key, digest_size=16, person=_PERSONALIZATION).digest()
    index = int.from_bytes(digest[:8], "big") % bit_count
    step = (int.from_bytes(digest[8:], "big") | 1) % bit_count
    return index, step


def _indices(key: bytes, bit_count: int, hash_count: int) -> Iterator[int]:
    index, step = _hash_pair(key, bit_count)
    for _ in range(hash_count):
        yield index
        index = (index + step) % bit_count


@dataclass(frozen=True, slots=True)
class BloomFilter:
    """Immutable membership hint; positive answers are not proof of presence."""

    bits: bytes
    hash_count: int

    @property
    def bit_count(self) -> int:
        """Return the exact persisted bit count."""
        return len(self.bits) * 8

    def may_contain(self, key: bytes) -> bool:
        """Return false only when an inserted key cannot be present."""
        bits = self.bits
        bit_count = len(bits) * 8
        index, step = _hash_pair(key, bit_count)
        # Membership can stop at its first missing bit. Keep that hot loop here
        # instead of suspending/resuming the construction iterator for every bit.
        for _ in range(self.hash_count):
            if not bits[index // 8] & (1 << (index % 8)):
                return False
            index = (index + step) % bit_count
        return True


def build_filter(keys: Iterable[bytes], entry_count: int, config: BloomConfig) -> BloomFilter:
    """Build a filter with the dimensions and bit order in ``docs/format.md``."""
    bit_count, hash_count = config.dimensions(entry_count)
    bits = bytearray(bit_count // 8)
    for key in keys:
        for index in _indices(key, bit_count, hash_count):
            bits[index // 8] |= 1 << (index % 8)
    return BloomFilter(bytes(bits), hash_count)
