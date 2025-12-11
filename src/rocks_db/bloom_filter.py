"""Key filter abstraction used by the LSM engine."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from enum import Enum
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field
from rbloom import Bloom

__all__ = (
    "BloomFilterLike",
    "NoOpBloomFilter",
    "SetBloomFilter",
    "create_bloom_filter",
)


class BloomFilterType(str, Enum):
    """Enumeration of available BloomFilter backends for benchmarking."""

    BLOOM = "bloom"
    SET = "set"
    DISABLED = "disabled"


class BloomFilterConfig(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    type: BloomFilterType = Field(..., description="Which Bloom Filter implementation to use.")
    args: dict[str, Any] = Field(
        default_factory=dict,
        description="Implementation-specific constructor arguments.",
    )


class BloomFilterLike(Protocol):
    """Minimal protocol implemented by BloomFilter and NoOpBloomFilter."""

    def add(self, key: bytes) -> None:
        """Add a single key to the filter."""
        ...

    def update(self, keys: Iterable[bytes]) -> None:
        """Add multiple keys to the filter."""
        ...

    def __contains__(self, key: bytes) -> bool:
        """Check if a key is in the filter."""
        ...


class NoOpBloomFilter:
    """Bloom filter that behaves as if filtering is disabled.

    - All insert/update operations are ignored.
    - Membership tests always return True ("maybe present").

    This guarantees that call sites like:

        if key not in bloom:
            # skip SSTable

    NEVER skip when Bloom filtering is disabled. The NoOpBloomFilter
    therefore preserves correctness while letting the rest of the code
    assume it always has a BloomFilter-like object.
    """

    def add(self, key: bytes) -> None:
        return

    def update(self, keys: Iterable[bytes]) -> None:
        return

    @property
    def is_enabled(self):
        return False

    def __contains__(self, key: bytes) -> bool:
        # Always maybe: never rule anything out.
        return True


class SetBloomFilter:
    """Alias for the key filter type used by SSTables."""

    __slots__ = ("_data",)

    def __init__(self):
        self._data: set[bytes] = set()

    def add(self, key: bytes) -> None:
        self._data.add(key)

    def update(self, keys: Iterable[bytes]) -> None:
        self._data.update(keys)

    @property
    def is_enabled(self):
        return True

    def __contains__(self, key: bytes) -> bool:
        return key in self._data


class BloomFilter:
    """Alias for the key filter type used by SSTables."""

    __slots__ = ("_data",)

    def __init__(self, **kwargs):
        self._data = Bloom(**kwargs)

    def add(self, key: bytes) -> None:
        self._data.add(key)

    def update(self, keys: Iterable[bytes]) -> None:
        self._data.update(keys)

    @property
    def is_enabled(self):
        return True

    def __contains__(self, key: bytes) -> bool:
        return key in self._data


_BLOOM_FILTER_REGISTRY: Mapping[BloomFilterType, Callable[..., BloomFilterLike]] = {
    BloomFilterType.BLOOM: BloomFilter,
    BloomFilterType.SET: SetBloomFilter,
    BloomFilterType.DISABLED: NoOpBloomFilter,
}


def create_bloom_filter(
    type: BloomFilterType,
    args: dict[str, Any] | None = None,
) -> BloomFilterLike:
    """Instantiate a BloomFilter."""
    if args is None:
        args = {}

    try:
        compaction_class = _BLOOM_FILTER_REGISTRY[type]
    except KeyError as exc:
        raise ValueError(f"Unknown disk type: {type!r}") from exc

    return compaction_class(**args)
