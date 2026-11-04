"""Validate configuration once, before it reaches storage algorithms."""

from __future__ import annotations

import tomllib
from math import ceil, floor, log
from typing import TYPE_CHECKING, Annotated, Final, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from .errors import ArgumentError, ConfigurationError
from .record import RECORD_HEADER_BYTES

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

MAX_BUDGET_BYTES: Final = 1 << 30
MAX_RECORD_BYTES: Final = 16 << 20
MAX_THRESHOLD: Final = 1024
MAX_FILTER_BYTES: Final = 16 << 20
_MIN_FILTER_BITS: Final = 64
_MAX_HASH_COUNT: Final = 32
_LOG_TWO: Final = log(2)

type CaptureMode = Literal["off", "counters"]
type _ByteBudget = Annotated[int, Field(gt=0, le=MAX_BUDGET_BYTES)]
type _Threshold = Annotated[int, Field(gt=0, le=MAX_THRESHOLD)]


class _StrictModel(BaseModel):
    """External values never coerce or ignore misspelled fields."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid", validate_default=True)


class _CompactionLimits(_StrictModel):
    max_l0_tables: _Threshold = 4
    level_bytes: _ByteBudget = 16 << 20
    multiplier: Annotated[int, Field(gt=1, le=MAX_THRESHOLD)] = 10


class LeveledConfig(_CompactionLimits):
    """Maintain disjoint key ranges in levels above zero."""

    kind: Literal["leveled"] = "leveled"


class TieredConfig(_CompactionLimits):
    """Merge all tables in an over-budget level as one sorted run."""

    kind: Literal["tiered"] = "tiered"


type CompactionConfig = Annotated[LeveledConfig | TieredConfig, Field(discriminator="kind")]


class BloomOffConfig(_StrictModel):
    """Explicitly omit Bloom filters from newly written tables."""

    kind: Literal["off"] = "off"


class BloomConfig(_StrictModel):
    """Build the self-contained persisted Bloom filter."""

    kind: Literal["bloom"] = "bloom"
    false_positive_rate: Annotated[float, Field(ge=1e-6, le=0.5, allow_inf_nan=False)] = 0.01

    def dimensions(self, entry_count: int) -> tuple[int, int]:
        """Derive bounded ``(bit_count, hash_count)`` using the format's one rule.

        Both configuration admission and table construction call this operation,
        so resource validation cannot drift from the actual filter dimensions.
        """
        if type(entry_count) is not int or entry_count < 1:
            message = "Bloom filter entry count must be a positive integer"
            raise ArgumentError(message)
        # Every admitted probability needs more than one bit per entry. This
        # early bound also prevents converting an enormous integer to a float.
        if entry_count > MAX_FILTER_BYTES * 8:
            message = "Bloom filter exceeds the 16 MiB resource limit"
            raise ArgumentError(message)
        raw = ceil(-entry_count * log(self.false_positive_rate) / (_LOG_TWO**2))
        bits = 8 * ((max(_MIN_FILTER_BITS, raw) + 7) // 8)
        if bits // 8 > MAX_FILTER_BYTES:
            message = "Bloom filter exceeds the 16 MiB resource limit"
            raise ArgumentError(message)
        hashes = min(_MAX_HASH_COUNT, max(1, floor((bits / entry_count) * _LOG_TWO + 0.5)))
        return bits, hashes


type FilterConfig = Annotated[BloomOffConfig | BloomConfig, Field(discriminator="kind")]


class ReadCacheOffConfig(_StrictModel):
    """Decode current table bytes without retaining validated images."""

    kind: Literal["off"] = "off"


class ValidatedCacheConfig(_StrictModel):
    """Bound retained exact-image validation results for one database handle."""

    kind: Literal["validated"] = "validated"
    capacity_bytes: _ByteBudget = 1 << 20


type ReadCacheConfig = Annotated[
    ReadCacheOffConfig | ValidatedCacheConfig, Field(discriminator="kind")
]


class StoreConfig(_StrictModel):
    """Complete configuration, separating persistent settings from runtime policy."""

    memtable_bytes: _ByteBudget = 1 << 20
    table_bytes: _ByteBudget = 4 << 20
    block_bytes: _ByteBudget = 16 << 10
    max_record_bytes: Annotated[int, Field(gt=0, le=MAX_RECORD_BYTES)] = MAX_RECORD_BYTES
    compaction: CompactionConfig = Field(default_factory=LeveledConfig)
    bloom: FilterConfig = Field(default_factory=BloomConfig)
    instrumentation: CaptureMode = "counters"
    read_cache: ReadCacheConfig = Field(default_factory=ValidatedCacheConfig)

    @model_validator(mode="after")
    def validate_filter_capacity(self) -> Self:
        """Admit only table targets whose conservative filter bound can fit.

        A record requires at least 17 payload bytes. The ceiling is deliberately
        conservative: unique keys and actual values usually need more space.
        One record may exceed a soft table target, but then its table has one key.
        """
        if isinstance(self.bloom, BloomConfig):
            upper_entries = max(
                1,
                (self.table_bytes + RECORD_HEADER_BYTES - 1) // RECORD_HEADER_BYTES,
            )
            self.bloom.dimensions(upper_entries)
        return self

    def engine_dict(self) -> dict[str, object]:
        """Return validated JSON-compatible settings that affect engine behavior."""
        return self.model_dump(mode="json", exclude={"instrumentation", "read_cache"})


def parse_config(value: object, *, source: str = "configuration") -> StoreConfig:
    """Admit a parsed configuration, preserving field context in failures."""
    try:
        return StoreConfig.model_validate(value)
    except ValidationError as exc:
        message = f"invalid configuration in {source}: {exc}"
        raise ConfigurationError(message) from exc


def load_config(path: Path, overrides: Mapping[str, object] | None = None) -> StoreConfig:
    """Read TOML and apply explicit top-level overrides before one validation.

    A nested override replaces that whole tagged value. This keeps switching a
    variant from accidentally inheriting options owned by another variant.
    """
    try:
        with path.open("rb") as stream:
            values: dict[str, object] = tomllib.load(stream)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        message = f"cannot read configuration {path}: {exc}"
        raise ConfigurationError(message) from exc
    if overrides is not None:
        values.update(overrides)
    return parse_config(values, source=str(path))
