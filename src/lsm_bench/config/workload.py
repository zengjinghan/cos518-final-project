"""Deterministic finite keyspaces and operation distributions."""

from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from .model import Model


class Uniform(Model):
    """Uniform probability across the configured keyspace."""

    kind: Literal["uniform"] = "uniform"


class HotSet(Model):
    """Send a configured probability mass to the lowest numbered hot keys."""

    kind: Literal["hot-set"] = "hot-set"
    hot_fraction: Annotated[float, Field(gt=0, lt=1, allow_inf_nan=False)] = 0.1
    hot_probability: Annotated[float, Field(ge=0, le=1, allow_inf_nan=False)] = 0.9


class Zipf(Model):
    """A bounded rank distribution, not modulo reduction of unbounded samples."""

    kind: Literal["zipf"] = "zipf"
    exponent: Annotated[float, Field(gt=0, le=4, allow_inf_nan=False)] = 1.2


type Distribution = Annotated[Uniform | HotSet | Zipf, Field(discriminator="kind")]
type _Fraction = Annotated[float, Field(ge=0, le=1, allow_inf_nan=False)]


class WorkloadConfig(Model):
    """A finite deterministic mutation/read trace with explicit encoding capacity."""

    key_offset: Annotated[int, Field(ge=0, le=(1 << 64) - 1)] = 0
    key_stride: Annotated[int, Field(gt=0, le=65536)] = 1
    scan_length: Annotated[int, Field(gt=0, le=1_000_000)] = 16
    operations: Annotated[int, Field(gt=0, le=1_000_000)] = 2000
    keys: Annotated[int, Field(gt=0, le=1_000_000)] = 1000
    key_bytes: Annotated[int, Field(gt=0, le=64)] = 8
    value_bytes: Annotated[int, Field(ge=0, le=1 << 20)] = 64
    put_fraction: _Fraction = 0.6
    get_fraction: _Fraction = 0.3
    scan_fraction: _Fraction = 0.05
    seed: Annotated[int, Field(ge=0, le=(1 << 64) - 1)] = 42
    distribution: Distribution = Field(default_factory=Uniform)

    @model_validator(mode="after")
    def validate_trace(self) -> Self:
        """Reject impossible operation mixes or lossy integer key encodings."""
        if self.put_fraction + self.get_fraction + self.scan_fraction > 1:
            raise ValueError("put/get/scan fractions must sum to at most one")
        if self.key_offset + (self.keys - 1) * self.key_stride >= 1 << (8 * self.key_bytes):
            raise ValueError("key_bytes cannot encode the configured keyspace")
        if isinstance(self.distribution, HotSet):
            hot = int(self.keys * self.distribution.hot_fraction)
            if not 0 < hot < self.keys:
                raise ValueError("hot_fraction must select a nonempty proper hot set")
        if self.operations * (self.key_bytes + self.value_bytes) > 128 << 20:
            raise ValueError("materialized trace exceeds the 128-MiB payload budget")
        return self
