"""Configuration schema for the benchmarking framework.

This module defines all user-facing Pydantic models that describe:
- how to configure the LSM engine (LSMConfig),
- how to configure disks and compaction strategies (DiskConfig, CompactionConfig),
- how workloads are parameterized (WorkloadConfig),
- how individual experiments are represented (ExperimentConfig),
- how a base benchmark configuration is specified (BenchmarkBaseConfig),
- how a sweep / grid is specified (GridConfig and its per-entity grids).

These models are pure data specifications: no engine logic, no factory logic,
no execution logic. The benchmark runner:

- parses a BenchmarkBaseConfig (from base YAML),
- parses a GridConfig (from sweep YAML),
- expands them into concrete ExperimentConfig instances,
- instantiates Disk / Compactor via factories,
- and executes experiments accordingly.

By centralizing all configuration definitions here, the rest of the system
remains clean, declarative, and easy to extend.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ..compaction import CompactionType
from ..disk import DiskType
from ..lsm import LSMConfig
from .workloads import WorkloadType


class WorkloadConfig(BaseModel):
    """Configuration of the logical workload applied to the engine.

    This is both:
    - the base workload for the benchmark, and
    - the unit for workload variants in the grid (you can override fields).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    type: WorkloadType
    """Type of workload (key distribution and op mix)."""

    n_ops: int = Field(..., gt=0)
    """Total number of operations in the workload."""

    key_range: int = Field(1_000_000, gt=0)
    """Upper bound on the keyspace; keys are drawn from [0, key_range)."""

    value_range: int = Field(1_000_000, gt=0)
    """Upper bound on the values; values are drawn from [1, value_range]."""

    key_size_bytes: int = Field(8, gt=0)
    """Size of encoded keys in bytes."""

    value_size_bytes: int = Field(8, gt=0)
    """Size of encoded values in bytes."""

    put_fraction: float = Field(0.75, ge=0.0, le=1.0)
    """Probability of a PUT operation (0.0 to 1.0)."""

    get_fraction: float = Field(0.10, ge=0.0, le=1.0)
    """Probability of a GET operation (0.0 to 1.0)."""

    get_existing_keys_only: bool = False
    """
    If True, GET operations attempt to sample uniformly from currently 
    'live' keys (avoiding 404s). If False, GET keys are drawn from the 
    standard distribution for that workload type.
    """

    seed: int = 0
    """Base random seed used to generate the workload."""

    params: dict[str, Any] = Field(default_factory=dict)
    """
    Workload-specific parameters, interpreted by the workload generator.

    Examples:
        - Zipf:   {"theta": 1.2}
        - Skewed: {"hot_ratio": 0.01, "hot_access_prob": 0.8}
    """

    @property
    def delete_fraction(self) -> float:
        """Probability of a DELETE operation.

        Computed as the remainder of the probability mass:
        1.0 - (put_fraction + get_fraction).
        """
        return 1.0 - (self.put_fraction + self.get_fraction)

    @property
    def put_limit(self) -> float:
        """Inclusive upper bound RNG threshold for PUT operations.

        If `rng.random() < put_limit`, the operation is a PUT.
        """
        return self.put_fraction

    @property
    def get_limit(self) -> float:
        """Inclusive upper bound RNG threshold for GET operations.

        If `put_limit <= rng.random() < get_limit`, the operation is a GET.
        """
        return self.put_fraction + self.get_fraction

    @property
    def delete_limit(self) -> float:
        """Inclusive upper bound RNG threshold for DELETE operations.

        If `get_limit <= rng.random() < delete_limit`, the operation is a DELETE.
        Always returns 1.0.
        """
        return 1.0

    @model_validator(mode="after")
    def check_fractions(self) -> WorkloadConfig:
        """Ensure that operation probabilities do not exceed 100%."""
        total = self.put_fraction + self.get_fraction
        if total > 1.0:
            raise ValueError(
                f"Sum of put_fraction ({self.put_fraction}) and get_fraction ({self.get_fraction}) cannot exceed 1.0"
            )
        return self


class DiskConfig(BaseModel):
    """Configuration for constructing a Disk implementation.

    This is the only place where the disk type and its arguments live.

    The benchmark runner will turn this into a concrete Disk via a factory, e.g.:

        disk = create_disk(disk_cfg.type, disk_cfg.args)
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    type: DiskType = Field(..., description="Which Disk implementation to use.")
    args: dict[str, Any] = Field(
        default_factory=dict,
        description="Implementation-specific constructor arguments.",
    )


class CompactionConfig(BaseModel):
    """Configuration for constructing a compaction strategy.

    This is the only place where the compaction type and its arguments live.

    The benchmark runner will turn this into a concrete strategy via a factory, e.g.:

        compactor = create_compactor(compaction_cfg.type, compaction_cfg.args)
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    type: CompactionType = Field(
        ...,
        description="High-level compaction strategy (tiered, leveled, fifo, hybrid, ...).",
    )
    args: dict[str, Any] = Field(
        default_factory=dict,
        description="Implementation-specific arguments for the strategy.",
    )


class ExperimentConfig(BaseModel):
    """Fully-specified configuration for a single experiment.

    This is what `run_single_experiment` should accept:
        - lsm: pure LSMConfig (no disk / compaction baked in)
        - disk: DiskConfig (how to build the Disk)
        - compaction: CompactionConfig (how to build the CompactionStrategy)
        - workload: WorkloadConfig (what to run)
        - repetitions: how many times to repeat with different seeds

    Note: ExperimentConfig is *derived* from:
        - a BenchmarkBaseConfig (base per-entity configs), and
        - a GridConfig (sweep specification).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str = Field(..., min_length=1)
    """Human-readable label for this experiment configuration."""

    repetitions: int = Field(1, gt=0)
    """How many times to repeat this experiment with different seeds."""

    lsm: LSMConfig
    """Configuration of the LSM engine instance (pure engine geometry)."""

    disk: DiskConfig
    """Disk configuration (type + args) to use for this experiment."""

    compaction: CompactionConfig
    """Compaction strategy configuration for this experiment."""

    workload: WorkloadConfig
    """Configuration of the workload applied to the engine."""


class GridElement(BaseModel):
    """Sweep specification for any configurable entity.

    Each element may define:
    - params: a dict mapping field names -> lists of values.
        These generate a Cartesian product of combinations.
    - variants: a list of dict patches, each describing a full or partial
        override of the base configuration. Each variant is applied
        independently on top of the base.

    Examples:
        params:
            write_buffer_size: [256000, 512000]
            enable_bloom_filter: [true, false]

        variants:
            - { max_l0_files: 8 }
            - { max_bytes_for_level_multiplier: 10 }
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    params: dict[str, Sequence[Any]] = Field(default_factory=dict)
    variants: list[dict[str, Any]] = Field(default_factory=list)


class GridConfig(BaseModel):
    """Grid / sweep configuration layered on top of a BenchmarkBaseConfig.

    Typically loaded from a file like `lsm_sweep.yaml`.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    lsm: GridElement = Field(default_factory=GridElement)
    disk: GridElement = Field(default_factory=GridElement)
    compaction: GridElement = Field(default_factory=GridElement)
    workload: GridElement = Field(default_factory=GridElement)
