"""Experiment grid expansion.

This module expands:

- a BenchmarkBaseConfig (one base config per entity), and
- a GridConfig (sweeps / variants per entity)

into a flat sequence of ExperimentConfig objects.

Conceptually:

    experiments ~= cartesian_product(
        LSM configs,
        Disk configs,
        Compaction configs,
        Workload configs,
    )
"""

from __future__ import annotations

import itertools
from collections.abc import Iterable, Sequence
from typing import Any, TypeVar

from pydantic import BaseModel

from .config_schema import (
    CompactionConfig,
    DiskConfig,
    ExperimentConfig,
    GridConfig,
    LSMConfig,
    WorkloadConfig,
)

__all__ = ("generate_experiment_configs",)

ConfigT = TypeVar("ConfigT", bound=BaseModel)


def _expand_configs(
    base: ConfigT,
    params: dict[str, Sequence[Any]],
    variants: list[dict[str, Any]],
    cls: type[ConfigT],
) -> list[ConfigT]:
    """Expand a base config into a list of configs.

    Includes:
    - all combinations from `params` (Cartesian product),
    - any explicit `variants`.

    `cls` is the Pydantic model class (e.g. LSMConfig, DiskConfig).
    """
    configs: list[ConfigT] = []
    base_kwargs = base.model_dump()

    # 1) Expand param-based grid (Cartesian product).
    if params:
        items = list(params.items())
        param_names = [name for name, _ in items]
        value_lists = [values for _, values in items]

        for combo in itertools.product(*value_lists):
            overrides = dict(zip(param_names, combo, strict=True))
            merged = base_kwargs | overrides  # overrides takes priority
            configs.append(cls.model_validate(merged))

    # 2) Expand variant-based grid (base + patch-per-variant).
    if variants:
        for variant in variants:
            merged = base_kwargs | variant  # variant takes priority
            configs.append(cls.model_validate(merged))

    if not configs:
        return [base]

    return configs


def generate_experiment_configs(
    base_cfg: ExperimentConfig,
    grid_cfg: GridConfig,
) -> Iterable[ExperimentConfig]:
    """Yield ExperimentConfig objects for the full benchmark grid.

    Axes:
        LSM x Disk x Compaction x Workload

        where each axis is expanded from base + param sweeps + explicit variants.
    """
    lsm_configs = _expand_configs(
        base=base_cfg.lsm,
        params=grid_cfg.lsm.params,
        variants=grid_cfg.lsm.variants,
        cls=LSMConfig,
    )

    disk_configs = _expand_configs(
        base=base_cfg.disk,
        params=grid_cfg.disk.params,
        variants=grid_cfg.disk.variants,
        cls=DiskConfig,
    )

    compaction_configs = _expand_configs(
        base=base_cfg.compaction,
        params=grid_cfg.compaction.params,
        variants=grid_cfg.compaction.variants,
        cls=CompactionConfig,
    )

    workload_configs = _expand_configs(
        base=base_cfg.workload,
        params=grid_cfg.workload.params,
        variants=grid_cfg.workload.variants,
        cls=WorkloadConfig,
    )

    exp_id = 0

    for lsm, disk, compaction, workload in itertools.product(
        lsm_configs,
        disk_configs,
        compaction_configs,
        workload_configs,
    ):
        yield ExperimentConfig(
            name=f"{base_cfg.name}_{exp_id}",
            repetitions=base_cfg.repetitions,
            lsm=lsm,
            disk=disk,
            compaction=compaction,
            workload=workload,
        )
        exp_id += 1
