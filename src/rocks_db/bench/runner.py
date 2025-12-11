"""Execution engine for running a single fully-specified ExperimentConfig.

This module contains no grid logic and no configuration parsing.
Its responsibilities are:

    - Construct Disk and CompactionStrategy objects from config.
    - Instantiate an LSMDatabase with injected dependencies.
    - Generate and execute the workload.
    - Collect per-run metrics (latencies, counters, amplification).
    - Return results as a list[RunResult] (one per repetition).

All higher-level orchestration is handled in grid.py and run.py.
"""

from __future__ import annotations

import time
import traceback
from collections.abc import Iterable
from typing import TYPE_CHECKING

import numpy as np

from ..compaction import create_compactor
from ..disk import create_disk
from ..lsm import LSMDatabase
from ..metrics import get_metrics_snapshot
from .benchmark_results import RunResult
from .workloads import OpCode, generate_workload

if TYPE_CHECKING:
    from .config_schema import ExperimentConfig
    from .workloads import WorkloadOp

__all__ = ("run_single_experiment",)


def run_workload(
    lsm: LSMDatabase,
    ops: Iterable[WorkloadOp],
) -> tuple[list[float], list[float], str | None]:
    """Returns read_latencies, write_latencies and any error message."""
    read_latencies: list[float] = []
    write_latencies: list[float] = []
    error_message: str | None = None

    # Execute workload
    try:
        for op, key, value in ops:
            t0 = time.perf_counter_ns()

            if op is OpCode.PUT:
                assert value is not None
                lsm.put(key, value)
                is_write = True
            elif op is OpCode.DELETE:
                lsm.delete(key)
                is_write = True
            else:  # op is OpCode.GET
                _ = lsm.get(key)
                is_write = False

            dt = time.perf_counter_ns() - t0
            if is_write:
                write_latencies.append(dt)
            else:
                read_latencies.append(dt)
    except Exception:
        error_message = traceback.format_exc()
        print(error_message)

    return read_latencies, write_latencies, error_message


def run_single_experiment(cfg: ExperimentConfig) -> list[RunResult]:
    """Execute one ExperimentConfig for `cfg.repetitions` runs.

    Notes:
    - Disk and Compactor objects are created fresh for each repetition.
    - LSMDatabase receives both as constructor-injected dependencies (DI).
    - Every repetition uses seed = base_seed + rep.
    - Latency distributions are captured, including p50 / p90 / p95 / p99.

    Returns:
        list[RunResult] (one per repetition).
    """
    results: list[RunResult] = []

    for rep in range(cfg.repetitions):
        seed = cfg.workload.seed + rep

        # Construct system components
        disk = create_disk(cfg.disk.type, cfg.disk.args)
        compactor = create_compactor(cfg.compaction.type, cfg.compaction.args)

        lsm = LSMDatabase(
            config=cfg.lsm,
            disk=disk,
            compactor=compactor,
        )

        # Prepare workload
        workload = cfg.workload.model_copy(update={"seed": seed})
        ops = generate_workload(workload)

        start_timestamp = time.time()
        elapsed_ns = time.perf_counter_ns()

        read_latencies, write_latencies, error_message = run_workload(lsm, ops)

        elapsed_ns = time.perf_counter_ns() - elapsed_ns
        end_timestamp = time.time()

        # Metrics collection
        metrics_snapshot = get_metrics_snapshot(lsm)

        elapsed_s = elapsed_ns * 1e-9
        throughput = lsm.metrics.total_ops / elapsed_s if elapsed_s > 0 else 0.0

        # Handle degenerate cases gracefully
        _read_latencies = np.array(read_latencies, dtype=float)
        _write_latencies = np.array(write_latencies, dtype=float)

        def safe_percentile(arr: np.ndarray, p: int) -> float:
            return float(np.percentile(arr, p)) if arr.size > 0 else 0.0

        def safe_min(arr):
            return float(np.min(arr)) if arr.size > 0 else 0.0

        def safe_max(arr):
            return float(np.max(arr)) if arr.size > 0 else 0.0

        def safe_mean(arr):
            return float(np.mean(arr)) if arr.size > 0 else 0.0

        results.append(
            RunResult(
                experiment_name=cfg.name,
                repetition_id=rep,
                # Status
                is_success=error_message is None,
                error=error_message,
                # Config
                config=cfg,
                # Timing
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                elapsed_ns=elapsed_ns,
                # Throughput
                throughput_ops_per_sec=throughput,
                # Read latencies
                min_read_latency_ns=safe_min(_read_latencies),
                mean_read_latency_ns=safe_mean(_read_latencies),
                max_read_latency_ns=safe_max(_read_latencies),
                p50_read_latency_ns=safe_percentile(_read_latencies, 50),
                p90_read_latency_ns=safe_percentile(_read_latencies, 90),
                p95_read_latency_ns=safe_percentile(_read_latencies, 95),
                p99_read_latency_ns=safe_percentile(_read_latencies, 99),
                # Write latencies
                min_write_latency_ns=safe_min(_write_latencies),
                mean_write_latency_ns=safe_mean(_write_latencies),
                max_write_latency_ns=safe_max(_write_latencies),
                p50_write_latency_ns=safe_percentile(_write_latencies, 50),
                p90_write_latency_ns=safe_percentile(_write_latencies, 90),
                p95_write_latency_ns=safe_percentile(_write_latencies, 95),
                p99_write_latency_ns=safe_percentile(_write_latencies, 99),
                # Others
                **metrics_snapshot.to_dict(),
            )
        )

    return results
