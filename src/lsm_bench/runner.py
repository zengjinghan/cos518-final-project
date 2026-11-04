"""Coordinate validated benchmark phases without owning their rules."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter_ns
from typing import TYPE_CHECKING

from lsm_store import Database
from lsm_store.errors import CorruptionError

from .environment import DEFAULT_SCRATCH, observe
from .measurement import execute, measure
from .preparation import prepare
from .result import Amplifications, Phase, Preparation, RunResult
from .source import source_identity
from .validation import validate, verify_persisted
from .workload import build_trace

if TYPE_CHECKING:
    from cProfile import Profile

    from .config import BenchmarkConfig
    from .environment import Environment
    from .validation import ValidatedTrace


def run(
    config: BenchmarkConfig, *, parent: Path = DEFAULT_SCRATCH, profiler: Profile | None = None
) -> RunResult:
    """Create only private stores on the explicitly observed scratch filesystem."""
    environment = observe(parent)
    trace = build_trace(config)
    with TemporaryDirectory(prefix="lsm-bench-", dir=parent) as directory:
        root = Path(directory)
        validated = validate(root / "reference", config, trace)
        return _measure(root / "measured", config, validated, environment, profiler)


def _measure(
    path: Path,
    config: BenchmarkConfig,
    validated: ValidatedTrace,
    environment: Environment,
    profiler: Profile | None,
) -> RunResult:
    trace, oracle = validated.trace, validated.oracle
    preparation = prepare(path, config, trace.load)
    open_started = perf_counter_ns()
    with Database.open(path, config.store, create=False) as database:
        opening = Preparation(perf_counter_ns() - open_started, database.snapshot())
        before_warmup = database.snapshot()
        started = perf_counter_ns()
        for operation in trace.warmup:
            execute(database, operation)
        warmup_ns = perf_counter_ns() - started
        before = database.snapshot()
        measured = (
            measure(database, trace.measured, config.measurement)
            if profiler is None
            else profiler.runcall(measure, database, trace.measured, config.measurement)
        )
        after = database.snapshot()
        window = after.since(before)
        read_cache = database.read_cache_info()
        if window.mode == "counters" and window.logical != oracle.snapshot():
            raise CorruptionError("benchmark measured logical counters mismatch")
        started = perf_counter_ns()
        database.flush()
        flush_ns = perf_counter_ns() - started
        flushed = database.snapshot()
        started = perf_counter_ns()
        inspection = database.inspect()
        inspection_ns = perf_counter_ns() - started
        inspected = database.snapshot()
    started = perf_counter_ns()
    verify_persisted(path, config, oracle.values)
    post_ns = perf_counter_ns() - started
    return RunResult(
        config=config,
        source=source_identity(),
        environment=environment,
        trace_sha256=trace.sha256,
        timing="unprofiled" if profiler is None else "cprofile",
        operations=len(trace.measured),
        expected_logical=oracle.snapshot(),
        reference_validation_ns=validated.elapsed_ns,
        preparation=preparation,
        opening=opening,
        warmup=Phase(warmup_ns, before.since(before_warmup)),
        elapsed_ns=measured.elapsed_ns,
        cpu_ns=measured.cpu_ns,
        throughput_ops_per_second=len(trace.measured) * 1e9 / measured.elapsed_ns,
        throughput_with_final_flush_ops_per_second=len(trace.measured)
        * 1e9
        / (measured.elapsed_ns + flush_ns),
        capture=measured.capture,
        read_cache=read_cache,
        operations_window=window,
        final_flush=Phase(flush_ns, flushed.since(after)),
        inspection_phase=Phase(inspection_ns, inspected.since(flushed)),
        post_validation_ns=post_ns,
        amplification=Amplifications.from_window(window),
        inspection=inspection,
    )
