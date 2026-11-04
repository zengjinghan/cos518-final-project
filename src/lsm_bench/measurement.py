"""Operation timing has two explicit paths and one shared dispatcher."""

from dataclasses import dataclass
from time import perf_counter_ns, process_time_ns

from lsm_store import Database

from .config import MeasurementMode
from .result import (
    Capture,
    LatencyCapture,
    LatencySeries,
    NoSamples,
    SampledLatencies,
    ThroughputCapture,
)
from .statistics import summarize
from .workload import Operation, Read, Remove, Scan, Write


@dataclass(frozen=True, slots=True)
class Measurement:
    """Clock observations, with capture state attached explicitly."""

    elapsed_ns: int
    cpu_ns: int
    capture: Capture


def execute(database: Database, operation: Operation) -> None:
    """Dispatch without oracle work or benchmark accounting in engine code."""
    match operation:
        case Write(key, value):
            database.put(key, value)
        case Remove(key):
            database.delete(key)
        case Read(key):
            database.get(key)
        case Scan(start, end):
            database.scan(start, end)


def _series(samples: list[int]) -> LatencySeries:
    distribution = summarize(samples)
    return (
        NoSamples()
        if distribution is None
        else SampledLatencies(samples_ns=tuple(samples), distribution=distribution)
    )


def _throughput(database: Database, operations: tuple[Operation, ...]) -> Measurement:
    cpu_start = process_time_ns()
    started = perf_counter_ns()
    for operation in operations:
        execute(database, operation)
    elapsed = perf_counter_ns() - started
    cpu = process_time_ns() - cpu_start
    return Measurement(elapsed, cpu, ThroughputCapture())


def _latency(database: Database, operations: tuple[Operation, ...]) -> Measurement:
    puts: list[int] = []
    deletes: list[int] = []
    gets: list[int] = []
    scans: list[int] = []
    cpu_start = process_time_ns()
    started = perf_counter_ns()
    for operation in operations:
        start = perf_counter_ns()
        execute(database, operation)
        elapsed = perf_counter_ns() - start
        match operation:
            case Write():
                puts.append(elapsed)
            case Remove():
                deletes.append(elapsed)
            case Read():
                gets.append(elapsed)
            case Scan():
                scans.append(elapsed)
    elapsed = perf_counter_ns() - started
    cpu = process_time_ns() - cpu_start
    capture = LatencyCapture(
        puts=_series(puts), deletes=_series(deletes), gets=_series(gets), scans=_series(scans)
    )
    return Measurement(elapsed, cpu, capture)


def measure(
    database: Database, operations: tuple[Operation, ...], mode: MeasurementMode
) -> Measurement:
    """Choose a mode before entering its operation loop."""
    result = (
        _throughput(database, operations)
        if mode == "throughput"
        else _latency(database, operations)
    )
    if result.elapsed_ns <= 0:
        raise ValueError("operation clock did not advance")
    return result
