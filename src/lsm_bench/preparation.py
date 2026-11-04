"""Prepare and reopen the requested state outside measured operations."""

from pathlib import Path
from time import perf_counter_ns

from lsm_store import Database

from .config import BenchmarkConfig
from .result import Preparation
from .workload import Write


def prepare(path: Path, config: BenchmarkConfig, records: tuple[Write, ...]) -> Preparation:
    """Load through the public API without adding an alternative storage path."""
    started = perf_counter_ns()
    with Database.open(path, config.store) as database:
        for record in records:
            database.put(record.key, record.value)
        if config.initial_state == "table":
            database.flush()
        counters = database.snapshot()
    return Preparation(perf_counter_ns() - started, counters)
