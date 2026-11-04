"""Independent map semantics and exact logical expectations outside timing."""

from dataclasses import dataclass, field
from pathlib import Path
from time import perf_counter_ns

from lsm_store import Database
from lsm_store.errors import CorruptionError
from lsm_store.metrics.snapshot import LogicalStats

from .config import BenchmarkConfig
from .preparation import prepare
from .workload import Operation, Read, Remove, Scan, Trace, Write


@dataclass(slots=True)
class Oracle:
    """Own expected state and derive counts without engine accounting helpers."""

    values: dict[bytes, bytes] = field(default_factory=dict[bytes, bytes])
    puts: int = 0
    deletes: int = 0
    gets: int = 0
    scans: int = 0
    hits: int = 0
    misses: int = 0
    scan_rows: int = 0
    mutation_bytes: int = 0
    returned_bytes: int = 0

    def apply(self, database: Database, operation: Operation) -> None:
        """Check actual responses while advancing the independent finite map."""
        match operation:
            case Write(key, value):
                database.put(key, value)
                self.values[key] = value
                self.puts += 1
                self.mutation_bytes += len(key) + len(value)
            case Remove(key):
                database.delete(key)
                self.values.pop(key, None)
                self.deletes += 1
                self.mutation_bytes += len(key)
            case Read(key):
                expected = self.values.get(key)
                if database.get(key) != expected:
                    raise CorruptionError("benchmark reference: point lookup mismatch")
                self.gets += 1
                if expected is None:
                    self.misses += 1
                else:
                    self.hits += 1
                    self.returned_bytes += len(key) + len(expected)
            case Scan(start, end):
                expected_rows = sorted(
                    (key, value) for key, value in self.values.items() if start <= key <= end
                )
                if database.scan(start, end) != expected_rows:
                    raise CorruptionError("benchmark reference: scan mismatch")
                self.scans += 1
                self.scan_rows += len(expected_rows)
                self.returned_bytes += sum(len(key) + len(value) for key, value in expected_rows)

    def snapshot(self) -> LogicalStats:
        """Expose named immutable expectations for one measured trace."""
        return LogicalStats(
            self.puts,
            self.deletes,
            self.gets,
            self.scans,
            self.hits,
            self.misses,
            self.scan_rows,
            self.mutation_bytes,
            self.returned_bytes,
        )


def verify_persisted(path: Path, config: BenchmarkConfig, expected: dict[bytes, bytes]) -> None:
    """Check the entire reopened map without charging it to throughput."""
    with Database.open(path, capture=config.store.instrumentation, create=False) as database:
        maximum = b"\xff" * config.workload.key_bytes
        if database.scan(b"", maximum) != sorted(expected.items()):
            raise CorruptionError("benchmark persisted final map mismatch")


@dataclass(frozen=True, slots=True)
class ValidatedTrace:
    """A reference-checked trace and its independent expectations."""

    trace: Trace
    oracle: Oracle
    elapsed_ns: int


def validate(path: Path, config: BenchmarkConfig, trace: Trace) -> ValidatedTrace:
    """Use the same load/warmup/capture, then check every reference operation."""
    started = perf_counter_ns()
    prepare(path, config, trace.load)
    oracle = Oracle({record.key: record.value for record in trace.load})
    with Database.open(path, config.store, create=False) as database:
        for operation in trace.warmup:
            oracle.apply(database, operation)
        oracle = Oracle(oracle.values)
        for operation in trace.measured:
            oracle.apply(database, operation)
        database.flush()
    verify_persisted(path, config, oracle.values)
    return ValidatedTrace(trace, oracle, perf_counter_ns() - started)
