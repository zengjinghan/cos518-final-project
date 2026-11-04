from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from lsm_bench import measurement
from lsm_bench.config import BenchmarkConfig, WorkloadConfig
from lsm_bench.result import (
    LatencyCapture,
    NoSamples,
    RunResult,
    SampledLatencies,
    ThroughputCapture,
)
from lsm_bench.runner import run
from lsm_bench.workload import Read, build_trace
from lsm_store import Database, StoreConfig
from lsm_store.errors import CorruptionError
from lsm_store.metrics import IoPurpose

if TYPE_CHECKING:
    from pathlib import Path

    from lsm_bench.config import InitialState, MeasurementMode
    from lsm_bench.workload import Operation


def test_throughput_has_one_clock_pair_without_samples(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []

    def wall() -> int:
        calls.append("wall")
        return len(calls) * 100

    def cpu() -> int:
        calls.append("cpu")
        return len(calls) * 10

    monkeypatch.setattr(measurement, "perf_counter_ns", wall)
    monkeypatch.setattr(measurement, "process_time_ns", cpu)
    with Database.open(tmp_path / "store") as database:
        result = measurement.measure(database, tuple(Read(b"key") for _ in range(20)), "throughput")
    assert calls == ["cpu", "wall", "wall", "cpu"]
    assert isinstance(result.capture, ThroughputCapture)
    assert result.elapsed_ns == 100


@pytest.mark.parametrize("state", ["resident", "table"])
@pytest.mark.parametrize("mode", ["throughput", "latency"])
def test_loaded_hits_exclude_load_warmup_and_inspection(
    tmp_path: Path, state: InitialState, mode: MeasurementMode
) -> None:
    config = BenchmarkConfig(
        initial_state=state,
        measurement=mode,
        warmup_operations=7,
        workload=WorkloadConfig(
            operations=20,
            keys=4,
            key_bytes=2,
            value_bytes=4,
            put_fraction=0.0,
            get_fraction=1.0,
            scan_fraction=0.0,
        ),
    )
    result = run(config, parent=tmp_path)
    assert result.expected_logical.hits == result.operations_window.logical.hits == 20
    assert result.operations_window.logical.misses == 0
    assert result.preparation.counters.logical.puts == 4
    assert result.warmup.counters.logical.gets == 7
    assert result.inspection.live_keys == 4
    point_reads = result.operations_window.io[IoPurpose.POINT_READ].read_calls
    assert (point_reads == 0) == (state == "resident")
    assert result.inspection.memtable_bytes == 0  # Finalization has flushed both states.
    assert result.format_version == 2
    assert result.read_cache.retained_bytes <= result.read_cache.capacity_bytes
    assert RunResult.model_validate_json(result.model_dump_json()) == result
    with pytest.raises(ValueError, match="format_version"):
        RunResult.model_validate_json(
            result.model_dump_json().replace('"format_version":2', '"format_version":1', 1)
        )
    if isinstance(result.capture, LatencyCapture):
        assert isinstance(result.capture.gets, SampledLatencies)
        assert len(result.capture.gets.samples_ns) == 20
        assert isinstance(result.capture.puts, NoSamples)
    else:
        assert isinstance(result.capture, ThroughputCapture)
    assert not list(tmp_path.iterdir())


def test_interleaved_missing_queries_and_bounded_scans() -> None:
    config = BenchmarkConfig(
        initial_state="table",
        workload=WorkloadConfig(
            operations=12,
            keys=4,
            key_bytes=1,
            value_bytes=1,
            key_stride=2,
            key_offset=1,
            put_fraction=0.0,
            get_fraction=1.0,
            scan_fraction=0.0,
        ),
    )
    trace = build_trace(config)
    assert {int.from_bytes(item.key) for item in trace.load} == {0, 2, 4, 6}
    assert all(isinstance(item, Read) and int.from_bytes(item.key) % 2 for item in trace.measured)
    result = run(config)
    assert result.expected_logical.misses == result.operations_window.logical.misses == 12
    assert result.inspection.live_keys == 4
    scan = run(
        BenchmarkConfig(
            initial_state="table",
            workload=WorkloadConfig(
                operations=12,
                keys=4,
                key_bytes=1,
                value_bytes=1,
                scan_length=2,
                put_fraction=0.0,
                get_fraction=0.0,
                scan_fraction=1.0,
            ),
        )
    )
    assert 12 <= scan.expected_logical.scan_rows <= 24


def test_measurement_modes_replay_the_same_trace() -> None:
    workload = WorkloadConfig(operations=10, keys=4)
    throughput = run(
        BenchmarkConfig(workload=workload, warmup_operations=3, measurement="throughput")
    )
    latency = run(BenchmarkConfig(workload=workload, warmup_operations=3, measurement="latency"))
    assert throughput.trace_sha256 == latency.trace_sha256
    assert throughput.expected_logical == latency.expected_logical
    assert throughput.operations_window.logical == latency.operations_window.logical
    assert throughput.inspection == latency.inspection


def test_timed_dropped_reads_fail_exact_logical_control(monkeypatch: pytest.MonkeyPatch) -> None:
    def drop(_database: Database, _operation: Operation) -> None:
        pass

    monkeypatch.setattr(measurement, "execute", drop)
    config = BenchmarkConfig(
        initial_state="table",
        workload=WorkloadConfig(
            operations=2, keys=2, put_fraction=0.0, get_fraction=1.0, scan_fraction=0.0
        ),
    )
    with pytest.raises(CorruptionError, match="logical counters mismatch"):
        run(config)


@pytest.mark.parametrize(
    "settings",
    [
        {"initial_state": "resident"},
        {
            "initial_state": "resident",
            "store": StoreConfig(memtable_bytes=1),
            "workload": WorkloadConfig(
                keys=2, put_fraction=0.0, get_fraction=1.0, scan_fraction=0.0
            ),
        },
    ],
)
def test_invalid_resident_contract_fails_before_execution(settings: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="resident"):
        BenchmarkConfig.model_validate(settings)


def test_stride_capacity_is_checked() -> None:
    with pytest.raises(ValueError, match="keyspace"):
        WorkloadConfig(keys=129, key_bytes=1, key_stride=2)
