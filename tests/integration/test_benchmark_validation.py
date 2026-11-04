from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from lsm_bench import measurement
from lsm_bench.config import BenchmarkConfig, WorkloadConfig
from lsm_bench.result import LatencyCapture, RunResult, SampledLatencies
from lsm_bench.runner import run
from lsm_bench.workload import Write
from lsm_store import Database
from lsm_store.config import StoreConfig
from lsm_store.errors import CorruptionError

if TYPE_CHECKING:
    from lsm_bench.workload import Operation


def test_correctness_control_rejects_a_bug_only_in_requested_capture_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_get = Database.get

    def incorrect_counted_get(database: Database, key: bytes) -> bytes | None:
        value = original_get(database, key)
        return b"wrong" if database.snapshot().mode == "counters" else value

    monkeypatch.setattr(Database, "get", incorrect_counted_get)
    config = BenchmarkConfig(
        workload=WorkloadConfig(
            operations=4,
            keys=2,
            put_fraction=0.0,
            get_fraction=1.0,
            scan_fraction=0.0,
        ),
    )
    with pytest.raises(CorruptionError, match="point lookup mismatch"):
        run(config)


def test_disabled_capture_artifact_round_trip_does_not_invent_measurements() -> None:
    config = BenchmarkConfig(
        measurement="latency",
        store=StoreConfig(instrumentation="off", memtable_bytes=128, table_bytes=256),
        workload=WorkloadConfig(operations=12, keys=4),
    )
    result = run(config)
    restored = RunResult.model_validate_json(result.model_dump_json())
    assert restored.operations == 12
    assert restored.correctness is True
    assert restored.operations_window.mode == "off"
    assert restored.operations_window.logical.puts == 0
    assert restored.amplification.all_write_bytes.value is None
    assert restored.amplification.foreground_read_bytes.value is None
    assert isinstance(restored.capture, LatencyCapture)
    assert (
        sum(
            len(series.samples_ns)
            for series in (
                restored.capture.puts,
                restored.capture.deletes,
                restored.capture.gets,
                restored.capture.scans,
            )
            if isinstance(series, SampledLatencies)
        )
        == restored.operations
    )


def test_timed_replay_cannot_claim_success_after_omitting_a_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_execute = measurement.execute

    def omit_writes(database: Database, operation: Operation) -> None:
        if not isinstance(operation, Write):
            original_execute(database, operation)

    monkeypatch.setattr(measurement, "execute", omit_writes)
    config = BenchmarkConfig(
        store=StoreConfig(instrumentation="off"),
        workload=WorkloadConfig(
            operations=1,
            keys=2,
            put_fraction=1.0,
            get_fraction=0.0,
            scan_fraction=0.0,
        ),
    )
    with pytest.raises(CorruptionError, match="persisted final map mismatch"):
        run(config)
