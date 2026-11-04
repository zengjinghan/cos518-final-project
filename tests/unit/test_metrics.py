from __future__ import annotations

from dataclasses import FrozenInstanceError, replace

import pytest
from pydantic import TypeAdapter

from lsm_store.errors import ArgumentError
from lsm_store.metrics import (
    IoPurpose,
    LogicalStats,
    Metrics,
    MetricsSnapshot,
    MetricsWindow,
    Ratio,
)


def test_windows_preserve_counts_and_keep_scans_out_of_point_ratio() -> None:
    metrics = Metrics()
    # Recovery/setup is intentionally outside the selected window.
    metrics.io[IoPurpose.RECOVERY].read_bytes = 1000
    before = metrics.snapshot()
    metrics.logical.puts = 1
    metrics.logical.hits = 1
    metrics.logical.misses = 1
    metrics.logical.scans = 1
    metrics.logical.scan_rows = 2
    metrics.logical.mutation_bytes = 10
    metrics.logical.returned_bytes = 25
    metrics.io[IoPurpose.JOURNAL].write_bytes = 20
    metrics.io[IoPurpose.FLUSH].write_bytes = 30
    metrics.io[IoPurpose.COMPACTION].write_bytes = 40
    metrics.io[IoPurpose.METADATA].write_bytes = 10
    metrics.io[IoPurpose.POINT_READ].read_bytes = 50
    metrics.io[IoPurpose.POINT_READ].read_calls = 2
    metrics.io[IoPurpose.SCAN].read_bytes = 50
    metrics.io[IoPurpose.SCAN].read_calls = 100
    metrics.io[IoPurpose.INSPECTION].read_bytes = 9999
    metrics.maintenance.compactions = 1
    metrics.read_cache.block_reuses = 3
    window = metrics.snapshot().since(before)
    assert window.mode == "counters"
    assert window.write_amplification == Ratio(100, 10)
    assert window.table_write_amplification == Ratio(70, 10)
    assert window.read_amplification == Ratio(100, 25)
    assert window.point_read_calls_per_get == Ratio(2, 2)
    assert window.io[IoPurpose.RECOVERY].read_bytes == 0
    assert window.io[IoPurpose.INSPECTION].read_bytes == 9999
    assert window.logical.scan_rows == 2
    assert window.logical.gets == window.logical.hits + window.logical.misses == 2
    assert window.maintenance.compactions == 1
    assert window.read_cache.block_reuses == 3


def test_snapshot_is_detached_and_immutable() -> None:
    metrics = Metrics()
    snapshot = metrics.snapshot()
    metrics.logical.puts += 1
    metrics.io[IoPurpose.JOURNAL].write_bytes += 7
    assert snapshot.logical.puts == 0
    assert snapshot.io[IoPurpose.JOURNAL].write_bytes == 0
    field_name = "puts"
    with pytest.raises(FrozenInstanceError):
        setattr(snapshot.logical, field_name, 42)


def test_metric_windows_reject_foreign_origin_and_reverse_order() -> None:
    metrics = Metrics()
    before = metrics.snapshot()
    metrics.logical.hits = 1
    after = metrics.snapshot()
    with pytest.raises(ArgumentError, match="same handle"):
        after.since(Metrics().snapshot())
    with pytest.raises(ArgumentError, match="larger counter"):
        before.since(after)
    with pytest.raises(ArgumentError, match="capture mode"):
        after.since(replace(before, mode="off"))


def test_metric_windows_reject_decreasing_physical_counters() -> None:
    metrics = Metrics()
    before = metrics.snapshot()
    metrics.io[IoPurpose.FLUSH].write_bytes = 10
    after = metrics.snapshot()
    with pytest.raises(ArgumentError, match="larger counter"):
        before.since(after)
    metrics.maintenance.flushes = 1
    with pytest.raises(ArgumentError, match="larger counter"):
        after.since(metrics.snapshot())


def test_disabled_capture_is_explicit_and_has_unavailable_ratios() -> None:
    metrics = Metrics("off")
    before = metrics.snapshot()
    # Every production owner follows this guard, rather than a no-op event API.
    if metrics.enabled:
        metrics.logical.hits += 1
    window = metrics.snapshot().since(before)
    assert window.mode == "off"
    assert window.logical == LogicalStats()
    assert window.read_amplification.value is None
    assert window.write_amplification.value is None
    assert all(part.read_calls == part.write_calls == 0 for part in window.io)


def test_metric_records_support_strict_external_result_serialization() -> None:
    metrics = Metrics()
    before = metrics.snapshot()
    metrics.logical.hits = 1
    after = metrics.snapshot()
    snapshot_adapter = TypeAdapter(MetricsSnapshot)
    window_adapter = TypeAdapter(MetricsWindow)
    assert snapshot_adapter.validate_json(snapshot_adapter.dump_json(after), strict=True) == after
    window = after.since(before)
    assert window_adapter.validate_json(window_adapter.dump_json(window), strict=True) == window


def test_ratios_keep_zero_distinct_from_unavailable() -> None:
    assert Ratio(0, 0).value is None
    assert Ratio(5, 0).value is None
    assert Ratio(0, 5).value == 0.0
    assert Ratio(3, 2).value == 1.5
    with pytest.raises(ArgumentError, match="nonnegative integers"):
        Ratio(-1, 2)
    with pytest.raises(ArgumentError, match="nonnegative integers"):
        Ratio(1, -2)
    with pytest.raises(ArgumentError, match="nonnegative integers"):
        Ratio(numerator=True, denominator=2)
