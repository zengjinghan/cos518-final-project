"""Human-readable views of strict run and suite artifacts."""

from pathlib import Path
from typing import Annotated

from pydantic import Field, TypeAdapter, ValidationError

from lsm_store.errors import ConfigurationError

from .aggregation import Quantiles
from .result import LatencyCapture, RunResult, SampledLatencies
from .suite import SuiteResult

type Artifact = Annotated[RunResult | SuiteResult, Field(discriminator="kind")]
_SCHEMA = TypeAdapter(Artifact)


def read(path: Path) -> Artifact:
    """Reject unknown formats instead of guessing artifact versions."""
    try:
        return _SCHEMA.validate_json(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, ValidationError) as error:
        raise ConfigurationError(f"invalid benchmark artifact {path}: {error}") from error


def render(result: Artifact) -> str:
    """Label sampled/profiled timings and incomplete comparison cells."""
    if isinstance(result, RunResult):
        lines = [
            (
                f"{result.config.measurement} / {result.config.store.instrumentation} / "
                f"{result.timing}"
            ),
            (
                f"{result.throughput_ops_per_second:,.1f} ops/s; {result.operations:,} operations; "
                f"including final flush "
                f"{result.throughput_with_final_flush_ops_per_second:,.1f} ops/s"
            ),
        ]
        state = result.read_cache
        work = result.operations_window.read_cache
        lines.append(
            f"read cache: {state.retained_bytes:,}/{state.capacity_bytes:,} charged bytes; "
            f"{state.entries:,} entries; block decodes/reuses "
            f"{work.block_decodes:,}/{work.block_reuses:,}; "
            f"capture {result.operations_window.mode}"
        )
        if isinstance(result.capture, LatencyCapture):
            for name, series in (
                ("put", result.capture.puts),
                ("delete", result.capture.deletes),
                ("get", result.capture.gets),
                ("scan", result.capture.scans),
            ):
                if isinstance(series, SampledLatencies):
                    distribution = series.distribution
                    lines.append(
                        f"{name}: p50 {distribution.p50_ns / 1000:,.2f} us; "
                        f"p95 {distribution.p95_ns / 1000:,.2f} us; "
                        f"p99 {distribution.p99_ns / 1000:,.2f} us"
                    )
        return "\n".join(lines)
    lines = [
        f"Suite: {result.status.kind}; {len(result.observations)}/{result.expected_runs} runs",
        "case | mode | capture | runs | median ops/s | MAD | min..max",
    ]
    for group in result.groups:
        stats = group.throughput_ops_per_second
        lines.append(
            f"{group.case} | {group.measurement} | {group.capture} | "
            f"{group.completed}/{group.expected} | "
            f"{stats.median:,.1f} | {stats.mad:,.1f} | {stats.minimum:,.1f}..{stats.maximum:,.1f}"
        )
        if group.measurement == "latency":
            for name, quantiles in (
                ("put", group.latency.puts),
                ("delete", group.latency.deletes),
                ("get", group.latency.gets),
                ("scan", group.latency.scans),
            ):
                if isinstance(quantiles, Quantiles):
                    lines.append(
                        f"  {name} median per-run p50/p95/p99: "
                        f"{quantiles.p50_ns.median / 1000:,.2f} / "
                        f"{quantiles.p95_ns.median / 1000:,.2f} / "
                        f"{quantiles.p99_ns.median / 1000:,.2f} us"
                    )
    for comparison in result.capture_comparisons:
        lines.append(
            f"{comparison.case} / {comparison.measurement}: paired capture cost "
            f"{comparison.added_ns_per_operation.median:,.2f} ns/op; "
            f"MAD {comparison.added_ns_per_operation.mad:,.2f}; "
            f"{comparison.paired_repetitions} pairs"
        )
    return "\n".join(lines)
