"""Across-run summaries preserve quantile and repetition boundaries."""

from typing import Annotated, Literal

from pydantic import Field

from lsm_store.config import CaptureMode

from .config import MeasurementMode
from .result import LatencyCapture, NoSamples, ResultModel, RunResult, SampledLatencies
from .statistics import Latencies, Variation, variation


class Quantiles(ResultModel):
    """Variation of per-run quantiles, never pooled-sample percentiles."""

    kind: Literal["quantiles"] = "quantiles"
    p50_ns: Variation
    p95_ns: Variation
    p99_ns: Variation


type QuantileGroup = Annotated[NoSamples | Quantiles, Field(discriminator="kind")]


class LatencyGroups(ResultModel):
    """Empty families stay explicitly empty."""

    puts: QuantileGroup
    deletes: QuantileGroup
    gets: QuantileGroup
    scans: QuantileGroup


class DistributionOnly(ResultModel):
    """A retained run distribution, without a duplicate of its literal samples."""

    kind: Literal["distribution"] = "distribution"
    distribution: Latencies


type SampleSummary = Annotated[NoSamples | DistributionOnly, Field(discriminator="kind")]


class Observation(ResultModel):
    """Small summary of one already retained full run."""

    case: str
    repetition: int
    measurement: MeasurementMode
    capture: CaptureMode
    file: str
    throughput_ops_per_second: float
    elapsed_ns: int
    operations: int
    trace_sha256: str
    puts: SampleSummary
    deletes: SampleSummary
    gets: SampleSummary
    scans: SampleSummary


def observation(result: RunResult, case: str, repetition: int, file: str) -> Observation:
    """Retain only summary distributions, not all run samples in suite memory."""
    families = result.capture if isinstance(result.capture, LatencyCapture) else None

    def compact(series: SampledLatencies | NoSamples) -> SampleSummary:
        # Full literal samples are retained in the run artifact, not duplicated here.
        return (
            NoSamples()
            if isinstance(series, NoSamples)
            else DistributionOnly(distribution=series.distribution)
        )

    return Observation(
        case=case,
        repetition=repetition,
        measurement=result.config.measurement,
        capture=result.config.store.instrumentation,
        file=file,
        throughput_ops_per_second=result.throughput_ops_per_second,
        elapsed_ns=result.elapsed_ns,
        operations=result.operations,
        trace_sha256=result.trace_sha256,
        puts=NoSamples() if families is None else compact(families.puts),
        deletes=NoSamples() if families is None else compact(families.deletes),
        gets=NoSamples() if families is None else compact(families.gets),
        scans=NoSamples() if families is None else compact(families.scans),
    )


class Group(ResultModel):
    """One case/mode/capture cell, including incomplete repetition counts."""

    case: str
    measurement: MeasurementMode
    capture: CaptureMode
    completed: int
    expected: int
    throughput_ops_per_second: Variation
    latency: LatencyGroups
    files: tuple[str, ...]


def _quantiles(series: tuple[SampleSummary, ...]) -> QuantileGroup:
    sampled = tuple(item.distribution for item in series if isinstance(item, DistributionOnly))
    if not sampled:
        return NoSamples()
    return Quantiles(
        p50_ns=variation(tuple(float(item.p50_ns) for item in sampled)),
        p95_ns=variation(tuple(float(item.p95_ns) for item in sampled)),
        p99_ns=variation(tuple(float(item.p99_ns) for item in sampled)),
    )


def groups(observations: tuple[Observation, ...], repetitions: int) -> tuple[Group, ...]:
    """Summarize every observed cell without selecting winning repetitions."""
    buckets: dict[tuple[str, MeasurementMode, CaptureMode], list[Observation]] = {}
    for item in observations:
        buckets.setdefault((item.case, item.measurement, item.capture), []).append(item)
    result: list[Group] = []
    for (case, measurement, capture), items in sorted(buckets.items()):
        result.append(
            Group(
                case=case,
                measurement=measurement,
                capture=capture,
                completed=len(items),
                expected=repetitions,
                throughput_ops_per_second=variation(
                    tuple(item.throughput_ops_per_second for item in items)
                ),
                latency=LatencyGroups(
                    puts=_quantiles(tuple(item.puts for item in items)),
                    deletes=_quantiles(tuple(item.deletes for item in items)),
                    gets=_quantiles(tuple(item.gets for item in items)),
                    scans=_quantiles(tuple(item.scans for item in items)),
                ),
                files=tuple(item.file for item in items),
            )
        )
    return tuple(result)


class CaptureComparison(ResultModel):
    """Paired elapsed-cost differences from identical traces and repetition IDs."""

    case: str
    measurement: MeasurementMode
    paired_repetitions: int
    added_ns_per_operation: Variation
    relative_added_cost: Variation


def comparisons(observations: tuple[Observation, ...]) -> tuple[CaptureComparison, ...]:
    """Compare only complete off/counters pairs; never fill missing observations."""
    pairs: dict[tuple[str, MeasurementMode, int], dict[CaptureMode, Observation]] = {}
    for item in observations:
        pairs.setdefault((item.case, item.measurement, item.repetition), {})[item.capture] = item
    costs: dict[tuple[str, MeasurementMode], list[tuple[float, float]]] = {}
    for (case, measurement, _repetition), pair in pairs.items():
        if "off" not in pair or "counters" not in pair:
            continue
        off, counted = pair["off"], pair["counters"]
        if off.operations != counted.operations or off.trace_sha256 != counted.trace_sha256:
            raise ValueError("capture comparison requires matching operation counts")
        added = (counted.elapsed_ns - off.elapsed_ns) / off.operations
        relative = counted.elapsed_ns / off.elapsed_ns - 1
        costs.setdefault((case, measurement), []).append((added, relative))
    return tuple(
        CaptureComparison(
            case=case,
            measurement=measurement,
            paired_repetitions=len(values),
            added_ns_per_operation=variation(tuple(item[0] for item in values)),
            relative_added_cost=variation(tuple(item[1] for item in values)),
        )
        for (case, measurement), values in sorted(costs.items())
    )
