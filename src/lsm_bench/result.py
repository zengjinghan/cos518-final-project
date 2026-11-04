"""Versioned, strict artifacts distinguish unsampled and sampled measurements."""

from dataclasses import dataclass
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lsm_store.inspection import StoreInspection
from lsm_store.metrics import MetricsSnapshot, MetricsWindow, Ratio
from lsm_store.metrics.snapshot import LogicalStats
from lsm_store.table.cache import ReadCacheInfo

from .config import BenchmarkConfig
from .environment import Environment
from .source import SourceIdentity
from .statistics import Latencies


class ResultModel(BaseModel):
    """Result inputs reject unknown fields and nonfinite values."""

    model_config = ConfigDict(frozen=True, strict=True, extra="forbid", allow_inf_nan=False)


class NoSamples(ResultModel):
    """No operation of this kind was sampled."""

    kind: Literal["empty"] = "empty"


class SampledLatencies(ResultModel):
    """Literal samples and their nearest-rank summary."""

    kind: Literal["samples"] = "samples"
    samples_ns: tuple[int, ...]
    distribution: Latencies

    @model_validator(mode="after")
    def validate_population(self) -> Self:
        """A sampled family has a nonempty population with matching count."""
        if not self.samples_ns or len(self.samples_ns) != self.distribution.count:
            raise ValueError("latency samples must match a nonempty distribution")
        if any(value < 0 for value in self.samples_ns):
            raise ValueError("latency samples cannot be negative")
        return self


type LatencySeries = Annotated[NoSamples | SampledLatencies, Field(discriminator="kind")]


class ThroughputCapture(ResultModel):
    """No operation clocks or latency containers were used."""

    kind: Literal["throughput"] = "throughput"


class LatencyCapture(ResultModel):
    """Sampled operation families remain separate."""

    kind: Literal["latency"] = "latency"
    puts: LatencySeries
    deletes: LatencySeries
    gets: LatencySeries
    scans: LatencySeries


type Capture = Annotated[ThroughputCapture | LatencyCapture, Field(discriminator="kind")]


@dataclass(frozen=True, slots=True)
class Preparation:
    """Creation/load cost and its handle's complete cumulative counters."""

    elapsed_ns: int
    counters: MetricsSnapshot


@dataclass(frozen=True, slots=True)
class Phase:
    """A bounded phase on the measured handle."""

    elapsed_ns: int
    counters: MetricsWindow


@dataclass(frozen=True, slots=True)
class RatioResult:
    """Preserve both exact counts and undefined division in JSON artifacts."""

    numerator: int
    denominator: int
    value: float | None

    @classmethod
    def from_ratio(cls, ratio: Ratio) -> "RatioResult":
        """Expose the ratio value alongside its independently inspectable operands."""
        return cls(ratio.numerator, ratio.denominator, ratio.value)


@dataclass(frozen=True, slots=True)
class Amplifications:
    """Different byte/count ratios must not share an ambiguous read-amp label."""

    all_write_bytes: RatioResult
    table_write_bytes: RatioResult
    foreground_read_bytes: RatioResult
    point_calls_per_get: RatioResult

    @classmethod
    def from_window(cls, window: MetricsWindow) -> "Amplifications":
        """Compute every ratio from the same explicit counter window."""
        return cls(
            RatioResult.from_ratio(window.write_amplification),
            RatioResult.from_ratio(window.table_write_amplification),
            RatioResult.from_ratio(window.read_amplification),
            RatioResult.from_ratio(window.point_read_calls_per_get),
        )


class RunResult(ResultModel):
    """One validated trace; profiled timing is explicitly diagnostic."""

    kind: Literal["run"] = "run"
    format_version: Literal[2] = 2
    config: BenchmarkConfig
    source: SourceIdentity
    environment: Environment
    trace_sha256: str
    timing: Literal["unprofiled", "cprofile"]
    durability: Literal["fsync-per-mutation"] = "fsync-per-mutation"
    correctness: Literal[True] = True
    correctness_scope: str = (
        "reference responses, enabled-counter agreement, persisted final map; "
        "not each timed response"
    )
    operations: int
    expected_logical: LogicalStats
    reference_validation_ns: int
    preparation: Preparation
    opening: Preparation
    warmup: Phase
    elapsed_ns: int
    cpu_ns: int
    throughput_ops_per_second: float
    throughput_with_final_flush_ops_per_second: float
    capture: Capture
    read_cache: ReadCacheInfo
    operations_window: MetricsWindow
    final_flush: Phase
    inspection_phase: Phase
    post_validation_ns: int
    amplification: Amplifications
    inspection: StoreInspection

    @model_validator(mode="after")
    def validate_capture(self) -> Self:
        """Reject mislabeled timing modes and incomplete sampled traces."""
        if self.capture.kind != self.config.measurement:
            raise ValueError("capture does not match the configured measurement mode")
        if self.operations != self.config.workload.operations or self.elapsed_ns <= 0:
            raise ValueError("result must retain the complete positive-duration trace")
        if isinstance(self.capture, LatencyCapture):
            series = (
                self.capture.puts,
                self.capture.deletes,
                self.capture.gets,
                self.capture.scans,
            )
            samples = sum(
                len(item.samples_ns) for item in series if isinstance(item, SampledLatencies)
            )
            if samples != self.operations:
                raise ValueError("latency capture must sample every measured operation")
        return self
