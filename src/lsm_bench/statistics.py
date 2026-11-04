"""Exact sample quantiles and robust across-run summaries."""

from dataclasses import dataclass
from math import ceil
from statistics import fmean, median


@dataclass(frozen=True, slots=True)
class Latencies:
    """Exact nearest-rank summaries of a finite nanosecond sample population."""

    count: int
    minimum_ns: int
    mean_ns: float
    p50_ns: int
    p95_ns: int
    p99_ns: int
    maximum_ns: int


def summarize(samples: list[int]) -> Latencies | None:
    """Return unavailable for no observations, not a fabricated zero percentile."""
    if not samples:
        return None
    ordered = sorted(samples)
    return Latencies(
        len(ordered),
        ordered[0],
        fmean(ordered),
        ordered[ceil(0.50 * len(ordered)) - 1],
        ordered[ceil(0.95 * len(ordered)) - 1],
        ordered[ceil(0.99 * len(ordered)) - 1],
        ordered[-1],
    )


@dataclass(frozen=True, slots=True)
class Variation:
    """Describe observed repetitions without a population-confidence claim."""

    count: int
    minimum: float
    median: float
    maximum: float
    mad: float


def variation(values: tuple[float, ...]) -> Variation:
    """Retain every repetition's range and median absolute deviation."""
    if not values:
        raise ValueError("cannot summarize an empty repetition group")
    center = median(values)
    return Variation(
        len(values), min(values), center, max(values), median(abs(v - center) for v in values)
    )
