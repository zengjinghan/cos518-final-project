"""Reproducible scheduling, bounded execution and immediate raw retention."""

from dataclasses import dataclass
from pathlib import Path
from random import Random
from time import perf_counter_ns
from typing import Annotated, Literal

from pydantic import Field

from lsm_store.config import CaptureMode, StoreConfig
from lsm_store.errors import CorruptionError

from .aggregation import CaptureComparison, Group, Observation, comparisons, groups, observation
from .artifacts import check_output, write
from .config import BenchmarkConfig, MeasurementMode, NamedScenario, SuiteConfig
from .environment import DEFAULT_SCRATCH, Environment, observe
from .result import ResultModel, RunResult
from .runner import run
from .source import SourceIdentity, archive_source, source_identity


class Complete(ResultModel):
    """Every prescribed repetition completed."""

    kind: Literal["complete"] = "complete"


class BudgetExhausted(ResultModel):
    """The between-run deadline was exceeded; no hard syscall timeout is claimed."""

    kind: Literal["budget_exhausted"] = "budget_exhausted"


class Failed(ResultModel):
    """A failed unit invalidates successful-suite interpretation."""

    kind: Literal["failed"] = "failed"
    error: str


type Status = Annotated[Complete | BudgetExhausted | Failed, Field(discriminator="kind")]


class SuiteResult(ResultModel):
    """A self-contained summary plus relative names of primary run artifacts."""

    kind: Literal["suite"] = "suite"
    format_version: Literal[2] = 2
    config: SuiteConfig
    source: SourceIdentity
    environment: Environment
    status: Status
    elapsed_ns: int
    expected_runs: int
    observations: tuple[Observation, ...]
    groups: tuple[Group, ...]
    capture_comparisons: tuple[CaptureComparison, ...]


@dataclass(frozen=True, slots=True)
class _Job:
    case: NamedScenario
    measurement: MeasurementMode
    capture: CaptureMode
    repetition: int


def _jobs(config: SuiteConfig) -> tuple[_Job, ...]:
    rng = Random(config.order_seed)  # noqa: S311 -- Reproducible schedule, not a secret.
    result: list[_Job] = []
    for repetition in range(config.repetitions):
        jobs = [
            _Job(case, measurement, capture, repetition)
            for case in config.cases
            for measurement in config.measurements
            for capture in config.captures
        ]
        rng.shuffle(jobs)
        result.extend(jobs)
    return tuple(result)


def _configuration(job: _Job) -> BenchmarkConfig:
    scenario = job.case.scenario
    store = StoreConfig.model_validate(
        {**scenario.store.model_dump(), "instrumentation": job.capture}
    )
    return BenchmarkConfig(
        store=store,
        workload=scenario.workload,
        initial_state=scenario.initial_state,
        warmup_operations=scenario.warmup_operations,
        measurement=job.measurement,
    )


def _check_identity(result: RunResult, source: SourceIdentity, environment: Environment) -> None:
    if (
        result.source != source
        or result.environment != environment
        or result.timing != "unprofiled"
    ):
        raise CorruptionError("benchmark source/environment changed during the suite")


def run_suite(config: SuiteConfig, output: Path, *, parent: Path = DEFAULT_SCRATCH) -> SuiteResult:
    """Retain partial observations on budget stop or unexpected failure."""
    check_output(output)
    environment = observe(parent)
    source = source_identity()
    output.mkdir()
    archive_source(output / "source.zip")
    write(output / "config.json", config)
    schedule = _jobs(config)
    observations: list[Observation] = []
    started = perf_counter_ns()
    status: Status = Failed(error="interrupted before completion")
    try:
        for job in schedule:
            if perf_counter_ns() - started >= config.budget_seconds * 1_000_000_000:
                status = BudgetExhausted()
                break
            result = run(_configuration(job), parent=parent)
            name = f"{job.case.name}-{job.measurement}-{job.capture}-{job.repetition + 1:02d}.json"
            write(output / name, result)
            observations.append(observation(result, job.case.name, job.repetition, name))
            _check_identity(result, source, environment)
            if perf_counter_ns() - started >= config.budget_seconds * 1_000_000_000:
                status = BudgetExhausted()
                break
        else:
            status = Complete()
    except BaseException as error:
        status = Failed(error=f"{type(error).__name__}: {error}")
        raise
    finally:
        summary = SuiteResult(
            config=config,
            source=source,
            environment=environment,
            status=status,
            elapsed_ns=perf_counter_ns() - started,
            expected_runs=len(schedule),
            observations=tuple(observations),
            groups=groups(tuple(observations), config.repetitions),
            capture_comparisons=comparisons(tuple(observations)),
        )
        write(output / "summary.json", summary)
    return summary
