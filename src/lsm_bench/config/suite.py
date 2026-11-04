"""A finite, reproducibly scheduled collection of complete scenarios."""

from typing import Annotated, Final, Self

from pydantic import Field, model_validator

from lsm_store.config import CaptureMode

from .model import Array, Model
from .run import MeasurementMode, ScenarioConfig

_MAX_RUNS: Final = 512


class NamedScenario(Model):
    """An artifact-safe name and its complete scenario configuration."""

    name: Annotated[str, Field(pattern=r"^[a-z][a-z0-9_-]{0,47}$")]
    scenario: ScenarioConfig


class SuiteConfig(Model):
    """Capture axes are owned by the suite, never duplicated inside scenarios."""

    repetitions: Annotated[int, Field(ge=1, le=30)] = 3
    measurements: Array[MeasurementMode] = ("throughput", "latency")
    captures: Array[CaptureMode] = ("counters",)
    order_seed: Annotated[int, Field(ge=0, le=(1 << 64) - 1)] = 42
    budget_seconds: Annotated[int, Field(gt=0, le=3600)] = 300
    cases: Annotated[Array[NamedScenario], Field(min_length=1, max_length=64)]

    @model_validator(mode="after")
    def validate_axes(self) -> Self:
        """Every scheduled axis is nonempty and unique."""
        for values in (self.measurements, self.captures, tuple(case.name for case in self.cases)):
            if not values or len(values) != len(set(values)):
                raise ValueError("suite names and capture axes must be nonempty and unique")
        if (
            len(self.cases) * len(self.measurements) * len(self.captures) * self.repetitions
            > _MAX_RUNS
        ):
            raise ValueError("suite exceeds the 512-run budget")
        return self
