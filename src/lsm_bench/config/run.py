"""Initial state, warmup and capture are explicit scenario semantics."""

from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from lsm_store.config import StoreConfig
from lsm_store.framing import HEADER_BYTES
from lsm_store.record import RECORD_HEADER_BYTES

from .model import Model
from .workload import WorkloadConfig

type MeasurementMode = Literal["throughput", "latency"]
type InitialState = Literal["empty", "resident", "table"]


class ScenarioConfig(Model):
    """One workload and its prepared state, independent of capture scheduling."""

    store: StoreConfig = Field(default_factory=StoreConfig)
    workload: WorkloadConfig = Field(default_factory=WorkloadConfig)
    initial_state: InitialState = "empty"
    warmup_operations: Annotated[int, Field(ge=0, le=1_000_000)] = 0

    @model_validator(mode="after")
    def validate_capacity(self) -> Self:
        """Reject lossy records, oversized traces and unintended resident flushes."""
        workload = self.workload
        payload = workload.key_bytes + workload.value_bytes
        if payload > self.store.max_record_bytes:
            raise ValueError("generated key/value length exceeds store.max_record_bytes")
        load_count = 0 if self.initial_state == "empty" else workload.keys
        if (load_count + workload.operations + self.warmup_operations) * payload > 128 << 20:
            raise ValueError("combined trace exceeds the 128-MiB payload budget")
        if self.initial_state == "resident":
            if workload.put_fraction or workload.get_fraction + workload.scan_fraction != 1:
                raise ValueError("resident scenarios require a read-only operation mix")
            framed = workload.keys * (payload + HEADER_BYTES + RECORD_HEADER_BYTES)
            if framed + 8 >= self.store.memtable_bytes:
                raise ValueError(
                    "resident load must fit below the journal/memtable flush threshold"
                )
        return self


class BenchmarkConfig(ScenarioConfig):
    """A complete single-run configuration."""

    measurement: MeasurementMode = "throughput"
