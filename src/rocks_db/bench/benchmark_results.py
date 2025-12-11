"""Schema definitions for benchmark output.

This module defines Pydantic models representing:
- Benchmark-level metadata (environment, git, system info),
- Per-run results for each experiment repetition,
- The aggregate result object saved incrementally during the benchmark.

These classes are pure data containers and contain *no* benchmark logic.
"""

from __future__ import annotations

import importlib.metadata
import platform
import shlex
import subprocess
import sys
from typing import Any

import psutil
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from ..metrics import MetricsSnapshot
from .config_schema import ExperimentConfig

__all__ = (
    "BenchmarkMetadata",
    "RunResult",
    "RunResultList",
    "BenchmarkResult",
)


def get_git_repo_url():
    """Return the current git repo URL, or None if unavailable."""
    try:
        return subprocess.check_output(["git", "config", "--get", "remote.origin.url"]).strip().decode()
    except subprocess.CalledProcessError:
        return None


def get_git_branch() -> str | None:
    """Return the current git branch, or None if unavailable."""
    try:
        return subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"]).strip().decode()
    except subprocess.CalledProcessError:
        return None


def get_git_commit_id() -> str | None:
    """Return the current git commit hash, or None if unavailable."""
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"]).strip().decode()
    except subprocess.CalledProcessError:
        return None


def check_if_git_has_uncommitted_changes() -> bool | None:
    """Checks for uncommitted changes in the current Git repository."""
    try:
        output = subprocess.check_output(["git", "status", "--porcelain"]).strip().decode()
        return len(output) != 0
    except subprocess.CalledProcessError:
        return None


def get_installed_python_packages() -> dict[str, str]:
    """Get all installed packages and their versions."""
    packages = importlib.metadata.distributions()
    return {pkg.metadata["Name"]: pkg.metadata["Version"] for pkg in packages}


class BenchmarkMetadata(BaseModel):
    """Global metadata for a full benchmark run."""

    model_config = ConfigDict(extra="forbid")

    name: str
    output_path: str
    args: dict[str, Any]

    start_timestamp: float
    end_timestamp: float

    # Whether all runs succeeded
    is_success: bool = True

    python_executable: str = Field(default=sys.executable)
    command: str | None = Field(default_factory=lambda: shlex.join(sys.argv))

    git_repo: str | None = Field(default_factory=get_git_repo_url)
    git_commit: str | None = Field(default_factory=get_git_commit_id)
    git_branch: str | None = Field(default_factory=get_git_branch)
    git_has_uncommitted_changes: bool | None = Field(default_factory=check_if_git_has_uncommitted_changes)

    platform: str = Field(default_factory=lambda: platform.platform())
    system: str = Field(default_factory=lambda: platform.system())
    machine_type: str = Field(default_factory=lambda: platform.machine())

    num_cpu_physical_cores: int | None = Field(default_factory=lambda: psutil.cpu_count(logical=False))
    num_cpu_logical_cores: int | None = Field(default_factory=lambda: psutil.cpu_count(logical=True))
    total_memory_bytes: int | None = Field(default_factory=lambda: psutil.virtual_memory().total)

    python_version: str = Field(default_factory=lambda: platform.python_version())
    python_packages: dict[str, str] = Field(default_factory=lambda: get_installed_python_packages())


class RunResult(MetricsSnapshot):
    """Per-run metrics for a single repetition of an experiment."""

    model_config = ConfigDict(extra="forbid")

    # Experiment identity
    experiment_name: str
    repetition_id: int

    # Status
    is_success: bool = True
    error: str | None = None

    # Config
    config: ExperimentConfig

    # Timing
    start_timestamp: float
    end_timestamp: float
    elapsed_ns: float  # This is not end_timestamp - start_timestamp, but rather uses perf_counter_ns

    # Throughput
    throughput_ops_per_sec: float

    # Latency (means + percentiles)
    min_read_latency_ns: float
    mean_read_latency_ns: float
    max_read_latency_ns: float
    p50_read_latency_ns: float
    p90_read_latency_ns: float
    p95_read_latency_ns: float
    p99_read_latency_ns: float

    min_write_latency_ns: float
    mean_write_latency_ns: float
    max_write_latency_ns: float
    p50_write_latency_ns: float
    p90_write_latency_ns: float
    p95_write_latency_ns: float
    p99_write_latency_ns: float


RunResultList = TypeAdapter(list[RunResult])


class BenchmarkResult(BaseModel):
    """Top-level benchmark result: metadata + all run results."""

    model_config = ConfigDict(extra="forbid")

    metadata: BenchmarkMetadata
    runs: list[RunResult] = Field(default_factory=list)
