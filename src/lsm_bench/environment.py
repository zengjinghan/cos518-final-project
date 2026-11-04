"""Observe the supported measurement host without recording personal paths."""

import gc
import os
import platform
from dataclasses import dataclass
from importlib.metadata import distributions
from pathlib import Path
from time import get_clock_info
from typing import Final

from lsm_store.errors import ConfigurationError

DEFAULT_SCRATCH: Final = Path("/tmp")  # noqa: S108 -- Parent only; stores use secure TemporaryDirectory.


@dataclass(frozen=True, slots=True)
class Package:
    """An installed distribution's public identity."""

    name: str
    version: str


@dataclass(frozen=True, slots=True)
class Environment:
    """Material host settings, not hostname or checkout identity."""

    cpu_model: str | None
    affinity: tuple[int, ...]
    filesystem: str
    filesystem_block_bytes: int
    gc_enabled: bool
    wall_clock_resolution_ns: float
    packages: tuple[Package, ...]


def _unescape(value: str) -> str:
    for encoded, literal in ((r"\040", " "), (r"\011", "\t"), (r"\012", "\n"), (r"\134", "\\")):
        value = value.replace(encoded, literal)
    return value


def observe(parent: Path) -> Environment:
    """Require local ext4 and retain native settings before measurement."""
    if platform.system() != "Linux":
        raise ConfigurationError("benchmarks require Linux and a local ext4 scratch directory")
    if not parent.is_dir():
        raise ConfigurationError(f"benchmark scratch parent does not exist: {parent}")
    try:
        resolved = parent.resolve()
        candidates: list[tuple[int, str]] = []
        for line in Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines():
            location, information = line.split(" - ", 1)
            mount = Path(_unescape(location.split()[4]))
            if resolved.is_relative_to(mount):
                candidates.append((len(mount.parts), information.split()[0]))
        if not candidates:
            raise ConfigurationError("cannot identify the benchmark scratch filesystem")
        filesystem = max(candidates)[1]
        if filesystem != "ext4":
            raise ConfigurationError(f"benchmark scratch must use ext4; found {filesystem}")
        cpu = next(
            (
                line.partition(":")[2].strip()
                for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines()
                if line.startswith("model name")
            ),
            None,
        )
        packages: list[Package] = []
        for distribution in distributions():
            name = distribution.metadata.get("Name")
            if name is None:
                raise ConfigurationError("installed package has no distribution name")
            packages.append(Package(name, distribution.version))
        return Environment(
            cpu,
            tuple(sorted(os.sched_getaffinity(0))),
            filesystem,
            os.statvfs(parent).f_bsize,
            gc.isenabled(),
            get_clock_info("perf_counter").resolution * 1e9,
            tuple(sorted(packages, key=lambda item: item.name.lower())),
        )
    except (OSError, ValueError, IndexError) as error:
        raise ConfigurationError(f"cannot observe benchmark environment: {error}") from error
