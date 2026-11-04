"""Profile only the measured phase through the canonical benchmark runner."""

import cProfile
import pstats
from pathlib import Path

from .artifacts import check_output, write
from .config import BenchmarkConfig
from .environment import DEFAULT_SCRATCH, observe
from .result import RunResult
from .runner import run


def profile(config: BenchmarkConfig, output: Path, *, parent: Path = DEFAULT_SCRATCH) -> RunResult:
    """Keep diagnostic profile artifacts separate from accepted suite timings."""
    check_output(output)
    observe(parent)
    output.mkdir()
    profiler = cProfile.Profile()
    result = run(config, parent=parent, profiler=profiler)
    with (output / "profile.txt").open("x", encoding="utf-8") as target:
        statistics = pstats.Stats(profiler, stream=target).strip_dirs().sort_stats("cumulative")
        statistics.dump_stats(str(output / "profile.prof"))
        statistics.print_stats(40)
    write(output / "result.json", result)
    return result
