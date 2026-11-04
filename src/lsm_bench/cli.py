"""Run, repeat, inspect and profile explicitly configured database benchmarks."""

import argparse
import json
import sys
from pathlib import Path

from lsm_store.errors import ConfigurationError, StoreError

from .artifacts import check_output, write
from .config import BenchmarkConfig, SuiteConfig, load
from .environment import DEFAULT_SCRATCH
from .profiling import profile
from .reporting import read, render
from .runner import run
from .suite import Complete, run_suite


class _Arguments(argparse.Namespace):
    command: str = ""
    config: Path = Path()
    output: Path = Path()
    scratch: Path = DEFAULT_SCRATCH
    artifact: Path = Path()
    debug: bool = False
    schema: bool = False
    kind: str = "run"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lsm-bench", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("run", "suite", "profile"):
        command = commands.add_parser(name)
        command.add_argument("--config", type=Path, required=True)
        command.add_argument("--output", type=Path, required=True)
        command.add_argument("--scratch", type=Path, default=DEFAULT_SCRATCH)
        command.add_argument("--debug", action="store_true")
    check = commands.add_parser("check-config")
    check.add_argument("--config", type=Path, required=True)
    check.add_argument("--kind", choices=("run", "suite"), default="run")
    check.add_argument("--schema", action="store_true")
    check.add_argument("--debug", action="store_true")
    report = commands.add_parser("report")
    report.add_argument("artifact", type=Path)
    report.add_argument("--debug", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Validate before effects and return nonzero for incomplete suites."""
    arguments = _Arguments()
    _parser().parse_args(argv, namespace=arguments)
    try:
        if arguments.command == "check-config":
            schema = SuiteConfig if arguments.kind == "suite" else BenchmarkConfig
            resolved = load(arguments.config, schema)
            print(
                json.dumps(
                    schema.model_json_schema()
                    if arguments.schema
                    else resolved.model_dump(mode="json"),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        if arguments.command == "report":
            print(render(read(arguments.artifact)))
            return 0
        if arguments.command == "suite":
            result = run_suite(
                load(arguments.config, SuiteConfig), arguments.output, parent=arguments.scratch
            )
            print(render(result))
            return 0 if isinstance(result.status, Complete) else 1
        config = load(arguments.config, BenchmarkConfig)
        if arguments.command == "profile":
            result_run = profile(config, arguments.output, parent=arguments.scratch)
        else:
            check_output(arguments.output)
            result_run = run(config, parent=arguments.scratch)
            write(arguments.output, result_run)
        print(render(result_run))
    except ConfigurationError as error:
        if arguments.debug:
            raise
        print(f"configuration error: {error}", file=sys.stderr)
        return 2
    except (StoreError, OSError) as error:
        if arguments.debug:
            raise
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
