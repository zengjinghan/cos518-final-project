"""A thin UTF-8 command adapter; persistent values are emitted losslessly as hex."""

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

from .config import CaptureMode, StoreConfig, load_config, parse_config
from .database import Database
from .errors import ArgumentError, ConfigurationError, StoreError


class _Arguments(argparse.Namespace):
    """Typed adapter-owned parser result; subcommands define required operands."""

    command: str = ""
    path: Path = Path()
    config: Path | None = None
    capture: CaptureMode | None = None
    memtable_bytes: int | None = None
    key: str = ""
    value: str = ""
    start: str = ""
    end: str = ""
    debug: bool = False
    schema: bool = False


@dataclass(frozen=True, slots=True)
class _Operands:
    key: bytes
    value: bytes
    start: bytes
    end: bytes


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lsm-store", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("check-config", "put", "get", "delete", "scan", "flush", "inspect"):
        command = commands.add_parser(name)
        command.add_argument("--config", type=Path)
        command.add_argument("--capture", choices=("off", "counters"))
        command.add_argument("--memtable-bytes", type=int)
        command.add_argument("--debug", action="store_true")
        if name == "check-config":
            command.add_argument("--schema", action="store_true")
            continue
        command.add_argument("path", type=Path)
        if name in ("put", "get", "delete"):
            command.add_argument("key")
        if name == "put":
            command.add_argument("value")
        if name == "scan":
            command.add_argument("start")
            command.add_argument("end")
    return parser


def _configuration(arguments: _Arguments) -> StoreConfig | None:
    overrides: dict[str, object] = {}
    if arguments.memtable_bytes is not None:
        overrides["memtable_bytes"] = arguments.memtable_bytes
    if arguments.capture is not None:
        overrides["instrumentation"] = arguments.capture
    if arguments.config is not None:
        return load_config(arguments.config, overrides)
    has_engine_override = arguments.memtable_bytes is not None
    return (
        parse_config(overrides)
        if has_engine_override or arguments.command == "check-config"
        else None
    )


def _operands(arguments: _Arguments) -> _Operands:
    try:
        return _Operands(
            arguments.key.encode(),
            arguments.value.encode(),
            arguments.start.encode(),
            arguments.end.encode(),
        )
    except UnicodeError as error:
        raise ArgumentError(f"command operands must be encodable as UTF-8: {error}") from error


def main(argv: list[str] | None = None) -> int:
    """Run a command with stable exit categories and contextual stderr diagnostics."""
    arguments = _Arguments()
    _parser().parse_args(argv, namespace=arguments)
    try:
        configuration = _configuration(arguments)
        operands = _operands(arguments)
        if arguments.command == "check-config":
            resolved = StoreConfig() if configuration is None else configuration
            print(
                json.dumps(
                    StoreConfig.model_json_schema()
                    if arguments.schema
                    else resolved.model_dump(mode="json"),
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        with Database.open(
            arguments.path.expanduser().absolute(),
            configuration,
            capture=arguments.capture,
            create=arguments.command == "put",
        ) as database:
            return _execute(database, arguments, operands)
    except (ConfigurationError, ArgumentError) as error:
        if arguments.debug:
            raise
        print(f"configuration/argument error: {error}", file=sys.stderr)
        return 2
    except StoreError as error:
        if arguments.debug:
            raise
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        return 1


def _execute(database: Database, arguments: _Arguments, operands: _Operands) -> int:
    match arguments.command:
        case "put":
            database.put(operands.key, operands.value)
        case "delete":
            database.delete(operands.key)
        case "get":
            value = database.get(operands.key)
            if value is None:
                print(json.dumps({"found": False}))
                return 3
            print(json.dumps({"found": True, "value_hex": value.hex()}))
        case "scan":
            print(
                json.dumps(
                    [
                        {"key_hex": key.hex(), "value_hex": value.hex()}
                        for key, value in database.scan(operands.start, operands.end)
                    ]
                )
            )
        case "flush":
            database.flush()
        case "inspect":
            inspection = database.inspect()
            report = asdict(inspection)
            for name, ratio in (
                ("table_space_amplification", inspection.table_space_amplification),
                ("total_space_amplification", inspection.total_space_amplification),
            ):
                report[name] = {
                    "numerator": ratio.numerator,
                    "denominator": ratio.denominator,
                    "value": ratio.value,
                }
            print(json.dumps(report, indent=2, sort_keys=True))
        case _:
            raise ArgumentError(f"unsupported command: {arguments.command}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
