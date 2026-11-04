"""Verify built artifacts and real installed clients outside the source checkout."""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import subprocess
import tarfile
import tempfile
import zipfile
from email.parser import BytesParser
from pathlib import Path, PurePosixPath
from typing import cast

_BENCHMARK_OPERATIONS = 32
_API_SMOKE = """
import sys
from importlib.resources import files
from pathlib import Path
import lsm_store
import lsm_bench
from lsm_store import Database

checkout = Path(sys.argv[1])
assert sys.version_info[:2] == (3, 12)
for package in (lsm_store, lsm_bench):
    origin = Path(package.__file__).resolve()
    assert origin.is_relative_to(Path(sys.prefix).resolve()), origin
    assert not origin.is_relative_to(checkout), origin
    assert files(package).joinpath('py.typed').is_file(), package.__name__
    print(f'{package.__name__}: {origin}')
path = Path('api-store')
with Database.open(path) as database:
    database.put(b'', b'')
    database.put(b'b', b'old')
    database.flush()
    database.put(b'b', b'new')
    database.put(b'c', b'deleted')
    database.delete(b'c')
    assert database.get(b'') == b''
    assert database.get(b'b') == b'new'
    assert database.scan(b'', b'z') == [(b'', b''), (b'b', b'new')]
with Database.open(path) as recovered:
    assert recovered.scan(b'', b'z') == [(b'', b''), (b'b', b'new')]
    recovered.flush()
"""


def _run(command: list[str], directory: Path, *, expected: int = 0) -> str:
    print("+", shlex.join(command), flush=True)
    environment = {
        key: value for key, value in os.environ.items() if key not in {"PYTHONPATH", "PYTHONHOME"}
    }
    # Callers supply fixed programs and test-owned operands; no shell is involved.
    result = subprocess.run(  # noqa: S603
        command,
        cwd=directory,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    if result.returncode != expected:
        # Do not forward installer diagnostics that might contain index credentials.
        raise RuntimeError(f"command exited {result.returncode}, expected {expected}")
    return result.stdout


def _check_archives(root: Path, sdist: Path, wheel: Path) -> None:
    with tarfile.open(sdist) as archive:
        names = {
            "/".join(PurePosixPath(member.name).parts[1:]) for member in archive if member.isfile()
        }
    required = {"LICENSE.txt", "README.md", "prek.toml", "uv.lock"}
    for directory, pattern in (("docs", "*.md"), ("tests", "*.py"), ("examples", "*.toml")):
        paths = {path.relative_to(root).as_posix() for path in (root / directory).rglob(pattern)}
        if not paths:
            raise RuntimeError(f"source has no {directory} inputs to verify")
        required.update(paths)
    if missing := required - names:
        raise RuntimeError(f"sdist is missing required inputs: {sorted(missing)}")
    if {name for name in names if name.startswith("docs/")} != {
        name for name in required if name.startswith("docs/")
    }:
        raise RuntimeError("sdist documentation does not match the maintained documents")
    with zipfile.ZipFile(wheel) as archive:
        wheel_names = set(archive.namelist())
        metadata_paths = [name for name in wheel_names if name.endswith(".dist-info/METADATA")]
        if len(metadata_paths) != 1:
            raise RuntimeError("wheel must contain exactly one package metadata file")
        metadata = BytesParser().parsebytes(archive.read(metadata_paths[0]))
    if any(
        name in metadata for name in ("Author", "Author-email", "Maintainer", "Maintainer-email")
    ):
        raise RuntimeError("package metadata must not contain personal attribution fields")
    expected = {
        path.relative_to(root / "src").as_posix()
        for package in ("lsm_store", "lsm_bench")
        for path in (root / "src" / package).rglob("*")
        if path.is_file() and (path.suffix == ".py" or path.name == "py.typed")
    }
    actual = {
        name
        for name in wheel_names
        if not name.endswith("/") and not PurePosixPath(name).parts[0].endswith(".dist-info")
    }
    if actual != expected:
        raise RuntimeError("wheel contents do not match the maintained packages")


def _check_clients(root: Path, scratch: Path, environment: Path) -> None:
    binaries = environment / "bin"
    smoke = scratch / "api_smoke.py"
    smoke.write_text(_API_SMOKE, encoding="utf-8")
    print(_run([str(binaries / "python"), "-I", str(smoke), str(root)], scratch), end="")
    store = str(binaries / "lsm-store")
    _run([store, "check-config"], scratch)
    _run([store, "put", "cli-store", "key", "value"], scratch)
    found: object = json.loads(_run([store, "get", "cli-store", "key"], scratch))
    if found != {"found": True, "value_hex": b"value".hex()}:
        raise RuntimeError("installed point-read command returned the wrong value")
    rows: object = json.loads(_run([store, "scan", "cli-store", "", "z"], scratch))
    if rows != [{"key_hex": b"key".hex(), "value_hex": b"value".hex()}]:
        raise RuntimeError("installed scan command returned the wrong rows")
    _run([store, "flush", "cli-store"], scratch)
    _run([store, "delete", "cli-store", "key"], scratch)
    _run([store, "get", "cli-store", "key"], scratch, expected=3)
    configuration = scratch / "benchmark.toml"
    configuration.write_text(
        "[store]\nmemtable_bytes=128\ntable_bytes=256\nblock_bytes=64\n"
        f"[workload]\noperations={_BENCHMARK_OPERATIONS}\nkeys=8\n"
        "key_bytes=1\nvalue_bytes=4\nseed=1\n",
        encoding="utf-8",
    )
    output = scratch / "result.json"
    benchmark = str(binaries / "lsm-bench")
    command = [benchmark, "run", "--config", str(configuration), "--output", str(output)]
    _run(command, scratch)
    parsed: object = json.loads(output.read_text(encoding="utf-8"))
    if not isinstance(parsed, dict):
        raise TypeError("installed benchmark did not write a JSON object")
    result = cast("dict[str, object]", parsed)  # JSON object keys are strings.
    if result.get("correctness") is not True or result.get("operations") != _BENCHMARK_OPERATIONS:
        raise RuntimeError("installed benchmark did not validate the requested trace")
    print("installed benchmark source:", json.dumps(result.get("source"), sort_keys=True))


def _check_benchmark_commands(scratch: Path, environment: Path) -> None:
    benchmark = str(environment / "bin" / "lsm-bench")
    configuration = scratch / "benchmark.toml"
    _run([benchmark, "check-config", "--config", str(configuration)], scratch)
    _run([benchmark, "report", str(scratch / "result.json")], scratch)
    suite = scratch / "suite.toml"
    suite.write_text(
        'repetitions=1\nmeasurements=["throughput","latency"]\n'
        '[[cases]]\nname="loaded"\n[cases.scenario]\ninitial_state="table"\n'
        "[cases.scenario.workload]\noperations=8\nkeys=2\n"
        "put_fraction=0.0\nget_fraction=1.0\nscan_fraction=0.0\n",
        encoding="utf-8",
    )
    output = scratch / "suite"
    _run([benchmark, "suite", "--config", str(suite), "--output", str(output)], scratch)
    _run([benchmark, "report", str(output / "summary.json")], scratch)
    _run(
        [
            benchmark,
            "profile",
            "--config",
            str(configuration),
            "--output",
            str(scratch / "profile"),
        ],
        scratch,
    )
    if not (output / "source.zip").is_file():
        raise RuntimeError("installed suite did not preserve its source")


def main() -> None:
    """Build once, check archive contents, then verify a hash-locked clean install."""
    root = Path(__file__).resolve().parents[1]
    # Wheel contents and the installed client report identify the artifact. Git is
    # deliberately not required: this command also belongs in an extracted sdist.
    print(f"source root: {root}")
    # Securely create a private directory on the local Linux scratch filesystem.
    with tempfile.TemporaryDirectory(prefix="lsm-distribution-", dir="/tmp") as name:
        scratch = Path(name)
        distribution = scratch / "dist"
        _run(["uv", "build", "--sdist", "--out-dir", str(distribution)], root)
        sdists = tuple(distribution.glob("*.tar.gz"))
        if len(sdists) != 1:
            raise RuntimeError("build must produce exactly one sdist")
        extracted = scratch / "source"
        extracted.mkdir()
        with tarfile.open(sdists[0]) as archive:
            archive.extractall(extracted, filter="data")
        roots = tuple(extracted.iterdir())
        if len(roots) != 1 or not roots[0].is_dir():
            raise RuntimeError("sdist must have exactly one source root")
        # Build from the extracted archive: deleted modules in a checkout's build
        # cache must not leak into the wheel, and an sdist must build without Git.
        _run(["uv", "build", "--wheel", "--out-dir", str(distribution)], roots[0])
        wheels = tuple(distribution.glob("*.whl"))
        if len(wheels) != 1:
            raise RuntimeError("build must produce exactly one wheel")
        _check_archives(root, sdists[0], wheels[0])
        requirements = scratch / "runtime.txt"
        export = ["uv", "export", "--locked", "--no-dev", "--no-emit-project"]
        _run([*export, "--output-file", str(requirements)], root)
        environment = scratch / "venv"
        venv = ["uv", "venv", "--python", "3.12", "--no-python-downloads", "--no-project"]
        _run([*venv, str(environment)], scratch)
        python = str(environment / "bin" / "python")
        install = ["uv", "pip", "install", "--python", python, "--no-deps"]
        _run([*install, "--require-hashes", "-r", str(requirements)], scratch)
        _run([*install, str(wheels[0])], scratch)
        _check_clients(root, scratch, environment)
        _check_benchmark_commands(scratch, environment)
        print("wheel_sha256:", hashlib.sha256(wheels[0].read_bytes()).hexdigest())
    print("distribution verification passed")


if __name__ == "__main__":
    main()
