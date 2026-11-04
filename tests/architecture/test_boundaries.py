import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CORE = ROOT / "src" / "lsm_store"
ROOT_MODULES = {
    "__init__.py",
    "cli.py",
    "config.py",
    "database.py",
    "errors.py",
    "framing.py",
    "inspection.py",
    "maintenance.py",
    "memtable.py",
    "merge.py",
    "record.py",
    "version.py",
}
PACKAGES = {"storage", "table", "compaction", "metrics"}


def imported_modules(node: ast.AST, path: Path, package_root: Path = CORE) -> list[str]:
    if isinstance(node, ast.Import):
        return [alias.name for alias in node.names]
    if not isinstance(node, ast.ImportFrom):
        return []
    package = (package_root.name, *path.relative_to(package_root).parent.parts)
    prefix = package[: len(package) - node.level + 1] if node.level else ()
    base = ".".join((*prefix, *((node.module or "").split(".") if node.module else ())))
    # Ruff canonicalizes `import package.child as child` to this from-import.
    # Resolve the admitted submodule rather than mistaking it for the public API.
    if base == package_root.name or not node.module:
        return [f"{base}.{alias.name}" for alias in node.names]
    return [base]


def test_folder_grammar_matches_owned_packages() -> None:
    assert {path.name for path in CORE.glob("*.py")} == ROOT_MODULES
    assert {
        path.name for path in CORE.iterdir() if path.is_dir() and path.name != "__pycache__"
    } == PACKAGES
    assert {
        path.name
        for path in (ROOT / "src").iterdir()
        if path.is_dir() and not path.name.endswith(".egg-info") and path.name != "__pycache__"
    } == {"lsm_store", "lsm_bench"}
    assert not (ROOT / ".github").exists()
    assert (ROOT / "prek.toml").is_file()
    assert (CORE / "py.typed").is_file()
    assert not list((ROOT / "examples").rglob("*.yaml"))


def test_dependency_direction_and_no_dynamic_bypasses() -> None:
    for path in CORE.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        relative = path.relative_to(CORE)
        for node in ast.walk(tree):
            modules = imported_modules(node, path)
            assert not any(module.startswith("lsm_bench") for module in modules), path
            if relative.parts[0] == "compaction":
                assert "lsm_store" not in modules, path
                assert not any(
                    module.startswith(
                        (
                            "os",
                            "pathlib",
                            "subprocess",
                            "lsm_store.storage",
                            "lsm_store.database",
                            "lsm_store.Database",
                        )
                    )
                    for module in modules
                ), path
            if relative.parts[0] in {"storage", "table"}:
                assert "lsm_store" not in modules, path
                assert not any(
                    module.startswith(
                        (
                            "lsm_store.database",
                            "lsm_store.Database",
                            "lsm_store.version",
                            "lsm_store.maintenance",
                            "lsm_store.cli",
                        )
                    )
                    for module in modules
                ), path
            if isinstance(node, ast.Name):
                assert node.id not in {"Any", "__import__", "eval", "exec"}, path
            if isinstance(node, ast.Attribute):
                assert node.attr != "import_module", path


def test_library_has_no_implicit_printing_or_operation_timers() -> None:
    for path in CORE.rglob("*.py"):
        if path.name == "cli.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                assert node.func.id not in {
                    "print",
                    "perf_counter",
                    "perf_counter_ns",
                    "time_ns",
                }, path


def test_benchmark_ownership_is_acyclic_and_has_one_runner() -> None:
    root = ROOT / "src" / "lsm_bench"
    assert {path.name for path in root.glob("*.py")} == {
        "__init__.py",
        "aggregation.py",
        "artifacts.py",
        "cli.py",
        "environment.py",
        "measurement.py",
        "preparation.py",
        "profiling.py",
        "reporting.py",
        "result.py",
        "runner.py",
        "source.py",
        "statistics.py",
        "suite.py",
        "validation.py",
        "workload.py",
    }
    assert {
        path.name for path in root.iterdir() if path.is_dir() and path.name != "__pycache__"
    } == {"config"}
    for name in ("workload.py", "measurement.py", "preparation.py", "validation.py", "runner.py"):
        path = root / name
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            modules = imported_modules(node, path, root)
            assert not any(
                module.startswith(
                    (
                        "lsm_bench.cli",
                        "lsm_bench.suite",
                        "lsm_bench.profiling",
                        "lsm_bench.reporting",
                    )
                )
                for module in modules
            ), path
            if name == "measurement.py":
                assert not any(
                    module.startswith(("lsm_bench.validation", "lsm_bench.artifacts"))
                    for module in modules
                ), path
