from __future__ import annotations

from pathlib import Path

import pytest

from lsm_bench import suite
from lsm_bench.cli import main
from lsm_bench.config import (
    BenchmarkConfig,
    NamedScenario,
    ScenarioConfig,
    SuiteConfig,
    WorkloadConfig,
    load,
)
from lsm_bench.profiling import profile
from lsm_bench.reporting import read, render
from lsm_bench.suite import BudgetExhausted, Complete, Failed, SuiteResult, run_suite


def _scenario() -> NamedScenario:
    return NamedScenario(
        name="hits",
        scenario=ScenarioConfig(
            initial_state="resident",
            warmup_operations=2,
            workload=WorkloadConfig(
                operations=8,
                keys=2,
                key_bytes=1,
                value_bytes=2,
                put_fraction=0.0,
                get_fraction=1.0,
                scan_fraction=0.0,
            ),
        ),
    )


def test_suite_retains_runs_source_raw_samples_and_pair_comparisons(tmp_path: Path) -> None:
    config = SuiteConfig(cases=(_scenario(),), repetitions=2, captures=("off", "counters"))
    result = run_suite(config, tmp_path / "suite")
    assert isinstance(result.status, Complete)
    assert len(result.observations) == result.expected_runs == 8
    assert all(group.completed == group.expected == 2 for group in result.groups)
    assert len(result.capture_comparisons) == 2
    assert all(item.paired_repetitions == 2 for item in result.capture_comparisons)
    assert read(tmp_path / "suite/summary.json") == result
    assert (tmp_path / "suite/source.zip").is_file()
    for item in result.observations:
        assert read(tmp_path / "suite" / item.file).kind == "run"
    assert "median ops/s" in render(result)
    with pytest.raises(ValueError, match="already exists"):
        run_suite(config, tmp_path / "suite")


def test_budget_stop_retains_completed_unit_and_reports_partial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = iter((0, 0, 2_000_000_000, 2_000_000_000))
    monkeypatch.setattr(suite, "perf_counter_ns", lambda: next(clock))
    config = SuiteConfig(
        cases=(_scenario(),), measurements=("throughput",), repetitions=2, budget_seconds=1
    )
    result = run_suite(config, tmp_path / "partial")
    assert isinstance(result.status, BudgetExhausted)
    assert len(result.observations) == 1
    assert result.groups[0].completed == 1
    assert result.groups[0].expected == 2
    restored = SuiteResult.model_validate_json((tmp_path / "partial/summary.json").read_text())
    assert isinstance(restored.status, BudgetExhausted)


def test_failed_unit_retains_failure_not_success(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(_config: BenchmarkConfig, *, parent: Path) -> None:
        del parent
        raise RuntimeError("injected invalid evaluator")

    monkeypatch.setattr(suite, "run", fail)
    with pytest.raises(RuntimeError, match="invalid evaluator"):
        run_suite(SuiteConfig(cases=(_scenario(),)), tmp_path / "failed")
    result = read(tmp_path / "failed/summary.json")
    assert isinstance(result, SuiteResult)
    assert isinstance(result.status, Failed)
    assert not result.observations


def test_toml_cli_and_profile_are_real_entry_points(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = tmp_path / "run.toml"
    config.write_text("""measurement="latency"
initial_state="table"
warmup_operations=2
[workload]
operations=8
keys=2
key_bytes=1
value_bytes=2
put_fraction=0.0
get_fraction=1.0
scan_fraction=0.0
""")
    assert main(["run", "--config", str(config), "--output", str(tmp_path / "run.json")]) == 0
    assert main(["report", str(tmp_path / "run.json")]) == 0
    assert main(["check-config", "--config", str(config), "--schema"]) == 0
    assert "p99" in capsys.readouterr().out
    assert main(["run", "--config", str(config), "--output", str(tmp_path / "run.json")]) == 2
    result = profile(load(config, BenchmarkConfig), tmp_path / "profile")
    assert result.timing == "cprofile"
    assert (tmp_path / "profile/profile.prof").is_file()
    assert str(Path.cwd()).encode() not in (tmp_path / "profile/profile.prof").read_bytes()
    text = (tmp_path / "profile/profile.txt").read_text()
    assert "measurement.py" in text
    assert "validation.py" not in text
    suite_config = tmp_path / "suite.toml"
    suite_config.write_text("""repetitions=1
measurements=["throughput"]
captures=["counters"]
[[cases]]
name="tiny"
[cases.scenario.workload]
operations=2
keys=2
""")
    assert main(["suite", "--config", str(suite_config), "--output", str(tmp_path / "suite")]) == 0
    assert "Suite: complete" in capsys.readouterr().out


def test_duplicate_suite_axes_are_rejected() -> None:
    with pytest.raises(ValueError, match="unique"):
        SuiteConfig(cases=(_scenario(),), measurements=("throughput", "throughput"))
