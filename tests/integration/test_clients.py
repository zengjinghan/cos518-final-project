import json
from pathlib import Path

import pytest

from lsm_bench.config import BenchmarkConfig, HotSet, WorkloadConfig, Zipf
from lsm_bench.result import LatencyCapture, NoSamples
from lsm_bench.runner import run
from lsm_bench.workload import generate
from lsm_store import Database, StoreConfig
from lsm_store.cli import main
from lsm_store.errors import ConfigurationError


def test_cli_uses_persisted_config_and_independent_capture(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "store"
    config = tmp_path / "config.toml"
    config.write_text("memtable_bytes = 1234\n", encoding="utf-8")
    assert main(["put", str(path), "hello", "world", "--config", str(config)]) == 0
    assert main(["get", str(path), "hello", "--capture", "off"]) == 0
    assert "776f726c64" in capsys.readouterr().out
    assert main(["get", str(path), "missing"]) == 3
    assert main(["scan", str(path), "a", "z"]) == 0
    assert main(["delete", str(path), "hello"]) == 0
    assert main(["get", str(path), "hello"]) == 3
    assert main(["flush", str(path)]) == 0
    capsys.readouterr()
    assert main(["inspect", str(path)]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["table_space_amplification"]["value"] is None
    assert main(["get", str(tmp_path / "absent"), "key"]) == 1
    assert not (tmp_path / "absent").exists()


def test_invalid_cli_inputs_have_no_store_effects(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "store"
    assert main(["put", str(path), "key", "value", "--memtable-bytes", "0"]) == 2
    assert not path.exists()
    assert "memtable_bytes" in capsys.readouterr().err
    assert main(["put", str(path), "\ud800", "value"]) == 2
    assert not path.exists()
    assert "UTF-8" in capsys.readouterr().err
    assert main(["check-config", "--capture", "off"]) == 0
    assert '"instrumentation": "off"' in capsys.readouterr().out
    assert main(["check-config", "--schema"]) == 0
    assert '"discriminator"' in capsys.readouterr().out


@pytest.mark.parametrize("distribution", [HotSet(), Zipf()])
def test_workload_determinism_and_exact_population(distribution: HotSet | Zipf) -> None:
    config = WorkloadConfig(operations=40, keys=20, distribution=distribution)
    assert tuple(generate(config)) == tuple(generate(config))
    assert len(tuple(generate(config))) == 40


def test_benchmark_uses_separate_validated_windows(tmp_path: Path) -> None:
    config = BenchmarkConfig(
        store=StoreConfig(memtable_bytes=200, table_bytes=200, block_bytes=64),
        workload=WorkloadConfig(operations=80, keys=20, key_bytes=2, value_bytes=4),
    )
    result = run(config, parent=tmp_path)
    assert result.correctness
    assert result.operations == 80
    logical = result.operations_window.logical
    assert logical.puts + logical.deletes + logical.gets + logical.scans == 80
    assert result.reference_validation_ns > 0
    assert result.post_validation_ns > 0
    assert result.source.source_sha256
    assert result.model_dump_json()
    assert not list(tmp_path.iterdir())


def test_read_only_empty_samples_and_invalid_trace_contract() -> None:
    result = run(
        BenchmarkConfig(
            measurement="latency",
            workload=WorkloadConfig(
                operations=4,
                keys=2,
                put_fraction=0.0,
                get_fraction=1.0,
                scan_fraction=0.0,
            ),
        )
    )
    assert isinstance(result.capture, LatencyCapture)
    assert isinstance(result.capture.puts, NoSamples)
    assert isinstance(result.capture.deletes, NoSamples)
    assert isinstance(result.capture.scans, NoSamples)
    assert result.amplification.all_write_bytes.value is None
    with pytest.raises(ValueError, match="keyspace"):
        WorkloadConfig(keys=257, key_bytes=1)
    with pytest.raises(ValueError, match="fractions"):
        WorkloadConfig(put_fraction=0.9, get_fraction=0.9)
    with pytest.raises(ConfigurationError):
        Database.open(Path("unused"), StoreConfig(), capture="invalid")  # pyright: ignore[reportArgumentType] # Exercise a runtime boundary.
