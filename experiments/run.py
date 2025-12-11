"""Entry point for running an LSM benchmark.

Responsibilities:
- Parse a base ExperimentConfig YAML and an optional grid/sweep YAML.
- Expand them into concrete ExperimentConfig objects via the grid generator.
- Run each experiment (with repetitions) using run_single_experiment().
- Aggregate all RunResult objects into a BenchmarkResult.
- Persist results incrementally after each experiment for crash safety.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml
from rich import print
from rich.progress import track
from rich.table import Table

from rocks_db.bench.benchmark_results import (
    BenchmarkMetadata,
    BenchmarkResult,
    RunResultList,
)
from rocks_db.bench.config_schema import ExperimentConfig, GridConfig
from rocks_db.bench.grid import generate_experiment_configs
from rocks_db.bench.runner import run_single_experiment


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the benchmark runner."""
    parser = argparse.ArgumentParser(description="LSM benchmark runner")

    # Base configuration: a single fully-specified ExperimentConfig.
    parser.add_argument(
        "-b",
        "--base-config",
        dest="base_config_path",
        type=Path,
        help="Path to a YAML base experiment configuration file (ExperimentConfig).",
    )

    # Optional grid / sweep configuration layered on top of the base.
    parser.add_argument(
        "-g",
        "--grid-config",
        dest="grid_config_path",
        type=Path,
        default=None,
        help="Optional YAML grid configuration file (GridConfig) to sweep over.",
    )

    # Output directory for results (JSON + CSV).
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=None,
        help=(
            "Directory to store benchmark results."
            " If not given, ./results/<timestamp>__<base-config-file>[__<grid-config-file>]/ is used."
        ),
    )

    return parser.parse_args()


def process_args(args: argparse.Namespace) -> argparse.Namespace:
    """Normalize and resolve CLI arguments (paths etc.)."""
    args.base_config_path = args.base_config_path.expanduser().resolve()

    if args.grid_config_path is not None:
        args.grid_config_path = args.grid_config_path.expanduser().resolve()

    if args.output_dir is not None:
        args.output_dir = args.output_dir.expanduser().resolve()

    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

    if args.output_dir is None:
        suffix = args.base_config_path.stem
        if args.grid_config_path is not None:
            suffix = f"{suffix}__{args.grid_config_path.stem}"

        args.output_dir = (Path("results") / f"{timestamp}__{suffix}").resolve()

    return args


def main() -> None:
    args = parse_args()
    args = process_args(args)

    # Load base experiment config (single ExperimentConfig from YAML).
    base_raw = yaml.safe_load(args.base_config_path.read_text(encoding="utf-8"))
    base_config = ExperimentConfig(**base_raw)

    # Load grid config (optional). If omitted, use an empty GridConfig,
    # which means "no sweeps; just run the base experiment".
    if args.grid_config_path is not None:
        grid_raw = yaml.safe_load(args.grid_config_path.read_text(encoding="utf-8"))
        grid_config = GridConfig(**grid_raw)
    else:
        grid_config = GridConfig()

    # Fail fast if directory already exists (to avoid overwriting runs).
    args.output_dir.mkdir(parents=True, exist_ok=False)

    results_json_path = args.output_dir / "results.json"
    runs_csv_path = args.output_dir / "runs.csv"

    # Initialize metadata and global result container.
    start_ts = time.time()

    metadata = BenchmarkMetadata(
        name=base_config.name,
        args=vars(args),
        start_timestamp=start_ts,
        end_timestamp=start_ts,
        output_path=str(results_json_path),
    )

    benchmark_result = BenchmarkResult(metadata=metadata, runs=[])

    # Generate concrete ExperimentConfig objects from base + grid,
    # run them one-by-one, and persist results incrementally.
    for experiment_config in track(list(generate_experiment_configs(base_config, grid_config))):
        runs = run_single_experiment(experiment_config)
        benchmark_result.runs.extend(runs)

        # Persist incrementally (crash-safe): JSON + CSV.
        with open(results_json_path, "w", encoding="utf-8") as f:
            json.dump(benchmark_result.model_dump(mode="json"), f, indent="\t")

        df_runs = pd.DataFrame(RunResultList.dump_python(benchmark_result.runs))
        df_runs.drop(columns=["config"]).to_csv(runs_csv_path, index=False)

        # Console progress output for quick feedback.
        print(f"\nExperiment: {experiment_config.name}")
        for run in runs:
            result_row = {
                "Rep": f"{run.repetition_id}",
                "Success": f"[green]{run.is_success}[/green]" if run.is_success else f"[red]{run.is_success}[/red]",
                "Elapsed\nTime (s)": f"{run.elapsed_ns * 1e-9:.3f}",
                "Memtable\nBytes": f"{run.config.lsm.write_buffer_size:_}",
                "Level\nBytes\nMulti": f"{run.config.lsm.max_bytes_for_level_multiplier}",
                "Bloom\nFilter": f"{run.config.lsm.bloom_filter.type.value}",
                "Compaction": f"{run.config.compaction.type.value}",
                "Key\nRange": f"{run.config.workload.key_range:_}",
                "#Ops": f"{run.config.workload.n_ops:_}",
                "Tput\n(ops/s)": f"{run.throughput_ops_per_sec:_.0f}",
                "Read Amp\nLogical": f"{run.read_amplification_logical:.2f}",
                "Write Amp\nLogical": f"{run.write_amplification_logical:.2f}",
                "Space\nAmp": f"{run.space_amplification:.2f}",
                "P50 Read\nLat (us)": f"{run.p50_read_latency_ns * 1e-3:.2f}",
                "P99 Write\nLat (us)": f"{run.p99_write_latency_ns * 1e-3:.2f}",
            }
            table = Table(*result_row.keys())
            table.add_row(*result_row.values())

            print(table)
            print("Level sizes (files): %s" % {f"L{lvl}": size for lvl, size in enumerate(run.level_sizes)})
            print("Level sizes (bytes): %s" % {f"L{lvl}": size for lvl, size in enumerate(run.level_sizes_bytes)})
            print(f"SSTable Total Keys: {run.sstable_num_keys}")
            print(f"SSTable Unique Keys: {run.sstable_num_unique_keys}")

    # Final timestamp + success flag + final write.
    benchmark_result.metadata.end_timestamp = time.time()
    benchmark_result.metadata.is_success = all(run.is_success for run in benchmark_result.runs)

    with open(results_json_path, "w", encoding="utf-8") as f:
        json.dump(benchmark_result.model_dump(mode="json"), f, indent="\t")

    df_runs = pd.DataFrame(RunResultList.dump_python(benchmark_result.runs))
    df_runs.drop(columns=["config"]).to_csv(runs_csv_path, index=False)

    print(f"\nSaved results to {args.output_dir}")


if __name__ == "__main__":
    main()
