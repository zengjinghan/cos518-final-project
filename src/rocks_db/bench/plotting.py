"""Simple plotting helpers for visualizing LSM benchmark results.

These helpers are intentionally minimal and generic: they operate on
Pandas DataFrames produced by the benchmark module and rely on standard
matplotlib plotting functions. The goal is to make quick, exploratory
plots easy to write while keeping the core engine free of plotting
dependencies.
"""

from __future__ import annotations

import pandas as pd
from matplotlib import pyplot as plt
from matplotlib.axes import Axes

__all__ = (
    "plot_metric_vs_param",
    "plot_latency_histogram",
)


def plot_metric_vs_param(
    df: pd.DataFrame,
    metric: str,
    param: str,
    hue: str | None = None,
    ax: Axes | None = None,
) -> Axes:
    """Plot a metric as a function of a configuration parameter.

    Args:
        df: Benchmark results, typically produced by `run_experiment_grid()`.
        metric: Column name of the metric to plot on the y-axis
            (e.g. "write_amp_logical", "throughput_ops_per_sec").
        param: Column name of the configuration parameter to plot on the x-axis
            (e.g. "write_buffer_size", "max_bytes_for_level_multiplier").
        hue: Optional column name used to group/colour separate lines, for
            example "compaction_mode".
        ax: If provided, plotting is done on this Axes; otherwise a new figure
            and Axes are created.

    Returns:
        The matplotlib Axes on which the plot was drawn.
    """
    if ax is None:
        _, ax = plt.subplots()

    if hue is None:
        # Simple case: one line, sorted by param.
        data = df.sort_values(param)
        ax.plot(data[param], data[metric], marker="o")
    else:
        # Group by hue and plot one line per group.
        grouped = df.groupby(hue)
        for hue_value, group in grouped:
            group_sorted = group.sort_values(param)
            ax.plot(
                group_sorted[param],
                group_sorted[metric],
                marker="o",
                label=str(hue_value),
            )
        ax.legend(title=hue)

    ax.set_xlabel(param)
    ax.set_ylabel(metric)
    ax.set_title(f"{metric} vs {param}")

    ax.grid(True, linestyle="--", linewidth=0.5, alpha=0.5)
    return ax


def plot_latency_histogram(
    df: pd.DataFrame,
    column: str,
    bins: int = 50,
    ax: Axes | None = None,
) -> Axes:
    """Plot a histogram of mean latency across experiment repetitions.

    Args:
        df: Benchmark results, typically produced by `run_experiment_grid()`.
        column: Column name of the latency metric to visualize, for example:
            - "mean_read_latency_s"
            - "mean_write_latency_s"
        bins: Number of histogram bins.
        ax: If provided, plotting is done on this Axes; otherwise a new figure
            and Axes are created.

    Returns:
        The matplotlib `Axes` on which the plot was drawn.
    """
    if ax is None:
        _, ax = plt.subplots()

    series = df[column].dropna()
    ax.hist(series, bins=bins)

    ax.set_xlabel(column)
    ax.set_ylabel("count")
    ax.set_title(f"Histogram of {column}")

    return ax
