from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pandas as pd


def _load_matplotlib():
    try:
        cache_dir = Path(tempfile.gettempdir()) / "pit_project_matplotlib"
        cache_dir.mkdir(parents=True, exist_ok=True)
        os.environ.setdefault("MPLCONFIGDIR", str(cache_dir))
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "Report chart generation requires matplotlib. Install dependencies from requirements.txt first."
        ) from error
    return plt


def _save_bar_chart(df: pd.DataFrame, *, x: str, y: str, title: str, output_path: Path, color: str) -> None:
    plt = _load_matplotlib()
    figure, axis = plt.subplots(figsize=(10, 5))
    axis.bar(df[x], df[y], color=color)
    axis.set_title(title)
    axis.set_ylabel(y)
    axis.tick_params(axis="x", rotation=45, labelsize=9)
    figure.tight_layout()
    figure.savefig(output_path, dpi=160)
    plt.close(figure)


def _save_heatmap(matrix_df: pd.DataFrame, *, title: str, output_path: Path) -> None:
    plt = _load_matplotlib()
    value_df = matrix_df.set_index("factor_name")
    figure, axis = plt.subplots(figsize=(12, 10))
    image = axis.imshow(value_df.to_numpy(dtype="float64"), cmap="coolwarm", vmin=-1, vmax=1)
    axis.set_title(title)
    axis.set_xticks(range(len(value_df.columns)))
    axis.set_xticklabels(value_df.columns, rotation=90, fontsize=8)
    axis.set_yticks(range(len(value_df.index)))
    axis.set_yticklabels(value_df.index, fontsize=8)
    figure.colorbar(image, ax=axis, fraction=0.046, pad=0.04)
    figure.tight_layout()
    figure.savefig(output_path, dpi=180)
    plt.close(figure)


def generate_report_charts(payload: dict[str, object], output_dir: Path) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)

    evaluation_summary: pd.DataFrame = payload["evaluation_summary"]  # type: ignore[assignment]
    robustness_summary: pd.DataFrame = payload["robustness_summary"]  # type: ignore[assignment]
    factor_correlation_matrix: pd.DataFrame = payload["factor_correlation_matrix"]  # type: ignore[assignment]

    ic_chart_df = evaluation_summary.assign(display_name=evaluation_summary["factor_name"] + " | " + evaluation_summary["label_name"])
    ic_chart_df = ic_chart_df.sort_values("ic_mean", ascending=False).head(10)
    spread_chart_df = evaluation_summary.assign(display_name=evaluation_summary["factor_name"] + " | " + evaluation_summary["label_name"])
    spread_chart_df = spread_chart_df.sort_values("spread_mean", ascending=False).head(10)
    robustness_chart_df = robustness_summary.assign(display_name=robustness_summary["factor_name"] + " | " + robustness_summary["label_name"])
    robustness_chart_df = robustness_chart_df.sort_values("ic_sign_consistent_ratio", ascending=True).head(10)

    chart_paths = {
        "ic_leaderboard": output_dir / "ic_leaderboard.png",
        "spread_leaderboard": output_dir / "spread_leaderboard.png",
        "correlation_heatmap": output_dir / "factor_correlation_heatmap.png",
        "robustness_consistency": output_dir / "robustness_consistency.png",
    }

    _save_bar_chart(
        ic_chart_df,
        x="display_name",
        y="ic_mean",
        title="Top IC Signals",
        output_path=chart_paths["ic_leaderboard"],
        color="#1f77b4",
    )
    _save_bar_chart(
        spread_chart_df,
        x="display_name",
        y="spread_mean",
        title="Top Spread Signals",
        output_path=chart_paths["spread_leaderboard"],
        color="#2ca02c",
    )
    _save_heatmap(
        factor_correlation_matrix,
        title="Factor Correlation Heatmap",
        output_path=chart_paths["correlation_heatmap"],
    )
    _save_bar_chart(
        robustness_chart_df,
        x="display_name",
        y="ic_sign_consistent_ratio",
        title="Weakest IC Sign Consistency",
        output_path=chart_paths["robustness_consistency"],
        color="#d62728",
    )

    return {name: path.name for name, path in chart_paths.items()}
