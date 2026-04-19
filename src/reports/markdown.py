from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

from src.reports.base import BaseReportBuilder, ReportContext


def _format_metric(value: Any, digits: int = 4) -> str:
    if value is None or pd.isna(value):
        return "NA"
    if isinstance(value, (int,)):
        return str(value)
    return f"{float(value):.{digits}f}"


def _render_table(df: pd.DataFrame, columns: list[str]) -> list[str]:
    if df.empty:
        return ["No data available.", ""]

    header = "| " + " | ".join(columns) + " |"
    divider = "| " + " | ".join(["---"] * len(columns)) + " |"
    rows = [header, divider]
    for _, row in df.iterrows():
        cells = []
        for column in columns:
            value = row[column]
            cells.append(_format_metric(value) if isinstance(value, (float, int)) else str(value))
        rows.append("| " + " | ".join(cells) + " |")
    rows.append("")
    return rows


def render_research_report(payload: dict[str, Any], *, generated_at: datetime, context: ReportContext) -> str:
    evaluation_summary: pd.DataFrame = payload["evaluation_summary"]
    fama_summary: pd.DataFrame = payload["fama_macbeth_summary"]
    correlation_summary: pd.DataFrame = payload["factor_correlation_summary"]
    redundancy_summary: pd.DataFrame = payload["redundancy_summary"]
    robustness_summary: pd.DataFrame = payload["robustness_summary"]
    manifest: dict[str, Any] = payload["manifest"]

    top_ic = evaluation_summary.sort_values("ic_mean", ascending=False).head(5)
    weakest_ic = evaluation_summary.sort_values("ic_mean", ascending=True).head(5)
    top_spread = evaluation_summary.sort_values("spread_mean", ascending=False).head(5)
    top_fama = fama_summary.loc[fama_summary["term_name"] != "intercept"].sort_values("t_stat", ascending=False).head(5)
    high_corr = correlation_summary.loc[
        correlation_summary["left_factor_name"] != correlation_summary["right_factor_name"]
    ].sort_values("mean_abs_correlation", ascending=False).head(5)
    highest_redundancy = redundancy_summary.sort_values("mean_r2", ascending=False).head(5)
    weakest_robustness = robustness_summary.sort_values("ic_sign_consistent_ratio", ascending=True).head(5)

    lines: list[str] = []
    lines.append("# Factor Research Report")
    lines.append("")
    lines.append(f"- Experiment: `{context.experiment_slug}`")
    lines.append(f"- Generated at: `{generated_at.isoformat()}`")
    lines.append(f"- Factors covered: `{len(manifest.get('factor_names', []))}`")
    lines.append(f"- Labels covered: `{len(manifest.get('label_names', []))}`")
    lines.append("")

    lines.append("## Overview")
    lines.append("")
    lines.append(f"- Evaluation rows: `{len(evaluation_summary)}`")
    lines.append(f"- Fama-MacBeth rows: `{len(fama_summary)}`")
    lines.append(f"- Correlation pairs: `{len(correlation_summary)}`")
    lines.append(f"- Redundancy rows: `{len(redundancy_summary)}`")
    lines.append(f"- Robustness rows: `{len(robustness_summary)}`")
    lines.append("")

    lines.append("## Strongest IC Signals")
    lines.append("")
    lines.extend(
        _render_table(
            top_ic.loc[:, ["factor_name", "label_name", "ic_mean", "icir", "spread_mean", "mean_is_monotonic"]],
            ["factor_name", "label_name", "ic_mean", "icir", "spread_mean", "mean_is_monotonic"],
        )
    )

    lines.append("## Weakest IC Signals")
    lines.append("")
    lines.extend(
        _render_table(
            weakest_ic.loc[:, ["factor_name", "label_name", "ic_mean", "icir", "spread_mean", "mean_is_monotonic"]],
            ["factor_name", "label_name", "ic_mean", "icir", "spread_mean", "mean_is_monotonic"],
        )
    )

    lines.append("## Strongest Spreads")
    lines.append("")
    lines.extend(
        _render_table(
            top_spread.loc[:, ["factor_name", "label_name", "spread_mean", "spread_ir", "monotonic_hit_rate"]],
            ["factor_name", "label_name", "spread_mean", "spread_ir", "monotonic_hit_rate"],
        )
    )

    lines.append("## Fama-MacBeth Highlights")
    lines.append("")
    lines.extend(
        _render_table(
            top_fama.loc[:, ["term_name", "label_name", "coef_mean", "t_stat", "positive_ratio"]],
            ["term_name", "label_name", "coef_mean", "t_stat", "positive_ratio"],
        )
    )

    lines.append("## Highest Correlation Pairs")
    lines.append("")
    lines.extend(
        _render_table(
            high_corr.loc[:, ["left_factor_name", "right_factor_name", "mean_correlation", "mean_abs_correlation"]],
            ["left_factor_name", "right_factor_name", "mean_correlation", "mean_abs_correlation"],
        )
    )

    lines.append("## Highest Redundancy")
    lines.append("")
    lines.extend(
        _render_table(
            highest_redundancy.loc[:, ["factor_name", "label_name", "mean_r2", "residual_ic_mean", "residual_spread_mean"]],
            ["factor_name", "label_name", "mean_r2", "residual_ic_mean", "residual_spread_mean"],
        )
    )

    lines.append("## Weakest Robustness")
    lines.append("")
    lines.extend(
        _render_table(
            weakest_robustness.loc[:, ["factor_name", "label_name", "ic_sign_consistent_ratio", "spread_sign_consistent_ratio"]],
            ["factor_name", "label_name", "ic_sign_consistent_ratio", "spread_sign_consistent_ratio"],
        )
    )

    return "\n".join(lines).rstrip() + "\n"


class MarkdownResearchReportBuilder(BaseReportBuilder):
    name = "markdown"

    def build(self, payload: dict[str, Any], context: ReportContext) -> str:
        return render_research_report(payload, generated_at=datetime.now(timezone.utc), context=context)
