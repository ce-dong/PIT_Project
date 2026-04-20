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
    subperiod_summary: pd.DataFrame = payload.get("subperiod_summary", pd.DataFrame())
    manifest: dict[str, Any] = payload["manifest"]
    factor_manifest: dict[str, Any] = payload.get("factor_manifest", {})
    chart_paths: dict[str, str] = payload.get("chart_paths", {})

    top_ic = evaluation_summary.sort_values("ic_mean", ascending=False).head(5)
    weakest_ic = evaluation_summary.sort_values("ic_mean", ascending=True).head(5)
    top_spread = evaluation_summary.sort_values("spread_mean", ascending=False).head(5)
    top_fama = fama_summary.loc[fama_summary["term_name"] != "intercept"].sort_values("t_stat", ascending=False).head(5)
    high_corr = correlation_summary.loc[
        correlation_summary["left_factor_name"] != correlation_summary["right_factor_name"]
    ].sort_values("mean_abs_correlation", ascending=False).head(5)
    highest_redundancy = redundancy_summary.sort_values("mean_r2", ascending=False).head(5)
    weakest_robustness = robustness_summary.sort_values("ic_sign_consistent_ratio", ascending=True).head(5)
    top_robustness = robustness_summary.sort_values(
        ["ic_sign_consistent_ratio", "spread_sign_consistent_ratio"],
        ascending=False,
    ).head(5)
    subperiod_highlights = subperiod_summary.sort_values(["ic_mean", "spread_mean"], ascending=False).head(6) if not subperiod_summary.empty else pd.DataFrame()

    top_ic_row = top_ic.iloc[0] if not top_ic.empty else None
    top_spread_row = top_spread.iloc[0] if not top_spread.empty else None
    top_fama_row = top_fama.iloc[0] if not top_fama.empty else None
    top_corr_row = high_corr.iloc[0] if not high_corr.empty else None
    top_robust_row = top_robustness.iloc[0] if not top_robustness.empty else None
    preprocess_profiles = factor_manifest.get("preprocess_profiles", {})
    unique_preprocess_profiles = sorted({" -> ".join(profile) for profile in preprocess_profiles.values() if profile})

    lines: list[str] = []
    lines.append("# Factor Research Report")
    lines.append("")
    lines.append(f"- Experiment: `{context.experiment_slug}`")
    lines.append(f"- Generated at: `{generated_at.isoformat()}`")
    lines.append(f"- Factors covered: `{len(manifest.get('factor_names', []))}`")
    lines.append(f"- Labels covered: `{len(manifest.get('label_names', []))}`")
    lines.append("")

    lines.append("## Key Takeaways")
    lines.append("")
    if top_ic_row is not None:
        lines.append(
            f"- Best IC signal: `{top_ic_row['factor_name']}` on `{top_ic_row['label_name']}` with `ic_mean={_format_metric(top_ic_row['ic_mean'])}` and `ICIR={_format_metric(top_ic_row['icir'])}`."
        )
    if top_spread_row is not None:
        lines.append(
            f"- Strongest spread: `{top_spread_row['factor_name']}` on `{top_spread_row['label_name']}` with `spread_mean={_format_metric(top_spread_row['spread_mean'])}` and `spread_ir={_format_metric(top_spread_row['spread_ir'])}`."
        )
    if top_robust_row is not None:
        lines.append(
            f"- Most stable factor: `{top_robust_row['factor_name']}` on `{top_robust_row['label_name']}` with `IC sign consistency={_format_metric(top_robust_row['ic_sign_consistent_ratio'])}`."
        )
    if top_fama_row is not None:
        lines.append(
            f"- Strongest cross-sectional pricing result: `{top_fama_row['term_name']}` on `{top_fama_row['label_name']}` with `t-stat={_format_metric(top_fama_row['t_stat'])}`."
        )
    if top_corr_row is not None:
        lines.append(
            f"- Highest factor overlap: `{top_corr_row['left_factor_name']}` vs `{top_corr_row['right_factor_name']}` with `|corr|={_format_metric(top_corr_row['mean_abs_correlation'])}`."
        )
    lines.append("")

    lines.append("## Overview")
    lines.append("")
    lines.append(f"- Evaluation rows: `{len(evaluation_summary)}`")
    lines.append(f"- Fama-MacBeth rows: `{len(fama_summary)}`")
    lines.append(f"- Correlation pairs: `{len(correlation_summary)}`")
    lines.append(f"- Redundancy rows: `{len(redundancy_summary)}`")
    lines.append(f"- Robustness rows: `{len(robustness_summary)}`")
    lines.append(f"- Subperiod rows: `{len(subperiod_summary)}`")
    lines.append("")

    lines.append("## Methodology Snapshot")
    lines.append("")
    lines.append("- Frequency: monthly cross-sectional rebalancing with strict point-in-time joins.")
    lines.append("- Inputs: `calendar_table`, `adjusted_price_panel`, `monthly_universe`, and `monthly_snapshot_base`.")
    lines.append(f"- Labels: `{', '.join(manifest.get('label_names', [])) or 'NA'}`.")
    if unique_preprocess_profiles:
        lines.append(f"- Preprocessing: `{'; '.join(unique_preprocess_profiles)}`.")
    else:
        lines.append("- Preprocessing: winsorization, neutralization, and z-score standardization.")
    lines.append("- Evaluation: Rank IC, quantile portfolio, top-bottom spread, monotonicity, Fama-MacBeth, correlation, redundancy, and robustness.")
    lines.append("")

    if chart_paths:
        lines.append("## Charts")
        lines.append("")
        if "ic_leaderboard" in chart_paths:
            lines.append(f"![IC Leaderboard]({chart_paths['ic_leaderboard']})")
            lines.append("")
        if "spread_leaderboard" in chart_paths:
            lines.append(f"![Spread Leaderboard]({chart_paths['spread_leaderboard']})")
            lines.append("")
        if "correlation_heatmap" in chart_paths:
            lines.append(f"![Factor Correlation Heatmap]({chart_paths['correlation_heatmap']})")
            lines.append("")
        if "robustness_consistency" in chart_paths:
            lines.append(f"![Robustness Consistency]({chart_paths['robustness_consistency']})")
            lines.append("")

    lines.append("## Experiment Outputs")
    lines.append("")
    lines.append("- Factor panel: `data/panel/<experiment_slug>/factor_panel/`")
    lines.append("- Label panel: `data/panel/<experiment_slug>/label_panel/`")
    lines.append("- Evaluation artifacts: `data/experiments/<experiment_slug>/evaluation/`")
    lines.append("- Report bundle: `data/experiments/<experiment_slug>/reports/`")
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

    lines.append("## Robustness Leaders")
    lines.append("")
    lines.extend(
        _render_table(
            top_robustness.loc[:, ["factor_name", "label_name", "ic_sign_consistent_ratio", "spread_sign_consistent_ratio"]],
            ["factor_name", "label_name", "ic_sign_consistent_ratio", "spread_sign_consistent_ratio"],
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

    if not subperiod_highlights.empty:
        lines.append("## Subperiod Highlights")
        lines.append("")
        lines.extend(
            _render_table(
                subperiod_highlights.loc[:, ["factor_name", "label_name", "period_label", "ic_mean", "spread_mean"]],
                ["factor_name", "label_name", "period_label", "ic_mean", "spread_mean"],
            )
        )

    return "\n".join(lines).rstrip() + "\n"


class MarkdownResearchReportBuilder(BaseReportBuilder):
    name = "markdown"

    def build(self, payload: dict[str, Any], context: ReportContext) -> str:
        return render_research_report(payload, generated_at=datetime.now(timezone.utc), context=context)
