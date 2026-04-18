from __future__ import annotations

from typing import Any

import pandas as pd

from src.evaluation.base import BaseEvaluator, EvaluationContext


def build_evaluation_input(
    factor_panel_df: pd.DataFrame,
    label_panel_df: pd.DataFrame,
    *,
    factor_fields: tuple[str, ...],
    label_fields: tuple[str, ...],
) -> pd.DataFrame:
    factor_columns = ["rebalance_date", "ts_code", *factor_fields]
    if "is_eligible" in factor_panel_df.columns:
        factor_columns.append("is_eligible")

    factor_view = factor_panel_df.loc[:, factor_columns].copy()
    label_view = label_panel_df.loc[:, ["rebalance_date", "ts_code", *label_fields]].copy()

    factor_view["rebalance_date"] = pd.to_datetime(factor_view["rebalance_date"], errors="coerce")
    label_view["rebalance_date"] = pd.to_datetime(label_view["rebalance_date"], errors="coerce")

    merged = factor_view.merge(label_view, on=["rebalance_date", "ts_code"], how="inner", validate="one_to_one")
    if "is_eligible" in merged.columns:
        merged = merged.loc[merged["is_eligible"].fillna(False).astype(bool)].reset_index(drop=True)
    return merged


def _rank_correlation(df: pd.DataFrame, factor_field: str, label_field: str) -> tuple[float, int]:
    valid = df.loc[:, [factor_field, label_field]].dropna()
    if len(valid) < 2:
        return float("nan"), len(valid)

    factor_rank = valid[factor_field].rank(method="average")
    label_rank = valid[label_field].rank(method="average")
    return factor_rank.corr(label_rank), len(valid)


def build_rank_ic_tables(
    panel_df: pd.DataFrame,
    *,
    factor_names: tuple[str, ...],
    factor_fields: tuple[str, ...],
    label_names: tuple[str, ...],
    label_fields: tuple[str, ...],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    timeseries_records: list[dict[str, Any]] = []

    for factor_name, factor_field in zip(factor_names, factor_fields, strict=True):
        for label_name, label_field in zip(label_names, label_fields, strict=True):
            for rebalance_date, group in panel_df.groupby("rebalance_date", sort=True):
                rank_ic, universe_size = _rank_correlation(group, factor_field, label_field)
                timeseries_records.append(
                    {
                        "rebalance_date": rebalance_date,
                        "factor_name": factor_name,
                        "factor_field": factor_field,
                        "label_name": label_name,
                        "label_field": label_field,
                        "rank_ic": rank_ic,
                        "universe_size": universe_size,
                    }
                )

    ic_timeseries = pd.DataFrame(timeseries_records)
    if ic_timeseries.empty:
        summary_columns = [
            "factor_name",
            "factor_field",
            "label_name",
            "label_field",
            "observation_months",
            "ic_mean",
            "ic_std",
            "icir",
            "ic_hit_rate",
            "mean_universe_size",
        ]
        return ic_timeseries, pd.DataFrame(columns=summary_columns)

    ic_timeseries = ic_timeseries.sort_values(["factor_name", "label_name", "rebalance_date"]).reset_index(drop=True)

    summary_records: list[dict[str, Any]] = []
    grouped = ic_timeseries.groupby(["factor_name", "factor_field", "label_name", "label_field"], sort=True)
    for (factor_name, factor_field, label_name, label_field), group in grouped:
        ic_values = group["rank_ic"].dropna()
        ic_mean = ic_values.mean() if not ic_values.empty else float("nan")
        ic_std = ic_values.std(ddof=1) if len(ic_values) > 1 else float("nan")
        icir = ic_mean / ic_std if pd.notna(ic_mean) and pd.notna(ic_std) and ic_std != 0 else float("nan")
        ic_hit_rate = (ic_values > 0).mean() if not ic_values.empty else float("nan")
        summary_records.append(
            {
                "factor_name": factor_name,
                "factor_field": factor_field,
                "label_name": label_name,
                "label_field": label_field,
                "observation_months": int(ic_values.shape[0]),
                "ic_mean": ic_mean,
                "ic_std": ic_std,
                "icir": icir,
                "ic_hit_rate": ic_hit_rate,
                "mean_universe_size": group["universe_size"].mean(),
            }
        )

    ic_summary = pd.DataFrame(summary_records).sort_values(["factor_name", "label_name"]).reset_index(drop=True)
    return ic_timeseries, ic_summary


class RankICEvaluator(BaseEvaluator):
    name = "rank_ic"

    def evaluate(self, panel_df: pd.DataFrame, context: EvaluationContext) -> dict[str, Any]:
        ic_timeseries, ic_summary = build_rank_ic_tables(
            panel_df,
            factor_names=context.factor_names,
            factor_fields=context.factor_fields,
            label_names=context.label_names,
            label_fields=context.label_fields,
        )
        return {
            "ic_timeseries": ic_timeseries,
            "ic_summary": ic_summary,
        }
