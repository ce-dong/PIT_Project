from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd


def _build_period_table(rebalance_dates: pd.Series, period_count: int) -> pd.DataFrame:
    unique_dates = sorted(pd.to_datetime(rebalance_dates, errors="coerce").dropna().unique())
    if not unique_dates:
        return pd.DataFrame(columns=["period_id", "period_label", "period_start", "period_end", "month_count"])

    buckets = [bucket.tolist() for bucket in np.array_split(unique_dates, min(period_count, len(unique_dates))) if len(bucket) > 0]
    records: list[dict[str, Any]] = []
    for idx, bucket in enumerate(buckets, start=1):
        records.append(
            {
                "period_id": idx,
                "period_label": f"period_{idx}",
                "period_start": bucket[0],
                "period_end": bucket[-1],
                "month_count": len(bucket),
            }
        )
    return pd.DataFrame(records)


def _safe_ir(mean_value: float, std_value: float) -> float:
    if pd.isna(mean_value) or pd.isna(std_value) or std_value == 0:
        return float("nan")
    return mean_value / std_value


def build_subperiod_robustness_tables(
    ic_timeseries: pd.DataFrame,
    spread_timeseries: pd.DataFrame,
    evaluation_summary: pd.DataFrame,
    *,
    period_count: int = 3,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    period_table = _build_period_table(ic_timeseries["rebalance_date"], period_count)
    subperiod_columns = [
        "period_id",
        "period_label",
        "period_start",
        "period_end",
        "factor_name",
        "factor_field",
        "label_name",
        "label_field",
        "observation_months",
        "ic_mean",
        "ic_std",
        "icir",
        "spread_mean",
        "spread_std",
        "spread_ir",
        "spread_hit_rate",
    ]
    robustness_columns = [
        "factor_name",
        "factor_field",
        "label_name",
        "label_field",
        "period_count",
        "full_sample_ic_mean",
        "full_sample_spread_mean",
        "ic_sign_consistent_ratio",
        "spread_sign_consistent_ratio",
        "min_period_ic_mean",
        "max_period_ic_mean",
        "min_period_spread_mean",
        "max_period_spread_mean",
    ]
    if period_table.empty:
        return period_table, pd.DataFrame(columns=subperiod_columns), pd.DataFrame(columns=robustness_columns)

    ic_working = ic_timeseries.copy()
    spread_working = spread_timeseries.copy()
    ic_working["rebalance_date"] = pd.to_datetime(ic_working["rebalance_date"], errors="coerce")
    spread_working["rebalance_date"] = pd.to_datetime(spread_working["rebalance_date"], errors="coerce")

    subperiod_records: list[dict[str, Any]] = []
    group_keys = ["factor_name", "factor_field", "label_name", "label_field"]
    for period in period_table.itertuples(index=False):
        ic_slice = ic_working.loc[
            (ic_working["rebalance_date"] >= period.period_start) & (ic_working["rebalance_date"] <= period.period_end)
        ].copy()
        spread_slice = spread_working.loc[
            (spread_working["rebalance_date"] >= period.period_start)
            & (spread_working["rebalance_date"] <= period.period_end)
        ].copy()

        if ic_slice.empty:
            continue

        ic_agg = (
            ic_slice.groupby(group_keys, sort=True)
            .agg(
                observation_months=("rank_ic", lambda values: values.dropna().shape[0]),
                ic_mean=("rank_ic", "mean"),
                ic_std=("rank_ic", lambda values: values.std(ddof=1)),
            )
            .reset_index()
        )
        spread_agg = (
            spread_slice.groupby(group_keys, sort=True)
            .agg(
                spread_mean=("top_bottom_spread", "mean"),
                spread_std=("top_bottom_spread", lambda values: values.std(ddof=1)),
                spread_hit_rate=("top_bottom_spread", lambda values: (values > 0).mean()),
            )
            .reset_index()
        )
        merged = ic_agg.merge(spread_agg, on=group_keys, how="left")
        merged["icir"] = merged.apply(lambda row: _safe_ir(row["ic_mean"], row["ic_std"]), axis=1)
        merged["spread_ir"] = merged.apply(lambda row: _safe_ir(row["spread_mean"], row["spread_std"]), axis=1)
        merged["period_id"] = period.period_id
        merged["period_label"] = period.period_label
        merged["period_start"] = period.period_start
        merged["period_end"] = period.period_end
        subperiod_records.extend(merged.loc[:, subperiod_columns].to_dict("records"))

    subperiod_summary = pd.DataFrame(subperiod_records, columns=subperiod_columns)
    if subperiod_summary.empty:
        return period_table, subperiod_summary, pd.DataFrame(columns=robustness_columns)

    subperiod_summary = subperiod_summary.sort_values(
        ["factor_name", "label_name", "period_id"]
    ).reset_index(drop=True)

    reference = evaluation_summary.loc[
        :,
        ["factor_name", "factor_field", "label_name", "label_field", "ic_mean", "spread_mean"],
    ].rename(
        columns={
            "ic_mean": "full_sample_ic_mean",
            "spread_mean": "full_sample_spread_mean",
        }
    )
    robustness = subperiod_summary.merge(reference, on=group_keys, how="left")

    robustness_records: list[dict[str, Any]] = []
    for group_key, group in robustness.groupby(group_keys, sort=True):
        full_ic_mean = group["full_sample_ic_mean"].iloc[0]
        full_spread_mean = group["full_sample_spread_mean"].iloc[0]

        if pd.isna(full_ic_mean) or full_ic_mean == 0:
            ic_sign_consistent_ratio = float("nan")
        else:
            ic_sign_consistent_ratio = (np.sign(group["ic_mean"]) == np.sign(full_ic_mean)).mean()

        if pd.isna(full_spread_mean) or full_spread_mean == 0:
            spread_sign_consistent_ratio = float("nan")
        else:
            spread_sign_consistent_ratio = (np.sign(group["spread_mean"]) == np.sign(full_spread_mean)).mean()

        robustness_records.append(
            {
                "factor_name": group_key[0],
                "factor_field": group_key[1],
                "label_name": group_key[2],
                "label_field": group_key[3],
                "period_count": int(group["period_id"].nunique()),
                "full_sample_ic_mean": full_ic_mean,
                "full_sample_spread_mean": full_spread_mean,
                "ic_sign_consistent_ratio": ic_sign_consistent_ratio,
                "spread_sign_consistent_ratio": spread_sign_consistent_ratio,
                "min_period_ic_mean": group["ic_mean"].min(),
                "max_period_ic_mean": group["ic_mean"].max(),
                "min_period_spread_mean": group["spread_mean"].min(),
                "max_period_spread_mean": group["spread_mean"].max(),
            }
        )

    robustness_summary = pd.DataFrame(robustness_records, columns=robustness_columns).sort_values(
        ["factor_name", "label_name"]
    ).reset_index(drop=True)
    return period_table, subperiod_summary, robustness_summary
