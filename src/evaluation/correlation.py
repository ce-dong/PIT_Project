from __future__ import annotations

from itertools import combinations_with_replacement
from typing import Any

import numpy as np
import pandas as pd


def build_factor_correlation_tables(
    factor_panel_df: pd.DataFrame,
    *,
    factor_names: tuple[str, ...],
    factor_fields: tuple[str, ...],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    factor_view = factor_panel_df.loc[:, ["rebalance_date", *factor_fields]].copy()
    factor_view["rebalance_date"] = pd.to_datetime(factor_view["rebalance_date"], errors="coerce")

    if "is_eligible" in factor_panel_df.columns:
        factor_view = factor_view.loc[factor_panel_df["is_eligible"].fillna(False).astype(bool)].reset_index(drop=True)

    timeseries_records: list[dict[str, Any]] = []
    factor_pairs = list(zip(factor_names, factor_fields, strict=True))

    for rebalance_date, group in factor_view.groupby("rebalance_date", sort=True):
        month_data = group.loc[:, list(factor_fields)].apply(pd.to_numeric, errors="coerce")
        if month_data.empty:
            continue

        correlation_matrix = month_data.corr(method="pearson")
        valid_counts = month_data.notna().astype("int64").T.dot(month_data.notna().astype("int64"))
        universe_size = len(month_data)

        for (left_name, left_field), (right_name, right_field) in combinations_with_replacement(factor_pairs, 2):
            timeseries_records.append(
                {
                    "rebalance_date": rebalance_date,
                    "left_factor_name": left_name,
                    "left_factor_field": left_field,
                    "right_factor_name": right_name,
                    "right_factor_field": right_field,
                    "correlation": correlation_matrix.loc[left_field, right_field],
                    "pair_observation_count": int(valid_counts.loc[left_field, right_field]),
                    "universe_size": universe_size,
                }
            )

    correlation_timeseries = pd.DataFrame(timeseries_records)
    summary_columns = [
        "left_factor_name",
        "left_factor_field",
        "right_factor_name",
        "right_factor_field",
        "observation_months",
        "mean_correlation",
        "std_correlation",
        "mean_abs_correlation",
        "max_abs_correlation",
        "mean_pair_observation_count",
        "mean_universe_size",
    ]
    matrix_columns = ["factor_name", *factor_names]

    if correlation_timeseries.empty:
        return (
            correlation_timeseries,
            pd.DataFrame(columns=summary_columns),
            pd.DataFrame(columns=matrix_columns),
        )

    correlation_timeseries = correlation_timeseries.sort_values(
        ["left_factor_name", "right_factor_name", "rebalance_date"]
    ).reset_index(drop=True)

    summary_records: list[dict[str, Any]] = []
    grouped = correlation_timeseries.groupby(
        ["left_factor_name", "left_factor_field", "right_factor_name", "right_factor_field"],
        sort=True,
    )
    for (left_name, left_field, right_name, right_field), group in grouped:
        correlations = group["correlation"].dropna()
        summary_records.append(
            {
                "left_factor_name": left_name,
                "left_factor_field": left_field,
                "right_factor_name": right_name,
                "right_factor_field": right_field,
                "observation_months": int(len(correlations)),
                "mean_correlation": correlations.mean() if not correlations.empty else float("nan"),
                "std_correlation": correlations.std(ddof=1) if len(correlations) > 1 else float("nan"),
                "mean_abs_correlation": correlations.abs().mean() if not correlations.empty else float("nan"),
                "max_abs_correlation": correlations.abs().max() if not correlations.empty else float("nan"),
                "mean_pair_observation_count": group["pair_observation_count"].mean(),
                "mean_universe_size": group["universe_size"].mean(),
            }
        )

    correlation_summary = pd.DataFrame(summary_records, columns=summary_columns).sort_values(
        ["left_factor_name", "right_factor_name"]
    ).reset_index(drop=True)

    matrix = pd.DataFrame(np.nan, index=factor_names, columns=factor_names, dtype="float64")
    for row in correlation_summary.itertuples(index=False):
        matrix.loc[row.left_factor_name, row.right_factor_name] = row.mean_correlation
        matrix.loc[row.right_factor_name, row.left_factor_name] = row.mean_correlation

    correlation_matrix = matrix.reset_index(names="factor_name")
    return correlation_timeseries, correlation_summary, correlation_matrix.loc[:, matrix_columns]
