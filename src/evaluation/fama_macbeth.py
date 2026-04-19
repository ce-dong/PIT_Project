from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd


def build_fama_macbeth_tables(
    panel_df: pd.DataFrame,
    *,
    factor_names: tuple[str, ...],
    factor_fields: tuple[str, ...],
    label_names: tuple[str, ...],
    label_fields: tuple[str, ...],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    timeseries_records: list[dict[str, Any]] = []

    coefficient_names = ("intercept", *factor_names)
    coefficient_fields = ("intercept", *factor_fields)

    for label_name, label_field in zip(label_names, label_fields, strict=True):
        for rebalance_date, group in panel_df.groupby("rebalance_date", sort=True):
            valid = group.loc[:, [*factor_fields, label_field]].dropna().copy()
            if len(valid) < len(factor_fields) + 1:
                continue

            design_matrix = valid.loc[:, factor_fields].to_numpy(dtype="float64")
            design_matrix = np.column_stack([np.ones(len(valid)), design_matrix])
            response = valid[label_field].to_numpy(dtype="float64")

            coefficients, _, design_rank, _ = np.linalg.lstsq(design_matrix, response, rcond=None)
            full_rank = int(design_rank) == design_matrix.shape[1]

            for coefficient_name, coefficient_field, coefficient_value in zip(
                coefficient_names,
                coefficient_fields,
                coefficients,
                strict=True,
            ):
                timeseries_records.append(
                    {
                        "rebalance_date": rebalance_date,
                        "label_name": label_name,
                        "label_field": label_field,
                        "term_name": coefficient_name,
                        "term_field": coefficient_field,
                        "coefficient": float(coefficient_value),
                        "n_obs": int(len(valid)),
                        "regressor_count": int(len(factor_fields)),
                        "design_rank": int(design_rank),
                        "full_rank": full_rank,
                    }
                )

    coefficient_timeseries = pd.DataFrame(timeseries_records)
    summary_columns = [
        "label_name",
        "label_field",
        "term_name",
        "term_field",
        "observation_months",
        "coef_mean",
        "coef_std",
        "t_stat",
        "positive_ratio",
        "mean_n_obs",
        "full_rank_ratio",
    ]
    if coefficient_timeseries.empty:
        return coefficient_timeseries, pd.DataFrame(columns=summary_columns)

    coefficient_timeseries = coefficient_timeseries.sort_values(
        ["label_name", "term_name", "rebalance_date"]
    ).reset_index(drop=True)

    summary_records: list[dict[str, Any]] = []
    grouped = coefficient_timeseries.groupby(["label_name", "label_field", "term_name", "term_field"], sort=True)
    for (label_name, label_field, term_name, term_field), group in grouped:
        coefficients = group["coefficient"].dropna()
        coef_mean = coefficients.mean() if not coefficients.empty else float("nan")
        coef_std = coefficients.std(ddof=1) if len(coefficients) > 1 else float("nan")
        t_stat = float("nan")
        if pd.notna(coef_mean) and pd.notna(coef_std) and coef_std != 0:
            t_stat = coef_mean / (coef_std / math.sqrt(len(coefficients)))

        summary_records.append(
            {
                "label_name": label_name,
                "label_field": label_field,
                "term_name": term_name,
                "term_field": term_field,
                "observation_months": int(len(coefficients)),
                "coef_mean": coef_mean,
                "coef_std": coef_std,
                "t_stat": t_stat,
                "positive_ratio": (coefficients > 0).mean() if not coefficients.empty else float("nan"),
                "mean_n_obs": group["n_obs"].mean(),
                "full_rank_ratio": group["full_rank"].mean(),
            }
        )

    coefficient_summary = pd.DataFrame(summary_records, columns=summary_columns).sort_values(
        ["label_name", "term_name"]
    ).reset_index(drop=True)
    return coefficient_timeseries, coefficient_summary
