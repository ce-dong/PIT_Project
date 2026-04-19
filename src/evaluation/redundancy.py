from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd


def _assign_quantile_buckets(series: pd.Series, quantile_count: int) -> pd.Series:
    output = pd.Series(pd.NA, index=series.index, dtype="Int64")
    valid = series.dropna()
    if len(valid) < quantile_count:
        return output
    ranked = valid.rank(method="first")
    buckets = pd.qcut(ranked, q=quantile_count, labels=False) + 1
    output.loc[valid.index] = buckets.astype("int64")
    return output


def _rank_correlation(factor: pd.Series, label: pd.Series) -> tuple[float, int]:
    valid = pd.DataFrame({"factor": factor, "label": label}).dropna()
    if len(valid) < 2:
        return float("nan"), len(valid)
    return valid["factor"].rank(method="average").corr(valid["label"].rank(method="average")), len(valid)


def build_redundancy_tables(
    aligned_panel: pd.DataFrame,
    *,
    factor_names: tuple[str, ...],
    factor_fields: tuple[str, ...],
    label_names: tuple[str, ...],
    label_fields: tuple[str, ...],
    quantile_count: int = 5,
    evaluation_summary: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    timeseries_columns = [
        "rebalance_date",
        "factor_name",
        "factor_field",
        "label_name",
        "label_field",
        "residual_rank_ic",
        "residual_top_bottom_spread",
        "regression_r2",
        "residual_universe_size",
    ]
    summary_columns = [
        "factor_name",
        "factor_field",
        "label_name",
        "label_field",
        "observation_months",
        "mean_r2",
        "residual_ic_mean",
        "residual_ic_std",
        "residual_icir",
        "residual_ic_hit_rate",
        "residual_spread_mean",
        "residual_spread_std",
        "residual_spread_ir",
        "residual_spread_hit_rate",
        "mean_residual_universe_size",
        "residual_ic_retention",
        "residual_spread_retention",
    ]
    if len(factor_fields) <= 1:
        return pd.DataFrame(columns=timeseries_columns), pd.DataFrame(columns=summary_columns)

    working = aligned_panel.loc[:, ["rebalance_date", "ts_code", *factor_fields, *label_fields]].copy()
    working["rebalance_date"] = pd.to_datetime(working["rebalance_date"], errors="coerce")

    factor_pairs = list(zip(factor_names, factor_fields, strict=True))
    label_pairs = list(zip(label_names, label_fields, strict=True))
    timeseries_records: list[dict[str, Any]] = []

    for rebalance_date, month_group in working.groupby("rebalance_date", sort=True):
        month_group = month_group.reset_index(drop=True)
        for target_name, target_field in factor_pairs:
            other_fields = [field for field in factor_fields if field != target_field]
            if not other_fields:
                continue

            regression_sample = month_group.loc[:, ["ts_code", target_field, *other_fields]].dropna().copy()
            if len(regression_sample) < len(other_fields) + 1:
                continue

            y = regression_sample[target_field].to_numpy(dtype="float64")
            x = regression_sample.loc[:, other_fields].to_numpy(dtype="float64")
            x = np.column_stack([np.ones(len(regression_sample)), x])
            coefficients, _, _, _ = np.linalg.lstsq(x, y, rcond=None)
            fitted = x @ coefficients
            residuals = y - fitted

            sse = float(np.square(residuals).sum())
            centered = y - y.mean()
            sst = float(np.square(centered).sum())
            regression_r2 = 1.0 - sse / sst if sst > 0 else float("nan")

            residual_frame = regression_sample.loc[:, ["ts_code"]].copy()
            residual_frame["residual_factor"] = residuals

            for label_name, label_field in label_pairs:
                label_sample = month_group.loc[:, ["ts_code", label_field]].dropna().copy()
                evaluation_sample = residual_frame.merge(label_sample, on="ts_code", how="inner")
                residual_universe_size = len(evaluation_sample)
                residual_rank_ic = float("nan")
                residual_top_bottom_spread = float("nan")
                residual_std = evaluation_sample["residual_factor"].std(ddof=0)
                if pd.notna(residual_std) and residual_std > 1e-12:
                    residual_rank_ic, residual_universe_size = _rank_correlation(
                        evaluation_sample["residual_factor"],
                        evaluation_sample[label_field],
                    )
                    if len(evaluation_sample) >= quantile_count:
                        evaluation_sample["quantile"] = _assign_quantile_buckets(
                            evaluation_sample["residual_factor"],
                            quantile_count,
                        )
                        evaluation_sample = evaluation_sample.dropna(subset=["quantile"]).copy()
                        if not evaluation_sample.empty:
                            quantile_returns = evaluation_sample.groupby("quantile", sort=True)[label_field].mean()
                            top_return = quantile_returns.get(quantile_count)
                            bottom_return = quantile_returns.get(1)
                            if pd.notna(top_return) and pd.notna(bottom_return):
                                residual_top_bottom_spread = float(top_return - bottom_return)

                timeseries_records.append(
                    {
                        "rebalance_date": rebalance_date,
                        "factor_name": target_name,
                        "factor_field": target_field,
                        "label_name": label_name,
                        "label_field": label_field,
                        "residual_rank_ic": residual_rank_ic,
                        "residual_top_bottom_spread": residual_top_bottom_spread,
                        "regression_r2": regression_r2,
                        "residual_universe_size": residual_universe_size,
                    }
                )

    redundancy_timeseries = pd.DataFrame(timeseries_records, columns=timeseries_columns)
    if redundancy_timeseries.empty:
        return redundancy_timeseries, pd.DataFrame(columns=summary_columns)

    redundancy_timeseries = redundancy_timeseries.sort_values(
        ["factor_name", "label_name", "rebalance_date"]
    ).reset_index(drop=True)

    summary_records: list[dict[str, Any]] = []
    grouped = redundancy_timeseries.groupby(["factor_name", "factor_field", "label_name", "label_field"], sort=True)
    for (factor_name, factor_field, label_name, label_field), group in grouped:
        residual_ic = group["residual_rank_ic"].dropna()
        residual_spread = group["residual_top_bottom_spread"].dropna()
        residual_ic_mean = residual_ic.mean() if not residual_ic.empty else float("nan")
        residual_ic_std = residual_ic.std(ddof=1) if len(residual_ic) > 1 else float("nan")
        residual_spread_mean = residual_spread.mean() if not residual_spread.empty else float("nan")
        residual_spread_std = residual_spread.std(ddof=1) if len(residual_spread) > 1 else float("nan")

        summary_records.append(
            {
                "factor_name": factor_name,
                "factor_field": factor_field,
                "label_name": label_name,
                "label_field": label_field,
                "observation_months": int(group["rebalance_date"].nunique()),
                "mean_r2": group["regression_r2"].mean(),
                "residual_ic_mean": residual_ic_mean,
                "residual_ic_std": residual_ic_std,
                "residual_icir": residual_ic_mean / residual_ic_std if pd.notna(residual_ic_std) and residual_ic_std != 0 else float("nan"),
                "residual_ic_hit_rate": (residual_ic > 0).mean() if not residual_ic.empty else float("nan"),
                "residual_spread_mean": residual_spread_mean,
                "residual_spread_std": residual_spread_std,
                "residual_spread_ir": residual_spread_mean / residual_spread_std if pd.notna(residual_spread_std) and residual_spread_std != 0 else float("nan"),
                "residual_spread_hit_rate": (residual_spread > 0).mean() if not residual_spread.empty else float("nan"),
                "mean_residual_universe_size": group["residual_universe_size"].mean(),
                "residual_ic_retention": float("nan"),
                "residual_spread_retention": float("nan"),
            }
        )

    redundancy_summary = pd.DataFrame(summary_records, columns=summary_columns).sort_values(
        ["factor_name", "label_name"]
    ).reset_index(drop=True)

    if evaluation_summary is not None and not evaluation_summary.empty:
        reference = evaluation_summary.loc[
            :,
            ["factor_name", "factor_field", "label_name", "label_field", "ic_mean", "spread_mean"],
        ].copy()
        redundancy_summary = redundancy_summary.merge(
            reference,
            on=["factor_name", "factor_field", "label_name", "label_field"],
            how="left",
        )
        redundancy_summary["residual_ic_retention"] = redundancy_summary["residual_ic_mean"] / redundancy_summary["ic_mean"]
        redundancy_summary["residual_spread_retention"] = redundancy_summary["residual_spread_mean"] / redundancy_summary["spread_mean"]
        zero_ic = redundancy_summary["ic_mean"].isna() | (redundancy_summary["ic_mean"] == 0)
        zero_spread = redundancy_summary["spread_mean"].isna() | (redundancy_summary["spread_mean"] == 0)
        redundancy_summary.loc[zero_ic, "residual_ic_retention"] = np.nan
        redundancy_summary.loc[zero_spread, "residual_spread_retention"] = np.nan
        redundancy_summary = redundancy_summary.drop(columns=["ic_mean", "spread_mean"])

    return redundancy_timeseries, redundancy_summary
