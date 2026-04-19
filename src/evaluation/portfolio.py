from __future__ import annotations

from typing import Any

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


def build_quantile_portfolio_tables(
    panel_df: pd.DataFrame,
    *,
    factor_names: tuple[str, ...],
    factor_fields: tuple[str, ...],
    label_names: tuple[str, ...],
    label_fields: tuple[str, ...],
    quantile_count: int = 5,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    quantile_records: list[dict[str, Any]] = []

    for factor_name, factor_field in zip(factor_names, factor_fields, strict=True):
        for label_name, label_field in zip(label_names, label_fields, strict=True):
            for rebalance_date, group in panel_df.groupby("rebalance_date", sort=True):
                valid = group.loc[:, [factor_field, label_field]].dropna().copy()
                if len(valid) < quantile_count:
                    continue

                valid["quantile"] = _assign_quantile_buckets(valid[factor_field], quantile_count)
                valid = valid.dropna(subset=["quantile"]).copy()
                if valid.empty:
                    continue

                quantile_returns = valid.groupby("quantile", sort=True)[label_field].mean()
                universe_size = len(valid)
                for quantile, quantile_return in quantile_returns.items():
                    quantile_records.append(
                        {
                            "rebalance_date": rebalance_date,
                            "factor_name": factor_name,
                            "factor_field": factor_field,
                            "label_name": label_name,
                            "label_field": label_field,
                            "quantile": int(quantile),
                            "quantile_return": quantile_return,
                            "universe_size": universe_size,
                        }
                    )

    quantile_timeseries = pd.DataFrame(quantile_records)
    quantile_summary_columns = [
        "factor_name",
        "factor_field",
        "label_name",
        "label_field",
        "quantile",
        "observation_months",
        "mean_return",
        "std_return",
        "return_hit_rate",
        "mean_universe_size",
    ]
    spread_summary_columns = [
        "factor_name",
        "factor_field",
        "label_name",
        "label_field",
        "observation_months",
        "spread_mean",
        "spread_std",
        "spread_ir",
        "spread_hit_rate",
        "mean_universe_size",
    ]
    if quantile_timeseries.empty:
        return (
            quantile_timeseries,
            pd.DataFrame(columns=quantile_summary_columns),
            pd.DataFrame(
                columns=[
                    "rebalance_date",
                    "factor_name",
                    "factor_field",
                    "label_name",
                    "label_field",
                    "top_quantile",
                    "bottom_quantile",
                    "top_bottom_spread",
                    "universe_size",
                ]
            ),
            pd.DataFrame(columns=spread_summary_columns),
        )

    quantile_timeseries = quantile_timeseries.sort_values(
        ["factor_name", "label_name", "quantile", "rebalance_date"]
    ).reset_index(drop=True)

    quantile_summary = (
        quantile_timeseries.groupby(
            ["factor_name", "factor_field", "label_name", "label_field", "quantile"],
            sort=True,
        )
        .agg(
            observation_months=("quantile_return", "count"),
            mean_return=("quantile_return", "mean"),
            std_return=("quantile_return", lambda values: values.std(ddof=1)),
            return_hit_rate=("quantile_return", lambda values: (values > 0).mean()),
            mean_universe_size=("universe_size", "mean"),
        )
        .reset_index()
    )

    spread_source = quantile_timeseries.loc[
        quantile_timeseries["quantile"].isin([1, quantile_count]),
        [
            "rebalance_date",
            "factor_name",
            "factor_field",
            "label_name",
            "label_field",
            "quantile",
            "quantile_return",
            "universe_size",
        ],
    ].copy()
    top_bottom_spread = (
        spread_source.pivot_table(
            index=["rebalance_date", "factor_name", "factor_field", "label_name", "label_field"],
            columns="quantile",
            values=["quantile_return", "universe_size"],
            aggfunc="first",
        )
        .reset_index()
    )
    top_bottom_spread.columns = [
        "rebalance_date",
        "factor_name",
        "factor_field",
        "label_name",
        "label_field",
        "bottom_quantile_return",
        "top_quantile_return",
        "bottom_universe_size",
        "top_universe_size",
    ]
    top_bottom_spread["top_quantile"] = quantile_count
    top_bottom_spread["bottom_quantile"] = 1
    top_bottom_spread["top_bottom_spread"] = (
        top_bottom_spread["top_quantile_return"] - top_bottom_spread["bottom_quantile_return"]
    )
    top_bottom_spread["universe_size"] = (
        top_bottom_spread["top_universe_size"].fillna(top_bottom_spread["bottom_universe_size"])
    )
    top_bottom_spread = top_bottom_spread.loc[
        :,
        [
            "rebalance_date",
            "factor_name",
            "factor_field",
            "label_name",
            "label_field",
            "top_quantile",
            "bottom_quantile",
            "top_bottom_spread",
            "universe_size",
        ],
    ].sort_values(["factor_name", "label_name", "rebalance_date"]).reset_index(drop=True)

    spread_summary = (
        top_bottom_spread.groupby(["factor_name", "factor_field", "label_name", "label_field"], sort=True)
        .agg(
            observation_months=("top_bottom_spread", "count"),
            spread_mean=("top_bottom_spread", "mean"),
            spread_std=("top_bottom_spread", lambda values: values.std(ddof=1)),
            spread_hit_rate=("top_bottom_spread", lambda values: (values > 0).mean()),
            mean_universe_size=("universe_size", "mean"),
        )
        .reset_index()
    )
    spread_summary["spread_ir"] = spread_summary["spread_mean"] / spread_summary["spread_std"]
    spread_summary.loc[spread_summary["spread_std"].isna() | (spread_summary["spread_std"] == 0), "spread_ir"] = pd.NA
    spread_summary = spread_summary.loc[:, spread_summary_columns].sort_values(["factor_name", "label_name"]).reset_index(drop=True)

    return quantile_timeseries, quantile_summary, top_bottom_spread, spread_summary
