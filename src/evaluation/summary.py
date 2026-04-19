from __future__ import annotations

import numpy as np
import pandas as pd


_GROUP_KEYS = ["factor_name", "factor_field", "label_name", "label_field"]


def _is_monotonic(values: pd.Series, *, direction: str, tolerance: float = 1e-12) -> bool:
    diffs = values.diff().dropna()
    if diffs.empty:
        return False
    if direction == "high_minus_low":
        return bool((diffs >= -tolerance).all())
    if direction == "low_minus_high":
        return bool((diffs <= tolerance).all())
    raise ValueError(f"Unsupported monotonicity direction: {direction}")


def _spearman_correlation(left: pd.Series, right: pd.Series) -> float:
    valid = pd.DataFrame({"left": left, "right": right}).dropna()
    if len(valid) < 2:
        return float("nan")
    left_rank = valid["left"].rank(method="average")
    right_rank = valid["right"].rank(method="average")
    return left_rank.corr(right_rank)


def build_monotonicity_summary(
    quantile_timeseries: pd.DataFrame,
    quantile_summary: pd.DataFrame,
    *,
    quantile_count: int,
) -> pd.DataFrame:
    summary_columns = [
        "factor_name",
        "factor_field",
        "label_name",
        "label_field",
        "preferred_direction",
        "observation_months",
        "mean_return_spearman",
        "mean_is_monotonic",
        "monotonic_hit_rate",
        "average_step_return",
    ]
    if quantile_summary.empty or quantile_timeseries.empty:
        return pd.DataFrame(columns=summary_columns)

    quantile_curve = (
        quantile_summary.loc[:, [*_GROUP_KEYS, "quantile", "mean_return"]]
        .sort_values([*_GROUP_KEYS, "quantile"])
        .reset_index(drop=True)
    )
    timeseries_curve = (
        quantile_timeseries.loc[:, [*_GROUP_KEYS, "rebalance_date", "quantile", "quantile_return"]]
        .sort_values([*_GROUP_KEYS, "rebalance_date", "quantile"])
        .reset_index(drop=True)
    )

    records: list[dict[str, object]] = []
    for group_key, group in quantile_curve.groupby(_GROUP_KEYS, sort=True):
        ordered = group.sort_values("quantile").reset_index(drop=True)
        if len(ordered) != quantile_count:
            continue

        preferred_direction = "high_minus_low" if ordered["mean_return"].iloc[-1] >= ordered["mean_return"].iloc[0] else "low_minus_high"
        mean_return_spearman = _spearman_correlation(ordered["quantile"], ordered["mean_return"])
        mean_is_monotonic = _is_monotonic(ordered["mean_return"], direction=preferred_direction)
        diffs = ordered["mean_return"].diff().dropna()
        average_step_return = diffs.mean() if preferred_direction == "high_minus_low" else (-diffs).mean()

        month_curves = timeseries_curve
        for key_name, key_value in zip(_GROUP_KEYS, group_key, strict=True):
            month_curves = month_curves.loc[month_curves[key_name] == key_value]

        monotonic_months = 0
        observed_months = 0
        for _, month_group in month_curves.groupby("rebalance_date", sort=True):
            month_ordered = month_group.sort_values("quantile").reset_index(drop=True)
            if len(month_ordered) != quantile_count:
                continue
            observed_months += 1
            monotonic_months += int(_is_monotonic(month_ordered["quantile_return"], direction=preferred_direction))

        records.append(
            {
                "factor_name": group_key[0],
                "factor_field": group_key[1],
                "label_name": group_key[2],
                "label_field": group_key[3],
                "preferred_direction": preferred_direction,
                "observation_months": observed_months,
                "mean_return_spearman": mean_return_spearman,
                "mean_is_monotonic": mean_is_monotonic,
                "monotonic_hit_rate": monotonic_months / observed_months if observed_months > 0 else np.nan,
                "average_step_return": average_step_return,
            }
        )

    return pd.DataFrame(records, columns=summary_columns).sort_values(["factor_name", "label_name"]).reset_index(drop=True)


def build_evaluation_summary(
    ic_summary: pd.DataFrame,
    spread_summary: pd.DataFrame,
    monotonicity_summary: pd.DataFrame,
) -> pd.DataFrame:
    if ic_summary.empty:
        columns = [
            *_GROUP_KEYS,
            "observation_months",
            "ic_mean",
            "ic_std",
            "icir",
            "ic_hit_rate",
            "spread_mean",
            "spread_std",
            "spread_ir",
            "spread_hit_rate",
            "preferred_direction",
            "mean_return_spearman",
            "mean_is_monotonic",
            "monotonic_hit_rate",
            "average_step_return",
            "mean_universe_size",
        ]
        return pd.DataFrame(columns=columns)

    summary = ic_summary.loc[
        :,
        [*_GROUP_KEYS, "observation_months", "ic_mean", "ic_std", "icir", "ic_hit_rate", "mean_universe_size"],
    ].copy()
    summary = summary.rename(columns={"observation_months": "ic_observation_months"})

    spread_view = spread_summary.loc[
        :,
        [*_GROUP_KEYS, "observation_months", "spread_mean", "spread_std", "spread_ir", "spread_hit_rate"],
    ].copy()
    spread_view = spread_view.rename(columns={"observation_months": "spread_observation_months"})

    monotonicity_view = monotonicity_summary.loc[
        :,
        [
            *_GROUP_KEYS,
            "preferred_direction",
            "observation_months",
            "mean_return_spearman",
            "mean_is_monotonic",
            "monotonic_hit_rate",
            "average_step_return",
        ],
    ].copy()
    monotonicity_view = monotonicity_view.rename(columns={"observation_months": "monotonicity_observation_months"})

    summary = summary.merge(spread_view, on=_GROUP_KEYS, how="left")
    summary = summary.merge(monotonicity_view, on=_GROUP_KEYS, how="left")
    summary["observation_months"] = summary[
        ["ic_observation_months", "spread_observation_months", "monotonicity_observation_months"]
    ].max(axis=1)

    ordered_columns = [
        *_GROUP_KEYS,
        "observation_months",
        "ic_mean",
        "ic_std",
        "icir",
        "ic_hit_rate",
        "spread_mean",
        "spread_std",
        "spread_ir",
        "spread_hit_rate",
        "preferred_direction",
        "mean_return_spearman",
        "mean_is_monotonic",
        "monotonic_hit_rate",
        "average_step_return",
        "mean_universe_size",
    ]
    return summary.loc[:, ordered_columns].sort_values(["factor_name", "label_name"]).reset_index(drop=True)
