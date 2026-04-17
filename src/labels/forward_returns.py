from __future__ import annotations

import numpy as np
import pandas as pd

from src.labels.base import BaseLabelBuilder, LabelContext
from src.labels.registry import LABEL_REGISTRY, LabelSpec


def _empty_label_panel(specs: list[LabelSpec]) -> pd.DataFrame:
    columns = [
        "rebalance_date",
        "trade_execution_date",
        "ts_code",
        "is_eligible",
        "exclude_reason",
        "label_start_date",
        "label_start_adj_close",
    ]
    for spec in specs:
        columns.extend(
            [
                f"{spec.output_field}_end_date",
                f"{spec.output_field}_end_adj_close",
                spec.output_field,
            ]
        )
    columns.extend(["year", "month"])
    return pd.DataFrame(columns=columns)


def _normalize_dates(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    output = df.copy()
    for column in columns:
        if column in output.columns:
            output[column] = pd.to_datetime(output[column], errors="coerce")
    return output


def _build_execution_schedule(monthly_universe_df: pd.DataFrame, specs: list[LabelSpec]) -> pd.DataFrame:
    schedule = monthly_universe_df.loc[:, ["rebalance_date", "trade_execution_date"]].drop_duplicates().copy()
    duplicated = schedule["rebalance_date"].duplicated(keep=False)
    if duplicated.any():
        raise ValueError("monthly_universe must map each rebalance_date to a single trade_execution_date.")
    schedule = schedule.sort_values("rebalance_date").reset_index(drop=True)
    for spec in specs:
        schedule[f"{spec.output_field}_end_date"] = schedule["trade_execution_date"].shift(-spec.horizon_months)
    return schedule


def build_forward_return_label_panel(
    monthly_universe_df: pd.DataFrame,
    adjusted_price_df: pd.DataFrame,
    specs: list[LabelSpec],
) -> pd.DataFrame:
    if not specs:
        raise ValueError("At least one forward-return label spec is required.")
    if monthly_universe_df.empty:
        return _empty_label_panel(specs)

    for spec in specs:
        if spec.stage != "forward_return":
            raise ValueError(f"Unsupported label stage for forward-return builder: {spec.stage}")
        if spec.start_anchor != "trade_execution_date":
            raise ValueError(f"Unsupported start anchor for forward-return builder: {spec.start_anchor}")

    universe = _normalize_dates(monthly_universe_df, ["rebalance_date", "trade_execution_date"]).copy()
    prices = _normalize_dates(adjusted_price_df, ["trade_date"]).copy()

    prices = prices.loc[:, ["ts_code", "trade_date", "adj_close"]].copy()
    prices = prices.sort_values(["ts_code", "trade_date"]).drop_duplicates(subset=["ts_code", "trade_date"], keep="last")

    schedule = _build_execution_schedule(universe, specs)
    working = universe.merge(schedule, on=["rebalance_date", "trade_execution_date"], how="left")

    working["label_start_date"] = working["trade_execution_date"]
    start_prices = prices.rename(columns={"trade_date": "label_start_date", "adj_close": "label_start_adj_close"})
    working = working.merge(start_prices, on=["ts_code", "label_start_date"], how="left")

    for spec in specs:
        end_date_col = f"{spec.output_field}_end_date"
        end_price_col = f"{spec.output_field}_end_adj_close"
        return_col = spec.output_field

        end_prices = prices.rename(columns={"trade_date": end_date_col, "adj_close": end_price_col})
        working = working.merge(end_prices, on=["ts_code", end_date_col], how="left")

        valid = (
            working["label_start_adj_close"].notna()
            & working[end_price_col].notna()
            & (working["label_start_adj_close"] != 0)
        )
        working[return_col] = np.where(
            valid,
            working[end_price_col] / working["label_start_adj_close"] - 1.0,
            np.nan,
        )

    if "year" not in working.columns:
        working["year"] = working["rebalance_date"].dt.year
    if "month" not in working.columns:
        working["month"] = working["rebalance_date"].dt.month

    base_columns = [
        "rebalance_date",
        "trade_execution_date",
        "ts_code",
        "is_eligible",
        "exclude_reason",
        "label_start_date",
        "label_start_adj_close",
    ]
    label_columns: list[str] = []
    for spec in specs:
        label_columns.extend(
            [
                f"{spec.output_field}_end_date",
                f"{spec.output_field}_end_adj_close",
                spec.output_field,
            ]
        )

    ordered_columns = [column for column in [*base_columns, *label_columns, "year", "month"] if column in working.columns]
    return working.loc[:, ordered_columns].sort_values(["rebalance_date", "ts_code"]).reset_index(drop=True)


class ForwardReturnLabelBuilder(BaseLabelBuilder):
    name = "forward_return"

    def build(
        self,
        monthly_universe_df: pd.DataFrame,
        adjusted_price_df: pd.DataFrame,
        calendar_df: pd.DataFrame,
        context: LabelContext,
    ) -> pd.DataFrame:
        del calendar_df
        selected_specs = LABEL_REGISTRY.list(
            names=context.label_names if context.label_names else None,
            stage="forward_return",
        )
        return build_forward_return_label_panel(monthly_universe_df, adjusted_price_df, selected_specs)
