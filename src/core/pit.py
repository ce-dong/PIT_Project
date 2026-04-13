from __future__ import annotations

import pandas as pd


def _empty_monthly_snapshot_base() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "rebalance_date",
            "trade_execution_date",
            "ts_code",
            "exchange",
            "market",
            "list_date",
            "delist_date",
            "is_eligible",
            "exclude_reason",
            "days_since_list",
            "valid_trade_days_20d",
            "median_amount_20d",
            "has_price_coverage",
            "is_st_flag",
            "is_suspended_flag",
            "price_trade_date",
            "daily_basic_trade_date",
            "close",
            "adj_close",
            "total_mv",
            "circ_mv",
            "pb",
            "pe_ttm",
            "ps_ttm",
            "dv_ttm",
            "turnover_rate",
            "turnover_rate_f",
            "volume_ratio",
            "amount",
            "vol",
        ]
    )


def _asof_join_by_ts_code(
    left: pd.DataFrame,
    right: pd.DataFrame,
    *,
    left_time_col: str,
    right_time_col: str,
    right_value_cols: list[str],
    right_time_alias: str,
) -> pd.DataFrame:
    if left.empty:
        columns = ["ts_code", left_time_col, right_time_alias, *right_value_cols]
        return pd.DataFrame(columns=columns)

    left_frame = left[["ts_code", left_time_col]].copy()
    left_frame[left_time_col] = pd.to_datetime(left_frame[left_time_col], errors="coerce")
    left_frame = left_frame.sort_values([left_time_col, "ts_code"], kind="mergesort").reset_index(drop=True)

    right_frame = right[["ts_code", right_time_col, *right_value_cols]].copy()
    right_frame[right_time_col] = pd.to_datetime(right_frame[right_time_col], errors="coerce")
    right_frame = right_frame.dropna(subset=["ts_code", right_time_col])
    right_frame = right_frame.sort_values([right_time_col, "ts_code"], kind="mergesort").reset_index(drop=True)
    right_frame = right_frame.drop_duplicates(subset=["ts_code", right_time_col], keep="last")
    right_frame = right_frame.rename(columns={right_time_col: right_time_alias})
    right_frame = right_frame.sort_values([right_time_alias, "ts_code"], kind="mergesort").reset_index(drop=True)

    return pd.merge_asof(
        left_frame,
        right_frame,
        by="ts_code",
        left_on=left_time_col,
        right_on=right_time_alias,
        direction="backward",
        allow_exact_matches=True,
    )


def build_monthly_snapshot_base(
    monthly_universe: pd.DataFrame,
    adjusted_price_panel: pd.DataFrame,
    daily_basic: pd.DataFrame,
) -> pd.DataFrame:
    if monthly_universe.empty:
        return _empty_monthly_snapshot_base()

    universe = monthly_universe.copy()
    for column in ("rebalance_date", "trade_execution_date", "list_date", "delist_date"):
        if column in universe.columns:
            universe[column] = pd.to_datetime(universe[column], errors="coerce")

    price_snapshot = _asof_join_by_ts_code(
        universe,
        adjusted_price_panel,
        left_time_col="rebalance_date",
        right_time_col="trade_date",
        right_value_cols=["close", "adj_close", "amount", "vol"],
        right_time_alias="price_trade_date",
    )
    basic_snapshot = _asof_join_by_ts_code(
        universe,
        daily_basic,
        left_time_col="rebalance_date",
        right_time_col="trade_date",
        right_value_cols=[
            "total_mv",
            "circ_mv",
            "pb",
            "pe_ttm",
            "ps_ttm",
            "dv_ttm",
            "turnover_rate",
            "turnover_rate_f",
            "volume_ratio",
        ],
        right_time_alias="daily_basic_trade_date",
    )

    snapshot = universe.merge(
        price_snapshot,
        on=["ts_code", "rebalance_date"],
        how="left",
        validate="one_to_one",
    ).merge(
        basic_snapshot,
        on=["ts_code", "rebalance_date"],
        how="left",
        validate="one_to_one",
    )

    result = snapshot[
        [
            "rebalance_date",
            "trade_execution_date",
            "ts_code",
            "exchange",
            "market",
            "list_date",
            "delist_date",
            "is_eligible",
            "exclude_reason",
            "days_since_list",
            "valid_trade_days_20d",
            "median_amount_20d",
            "has_price_coverage",
            "is_st_flag",
            "is_suspended_flag",
            "price_trade_date",
            "daily_basic_trade_date",
            "close",
            "adj_close",
            "total_mv",
            "circ_mv",
            "pb",
            "pe_ttm",
            "ps_ttm",
            "dv_ttm",
            "turnover_rate",
            "turnover_rate_f",
            "volume_ratio",
            "amount",
            "vol",
        ]
    ].copy()
    result = result.sort_values(["rebalance_date", "ts_code"]).reset_index(drop=True)
    return result
