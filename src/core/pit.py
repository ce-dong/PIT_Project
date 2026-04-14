from __future__ import annotations

import numpy as np
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
    left_keep_cols: list[str] | None = None,
) -> pd.DataFrame:
    keep_cols = ["ts_code", left_time_col, *(left_keep_cols or [])]
    if left.empty:
        columns = [*keep_cols, right_time_alias, *right_value_cols]
        return pd.DataFrame(columns=columns)

    left_frame = left[keep_cols].copy()
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


def build_fina_indicator_pit_table(raw_fina_indicator: pd.DataFrame, calendar_table: pd.DataFrame) -> pd.DataFrame:
    if raw_fina_indicator.empty:
        return pd.DataFrame(
            columns=[
                "ts_code",
                "fi_report_period",
                "fi_ann_date",
                "fi_availability_date",
                "fi_tradable_date",
            ]
        )

    fina_indicator = raw_fina_indicator.copy()
    fina_indicator["ann_date"] = pd.to_datetime(fina_indicator["ann_date"], errors="coerce")
    fina_indicator["end_date"] = pd.to_datetime(fina_indicator["end_date"], errors="coerce")
    fina_indicator = fina_indicator.dropna(subset=["ts_code", "ann_date", "end_date"]).reset_index(drop=True)
    fina_indicator["report_period"] = fina_indicator["end_date"]
    fina_indicator["availability_date"] = fina_indicator["ann_date"]

    trading_dates = pd.to_datetime(calendar_table["trade_date"], errors="coerce").dropna().sort_values().reset_index(drop=True)
    trading_array = trading_dates.to_numpy(dtype="datetime64[ns]")
    availability_array = fina_indicator["availability_date"].to_numpy(dtype="datetime64[ns]")
    next_trade_positions = np.searchsorted(trading_array, availability_array, side="right")
    tradable_dates = pd.Series(pd.NaT, index=fina_indicator.index, dtype="datetime64[ns]")
    valid_positions = next_trade_positions < len(trading_array)
    tradable_dates.loc[valid_positions] = trading_dates.iloc[next_trade_positions[valid_positions]].to_numpy()
    fina_indicator["tradable_date"] = tradable_dates

    base_cols = ["ts_code", "report_period", "ann_date", "availability_date", "tradable_date"]
    metric_cols = [column for column in fina_indicator.columns if column not in {"ts_code", "ann_date", "end_date", "report_period", "availability_date", "tradable_date"}]

    rename_map = {
        "report_period": "fi_report_period",
        "ann_date": "fi_ann_date",
        "availability_date": "fi_availability_date",
        "tradable_date": "fi_tradable_date",
    }
    rename_map.update({column: f"fi_{column}" for column in metric_cols})

    result = fina_indicator[["ts_code", *base_cols[1:], *metric_cols]].rename(columns=rename_map)
    result = result.sort_values(["fi_tradable_date", "ts_code", "fi_report_period", "fi_ann_date"], kind="mergesort").reset_index(drop=True)
    return result


def build_monthly_snapshot_base(
    monthly_universe: pd.DataFrame,
    adjusted_price_panel: pd.DataFrame,
    daily_basic: pd.DataFrame,
    *,
    raw_fina_indicator: pd.DataFrame | None = None,
    calendar_table: pd.DataFrame | None = None,
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

    if raw_fina_indicator is not None and calendar_table is not None and not raw_fina_indicator.empty:
        fina_indicator_pit = build_fina_indicator_pit_table(raw_fina_indicator, calendar_table)
        fina_indicator_cols = [column for column in fina_indicator_pit.columns if column not in {"ts_code", "fi_tradable_date"}]
        fina_indicator_snapshot = _asof_join_by_ts_code(
            universe,
            fina_indicator_pit,
            left_time_col="trade_execution_date",
            right_time_col="fi_tradable_date",
            right_value_cols=fina_indicator_cols,
            right_time_alias="fi_tradable_date",
            left_keep_cols=["rebalance_date"],
        )
        snapshot = snapshot.merge(
            fina_indicator_snapshot,
            on=["ts_code", "rebalance_date", "trade_execution_date"],
            how="left",
            validate="one_to_one",
        )

    ordered_cols = [
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
    fi_cols = sorted(column for column in snapshot.columns if column.startswith("fi_"))
    result = snapshot[[*ordered_cols, *fi_cols]].copy()
    result = result.sort_values(["rebalance_date", "ts_code"]).reset_index(drop=True)
    return result
