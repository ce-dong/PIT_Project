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
            "fi_report_period",
            "fi_ann_date",
            "fi_availability_date",
            "fi_tradable_date",
            "fc_report_period",
            "fc_ann_date",
            "fc_availability_date",
            "fc_tradable_date",
            "ex_report_period",
            "ex_ann_date",
            "ex_availability_date",
            "ex_tradable_date",
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
    right_sort_cols: list[str] | None = None,
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
    pre_sort_cols = [right_time_col, "ts_code", *(right_sort_cols or [])]
    right_frame = right_frame.sort_values(pre_sort_cols, kind="mergesort").reset_index(drop=True)
    right_frame = right_frame.drop_duplicates(subset=["ts_code", right_time_col], keep="last")
    right_frame = right_frame.rename(columns={right_time_col: right_time_alias})
    post_sort_cols = [right_time_alias, "ts_code", *(right_sort_cols or [])]
    right_frame = right_frame.sort_values(post_sort_cols, kind="mergesort").reset_index(drop=True)

    return pd.merge_asof(
        left_frame,
        right_frame,
        by="ts_code",
        left_on=left_time_col,
        right_on=right_time_alias,
        direction="backward",
        allow_exact_matches=True,
    )


def build_financial_statement_pit_table(
    raw_statement: pd.DataFrame,
    calendar_table: pd.DataFrame,
    *,
    prefix: str,
    availability_source_cols: list[str],
    include_f_ann_date: bool,
) -> pd.DataFrame:
    if raw_statement.empty:
        base_columns = [
            "ts_code",
            f"{prefix}_report_period",
            f"{prefix}_ann_date",
            f"{prefix}_availability_date",
            f"{prefix}_tradable_date",
        ]
        if include_f_ann_date:
            base_columns.insert(3, f"{prefix}_f_ann_date")
        return pd.DataFrame(columns=base_columns)

    statement = raw_statement.copy()
    for column in statement.columns:
        if column == "end_date" or column.endswith("_date"):
            statement[column] = pd.to_datetime(statement[column], errors="coerce")
    statement = statement.dropna(subset=["ts_code", "ann_date", "end_date"]).reset_index(drop=True)
    statement["report_period"] = statement["end_date"]

    availability_series = None
    for source_col in availability_source_cols:
        if source_col in statement.columns:
            if availability_series is None:
                availability_series = statement[source_col].copy()
            else:
                availability_series = availability_series.fillna(statement[source_col])
    if availability_series is None:
        availability_series = statement["ann_date"].copy()
    statement["availability_date"] = availability_series

    trading_dates = pd.to_datetime(calendar_table["trade_date"], errors="coerce").dropna().sort_values().reset_index(drop=True)
    trading_array = trading_dates.to_numpy(dtype="datetime64[ns]")
    availability_array = statement["availability_date"].to_numpy(dtype="datetime64[ns]")
    next_trade_positions = np.searchsorted(trading_array, availability_array, side="right")
    tradable_dates = pd.Series(pd.NaT, index=statement.index, dtype="datetime64[ns]")
    valid_positions = next_trade_positions < len(trading_array)
    tradable_dates.loc[valid_positions] = trading_dates.iloc[next_trade_positions[valid_positions]].to_numpy()
    statement["tradable_date"] = tradable_dates

    excluded_cols = {"ts_code", "ann_date", "end_date", "report_period", "availability_date", "tradable_date"}
    if "f_ann_date" in statement.columns:
        excluded_cols.add("f_ann_date")
    metric_cols = [column for column in statement.columns if column not in excluded_cols]

    rename_map = {
        "report_period": f"{prefix}_report_period",
        "ann_date": f"{prefix}_ann_date",
        "availability_date": f"{prefix}_availability_date",
        "tradable_date": f"{prefix}_tradable_date",
    }
    selected_cols = ["ts_code", "report_period", "ann_date"]
    if include_f_ann_date and "f_ann_date" in statement.columns:
        rename_map["f_ann_date"] = f"{prefix}_f_ann_date"
        selected_cols.append("f_ann_date")
    selected_cols.extend(["availability_date", "tradable_date", *metric_cols])
    rename_map.update({column: f"{prefix}_{column}" for column in metric_cols})

    result = statement[selected_cols].rename(columns=rename_map)
    sort_cols = [f"{prefix}_tradable_date", "ts_code", f"{prefix}_report_period", f"{prefix}_ann_date"]
    if include_f_ann_date and f"{prefix}_f_ann_date" in result.columns:
        sort_cols.append(f"{prefix}_f_ann_date")
    if f"{prefix}_update_flag" in result.columns:
        sort_cols.append(f"{prefix}_update_flag")
    result = result.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)
    return result


def build_fina_indicator_pit_table(raw_fina_indicator: pd.DataFrame, calendar_table: pd.DataFrame) -> pd.DataFrame:
    return build_financial_statement_pit_table(
        raw_fina_indicator,
        calendar_table,
        prefix="fi",
        availability_source_cols=["ann_date"],
        include_f_ann_date=False,
    )


def build_income_pit_table(raw_income: pd.DataFrame, calendar_table: pd.DataFrame) -> pd.DataFrame:
    return build_financial_statement_pit_table(
        raw_income,
        calendar_table,
        prefix="inc",
        availability_source_cols=["f_ann_date", "ann_date"],
        include_f_ann_date=True,
    )


def build_balancesheet_pit_table(raw_balancesheet: pd.DataFrame, calendar_table: pd.DataFrame) -> pd.DataFrame:
    return build_financial_statement_pit_table(
        raw_balancesheet,
        calendar_table,
        prefix="bs",
        availability_source_cols=["f_ann_date", "ann_date"],
        include_f_ann_date=True,
    )


def build_cashflow_pit_table(raw_cashflow: pd.DataFrame, calendar_table: pd.DataFrame) -> pd.DataFrame:
    return build_financial_statement_pit_table(
        raw_cashflow,
        calendar_table,
        prefix="cf",
        availability_source_cols=["f_ann_date", "ann_date"],
        include_f_ann_date=True,
    )


def build_forecast_pit_table(raw_forecast: pd.DataFrame, calendar_table: pd.DataFrame) -> pd.DataFrame:
    return build_financial_statement_pit_table(
        raw_forecast,
        calendar_table,
        prefix="fc",
        availability_source_cols=["ann_date"],
        include_f_ann_date=False,
    )


def build_express_pit_table(raw_express: pd.DataFrame, calendar_table: pd.DataFrame) -> pd.DataFrame:
    return build_financial_statement_pit_table(
        raw_express,
        calendar_table,
        prefix="ex",
        availability_source_cols=["ann_date"],
        include_f_ann_date=False,
    )


def _join_financial_snapshot(
    snapshot: pd.DataFrame,
    universe: pd.DataFrame,
    raw_statement: pd.DataFrame | None,
    calendar_table: pd.DataFrame | None,
    *,
    build_pit_table,
    prefix: str,
) -> pd.DataFrame:
    if raw_statement is None or calendar_table is None or raw_statement.empty:
        return snapshot

    pit_table = build_pit_table(raw_statement, calendar_table)
    tradable_col = f"{prefix}_tradable_date"
    report_period_col = f"{prefix}_report_period"
    ann_date_col = f"{prefix}_ann_date"
    sort_cols = [report_period_col, ann_date_col]
    f_ann_col = f"{prefix}_f_ann_date"
    if f_ann_col in pit_table.columns:
        sort_cols.append(f_ann_col)
    update_flag_col = f"{prefix}_update_flag"
    if update_flag_col in pit_table.columns:
        sort_cols.append(update_flag_col)

    pit_cols = [column for column in pit_table.columns if column not in {"ts_code", tradable_col}]
    pit_snapshot = _asof_join_by_ts_code(
        universe,
        pit_table,
        left_time_col="trade_execution_date",
        right_time_col=tradable_col,
        right_value_cols=pit_cols,
        right_time_alias=tradable_col,
        left_keep_cols=["rebalance_date"],
        right_sort_cols=sort_cols,
    )
    return snapshot.merge(
        pit_snapshot,
        on=["ts_code", "rebalance_date", "trade_execution_date"],
        how="left",
        validate="one_to_one",
    )


def build_monthly_snapshot_base(
    monthly_universe: pd.DataFrame,
    adjusted_price_panel: pd.DataFrame,
    daily_basic: pd.DataFrame,
    *,
    raw_fina_indicator: pd.DataFrame | None = None,
    raw_income: pd.DataFrame | None = None,
    raw_balancesheet: pd.DataFrame | None = None,
    raw_cashflow: pd.DataFrame | None = None,
    raw_forecast: pd.DataFrame | None = None,
    raw_express: pd.DataFrame | None = None,
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

    snapshot = _join_financial_snapshot(
        snapshot,
        universe,
        raw_fina_indicator,
        calendar_table,
        build_pit_table=build_fina_indicator_pit_table,
        prefix="fi",
    )
    snapshot = _join_financial_snapshot(
        snapshot,
        universe,
        raw_income,
        calendar_table,
        build_pit_table=build_income_pit_table,
        prefix="inc",
    )
    snapshot = _join_financial_snapshot(
        snapshot,
        universe,
        raw_balancesheet,
        calendar_table,
        build_pit_table=build_balancesheet_pit_table,
        prefix="bs",
    )
    snapshot = _join_financial_snapshot(
        snapshot,
        universe,
        raw_cashflow,
        calendar_table,
        build_pit_table=build_cashflow_pit_table,
        prefix="cf",
    )
    snapshot = _join_financial_snapshot(
        snapshot,
        universe,
        raw_forecast,
        calendar_table,
        build_pit_table=build_forecast_pit_table,
        prefix="fc",
    )
    snapshot = _join_financial_snapshot(
        snapshot,
        universe,
        raw_express,
        calendar_table,
        build_pit_table=build_express_pit_table,
        prefix="ex",
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
    financial_cols = sorted(
        column for column in snapshot.columns if column.startswith(("fi_", "inc_", "bs_", "cf_", "fc_", "ex_"))
    )
    result = snapshot[[*ordered_cols, *financial_cols]].copy()
    result = result.sort_values(["rebalance_date", "ts_code"]).reset_index(drop=True)
    return result
