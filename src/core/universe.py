from __future__ import annotations

import numpy as np
import pandas as pd


ALLOWED_MARKETS = {"主板", "创业板", "科创板"}
ALLOWED_TS_CODE_SUFFIXES = (".SH", ".SZ")


def _compute_recent_trading_features(
    adjusted_price_panel: pd.DataFrame,
    month_end_dates: pd.Series,
    liquidity_window: int,
) -> pd.DataFrame:
    price = adjusted_price_panel.copy()
    price["trade_date"] = pd.to_datetime(price["trade_date"], errors="coerce")
    price = price.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)

    valid_trade = (price["amount"].notna()) & (price["adj_close"].notna())
    valid_adj_factor = price["adj_factor"].notna()

    rolling_group = price.groupby("ts_code", sort=False)
    price["valid_trade_days_20d"] = (
        valid_trade.astype("int64")
        .groupby(price["ts_code"])
        .rolling(liquidity_window, min_periods=1)
        .sum()
        .reset_index(level=0, drop=True)
    )
    price["median_amount_20d"] = (
        rolling_group["amount"]
        .rolling(liquidity_window, min_periods=1)
        .median()
        .reset_index(level=0, drop=True)
    )
    price["valid_adj_factor_days_20d"] = (
        valid_adj_factor.astype("int64")
        .groupby(price["ts_code"])
        .rolling(liquidity_window, min_periods=1)
        .sum()
        .reset_index(level=0, drop=True)
    )

    month_end_mask = price["trade_date"].isin(pd.to_datetime(month_end_dates))
    features = price.loc[
        month_end_mask,
        [
            "ts_code",
            "trade_date",
            "valid_trade_days_20d",
            "median_amount_20d",
            "valid_adj_factor_days_20d",
        ],
    ].copy()
    features = features.rename(columns={"trade_date": "rebalance_date"})
    features["has_price_coverage"] = features["valid_trade_days_20d"] > 0
    return features


def _prepare_stock_basic(stock_basic: pd.DataFrame, calendar_table: pd.DataFrame) -> pd.DataFrame:
    stocks = stock_basic.copy()
    stocks["list_date"] = pd.to_datetime(stocks["list_date"], errors="coerce")
    stocks["delist_date"] = pd.to_datetime(stocks["delist_date"], errors="coerce")
    trading_dates = pd.to_datetime(calendar_table["trade_date"]).sort_values().reset_index(drop=True)
    trading_array = trading_dates.to_numpy(dtype="datetime64[ns]")

    list_dates = stocks["list_date"].to_numpy(dtype="datetime64[ns]")
    list_trade_positions = np.searchsorted(trading_array, list_dates, side="left")
    list_trade_positions = np.where(
        (pd.isna(stocks["list_date"])) | (list_trade_positions >= len(trading_array)),
        -1,
        list_trade_positions,
    )

    effective_list_trade_dates = pd.Series(pd.NaT, index=stocks.index, dtype="datetime64[ns]")
    valid_list_mask = list_trade_positions >= 0
    effective_list_trade_dates.loc[valid_list_mask] = trading_dates.iloc[list_trade_positions[valid_list_mask]].to_numpy()

    stocks["effective_list_trade_date"] = effective_list_trade_dates
    stocks["list_trade_idx"] = pd.Series(list_trade_positions, index=stocks.index).astype("Int64")
    stocks["is_common_a"] = stocks["ts_code"].str.endswith(ALLOWED_TS_CODE_SUFFIXES) & stocks["market"].isin(ALLOWED_MARKETS)
    return stocks


def _join_reasons(reason_frame: pd.DataFrame) -> pd.Series:
    return reason_frame.apply(
        lambda row: "|".join([reason for reason in row if reason]),
        axis=1,
    )


def build_monthly_universe(
    calendar_table: pd.DataFrame,
    stock_basic: pd.DataFrame,
    adjusted_price_panel: pd.DataFrame,
    *,
    ipo_min_trade_days: int,
    liquidity_window: int,
    min_valid_trade_days: int,
    min_median_amount: float,
) -> pd.DataFrame:
    if calendar_table.empty or stock_basic.empty:
        return pd.DataFrame(
            columns=[
                "rebalance_date",
                "trade_execution_date",
                "ts_code",
                "exchange",
                "market",
                "list_date",
                "delist_date",
                "days_since_list",
                "valid_trade_days_20d",
                "median_amount_20d",
                "has_price_coverage",
                "is_st_flag",
                "is_suspended_flag",
                "is_eligible",
                "exclude_reason",
            ]
        )

    calendar = calendar_table.copy()
    calendar["trade_date"] = pd.to_datetime(calendar["trade_date"], errors="coerce")
    calendar["next_trade_date"] = pd.to_datetime(calendar["next_trade_date"], errors="coerce")
    calendar = calendar.sort_values("trade_date").reset_index(drop=True)
    calendar["trade_idx"] = np.arange(len(calendar))

    min_price_trade_date = pd.to_datetime(adjusted_price_panel["trade_date"], errors="coerce").min()
    max_price_trade_date = pd.to_datetime(adjusted_price_panel["trade_date"], errors="coerce").max()
    month_ends = calendar.loc[
        (calendar["is_month_end"])
        & (calendar["trade_date"] >= min_price_trade_date)
        & (calendar["trade_date"] <= max_price_trade_date),
        ["trade_date", "next_trade_date", "trade_idx"],
    ].copy()
    month_ends = month_ends.rename(
        columns={
            "trade_date": "rebalance_date",
            "next_trade_date": "trade_execution_date",
            "trade_idx": "rebalance_trade_idx",
        }
    )
    if not month_ends.empty and int(month_ends["rebalance_trade_idx"].min()) + 1 < ipo_min_trade_days:
        raise ValueError(
            "calendar_table does not start early enough to support the IPO seasoning rule "
            f"for the earliest rebalance date. Need at least {ipo_min_trade_days} trade days "
            "before the first rebalance_date."
        )

    stocks = _prepare_stock_basic(stock_basic, calendar)
    month_end_features = _compute_recent_trading_features(
        adjusted_price_panel,
        month_ends["rebalance_date"],
        liquidity_window=liquidity_window,
    )

    frame = month_ends.assign(_key=1).merge(stocks.assign(_key=1), on="_key").drop(columns="_key")
    frame = frame.merge(month_end_features, on=["rebalance_date", "ts_code"], how="left")

    frame["is_st_flag"] = pd.NA
    frame["is_suspended_flag"] = pd.NA

    is_listed = frame["effective_list_trade_date"].notna() & (frame["rebalance_date"] >= frame["effective_list_trade_date"])
    frame["days_since_list"] = pd.Series(pd.NA, index=frame.index, dtype="Int64")
    frame.loc[is_listed, "days_since_list"] = (
        frame.loc[is_listed, "rebalance_trade_idx"] - frame.loc[is_listed, "list_trade_idx"] + 1
    ).astype("Int64")

    not_delisted = frame["delist_date"].isna() | (frame["trade_execution_date"] <= frame["delist_date"])
    has_price_coverage = frame["has_price_coverage"].astype("boolean").fillna(False)
    enough_liquidity_days = frame["valid_trade_days_20d"].fillna(0) >= min_valid_trade_days
    enough_liquidity_amount = frame["median_amount_20d"].fillna(0.0) >= min_median_amount
    has_adj_factor = frame["valid_adj_factor_days_20d"].fillna(0) > 0
    seasoned_enough = frame["days_since_list"].fillna(0) >= ipo_min_trade_days

    reasons = pd.DataFrame(
        {
            "not_common_a": np.where(~frame["is_common_a"], "NOT_COMMON_A", ""),
            "not_listed_yet": np.where(~is_listed, "NOT_LISTED_YET", ""),
            "delisted": np.where(~not_delisted, "DELISTED", ""),
            "ipo_lt_120d": np.where(is_listed & ~seasoned_enough, "IPO_LT_120D", ""),
            "no_price_history": np.where(frame["is_common_a"] & is_listed & ~has_price_coverage, "NO_PRICE_HISTORY", ""),
            "missing_adj_factor": np.where(
                frame["is_common_a"] & is_listed & has_price_coverage & ~has_adj_factor,
                "MISSING_ADJ_FACTOR",
                "",
            ),
            "low_liquidity": np.where(
                frame["is_common_a"] & is_listed & has_price_coverage & has_adj_factor & (~enough_liquidity_days | ~enough_liquidity_amount),
                "LOW_LIQUIDITY_20D",
                "",
            ),
        }
    )

    frame["exclude_reason"] = _join_reasons(reasons)
    frame["is_eligible"] = frame["exclude_reason"] == ""

    result = frame[
        [
            "rebalance_date",
            "trade_execution_date",
            "ts_code",
            "exchange",
            "market",
            "list_date",
            "delist_date",
            "days_since_list",
            "valid_trade_days_20d",
            "median_amount_20d",
            "has_price_coverage",
            "is_st_flag",
            "is_suspended_flag",
            "is_eligible",
            "exclude_reason",
        ]
    ].copy()
    result = result.sort_values(["rebalance_date", "ts_code"]).reset_index(drop=True)
    return result
