from __future__ import annotations

import pandas as pd


def build_adjusted_price_panel(daily_df: pd.DataFrame, adj_factor_df: pd.DataFrame) -> pd.DataFrame:
    if daily_df.empty:
        columns = [
            "ts_code",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "change",
            "pct_chg",
            "vol",
            "amount",
            "adj_factor",
            "adj_open",
            "adj_high",
            "adj_low",
            "adj_close",
        ]
        return pd.DataFrame(columns=columns)

    daily = daily_df.copy()
    adj_factor = adj_factor_df.copy()

    daily["trade_date"] = pd.to_datetime(daily["trade_date"], errors="coerce")
    adj_factor["trade_date"] = pd.to_datetime(adj_factor["trade_date"], errors="coerce")

    merged = daily.merge(
        adj_factor[["ts_code", "trade_date", "adj_factor"]],
        on=["ts_code", "trade_date"],
        how="left",
        validate="one_to_one",
    )

    for price_col in ("open", "high", "low", "close"):
        merged[f"adj_{price_col}"] = merged[price_col] * merged["adj_factor"]

    merged = merged.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
    return merged[
        [
            "ts_code",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "change",
            "pct_chg",
            "vol",
            "amount",
            "adj_factor",
            "adj_open",
            "adj_high",
            "adj_low",
            "adj_close",
        ]
    ]

