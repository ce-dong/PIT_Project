from __future__ import annotations

import pandas as pd


def build_calendar_table(raw_trade_cal: pd.DataFrame) -> pd.DataFrame:
    if raw_trade_cal.empty:
        return pd.DataFrame(
            columns=["trade_date", "prev_trade_date", "next_trade_date", "is_month_end", "month"]
        )

    working = raw_trade_cal.copy()
    working["cal_date"] = pd.to_datetime(working["cal_date"], errors="coerce")
    if "pretrade_date" in working.columns:
        working["pretrade_date"] = pd.to_datetime(working["pretrade_date"], errors="coerce")

    open_days = working.loc[working["is_open"] == 1, ["cal_date", "pretrade_date"]].copy()
    open_days = open_days.sort_values("cal_date").drop_duplicates(subset=["cal_date"], keep="last")
    open_days = open_days.rename(
        columns={
            "cal_date": "trade_date",
            "pretrade_date": "prev_trade_date",
        }
    ).reset_index(drop=True)

    open_days["next_trade_date"] = open_days["trade_date"].shift(-1)
    open_days["month"] = open_days["trade_date"].dt.strftime("%Y-%m")
    open_days["is_month_end"] = open_days["month"] != open_days["month"].shift(-1)
    open_days["is_month_end"] = open_days["is_month_end"].fillna(True)
    return open_days[["trade_date", "prev_trade_date", "next_trade_date", "is_month_end", "month"]]

