from __future__ import annotations

import pandas as pd

from src.core.calendar import build_calendar_table


def test_build_calendar_table_adds_next_trade_date_and_month_end_flags():
    raw = pd.DataFrame(
        [
            {"exchange": "SSE", "cal_date": "20240130", "is_open": 1, "pretrade_date": "20240129"},
            {"exchange": "SSE", "cal_date": "20240131", "is_open": 1, "pretrade_date": "20240130"},
            {"exchange": "SSE", "cal_date": "20240201", "is_open": 1, "pretrade_date": "20240131"},
            {"exchange": "SSE", "cal_date": "20240202", "is_open": 1, "pretrade_date": "20240201"},
        ]
    )

    result = build_calendar_table(raw)

    assert result["trade_date"].dt.strftime("%Y%m%d").tolist() == ["20240130", "20240131", "20240201", "20240202"]
    assert result["next_trade_date"].dt.strftime("%Y%m%d").tolist()[:3] == ["20240131", "20240201", "20240202"]
    assert pd.isna(result.iloc[-1]["next_trade_date"])
    assert result["is_month_end"].tolist() == [False, True, False, True]
    assert result["month"].tolist() == ["2024-01", "2024-01", "2024-02", "2024-02"]

