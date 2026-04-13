from __future__ import annotations

import pandas as pd

from src.core.adjustments import build_adjusted_price_panel


def test_build_adjusted_price_panel_merges_and_adjusts_price_columns():
    daily = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": "20240110",
                "open": 10.0,
                "high": 11.0,
                "low": 9.0,
                "close": 10.5,
                "pre_close": 10.2,
                "change": 0.3,
                "pct_chg": 2.94,
                "vol": 1000.0,
                "amount": 2000.0,
            }
        ]
    )
    adj_factor = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": "20240110",
                "adj_factor": 2.0,
            }
        ]
    )

    result = build_adjusted_price_panel(daily, adj_factor)

    assert len(result) == 1
    row = result.iloc[0]
    assert row["adj_open"] == 20.0
    assert row["adj_high"] == 22.0
    assert row["adj_low"] == 18.0
    assert row["adj_close"] == 21.0
    assert row["adj_factor"] == 2.0
