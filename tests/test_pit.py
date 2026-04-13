from __future__ import annotations

import pandas as pd

from src.core.pit import build_monthly_snapshot_base


def test_build_monthly_snapshot_base_uses_latest_available_market_rows():
    monthly_universe = pd.DataFrame(
        [
            {
                "rebalance_date": "2024-01-31",
                "trade_execution_date": "2024-02-01",
                "ts_code": "000001.SZ",
                "exchange": "SZSE",
                "market": "主板",
                "list_date": "2023-01-02",
                "delist_date": None,
                "days_since_list": 250,
                "valid_trade_days_20d": 20,
                "median_amount_20d": 30000.0,
                "has_price_coverage": True,
                "is_st_flag": None,
                "is_suspended_flag": None,
                "is_eligible": True,
                "exclude_reason": "",
            },
            {
                "rebalance_date": "2024-01-31",
                "trade_execution_date": "2024-02-01",
                "ts_code": "000002.SZ",
                "exchange": "SZSE",
                "market": "主板",
                "list_date": "2024-01-10",
                "delist_date": None,
                "days_since_list": 15,
                "valid_trade_days_20d": 10,
                "median_amount_20d": 9000.0,
                "has_price_coverage": True,
                "is_st_flag": None,
                "is_suspended_flag": None,
                "is_eligible": False,
                "exclude_reason": "IPO_LT_120D",
            },
        ]
    )
    adjusted_price_panel = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": "2024-01-30", "close": 10.0, "adj_close": 20.0, "amount": 30000.0, "vol": 1000.0},
            {"ts_code": "000001.SZ", "trade_date": "2024-01-31", "close": 10.2, "adj_close": 20.4, "amount": 32000.0, "vol": 1200.0},
            {"ts_code": "000001.SZ", "trade_date": "2024-02-01", "close": 10.4, "adj_close": 20.8, "amount": 35000.0, "vol": 1400.0},
            {"ts_code": "000002.SZ", "trade_date": "2024-01-30", "close": 5.0, "adj_close": 5.0, "amount": 9000.0, "vol": 800.0},
        ]
    )
    daily_basic = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": "2024-01-30",
                "total_mv": 100000.0,
                "circ_mv": 80000.0,
                "pb": 1.2,
                "pe_ttm": 10.0,
                "ps_ttm": 2.0,
                "dv_ttm": 1.0,
                "turnover_rate": 2.0,
                "turnover_rate_f": 2.5,
                "volume_ratio": 1.1,
            },
            {
                "ts_code": "000001.SZ",
                "trade_date": "2024-02-01",
                "total_mv": 101000.0,
                "circ_mv": 80500.0,
                "pb": 1.3,
                "pe_ttm": 10.5,
                "ps_ttm": 2.1,
                "dv_ttm": 1.1,
                "turnover_rate": 2.1,
                "turnover_rate_f": 2.6,
                "volume_ratio": 1.2,
            },
            {
                "ts_code": "000002.SZ",
                "trade_date": "2024-01-29",
                "total_mv": 50000.0,
                "circ_mv": 30000.0,
                "pb": 3.0,
                "pe_ttm": 40.0,
                "ps_ttm": 6.0,
                "dv_ttm": 0.0,
                "turnover_rate": 5.0,
                "turnover_rate_f": 6.0,
                "volume_ratio": 0.8,
            },
        ]
    )

    result = build_monthly_snapshot_base(monthly_universe, adjusted_price_panel, daily_basic)

    first = result.loc[result["ts_code"] == "000001.SZ"].iloc[0]
    assert str(first["price_trade_date"].date()) == "2024-01-31"
    assert str(first["daily_basic_trade_date"].date()) == "2024-01-30"
    assert first["close"] == 10.2
    assert first["adj_close"] == 20.4
    assert first["pb"] == 1.2
    assert first["amount"] == 32000.0
    assert bool(first["is_eligible"]) is True

    second = result.loc[result["ts_code"] == "000002.SZ"].iloc[0]
    assert str(second["price_trade_date"].date()) == "2024-01-30"
    assert str(second["daily_basic_trade_date"].date()) == "2024-01-29"
    assert second["exclude_reason"] == "IPO_LT_120D"
    assert bool(second["is_eligible"]) is False


def test_build_monthly_snapshot_base_returns_empty_schema_for_empty_universe():
    result = build_monthly_snapshot_base(
        pd.DataFrame(),
        pd.DataFrame(),
        pd.DataFrame(),
    )

    assert result.empty
    assert "price_trade_date" in result.columns
    assert "daily_basic_trade_date" in result.columns
