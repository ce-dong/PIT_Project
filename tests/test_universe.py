from __future__ import annotations

import pandas as pd

from src.core.universe import build_monthly_universe


def test_build_monthly_universe_applies_listing_and_liquidity_filters():
    calendar_table = pd.DataFrame(
        [
            {"trade_date": "2024-01-29", "prev_trade_date": "2024-01-26", "next_trade_date": "2024-01-30", "is_month_end": False, "month": "2024-01"},
            {"trade_date": "2024-01-30", "prev_trade_date": "2024-01-29", "next_trade_date": "2024-01-31", "is_month_end": False, "month": "2024-01"},
            {"trade_date": "2024-01-31", "prev_trade_date": "2024-01-30", "next_trade_date": "2024-02-01", "is_month_end": True, "month": "2024-01"},
            {"trade_date": "2024-02-01", "prev_trade_date": "2024-01-31", "next_trade_date": "2024-02-02", "is_month_end": False, "month": "2024-02"},
        ]
    )
    stock_basic = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "exchange": "SZSE", "market": "主板", "list_date": "2023-01-02", "delist_date": None},
            {"ts_code": "000002.SZ", "exchange": "SZSE", "market": "主板", "list_date": "2024-01-30", "delist_date": None},
        ]
    )
    adjusted_price_panel = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": "2024-01-29", "amount": 30000.0, "adj_factor": 1.0, "adj_close": 10.0},
            {"ts_code": "000001.SZ", "trade_date": "2024-01-30", "amount": 32000.0, "adj_factor": 1.0, "adj_close": 10.1},
            {"ts_code": "000001.SZ", "trade_date": "2024-01-31", "amount": 34000.0, "adj_factor": 1.0, "adj_close": 10.2},
            {"ts_code": "000002.SZ", "trade_date": "2024-01-30", "amount": 1000.0, "adj_factor": 1.0, "adj_close": 5.0},
            {"ts_code": "000002.SZ", "trade_date": "2024-01-31", "amount": 1000.0, "adj_factor": 1.0, "adj_close": 5.1},
        ]
    )

    result = build_monthly_universe(
        calendar_table,
        stock_basic,
        adjusted_price_panel,
        ipo_min_trade_days=3,
        liquidity_window=3,
        min_valid_trade_days=3,
        min_median_amount=20000.0,
    )

    eligible = result.loc[result["ts_code"] == "000001.SZ"].iloc[0]
    assert bool(eligible["is_eligible"]) is True
    assert eligible["exclude_reason"] == ""
    assert int(eligible["days_since_list"]) == 3

    ipo_filtered = result.loc[result["ts_code"] == "000002.SZ"].iloc[0]
    assert bool(ipo_filtered["is_eligible"]) is False
    assert "IPO_LT_120D" in ipo_filtered["exclude_reason"]
    assert "LOW_LIQUIDITY_20D" in ipo_filtered["exclude_reason"]


def test_build_monthly_universe_raises_when_calendar_starts_too_late():
    calendar_table = pd.DataFrame(
        [
            {"trade_date": "2024-01-30", "prev_trade_date": "2024-01-29", "next_trade_date": "2024-01-31", "is_month_end": False, "month": "2024-01"},
            {"trade_date": "2024-01-31", "prev_trade_date": "2024-01-30", "next_trade_date": "2024-02-01", "is_month_end": True, "month": "2024-01"},
            {"trade_date": "2024-02-01", "prev_trade_date": "2024-01-31", "next_trade_date": "2024-02-02", "is_month_end": False, "month": "2024-02"},
        ]
    )
    stock_basic = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "exchange": "SZSE", "market": "主板", "list_date": "1991-04-03", "delist_date": None},
        ]
    )
    adjusted_price_panel = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": "2024-01-30", "amount": 30000.0, "adj_factor": 1.0, "adj_close": 10.0},
            {"ts_code": "000001.SZ", "trade_date": "2024-01-31", "amount": 32000.0, "adj_factor": 1.0, "adj_close": 10.1},
        ]
    )

    try:
        build_monthly_universe(
            calendar_table,
            stock_basic,
            adjusted_price_panel,
            ipo_min_trade_days=3,
            liquidity_window=3,
            min_valid_trade_days=2,
            min_median_amount=20000.0,
        )
    except ValueError as exc:
        assert "calendar_table does not start early enough" in str(exc)
    else:
        raise AssertionError("Expected ValueError when calendar_table starts too late.")


def test_build_monthly_universe_uses_only_price_covered_months():
    calendar_table = pd.DataFrame(
        [
            {"trade_date": "2024-01-30", "prev_trade_date": "2024-01-29", "next_trade_date": "2024-01-31", "is_month_end": False, "month": "2024-01"},
            {"trade_date": "2024-01-31", "prev_trade_date": "2024-01-30", "next_trade_date": "2024-02-01", "is_month_end": True, "month": "2024-01"},
            {"trade_date": "2024-02-28", "prev_trade_date": "2024-02-27", "next_trade_date": "2024-02-29", "is_month_end": False, "month": "2024-02"},
            {"trade_date": "2024-02-29", "prev_trade_date": "2024-02-28", "next_trade_date": "2024-03-01", "is_month_end": True, "month": "2024-02"},
            {"trade_date": "2024-03-01", "prev_trade_date": "2024-02-29", "next_trade_date": "2024-03-04", "is_month_end": False, "month": "2024-03"},
        ]
    )
    stock_basic = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "exchange": "SZSE", "market": "主板", "list_date": "2023-01-02", "delist_date": None},
        ]
    )
    adjusted_price_panel = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": "2024-02-28", "amount": 30000.0, "adj_factor": 1.0, "adj_close": 10.0},
            {"ts_code": "000001.SZ", "trade_date": "2024-02-29", "amount": 32000.0, "adj_factor": 1.0, "adj_close": 10.1},
        ]
    )

    result = build_monthly_universe(
        calendar_table,
        stock_basic,
        adjusted_price_panel,
        ipo_min_trade_days=2,
        liquidity_window=2,
        min_valid_trade_days=1,
        min_median_amount=10000.0,
    )

    assert len(result) == 1
    assert str(result.iloc[0]["rebalance_date"].date()) == "2024-02-29"
