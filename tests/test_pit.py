from __future__ import annotations

import pandas as pd

from src.core.pit import (
    build_balancesheet_pit_table,
    build_cashflow_pit_table,
    build_fina_indicator_pit_table,
    build_income_pit_table,
    build_monthly_snapshot_base,
)


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


def test_build_fina_indicator_pit_table_maps_strict_next_trade_date():
    raw_fina_indicator = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "ann_date": "20240131", "end_date": "20231231", "roe": 10.0},
            {"ts_code": "000001.SZ", "ann_date": "20240201", "end_date": "20240331", "roe": 20.0},
        ]
    )
    calendar_table = pd.DataFrame(
        [
            {"trade_date": "2024-01-31"},
            {"trade_date": "2024-02-01"},
            {"trade_date": "2024-02-02"},
        ]
    )

    result = build_fina_indicator_pit_table(raw_fina_indicator, calendar_table)

    first = result.iloc[0]
    second = result.iloc[1]
    assert str(first["fi_tradable_date"].date()) == "2024-02-01"
    assert str(second["fi_tradable_date"].date()) == "2024-02-02"
    assert first["fi_roe"] == 10.0


def test_financial_statement_pit_prefers_f_ann_date_for_availability():
    raw_income = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "ann_date": "20240131", "f_ann_date": "20240202", "end_date": "20231231", "revenue": 100.0},
        ]
    )
    calendar_table = pd.DataFrame(
        [
            {"trade_date": "2024-02-01"},
            {"trade_date": "2024-02-02"},
            {"trade_date": "2024-02-05"},
        ]
    )

    result = build_income_pit_table(raw_income, calendar_table)

    row = result.iloc[0]
    assert str(row["inc_availability_date"].date()) == "2024-02-02"
    assert str(row["inc_tradable_date"].date()) == "2024-02-05"
    assert row["inc_revenue"] == 100.0


def test_build_monthly_snapshot_base_joins_fina_indicator_pit_on_trade_execution_date():
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
        ]
    )
    adjusted_price_panel = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": "2024-01-31", "close": 10.2, "adj_close": 20.4, "amount": 32000.0, "vol": 1200.0},
        ]
    )
    daily_basic = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": "2024-01-31",
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
        ]
    )
    raw_fina_indicator = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "ann_date": "20240131", "end_date": "20231231", "roe": 10.0, "roa": 5.0},
            {"ts_code": "000001.SZ", "ann_date": "20240201", "end_date": "20240331", "roe": 20.0, "roa": 6.0},
        ]
    )
    calendar_table = pd.DataFrame(
        [
            {"trade_date": "2024-01-31"},
            {"trade_date": "2024-02-01"},
            {"trade_date": "2024-02-02"},
        ]
    )

    result = build_monthly_snapshot_base(
        monthly_universe,
        adjusted_price_panel,
        daily_basic,
        raw_fina_indicator=raw_fina_indicator,
        calendar_table=calendar_table,
    )

    row = result.iloc[0]
    assert str(row["fi_tradable_date"].date()) == "2024-02-01"
    assert str(row["fi_report_period"].date()) == "2023-12-31"
    assert row["fi_roe"] == 10.0
    assert row["fi_roa"] == 5.0


def test_build_monthly_snapshot_base_joins_income_balancesheet_and_cashflow():
    monthly_universe = pd.DataFrame(
        [
            {
                "rebalance_date": "2024-02-29",
                "trade_execution_date": "2024-03-01",
                "ts_code": "000001.SZ",
                "exchange": "SZSE",
                "market": "主板",
                "list_date": "2023-01-02",
                "delist_date": None,
                "days_since_list": 270,
                "valid_trade_days_20d": 20,
                "median_amount_20d": 30000.0,
                "has_price_coverage": True,
                "is_st_flag": None,
                "is_suspended_flag": None,
                "is_eligible": True,
                "exclude_reason": "",
            },
        ]
    )
    adjusted_price_panel = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": "2024-02-29", "close": 10.2, "adj_close": 20.4, "amount": 32000.0, "vol": 1200.0},
        ]
    )
    daily_basic = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": "2024-02-29",
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
        ]
    )
    raw_income = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "ann_date": "20240220", "f_ann_date": "20240221", "end_date": "20231231", "revenue": 100.0},
            {"ts_code": "000001.SZ", "ann_date": "20240301", "f_ann_date": "20240301", "end_date": "20240331", "revenue": 120.0},
        ]
    )
    raw_balancesheet = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "ann_date": "20240222", "f_ann_date": "20240222", "end_date": "20231231", "total_assets": 500.0},
        ]
    )
    raw_cashflow = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "ann_date": "20240223", "f_ann_date": "20240226", "end_date": "20231231", "n_cashflow_act": 80.0},
        ]
    )
    calendar_table = pd.DataFrame(
        [
            {"trade_date": "2024-02-21"},
            {"trade_date": "2024-02-22"},
            {"trade_date": "2024-02-23"},
            {"trade_date": "2024-02-26"},
            {"trade_date": "2024-02-27"},
            {"trade_date": "2024-02-28"},
            {"trade_date": "2024-02-29"},
            {"trade_date": "2024-03-01"},
            {"trade_date": "2024-03-04"},
        ]
    )

    result = build_monthly_snapshot_base(
        monthly_universe,
        adjusted_price_panel,
        daily_basic,
        raw_income=raw_income,
        raw_balancesheet=raw_balancesheet,
        raw_cashflow=raw_cashflow,
        calendar_table=calendar_table,
    )

    row = result.iloc[0]
    assert str(row["inc_report_period"].date()) == "2023-12-31"
    assert str(row["inc_tradable_date"].date()) == "2024-02-22"
    assert row["inc_revenue"] == 100.0
    assert str(row["bs_tradable_date"].date()) == "2024-02-23"
    assert row["bs_total_assets"] == 500.0
    assert str(row["cf_tradable_date"].date()) == "2024-02-27"
    assert row["cf_n_cashflow_act"] == 80.0


def test_build_balancesheet_and_cashflow_pit_tables_use_f_ann_date():
    raw_balancesheet = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "ann_date": "20240131", "f_ann_date": "20240202", "end_date": "20231231", "total_assets": 500.0},
        ]
    )
    raw_cashflow = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "ann_date": "20240131", "f_ann_date": "20240201", "end_date": "20231231", "n_cashflow_act": 80.0},
        ]
    )
    calendar_table = pd.DataFrame(
        [
            {"trade_date": "2024-02-01"},
            {"trade_date": "2024-02-02"},
            {"trade_date": "2024-02-05"},
        ]
    )

    bs_result = build_balancesheet_pit_table(raw_balancesheet, calendar_table)
    cf_result = build_cashflow_pit_table(raw_cashflow, calendar_table)

    assert str(bs_result.iloc[0]["bs_tradable_date"].date()) == "2024-02-05"
    assert str(cf_result.iloc[0]["cf_tradable_date"].date()) == "2024-02-02"
