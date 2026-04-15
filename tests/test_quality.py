from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

from src.validators.core import (
    _append_snapshot_coverage_threshold_issues,
    validate_adjusted_price_panel_df,
    validate_calendar_table_df,
    validate_monthly_snapshot_base_df,
    validate_monthly_universe_df,
)
from src.validators.base import ValidationResult


def test_validate_calendar_table_df_passes_for_consistent_calendar():
    df = pd.DataFrame(
        [
            {"trade_date": "2024-01-30", "prev_trade_date": None, "next_trade_date": "2024-01-31", "is_month_end": False, "month": "2024-01"},
            {"trade_date": "2024-01-31", "prev_trade_date": "2024-01-30", "next_trade_date": "2024-02-01", "is_month_end": True, "month": "2024-01"},
            {"trade_date": "2024-02-01", "prev_trade_date": "2024-01-31", "next_trade_date": None, "is_month_end": True, "month": "2024-02"},
        ]
    )

    result = validate_calendar_table_df(df)

    assert result.passed is True
    assert result.error_count == 0


def test_validate_adjusted_price_panel_df_flags_formula_break():
    df = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": "2024-01-31",
                "open": 10.0,
                "close": 11.0,
                "adj_factor": 2.0,
                "adj_open": 20.0,
                "adj_close": 21.0,
                "year": 2024,
                "month": 1,
            }
        ]
    )

    result = validate_adjusted_price_panel_df(df)

    assert result.passed is False
    assert any(issue.check == "adj_close_formula" for issue in result.issues)


def test_validate_adjusted_price_panel_df_flags_missing_required_column():
    df = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "trade_date": "2024-01-31",
                "open": 10.0,
                "close": 11.0,
                "adj_factor": 2.0,
                "adj_open": 20.0,
                "year": 2024,
                "month": 1,
            }
        ]
    )

    result = validate_adjusted_price_panel_df(df)

    assert result.passed is False
    assert any(issue.check == "required_columns_present" for issue in result.issues)


def test_validate_monthly_universe_df_warns_on_missing_exclude_reason():
    df = pd.DataFrame(
        [
            {
                "rebalance_date": "2024-01-31",
                "trade_execution_date": "2024-02-01",
                "ts_code": "000001.SZ",
                "is_eligible": False,
                "exclude_reason": "",
                "year": 2024,
                "month": 1,
            }
        ]
    )

    result = validate_monthly_universe_df(df)

    assert result.passed is True
    assert result.warning_count == 1
    assert result.issues[0].check == "exclude_reason_present_for_ineligible_rows"


def test_validate_monthly_snapshot_base_df_flags_pit_cutoff_violation():
    universe = pd.DataFrame(
        [
            {
                "rebalance_date": "2024-01-31",
                "trade_execution_date": "2024-02-01",
                "ts_code": "000001.SZ",
                "is_eligible": True,
                "exclude_reason": "",
            }
        ]
    )
    snapshot = pd.DataFrame(
        [
            {
                "rebalance_date": "2024-01-31",
                "trade_execution_date": "2024-02-01",
                "ts_code": "000001.SZ",
                "price_trade_date": "2024-01-31",
                "daily_basic_trade_date": "2024-01-31",
                "fi_tradable_date": "2024-02-02",
                "is_eligible": True,
                "exclude_reason": "",
                "year": 2024,
                "month": 1,
            }
        ]
    )

    result = validate_monthly_snapshot_base_df(snapshot, universe)

    assert result.passed is False
    assert any(issue.check == "tradable_date_cutoff" for issue in result.issues)


def test_validate_monthly_snapshot_base_df_reports_family_coverage_metrics():
    universe = pd.DataFrame(
        [
            {
                "rebalance_date": "2024-01-31",
                "trade_execution_date": "2024-02-01",
                "ts_code": "000001.SZ",
                "is_eligible": True,
                "exclude_reason": "",
            }
        ]
    )
    snapshot = pd.DataFrame(
        [
            {
                "rebalance_date": "2024-01-31",
                "trade_execution_date": "2024-02-01",
                "ts_code": "000001.SZ",
                "price_trade_date": "2024-01-31",
                "daily_basic_trade_date": "2024-01-31",
                "fi_report_period": "2023-12-31",
                "is_eligible": True,
                "exclude_reason": "",
                "year": 2024,
                "month": 1,
            }
        ]
    )

    result = validate_monthly_snapshot_base_df(snapshot, universe)

    assert result.passed is True
    assert result.metrics["price_snapshot_coverage_ratio"] == 1.0
    assert result.metrics["fi_coverage_ratio"] == 1.0


def test_append_snapshot_coverage_threshold_issues_adds_warning_and_error():
    config = SimpleNamespace(
        quality_market_coverage_warn_ratio=0.75,
        quality_market_coverage_error_ratio=0.60,
        quality_financial_coverage_warn_ratio=0.75,
        quality_financial_coverage_error_ratio=0.60,
        quality_event_coverage_warn_ratio=0.50,
        quality_event_coverage_error_ratio=0.30,
    )
    result = ValidationResult(table_name="monthly_snapshot_base", row_count=1)
    result.add_metric("price_snapshot_coverage_ratio", 0.70)
    result.add_metric("fi_coverage_ratio", 0.55)
    result.add_metric("fc_coverage_ratio", 0.45)

    _append_snapshot_coverage_threshold_issues(result, config)

    checks = {issue.check: issue.level for issue in result.issues}
    assert checks["price_snapshot_coverage_ratio_threshold"] == "warning"
    assert checks["fi_coverage_ratio_threshold"] == "error"
    assert checks["fc_coverage_ratio_threshold"] == "warning"
