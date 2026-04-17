from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.config import AppConfig
from src.labels.forward_returns import build_forward_return_label_panel
from src.labels.registry import LABEL_REGISTRY
from src.labels.runner import build_label_panel
from src.research.experiment import ResearchRunConfig, initialize_experiment_layout
from src.storage.parquet import ParquetDataStore


def _make_config(project_root: Path) -> AppConfig:
    return AppConfig(
        project_root=project_root,
        data_root=project_root / "data",
        raw_data_root=project_root / "data" / "raw",
        lake_data_root=project_root / "data" / "lake",
        panel_data_root=project_root / "data" / "panel",
        experiments_root=project_root / "data" / "experiments",
        metadata_root=project_root / "data" / "metadata",
        log_root=project_root / "data" / "logs",
        reports_root=project_root / "data" / "reports",
        env_file=project_root / ".env",
        tushare_token="dummy",
        initial_history_start="20150101",
        calendar_start_date="19900101",
        calendar_future_days=366,
        daily_lookback_trade_days=20,
        adj_factor_lookback_trade_days=60,
        financial_lookback_days=120,
        universe_ipo_min_trade_days=120,
        universe_liquidity_window=20,
        universe_min_valid_trade_days=15,
        universe_min_median_amount=20000.0,
        quality_market_coverage_warn_ratio=0.75,
        quality_market_coverage_error_ratio=0.60,
        quality_financial_coverage_warn_ratio=0.75,
        quality_financial_coverage_error_ratio=0.60,
        quality_event_coverage_warn_ratio=0.50,
        quality_event_coverage_error_ratio=0.30,
        request_sleep_seconds=0.3,
        retry_attempts=3,
        calendar_exchange="SSE",
    )


def test_build_forward_return_label_panel_uses_future_execution_schedule():
    monthly_universe = pd.DataFrame(
        [
            {"rebalance_date": "2024-01-31", "trade_execution_date": "2024-02-01", "ts_code": "000001.SZ", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 1},
            {"rebalance_date": "2024-02-29", "trade_execution_date": "2024-03-01", "ts_code": "000001.SZ", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 2},
            {"rebalance_date": "2024-03-29", "trade_execution_date": "2024-04-01", "ts_code": "000001.SZ", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 3},
            {"rebalance_date": "2024-04-30", "trade_execution_date": "2024-05-06", "ts_code": "000001.SZ", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 4},
            {"rebalance_date": "2024-05-31", "trade_execution_date": "2024-06-03", "ts_code": "000001.SZ", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 5},
            {"rebalance_date": "2024-06-28", "trade_execution_date": "2024-07-01", "ts_code": "000001.SZ", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 6},
            {"rebalance_date": "2024-07-31", "trade_execution_date": "2024-08-01", "ts_code": "000001.SZ", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 7},
        ]
    )
    adjusted_price_panel = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": "2024-02-01", "adj_close": 10.0},
            {"ts_code": "000001.SZ", "trade_date": "2024-03-01", "adj_close": 11.0},
            {"ts_code": "000001.SZ", "trade_date": "2024-04-01", "adj_close": 12.0},
            {"ts_code": "000001.SZ", "trade_date": "2024-05-06", "adj_close": 15.0},
            {"ts_code": "000001.SZ", "trade_date": "2024-06-03", "adj_close": 15.0},
            {"ts_code": "000001.SZ", "trade_date": "2024-07-01", "adj_close": 18.0},
            {"ts_code": "000001.SZ", "trade_date": "2024-08-01", "adj_close": 20.0},
        ]
    )

    specs = LABEL_REGISTRY.list(names=["fwd_ret_1m", "fwd_ret_3m", "fwd_ret_6m"])
    result = build_forward_return_label_panel(monthly_universe, adjusted_price_panel, specs)

    first_row = result.iloc[0]
    last_row = result.iloc[-1]

    assert first_row["label_start_date"].strftime("%Y-%m-%d") == "2024-02-01"
    assert round(first_row["label_fwd_ret_1m"], 6) == 0.1
    assert round(first_row["label_fwd_ret_3m"], 6) == 0.5
    assert round(first_row["label_fwd_ret_6m"], 6) == 1.0
    assert pd.isna(last_row["label_fwd_ret_1m"])
    assert pd.isna(last_row["label_fwd_ret_3m"])
    assert pd.isna(last_row["label_fwd_ret_6m"])


def test_build_forward_return_label_panel_sets_nan_when_prices_are_missing():
    monthly_universe = pd.DataFrame(
        [
            {"rebalance_date": "2024-01-31", "trade_execution_date": "2024-02-01", "ts_code": "000001.SZ", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 1},
            {"rebalance_date": "2024-02-29", "trade_execution_date": "2024-03-01", "ts_code": "000001.SZ", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 2},
        ]
    )
    adjusted_price_panel = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": "2024-02-01", "adj_close": 10.0},
        ]
    )

    specs = LABEL_REGISTRY.list(names=["fwd_ret_1m"])
    result = build_forward_return_label_panel(monthly_universe, adjusted_price_panel, specs)

    assert pd.isna(result.iloc[0]["label_fwd_ret_1m"])


def test_build_label_panel_writes_partitioned_output_and_manifest(tmp_path: Path):
    config = _make_config(tmp_path)
    config.ensure_directories()
    lake_store = ParquetDataStore(config.lake_data_root)

    monthly_universe = pd.DataFrame(
        [
            {"rebalance_date": pd.Timestamp("2024-01-31"), "trade_execution_date": pd.Timestamp("2024-02-01"), "ts_code": "000001.SZ", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 1},
            {"rebalance_date": pd.Timestamp("2024-02-29"), "trade_execution_date": pd.Timestamp("2024-03-01"), "ts_code": "000001.SZ", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 2},
        ]
    )
    adjusted_price_panel = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": pd.Timestamp("2024-02-01"), "adj_close": 10.0},
            {"ts_code": "000001.SZ", "trade_date": pd.Timestamp("2024-03-01"), "adj_close": 11.0},
        ]
    )
    calendar_table = pd.DataFrame(
        [
            {"trade_date": pd.Timestamp("2024-01-31"), "prev_trade_date": pd.NaT, "next_trade_date": pd.Timestamp("2024-02-01"), "is_month_end": True, "month": "2024-01"},
            {"trade_date": pd.Timestamp("2024-02-01"), "prev_trade_date": pd.Timestamp("2024-01-31"), "next_trade_date": pd.Timestamp("2024-02-29"), "is_month_end": False, "month": "2024-02"},
            {"trade_date": pd.Timestamp("2024-02-29"), "prev_trade_date": pd.Timestamp("2024-02-01"), "next_trade_date": pd.Timestamp("2024-03-01"), "is_month_end": True, "month": "2024-02"},
            {"trade_date": pd.Timestamp("2024-03-01"), "prev_trade_date": pd.Timestamp("2024-02-29"), "next_trade_date": pd.NaT, "is_month_end": False, "month": "2024-03"},
        ]
    )

    lake_store.overwrite_table("monthly_universe", monthly_universe)
    lake_store.overwrite_table("adjusted_price_panel", adjusted_price_panel)
    lake_store.overwrite_table("calendar_table", calendar_table)

    run_config = ResearchRunConfig(experiment_name="Label Build Test")
    initialize_experiment_layout(config, run_config)
    result = build_label_panel(config, run_config, label_names=("fwd_ret_1m",))

    panel_file = config.panel_data_root / run_config.experiment_slug / "label_panel" / "year=2024" / "month=01" / "data.parquet"
    manifest_file = config.experiments_root / run_config.experiment_slug / "labels" / "label_panel_manifest.json"

    assert result["rows_written"] == 2
    assert panel_file.exists()
    assert manifest_file.exists()

    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    assert manifest["label_names"] == ["fwd_ret_1m"]
