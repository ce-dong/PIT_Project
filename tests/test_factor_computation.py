from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.config import AppConfig
from src.features.computation import build_factor_panel
from src.features.registry import FACTOR_REGISTRY
from src.features.runner import build_factor_panel_artifact
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


def test_build_factor_panel_computes_snapshot_and_lagged_factors():
    snapshot_df = pd.DataFrame(
        [
            {"rebalance_date": "2024-01-31", "trade_execution_date": "2024-02-01", "ts_code": "AAA", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 1, "adj_close": 10.0, "total_mv": 100.0, "pb": 2.0},
            {"rebalance_date": "2024-01-31", "trade_execution_date": "2024-02-01", "ts_code": "BBB", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 1, "adj_close": 20.0, "total_mv": 400.0, "pb": 4.0},
            {"rebalance_date": "2024-02-29", "trade_execution_date": "2024-03-01", "ts_code": "AAA", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 2, "adj_close": 11.0, "total_mv": 110.0, "pb": 2.2},
            {"rebalance_date": "2024-02-29", "trade_execution_date": "2024-03-01", "ts_code": "BBB", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 2, "adj_close": 18.0, "total_mv": 380.0, "pb": 3.8},
            {"rebalance_date": "2024-03-29", "trade_execution_date": "2024-04-01", "ts_code": "AAA", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 3, "adj_close": 12.0, "total_mv": 120.0, "pb": 2.4},
            {"rebalance_date": "2024-03-29", "trade_execution_date": "2024-04-01", "ts_code": "BBB", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 3, "adj_close": 21.0, "total_mv": 420.0, "pb": 4.2},
            {"rebalance_date": "2024-04-30", "trade_execution_date": "2024-05-06", "ts_code": "AAA", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 4, "adj_close": 13.0, "total_mv": 130.0, "pb": 2.6},
            {"rebalance_date": "2024-04-30", "trade_execution_date": "2024-05-06", "ts_code": "BBB", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 4, "adj_close": 19.0, "total_mv": 390.0, "pb": 3.9},
        ]
    )

    specs = FACTOR_REGISTRY.list(names=["size", "book_to_market", "reversal_1m", "momentum_3_1"])
    result = build_factor_panel(snapshot_df, pd.DataFrame(), specs)

    jan = result.loc[result["rebalance_date"] == pd.Timestamp("2024-01-31")]
    apr = result.loc[result["rebalance_date"] == pd.Timestamp("2024-04-30")].sort_values("ts_code").reset_index(drop=True)

    assert abs(jan["factor_size"].mean()) < 1e-12
    assert round(apr.loc[0, "factor_reversal_1m_raw"], 6) == -0.083333
    assert round(apr.loc[1, "factor_reversal_1m_raw"], 6) == 0.095238
    assert round(apr.loc[0, "factor_momentum_3_1_raw"], 6) == 0.2
    assert round(apr.loc[1, "factor_momentum_3_1_raw"], 6) == 0.05


def test_build_factor_panel_computes_daily_risk_factors():
    snapshot_df = pd.DataFrame(
        [
            {"rebalance_date": "2024-06-28", "trade_execution_date": "2024-07-01", "ts_code": "AAA", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 6, "adj_close": 10.0},
            {"rebalance_date": "2024-06-28", "trade_execution_date": "2024-07-01", "ts_code": "BBB", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 6, "adj_close": 20.0},
        ]
    )
    dates = pd.date_range("2023-07-01", periods=280, freq="B")
    adjusted_rows = []
    price_a = 10.0
    price_b = 20.0
    for i, trade_date in enumerate(dates):
        price_a *= 1.001 + (0.0002 if i % 2 == 0 else -0.0001)
        price_b *= 1.0005 + (0.0001 if i % 3 == 0 else -0.00005)
        adjusted_rows.append({"ts_code": "AAA", "trade_date": trade_date, "adj_close": price_a, "amount": 1000000 + i})
        adjusted_rows.append({"ts_code": "BBB", "trade_date": trade_date, "adj_close": price_b, "amount": 1200000 + i})
    adjusted_price_df = pd.DataFrame(adjusted_rows)

    specs = FACTOR_REGISTRY.list(names=["beta", "volatility", "amihud_illiquidity", "idiosyncratic_volatility"])
    result = build_factor_panel(snapshot_df, adjusted_price_df, specs)

    assert result["factor_beta_raw"].notna().all()
    assert result["factor_volatility_raw"].notna().all()
    assert result["factor_amihud_illiquidity_raw"].notna().all()
    assert result["factor_idiosyncratic_volatility_raw"].notna().all()


def test_build_factor_panel_artifact_writes_partitioned_output_and_manifest(tmp_path: Path):
    config = _make_config(tmp_path)
    config.ensure_directories()
    lake_store = ParquetDataStore(config.lake_data_root)

    snapshot_df = pd.DataFrame(
        [
            {"rebalance_date": pd.Timestamp("2024-01-31"), "trade_execution_date": pd.Timestamp("2024-02-01"), "ts_code": "AAA", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 1, "adj_close": 10.0, "total_mv": 100.0, "pb": 2.0},
            {"rebalance_date": pd.Timestamp("2024-01-31"), "trade_execution_date": pd.Timestamp("2024-02-01"), "ts_code": "BBB", "is_eligible": True, "exclude_reason": "", "year": 2024, "month": 1, "adj_close": 20.0, "total_mv": 400.0, "pb": 4.0},
        ]
    )

    lake_store.overwrite_table("monthly_snapshot_base", snapshot_df)

    run_config = ResearchRunConfig(experiment_name="Factor Build Test")
    initialize_experiment_layout(config, run_config)
    result = build_factor_panel_artifact(config, run_config, factor_names=("size", "book_to_market"))

    panel_file = config.panel_data_root / run_config.experiment_slug / "factor_panel" / "year=2024" / "month=01" / "data.parquet"
    manifest_file = config.experiments_root / run_config.experiment_slug / "features" / "factor_panel_manifest.json"

    assert result["rows_written"] == 2
    assert panel_file.exists()
    assert manifest_file.exists()

    manifest = json.loads(manifest_file.read_text(encoding="utf-8"))
    assert manifest["factor_names"] == ["size", "book_to_market"]
