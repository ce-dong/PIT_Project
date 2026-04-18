from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.config import AppConfig
from src.evaluation.ic import build_evaluation_input, build_rank_ic_tables
from src.evaluation.runner import build_rank_ic_artifact
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


def test_build_evaluation_input_filters_to_eligible_rows():
    factor_panel = pd.DataFrame(
        [
            {"rebalance_date": "2024-01-31", "ts_code": "AAA", "is_eligible": True, "factor_size": 1.0},
            {"rebalance_date": "2024-01-31", "ts_code": "BBB", "is_eligible": False, "factor_size": 2.0},
        ]
    )
    label_panel = pd.DataFrame(
        [
            {"rebalance_date": "2024-01-31", "ts_code": "AAA", "label_fwd_ret_1m": 0.1},
            {"rebalance_date": "2024-01-31", "ts_code": "BBB", "label_fwd_ret_1m": 0.2},
        ]
    )

    aligned = build_evaluation_input(
        factor_panel,
        label_panel,
        factor_fields=("factor_size",),
        label_fields=("label_fwd_ret_1m",),
    )

    assert aligned["ts_code"].tolist() == ["AAA"]


def test_build_rank_ic_tables_computes_timeseries_and_summary():
    panel_df = pd.DataFrame(
        [
            {"rebalance_date": "2024-01-31", "ts_code": "AAA", "factor_size": 1.0, "label_fwd_ret_1m": 0.1},
            {"rebalance_date": "2024-01-31", "ts_code": "BBB", "factor_size": 2.0, "label_fwd_ret_1m": 0.2},
            {"rebalance_date": "2024-01-31", "ts_code": "CCC", "factor_size": 3.0, "label_fwd_ret_1m": 0.3},
            {"rebalance_date": "2024-02-29", "ts_code": "AAA", "factor_size": 1.0, "label_fwd_ret_1m": 0.3},
            {"rebalance_date": "2024-02-29", "ts_code": "BBB", "factor_size": 2.0, "label_fwd_ret_1m": 0.2},
            {"rebalance_date": "2024-02-29", "ts_code": "CCC", "factor_size": 3.0, "label_fwd_ret_1m": 0.1},
        ]
    )
    panel_df["rebalance_date"] = pd.to_datetime(panel_df["rebalance_date"])

    ic_timeseries, ic_summary = build_rank_ic_tables(
        panel_df,
        factor_names=("size",),
        factor_fields=("factor_size",),
        label_names=("fwd_ret_1m",),
        label_fields=("label_fwd_ret_1m",),
    )

    assert ic_timeseries["rank_ic"].round(6).tolist() == [1.0, -1.0]
    assert ic_summary.loc[0, "observation_months"] == 2
    assert round(ic_summary.loc[0, "ic_mean"], 6) == 0.0
    assert round(ic_summary.loc[0, "ic_std"], 6) == round(2**0.5, 6)
    assert round(ic_summary.loc[0, "ic_hit_rate"], 6) == 0.5


def test_build_rank_ic_artifact_writes_output_and_manifest(tmp_path: Path):
    config = _make_config(tmp_path)
    config.ensure_directories()
    panel_store = ParquetDataStore(config.panel_data_root)

    run_config = ResearchRunConfig(experiment_name="Evaluation Build Test")
    initialize_experiment_layout(config, run_config)

    factor_panel = pd.DataFrame(
        [
            {"rebalance_date": pd.Timestamp("2024-01-31"), "ts_code": "AAA", "is_eligible": True, "factor_size": 1.0, "year": 2024, "month": 1},
            {"rebalance_date": pd.Timestamp("2024-01-31"), "ts_code": "BBB", "is_eligible": True, "factor_size": 2.0, "year": 2024, "month": 1},
            {"rebalance_date": pd.Timestamp("2024-02-29"), "ts_code": "AAA", "is_eligible": True, "factor_size": 1.0, "year": 2024, "month": 2},
            {"rebalance_date": pd.Timestamp("2024-02-29"), "ts_code": "BBB", "is_eligible": True, "factor_size": 2.0, "year": 2024, "month": 2},
        ]
    )
    label_panel = pd.DataFrame(
        [
            {"rebalance_date": pd.Timestamp("2024-01-31"), "ts_code": "AAA", "label_fwd_ret_1m": 0.1, "year": 2024, "month": 1},
            {"rebalance_date": pd.Timestamp("2024-01-31"), "ts_code": "BBB", "label_fwd_ret_1m": 0.2, "year": 2024, "month": 1},
            {"rebalance_date": pd.Timestamp("2024-02-29"), "ts_code": "AAA", "label_fwd_ret_1m": 0.2, "year": 2024, "month": 2},
            {"rebalance_date": pd.Timestamp("2024-02-29"), "ts_code": "BBB", "label_fwd_ret_1m": 0.1, "year": 2024, "month": 2},
        ]
    )

    panel_store.replace_by_month(
        f"{run_config.experiment_slug}/factor_panel",
        factor_panel,
        partition_col="rebalance_date",
        primary_keys=["rebalance_date", "ts_code"],
    )
    panel_store.replace_by_month(
        f"{run_config.experiment_slug}/label_panel",
        label_panel,
        partition_col="rebalance_date",
        primary_keys=["rebalance_date", "ts_code"],
    )

    feature_manifest = config.experiments_root / run_config.experiment_slug / "features" / "factor_panel_manifest.json"
    label_manifest = config.experiments_root / run_config.experiment_slug / "labels" / "label_panel_manifest.json"
    feature_manifest.write_text(
        json.dumps({"factor_names": ["size"]}, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    label_manifest.write_text(
        json.dumps({"label_names": ["fwd_ret_1m"]}, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )

    result = build_rank_ic_artifact(config, run_config)

    timeseries_path = config.experiments_root / run_config.experiment_slug / "evaluation" / "rank_ic_timeseries.parquet"
    summary_path = config.experiments_root / run_config.experiment_slug / "evaluation" / "rank_ic_summary.parquet"
    manifest_path = config.experiments_root / run_config.experiment_slug / "evaluation" / "rank_ic_manifest.json"

    assert result["timeseries_rows"] == 2
    assert result["summary_rows"] == 1
    assert timeseries_path.exists()
    assert summary_path.exists()
    assert manifest_path.exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["factor_names"] == ["size"]
    assert manifest["label_names"] == ["fwd_ret_1m"]
