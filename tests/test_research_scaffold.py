from __future__ import annotations

import json
from pathlib import Path

from src.cli import build_parser
from src.config import AppConfig
from src.research.experiment import ResearchRunConfig, initialize_experiment_layout


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


def test_research_run_config_normalizes_name_and_date():
    config = ResearchRunConfig(experiment_name="Momentum Baseline V1", as_of_date="2026-04-16")

    assert config.experiment_name == "momentum_baseline_v1"
    assert config.as_of_date == "20260416"
    assert config.experiment_slug == "20260416__momentum_baseline_v1"


def test_initialize_experiment_layout_creates_stage_directories_and_manifest(tmp_path: Path):
    config = _make_config(tmp_path)
    config.ensure_directories()
    run_config = ResearchRunConfig(experiment_name="Cross Section Alpha")

    result = initialize_experiment_layout(config, run_config)

    experiment_root = tmp_path / result["experiment_root"]
    manifest_path = tmp_path / result["manifest_path"]

    assert experiment_root.exists()
    assert (experiment_root / "features").exists()
    assert (experiment_root / "labels").exists()
    assert (experiment_root / "evaluation").exists()
    assert (experiment_root / "reports").exists()
    assert manifest_path.exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["experiment_name"] == "cross_section_alpha"
    assert manifest["stage_contracts"]["features"] == [
        "monthly_universe",
        "monthly_snapshot_base",
        "adjusted_price_panel",
    ]


def test_cli_parser_accepts_research_init_subcommand():
    parser = build_parser()

    args = parser.parse_args(["research", "init", "--name", "Momentum Baseline", "--stage", "features"])

    assert args.command == "research"
    assert args.research_command == "init"
    assert args.name == "Momentum Baseline"
    assert args.stages == ["features"]


def test_cli_parser_accepts_research_factors_subcommand():
    parser = build_parser()

    args = parser.parse_args(["research", "factors", "--family", "momentum", "--name", "momentum_12_1"])

    assert args.command == "research"
    assert args.research_command == "factors"
    assert args.family == "momentum"
    assert args.factor_names == ["momentum_12_1"]
