from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from src.config import AppConfig
from src.reports.markdown import render_research_report
from src.reports.runner import build_research_report
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


def _sample_payload() -> dict[str, object]:
    return {
        "manifest": {"factor_names": ["size", "value"], "label_names": ["fwd_ret_1m"]},
        "factor_manifest": {
            "preprocess_profiles": {
                "size": ["winsorize", "industry_neutralize", "zscore"],
                "value": ["winsorize", "industry_neutralize", "size_neutralize", "zscore"],
            }
        },
        "chart_paths": {
            "ic_leaderboard": "ic_leaderboard.png",
            "spread_leaderboard": "spread_leaderboard.png",
            "correlation_heatmap": "factor_correlation_heatmap.png",
            "robustness_consistency": "robustness_consistency.png",
        },
        "evaluation_summary": pd.DataFrame(
            [
                {"factor_name": "size", "label_name": "fwd_ret_1m", "ic_mean": 0.12, "icir": 0.8, "spread_mean": 0.03, "spread_ir": 0.5, "mean_is_monotonic": True, "monotonic_hit_rate": 0.7},
                {"factor_name": "value", "label_name": "fwd_ret_1m", "ic_mean": -0.05, "icir": -0.4, "spread_mean": -0.01, "spread_ir": -0.2, "mean_is_monotonic": False, "monotonic_hit_rate": 0.3},
            ]
        ),
        "fama_macbeth_summary": pd.DataFrame(
            [
                {"term_name": "size", "label_name": "fwd_ret_1m", "coef_mean": 0.02, "t_stat": 2.1, "positive_ratio": 0.7},
                {"term_name": "value", "label_name": "fwd_ret_1m", "coef_mean": -0.01, "t_stat": -1.0, "positive_ratio": 0.4},
            ]
        ),
        "factor_correlation_summary": pd.DataFrame(
            [
                {"left_factor_name": "size", "right_factor_name": "value", "mean_correlation": 0.6, "mean_abs_correlation": 0.6},
            ]
        ),
        "factor_correlation_matrix": pd.DataFrame(
            [
                {"factor_name": "size", "size": 1.0, "value": 0.6},
                {"factor_name": "value", "size": 0.6, "value": 1.0},
            ]
        ),
        "redundancy_summary": pd.DataFrame(
            [
                {"factor_name": "size", "label_name": "fwd_ret_1m", "mean_r2": 0.2, "residual_ic_mean": 0.08, "residual_spread_mean": 0.02},
            ]
        ),
        "robustness_summary": pd.DataFrame(
            [
                {"factor_name": "size", "label_name": "fwd_ret_1m", "ic_sign_consistent_ratio": 1.0, "spread_sign_consistent_ratio": 0.67},
            ]
        ),
        "subperiod_summary": pd.DataFrame(
            [
                {"factor_name": "size", "label_name": "fwd_ret_1m", "period_label": "P1", "ic_mean": 0.10, "spread_mean": 0.02},
                {"factor_name": "value", "label_name": "fwd_ret_1m", "period_label": "P2", "ic_mean": 0.08, "spread_mean": 0.01},
            ]
        ),
    }


def test_render_research_report_includes_key_sections():
    from src.reports.base import ReportContext

    report = render_research_report(
        _sample_payload(),
        generated_at=pd.Timestamp("2026-04-19T00:00:00Z").to_pydatetime(),
        context=ReportContext(experiment_name="agent2", experiment_slug="agent2_baseline"),
    )

    assert "# Factor Research Report" in report
    assert "## Key Takeaways" in report
    assert "## Methodology Snapshot" in report
    assert "## Strongest IC Signals" in report
    assert "## Robustness Leaders" in report
    assert "## Highest Correlation Pairs" in report
    assert "## Charts" in report
    assert "`agent2_baseline`" in report
    assert "size" in report


def test_build_research_report_writes_markdown_and_manifest(tmp_path: Path):
    config = _make_config(tmp_path)
    config.ensure_directories()
    run_config = ResearchRunConfig(experiment_name="Report Build Test")
    initialize_experiment_layout(config, run_config)

    evaluation_root = config.experiments_root / run_config.experiment_slug / "evaluation"
    reports_root = config.experiments_root / run_config.experiment_slug / "reports"
    evaluation_root.mkdir(parents=True, exist_ok=True)
    reports_root.mkdir(parents=True, exist_ok=True)

    (evaluation_root / "rank_ic_manifest.json").write_text(
        json.dumps({"factor_names": ["size", "value"], "label_names": ["fwd_ret_1m"]}, indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    feature_manifest_root = config.experiments_root / run_config.experiment_slug / "features"
    feature_manifest_root.mkdir(parents=True, exist_ok=True)
    (feature_manifest_root / "factor_panel_manifest.json").write_text(
        json.dumps(_sample_payload()["factor_manifest"], indent=2, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )
    _sample_payload()["evaluation_summary"].to_parquet(evaluation_root / "evaluation_summary.parquet", index=False)
    _sample_payload()["fama_macbeth_summary"].to_parquet(evaluation_root / "fama_macbeth_summary.parquet", index=False)
    _sample_payload()["factor_correlation_summary"].to_parquet(evaluation_root / "factor_correlation_summary.parquet", index=False)
    _sample_payload()["factor_correlation_matrix"].to_parquet(evaluation_root / "factor_correlation_matrix.parquet", index=False)
    _sample_payload()["redundancy_summary"].to_parquet(evaluation_root / "redundancy_summary.parquet", index=False)
    _sample_payload()["robustness_summary"].to_parquet(evaluation_root / "robustness_summary.parquet", index=False)
    _sample_payload()["subperiod_summary"].to_parquet(evaluation_root / "subperiod_summary.parquet", index=False)

    result = build_research_report(config, run_config)

    report_path = config.experiments_root / run_config.experiment_slug / "reports" / "research_report.md"
    manifest_path = config.experiments_root / run_config.experiment_slug / "reports" / "report_manifest.json"
    ic_chart_path = config.experiments_root / run_config.experiment_slug / "reports" / "ic_leaderboard.png"
    assert report_path.exists()
    assert manifest_path.exists()
    assert ic_chart_path.exists()
    assert result["report_builder"] == "markdown"
    assert any(path.endswith("ic_leaderboard.png") for path in result["output_paths"])
    assert "Key Takeaways" in report_path.read_text(encoding="utf-8")
    assert "Strongest IC Signals" in report_path.read_text(encoding="utf-8")
