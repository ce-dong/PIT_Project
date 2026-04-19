from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import AppConfig
from src.evaluation.base import EvaluationContext
from src.evaluation.correlation import build_factor_correlation_tables
from src.evaluation.fama_macbeth import build_fama_macbeth_tables
from src.evaluation.ic import RankICEvaluator, build_evaluation_input
from src.evaluation.portfolio import build_quantile_portfolio_tables
from src.evaluation.redundancy import build_redundancy_tables
from src.evaluation.robustness import build_subperiod_robustness_tables
from src.evaluation.summary import build_evaluation_summary, build_monotonicity_summary
from src.features.registry import FACTOR_REGISTRY, FactorSpec
from src.labels.registry import LABEL_REGISTRY, LabelSpec
from src.research.experiment import ResearchRunConfig, resolve_research_paths
from src.storage.parquet import ParquetDataStore


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Required artifact manifest is missing: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_factor_specs(
    manifest_path: Path,
    factor_names: tuple[str, ...],
) -> list[FactorSpec]:
    if factor_names:
        return FACTOR_REGISTRY.list(names=factor_names)
    manifest = _load_manifest(manifest_path)
    return FACTOR_REGISTRY.list(names=manifest["factor_names"])


def _resolve_label_specs(
    manifest_path: Path,
    label_names: tuple[str, ...],
) -> list[LabelSpec]:
    if label_names:
        return LABEL_REGISTRY.list(names=label_names)
    manifest = _load_manifest(manifest_path)
    return LABEL_REGISTRY.list(names=manifest["label_names"])


def build_rank_ic_artifact(
    config: AppConfig,
    run_config: ResearchRunConfig,
    *,
    factor_names: tuple[str, ...] = (),
    label_names: tuple[str, ...] = (),
    dry_run: bool = False,
) -> dict[str, Any]:
    started_at = _now_iso()
    panel_store = ParquetDataStore(config.panel_data_root)
    run_paths = resolve_research_paths(config, run_config)

    factor_specs = _resolve_factor_specs(run_paths.stage_roots["features"] / "factor_panel_manifest.json", factor_names)
    label_specs = _resolve_label_specs(run_paths.stage_roots["labels"] / "label_panel_manifest.json", label_names)
    factor_fields = tuple(spec.output_field for spec in factor_specs)
    label_fields = tuple(spec.output_field for spec in label_specs)

    factor_panel = panel_store.read_table(
        f"{run_config.experiment_slug}/factor_panel",
        columns=["rebalance_date", "ts_code", "is_eligible", *factor_fields],
    )
    label_panel = panel_store.read_table(
        f"{run_config.experiment_slug}/label_panel",
        columns=["rebalance_date", "ts_code", *label_fields],
    )

    aligned_panel = build_evaluation_input(
        factor_panel,
        label_panel,
        factor_fields=factor_fields,
        label_fields=label_fields,
    )
    context = EvaluationContext(
        experiment_name=run_config.experiment_name,
        experiment_slug=run_config.experiment_slug,
        as_of_date=run_config.as_of_date,
        factor_names=tuple(spec.name for spec in factor_specs),
        factor_fields=factor_fields,
        label_names=tuple(spec.name for spec in label_specs),
        label_fields=label_fields,
    )

    evaluator = RankICEvaluator()
    evaluation_result = evaluator.evaluate(aligned_panel, context)
    ic_timeseries = evaluation_result["ic_timeseries"]
    ic_summary = evaluation_result["ic_summary"]
    quantile_timeseries, quantile_summary, spread_timeseries, spread_summary = build_quantile_portfolio_tables(
        aligned_panel,
        factor_names=context.factor_names,
        factor_fields=context.factor_fields,
        label_names=context.label_names,
        label_fields=context.label_fields,
        quantile_count=context.quantile_count,
    )
    monotonicity_summary = build_monotonicity_summary(
        quantile_timeseries,
        quantile_summary,
        quantile_count=context.quantile_count,
    )
    evaluation_summary = build_evaluation_summary(
        ic_summary,
        spread_summary,
        monotonicity_summary,
    )
    factor_correlation_timeseries, factor_correlation_summary, factor_correlation_matrix = build_factor_correlation_tables(
        factor_panel,
        factor_names=context.factor_names,
        factor_fields=context.factor_fields,
    )
    redundancy_timeseries, redundancy_summary = build_redundancy_tables(
        aligned_panel,
        factor_names=context.factor_names,
        factor_fields=context.factor_fields,
        label_names=context.label_names,
        label_fields=context.label_fields,
        quantile_count=context.quantile_count,
        evaluation_summary=evaluation_summary,
    )
    robustness_periods, subperiod_summary, robustness_summary = build_subperiod_robustness_tables(
        ic_timeseries,
        spread_timeseries,
        evaluation_summary,
    )
    fama_macbeth_timeseries, fama_macbeth_summary = build_fama_macbeth_tables(
        aligned_panel,
        factor_names=context.factor_names,
        factor_fields=context.factor_fields,
        label_names=context.label_names,
        label_fields=context.label_fields,
    )

    evaluation_root = run_paths.stage_roots["evaluation"]
    timeseries_path = evaluation_root / "rank_ic_timeseries.parquet"
    summary_path = evaluation_root / "rank_ic_summary.parquet"
    quantile_timeseries_path = evaluation_root / "quantile_returns.parquet"
    quantile_summary_path = evaluation_root / "quantile_summary.parquet"
    spread_timeseries_path = evaluation_root / "top_bottom_spread_timeseries.parquet"
    spread_summary_path = evaluation_root / "top_bottom_spread_summary.parquet"
    monotonicity_summary_path = evaluation_root / "monotonicity_summary.parquet"
    evaluation_summary_path = evaluation_root / "evaluation_summary.parquet"
    factor_correlation_timeseries_path = evaluation_root / "factor_correlation_timeseries.parquet"
    factor_correlation_summary_path = evaluation_root / "factor_correlation_summary.parquet"
    factor_correlation_matrix_path = evaluation_root / "factor_correlation_matrix.parquet"
    redundancy_timeseries_path = evaluation_root / "redundancy_timeseries.parquet"
    redundancy_summary_path = evaluation_root / "redundancy_summary.parquet"
    robustness_periods_path = evaluation_root / "robustness_periods.parquet"
    subperiod_summary_path = evaluation_root / "subperiod_summary.parquet"
    robustness_summary_path = evaluation_root / "robustness_summary.parquet"
    fama_macbeth_timeseries_path = evaluation_root / "fama_macbeth_timeseries.parquet"
    fama_macbeth_summary_path = evaluation_root / "fama_macbeth_summary.parquet"
    manifest_path = evaluation_root / "rank_ic_manifest.json"
    output_paths: list[str] = []

    manifest = {
        "experiment_name": run_config.experiment_name,
        "experiment_slug": run_config.experiment_slug,
        "metrics": [
            evaluator.name,
            "quantile_portfolio",
            "top_bottom_spread",
            "monotonicity_check",
            "evaluation_summary",
            "factor_correlation",
            "redundancy_analysis",
            "subperiod_robustness",
            "fama_macbeth",
        ],
        "factor_names": list(context.factor_names),
        "label_names": list(context.label_names),
        "quantile_count": context.quantile_count,
        "timeseries_path": str(timeseries_path),
        "summary_path": str(summary_path),
        "quantile_timeseries_path": str(quantile_timeseries_path),
        "quantile_summary_path": str(quantile_summary_path),
        "spread_timeseries_path": str(spread_timeseries_path),
        "spread_summary_path": str(spread_summary_path),
        "monotonicity_summary_path": str(monotonicity_summary_path),
        "evaluation_summary_path": str(evaluation_summary_path),
        "factor_correlation_timeseries_path": str(factor_correlation_timeseries_path),
        "factor_correlation_summary_path": str(factor_correlation_summary_path),
        "factor_correlation_matrix_path": str(factor_correlation_matrix_path),
        "redundancy_timeseries_path": str(redundancy_timeseries_path),
        "redundancy_summary_path": str(redundancy_summary_path),
        "robustness_periods_path": str(robustness_periods_path),
        "subperiod_summary_path": str(subperiod_summary_path),
        "robustness_summary_path": str(robustness_summary_path),
        "fama_macbeth_timeseries_path": str(fama_macbeth_timeseries_path),
        "fama_macbeth_summary_path": str(fama_macbeth_summary_path),
        "created_at": _now_iso(),
    }

    if not dry_run:
        ic_timeseries.to_parquet(timeseries_path, index=False)
        ic_summary.to_parquet(summary_path, index=False)
        quantile_timeseries.to_parquet(quantile_timeseries_path, index=False)
        quantile_summary.to_parquet(quantile_summary_path, index=False)
        spread_timeseries.to_parquet(spread_timeseries_path, index=False)
        spread_summary.to_parquet(spread_summary_path, index=False)
        monotonicity_summary.to_parquet(monotonicity_summary_path, index=False)
        evaluation_summary.to_parquet(evaluation_summary_path, index=False)
        factor_correlation_timeseries.to_parquet(factor_correlation_timeseries_path, index=False)
        factor_correlation_summary.to_parquet(factor_correlation_summary_path, index=False)
        factor_correlation_matrix.to_parquet(factor_correlation_matrix_path, index=False)
        redundancy_timeseries.to_parquet(redundancy_timeseries_path, index=False)
        redundancy_summary.to_parquet(redundancy_summary_path, index=False)
        robustness_periods.to_parquet(robustness_periods_path, index=False)
        subperiod_summary.to_parquet(subperiod_summary_path, index=False)
        robustness_summary.to_parquet(robustness_summary_path, index=False)
        fama_macbeth_timeseries.to_parquet(fama_macbeth_timeseries_path, index=False)
        fama_macbeth_summary.to_parquet(fama_macbeth_summary_path, index=False)
        manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
        output_paths = [
            str(timeseries_path.relative_to(config.project_root)),
            str(summary_path.relative_to(config.project_root)),
            str(quantile_timeseries_path.relative_to(config.project_root)),
            str(quantile_summary_path.relative_to(config.project_root)),
            str(spread_timeseries_path.relative_to(config.project_root)),
            str(spread_summary_path.relative_to(config.project_root)),
            str(monotonicity_summary_path.relative_to(config.project_root)),
            str(evaluation_summary_path.relative_to(config.project_root)),
            str(factor_correlation_timeseries_path.relative_to(config.project_root)),
            str(factor_correlation_summary_path.relative_to(config.project_root)),
            str(factor_correlation_matrix_path.relative_to(config.project_root)),
            str(redundancy_timeseries_path.relative_to(config.project_root)),
            str(redundancy_summary_path.relative_to(config.project_root)),
            str(robustness_periods_path.relative_to(config.project_root)),
            str(subperiod_summary_path.relative_to(config.project_root)),
            str(robustness_summary_path.relative_to(config.project_root)),
            str(fama_macbeth_timeseries_path.relative_to(config.project_root)),
            str(fama_macbeth_summary_path.relative_to(config.project_root)),
        ]

    return {
        "metric": evaluator.name,
        "experiment_name": run_config.experiment_name,
        "experiment_slug": run_config.experiment_slug,
        "aligned_rows": len(aligned_panel),
        "timeseries_rows": len(ic_timeseries),
        "summary_rows": len(ic_summary),
        "quantile_timeseries_rows": len(quantile_timeseries),
        "quantile_summary_rows": len(quantile_summary),
        "spread_timeseries_rows": len(spread_timeseries),
        "spread_summary_rows": len(spread_summary),
        "monotonicity_summary_rows": len(monotonicity_summary),
        "evaluation_summary_rows": len(evaluation_summary),
        "factor_correlation_timeseries_rows": len(factor_correlation_timeseries),
        "factor_correlation_summary_rows": len(factor_correlation_summary),
        "factor_correlation_matrix_rows": len(factor_correlation_matrix),
        "redundancy_timeseries_rows": len(redundancy_timeseries),
        "redundancy_summary_rows": len(redundancy_summary),
        "robustness_periods_rows": len(robustness_periods),
        "subperiod_summary_rows": len(subperiod_summary),
        "robustness_summary_rows": len(robustness_summary),
        "fama_macbeth_timeseries_rows": len(fama_macbeth_timeseries),
        "fama_macbeth_summary_rows": len(fama_macbeth_summary),
        "factor_names": list(context.factor_names),
        "label_names": list(context.label_names),
        "quantile_count": context.quantile_count,
        "output_paths": output_paths,
        "timeseries_path": str(timeseries_path.relative_to(config.project_root)),
        "summary_path": str(summary_path.relative_to(config.project_root)),
        "quantile_timeseries_path": str(quantile_timeseries_path.relative_to(config.project_root)),
        "quantile_summary_path": str(quantile_summary_path.relative_to(config.project_root)),
        "spread_timeseries_path": str(spread_timeseries_path.relative_to(config.project_root)),
        "spread_summary_path": str(spread_summary_path.relative_to(config.project_root)),
        "monotonicity_summary_path": str(monotonicity_summary_path.relative_to(config.project_root)),
        "evaluation_summary_path": str(evaluation_summary_path.relative_to(config.project_root)),
        "factor_correlation_timeseries_path": str(factor_correlation_timeseries_path.relative_to(config.project_root)),
        "factor_correlation_summary_path": str(factor_correlation_summary_path.relative_to(config.project_root)),
        "factor_correlation_matrix_path": str(factor_correlation_matrix_path.relative_to(config.project_root)),
        "redundancy_timeseries_path": str(redundancy_timeseries_path.relative_to(config.project_root)),
        "redundancy_summary_path": str(redundancy_summary_path.relative_to(config.project_root)),
        "robustness_periods_path": str(robustness_periods_path.relative_to(config.project_root)),
        "subperiod_summary_path": str(subperiod_summary_path.relative_to(config.project_root)),
        "robustness_summary_path": str(robustness_summary_path.relative_to(config.project_root)),
        "fama_macbeth_timeseries_path": str(fama_macbeth_timeseries_path.relative_to(config.project_root)),
        "fama_macbeth_summary_path": str(fama_macbeth_summary_path.relative_to(config.project_root)),
        "artifact_manifest_path": str(manifest_path.relative_to(config.project_root)),
        "started_at": started_at,
        "finished_at": _now_iso(),
        "dry_run": dry_run,
    }
