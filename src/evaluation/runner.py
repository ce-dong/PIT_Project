from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import AppConfig
from src.evaluation.base import EvaluationContext
from src.evaluation.ic import RankICEvaluator, build_evaluation_input
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

    evaluation_root = run_paths.stage_roots["evaluation"]
    timeseries_path = evaluation_root / "rank_ic_timeseries.parquet"
    summary_path = evaluation_root / "rank_ic_summary.parquet"
    manifest_path = evaluation_root / "rank_ic_manifest.json"
    output_paths: list[str] = []

    manifest = {
        "experiment_name": run_config.experiment_name,
        "experiment_slug": run_config.experiment_slug,
        "metric": evaluator.name,
        "factor_names": list(context.factor_names),
        "label_names": list(context.label_names),
        "timeseries_path": str(timeseries_path),
        "summary_path": str(summary_path),
        "created_at": _now_iso(),
    }

    if not dry_run:
        ic_timeseries.to_parquet(timeseries_path, index=False)
        ic_summary.to_parquet(summary_path, index=False)
        manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
        output_paths = [
            str(timeseries_path.relative_to(config.project_root)),
            str(summary_path.relative_to(config.project_root)),
        ]

    return {
        "metric": evaluator.name,
        "experiment_name": run_config.experiment_name,
        "experiment_slug": run_config.experiment_slug,
        "aligned_rows": len(aligned_panel),
        "timeseries_rows": len(ic_timeseries),
        "summary_rows": len(ic_summary),
        "factor_names": list(context.factor_names),
        "label_names": list(context.label_names),
        "output_paths": output_paths,
        "timeseries_path": str(timeseries_path.relative_to(config.project_root)),
        "summary_path": str(summary_path.relative_to(config.project_root)),
        "artifact_manifest_path": str(manifest_path.relative_to(config.project_root)),
        "started_at": started_at,
        "finished_at": _now_iso(),
        "dry_run": dry_run,
    }
