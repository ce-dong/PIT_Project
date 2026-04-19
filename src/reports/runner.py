from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.config import AppConfig
from src.reports.base import ReportContext
from src.reports.markdown import MarkdownResearchReportBuilder
from src.research.experiment import ResearchRunConfig, resolve_research_paths


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required report input is missing: {path}")
    return pd.read_parquet(path)


def build_research_report(
    config: AppConfig,
    run_config: ResearchRunConfig,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    started_at = _now_iso()
    run_paths = resolve_research_paths(config, run_config)
    evaluation_root = run_paths.stage_roots["evaluation"]
    reports_root = run_paths.stage_roots["reports"]

    manifest_path = evaluation_root / "rank_ic_manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Required evaluation manifest is missing: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    payload = {
        "manifest": manifest,
        "evaluation_summary": _read_parquet(evaluation_root / "evaluation_summary.parquet"),
        "fama_macbeth_summary": _read_parquet(evaluation_root / "fama_macbeth_summary.parquet"),
        "factor_correlation_summary": _read_parquet(evaluation_root / "factor_correlation_summary.parquet"),
        "redundancy_summary": _read_parquet(evaluation_root / "redundancy_summary.parquet"),
        "robustness_summary": _read_parquet(evaluation_root / "robustness_summary.parquet"),
    }
    context = ReportContext(
        experiment_name=run_config.experiment_name,
        experiment_slug=run_config.experiment_slug,
        as_of_date=run_config.as_of_date,
    )
    builder = MarkdownResearchReportBuilder()
    report_text = builder.build(payload, context)

    report_path = reports_root / context.output_file_name
    report_manifest_path = reports_root / "report_manifest.json"
    output_paths: list[str] = []
    report_manifest = {
        "experiment_name": run_config.experiment_name,
        "experiment_slug": run_config.experiment_slug,
        "report_builder": builder.name,
        "report_path": str(report_path),
        "generated_at": _now_iso(),
    }

    if not dry_run:
        reports_root.mkdir(parents=True, exist_ok=True)
        report_path.write_text(report_text, encoding="utf-8")
        report_manifest_path.write_text(json.dumps(report_manifest, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
        output_paths = [
            str(report_path.relative_to(config.project_root)),
            str(report_manifest_path.relative_to(config.project_root)),
        ]

    return {
        "experiment_name": run_config.experiment_name,
        "experiment_slug": run_config.experiment_slug,
        "report_builder": builder.name,
        "report_path": str(report_path.relative_to(config.project_root)),
        "artifact_manifest_path": str(report_manifest_path.relative_to(config.project_root)),
        "output_paths": output_paths,
        "report_preview": report_text[:500],
        "started_at": started_at,
        "finished_at": _now_iso(),
        "dry_run": dry_run,
    }
