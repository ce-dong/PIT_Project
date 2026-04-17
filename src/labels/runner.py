from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from src.config import AppConfig
from src.labels.base import LabelContext
from src.labels.forward_returns import ForwardReturnLabelBuilder
from src.labels.registry import LABEL_REGISTRY
from src.research.experiment import ResearchRunConfig, resolve_research_paths
from src.storage.parquet import ParquetDataStore


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_label_panel(
    config: AppConfig,
    run_config: ResearchRunConfig,
    *,
    label_names: tuple[str, ...] = (),
    dry_run: bool = False,
) -> dict[str, Any]:
    started_at = _now_iso()
    lake_store = ParquetDataStore(config.lake_data_root)
    panel_store = ParquetDataStore(config.panel_data_root)
    run_paths = resolve_research_paths(config, run_config)

    context = LabelContext(
        experiment_name=run_config.experiment_name,
        experiment_slug=run_config.experiment_slug,
        as_of_date=run_config.as_of_date,
        label_names=label_names,
    )

    selected_specs = LABEL_REGISTRY.list(names=label_names if label_names else None, stage="forward_return")
    monthly_universe = lake_store.read_table("monthly_universe")
    adjusted_price_panel = lake_store.read_table("adjusted_price_panel", columns=["ts_code", "trade_date", "adj_close"])
    calendar_table = lake_store.read_table("calendar_table")

    builder = ForwardReturnLabelBuilder()
    label_panel = builder.build(monthly_universe, adjusted_price_panel, calendar_table, context)

    table_name = f"{run_config.experiment_slug}/{context.output_table_name}"
    output_paths: list[str] = []
    artifact_manifest_path = run_paths.stage_roots["labels"] / "label_panel_manifest.json"

    artifact_manifest = {
        "experiment_name": run_config.experiment_name,
        "experiment_slug": run_config.experiment_slug,
        "label_names": [spec.name for spec in selected_specs],
        "output_table_name": table_name,
        "output_fields": [spec.output_field for spec in selected_specs],
        "created_at": _now_iso(),
    }

    if not dry_run:
        output_paths = panel_store.replace_by_month(
            table_name,
            label_panel,
            partition_col="rebalance_date",
            primary_keys=["rebalance_date", "ts_code"],
        )
        artifact_manifest_path.write_text(json.dumps(artifact_manifest, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    return {
        "table_name": context.output_table_name,
        "experiment_name": run_config.experiment_name,
        "experiment_slug": run_config.experiment_slug,
        "rows_written": len(label_panel),
        "label_names": [spec.name for spec in selected_specs],
        "output_fields": [spec.output_field for spec in selected_specs],
        "panel_table": f"data/panel/{table_name}",
        "output_paths": output_paths,
        "artifact_manifest_path": str(artifact_manifest_path.relative_to(config.project_root)),
        "started_at": started_at,
        "finished_at": _now_iso(),
        "dry_run": dry_run,
    }
