from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from src.config import AppConfig
from src.features.base import FeatureContext
from src.features.computation import DAILY_RISK_FACTOR_NAMES, DefaultFeatureBuilder
from src.features.registry import FACTOR_REGISTRY
from src.research.experiment import ResearchRunConfig, resolve_research_paths
from src.storage.parquet import ParquetDataStore


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _required_snapshot_columns(factor_names: tuple[str, ...]) -> list[str]:
    base_columns = {"rebalance_date", "trade_execution_date", "ts_code", "is_eligible", "exclude_reason", "year", "month", "adj_close"}
    for name in factor_names:
        spec = FACTOR_REGISTRY.get(name)
        if "size_neutralize" in spec.preprocess:
            base_columns.add("total_mv")
        if "industry_neutralize" in spec.preprocess:
            base_columns.add("market")
            base_columns.add("industry")
        for input_field in spec.inputs:
            if input_field.startswith("monthly_snapshot_base."):
                base_columns.add(input_field.split(".", 1)[1])
    return sorted(base_columns)


def _read_snapshot_with_optional_columns(lake_store: ParquetDataStore, columns: list[str]) -> pd.DataFrame:
    optional_columns = {"industry"}
    try:
        return lake_store.read_table("monthly_snapshot_base", columns=columns)
    except Exception as error:
        message = str(error)
        if not any(optional in message for optional in optional_columns):
            raise

        available_columns = [column for column in columns if column not in optional_columns]
        snapshot_df = lake_store.read_table("monthly_snapshot_base", columns=available_columns)
        for column in columns:
            if column not in snapshot_df.columns:
                snapshot_df[column] = pd.NA
        return snapshot_df.loc[:, columns]


def build_factor_panel_artifact(
    config: AppConfig,
    run_config: ResearchRunConfig,
    *,
    factor_names: tuple[str, ...] = (),
    dry_run: bool = False,
) -> dict[str, Any]:
    started_at = _now_iso()
    if factor_names:
        selected_specs = [FACTOR_REGISTRY.get(name) for name in factor_names]
    else:
        selected_specs = FACTOR_REGISTRY.list()
    selected_names = tuple(spec.name for spec in selected_specs)

    lake_store = ParquetDataStore(config.lake_data_root)
    panel_store = ParquetDataStore(config.panel_data_root)
    run_paths = resolve_research_paths(config, run_config)

    snapshot_columns = _required_snapshot_columns(selected_names)
    snapshot_df = _read_snapshot_with_optional_columns(lake_store, snapshot_columns)

    adjusted_price_df = lake_store.read_table("adjusted_price_panel", columns=["ts_code", "trade_date", "adj_close", "amount"]) if set(selected_names) & DAILY_RISK_FACTOR_NAMES else None
    if adjusted_price_df is None:
        adjusted_price_df = pd.DataFrame(columns=["ts_code", "trade_date", "adj_close", "amount"])

    context = FeatureContext(
        experiment_name=run_config.experiment_name,
        experiment_slug=run_config.experiment_slug,
        as_of_date=run_config.as_of_date,
        factor_names=selected_names,
    )
    builder = DefaultFeatureBuilder()
    factor_panel = builder.build(snapshot_df, adjusted_price_df, context)

    table_name = f"{run_config.experiment_slug}/{context.output_table_name}"
    output_paths: list[str] = []
    artifact_manifest_path = run_paths.stage_roots["features"] / "factor_panel_manifest.json"
    artifact_manifest = {
        "experiment_name": run_config.experiment_name,
        "experiment_slug": run_config.experiment_slug,
        "factor_names": list(selected_names),
        "output_table_name": table_name,
        "output_fields": [spec.output_field for spec in selected_specs],
        "preprocess_profiles": {spec.name: list(spec.preprocess) for spec in selected_specs},
        "created_at": _now_iso(),
    }

    if not dry_run:
        output_paths = panel_store.replace_by_month(
            table_name,
            factor_panel,
            partition_col="rebalance_date",
            primary_keys=["rebalance_date", "ts_code"],
        )
        artifact_manifest_path.write_text(json.dumps(artifact_manifest, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    return {
        "table_name": context.output_table_name,
        "experiment_name": run_config.experiment_name,
        "experiment_slug": run_config.experiment_slug,
        "rows_written": len(factor_panel),
        "factor_names": list(selected_names),
        "output_fields": [spec.output_field for spec in selected_specs],
        "panel_table": f"data/panel/{table_name}",
        "output_paths": output_paths,
        "artifact_manifest_path": str(artifact_manifest_path.relative_to(config.project_root)),
        "started_at": started_at,
        "finished_at": _now_iso(),
        "dry_run": dry_run,
    }
