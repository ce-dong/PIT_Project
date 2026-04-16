from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from src.adapters.tushare.client import TushareClient
from src.builders.base import BuildContext
from src.builders.registry import BUILDER_REGISTRY, BUILD_ORDER
from src.config import AppConfig
from src.features.registry import FACTOR_REGISTRY
from src.research.experiment import RESEARCH_STAGE_ORDER, ResearchRunConfig, initialize_experiment_layout
from src.storage.parquet import ParquetDataStore
from src.storage.state import IngestionStateStore
from src.updaters.base import UpdateContext
from src.updaters.registry import CORE_TABLE_ORDER, UPDATER_REGISTRY
from src.validators.core import CORE_VALIDATION_ORDER, run_core_validations
from src.validators.reporting import write_validation_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="A-share PIT platform CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser("ingest", help="Run raw data ingestion.")
    ingest_parser.add_argument("table", choices=[*CORE_TABLE_ORDER, "all"], help="Table to ingest.")
    ingest_parser.add_argument("--start", dest="start_date", help="Override start date in YYYYMMDD.")
    ingest_parser.add_argument("--end", dest="end_date", help="Override end date in YYYYMMDD.")
    ingest_parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Ignore watermarks and rebuild the selected table from the configured start date.",
    )
    ingest_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch metadata and Tushare rows without writing parquet output or state.",
    )

    build_parser = subparsers.add_parser("build", help="Build standardized lake tables from raw data.")
    build_parser.add_argument("table", choices=[*BUILD_ORDER, "all"], help="Derived table to build.")
    build_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run transformations without writing output tables.",
    )

    validate_parser = subparsers.add_parser("validate", help="Run data-quality validation on core lake tables.")
    validate_parser.add_argument(
        "table",
        choices=[*CORE_VALIDATION_ORDER, "all"],
        help="Core lake table to validate.",
    )
    validate_parser.add_argument(
        "--write-report",
        action="store_true",
        help="Write a markdown validation report under data/reports.",
    )

    research_parser = subparsers.add_parser("research", help="Manage downstream factor-research experiments.")
    research_subparsers = research_parser.add_subparsers(dest="research_command", required=True)

    init_parser = research_subparsers.add_parser("init", help="Initialize the Agent 2 experiment scaffold.")
    init_parser.add_argument("--name", required=True, help="Human-readable experiment name.")
    init_parser.add_argument(
        "--as-of",
        dest="as_of_date",
        help="Optional as-of date for the experiment namespace in YYYYMMDD or YYYY-MM-DD.",
    )
    init_parser.add_argument(
        "--stage",
        dest="stages",
        action="append",
        choices=list(RESEARCH_STAGE_ORDER),
        help="Optional stage to include. Repeat to narrow the scaffold to a subset of stages.",
    )
    init_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview the experiment scaffold without writing directories or a manifest.",
    )

    factors_parser = research_subparsers.add_parser("factors", help="Inspect registered factor definitions.")
    factors_parser.add_argument(
        "--family",
        choices=sorted(FACTOR_REGISTRY.families()),
        help="Optionally filter the registry by factor family.",
    )
    factors_parser.add_argument(
        "--name",
        dest="factor_names",
        action="append",
        help="Optionally request one or more specific factors by name.",
    )
    return parser


def _build_runtime() -> tuple[AppConfig, TushareClient, ParquetDataStore, ParquetDataStore, IngestionStateStore]:
    config = AppConfig.load()
    config.ensure_directories()
    client = TushareClient(
        token=config.tushare_token,
        retry_attempts=config.retry_attempts,
        sleep_seconds=config.request_sleep_seconds,
    )
    raw_store = ParquetDataStore(config.raw_data_root)
    lake_store = ParquetDataStore(config.lake_data_root)
    state_store = IngestionStateStore(config.metadata_root / "ingestion_state.json")
    return config, client, raw_store, lake_store, state_store


def _load_config(*, require_tushare_token: bool = True) -> AppConfig:
    config = AppConfig.load(require_tushare_token=require_tushare_token)
    config.ensure_directories()
    return config


def run_ingest(args: argparse.Namespace) -> int:
    config, client, raw_store, _, state_store = _build_runtime()
    tables = CORE_TABLE_ORDER if args.table == "all" else [args.table]
    results = []
    for table_name in tables:
        updater_cls = UPDATER_REGISTRY[table_name]
        updater = updater_cls(config, client, raw_store, state_store)
        result = updater.run(
            UpdateContext(
                start_date=args.start_date,
                end_date=args.end_date,
                full_refresh=args.full_refresh,
                dry_run=args.dry_run,
            )
        )
        results.append(result)

    print(json.dumps(results, indent=2, ensure_ascii=True, default=str))
    return 0


def run_build(args: argparse.Namespace) -> int:
    config, _, raw_store, lake_store, _ = _build_runtime()
    tables = BUILD_ORDER if args.table == "all" else [args.table]
    results = []
    for table_name in tables:
        builder_cls = BUILDER_REGISTRY[table_name]
        builder = builder_cls(config, raw_store, lake_store)
        result = builder.run(BuildContext(dry_run=args.dry_run))
        results.append(result)

    print(json.dumps(results, indent=2, ensure_ascii=True, default=str))
    return 0


def run_validate(args: argparse.Namespace) -> int:
    config, _, _, lake_store, _ = _build_runtime()
    tables = CORE_VALIDATION_ORDER if args.table == "all" else [args.table]
    results = run_core_validations(config, lake_store, tables=tables)
    if args.write_report:
        timestamp = datetime.now(timezone.utc)
        write_validation_report(
            config.reports_root / "latest_quality_report.md",
            results,
            generated_at=timestamp,
            command=f"python -m src.cli validate {args.table}",
        )
        write_validation_report(
            config.reports_root / f"quality_report_{timestamp.strftime('%Y%m%dT%H%M%SZ')}.md",
            results,
            generated_at=timestamp,
            command=f"python -m src.cli validate {args.table}",
        )
    print(json.dumps([result.to_dict() for result in results], indent=2, ensure_ascii=True, default=str))
    has_error = any(result.error_count > 0 for result in results)
    return 1 if has_error else 0


def run_research(args: argparse.Namespace) -> int:
    config = _load_config(require_tushare_token=False)
    if args.research_command == "init":
        run_config = ResearchRunConfig(
            experiment_name=args.name,
            as_of_date=args.as_of_date,
            stages=tuple(args.stages) if args.stages else RESEARCH_STAGE_ORDER,
        )
        result = initialize_experiment_layout(config, run_config, dry_run=args.dry_run)
        print(json.dumps(result, indent=2, ensure_ascii=True, default=str))
        return 0
    if args.research_command == "factors":
        specs = FACTOR_REGISTRY.list(names=args.factor_names, family=args.family)
        payload = {
            "count": len(specs),
            "families": sorted({spec.family for spec in specs}),
            "factors": [spec.to_dict() for spec in specs],
        }
        print(json.dumps(payload, indent=2, ensure_ascii=True, default=str))
        return 0
    raise ValueError(f"Unsupported research subcommand: {args.research_command}")


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "ingest":
        return run_ingest(args)
    if args.command == "build":
        return run_build(args)
    if args.command == "validate":
        return run_validate(args)
    if args.command == "research":
        return run_research(args)
    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
