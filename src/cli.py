from __future__ import annotations

import argparse
import json

from src.adapters.tushare.client import TushareClient
from src.config import AppConfig
from src.storage.parquet import ParquetDataStore
from src.storage.state import IngestionStateStore
from src.updaters.base import UpdateContext
from src.updaters.registry import CORE_TABLE_ORDER, UPDATER_REGISTRY


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
    return parser


def _build_runtime() -> tuple[AppConfig, TushareClient, ParquetDataStore, IngestionStateStore]:
    config = AppConfig.load()
    config.ensure_directories()
    client = TushareClient(
        token=config.tushare_token,
        retry_attempts=config.retry_attempts,
        sleep_seconds=config.request_sleep_seconds,
    )
    store = ParquetDataStore(config.raw_data_root)
    state_store = IngestionStateStore(config.metadata_root / "ingestion_state.json")
    return config, client, store, state_store


def run_ingest(args: argparse.Namespace) -> int:
    config, client, store, state_store = _build_runtime()
    tables = CORE_TABLE_ORDER if args.table == "all" else [args.table]
    results = []
    for table_name in tables:
        updater_cls = UPDATER_REGISTRY[table_name]
        updater = updater_cls(config, client, store, state_store)
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


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "ingest":
        return run_ingest(args)
    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

