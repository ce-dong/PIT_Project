from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from src.config import AppConfig
from src.logging_utils import get_logger
from src.storage.parquet import ParquetDataStore


@dataclass
class BuildContext:
    dry_run: bool = False


class BaseBuilder:
    table_name = ""

    def __init__(self, config: AppConfig, raw_store: ParquetDataStore, lake_store: ParquetDataStore) -> None:
        self.config = config
        self.raw_store = raw_store
        self.lake_store = lake_store
        self.logger = get_logger(self.__class__.__name__, config.log_root)

    def run(self, context: BuildContext) -> dict[str, Any]:
        raise NotImplementedError

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _result(
        self,
        *,
        started_at: str,
        rows_written: int,
        output_paths: list[str],
        dry_run: bool,
    ) -> dict[str, Any]:
        return {
            "table_name": self.table_name,
            "rows_written": rows_written,
            "output_paths": output_paths,
            "started_at": started_at,
            "finished_at": self._now_iso(),
            "dry_run": dry_run,
        }

