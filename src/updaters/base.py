from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd

from src.adapters.tushare.client import TushareClient
from src.config import AppConfig
from src.logging_utils import get_logger
from src.storage.parquet import ParquetDataStore
from src.storage.state import IngestionStateStore


@dataclass
class UpdateContext:
    start_date: str | None = None
    end_date: str | None = None
    full_refresh: bool = False
    dry_run: bool = False


class BaseUpdater:
    table_name = ""
    primary_keys: list[str] = []
    partition_col: str | None = None

    def __init__(
        self,
        config: AppConfig,
        client: TushareClient,
        store: ParquetDataStore,
        state_store: IngestionStateStore,
    ) -> None:
        self.config = config
        self.client = client
        self.store = store
        self.state_store = state_store
        self.logger = get_logger(self.__class__.__name__, config.log_root)

    def run(self, context: UpdateContext) -> dict[str, Any]:
        raise NotImplementedError

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _today_yyyymmdd(self) -> str:
        return date.today().strftime("%Y%m%d")

    def _calculate_trade_date_start(self, lookback_trade_days: int, requested_start: str | None) -> str:
        if requested_start:
            return requested_start

        last_state = self.state_store.get(self.table_name)
        last_success_trade_date = last_state.get("last_success_trade_date")
        if not last_success_trade_date:
            return self.config.initial_history_start

        anchor = datetime.strptime(last_success_trade_date, "%Y%m%d").date()
        probe_start = (anchor - timedelta(days=max(lookback_trade_days * 4, 30))).strftime("%Y%m%d")
        calendar = self.client.fetch_trade_cal(
            exchange=self.config.calendar_exchange,
            start_date=probe_start,
            end_date=last_success_trade_date,
        )
        open_dates = sorted(calendar.loc[calendar["is_open"] == 1, "cal_date"].tolist())
        if not open_dates:
            return last_success_trade_date

        idx = max(0, len(open_dates) - 1 - lookback_trade_days)
        return open_dates[idx]

    def _fetch_open_trade_dates(self, start_date: str, end_date: str) -> list[str]:
        calendar = self.client.fetch_trade_cal(
            exchange=self.config.calendar_exchange,
            start_date=start_date,
            end_date=end_date,
        )
        open_dates = calendar.loc[calendar["is_open"] == 1, "cal_date"].tolist()
        return sorted(open_dates)

    def _finalize(
        self,
        *,
        started_at: str,
        rows_written: int,
        updated_partitions: list[str],
        mode: str,
        last_success_trade_date: str | None,
        dry_run: bool,
    ) -> dict[str, Any]:
        finished_at = self._now_iso()
        result = {
            "table_name": self.table_name,
            "rows_written": rows_written,
            "updated_partitions": updated_partitions,
            "mode": mode,
            "last_success_trade_date": last_success_trade_date,
            "started_at": started_at,
            "finished_at": finished_at,
            "dry_run": dry_run,
        }
        if not dry_run:
            self.state_store.mark_success(
                self.table_name,
                started_at=started_at,
                finished_at=finished_at,
                rows_written=rows_written,
                updated_partitions=updated_partitions,
                mode=mode,
                last_success_trade_date=last_success_trade_date,
            )
        return result

    def _concat_frames(self, frames: list[pd.DataFrame]) -> pd.DataFrame:
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

