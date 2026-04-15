from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

from src.storage.parquet import normalize_date_columns
from src.updaters.base import BaseUpdater, UpdateContext


class AnnouncementEventUpdater(BaseUpdater):
    endpoint_name = ""
    table_name = ""
    primary_keys = ["ts_code", "ann_date", "end_date"]
    partition_col = "ann_date"
    extra_date_columns: list[str] = []

    def _fetch_event(self, ann_date: str) -> pd.DataFrame:
        if not self.endpoint_name:
            raise NotImplementedError("endpoint_name must be defined by subclasses.")
        return self.client._call(self.endpoint_name, ann_date=ann_date)

    def _iter_calendar_dates(self, start_date: str, end_date: str) -> list[str]:
        start = datetime.strptime(start_date, "%Y%m%d").date()
        end = datetime.strptime(end_date, "%Y%m%d").date()
        if end < start:
            return []
        num_days = (end - start).days
        return [(start + timedelta(days=offset)).strftime("%Y%m%d") for offset in range(num_days + 1)]

    def run(self, context: UpdateContext) -> dict[str, object]:
        started_at = self._now_iso()
        if context.full_refresh:
            start_date = context.start_date or self.config.initial_history_start
        else:
            start_date = self._calculate_calendar_date_start(
                self.config.financial_lookback_days,
                context.start_date,
                state_key="last_success_ann_date",
            )
        end_date = context.end_date or self._today_yyyymmdd()
        ann_dates = self._iter_calendar_dates(start_date, end_date)
        self.logger.info(
            "Updating %s for %s announcement dates between %s and %s",
            self.table_name,
            len(ann_dates),
            start_date,
            end_date,
        )

        frames: list[pd.DataFrame] = []
        for idx, ann_date in enumerate(ann_dates, start=1):
            self.logger.info("Fetching %s for ann_date=%s (%s/%s)", self.table_name, ann_date, idx, len(ann_dates))
            frame = self._fetch_event(ann_date=ann_date)
            if not frame.empty:
                frames.append(frame)

        df = self._concat_frames(frames)
        if not df.empty:
            df = normalize_date_columns(df, ["ann_date", "end_date", *self.extra_date_columns])
            df = df.sort_values(self.primary_keys).reset_index(drop=True)

        updated_partitions: list[str] = []
        if not context.dry_run and not df.empty:
            updated_partitions = self.store.upsert_by_month(
                self.table_name,
                df,
                partition_col=self.partition_col,
                primary_keys=self.primary_keys,
            )

        last_success_ann_date = None
        if not df.empty and df["ann_date"].notna().any():
            last_success_ann_date = df["ann_date"].max().strftime("%Y%m%d")

        mode = "full_refresh" if context.full_refresh else "incremental"
        return self._finalize(
            started_at=started_at,
            rows_written=len(df),
            updated_partitions=updated_partitions,
            mode=mode,
            last_success_trade_date=None,
            dry_run=context.dry_run,
            extra_state={"last_success_ann_date": last_success_ann_date},
        )
