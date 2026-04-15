from __future__ import annotations

import pandas as pd

from src.storage.parquet import normalize_date_columns
from src.updaters.base import BaseUpdater, UpdateContext


ALLOWED_MARKETS = {"主板", "创业板", "科创板"}
ALLOWED_TS_CODE_SUFFIXES = (".SH", ".SZ")


class FinancialStatementUpdater(BaseUpdater):
    endpoint_name = ""
    table_name = ""
    primary_keys = ["ts_code", "ann_date", "end_date"]
    partition_col = "ann_date"

    def _fetch_statement(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        if not self.endpoint_name:
            raise NotImplementedError("endpoint_name must be defined by subclasses.")
        return self.client._call(self.endpoint_name, ts_code=ts_code, start_date=start_date, end_date=end_date)

    def _load_target_stock_codes(self) -> list[str]:
        stock_basic = self.store.read_table("stock_basic", columns=["ts_code", "market"])
        is_common_a = stock_basic["ts_code"].str.endswith(ALLOWED_TS_CODE_SUFFIXES) & stock_basic["market"].isin(ALLOWED_MARKETS)
        return stock_basic.loc[is_common_a, "ts_code"].drop_duplicates().sort_values().tolist()

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
        stock_codes = self._load_target_stock_codes()
        self.logger.info(
            "Updating %s for %s common A-share tickers between %s and %s",
            self.table_name,
            len(stock_codes),
            start_date,
            end_date,
        )

        frames: list[pd.DataFrame] = []
        for idx, ts_code in enumerate(stock_codes, start=1):
            self.logger.info("Fetching %s for ts_code=%s (%s/%s)", self.table_name, ts_code, idx, len(stock_codes))
            frame = self._fetch_statement(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if not frame.empty:
                frames.append(frame)

        df = self._concat_frames(frames)
        if not df.empty:
            df = normalize_date_columns(df, ["ann_date", "f_ann_date", "end_date"])
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
