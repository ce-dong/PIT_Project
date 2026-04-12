from __future__ import annotations

from src.storage.parquet import normalize_date_columns
from src.updaters.base import BaseUpdater, UpdateContext


class AdjFactorUpdater(BaseUpdater):
    table_name = "adj_factor"
    primary_keys = ["ts_code", "trade_date"]
    partition_col = "trade_date"

    def run(self, context: UpdateContext) -> dict[str, object]:
        started_at = self._now_iso()
        if context.full_refresh:
            start_date = context.start_date or self.config.initial_history_start
        else:
            start_date = self._calculate_trade_date_start(
                self.config.adj_factor_lookback_trade_days,
                context.start_date,
            )
        end_date = context.end_date or self._today_yyyymmdd()
        trade_dates = self._fetch_open_trade_dates(start_date, end_date)
        self.logger.info("Updating %s for %s open trade dates", self.table_name, len(trade_dates))

        frames = []
        for trade_date in trade_dates:
            self.logger.info("Fetching %s for trade_date=%s", self.table_name, trade_date)
            frame = self.client.fetch_adj_factor(trade_date=trade_date)
            if not frame.empty:
                frames.append(frame)

        df = self._concat_frames(frames)
        if not df.empty:
            df = normalize_date_columns(df, ["trade_date"])

        updated_partitions = []
        if not context.dry_run and not df.empty:
            updated_partitions = self.store.upsert_by_month(
                self.table_name,
                df,
                partition_col=self.partition_col,
                primary_keys=self.primary_keys,
            )

        last_success_trade_date = trade_dates[-1] if trade_dates else None
        mode = "full_refresh" if context.full_refresh else "incremental"
        return self._finalize(
            started_at=started_at,
            rows_written=len(df),
            updated_partitions=updated_partitions,
            mode=mode,
            last_success_trade_date=last_success_trade_date,
            dry_run=context.dry_run,
        )

