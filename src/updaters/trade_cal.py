from __future__ import annotations

from src.storage.parquet import normalize_date_columns
from src.updaters.base import BaseUpdater, UpdateContext


class TradeCalUpdater(BaseUpdater):
    table_name = "trade_cal"

    def run(self, context: UpdateContext) -> dict[str, object]:
        started_at = self._now_iso()
        start_date = context.start_date or self.config.calendar_start_date
        end_date = context.end_date or self.config.calendar_end_date()
        self.logger.info("Refreshing %s from %s to %s", self.table_name, start_date, end_date)

        df = self.client.fetch_trade_cal(
            exchange=self.config.calendar_exchange,
            start_date=start_date,
            end_date=end_date,
        )
        df = normalize_date_columns(df, ["cal_date", "pretrade_date"])
        df = df.sort_values(["exchange", "cal_date"]).reset_index(drop=True)

        updated_partitions = []
        if not context.dry_run:
            updated_partitions = self.store.overwrite_table(self.table_name, df)

        open_dates = df.loc[df["is_open"] == 1, "cal_date"]
        last_success_trade_date = None if open_dates.empty else open_dates.max().strftime("%Y%m%d")
        return self._finalize(
            started_at=started_at,
            rows_written=len(df),
            updated_partitions=updated_partitions,
            mode="full_refresh",
            last_success_trade_date=last_success_trade_date,
            dry_run=context.dry_run,
        )

