from __future__ import annotations

import pandas as pd

from src.storage.parquet import normalize_date_columns
from src.updaters.base import BaseUpdater, UpdateContext


class StockBasicUpdater(BaseUpdater):
    table_name = "stock_basic"

    def run(self, context: UpdateContext) -> dict[str, object]:
        started_at = self._now_iso()
        self.logger.info("Refreshing %s for list_status in L/D/P", self.table_name)

        frames = [self.client.fetch_stock_basic(status) for status in ("L", "D", "P")]
        df = pd.concat(frames, ignore_index=True)
        df = normalize_date_columns(df, ["list_date", "delist_date"])
        df = df.drop_duplicates(subset=["ts_code"], keep="last").sort_values(["ts_code"]).reset_index(drop=True)

        updated_partitions = []
        if not context.dry_run:
            updated_partitions = self.store.overwrite_table(self.table_name, df)

        return self._finalize(
            started_at=started_at,
            rows_written=len(df),
            updated_partitions=updated_partitions,
            mode="full_refresh",
            last_success_trade_date=None,
            dry_run=context.dry_run,
        )

