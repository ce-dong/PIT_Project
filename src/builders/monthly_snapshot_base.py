from __future__ import annotations

import pandas as pd

from src.builders.base import BaseBuilder, BuildContext
from src.core.pit import build_monthly_snapshot_base


class MonthlySnapshotBaseBuilder(BaseBuilder):
    table_name = "monthly_snapshot_base"

    def run(self, context: BuildContext) -> dict[str, object]:
        started_at = self._now_iso()
        self.logger.info(
            "Building %s from monthly_universe, adjusted_price_panel, and daily_basic",
            self.table_name,
        )

        monthly_universe = self.lake_store.read_table("monthly_universe")
        calendar_table = self.lake_store.read_table("calendar_table")
        adjusted_price_panel = self.lake_store.read_table(
            "adjusted_price_panel",
            columns=["ts_code", "trade_date", "close", "adj_close", "amount", "vol"],
        )
        daily_basic = self.raw_store.read_table(
            "daily_basic",
            columns=[
                "ts_code",
                "trade_date",
                "total_mv",
                "circ_mv",
                "pb",
                "pe_ttm",
                "ps_ttm",
                "dv_ttm",
                "turnover_rate",
                "turnover_rate_f",
                "volume_ratio",
            ],
        )
        try:
            fina_indicator = self.raw_store.read_table("fina_indicator")
        except FileNotFoundError:
            self.logger.info("Raw fina_indicator not found; building monthly_snapshot_base without financial PIT columns")
            fina_indicator = pd.DataFrame()

        monthly_snapshot_base = build_monthly_snapshot_base(
            monthly_universe,
            adjusted_price_panel,
            daily_basic,
            raw_fina_indicator=fina_indicator,
            calendar_table=calendar_table,
        )

        output_paths: list[str] = []
        if not context.dry_run:
            output_paths = self.lake_store.replace_by_month(
                self.table_name,
                monthly_snapshot_base,
                partition_col="rebalance_date",
                primary_keys=["rebalance_date", "ts_code"],
            )

        return self._result(
            started_at=started_at,
            rows_written=len(monthly_snapshot_base),
            output_paths=output_paths,
            dry_run=context.dry_run,
        )
