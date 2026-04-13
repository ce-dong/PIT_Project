from __future__ import annotations

from src.builders.base import BaseBuilder, BuildContext
from src.core.universe import build_monthly_universe


class MonthlyUniverseBuilder(BaseBuilder):
    table_name = "monthly_universe"

    def run(self, context: BuildContext) -> dict[str, object]:
        started_at = self._now_iso()
        self.logger.info("Building %s from calendar_table, stock_basic, and adjusted_price_panel", self.table_name)

        calendar_table = self.lake_store.read_table("calendar_table")
        stock_basic = self.raw_store.read_table("stock_basic")
        adjusted_price_panel = self.lake_store.read_table(
            "adjusted_price_panel",
            columns=["ts_code", "trade_date", "amount", "adj_factor", "adj_close"],
        )
        monthly_universe = build_monthly_universe(
            calendar_table,
            stock_basic,
            adjusted_price_panel,
            ipo_min_trade_days=self.config.universe_ipo_min_trade_days,
            liquidity_window=self.config.universe_liquidity_window,
            min_valid_trade_days=self.config.universe_min_valid_trade_days,
            min_median_amount=self.config.universe_min_median_amount,
        )

        output_paths: list[str] = []
        if not context.dry_run:
            output_paths = self.lake_store.replace_by_month(
                self.table_name,
                monthly_universe,
                partition_col="rebalance_date",
                primary_keys=["rebalance_date", "ts_code"],
            )

        return self._result(
            started_at=started_at,
            rows_written=len(monthly_universe),
            output_paths=output_paths,
            dry_run=context.dry_run,
        )

