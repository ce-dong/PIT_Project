from __future__ import annotations

from src.builders.base import BaseBuilder, BuildContext
from src.core.calendar import build_calendar_table


class CalendarTableBuilder(BaseBuilder):
    table_name = "calendar_table"

    def run(self, context: BuildContext) -> dict[str, object]:
        started_at = self._now_iso()
        self.logger.info("Building %s from raw trade_cal", self.table_name)
        raw_trade_cal = self.raw_store.read_table("trade_cal")
        calendar_table = build_calendar_table(raw_trade_cal)

        output_paths: list[str] = []
        if not context.dry_run:
            output_paths = self.lake_store.overwrite_table(self.table_name, calendar_table)

        return self._result(
            started_at=started_at,
            rows_written=len(calendar_table),
            output_paths=output_paths,
            dry_run=context.dry_run,
        )

