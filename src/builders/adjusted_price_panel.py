from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.builders.base import BaseBuilder, BuildContext
from src.core.adjustments import build_adjusted_price_panel


class AdjustedPricePanelBuilder(BaseBuilder):
    table_name = "adjusted_price_panel"

    def run(self, context: BuildContext) -> dict[str, object]:
        started_at = self._now_iso()
        self.logger.info("Building %s from raw daily and adj_factor", self.table_name)
        daily_partitions = self.raw_store.list_partition_files("daily")
        output_paths: list[str] = []
        rows_written = 0

        if not context.dry_run:
            self.lake_store.clear_table(self.table_name)

        for daily_partition_path in daily_partitions:
            relative_partition_path = daily_partition_path.relative_to(self.raw_store.root / "daily")
            adj_partition_path = self.raw_store.root / "adj_factor" / relative_partition_path
            if not adj_partition_path.exists():
                raise FileNotFoundError(
                    f"Missing adj_factor partition for {relative_partition_path}. "
                    f"Expected at {adj_partition_path}."
                )

            self.logger.info("Building %s partition %s", self.table_name, relative_partition_path)
            raw_daily = pd.read_parquet(daily_partition_path)
            raw_adj_factor = pd.read_parquet(adj_partition_path)
            adjusted_partition = build_adjusted_price_panel(raw_daily, raw_adj_factor)
            rows_written += len(adjusted_partition)

            if not context.dry_run:
                output_paths.append(
                    self.lake_store.write_partition_file(
                        self.table_name,
                        Path(relative_partition_path),
                        adjusted_partition,
                    )
                )

        return self._result(
            started_at=started_at,
            rows_written=rows_written,
            output_paths=output_paths,
            dry_run=context.dry_run,
        )
