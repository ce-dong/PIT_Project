from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


FEATURE_REQUIRED_TABLES = ("monthly_universe", "monthly_snapshot_base", "adjusted_price_panel")


@dataclass(frozen=True)
class FeatureContext:
    experiment_name: str
    experiment_slug: str
    as_of_date: str | None = None
    output_table_name: str = "factor_panel"
    required_tables: tuple[str, ...] = FEATURE_REQUIRED_TABLES


class BaseFeatureBuilder:
    family = ""
    name = ""

    def build(self, snapshot_df: pd.DataFrame, context: FeatureContext) -> pd.DataFrame:
        raise NotImplementedError
