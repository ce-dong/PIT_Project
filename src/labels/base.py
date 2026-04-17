from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


LABEL_REQUIRED_TABLES = ("calendar_table", "adjusted_price_panel", "monthly_universe")


@dataclass(frozen=True)
class LabelContext:
    experiment_name: str
    experiment_slug: str
    as_of_date: str | None = None
    output_table_name: str = "label_panel"
    required_tables: tuple[str, ...] = LABEL_REQUIRED_TABLES
    label_names: tuple[str, ...] = ()


class BaseLabelBuilder:
    name = ""

    def build(
        self,
        monthly_universe_df: pd.DataFrame,
        adjusted_price_df: pd.DataFrame,
        calendar_df: pd.DataFrame,
        context: LabelContext,
    ) -> pd.DataFrame:
        raise NotImplementedError
