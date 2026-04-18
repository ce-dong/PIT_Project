from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


EVALUATION_REQUIRED_ARTIFACTS = ("factor_panel", "label_panel")


@dataclass(frozen=True)
class EvaluationContext:
    experiment_name: str
    experiment_slug: str
    as_of_date: str | None = None
    output_table_name: str = "rank_ic"
    required_artifacts: tuple[str, ...] = EVALUATION_REQUIRED_ARTIFACTS
    factor_names: tuple[str, ...] = ()
    factor_fields: tuple[str, ...] = ()
    label_names: tuple[str, ...] = ()
    label_fields: tuple[str, ...] = ()


class BaseEvaluator:
    name = ""

    def evaluate(self, panel_df: pd.DataFrame, context: EvaluationContext) -> dict[str, Any]:
        raise NotImplementedError
