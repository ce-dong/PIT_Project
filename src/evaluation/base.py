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
    required_artifacts: tuple[str, ...] = EVALUATION_REQUIRED_ARTIFACTS


class BaseEvaluator:
    name = ""

    def evaluate(self, panel_df: pd.DataFrame, context: EvaluationContext) -> dict[str, Any]:
        raise NotImplementedError
