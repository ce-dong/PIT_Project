from __future__ import annotations

from dataclasses import dataclass
from typing import Any


REPORT_REQUIRED_ARTIFACTS = ("evaluation_summary",)


@dataclass(frozen=True)
class ReportContext:
    experiment_name: str
    experiment_slug: str
    as_of_date: str | None = None
    required_artifacts: tuple[str, ...] = REPORT_REQUIRED_ARTIFACTS


class BaseReportBuilder:
    name = ""

    def build(self, payload: dict[str, Any], context: ReportContext) -> str:
        raise NotImplementedError
