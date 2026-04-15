from __future__ import annotations

from datetime import datetime, timezone

from src.validators.base import ValidationResult
from src.validators.reporting import render_validation_report


def test_render_validation_report_includes_summary_and_metrics():
    result = ValidationResult(table_name="monthly_snapshot_base", row_count=10)
    result.add_metric("price_snapshot_coverage_ratio", 0.8)
    report = render_validation_report(
        [result],
        generated_at=datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc),
        command="python -m src.cli validate all",
    )

    assert "# Data Quality Report" in report
    assert "`monthly_snapshot_base`" in report
    assert "`price_snapshot_coverage_ratio`: `0.8`" in report
