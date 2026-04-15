from __future__ import annotations

from datetime import datetime
from pathlib import Path

from src.validators.base import ValidationResult


def render_validation_report(results: list[ValidationResult], *, generated_at: datetime, command: str) -> str:
    lines: list[str] = []
    lines.append("# Data Quality Report")
    lines.append("")
    lines.append(f"- Generated at: `{generated_at.isoformat()}`")
    lines.append(f"- Command: `{command}`")
    lines.append("")

    total_errors = sum(result.error_count for result in results)
    total_warnings = sum(result.warning_count for result in results)
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Tables checked: `{len(results)}`")
    lines.append(f"- Total errors: `{total_errors}`")
    lines.append(f"- Total warnings: `{total_warnings}`")
    lines.append("")

    for result in results:
        lines.append(f"## `{result.table_name}`")
        lines.append("")
        lines.append(f"- Passed: `{result.passed}`")
        lines.append(f"- Row count: `{result.row_count}`")
        lines.append(f"- Errors: `{result.error_count}`")
        lines.append(f"- Warnings: `{result.warning_count}`")
        if result.metrics:
            lines.append("- Metrics:")
            for key, value in sorted(result.metrics.items()):
                lines.append(f"  - `{key}`: `{value}`")
        if result.issues:
            lines.append("- Issues:")
            for issue in result.issues:
                lines.append(
                    f"  - `{issue.level}` `{issue.check}`: {issue.message} "
                    f"(failed_rows={issue.failed_rows})"
                )
        else:
            lines.append("- Issues: none")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def write_validation_report(
    output_path: Path,
    results: list[ValidationResult],
    *,
    generated_at: datetime,
    command: str,
) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_validation_report(results, generated_at=generated_at, command=command),
        encoding="utf-8",
    )
    return output_path
