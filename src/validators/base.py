from __future__ import annotations

from dataclasses import asdict, dataclass, field


@dataclass(frozen=True)
class ValidationIssue:
    level: str
    check: str
    failed_rows: int
    message: str


@dataclass
class ValidationResult:
    table_name: str
    row_count: int
    issues: list[ValidationIssue] = field(default_factory=list)
    metrics: dict[str, object] = field(default_factory=dict)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.level == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.level == "warning")

    @property
    def passed(self) -> bool:
        return self.error_count == 0

    def add_issue(self, *, level: str, check: str, failed_rows: int, message: str) -> None:
        self.issues.append(
            ValidationIssue(
                level=level,
                check=check,
                failed_rows=int(failed_rows),
                message=message,
            )
        )

    def add_metric(self, key: str, value: object) -> None:
        self.metrics[key] = value

    def to_dict(self) -> dict[str, object]:
        return {
            "table_name": self.table_name,
            "row_count": self.row_count,
            "passed": self.passed,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "metrics": self.metrics,
            "issues": [asdict(issue) for issue in self.issues],
        }
