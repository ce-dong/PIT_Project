from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import AppConfig
from src.evaluation.base import EVALUATION_REQUIRED_ARTIFACTS
from src.features.base import FEATURE_REQUIRED_TABLES
from src.labels.base import LABEL_REQUIRED_TABLES
from src.reports.base import REPORT_REQUIRED_ARTIFACTS


RESEARCH_STAGE_ORDER = ("features", "labels", "evaluation", "reports")
_AS_OF_DATE_PATTERN = re.compile(r"^\d{8}$")


def normalize_experiment_name(name: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", name.strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized:
        raise ValueError("Experiment name must contain at least one alphanumeric character.")
    return normalized


def normalize_as_of_date(as_of_date: str | None) -> str | None:
    if as_of_date is None:
        return None
    normalized = as_of_date.replace("-", "").strip()
    if not _AS_OF_DATE_PATTERN.fullmatch(normalized):
        raise ValueError("As-of date must be in YYYYMMDD or YYYY-MM-DD format.")
    return normalized


def _stage_contracts() -> dict[str, tuple[str, ...]]:
    return {
        "features": FEATURE_REQUIRED_TABLES,
        "labels": LABEL_REQUIRED_TABLES,
        "evaluation": EVALUATION_REQUIRED_ARTIFACTS,
        "reports": REPORT_REQUIRED_ARTIFACTS,
    }


@dataclass(frozen=True)
class ResearchRunConfig:
    experiment_name: str
    as_of_date: str | None = None
    stages: tuple[str, ...] = RESEARCH_STAGE_ORDER

    def __post_init__(self) -> None:
        normalized_name = normalize_experiment_name(self.experiment_name)
        normalized_date = normalize_as_of_date(self.as_of_date)
        invalid_stages = [stage for stage in self.stages if stage not in RESEARCH_STAGE_ORDER]
        if invalid_stages:
            invalid = ", ".join(sorted(set(invalid_stages)))
            raise ValueError(f"Unsupported research stage(s): {invalid}")
        object.__setattr__(self, "experiment_name", normalized_name)
        object.__setattr__(self, "as_of_date", normalized_date)
        object.__setattr__(self, "stages", tuple(dict.fromkeys(self.stages)))

    @property
    def experiment_slug(self) -> str:
        if self.as_of_date:
            return f"{self.as_of_date}__{self.experiment_name}"
        return self.experiment_name


@dataclass(frozen=True)
class ResearchRunPaths:
    panel_root: Path
    experiments_root: Path
    experiment_root: Path
    manifest_path: Path
    stage_roots: dict[str, Path]


def resolve_research_paths(config: AppConfig, run_config: ResearchRunConfig) -> ResearchRunPaths:
    experiment_root = config.experiments_root / run_config.experiment_slug
    stage_roots = {stage: experiment_root / stage for stage in run_config.stages}
    return ResearchRunPaths(
        panel_root=config.panel_data_root,
        experiments_root=config.experiments_root,
        experiment_root=experiment_root,
        manifest_path=experiment_root / "manifest.json",
        stage_roots=stage_roots,
    )


def _relative_to_project(config: AppConfig, path: Path) -> str:
    return str(path.relative_to(config.project_root))


def _build_manifest(run_config: ResearchRunConfig, paths: ResearchRunPaths) -> dict[str, Any]:
    return {
        "experiment_name": run_config.experiment_name,
        "experiment_slug": run_config.experiment_slug,
        "as_of_date": run_config.as_of_date,
        "stages": list(run_config.stages),
        "stage_contracts": {stage: list(_stage_contracts()[stage]) for stage in run_config.stages},
        "panel_root": str(paths.panel_root),
        "stage_roots": {stage: str(path) for stage, path in paths.stage_roots.items()},
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def initialize_experiment_layout(
    config: AppConfig,
    run_config: ResearchRunConfig,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    paths = resolve_research_paths(config, run_config)
    directories = [paths.panel_root, paths.experiments_root, paths.experiment_root, *paths.stage_roots.values()]
    created_paths = [_relative_to_project(config, path) for path in directories]

    manifest = _build_manifest(run_config, paths)
    if not dry_run:
        for path in directories:
            path.mkdir(parents=True, exist_ok=True)
        paths.manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")

    return {
        "experiment_name": run_config.experiment_name,
        "experiment_slug": run_config.experiment_slug,
        "stages": list(run_config.stages),
        "panel_root": _relative_to_project(config, paths.panel_root),
        "experiment_root": _relative_to_project(config, paths.experiment_root),
        "manifest_path": _relative_to_project(config, paths.manifest_path),
        "stage_roots": {stage: _relative_to_project(config, path) for stage, path in paths.stage_roots.items()},
        "stage_contracts": {stage: list(_stage_contracts()[stage]) for stage in run_config.stages},
        "created_paths": created_paths,
        "dry_run": dry_run,
    }
