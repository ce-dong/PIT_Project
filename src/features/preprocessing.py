from __future__ import annotations

import numpy as np
import pandas as pd


CATEGORICAL_NEUTRALIZE_CANDIDATES = ("industry", "market")
SIZE_NEUTRALIZE_COLUMN = "total_mv"


def _finite_mask(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce").astype("float64")
    return numeric.notna() & np.isfinite(numeric)


def _resolve_categorical_column(frame: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if candidate in frame.columns and frame[candidate].notna().any():
            return candidate
    return None


def _apply_group_demean(values: pd.Series, groups: pd.Series, ref_mask: pd.Series) -> pd.Series:
    transformed = pd.to_numeric(values, errors="coerce").astype("float64").copy()
    valid_ref = ref_mask & _finite_mask(transformed) & groups.notna()
    if not valid_ref.any():
        return transformed

    group_means = transformed.loc[valid_ref].groupby(groups.loc[valid_ref], sort=False).mean()
    apply_mask = _finite_mask(transformed) & groups.notna() & groups.isin(group_means.index)
    if not apply_mask.any():
        return transformed

    transformed.loc[apply_mask] = transformed.loc[apply_mask] - groups.loc[apply_mask].map(group_means).astype("float64")
    return transformed


def _apply_size_residualization(values: pd.Series, size_series: pd.Series, ref_mask: pd.Series) -> pd.Series:
    transformed = pd.to_numeric(values, errors="coerce").astype("float64").copy()
    size_exposure = pd.to_numeric(size_series, errors="coerce").astype("float64")
    size_exposure = size_exposure.where(size_exposure > 0)
    log_size = np.log(size_exposure)

    valid_ref = ref_mask & _finite_mask(transformed) & _finite_mask(log_size)
    if valid_ref.sum() < 2:
        return transformed

    x_ref = log_size.loc[valid_ref]
    y_ref = transformed.loc[valid_ref]
    x_centered = x_ref - x_ref.mean()
    denominator = float((x_centered**2).sum())

    if not np.isfinite(denominator) or denominator <= 1e-12:
        slope = 0.0
        intercept = float(y_ref.mean())
    else:
        slope = float(((x_centered) * (y_ref - y_ref.mean())).sum() / denominator)
        intercept = float(y_ref.mean() - slope * x_ref.mean())

    apply_mask = _finite_mask(transformed) & _finite_mask(log_size)
    if not apply_mask.any():
        return transformed

    transformed.loc[apply_mask] = transformed.loc[apply_mask] - (intercept + slope * log_size.loc[apply_mask])
    return transformed


def apply_cross_section_preprocess(
    frame: pd.DataFrame,
    *,
    raw_col: str,
    output_col: str,
    steps: tuple[str, ...],
    eligible_col: str = "is_eligible",
    lower_quantile: float = 0.01,
    upper_quantile: float = 0.99,
    categorical_neutralize_candidates: tuple[str, ...] = CATEGORICAL_NEUTRALIZE_CANDIDATES,
    size_neutralize_col: str = SIZE_NEUTRALIZE_COLUMN,
) -> pd.DataFrame:
    if raw_col not in frame.columns:
        raise KeyError(f"Missing raw factor column: {raw_col}")

    working = frame.copy()
    transformed = pd.Series(np.nan, index=working.index, dtype="float64")

    for rebalance_date, indexer in working.groupby("rebalance_date", sort=True).groups.items():
        del rebalance_date
        group = working.loc[indexer]
        values = pd.to_numeric(group[raw_col], errors="coerce")
        transformed_group = values.copy()

        eligible_mask = group[eligible_col].fillna(False).astype(bool)
        ref_mask = eligible_mask & transformed_group.notna() & np.isfinite(transformed_group)
        ref_values = transformed_group.loc[ref_mask]
        categorical_col = _resolve_categorical_column(group, categorical_neutralize_candidates)

        for step in steps:
            if step == "winsorize":
                if not ref_values.empty:
                    lower = ref_values.quantile(lower_quantile)
                    upper = ref_values.quantile(upper_quantile)
                    transformed_group = transformed_group.clip(lower=lower, upper=upper)
                    ref_values = transformed_group.loc[eligible_mask & transformed_group.notna() & np.isfinite(transformed_group)]
            elif step == "zscore":
                ref_values = transformed_group.loc[eligible_mask & transformed_group.notna() & np.isfinite(transformed_group)]
                if ref_values.empty:
                    transformed_group = pd.Series(np.nan, index=group.index, dtype="float64")
                else:
                    std = ref_values.std(ddof=0)
                    if pd.isna(std) or std == 0:
                        transformed_group = pd.Series(np.nan, index=group.index, dtype="float64")
                    else:
                        mean = ref_values.mean()
                        transformed_group = (transformed_group - mean) / std
            elif step == "industry_neutralize":
                if categorical_col is not None:
                    transformed_group = _apply_group_demean(transformed_group, group[categorical_col], eligible_mask)
            elif step == "size_neutralize":
                if size_neutralize_col in group.columns:
                    transformed_group = _apply_size_residualization(transformed_group, group[size_neutralize_col], eligible_mask)
            else:
                raise ValueError(f"Unsupported preprocess step: {step}")

        transformed.loc[indexer] = transformed_group.astype("float64")

    working[output_col] = transformed
    return working
