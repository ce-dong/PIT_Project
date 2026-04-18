from __future__ import annotations

import numpy as np
import pandas as pd


def apply_cross_section_preprocess(
    frame: pd.DataFrame,
    *,
    raw_col: str,
    output_col: str,
    steps: tuple[str, ...],
    eligible_col: str = "is_eligible",
    lower_quantile: float = 0.01,
    upper_quantile: float = 0.99,
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
            else:
                raise ValueError(f"Unsupported preprocess step: {step}")

        transformed.loc[indexer] = transformed_group.astype("float64")

    working[output_col] = transformed
    return working
