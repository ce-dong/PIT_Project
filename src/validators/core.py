from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from src.config import AppConfig
from src.logging_utils import get_logger
from src.storage.parquet import ParquetDataStore
from src.validators.base import ValidationResult


CORE_VALIDATION_ORDER = [
    "calendar_table",
    "adjusted_price_panel",
    "monthly_universe",
    "monthly_snapshot_base",
]

REQUIRED_COLUMNS = {
    "calendar_table": ["trade_date", "prev_trade_date", "next_trade_date", "is_month_end", "month"],
    "adjusted_price_panel": ["ts_code", "trade_date", "open", "close", "adj_factor", "adj_open", "adj_close", "year", "month"],
    "monthly_universe": [
        "rebalance_date",
        "trade_execution_date",
        "ts_code",
        "is_eligible",
        "exclude_reason",
        "year",
        "month",
    ],
    "monthly_snapshot_base": [
        "rebalance_date",
        "trade_execution_date",
        "ts_code",
        "is_eligible",
        "exclude_reason",
        "price_trade_date",
        "daily_basic_trade_date",
        "year",
        "month",
    ],
}


def _first_partition_columns(store: ParquetDataStore, table_name: str) -> list[str]:
    partition_files = store.list_partition_files(table_name)
    if not partition_files:
        return []
    schema = pq.read_schema(partition_files[0])
    return schema.names


def _append_duplicate_issue(result: ValidationResult, df: pd.DataFrame, subset: list[str], check: str) -> None:
    duplicate_count = int(df.duplicated(subset=subset, keep=False).sum())
    if duplicate_count > 0:
        result.add_issue(
            level="error",
            check=check,
            failed_rows=duplicate_count,
            message=f"Duplicate rows detected on primary key {subset}.",
        )


def _append_missing_required_columns_issue(result: ValidationResult, df: pd.DataFrame, required_columns: list[str]) -> list[str]:
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        result.add_issue(
            level="error",
            check="required_columns_present",
            failed_rows=len(missing),
            message=f"Missing required columns: {missing}.",
        )
    return missing


def _append_partition_schema_issue(result: ValidationResult, store: ParquetDataStore, table_name: str) -> None:
    try:
        partition_files = store.list_partition_files(table_name)
    except FileNotFoundError:
        return

    if not partition_files:
        return

    baseline_names = None
    bad_files = 0
    for partition_file in partition_files:
        schema_names = tuple(pq.read_schema(partition_file).names)
        if baseline_names is None:
            baseline_names = schema_names
            continue
        if schema_names != baseline_names:
            bad_files += 1

    if bad_files > 0:
        result.add_issue(
            level="error",
            check="partition_schema_consistency",
            failed_rows=bad_files,
            message="Partitioned parquet files do not share the same schema.",
        )


def _count_series_mismatch(left: pd.Series, right: pd.Series) -> int:
    left_dt = pd.to_datetime(left, errors="coerce")
    right_dt = pd.to_datetime(right, errors="coerce")
    mismatch = ~((left_dt == right_dt) | (left_dt.isna() & right_dt.isna()))
    return int(mismatch.sum())


def _append_ratio_threshold_issue(
    result: ValidationResult,
    *,
    metric_key: str,
    warn_threshold: float,
    error_threshold: float,
    label: str,
) -> None:
    metric_value = result.metrics.get(metric_key)
    if metric_value is None:
        return

    ratio = float(metric_value)
    if ratio < error_threshold:
        result.add_issue(
            level="error",
            check=f"{metric_key}_threshold",
            failed_rows=0,
            message=f"{label} ({ratio:.6f}) is below the error threshold {error_threshold:.2f}.",
        )
    elif ratio < warn_threshold:
        result.add_issue(
            level="warning",
            check=f"{metric_key}_threshold",
            failed_rows=0,
            message=f"{label} ({ratio:.6f}) is below the warning threshold {warn_threshold:.2f}.",
        )


def _append_snapshot_coverage_threshold_issues(result: ValidationResult, config: AppConfig) -> None:
    market_metrics = {
        "price_snapshot_coverage_ratio": "Price snapshot coverage ratio",
        "daily_basic_snapshot_coverage_ratio": "Daily-basic snapshot coverage ratio",
    }
    for metric_key, label in market_metrics.items():
        _append_ratio_threshold_issue(
            result,
            metric_key=metric_key,
            warn_threshold=config.quality_market_coverage_warn_ratio,
            error_threshold=config.quality_market_coverage_error_ratio,
            label=label,
        )

    for metric_key in ("fi_coverage_ratio", "inc_coverage_ratio", "bs_coverage_ratio", "cf_coverage_ratio"):
        _append_ratio_threshold_issue(
            result,
            metric_key=metric_key,
            warn_threshold=config.quality_financial_coverage_warn_ratio,
            error_threshold=config.quality_financial_coverage_error_ratio,
            label=metric_key,
        )

    for metric_key in ("fc_coverage_ratio", "ex_coverage_ratio"):
        _append_ratio_threshold_issue(
            result,
            metric_key=metric_key,
            warn_threshold=config.quality_event_coverage_warn_ratio,
            error_threshold=config.quality_event_coverage_error_ratio,
            label=metric_key,
        )


def validate_calendar_table_df(df: pd.DataFrame) -> ValidationResult:
    result = ValidationResult(table_name="calendar_table", row_count=len(df))
    if df.empty:
        result.add_issue(level="error", check="non_empty", failed_rows=0, message="calendar_table is empty.")
        return result
    if _append_missing_required_columns_issue(result, df, REQUIRED_COLUMNS["calendar_table"]):
        return result

    working = df.copy()
    for column in ("trade_date", "prev_trade_date", "next_trade_date"):
        if column in working.columns:
            working[column] = pd.to_datetime(working[column], errors="coerce")

    _append_duplicate_issue(result, working, ["trade_date"], "primary_key_unique")

    month_labels = working["trade_date"].dt.strftime("%Y-%m")
    month_mismatch = int((month_labels != working["month"]).sum())
    if month_mismatch > 0:
        result.add_issue(
            level="error",
            check="month_label_consistency",
            failed_rows=month_mismatch,
            message="month must equal trade_date formatted as YYYY-MM.",
        )

    month_end_counts = working.groupby("month", dropna=False)["is_month_end"].sum(min_count=1)
    bad_months = int((month_end_counts != 1).sum())
    if bad_months > 0:
        result.add_issue(
            level="error",
            check="single_month_end_per_month",
            failed_rows=bad_months,
            message="Each month must have exactly one is_month_end=true row.",
        )

    sorted_dates = working["trade_date"].sort_values(ignore_index=True)
    prev_expected = sorted_dates.shift(1)
    next_expected = sorted_dates.shift(-1)
    sorted_working = working.sort_values("trade_date", kind="mergesort").reset_index(drop=True)
    prev_bad = _count_series_mismatch(sorted_working["prev_trade_date"], prev_expected)
    next_bad = _count_series_mismatch(sorted_working["next_trade_date"], next_expected)
    if prev_bad > 0:
        result.add_issue(
            level="error",
            check="prev_trade_date_link",
            failed_rows=prev_bad,
            message="prev_trade_date must equal the immediately preceding trade_date.",
        )
    if next_bad > 0:
        result.add_issue(
            level="error",
            check="next_trade_date_link",
            failed_rows=next_bad,
            message="next_trade_date must equal the immediately following trade_date.",
        )

    result.add_metric("month_count", int(working["month"].nunique(dropna=True)))
    result.add_metric("month_end_count", int(working["is_month_end"].sum()))

    return result


def validate_adjusted_price_panel_df(df: pd.DataFrame) -> ValidationResult:
    result = ValidationResult(table_name="adjusted_price_panel", row_count=len(df))
    if df.empty:
        result.add_issue(level="error", check="non_empty", failed_rows=0, message="adjusted_price_panel is empty.")
        return result
    if _append_missing_required_columns_issue(result, df, REQUIRED_COLUMNS["adjusted_price_panel"]):
        return result

    working = df.copy()
    working["trade_date"] = pd.to_datetime(working["trade_date"], errors="coerce")
    _append_duplicate_issue(result, working, ["ts_code", "trade_date"], "primary_key_unique")

    year_bad = int((working["trade_date"].dt.year != working["year"]).sum())
    month_bad = int((working["trade_date"].dt.month != working["month"]).sum())
    if year_bad > 0 or month_bad > 0:
        result.add_issue(
            level="error",
            check="partition_columns_match_trade_date",
            failed_rows=year_bad + month_bad,
            message="year/month must match trade_date.",
        )

    open_mask = working[["open", "adj_factor", "adj_open"]].notna().all(axis=1)
    close_mask = working[["close", "adj_factor", "adj_close"]].notna().all(axis=1)
    open_bad = int((~np.isclose(working.loc[open_mask, "adj_open"], working.loc[open_mask, "open"] * working.loc[open_mask, "adj_factor"])).sum())
    close_bad = int((~np.isclose(working.loc[close_mask, "adj_close"], working.loc[close_mask, "close"] * working.loc[close_mask, "adj_factor"])).sum())
    if open_bad > 0:
        result.add_issue(
            level="error",
            check="adj_open_formula",
            failed_rows=open_bad,
            message="adj_open must equal open * adj_factor where all inputs are present.",
        )
    if close_bad > 0:
        result.add_issue(
            level="error",
            check="adj_close_formula",
            failed_rows=close_bad,
            message="adj_close must equal close * adj_factor where all inputs are present.",
        )

    result.add_metric("unique_ts_code_count", int(working["ts_code"].nunique(dropna=True)))
    result.add_metric("trade_date_count", int(working["trade_date"].nunique(dropna=True)))
    result.add_metric("adj_factor_non_null_ratio", round(float(working["adj_factor"].notna().mean()), 6))
    result.add_metric("adj_close_non_null_ratio", round(float(working["adj_close"].notna().mean()), 6))

    return result


def validate_monthly_universe_df(df: pd.DataFrame) -> ValidationResult:
    result = ValidationResult(table_name="monthly_universe", row_count=len(df))
    if df.empty:
        result.add_issue(level="error", check="non_empty", failed_rows=0, message="monthly_universe is empty.")
        return result
    if _append_missing_required_columns_issue(result, df, REQUIRED_COLUMNS["monthly_universe"]):
        return result

    working = df.copy()
    for column in ("rebalance_date", "trade_execution_date"):
        working[column] = pd.to_datetime(working[column], errors="coerce")

    _append_duplicate_issue(result, working, ["rebalance_date", "ts_code"], "primary_key_unique")

    exec_bad = int((working["trade_execution_date"] <= working["rebalance_date"]).sum())
    if exec_bad > 0:
        result.add_issue(
            level="error",
            check="trade_execution_after_rebalance",
            failed_rows=exec_bad,
            message="trade_execution_date must be strictly after rebalance_date.",
        )

    year_bad = int((working["rebalance_date"].dt.year != working["year"]).sum())
    month_bad = int((working["rebalance_date"].dt.month != working["month"]).sum())
    if year_bad > 0 or month_bad > 0:
        result.add_issue(
            level="error",
            check="partition_columns_match_rebalance_date",
            failed_rows=year_bad + month_bad,
            message="year/month must match rebalance_date.",
        )

    missing_reason = int(((~working["is_eligible"]) & (working["exclude_reason"].fillna("") == "")).sum())
    if missing_reason > 0:
        result.add_issue(
            level="warning",
            check="exclude_reason_present_for_ineligible_rows",
            failed_rows=missing_reason,
            message="Ineligible rows should carry an exclude_reason for auditability.",
        )

    result.add_metric("eligible_row_count", int(working["is_eligible"].sum()))
    result.add_metric("eligible_ratio", round(float(working["is_eligible"].mean()), 6))
    result.add_metric("rebalance_month_count", int(working["rebalance_date"].dt.to_period("M").nunique()))
    result.add_metric("unique_ts_code_count", int(working["ts_code"].nunique(dropna=True)))

    return result


def validate_monthly_snapshot_base_df(df: pd.DataFrame, monthly_universe: pd.DataFrame) -> ValidationResult:
    result = ValidationResult(table_name="monthly_snapshot_base", row_count=len(df))
    if df.empty:
        result.add_issue(level="error", check="non_empty", failed_rows=0, message="monthly_snapshot_base is empty.")
        return result
    if _append_missing_required_columns_issue(result, df, REQUIRED_COLUMNS["monthly_snapshot_base"]):
        return result
    if _append_missing_required_columns_issue(
        result,
        monthly_universe,
        ["rebalance_date", "trade_execution_date", "ts_code", "is_eligible", "exclude_reason"],
    ):
        return result

    working = df.copy()
    universe = monthly_universe.copy()
    for column in ("rebalance_date", "trade_execution_date", "price_trade_date", "daily_basic_trade_date"):
        if column in working.columns:
            working[column] = pd.to_datetime(working[column], errors="coerce")
        if column in universe.columns:
            universe[column] = pd.to_datetime(universe[column], errors="coerce")

    _append_duplicate_issue(result, working, ["rebalance_date", "ts_code"], "primary_key_unique")

    year_bad = int((working["rebalance_date"].dt.year != working["year"]).sum())
    month_bad = int((working["rebalance_date"].dt.month != working["month"]).sum())
    if year_bad > 0 or month_bad > 0:
        result.add_issue(
            level="error",
            check="partition_columns_match_rebalance_date",
            failed_rows=year_bad + month_bad,
            message="year/month must match rebalance_date.",
        )

    price_bad = int(((working["price_trade_date"].notna()) & (working["price_trade_date"] > working["rebalance_date"])).sum())
    basic_bad = int(((working["daily_basic_trade_date"].notna()) & (working["daily_basic_trade_date"] > working["rebalance_date"])).sum())
    if price_bad > 0:
        result.add_issue(
            level="error",
            check="price_trade_date_cutoff",
            failed_rows=price_bad,
            message="price_trade_date must be on or before rebalance_date.",
        )
    if basic_bad > 0:
        result.add_issue(
            level="error",
            check="daily_basic_trade_date_cutoff",
            failed_rows=basic_bad,
            message="daily_basic_trade_date must be on or before rebalance_date.",
        )

    tradable_cols = [column for column in working.columns if column.endswith("_tradable_date") and column != "trade_execution_date"]
    tradable_bad = 0
    for column in tradable_cols:
        tradable_bad += int(((working[column].notna()) & (pd.to_datetime(working[column], errors="coerce") > working["trade_execution_date"])).sum())
    if tradable_bad > 0:
        result.add_issue(
            level="error",
            check="tradable_date_cutoff",
            failed_rows=tradable_bad,
            message="Financial and event tradable_date columns must be on or before trade_execution_date.",
        )

    snapshot_keys = working[["rebalance_date", "ts_code"]].drop_duplicates()
    universe_keys = universe[["rebalance_date", "ts_code"]].drop_duplicates()
    merged = snapshot_keys.merge(universe_keys, on=["rebalance_date", "ts_code"], how="outer", indicator=True)
    key_mismatch = int((merged["_merge"] != "both").sum())
    if key_mismatch > 0:
        result.add_issue(
            level="error",
            check="key_alignment_with_monthly_universe",
            failed_rows=key_mismatch,
            message="monthly_snapshot_base keys must align exactly with monthly_universe.",
        )

    eligibility_cols = ["rebalance_date", "ts_code", "trade_execution_date", "is_eligible", "exclude_reason"]
    compare = working[eligibility_cols].merge(
        universe[eligibility_cols],
        on=["rebalance_date", "ts_code", "trade_execution_date"],
        how="left",
        suffixes=("_snapshot", "_universe"),
    )
    eligibility_bad = int(
        (
            (compare["is_eligible_snapshot"] != compare["is_eligible_universe"])
            | (compare["exclude_reason_snapshot"].fillna("") != compare["exclude_reason_universe"].fillna(""))
        ).sum()
    )
    if eligibility_bad > 0:
        result.add_issue(
            level="error",
            check="universe_audit_columns_consistency",
            failed_rows=eligibility_bad,
            message="Snapshot universe audit columns must match monthly_universe.",
        )

    result.add_metric("price_snapshot_coverage_ratio", round(float(working["price_trade_date"].notna().mean()), 6))
    result.add_metric("daily_basic_snapshot_coverage_ratio", round(float(working["daily_basic_trade_date"].notna().mean()), 6))
    family_prefixes = ["fi", "inc", "bs", "cf", "fc", "ex"]
    for prefix in family_prefixes:
        report_col = f"{prefix}_report_period"
        if report_col in working.columns:
            result.add_metric(f"{prefix}_coverage_ratio", round(float(working[report_col].notna().mean()), 6))

    return result


def run_core_validations(config: AppConfig, lake_store: ParquetDataStore, *, tables: list[str]) -> list[ValidationResult]:
    logger = get_logger("DataQualityValidator", config.log_root)
    results: list[ValidationResult] = []

    monthly_universe_cache: pd.DataFrame | None = None
    for table_name in tables:
        logger.info("Validating %s", table_name)
        if table_name == "calendar_table":
            df = lake_store.read_table(
                "calendar_table",
                columns=["trade_date", "prev_trade_date", "next_trade_date", "is_month_end", "month"],
            )
            result = validate_calendar_table_df(df)
        elif table_name == "adjusted_price_panel":
            df = lake_store.read_table(
                "adjusted_price_panel",
                columns=[
                    "ts_code",
                    "trade_date",
                    "open",
                    "close",
                    "adj_factor",
                    "adj_open",
                    "adj_close",
                    "year",
                    "month",
                ],
            )
            result = validate_adjusted_price_panel_df(df)
            _append_partition_schema_issue(result, lake_store, "adjusted_price_panel")
        elif table_name == "monthly_universe":
            monthly_universe_cache = lake_store.read_table(
                "monthly_universe",
                columns=[
                    "rebalance_date",
                    "trade_execution_date",
                    "ts_code",
                    "is_eligible",
                    "exclude_reason",
                    "year",
                    "month",
                ],
            )
            result = validate_monthly_universe_df(monthly_universe_cache)
            _append_partition_schema_issue(result, lake_store, "monthly_universe")
        elif table_name == "monthly_snapshot_base":
            if monthly_universe_cache is None:
                monthly_universe_cache = lake_store.read_table(
                    "monthly_universe",
                    columns=["rebalance_date", "trade_execution_date", "ts_code", "is_eligible", "exclude_reason"],
                )
            snapshot_columns = _first_partition_columns(lake_store, "monthly_snapshot_base")
            required_columns = [
                *REQUIRED_COLUMNS["monthly_snapshot_base"],
                *[column for column in snapshot_columns if column.endswith("_tradable_date") and column != "trade_execution_date"],
                *[column for column in snapshot_columns if column.endswith("_report_period")],
            ]
            required_columns = list(dict.fromkeys(required_columns))
            df = lake_store.read_table("monthly_snapshot_base", columns=required_columns)
            result = validate_monthly_snapshot_base_df(df, monthly_universe_cache)
            _append_partition_schema_issue(result, lake_store, "monthly_snapshot_base")
            _append_snapshot_coverage_threshold_issues(result, config)
        else:  # pragma: no cover - CLI constrains table names
            raise KeyError(f"Unsupported validation target: {table_name}")

        logger.info(
            "Validation finished for %s: passed=%s errors=%s warnings=%s",
            result.table_name,
            result.passed,
            result.error_count,
            result.warning_count,
        )
        results.append(result)

    return results
