from __future__ import annotations

import numpy as np
import pandas as pd

from src.features.base import BaseFeatureBuilder, FeatureContext
from src.features.preprocessing import apply_cross_section_preprocess
from src.features.registry import FACTOR_REGISTRY, FactorSpec


DAILY_RISK_FACTOR_NAMES = {"beta", "volatility", "amihud_illiquidity", "idiosyncratic_volatility"}
ROLLING_WINDOW_DAYS = 252
ROLLING_MIN_OBS = 120


def _safe_log(series: pd.Series) -> pd.Series:
    output = pd.to_numeric(series, errors="coerce").astype("float64")
    output = output.where(output > 0)
    return np.log(output)


def _safe_inverse(series: pd.Series) -> pd.Series:
    output = pd.to_numeric(series, errors="coerce").astype("float64")
    output = output.where(output != 0)
    return 1.0 / output


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    num = pd.to_numeric(numerator, errors="coerce").astype("float64")
    den = pd.to_numeric(denominator, errors="coerce").astype("float64")
    den = den.where(den != 0)
    return num / den


def _sorted_snapshot(snapshot_df: pd.DataFrame) -> pd.DataFrame:
    working = snapshot_df.copy()
    working["rebalance_date"] = pd.to_datetime(working["rebalance_date"], errors="coerce")
    return working.sort_values(["ts_code", "rebalance_date"]).reset_index(drop=True)


def _compute_snapshot_formula(snapshot_df: pd.DataFrame, factor_name: str) -> pd.Series:
    if factor_name == "size":
        return _safe_log(snapshot_df["total_mv"])
    if factor_name == "book_to_market":
        return _safe_inverse(snapshot_df["pb"])
    if factor_name == "earnings_to_price":
        return _safe_inverse(snapshot_df["pe_ttm"])
    if factor_name == "sales_to_price":
        return _safe_inverse(snapshot_df["ps_ttm"])
    if factor_name == "cashflow_to_price":
        return _safe_divide(snapshot_df["cf_n_cashflow_act"], snapshot_df["total_mv"])
    if factor_name == "turnover":
        turnover = pd.to_numeric(snapshot_df["turnover_rate_f"], errors="coerce")
        return turnover.fillna(pd.to_numeric(snapshot_df["turnover_rate"], errors="coerce"))
    if factor_name == "roe":
        return pd.to_numeric(snapshot_df["fi_roe"], errors="coerce")
    if factor_name == "roa":
        return pd.to_numeric(snapshot_df["fi_roa"], errors="coerce")
    if factor_name == "gross_profitability":
        return _safe_divide(snapshot_df["inc_revenue"] - snapshot_df["inc_oper_cost"], snapshot_df["bs_total_assets"])
    if factor_name == "gross_margin":
        return pd.to_numeric(snapshot_df["fi_gross_margin"], errors="coerce")
    if factor_name == "operating_cash_flow_to_assets":
        return _safe_divide(snapshot_df["cf_n_cashflow_act"], snapshot_df["bs_total_assets"])
    if factor_name == "accruals":
        return _safe_divide(snapshot_df["inc_n_income_attr_p"] - snapshot_df["cf_n_cashflow_act"], snapshot_df["bs_total_assets"])
    if factor_name == "leverage":
        return _safe_divide(snapshot_df["bs_total_liab"], snapshot_df["bs_total_assets"])
    if factor_name == "net_operating_assets":
        return _safe_divide(snapshot_df["bs_total_assets"] - snapshot_df["bs_money_cap"] - snapshot_df["bs_total_liab"], snapshot_df["bs_total_assets"])
    raise ValueError(f"Unsupported snapshot formula factor: {factor_name}")


def _merge_series(frame: pd.DataFrame, series: pd.Series, name: str) -> pd.DataFrame:
    output = frame.copy()
    output[name] = series.astype("float64")
    return output


def _compute_monthly_lagged_factors(snapshot_df: pd.DataFrame, factor_names: set[str]) -> pd.DataFrame:
    working = _sorted_snapshot(snapshot_df)
    group = working.groupby("ts_code", sort=False)

    output_columns = ["rebalance_date", "ts_code"]

    if factor_names & {"momentum_12_1", "momentum_6_1", "momentum_3_1", "reversal_1m"}:
        lag_adj_close_1 = group["adj_close"].shift(1)
        if "momentum_12_1" in factor_names:
            lag_adj_close_12 = group["adj_close"].shift(12)
            working["factor_momentum_12_1_raw"] = _safe_divide(lag_adj_close_1, lag_adj_close_12) - 1.0
            output_columns.append("factor_momentum_12_1_raw")
        if "momentum_6_1" in factor_names:
            lag_adj_close_6 = group["adj_close"].shift(6)
            working["factor_momentum_6_1_raw"] = _safe_divide(lag_adj_close_1, lag_adj_close_6) - 1.0
            output_columns.append("factor_momentum_6_1_raw")
        if "momentum_3_1" in factor_names:
            lag_adj_close_3 = group["adj_close"].shift(3)
            working["factor_momentum_3_1_raw"] = _safe_divide(lag_adj_close_1, lag_adj_close_3) - 1.0
            output_columns.append("factor_momentum_3_1_raw")
        if "reversal_1m" in factor_names:
            working["factor_reversal_1m_raw"] = -(_safe_divide(working["adj_close"], lag_adj_close_1) - 1.0)
            output_columns.append("factor_reversal_1m_raw")

    if "asset_growth" in factor_names:
        lag_assets_12 = group["bs_total_assets"].shift(12)
        working["factor_asset_growth_raw"] = _safe_divide(working["bs_total_assets"], lag_assets_12) - 1.0
        output_columns.append("factor_asset_growth_raw")
    if "inventory_growth" in factor_names:
        lag_inventory_12 = group["bs_inventories"].shift(12)
        working["factor_inventory_growth_raw"] = _safe_divide(working["bs_inventories"], lag_inventory_12) - 1.0
        output_columns.append("factor_inventory_growth_raw")

    return working.loc[:, output_columns]


def _compute_daily_risk_factors(adjusted_price_df: pd.DataFrame, rebalance_dates: pd.Series) -> pd.DataFrame:
    if adjusted_price_df.empty:
        return pd.DataFrame(
            columns=[
                "rebalance_date",
                "ts_code",
                "factor_beta_raw",
                "factor_volatility_raw",
                "factor_amihud_illiquidity_raw",
                "factor_idiosyncratic_volatility_raw",
            ]
        )

    prices = adjusted_price_df.loc[:, ["ts_code", "trade_date", "adj_close", "amount"]].copy()
    prices["trade_date"] = pd.to_datetime(prices["trade_date"], errors="coerce")
    prices = prices.sort_values(["ts_code", "trade_date"]).drop_duplicates(subset=["ts_code", "trade_date"], keep="last")

    group = prices.groupby("ts_code", sort=False)
    prices["daily_ret"] = group["adj_close"].pct_change()
    market_ret = prices.groupby("trade_date", sort=False)["daily_ret"].mean().rename("market_ret")
    prices = prices.merge(market_ret, on="trade_date", how="left")

    prices["ret_sq"] = prices["daily_ret"] ** 2
    prices["market_sq"] = prices["market_ret"] ** 2
    prices["ret_market"] = prices["daily_ret"] * prices["market_ret"]
    prices["abs_ret_over_amount"] = np.where(
        pd.to_numeric(prices["amount"], errors="coerce") > 0,
        prices["daily_ret"].abs() / pd.to_numeric(prices["amount"], errors="coerce"),
        np.nan,
    )

    rolling_group = prices.groupby("ts_code", sort=False)
    prices["ret_mean"] = rolling_group["daily_ret"].rolling(ROLLING_WINDOW_DAYS, min_periods=ROLLING_MIN_OBS).mean().reset_index(level=0, drop=True)
    prices["ret_sq_mean"] = rolling_group["ret_sq"].rolling(ROLLING_WINDOW_DAYS, min_periods=ROLLING_MIN_OBS).mean().reset_index(level=0, drop=True)
    prices["market_mean"] = rolling_group["market_ret"].rolling(ROLLING_WINDOW_DAYS, min_periods=ROLLING_MIN_OBS).mean().reset_index(level=0, drop=True)
    prices["market_sq_mean"] = rolling_group["market_sq"].rolling(ROLLING_WINDOW_DAYS, min_periods=ROLLING_MIN_OBS).mean().reset_index(level=0, drop=True)
    prices["ret_market_mean"] = rolling_group["ret_market"].rolling(ROLLING_WINDOW_DAYS, min_periods=ROLLING_MIN_OBS).mean().reset_index(level=0, drop=True)
    prices["amihud_mean"] = rolling_group["abs_ret_over_amount"].rolling(ROLLING_WINDOW_DAYS, min_periods=ROLLING_MIN_OBS).mean().reset_index(level=0, drop=True)

    prices["var_i"] = prices["ret_sq_mean"] - prices["ret_mean"] ** 2
    prices["var_m"] = prices["market_sq_mean"] - prices["market_mean"] ** 2
    prices["cov_im"] = prices["ret_market_mean"] - prices["ret_mean"] * prices["market_mean"]

    prices["factor_beta_raw"] = np.where(prices["var_m"] > 0, prices["cov_im"] / prices["var_m"], np.nan)
    residual_var = prices["var_i"] - np.where(prices["var_m"] > 0, (prices["cov_im"] ** 2) / prices["var_m"], np.nan)
    prices["factor_volatility_raw"] = np.sqrt(prices["var_i"].clip(lower=0))
    prices["factor_idiosyncratic_volatility_raw"] = np.sqrt(pd.Series(residual_var, index=prices.index).clip(lower=0))
    prices["factor_amihud_illiquidity_raw"] = prices["amihud_mean"]

    rebalance_set = set(pd.to_datetime(rebalance_dates, errors="coerce").dropna().unique())
    monthly = prices.loc[prices["trade_date"].isin(rebalance_set)].copy()
    monthly = monthly.rename(columns={"trade_date": "rebalance_date"})
    return monthly.loc[
        :,
        [
            "rebalance_date",
            "ts_code",
            "factor_beta_raw",
            "factor_volatility_raw",
            "factor_amihud_illiquidity_raw",
            "factor_idiosyncratic_volatility_raw",
        ],
    ]


def build_factor_panel(
    snapshot_df: pd.DataFrame,
    adjusted_price_df: pd.DataFrame,
    specs: list[FactorSpec],
) -> pd.DataFrame:
    if not specs:
        raise ValueError("At least one factor spec is required.")

    base_columns = [
        "rebalance_date",
        "trade_execution_date",
        "ts_code",
        "is_eligible",
        "exclude_reason",
        "year",
        "month",
    ]
    missing = [column for column in base_columns if column not in snapshot_df.columns]
    if missing:
        raise KeyError(f"monthly_snapshot_base is missing required factor-panel columns: {missing}")

    panel = snapshot_df.loc[:, base_columns].copy()
    panel["rebalance_date"] = pd.to_datetime(panel["rebalance_date"], errors="coerce")
    panel["trade_execution_date"] = pd.to_datetime(panel["trade_execution_date"], errors="coerce")
    working_snapshot = _sorted_snapshot(snapshot_df)
    panel = panel.sort_values(["rebalance_date", "ts_code"]).reset_index(drop=True)

    requested_names = {spec.name for spec in specs}

    snapshot_formula_names = {
        "size",
        "book_to_market",
        "earnings_to_price",
        "sales_to_price",
        "cashflow_to_price",
        "turnover",
        "roe",
        "roa",
        "gross_profitability",
        "gross_margin",
        "operating_cash_flow_to_assets",
        "accruals",
        "leverage",
        "net_operating_assets",
    }
    lagged_factor_names = {
        "momentum_12_1",
        "momentum_6_1",
        "momentum_3_1",
        "reversal_1m",
        "asset_growth",
        "inventory_growth",
    }

    for factor_name in sorted(requested_names & snapshot_formula_names):
        spec = FACTOR_REGISTRY.get(factor_name)
        raw_col = f"{spec.output_field}_raw"
        raw_values = _compute_snapshot_formula(working_snapshot, factor_name)
        merged = _merge_series(working_snapshot.loc[:, ["rebalance_date", "ts_code"]], raw_values, raw_col)
        panel = panel.merge(merged, on=["rebalance_date", "ts_code"], how="left")
        panel = apply_cross_section_preprocess(panel, raw_col=raw_col, output_col=spec.output_field, steps=spec.preprocess)

    if requested_names & lagged_factor_names:
        lagged = _compute_monthly_lagged_factors(working_snapshot, requested_names & lagged_factor_names)
        panel = panel.merge(lagged, on=["rebalance_date", "ts_code"], how="left")
        for factor_name in sorted(requested_names & lagged_factor_names):
            spec = FACTOR_REGISTRY.get(factor_name)
            raw_col = f"{spec.output_field}_raw"
            panel = apply_cross_section_preprocess(panel, raw_col=raw_col, output_col=spec.output_field, steps=spec.preprocess)

    if requested_names & DAILY_RISK_FACTOR_NAMES:
        daily = _compute_daily_risk_factors(adjusted_price_df, panel["rebalance_date"])
        panel = panel.merge(daily, on=["rebalance_date", "ts_code"], how="left")
        for factor_name in sorted(requested_names & DAILY_RISK_FACTOR_NAMES):
            spec = FACTOR_REGISTRY.get(factor_name)
            raw_col = f"{spec.output_field}_raw"
            panel = apply_cross_section_preprocess(panel, raw_col=raw_col, output_col=spec.output_field, steps=spec.preprocess)

    ordered_columns = base_columns.copy()
    for spec in specs:
        ordered_columns.extend([f"{spec.output_field}_raw", spec.output_field])
    ordered_columns = [column for column in ordered_columns if column in panel.columns]
    return panel.loc[:, ordered_columns].sort_values(["rebalance_date", "ts_code"]).reset_index(drop=True)


class DefaultFeatureBuilder(BaseFeatureBuilder):
    name = "default"

    def build(
        self,
        snapshot_df: pd.DataFrame,
        adjusted_price_df: pd.DataFrame,
        context: FeatureContext,
    ) -> pd.DataFrame:
        specs = FACTOR_REGISTRY.list(names=context.factor_names if context.factor_names else None)
        return build_factor_panel(snapshot_df, adjusted_price_df, specs)
