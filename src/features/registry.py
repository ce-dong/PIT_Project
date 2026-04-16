from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable


FAMILY_ORDER = ("valuation", "momentum", "risk_liquidity", "quality", "investment")
_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


def _normalize_tokens(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(value.strip() for value in values if value and value.strip())


def _validate_identifier(value: str, *, label: str) -> None:
    if not _NAME_PATTERN.fullmatch(value):
        raise ValueError(f"{label} must match snake_case pattern: {value!r}")


@dataclass(frozen=True)
class FactorSpec:
    name: str
    family: str
    description: str
    formula: str
    inputs: tuple[str, ...]
    lag_rule: str
    preprocess: tuple[str, ...]
    output_field: str

    def __post_init__(self) -> None:
        _validate_identifier(self.name, label="Factor name")
        _validate_identifier(self.output_field, label="Output field")
        if self.family not in FAMILY_ORDER:
            raise ValueError(f"Unsupported factor family: {self.family}")
        if not self.description.strip():
            raise ValueError(f"Factor description cannot be empty for {self.name!r}")
        if not self.formula.strip():
            raise ValueError(f"Factor formula cannot be empty for {self.name!r}")
        if not self.inputs:
            raise ValueError(f"Factor inputs cannot be empty for {self.name!r}")
        if not self.lag_rule.strip():
            raise ValueError(f"Factor lag rule cannot be empty for {self.name!r}")
        if not self.preprocess:
            raise ValueError(f"Factor preprocess steps cannot be empty for {self.name!r}")
        object.__setattr__(self, "inputs", _normalize_tokens(self.inputs))
        object.__setattr__(self, "preprocess", _normalize_tokens(self.preprocess))

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "family": self.family,
            "description": self.description,
            "formula": self.formula,
            "inputs": list(self.inputs),
            "lag_rule": self.lag_rule,
            "preprocess": list(self.preprocess),
            "output_field": self.output_field,
        }


class FactorRegistry:
    def __init__(self) -> None:
        self._specs: dict[str, FactorSpec] = {}
        self._output_field_index: dict[str, str] = {}

    def register(self, spec: FactorSpec) -> None:
        if spec.name in self._specs:
            raise ValueError(f"Factor {spec.name!r} is already registered.")
        if spec.output_field in self._output_field_index:
            owner = self._output_field_index[spec.output_field]
            raise ValueError(f"Output field {spec.output_field!r} is already used by factor {owner!r}.")
        self._specs[spec.name] = spec
        self._output_field_index[spec.output_field] = spec.name

    def register_many(self, specs: Iterable[FactorSpec]) -> None:
        for spec in specs:
            self.register(spec)

    def get(self, name: str) -> FactorSpec:
        try:
            return self._specs[name]
        except KeyError as error:
            raise KeyError(f"Factor {name!r} is not registered.") from error

    def list(self, *, family: str | None = None, names: Iterable[str] | None = None) -> list[FactorSpec]:
        if family is not None and family not in FAMILY_ORDER:
            raise ValueError(f"Unsupported factor family: {family}")

        if names is not None:
            resolved = [self.get(name) for name in names]
        else:
            resolved = list(self._specs.values())

        if family is not None:
            resolved = [spec for spec in resolved if spec.family == family]

        return sorted(
            resolved,
            key=lambda spec: (FAMILY_ORDER.index(spec.family), spec.name),
        )

    def families(self) -> tuple[str, ...]:
        active = {spec.family for spec in self._specs.values()}
        return tuple(family for family in FAMILY_ORDER if family in active)

    def output_fields(self) -> tuple[str, ...]:
        return tuple(spec.output_field for spec in self.list())

    def __contains__(self, name: str) -> bool:
        return name in self._specs

    def __len__(self) -> int:
        return len(self._specs)


VALUATION_FACTOR_SPECS = (
    FactorSpec(
        name="size",
        family="valuation",
        description="Log market capitalization exposure computed from the latest point-in-time market value snapshot.",
        formula="log(total_mv)",
        inputs=("monthly_snapshot_base.total_mv",),
        lag_rule="Use the latest market snapshot with trade_date <= rebalance_date.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_size",
    ),
    FactorSpec(
        name="book_to_market",
        family="valuation",
        description="Book-to-market proxy using the inverse of the latest point-in-time price-to-book ratio.",
        formula="1 / pb",
        inputs=("monthly_snapshot_base.pb",),
        lag_rule="Use the latest market snapshot with trade_date <= rebalance_date.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_book_to_market",
    ),
    FactorSpec(
        name="earnings_to_price",
        family="valuation",
        description="Earnings yield proxy using the inverse of point-in-time trailing PE.",
        formula="1 / pe_ttm",
        inputs=("monthly_snapshot_base.pe_ttm",),
        lag_rule="Use the latest market snapshot with trade_date <= rebalance_date.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_earnings_to_price",
    ),
    FactorSpec(
        name="sales_to_price",
        family="valuation",
        description="Sales yield proxy using the inverse of point-in-time trailing price-to-sales.",
        formula="1 / ps_ttm",
        inputs=("monthly_snapshot_base.ps_ttm",),
        lag_rule="Use the latest market snapshot with trade_date <= rebalance_date.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_sales_to_price",
    ),
    FactorSpec(
        name="cashflow_to_price",
        family="valuation",
        description="Operating cash-flow yield using point-in-time operating cash flow scaled by market value.",
        formula="cf_n_cashflow_act / total_mv",
        inputs=("monthly_snapshot_base.cf_n_cashflow_act", "monthly_snapshot_base.total_mv"),
        lag_rule="Use the latest cashflow snapshot with cf_tradable_date <= trade_execution_date and scale by market snapshot at rebalance.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_cashflow_to_price",
    ),
)

MOMENTUM_FACTOR_SPECS = (
    FactorSpec(
        name="momentum_12_1",
        family="momentum",
        description="Twelve-to-one-month momentum measured from adjusted prices, skipping the most recent month.",
        formula="adj_return[t-12m, t-1m]",
        inputs=("adjusted_price_panel.adj_close", "calendar_table.trade_date"),
        lag_rule="Use only prices with trade_date <= rebalance_date and exclude the most recent one-month window.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_momentum_12_1",
    ),
    FactorSpec(
        name="momentum_6_1",
        family="momentum",
        description="Six-to-one-month momentum measured from adjusted prices, skipping the most recent month.",
        formula="adj_return[t-6m, t-1m]",
        inputs=("adjusted_price_panel.adj_close", "calendar_table.trade_date"),
        lag_rule="Use only prices with trade_date <= rebalance_date and exclude the most recent one-month window.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_momentum_6_1",
    ),
    FactorSpec(
        name="momentum_3_1",
        family="momentum",
        description="Three-to-one-month momentum measured from adjusted prices, skipping the most recent month.",
        formula="adj_return[t-3m, t-1m]",
        inputs=("adjusted_price_panel.adj_close", "calendar_table.trade_date"),
        lag_rule="Use only prices with trade_date <= rebalance_date and exclude the most recent one-month window.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_momentum_3_1",
    ),
    FactorSpec(
        name="reversal_1m",
        family="momentum",
        description="Short-term reversal computed as the negative of the most recent one-month adjusted return.",
        formula="-adj_return[t-1m, t]",
        inputs=("adjusted_price_panel.adj_close", "calendar_table.trade_date"),
        lag_rule="Use only prices with trade_date <= rebalance_date for the trailing one-month window.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_reversal_1m",
    ),
)

RISK_LIQUIDITY_FACTOR_SPECS = (
    FactorSpec(
        name="beta",
        family="risk_liquidity",
        description="Rolling market beta from daily adjusted returns over a trailing one-year window.",
        formula="cov(ret_i, ret_mkt) / var(ret_mkt)",
        inputs=("adjusted_price_panel.adj_close", "calendar_table.trade_date"),
        lag_rule="Use only prices with trade_date <= rebalance_date over the trailing 12-month estimation window.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_beta",
    ),
    FactorSpec(
        name="volatility",
        family="risk_liquidity",
        description="Trailing realized volatility from daily adjusted returns.",
        formula="std(daily_adj_returns)",
        inputs=("adjusted_price_panel.adj_close", "calendar_table.trade_date"),
        lag_rule="Use only prices with trade_date <= rebalance_date over the trailing estimation window.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_volatility",
    ),
    FactorSpec(
        name="turnover",
        family="risk_liquidity",
        description="Trailing turnover intensity using point-in-time daily_basic turnover fields.",
        formula="mean(turnover_rate)",
        inputs=("monthly_snapshot_base.turnover_rate", "monthly_snapshot_base.turnover_rate_f"),
        lag_rule="Use the latest daily_basic snapshot with trade_date <= rebalance_date; extend later to rolling aggregation if required.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_turnover",
    ),
    FactorSpec(
        name="amihud_illiquidity",
        family="risk_liquidity",
        description="Amihud illiquidity from absolute return scaled by traded amount.",
        formula="mean(abs(ret_d) / amount_d)",
        inputs=("adjusted_price_panel.adj_close", "monthly_snapshot_base.amount"),
        lag_rule="Use only prices and traded amount available on or before rebalance_date over the trailing estimation window.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_amihud_illiquidity",
    ),
    FactorSpec(
        name="idiosyncratic_volatility",
        family="risk_liquidity",
        description="Residual volatility from a trailing market-model regression.",
        formula="std(ret_i - alpha - beta * ret_mkt)",
        inputs=("adjusted_price_panel.adj_close", "calendar_table.trade_date"),
        lag_rule="Use only prices with trade_date <= rebalance_date over the trailing one-year estimation window.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_idiosyncratic_volatility",
    ),
)

QUALITY_FACTOR_SPECS = (
    FactorSpec(
        name="roe",
        family="quality",
        description="Point-in-time return on equity from the latest tradable financial indicator snapshot.",
        formula="fi_roe",
        inputs=("monthly_snapshot_base.fi_roe",),
        lag_rule="Use the latest fina_indicator snapshot with fi_tradable_date <= trade_execution_date.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_roe",
    ),
    FactorSpec(
        name="roa",
        family="quality",
        description="Point-in-time return on assets from the latest tradable financial indicator snapshot.",
        formula="fi_roa",
        inputs=("monthly_snapshot_base.fi_roa",),
        lag_rule="Use the latest fina_indicator snapshot with fi_tradable_date <= trade_execution_date.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_roa",
    ),
    FactorSpec(
        name="gross_profitability",
        family="quality",
        description="Gross profitability using point-in-time revenue minus operating cost scaled by total assets.",
        formula="(inc_revenue - inc_oper_cost) / bs_total_assets",
        inputs=(
            "monthly_snapshot_base.inc_revenue",
            "monthly_snapshot_base.inc_oper_cost",
            "monthly_snapshot_base.bs_total_assets",
        ),
        lag_rule="Use income and balance-sheet snapshots whose tradable_date is <= trade_execution_date.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_gross_profitability",
    ),
    FactorSpec(
        name="gross_margin",
        family="quality",
        description="Point-in-time gross margin from the latest financial indicator snapshot.",
        formula="fi_gross_margin",
        inputs=("monthly_snapshot_base.fi_gross_margin",),
        lag_rule="Use the latest fina_indicator snapshot with fi_tradable_date <= trade_execution_date.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_gross_margin",
    ),
    FactorSpec(
        name="operating_cash_flow_to_assets",
        family="quality",
        description="Operating cash flow scaled by total assets using point-in-time cashflow and balance-sheet snapshots.",
        formula="cf_n_cashflow_act / bs_total_assets",
        inputs=("monthly_snapshot_base.cf_n_cashflow_act", "monthly_snapshot_base.bs_total_assets"),
        lag_rule="Use cashflow and balance-sheet snapshots whose tradable_date is <= trade_execution_date.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_operating_cash_flow_to_assets",
    ),
    FactorSpec(
        name="accruals",
        family="quality",
        description="Accruals proxy from earnings minus operating cash flow scaled by total assets.",
        formula="(inc_n_income_attr_p - cf_n_cashflow_act) / bs_total_assets",
        inputs=(
            "monthly_snapshot_base.inc_n_income_attr_p",
            "monthly_snapshot_base.cf_n_cashflow_act",
            "monthly_snapshot_base.bs_total_assets",
        ),
        lag_rule="Use income, cashflow, and balance-sheet snapshots whose tradable_date is <= trade_execution_date.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_accruals",
    ),
)

INVESTMENT_FACTOR_SPECS = (
    FactorSpec(
        name="asset_growth",
        family="investment",
        description="Trailing asset growth from point-in-time total assets across annual reporting intervals.",
        formula="growth(bs_total_assets, 12m)",
        inputs=("monthly_snapshot_base.bs_total_assets", "monthly_snapshot_base.bs_report_period"),
        lag_rule="Use balance-sheet snapshots whose tradable_date is <= trade_execution_date and compare against the prior annual period.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_asset_growth",
    ),
    FactorSpec(
        name="inventory_growth",
        family="investment",
        description="Trailing inventory growth from point-in-time balance-sheet inventory values.",
        formula="growth(bs_inventories, 12m)",
        inputs=("monthly_snapshot_base.bs_inventories", "monthly_snapshot_base.bs_report_period"),
        lag_rule="Use balance-sheet snapshots whose tradable_date is <= trade_execution_date and compare against the prior annual period.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_inventory_growth",
    ),
    FactorSpec(
        name="leverage",
        family="investment",
        description="Balance-sheet leverage measured as total liabilities scaled by total assets.",
        formula="bs_total_liab / bs_total_assets",
        inputs=("monthly_snapshot_base.bs_total_liab", "monthly_snapshot_base.bs_total_assets"),
        lag_rule="Use the latest balance-sheet snapshot with bs_tradable_date <= trade_execution_date.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_leverage",
    ),
    FactorSpec(
        name="net_operating_assets",
        family="investment",
        description="Net operating assets proxy from operating asset base scaled by total assets.",
        formula="(bs_total_assets - bs_money_cap - bs_total_liab) / bs_total_assets",
        inputs=(
            "monthly_snapshot_base.bs_total_assets",
            "monthly_snapshot_base.bs_money_cap",
            "monthly_snapshot_base.bs_total_liab",
        ),
        lag_rule="Use the latest balance-sheet snapshot with bs_tradable_date <= trade_execution_date.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_net_operating_assets",
    ),
)

DEFAULT_FACTOR_SPECS = (
    *VALUATION_FACTOR_SPECS,
    *MOMENTUM_FACTOR_SPECS,
    *RISK_LIQUIDITY_FACTOR_SPECS,
    *QUALITY_FACTOR_SPECS,
    *INVESTMENT_FACTOR_SPECS,
)

FACTOR_REGISTRY = FactorRegistry()
FACTOR_REGISTRY.register_many(DEFAULT_FACTOR_SPECS)
