from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable


LABEL_STAGE_ORDER = ("forward_return",)
_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
_DATE_ANCHOR_OPTIONS = ("trade_execution_date", "rebalance_date")
_WINDOW_RULE_OPTIONS = ("full_window_required", "allow_partial_window")


def _normalize_tokens(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(value.strip() for value in values if value and value.strip())


def _validate_identifier(value: str, *, label: str) -> None:
    if not _NAME_PATTERN.fullmatch(value):
        raise ValueError(f"{label} must match snake_case pattern: {value!r}")


@dataclass(frozen=True)
class LabelSpec:
    name: str
    stage: str
    description: str
    formula: str
    inputs: tuple[str, ...]
    start_anchor: str
    window_rule: str
    missing_policy: str
    output_field: str
    horizon_months: int

    def __post_init__(self) -> None:
        _validate_identifier(self.name, label="Label name")
        _validate_identifier(self.output_field, label="Output field")
        if self.stage not in LABEL_STAGE_ORDER:
            raise ValueError(f"Unsupported label stage: {self.stage}")
        if not self.description.strip():
            raise ValueError(f"Label description cannot be empty for {self.name!r}")
        if not self.formula.strip():
            raise ValueError(f"Label formula cannot be empty for {self.name!r}")
        if not self.inputs:
            raise ValueError(f"Label inputs cannot be empty for {self.name!r}")
        if self.start_anchor not in _DATE_ANCHOR_OPTIONS:
            raise ValueError(f"Unsupported label start anchor: {self.start_anchor}")
        if self.window_rule not in _WINDOW_RULE_OPTIONS:
            raise ValueError(f"Unsupported label window rule: {self.window_rule}")
        if not self.missing_policy.strip():
            raise ValueError(f"Label missing policy cannot be empty for {self.name!r}")
        if self.horizon_months <= 0:
            raise ValueError(f"Label horizon_months must be positive for {self.name!r}")
        object.__setattr__(self, "inputs", _normalize_tokens(self.inputs))

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "stage": self.stage,
            "description": self.description,
            "formula": self.formula,
            "inputs": list(self.inputs),
            "start_anchor": self.start_anchor,
            "window_rule": self.window_rule,
            "missing_policy": self.missing_policy,
            "output_field": self.output_field,
            "horizon_months": self.horizon_months,
        }


class LabelRegistry:
    def __init__(self) -> None:
        self._specs: dict[str, LabelSpec] = {}
        self._output_field_index: dict[str, str] = {}

    def register(self, spec: LabelSpec) -> None:
        if spec.name in self._specs:
            raise ValueError(f"Label {spec.name!r} is already registered.")
        if spec.output_field in self._output_field_index:
            owner = self._output_field_index[spec.output_field]
            raise ValueError(f"Output field {spec.output_field!r} is already used by label {owner!r}.")
        self._specs[spec.name] = spec
        self._output_field_index[spec.output_field] = spec.name

    def register_many(self, specs: Iterable[LabelSpec]) -> None:
        for spec in specs:
            self.register(spec)

    def get(self, name: str) -> LabelSpec:
        try:
            return self._specs[name]
        except KeyError as error:
            raise KeyError(f"Label {name!r} is not registered.") from error

    def list(self, *, stage: str | None = None, names: Iterable[str] | None = None) -> list[LabelSpec]:
        if stage is not None and stage not in LABEL_STAGE_ORDER:
            raise ValueError(f"Unsupported label stage: {stage}")

        if names is not None:
            resolved = [self.get(name) for name in names]
        else:
            resolved = list(self._specs.values())

        if stage is not None:
            resolved = [spec for spec in resolved if spec.stage == stage]

        return sorted(resolved, key=lambda spec: (LABEL_STAGE_ORDER.index(spec.stage), spec.horizon_months, spec.name))

    def stages(self) -> tuple[str, ...]:
        active = {spec.stage for spec in self._specs.values()}
        return tuple(stage for stage in LABEL_STAGE_ORDER if stage in active)

    def output_fields(self) -> tuple[str, ...]:
        return tuple(spec.output_field for spec in self.list())

    def __contains__(self, name: str) -> bool:
        return name in self._specs

    def __len__(self) -> int:
        return len(self._specs)


FORWARD_RETURN_LABEL_SPECS = (
    LabelSpec(
        name="fwd_ret_1m",
        stage="forward_return",
        description="One-month forward return from adjusted close prices after the rebalance decision date.",
        formula="adj_close[t+1m] / adj_close[t+1d] - 1",
        inputs=("calendar_table.trade_date", "adjusted_price_panel.adj_close", "monthly_universe.trade_execution_date"),
        start_anchor="trade_execution_date",
        window_rule="full_window_required",
        missing_policy="Drop labels when either the start or end adjusted close is unavailable for the full horizon.",
        output_field="label_fwd_ret_1m",
        horizon_months=1,
    ),
    LabelSpec(
        name="fwd_ret_3m",
        stage="forward_return",
        description="Three-month forward return from adjusted close prices after the rebalance decision date.",
        formula="adj_close[t+3m] / adj_close[t+1d] - 1",
        inputs=("calendar_table.trade_date", "adjusted_price_panel.adj_close", "monthly_universe.trade_execution_date"),
        start_anchor="trade_execution_date",
        window_rule="full_window_required",
        missing_policy="Drop labels when either the start or end adjusted close is unavailable for the full horizon.",
        output_field="label_fwd_ret_3m",
        horizon_months=3,
    ),
    LabelSpec(
        name="fwd_ret_6m",
        stage="forward_return",
        description="Six-month forward return from adjusted close prices after the rebalance decision date.",
        formula="adj_close[t+6m] / adj_close[t+1d] - 1",
        inputs=("calendar_table.trade_date", "adjusted_price_panel.adj_close", "monthly_universe.trade_execution_date"),
        start_anchor="trade_execution_date",
        window_rule="full_window_required",
        missing_policy="Drop labels when either the start or end adjusted close is unavailable for the full horizon.",
        output_field="label_fwd_ret_6m",
        horizon_months=6,
    ),
)

DEFAULT_LABEL_SPECS = (*FORWARD_RETURN_LABEL_SPECS,)

LABEL_REGISTRY = LabelRegistry()
LABEL_REGISTRY.register_many(DEFAULT_LABEL_SPECS)
