from __future__ import annotations

import pytest

from src.features.registry import FACTOR_REGISTRY, FAMILY_ORDER, FactorRegistry, FactorSpec


def test_factor_registry_contains_v1_factor_catalog():
    assert len(FACTOR_REGISTRY) == 24
    assert FACTOR_REGISTRY.families() == FAMILY_ORDER
    assert FACTOR_REGISTRY.get("size").output_field == "factor_size"
    assert FACTOR_REGISTRY.get("reversal_1m").family == "momentum"
    assert FACTOR_REGISTRY.get("roe").inputs == ("monthly_snapshot_base.fi_roe",)


def test_factor_registry_can_filter_by_family_and_names():
    momentum_specs = FACTOR_REGISTRY.list(family="momentum")
    selected_specs = FACTOR_REGISTRY.list(names=["asset_growth", "size"])

    assert [spec.name for spec in momentum_specs] == [
        "momentum_12_1",
        "momentum_3_1",
        "momentum_6_1",
        "reversal_1m",
    ]
    assert [spec.name for spec in selected_specs] == ["size", "asset_growth"]


def test_factor_registry_rejects_duplicate_names_and_output_fields():
    registry = FactorRegistry()
    base_spec = FactorSpec(
        name="test_factor",
        family="valuation",
        description="Test factor.",
        formula="x",
        inputs=("monthly_snapshot_base.pb",),
        lag_rule="Use the latest available snapshot.",
        preprocess=("winsorize", "zscore"),
        output_field="factor_test_factor",
    )
    registry.register(base_spec)

    with pytest.raises(ValueError, match="already registered"):
        registry.register(base_spec)

    with pytest.raises(ValueError, match="already used"):
        registry.register(
            FactorSpec(
                name="another_test_factor",
                family="valuation",
                description="Another test factor.",
                formula="y",
                inputs=("monthly_snapshot_base.pe_ttm",),
                lag_rule="Use the latest available snapshot.",
                preprocess=("winsorize", "zscore"),
                output_field="factor_test_factor",
            )
        )


def test_factor_spec_validates_required_metadata():
    with pytest.raises(ValueError, match="snake_case"):
        FactorSpec(
            name="BadFactor",
            family="valuation",
            description="Bad name.",
            formula="x",
            inputs=("monthly_snapshot_base.pb",),
            lag_rule="Use the latest available snapshot.",
            preprocess=("winsorize", "zscore"),
            output_field="factor_bad",
        )

    with pytest.raises(ValueError, match="preprocess steps cannot be empty"):
        FactorSpec(
            name="bad_factor",
            family="valuation",
            description="Missing preprocess.",
            formula="x",
            inputs=("monthly_snapshot_base.pb",),
            lag_rule="Use the latest available snapshot.",
            preprocess=(),
            output_field="factor_bad_factor",
        )
