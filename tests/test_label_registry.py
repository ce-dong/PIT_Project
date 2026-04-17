from __future__ import annotations

import pytest

from src.labels.registry import LABEL_REGISTRY, LABEL_STAGE_ORDER, LabelRegistry, LabelSpec


def test_label_registry_contains_forward_return_catalog():
    assert len(LABEL_REGISTRY) == 3
    assert LABEL_REGISTRY.stages() == LABEL_STAGE_ORDER
    assert LABEL_REGISTRY.get("fwd_ret_1m").output_field == "label_fwd_ret_1m"
    assert LABEL_REGISTRY.get("fwd_ret_6m").horizon_months == 6


def test_label_registry_can_filter_by_stage_and_names():
    stage_specs = LABEL_REGISTRY.list(stage="forward_return")
    selected_specs = LABEL_REGISTRY.list(names=["fwd_ret_3m", "fwd_ret_1m"])

    assert [spec.name for spec in stage_specs] == ["fwd_ret_1m", "fwd_ret_3m", "fwd_ret_6m"]
    assert [spec.name for spec in selected_specs] == ["fwd_ret_1m", "fwd_ret_3m"]


def test_label_registry_rejects_duplicate_names_and_output_fields():
    registry = LabelRegistry()
    base_spec = LabelSpec(
        name="fwd_ret_test",
        stage="forward_return",
        description="Test label.",
        formula="end / start - 1",
        inputs=("adjusted_price_panel.adj_close",),
        start_anchor="trade_execution_date",
        window_rule="full_window_required",
        missing_policy="Drop when prices are missing.",
        output_field="label_fwd_ret_test",
        horizon_months=1,
    )
    registry.register(base_spec)

    with pytest.raises(ValueError, match="already registered"):
        registry.register(base_spec)

    with pytest.raises(ValueError, match="already used"):
        registry.register(
            LabelSpec(
                name="fwd_ret_test_2",
                stage="forward_return",
                description="Another test label.",
                formula="end / start - 1",
                inputs=("adjusted_price_panel.adj_close",),
                start_anchor="trade_execution_date",
                window_rule="full_window_required",
                missing_policy="Drop when prices are missing.",
                output_field="label_fwd_ret_test",
                horizon_months=3,
            )
        )


def test_label_spec_validates_required_metadata():
    with pytest.raises(ValueError, match="snake_case"):
        LabelSpec(
            name="FwdRet1M",
            stage="forward_return",
            description="Bad name.",
            formula="end / start - 1",
            inputs=("adjusted_price_panel.adj_close",),
            start_anchor="trade_execution_date",
            window_rule="full_window_required",
            missing_policy="Drop when prices are missing.",
            output_field="label_fwd_ret_bad",
            horizon_months=1,
        )

    with pytest.raises(ValueError, match="positive"):
        LabelSpec(
            name="fwd_ret_bad",
            stage="forward_return",
            description="Bad horizon.",
            formula="end / start - 1",
            inputs=("adjusted_price_panel.adj_close",),
            start_anchor="trade_execution_date",
            window_rule="full_window_required",
            missing_policy="Drop when prices are missing.",
            output_field="label_fwd_ret_bad",
            horizon_months=0,
        )
