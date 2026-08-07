from __future__ import annotations

from copy import deepcopy
from pathlib import Path

import pytest

from our_system_phase2.services.candidate_program_materialization_v1 import (
    ADAPTER_MARKET_PRELAGGED_BROADCAST,
    ADAPTER_STOCK_SESSION_CLOSE,
    resolve_program_materialization_plan_v1,
    verify_program_materialization_plan_v1,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "runtime/field_registry/cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)


def _compiled(*fields: str) -> dict:
    return {
        "physical_leaf_ids": list(fields),
        "external_adapter_requirements": [],
        "component_clock_requirements": {
            "component": {
                "field_requirements": {field: {} for field in fields},
            }
        },
    }


def _schedule() -> list[dict]:
    return [
        {
            "main_record_ordinal": 1,
            "pair_id": "cn.program_pair.test",
            "template_id": "BASE_MARKET",
            "primary_compiled": _compiled(
                "close", "ctx_sent_uplimit_num", "intraday_ret_from_open"
            ),
            "control_compiled": _compiled(
                "close", "ctx_sent_uplimit_num", "intraday_ret_from_open"
            ),
        }
    ]


def test_plan_uses_complete_compiled_union_and_scope_aware_adapters() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)

    plan = resolve_program_materialization_plan_v1(
        _schedule(), registry=registry, available_fields=["close", "unrelated_legacy_field"]
    )
    verify_program_materialization_plan_v1(plan)

    assert plan["required_physical_leaf_ids"] == [
        "close",
        "ctx_sent_uplimit_num",
        "intraday_ret_from_open",
    ]
    assert plan["missing_required_field_ids"] == [
        "ctx_sent_uplimit_num",
        "intraday_ret_from_open",
    ]
    adapters = {row["field_id"]: row["adapter_id"] for row in plan["field_bindings"]}
    assert adapters["ctx_sent_uplimit_num"] == ADAPTER_MARKET_PRELAGGED_BROADCAST
    assert adapters["intraday_ret_from_open"] == ADAPTER_STOCK_SESSION_CLOSE


def test_plan_fails_when_component_clocks_do_not_cover_compiler_leaves() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    schedule = _schedule()
    schedule[0]["primary_compiled"]["component_clock_requirements"]["component"][
        "field_requirements"
    ].pop("intraday_ret_from_open")

    with pytest.raises(ValueError, match="physical leaves and component clocks disagree"):
        resolve_program_materialization_plan_v1(
            schedule, registry=registry, available_fields=["close"]
        )


def test_plan_self_hash_is_fail_closed() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    plan = resolve_program_materialization_plan_v1(
        _schedule(), registry=registry, available_fields=["close"]
    )
    tampered = deepcopy(plan)
    tampered["missing_required_field_ids"] = []

    with pytest.raises(ValueError, match="self-hash drift"):
        verify_program_materialization_plan_v1(tampered)
