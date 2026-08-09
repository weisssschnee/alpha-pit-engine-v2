from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pytest

from our_system_phase2.runtime.cn_joint_program_phase_c_v0 import (
    ENHANCED_TEMPLATE_ORDER,
    EXPECTED_RECORDS,
    TEMPLATE_ORDER,
    _artifact_binding_path,
    _artifact_binding_size,
    build_phase_c_ask_plan_v0,
    build_phase_c_materialization_schedule_v0,
    phase_c_generation_arm_v0,
)
from our_system_phase2.services.candidate_program_materialization_v1 import (
    resolve_program_materialization_plan_v1,
)
from our_system_phase2.services.candidate_program_proposal_v0 import (
    ProgramSourceComponentV0,
)
from our_system_phase2.services.compositional_grammar import CompositionalGrammarV2
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "runtime/field_registry/cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)
ROOT_CONTRACT = REPO / "runtime/run_plans/cn_core_pack_development_discovery_v1.json"


def test_artifact_binding_accepts_both_phase_b_and_phase_c_schemas() -> None:
    assert _artifact_binding_path({"path": "ARTIFACT_MANIFEST.json"}) == (
        "ARTIFACT_MANIFEST.json"
    )
    assert _artifact_binding_size({"bytes": 17}) == 17
    assert _artifact_binding_path({"relative_path": "summary.json"}) == (
        "summary.json"
    )
    assert _artifact_binding_size({"size_bytes": 23}) == 23


def test_phase_c_ask_plan_freezes_exact_512_and_arm_quotas() -> None:
    rows = build_phase_c_ask_plan_v0()
    assert len(rows) == EXPECTED_RECORDS == 512
    assert [row["main_record_ordinal"] for row in rows] == list(range(512))
    assert Counter(row["template_id"] for row in rows) == Counter(
        {template_id: 64 for template_id in TEMPLATE_ORDER}
    )
    for template_id in ENHANCED_TEMPLATE_ORDER:
        template_rows = [row for row in rows if row["template_id"] == template_id]
        assert Counter(row["generation_arm"] for row in template_rows) == Counter(
            {
                "UNIFORM_FRESH": 28,
                "FACTORIZED_EXPLOIT": 24,
                "NOVELTY_RESERVE": 12,
            }
        )
        assert all(
            row["generation_arm"] == "UNIFORM_FRESH"
            for row in template_rows[:16]
        )
        assert all(not row["early_template_cancellation_allowed"] for row in template_rows)
    assert all(
        row["generation_arm"] == "UNIFORM_FRESH"
        for row in rows
        if row["template_id"] == "BASE"
    )


def test_phase_c_generation_arm_rejects_out_of_contract_ordinals() -> None:
    assert phase_c_generation_arm_v0("BASE_TEMPORAL", 16) == "FACTORIZED_EXPLOIT"
    assert phase_c_generation_arm_v0("BASE_TEMPORAL", 40) == "UNIFORM_FRESH"
    assert phase_c_generation_arm_v0("BASE_TEMPORAL", 52) == "NOVELTY_RESERVE"
    with pytest.raises(ValueError):
        phase_c_generation_arm_v0("BASE_TEMPORAL", 64)
    with pytest.raises(ValueError):
        phase_c_generation_arm_v0("UNKNOWN", 0)


def test_phase_c_materialization_schedule_binds_pair_identity_for_resolver() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    allowlists = json.loads(ROOT_CONTRACT.read_text(encoding="utf-8"))[
        "route_root_allowlists"
    ]
    grammar = CompositionalGrammarV2(registry, route_root_allowlist=allowlists)
    routes = {
        "base": "SLOW_CROSS_SECTIONAL_LEVEL",
        "temporal": "SLOW_TEMPORAL_CHANGE",
        "market": "MARKET_REGIME_CONDITION",
        "event": "DISCLOSURE_EVENT",
    }
    pools = {}
    for role, route_id in routes.items():
        pair = grammar.propose(route_id, attempt_index=0, seed=1729)
        pools[role] = (
            ProgramSourceComponentV0(
                role=role,
                primary=dict(pair.primary),
                control=dict(pair.control),
                proposal_id=f"proposal-{role}",
                trial_number=0,
                sampling_phase="STARTUP_RANDOM",
            ),
        )

    rows = build_phase_c_materialization_schedule_v0(
        registry=registry,
        pools=pools,
    )
    assert len(rows) == 256
    assert all(row["pair_id"] for row in rows)
    assert all(
        stable_hash(
            {
                key: value
                for key, value in row.items()
                if key != "schedule_record_sha256"
            }
        )
        == row["schedule_record_sha256"]
        for row in rows
    )
    available_fields = {
        field_id
        for row in rows
        for member in ("primary_compiled", "control_compiled")
        for field_id in row[member]["physical_leaf_ids"]
    }
    plan = resolve_program_materialization_plan_v1(
        rows,
        registry=registry,
        available_fields=available_fields,
    )
    assert plan["schedule_record_count"] == 256
    assert plan["template_counts"] == {
        template_id: 32 for template_id in TEMPLATE_ORDER
    }
