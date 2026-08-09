from __future__ import annotations

from collections import Counter

import pytest

from our_system_phase2.runtime.cn_joint_program_phase_c_v0 import (
    ENHANCED_TEMPLATE_ORDER,
    EXPECTED_RECORDS,
    TEMPLATE_ORDER,
    _artifact_binding_path,
    _artifact_binding_size,
    build_phase_c_ask_plan_v0,
    phase_c_generation_arm_v0,
)


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
