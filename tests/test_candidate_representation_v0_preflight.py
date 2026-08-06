from __future__ import annotations

from pathlib import Path

from our_system_phase2.runtime.cn_candidate_representation_v0_preflight import (
    build_candidate_representation_v0_preflight,
    verify_candidate_representation_v0_preflight,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "runtime/field_registry/cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)
ROOT_CONTRACT = (
    REPO / "runtime/run_plans/cn_core_pack_development_discovery_v1.json"
)


def test_zero_financial_preflight_writes_and_verifies_immutable_artifacts(
    tmp_path: Path,
) -> None:
    root = tmp_path / "preflight"
    closure = build_candidate_representation_v0_preflight(
        registry_path=REGISTRY,
        root_contract_path=ROOT_CONTRACT,
        output_root=root,
        quota_per_template=1,
        seeds=(1729,),
    )
    verified = verify_candidate_representation_v0_preflight(root)

    assert closure["status"] == "PREPARED_ZERO_FINANCIAL"
    assert verified == closure
    assert closure["template_count"] == 8
    assert closure["scheduled_attempts"] == 8
    assert closure["candidate_spec_count"] == 16
    assert closure["compatible_candidate_row_count"] == 16
    assert closure["artifact_count"] == 6
    assert closure["financial_read_count"] == 0
    assert closure["validation_read_count"] == 0
    assert closure["holdout_read_count"] == 0
    assert closure["forward_2026_read_count"] == 0
    assert closure["shared_tpe_credit"] is False
    assert closure["dynamic_budget_reallocation_allowed"] is False
    assert closure["underfill_spillover_allowed"] is False
