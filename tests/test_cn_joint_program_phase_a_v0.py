from __future__ import annotations

from pathlib import Path

from our_system_phase2.runtime.cn_joint_program_phase_a_v0 import (
    PHASE_A_AUDIT_STATUS,
    PHASE_A_STATUS,
    build_phase_a_v0,
    verify_phase_a_v0,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "runtime/field_registry/cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)
ROOT_CONTRACT = REPO / "runtime/run_plans/cn_core_pack_development_discovery_v1.json"


def test_phase_a_build_and_independent_replay_are_zero_financial(tmp_path: Path) -> None:
    root = tmp_path / "phase_a"
    closure = build_phase_a_v0(
        output_root=root,
        registry_path=REGISTRY,
        root_contract_path=ROOT_CONTRACT,
        repo_sha="test-sha",
    )
    assert closure["status"] == PHASE_A_STATUS
    audit = verify_phase_a_v0(
        phase_a_root=root,
        registry_path=REGISTRY,
        root_contract_path=ROOT_CONTRACT,
        audit_output=tmp_path / "audit.json",
    )
    assert audit["status"] == PHASE_A_AUDIT_STATUS
    assert audit["template_count_verified"] == 8
    assert audit["enhanced_control_count_verified"] == 7
    assert audit["zero_financial_and_sealed_reads"] is True
