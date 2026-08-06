from __future__ import annotations

from pathlib import Path

from our_system_phase2.runtime.cn_typed_candidate_program_v1_smoke import (
    AUDIT_STATUS,
    SMOKE_STATUS,
    build_program_v1_smoke,
    verify_program_v1_smoke,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "runtime/field_registry/cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)
ROOT_CONTRACT = REPO / "runtime/run_plans/cn_core_pack_development_discovery_v1.json"


def test_zero_financial_smoke_and_independent_verifier_close_all_artifacts(tmp_path) -> None:
    root = tmp_path / "smoke"
    closure = build_program_v1_smoke(
        output_root=root,
        registry_path=REGISTRY,
        root_contract_path=ROOT_CONTRACT,
        repo_sha="test-sha",
    )
    assert closure["status"] == SMOKE_STATUS
    assert closure["compile_ready_count"] == 5
    assert closure["blocked_fixture_count"] == 1
    assert closure["market_label_validation_holdout_historical_forward_reads"] == 0
    audit = verify_program_v1_smoke(
        smoke_root=root,
        registry_path=REGISTRY,
        root_contract_path=ROOT_CONTRACT,
        audit_output=tmp_path / "audit.json",
    )
    assert audit["status"] == AUDIT_STATUS
    assert audit["all_declared_artifacts_verified"] is True
    assert audit["zero_financial_and_sealed_reads_verified"] is True
