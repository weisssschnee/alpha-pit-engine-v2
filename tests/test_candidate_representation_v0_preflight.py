from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from our_system_phase2.runtime.cn_candidate_representation_v0_preflight import (
    CLOSURE_NAME,
    build_candidate_representation_v0_preflight,
    verify_candidate_representation_v0_preflight,
)
from our_system_phase2.services.unified_capability_registry import stable_hash

import app


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "runtime/field_registry/cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)
ROOT_CONTRACT = (
    REPO / "runtime/run_plans/cn_core_pack_development_discovery_v1.json"
)


def _rehash_artifact_and_closure(root: Path, artifact_name: str) -> None:
    artifact_path = root / artifact_name
    closure_path = root / CLOSURE_NAME
    closure = json.loads(closure_path.read_text(encoding="utf-8"))
    artifact = next(
        row for row in closure["artifacts"] if row["path"] == artifact_name
    )
    artifact["sha256"] = hashlib.sha256(artifact_path.read_bytes()).hexdigest()
    artifact["bytes"] = artifact_path.stat().st_size
    closure["closure_payload_sha256"] = stable_hash(
        {
            key: value
            for key, value in closure.items()
            if key != "closure_payload_sha256"
        }
    )
    closure_path.write_text(
        json.dumps(closure, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
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


def test_candidate_representation_preflight_is_registered_as_an_app_route() -> None:
    assert app.ROUTES["cn-candidate-representation-v0-preflight"] == (
        "our_system_phase2.runtime.cn_candidate_representation_v0_preflight"
    )


def test_preflight_verifier_rejects_adaptation_hidden_in_candidate_rows(
    tmp_path: Path,
) -> None:
    root = tmp_path / "preflight"
    build_candidate_representation_v0_preflight(
        registry_path=REGISTRY,
        root_contract_path=ROOT_CONTRACT,
        output_root=root,
        quota_per_template=1,
        seeds=(1729,),
    )
    rows_path = root / "compatible_candidate_rows_v0.jsonl"
    rows = [
        json.loads(line)
        for line in rows_path.read_text(encoding="utf-8").splitlines()
        if line
    ]
    rows[0]["optimizer_feedback_eligible"] = True
    rows_path.write_text(
        "".join(
            json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
            for row in rows
        ),
        encoding="utf-8",
        newline="\n",
    )
    _rehash_artifact_and_closure(root, rows_path.name)

    with pytest.raises(RuntimeError, match="row authority drift"):
        verify_candidate_representation_v0_preflight(root)


def test_preflight_verifier_rejects_prohibited_reads_hidden_in_summary(
    tmp_path: Path,
) -> None:
    root = tmp_path / "preflight"
    build_candidate_representation_v0_preflight(
        registry_path=REGISTRY,
        root_contract_path=ROOT_CONTRACT,
        output_root=root,
        quota_per_template=1,
        seeds=(1729,),
    )
    summary_path = root / "summary_v0.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["validation_read_count"] = 1
    summary_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    _rehash_artifact_and_closure(root, summary_path.name)

    with pytest.raises(RuntimeError, match="summary recorded prohibited reads"):
        verify_candidate_representation_v0_preflight(root)
