from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts.freeze_cn_search_core_v2_production_wave1_evidence import freeze
from our_system_phase2.services.unified_capability_registry import stable_hash


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    repo = tmp_path / "repo"
    (repo / "runtime" / "run_plans").mkdir(parents=True)
    root = tmp_path / "wave1"
    root.mkdir()

    terminal = {"status": "SEARCH_CORE_V2_PRODUCTION_WAVE1_COMPLETE", "x": 1}
    _write_json(root / "CN_SEARCH_CORE_V2_PRODUCTION_WAVE1_COMPLETE.json", terminal)

    final_state = {"schema_version": "cn_program_state_jump_optimizer_adapter_v2", "history": []}
    final_state["snapshot_hash"] = stable_hash(final_state)
    _write_json(root / "FINAL_MATURE_OPTIMIZER_STATE.json", final_state)

    (root / "PRODUCTIVE_CANDIDATES.jsonl").write_text('{"exact":"p"}\n', encoding="utf-8")
    (root / "STABLE_CANDIDATES.jsonl").write_text('{"exact":"s"}\n', encoding="utf-8")

    audit = {
        "schema_version": "cn_search_core_v2_production_wave1_postrun_audit_v1",
        "status": "SEARCH_CORE_V2_PRODUCTION_WAVE1_POSTRUN_AUDIT_COMPLETE_ARCHIVE_READY",
        "terminal": {"file_sha256": _sha(root / "CN_SEARCH_CORE_V2_PRODUCTION_WAVE1_COMPLETE.json")},
        "mature_state_lineage": {
            "final_snapshot_payload_sha256": final_state["snapshot_hash"],
            "final_memory_observations": 1344,
        },
        "candidate_archives": {
            "productive_count": 1,
            "stable_count": 1,
            "productive_file_sha256": _sha(root / "PRODUCTIVE_CANDIDATES.jsonl"),
            "stable_file_sha256": _sha(root / "STABLE_CANDIDATES.jsonl"),
        },
        "project_control_recommendation": {
            "automatic_validation_authorized": False,
            "automatic_promotion_authorized": False,
        },
    }
    audit["audit_payload_sha256"] = stable_hash(audit)
    audit_path = tmp_path / "audit.json"
    _write_json(audit_path, audit)
    return repo, root, audit_path


def test_freeze_copies_exact_bytes_and_preserves_development_only_boundary(tmp_path: Path) -> None:
    repo, root, audit_path = _fixture(tmp_path)
    manifest = freeze(
        repo=repo,
        output_root=root,
        postrun_audit=audit_path,
        source_run_id="wave1-test",
    )
    assert manifest["status"] == "SEARCH_CORE_V2_PRODUCTION_WAVE1_EVIDENCE_FROZEN_DEVELOPMENT_ONLY"
    assert manifest["productive_candidate_count"] == 1
    assert manifest["stable_candidate_count"] == 1
    assert manifest["final_mature_observations"] == 1344
    assert manifest["classification"] == "DEVELOPMENT_ONLY_NOT_ALPHA_QUALIFIED"
    assert manifest["validation_read"] is False
    assert manifest["holdout_read"] is False
    assert manifest["forward_read"] is False
    assert manifest["oos_authority"] == "NONE"
    assert manifest["automatic_promotion_authorized"] is False
    assert manifest["financial_evaluation_executed_by_freeze"] is False
    for key, source_name in {
        "terminal": "CN_SEARCH_CORE_V2_PRODUCTION_WAVE1_COMPLETE.json",
        "final_optimizer_state": "FINAL_MATURE_OPTIMIZER_STATE.json",
        "productive_candidates": "PRODUCTIVE_CANDIDATES.jsonl",
        "stable_candidates": "STABLE_CANDIDATES.jsonl",
    }.items():
        frozen = repo / manifest["artifacts"][key]["relative_path"]
        assert frozen.read_bytes() == (root / source_name).read_bytes()
        assert _sha(frozen) == manifest["artifacts"][key]["file_sha256"]


def test_freeze_fails_closed_if_destination_already_exists(tmp_path: Path) -> None:
    repo, root, audit_path = _fixture(tmp_path)
    freeze(repo=repo, output_root=root, postrun_audit=audit_path, source_run_id="wave1-test")
    with pytest.raises(FileExistsError):
        freeze(repo=repo, output_root=root, postrun_audit=audit_path, source_run_id="wave1-test")