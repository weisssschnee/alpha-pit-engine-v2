"""Freeze Search Core V2 Production Wave 1 before any new financial read."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from our_system_phase2.services.program_search_core_policy_v2 import (
    SEARCH_CORE_V2_PRIMARY_POLICY_ID,
    resolve_search_core_v2_primary_arm,
)
from our_system_phase2.services.program_search_state_jump_generator_v2 import (
    SEMANTIC_STATE_JUMP_GENERATOR_V2,
)
from our_system_phase2.services.unified_capability_registry import stable_hash

SCHEMA = "cn_search_core_v2_production_wave1_prefreeze_v1"
STATUS = "SEARCH_CORE_V2_PRODUCTION_WAVE1_PREFROZEN_BEFORE_FINANCIAL_READ"
POLICY_REVIEW = Path("runtime/run_plans/cn_search_core_v2_policy_review_20260825.json")
STAGE2_AUDIT = Path("runtime/run_plans/cn_search_core_v2_stage2_postrun_audit_20260825.json")
SOURCE_STATE = Path("runtime/run_plans/cn_search_core_v2_stage2_final_optimizer_state_888a276_20260825.json")
TEMPLATES = (
    "BASE_EVENT", "BASE_MARKET", "BASE_MARKET_EVENT", "BASE_TEMPORAL",
    "BASE_TEMPORAL_EVENT", "BASE_TEMPORAL_MARKET", "BASE_TEMPORAL_MARKET_EVENT",
)
ROUNDS = 2
BATCH_SIZE = 24
TOTAL_EVALUATIONS = ROUNDS * len(TEMPLATES) * BATCH_SIZE


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def build(repo: Path, *, source_repo_sha: str) -> dict[str, Any]:
    repo = repo.resolve()
    policy_path, audit_path, state_path = repo / POLICY_REVIEW, repo / STAGE2_AUDIT, repo / SOURCE_STATE
    policy, audit, state = _read(policy_path), _read(audit_path), _read(state_path)
    policy_hash = _verify(policy, "policy_review_payload_sha256", "Search Core V2 policy review")
    audit_hash = _verify(audit, "audit_payload_sha256", "Stage-2 postrun audit")
    state_hash = _verify(state, "snapshot_hash", "Stage-2 final mature state")
    resolved = resolve_search_core_v2_primary_arm(state)
    decision = dict(policy.get("project_control_decision") or {})
    if (
        policy.get("status") != "SEARCH_CORE_V2_POLICY_REVIEW_COMPLETE_MATURE_STATE_JUMP_PRIMARY"
        or policy.get("policy_id") != SEARCH_CORE_V2_PRIMARY_POLICY_ID
        or decision.get("search_core_policy_change_authorized") is not True
        or decision.get("automatic_financial_campaign_launch_authorized") is not False
        or str(decision.get("active_mature_snapshot_payload_sha256") or "") != state_hash
        or resolved["primary_arm"] != SEMANTIC_STATE_JUMP_GENERATOR_V2
        or int(resolved["development_observations"]) != 1008
    ):
        raise RuntimeError("Production Wave 1 policy authority drift")
    if (
        audit.get("status") != "SEARCH_CORE_V2_STAGE2_POSTRUN_AUDIT_COMPLETE_POLICY_REVIEW_ELIGIBLE"
        or str(audit["mature_state_lineage"]["final_snapshot_payload_sha256"]) != state_hash
        or int(audit["mature_state_lineage"]["final_memory_observations"]) != 1008
        or int(audit["mature_state_lineage"]["new_generator_vs_arm_a_overlap_count"]) != 0
    ):
        raise RuntimeError("Production Wave 1 Stage-2 evidence drift")
    implementation = {}
    for name, rel in {
        "runner": "scripts/run_cn_search_core_v2_production_wave1_v1.py",
        "state_jump_adapter": "src/our_system_phase2/services/program_search_state_jump_adapter_v2.py",
        "state_jump_generator": "src/our_system_phase2/services/program_search_state_jump_generator_v2.py",
        "primary_policy": "src/our_system_phase2/services/program_search_core_policy_v2.py",
    }.items():
        path = repo / rel
        if not path.is_file():
            raise FileNotFoundError(path)
        implementation[name] = {"relative_path": rel, "file_sha256": _sha(path)}
    payload = {
        "schema_version": SCHEMA,
        "status": STATUS,
        "source_repo_sha": str(source_repo_sha),
        "campaign_purpose": "USE_ADOPTED_MATURE_STATE_JUMP_POLICY_FOR_NEW_DEVELOPMENT_ONLY_ALPHA_DISCOVERY",
        "production_wave1": {
            "arm": SEMANTIC_STATE_JUMP_GENERATOR_V2,
            "rounds": ROUNDS,
            "templates": list(TEMPLATES),
            "batch_size": BATCH_SIZE,
            "per_template_budget": ROUNDS * BATCH_SIZE,
            "total_financial_evaluations": TOTAL_EVALUATIONS,
            "total_checkpoint_count": ROUNDS * len(TEMPLATES),
            "checkpoint_size": BATCH_SIZE,
            "automatic_followon": False,
        },
        "source_policy_review": {
            "relative_path": str(POLICY_REVIEW).replace("\\", "/"),
            "file_sha256": _sha(policy_path),
            "payload_sha256": policy_hash,
            "policy_id": SEARCH_CORE_V2_PRIMARY_POLICY_ID,
            "current_primary_arm": SEMANTIC_STATE_JUMP_GENERATOR_V2,
        },
        "source_stage2_postrun_audit": {
            "relative_path": str(STAGE2_AUDIT).replace("\\", "/"),
            "file_sha256": _sha(audit_path),
            "payload_sha256": audit_hash,
        },
        "mature_state": {
            "relative_path": str(SOURCE_STATE).replace("\\", "/"),
            "file_sha256": _sha(state_path),
            "payload_sha256": state_hash,
            "source_development_observations": 1008,
            "source_history_count": len(state["history"]),
            "source_generated_exact_count": len(state["generated_exact_identities"]),
            "expected_final_development_observations": 1008 + TOTAL_EVALUATIONS,
            "expected_final_history_count": len(state["history"]) + ROUNDS * len(TEMPLATES),
            "expected_final_generated_exact_count": len(state["generated_exact_identities"]) + TOTAL_EVALUATIONS,
        },
        "implementation_sha256": implementation,
        "candidate_archive_contract": {
            "productive_archive": "PRODUCTIVE_CANDIDATES.jsonl",
            "stable_archive": "STABLE_CANDIDATES.jsonl",
            "archive_scope": "THIS_WAVE_NEW_EXACTS_DEVELOPMENT_EVIDENCE_ONLY",
            "automatic_validation": False,
        },
        "evaluation_data_role": "DEVELOPMENT_ONLY",
        "restricted_reads": {"validation": 0, "holdout": 0, "historical_2023": 0, "forward_b": 0, "forward_2026": 0},
        "validation_feedback_used": False,
        "promotion_authorized": False,
        "oos_authority": "NONE",
        "capital_action_authorized": False,
        "automatic_followon_authorized": False,
        "financial_labels_read_by_builder": False,
        "candidate_evaluation_executed": False,
    }
    payload["prefreeze_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--source-repo-sha", required=True)
    parser.add_argument("--output", type=Path, default=Path("runtime/run_plans/cn_search_core_v2_production_wave1_prefreeze_20260825.json"))
    args = parser.parse_args(argv)
    payload = build(args.repo_root, source_repo_sha=args.source_repo_sha)
    out = args.output if args.output.is_absolute() else args.repo_root / args.output
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": payload["status"], "total": payload["production_wave1"]["total_financial_evaluations"], "source_observations": payload["mature_state"]["source_development_observations"], "expected_final_observations": payload["mature_state"]["expected_final_development_observations"], "payload": payload["prefreeze_payload_sha256"], "output": str(out.resolve())}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
