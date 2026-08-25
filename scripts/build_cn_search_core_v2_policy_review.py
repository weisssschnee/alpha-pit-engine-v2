"""Build the Search Core V2 production search-policy review from frozen development evidence."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from our_system_phase2.services.program_search_core_policy_v2 import (
    MIN_MATURE_DEVELOPMENT_OBSERVATIONS,
    SEARCH_CORE_V2_PRIMARY_POLICY_ID,
    resolve_search_core_v2_primary_arm,
)
from our_system_phase2.services.program_search_primitive_credit_v1 import PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1
from our_system_phase2.services.program_search_state_jump_generator_v2 import SEMANTIC_STATE_JUMP_GENERATOR_V2
from our_system_phase2.services.unified_capability_registry import stable_hash

SCHEMA = "cn_search_core_v2_policy_review_v1"
STATUS = "SEARCH_CORE_V2_POLICY_REVIEW_COMPLETE_MATURE_STATE_JUMP_PRIMARY"


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload); claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def build(repo: Path, *, source_repo_sha: str) -> dict[str, Any]:
    repo = repo.resolve()
    root = repo / "runtime" / "run_plans"
    s1_path = root / "cn_search_core_v2_stage1_postrun_audit_20260824.json"
    s15_path = root / "cn_search_core_v2_stage15_postrun_audit_20260825.json"
    s2_path = root / "cn_search_core_v2_stage2_postrun_audit_20260825.json"
    state_path = root / "cn_search_core_v2_stage2_final_optimizer_state_888a276_20260825.json"
    s1, s15, s2, state = map(_read, (s1_path, s15_path, s2_path, state_path))
    s1_hash = _verify(s1, "audit_payload_sha256", "Stage-1 audit")
    s15_hash = _verify(s15, "audit_payload_sha256", "Stage-1.5 audit")
    s2_hash = _verify(s2, "audit_payload_sha256", "Stage-2 audit")
    state_hash = _verify(state, "snapshot_hash", "Stage-2 mature optimizer")

    r0a = int(s1["stage1_metrics"]["round0"]["arm_a"]["productive"])
    r0b = int(s1["stage1_metrics"]["round0"]["arm_b"]["productive"])
    r1a = int(s1["stage1_metrics"]["round1"]["arm_a"]["productive"])
    r1b = int(s1["stage1_metrics"]["round1"]["arm_b"]["productive"])
    if not (r0b < r0a and r1b > r1a):
        raise RuntimeError("cold-to-mature crossover evidence drift")
    if s15.get("status") != "SEARCH_CORE_V2_STAGE15_POSTRUN_AUDIT_COMPLETE_STAGE2_REVIEW_ELIGIBLE":
        raise RuntimeError("Stage-1.5 policy evidence not PASS")
    if s2.get("status") != "SEARCH_CORE_V2_STAGE2_POSTRUN_AUDIT_COMPLETE_POLICY_REVIEW_ELIGIBLE":
        raise RuntimeError("Stage-2 policy evidence not PASS")
    d15 = dict(s15["decision_replay"]); d2 = dict(s2["decision_replay"])
    if (
        float(d15["productive_ratio_b_vs_a"]) < 1.05
        or int(d15["productive_template_win_count_b"]) < 4
        or float(d2["productive_ratio_b_vs_a"]) < 1.05
        or float(d2["stable_ratio_b_vs_a"]) < 1.0
        or float(d2["behavior_ratio_b_vs_a"]) < 0.95
        or int(d2["productive_template_win_count_b"]) != 7
        or float(d2["maximum_positive_productive_gain_template_fraction"]) > 0.40
    ):
        raise RuntimeError("mature Generator policy gate not satisfied")
    current = resolve_search_core_v2_primary_arm(state)
    if (
        current["primary_arm"] != SEMANTIC_STATE_JUMP_GENERATOR_V2
        or int(current["development_observations"]) != 1008
        or int(current["minimum_mature_development_observations"])
        != MIN_MATURE_DEVELOPMENT_OBSERVATIONS
    ):
        raise RuntimeError("final mature state policy resolution drift")

    payload = {
        "schema_version": SCHEMA,
        "status": STATUS,
        "source_repo_sha": str(source_repo_sha),
        "policy_id": SEARCH_CORE_V2_PRIMARY_POLICY_ID,
        "policy": {
            "cold_start_primary_arm": PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1,
            "mature_primary_arm": SEMANTIC_STATE_JUMP_GENERATOR_V2,
            "mature_switch_threshold_development_observations": MIN_MATURE_DEVELOPMENT_OBSERVATIONS,
            "current_primary_arm": current["primary_arm"],
            "current_mature_observations": current["development_observations"],
            "primitive_role": "BOOTSTRAP_AND_CONTROL",
            "mcts_role": "CHALLENGER_DIAGNOSTIC_ONLY",
            "generator_restore_failure": "FAIL_CLOSED_NO_SILENT_PRIMITIVE_FALLBACK",
            "mature_snapshot_reuse_scope": "DEVELOPMENT_SEARCH_ONLY_HASH_BOUND",
        },
        "evidence": {
            "stage1": {"relative_path": str(s1_path.relative_to(repo)).replace("\\", "/"), "file_sha256": _sha(s1_path), "payload_sha256": s1_hash, "round0_productive": {"primitive": r0a, "generator": r0b}, "round1_productive": {"primitive": r1a, "generator": r1b}},
            "stage15": {"relative_path": str(s15_path.relative_to(repo)).replace("\\", "/"), "file_sha256": _sha(s15_path), "payload_sha256": s15_hash, "productive_ratio_b_vs_a": d15["productive_ratio_b_vs_a"], "template_wins_b": d15["productive_template_win_count_b"]},
            "stage2": {"relative_path": str(s2_path.relative_to(repo)).replace("\\", "/"), "file_sha256": _sha(s2_path), "payload_sha256": s2_hash, "productive_ratio_b_vs_a": d2["productive_ratio_b_vs_a"], "stable_ratio_b_vs_a": d2["stable_ratio_b_vs_a"], "behavior_ratio_b_vs_a": d2["behavior_ratio_b_vs_a"], "template_wins_b": d2["productive_template_win_count_b"], "max_positive_gain_template_fraction": d2["maximum_positive_productive_gain_template_fraction"]},
            "mature_state": {"relative_path": str(state_path.relative_to(repo)).replace("\\", "/"), "file_sha256": _sha(state_path), "payload_sha256": state_hash, "development_observations": current["development_observations"], "history_count": current["history_count"]},
        },
        "project_control_decision": {
            "verdict": "ADOPT_MATURE_STATE_JUMP_PRIMARY_FOR_FUTURE_DEVELOPMENT_SEARCH",
            "search_core_policy_change_authorized": True,
            "automatic_financial_campaign_launch_authorized": False,
            "active_mature_snapshot_relative_path": str(state_path.relative_to(repo)).replace("\\", "/"),
            "active_mature_snapshot_payload_sha256": state_hash,
        },
        "authority_boundaries": {
            "evaluation_data_role": "DEVELOPMENT_ONLY",
            "validation_feedback_allowed": False,
            "holdout_feedback_allowed": False,
            "forward_feedback_allowed": False,
            "oos_authority": "NONE",
            "alpha_promotion_authorized": False,
            "capital_action_authorized": False,
        },
        "next_action": "USE_MATURE_STATE_JUMP_GENERATOR_V2_AS_PRIMARY_FOR_FUTURE_DEVELOPMENT_SEARCH;_RETAIN_PRIMITIVE_AS_BOOTSTRAP_CONTROL",
        "financial_evaluation_executed": False,
    }
    payload["policy_review_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--source-repo-sha", required=True)
    parser.add_argument("--output", type=Path, default=Path("runtime/run_plans/cn_search_core_v2_policy_review_20260825.json"))
    args=parser.parse_args(argv)
    payload=build(args.repo_root, source_repo_sha=args.source_repo_sha)
    out=args.output if args.output.is_absolute() else args.repo_root/args.output
    out.parent.mkdir(parents=True,exist_ok=True)
    out.write_text(json.dumps(payload,ensure_ascii=False,sort_keys=True,indent=2)+"\n",encoding="utf-8")
    print(json.dumps({"status":payload["status"],"policy_id":payload["policy_id"],"current_primary_arm":payload["policy"]["current_primary_arm"],"switch_threshold":payload["policy"]["mature_switch_threshold_development_observations"],"payload":payload["policy_review_payload_sha256"],"output":str(out.resolve())},sort_keys=True))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
