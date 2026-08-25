"""Freeze Search Core V2 Production Wave 2 before any new financial read."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from our_system_phase2.services.program_search_core_policy_v2 import (
    SEARCH_CORE_V2_PRIMARY_POLICY_ID,
    validate_search_core_v2_mature_continuation_snapshot,
)
from our_system_phase2.services.program_search_state_jump_generator_v2 import SEMANTIC_STATE_JUMP_GENERATOR_V2
from our_system_phase2.services.unified_capability_registry import stable_hash

SCHEMA = "cn_search_core_v2_production_wave2_prefreeze_v1"
STATUS = "SEARCH_CORE_V2_PRODUCTION_WAVE2_PREFROZEN_BEFORE_FINANCIAL_READ"
POLICY_REVIEW = Path("runtime/run_plans/cn_search_core_v2_policy_review_20260825.json")
WAVE1_AUDIT = Path("runtime/run_plans/cn_search_core_v2_production_wave1_postrun_audit_20260825.json")
WAVE1_EVIDENCE_FREEZE = Path("runtime/run_plans/cn_search_core_v2_production_wave1_evidence_freeze_20260825.json")
SOURCE_STATE = Path("runtime/run_plans/cn_search_core_v2_production_wave1_final_optimizer_state_412d51c_20260825.json")
PAIR_CHALLENGER = Path("runtime/run_plans/cn_search_core_v2_pair_native_qualification_challenger_v1_20260825.json")
TEMPLATES = (
    "BASE_EVENT", "BASE_MARKET", "BASE_MARKET_EVENT", "BASE_TEMPORAL",
    "BASE_TEMPORAL_EVENT", "BASE_TEMPORAL_MARKET", "BASE_TEMPORAL_MARKET_EVENT",
)
ROUNDS = 2
BATCH_SIZE = 24
SOURCE_OBSERVATIONS = 1344
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
    policy_path = repo / POLICY_REVIEW
    audit_path = repo / WAVE1_AUDIT
    freeze_path = repo / WAVE1_EVIDENCE_FREEZE
    state_path = repo / SOURCE_STATE
    challenger_path = repo / PAIR_CHALLENGER
    policy, audit, freeze, state, challenger = map(
        _read, (policy_path, audit_path, freeze_path, state_path, challenger_path)
    )
    policy_hash = _verify(policy, "policy_review_payload_sha256", "Search Core V2 policy review")
    audit_hash = _verify(audit, "audit_payload_sha256", "Wave1 postrun audit")
    freeze_hash = _verify(freeze, "evidence_freeze_payload_sha256", "Wave1 evidence freeze")
    state_hash = _verify(state, "snapshot_hash", "Wave1 final mature state")
    challenger_hash = _verify(challenger, "challenger_payload_sha256", "Pair-native challenger")
    resolved = validate_search_core_v2_mature_continuation_snapshot(state)
    decision = dict(policy.get("project_control_decision") or {})
    if (
        policy.get("status") != "SEARCH_CORE_V2_POLICY_REVIEW_COMPLETE_MATURE_STATE_JUMP_PRIMARY"
        or policy.get("policy_id") != SEARCH_CORE_V2_PRIMARY_POLICY_ID
        or decision.get("search_core_policy_change_authorized") is not True
        or decision.get("automatic_financial_campaign_launch_authorized") is not False
        or resolved["primary_arm"] != SEMANTIC_STATE_JUMP_GENERATOR_V2
        or int(resolved["development_observations"]) != SOURCE_OBSERVATIONS
    ):
        raise RuntimeError("Production Wave 2 policy authority drift")
    lineage = dict(audit.get("mature_state_lineage") or {})
    recommendation = dict(audit.get("project_control_recommendation") or {})
    if (
        audit.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE1_POSTRUN_AUDIT_COMPLETE_ARCHIVE_READY"
        or str(lineage.get("final_snapshot_payload_sha256") or "") != state_hash
        or int(lineage.get("final_memory_observations") or 0) != SOURCE_OBSERVATIONS
        or int(lineage.get("final_history_count") or 0) != len(state["history"])
        or int(lineage.get("final_generated_exact_count") or 0) != len(state["generated_exact_identities"])
        or int(lineage.get("new_generated_vs_source_seen_overlap_count", -1)) != 0
        or recommendation.get("automatic_validation_authorized") is not False
        or recommendation.get("automatic_promotion_authorized") is not False
        or recommendation.get("capital_action_authorized") is not False
    ):
        raise RuntimeError("Production Wave 2 Wave1 audit authority drift")
    frozen_state = dict(freeze.get("artifacts", {}).get("final_optimizer_state") or {})
    if (
        freeze.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE1_EVIDENCE_FROZEN_DEVELOPMENT_ONLY"
        or int(freeze.get("final_mature_observations") or 0) != SOURCE_OBSERVATIONS
        or bool(freeze.get("validation_read"))
        or bool(freeze.get("holdout_read"))
        or bool(freeze.get("forward_read"))
        or freeze.get("oos_authority") != "NONE"
        or freeze.get("automatic_promotion_authorized") is not False
        or str(frozen_state.get("relative_path") or "") != str(SOURCE_STATE).replace("\\", "/")
        or str(frozen_state.get("file_sha256") or "") != _sha(state_path)
    ):
        raise RuntimeError("Production Wave 2 Wave1 freeze authority drift")
    rule = dict(challenger.get("rule_contract") or {})
    boundaries = dict(challenger.get("authority_boundaries") or {})
    if (
        challenger.get("status") != "PAIR_NATIVE_CONTROL_ROBUST_CHALLENGER_V1_FROZEN_DIAGNOSTIC_ONLY_UNSEEN_TEST_REQUIRED"
        or challenger.get("rule_id") != "PAIR_NATIVE_PRIMARY_3OF3_CONTROL_2OF3_POSITIVE_V1"
        or int(rule.get("development_window_count") or 0) != 3
        or int(rule.get("primary_positive_development_window_count_required") or 0) != 3
        or int(rule.get("control_positive_development_window_count_minimum") or 0) != 2
        or rule.get("threshold_tuning") is not False
        or rule.get("uses_learned_parameters") is not False
        or rule.get("uses_validation_features") is not False
        or rule.get("uses_validation_labels_at_application_time") is not False
        or boundaries.get("automatic_challenger_adoption_authorized") is not False
        or boundaries.get("current_search_policy_changed") is not False
        or boundaries.get("same_slice_reselection_allowed") is not False
    ):
        raise RuntimeError("Production Wave 2 pair-native annotation contract drift")
    implementation = {}
    for name, rel in {
        "runner": "scripts/run_cn_search_core_v2_production_wave2_v1.py",
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
        "campaign_purpose": "CONTINUE_MATURE_STATE_JUMP_DEVELOPMENT_SEARCH_WITH_PAIR_NATIVE_ROBUSTNESS_ANNOTATION_ONLY",
        "production_wave2": {
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
        "source_wave1_postrun_audit": {
            "relative_path": str(WAVE1_AUDIT).replace("\\", "/"),
            "file_sha256": _sha(audit_path),
            "payload_sha256": audit_hash,
        },
        "source_wave1_evidence_freeze": {
            "relative_path": str(WAVE1_EVIDENCE_FREEZE).replace("\\", "/"),
            "file_sha256": _sha(freeze_path),
            "payload_sha256": freeze_hash,
        },
        "mature_state": {
            "relative_path": str(SOURCE_STATE).replace("\\", "/"),
            "file_sha256": _sha(state_path),
            "payload_sha256": state_hash,
            "source_development_observations": SOURCE_OBSERVATIONS,
            "source_history_count": len(state["history"]),
            "source_generated_exact_count": len(state["generated_exact_identities"]),
            "source_dead_region_count": int(resolved["dead_region_count"]),
            "learned_dead_regions_allowed_during_continuation": True,
            "dead_regions_skipped_by_generator": True,
            "expected_final_development_observations": SOURCE_OBSERVATIONS + TOTAL_EVALUATIONS,
            "expected_final_history_count": len(state["history"]) + ROUNDS * len(TEMPLATES),
            "expected_final_generated_exact_count": len(state["generated_exact_identities"]) + TOTAL_EVALUATIONS,
        },
        "pair_native_annotation_contract": {
            "source_relative_path": str(PAIR_CHALLENGER).replace("\\", "/"),
            "source_file_sha256": _sha(challenger_path),
            "source_payload_sha256": challenger_hash,
            "rule_id": challenger["rule_id"],
            "rule_contract": rule,
            "application_scope": "DEVELOPMENT_RESULT_ANNOTATION_AND_ARCHIVE_ONLY",
            "optimizer_feedback_write": False,
            "generator_ask_order_changed": False,
            "candidate_admission_changed": False,
            "automatic_policy_adoption": False,
            "validation_label_read_at_application": False,
        },
        "implementation_sha256": implementation,
        "candidate_archive_contract": {
            "productive_archive": "PRODUCTIVE_CANDIDATES.jsonl",
            "stable_archive": "STABLE_CANDIDATES.jsonl",
            "pair_native_challenger_archive": "PAIR_NATIVE_CHALLENGER_CANDIDATES.jsonl",
            "pair_native_challenger_membership": "DEVELOPMENT_PRODUCTIVE_AND_FROZEN_PAIR_NATIVE_RULE_HIT",
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
    parser.add_argument("--output", type=Path, default=Path("runtime/run_plans/cn_search_core_v2_production_wave2_prefreeze_20260825.json"))
    args = parser.parse_args(argv)
    payload = build(args.repo_root, source_repo_sha=args.source_repo_sha)
    out = args.output if args.output.is_absolute() else args.repo_root / args.output
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "status": payload["status"],
        "total": payload["production_wave2"]["total_financial_evaluations"],
        "source_observations": payload["mature_state"]["source_development_observations"],
        "expected_final_observations": payload["mature_state"]["expected_final_development_observations"],
        "pair_rule": payload["pair_native_annotation_contract"]["rule_id"],
        "payload": payload["prefreeze_payload_sha256"],
        "output": str(out.resolve()),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
