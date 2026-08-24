"""Freeze Search Core V2 Stage-2 scale confirmation before any new financial read."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from our_system_phase2.services.unified_capability_registry import stable_hash

SCHEMA = "cn_search_core_v2_stage2_prefreeze_v1"
STATUS = "SEARCH_CORE_V2_STAGE2_PREFROZEN_BEFORE_FINANCIAL_READ"
TEMPLATES = (
    "BASE_EVENT", "BASE_MARKET", "BASE_MARKET_EVENT", "BASE_TEMPORAL",
    "BASE_TEMPORAL_EVENT", "BASE_TEMPORAL_MARKET", "BASE_TEMPORAL_MARKET_EVENT",
)
PER_TEMPLATE_PER_ARM = 72
TOTAL_PER_ARM = 504
TOTAL_EVALUATIONS = 1008
PRIOR_USED_PER_TEMPLATE = 72
CUMULATIVE_PRIOR_PER_ARM = 504
TARGET_TOTAL_PER_ARM = 1008

STAGE1_PREFREEZE = Path("runtime/run_plans/cn_search_core_v2_stage1_prefreeze_20260824.json")
STAGE15_PREFREEZE = Path("runtime/run_plans/cn_search_core_v2_stage15_prefreeze_20260824.json")
STAGE15_POSTRUN_AUDIT = Path("runtime/run_plans/cn_search_core_v2_stage15_postrun_audit_20260825.json")
STAGE15_TERMINAL = Path("runtime/run_plans/cn_search_core_v2_stage15_complete_a6ba961_20260825.json")
STAGE15_FINAL_STATE = Path("runtime/run_plans/cn_search_core_v2_stage15_final_optimizer_state_a6ba961_20260825.json")


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _verify_self(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def build(repo: Path, *, source_repo_sha: str) -> dict[str, Any]:
    repo = repo.resolve()
    p1 = repo / STAGE1_PREFREEZE
    p15 = repo / STAGE15_PREFREEZE
    pa = repo / STAGE15_POSTRUN_AUDIT
    pt = repo / STAGE15_TERMINAL
    ps = repo / STAGE15_FINAL_STATE
    for path in (p1, p15, pa, pt, ps):
        if not path.is_file():
            raise FileNotFoundError(path)

    stage1 = _read(p1)
    stage1_hash = _verify_self(stage1, "prefreeze_payload_sha256", "Stage-1 prefreeze")
    stage15 = _read(p15)
    stage15_hash = _verify_self(stage15, "prefreeze_payload_sha256", "Stage-1.5 prefreeze")
    audit = _read(pa)
    audit_hash = _verify_self(audit, "audit_payload_sha256", "Stage-1.5 postrun audit")
    terminal = _read(pt)
    terminal_hash = _verify_self(terminal, "closure_payload_sha256", "Stage-1.5 terminal")
    snapshot = _read(ps)
    snapshot_hash = _verify_self(snapshot, "snapshot_hash", "Stage-1.5 final optimizer state")

    if stage1.get("status") != "SEARCH_CORE_V2_STAGE1_PREFROZEN_BEFORE_FINANCIAL_READ":
        raise RuntimeError("Stage-1 prefreeze status drift")
    if stage15.get("status") != "SEARCH_CORE_V2_STAGE15_PREFROZEN_BEFORE_FINANCIAL_READ":
        raise RuntimeError("Stage-1.5 prefreeze status drift")
    if audit.get("status") != "SEARCH_CORE_V2_STAGE15_POSTRUN_AUDIT_COMPLETE_STAGE2_REVIEW_ELIGIBLE":
        raise RuntimeError("Stage-1.5 postrun audit not Stage-2 review eligible")
    if int(audit["project_control_recommendation"]["remaining_stage2_budget_per_arm"]) != TOTAL_PER_ARM:
        raise RuntimeError("Stage-2 remaining budget drift")
    if str(audit["project_control_recommendation"]["primitive_next_slice"]) != "FROZEN_STAGE1_ORDER_INDEX_72_TO_143_PER_TEMPLATE":
        raise RuntimeError("Stage-2 Primitive slice recommendation drift")
    if (
        terminal.get("status") != "SEARCH_CORE_V2_STAGE15_COMPLETE"
        or int(terminal.get("evaluated") or 0) != 336
        or int(terminal.get("evaluated_per_arm") or 0) != 168
        or str(dict(terminal.get("decision") or {}).get("status") or "")
        != "MATURE_GENERATOR_STAGE15_CONFIRMATION_PASS_STAGE2_REVIEW_ELIGIBLE"
        or terminal.get("automatic_stage2_authorized") is not False
        or any(int(v) != 0 for v in dict(terminal.get("restricted_reads") or {}).values())
    ):
        raise RuntimeError("Stage-1.5 terminal drift")
    if snapshot.get("schema_version") != "cn_program_state_jump_optimizer_adapter_v2":
        raise RuntimeError("Stage-1.5 optimizer snapshot schema drift")
    if len(list(snapshot.get("history") or ())) != 21 or len(list(snapshot.get("generated_exact_identities") or ())) != 504:
        raise RuntimeError("Stage-1.5 optimizer snapshot cardinality drift")
    last = dict(list(snapshot["history"])[-1])
    diag = dict(last.get("generator_diagnostics") or {})
    if int(diag.get("memory_observations") or 0) != 504:
        raise RuntimeError("Stage-1.5 mature memory observation drift")

    orders = {
        str(key): list(map(str, value))
        for key, value in dict(stage1["arm_a_primitive"]["eligible_orders_by_template"]).items()
    }
    if set(orders) != set(TEMPLATES) or any(len(value) != 224 for value in orders.values()):
        raise RuntimeError("Stage-1 frozen Primitive order geometry drift")
    selected_by_template: dict[str, list[str]] = {}
    for template in TEMPLATES:
        selected = orders[template][PRIOR_USED_PER_TEMPLATE:PRIOR_USED_PER_TEMPLATE + PER_TEMPLATE_PER_ARM]
        if len(selected) != PER_TEMPLATE_PER_ARM:
            raise RuntimeError(f"Stage-2 Primitive underfill: {template}")
        selected_by_template[template] = selected
    selected_exacts = [exact for template in TEMPLATES for exact in selected_by_template[template]]
    if len(selected_exacts) != TOTAL_PER_ARM or len(set(selected_exacts)) != TOTAL_PER_ARM:
        raise RuntimeError("Stage-2 Primitive exact coverage drift")
    prior_stage15 = set(map(str, stage15["arm_a_primitive"]["selected_exact_identities"]))
    prior_stage1 = {
        exact
        for template in TEMPLATES
        for exact in orders[template][:48]
    }
    if set(selected_exacts) & (prior_stage1 | prior_stage15):
        raise RuntimeError("Stage-2 Primitive slice reuses prior financial exact")

    payload: dict[str, Any] = {
        "schema_version": SCHEMA,
        "status": STATUS,
        "source_repo_sha": str(source_repo_sha),
        "hypothesis": "MATURE_STATE_JUMP_GENERATOR_SCALES_ITS_CONFIRMED_ADVANTAGE_TO_CUMULATIVE_1008_PER_ARM_DEVELOPMENT_BUDGET",
        "stage2": {
            "templates": list(TEMPLATES),
            "per_template_budget_per_arm": PER_TEMPLATE_PER_ARM,
            "total_budget_per_arm": TOTAL_PER_ARM,
            "total_financial_evaluations": TOTAL_EVALUATIONS,
            "checkpoint_batch_size": 24,
            "checkpoints_per_template_per_arm": 3,
            "cumulative_prior_budget_per_arm": CUMULATIVE_PRIOR_PER_ARM,
            "target_cumulative_budget_per_arm": TARGET_TOTAL_PER_ARM,
            "automatic_followon_launch": False,
        },
        "source_stage1_prefreeze": {
            "relative_path": str(STAGE1_PREFREEZE).replace("\\", "/"),
            "file_sha256": _sha(p1),
            "payload_sha256": stage1_hash,
        },
        "source_stage15_prefreeze": {
            "relative_path": str(STAGE15_PREFREEZE).replace("\\", "/"),
            "file_sha256": _sha(p15),
            "payload_sha256": stage15_hash,
        },
        "source_stage15_postrun_audit": {
            "relative_path": str(STAGE15_POSTRUN_AUDIT).replace("\\", "/"),
            "file_sha256": _sha(pa),
            "payload_sha256": audit_hash,
        },
        "source_stage15_terminal": {
            "relative_path": str(STAGE15_TERMINAL).replace("\\", "/"),
            "file_sha256": _sha(pt),
            "payload_sha256": terminal_hash,
            "source_run_repo_sha": str(terminal["repo_sha"]),
            "evaluated": int(terminal["evaluated"]),
        },
        "arm_a_primitive": {
            "policy_id": "PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1",
            "source_supply_relative_path": str(stage1["arm_a_primitive"]["source_supply_relative_path"]),
            "source_supply_file_sha256": str(stage1["arm_a_primitive"]["source_supply_file_sha256"]),
            "source_supply_payload_sha256": str(stage1["arm_a_primitive"]["source_supply_payload_sha256"]),
            "stage_d_prefreeze_relative_path": str(stage1["arm_a_primitive"]["stage_d_prefreeze_relative_path"]),
            "stage_d_prefreeze_file_sha256": str(stage1["arm_a_primitive"]["stage_d_prefreeze_file_sha256"]),
            "stage_d_prefreeze_payload_sha256": str(stage1["arm_a_primitive"]["stage_d_prefreeze_payload_sha256"]),
            "selection_slice": "FROZEN_STAGE1_ORDER_INDEX_72_144",
            "selected_by_template": selected_by_template,
            "selected_exact_identities": selected_exacts,
            "selected_exact_identities_sha256": stable_hash(selected_exacts),
            "selected_count": len(selected_exacts),
            "candidate_labels_read_during_selection": False,
        },
        "arm_b_state_jump": {
            "policy_id": "SEMANTIC_STATE_JUMP_GENERATOR_V2",
            "continuation_mode": "RESTORE_EXACT_STAGE15_FINAL_OPTIMIZER_STATE",
            "source_snapshot_relative_path": str(STAGE15_FINAL_STATE).replace("\\", "/"),
            "source_snapshot_file_sha256": _sha(ps),
            "source_snapshot_payload_sha256": snapshot_hash,
            "source_history_count": len(snapshot["history"]),
            "source_generated_exact_count": len(snapshot["generated_exact_identities"]),
            "source_memory_observations": int(diag["memory_observations"]),
            "source_elite_count": int(diag["elite_count"]),
            "source_generated_unique_count": int(diag["generated_unique_count"]),
            "within_stage2_ask_tell_adaptation": True,
            "sealed_feedback_allowed": False,
            "candidate_exact_membership_prefrozen": False,
            "zero_financial_supply_audit_required_before_project_control": True,
        },
        "stage15_confirmation_reference": {
            "arm_a_productive": int(terminal["arm_metrics"]["PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1"]["productive"]),
            "arm_b_productive": int(terminal["arm_metrics"]["SEMANTIC_STATE_JUMP_GENERATOR_V2"]["productive"]),
            "arm_a_stable": int(terminal["arm_metrics"]["PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1"]["stable"]),
            "arm_b_stable": int(terminal["arm_metrics"]["SEMANTIC_STATE_JUMP_GENERATOR_V2"]["stable"]),
            "arm_a_behaviors": int(terminal["arm_metrics"]["PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1"]["distinct_behavior_pair_count"]),
            "arm_b_behaviors": int(terminal["arm_metrics"]["SEMANTIC_STATE_JUMP_GENERATOR_V2"]["distinct_behavior_pair_count"]),
            "productive_template_win_count_b": int(terminal["decision"]["productive_template_win_count_b"]),
            "maximum_positive_productive_gain_template_fraction": float(terminal["decision"]["maximum_positive_productive_gain_template_fraction"]),
            "used_for_candidate_selection": False,
        },
        "stage2_decision_contract": {
            "primary_comparator": "SAME_RUN_STAGE2_ARM_A_VS_MATURE_ARM_B",
            "scale_confirmation_pass": (
                "B productive >= A*1.05 AND B stable >= A*1.00 AND B behavior >= A*0.95 "
                "AND B productive wins >=4/7 templates AND max positive productive gain template fraction <=0.40"
            ),
            "clear_loss": "B productive < A*0.95 AND B stable <= A stable AND B behavior <= A behavior",
            "otherwise": "AMBIGUOUS_STAGE2_SCALE_CONFIRMATION_NO_POLICY_CHANGE",
            "search_core_policy_change_requires_separate_project_control_review": True,
            "automatic_policy_change_authorized": False,
        },
        "financial_labels_read_by_builder": False,
        "candidate_evaluation_executed": False,
        "restricted_reads": {
            "validation": 0, "holdout": 0, "historical_2023": 0,
            "forward_b": 0, "forward_2026": 0,
        },
        "promotion_authorized": False,
        "oos_authority": "NONE",
        "automatic_policy_change_authorized": False,
    }
    payload["prefreeze_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--source-repo-sha", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    payload = build(args.repo_root, source_repo_sha=args.source_repo_sha)
    out = args.output if args.output.is_absolute() else args.repo_root / args.output
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "status": payload["status"],
        "selected_per_arm": payload["stage2"]["total_budget_per_arm"],
        "total_evaluations": payload["stage2"]["total_financial_evaluations"],
        "prefreeze_payload_sha256": payload["prefreeze_payload_sha256"],
        "output": str(out.resolve()),
    }, sort_keys=True))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())