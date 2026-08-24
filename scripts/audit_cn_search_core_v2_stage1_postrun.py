"""Post-run audit for Search Core V2 Stage-1 and its historical sanity reference."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from our_system_phase2.services.unified_capability_registry import stable_hash


SCHEMA = "cn_search_core_v2_stage1_postrun_audit_v1"
STATUS = "SEARCH_CORE_V2_STAGE1_POSTRUN_AUDIT_COMPLETE_NO_STAGE2"
TEMPLATES = (
    "BASE_EVENT",
    "BASE_MARKET",
    "BASE_MARKET_EVENT",
    "BASE_TEMPORAL",
    "BASE_TEMPORAL_EVENT",
    "BASE_TEMPORAL_MARKET",
    "BASE_TEMPORAL_MARKET_EVENT",
)
A = "PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1"
B = "SEMANTIC_STATE_JUMP_GENERATOR_V2"


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify_self(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def _metric_view(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: row.get(key)
        for key in (
            "evaluated",
            "admitted",
            "productive",
            "stable",
            "distinct_behavior_pair_count",
            "distinct_structural_region_count",
            "round_index",
            "template_id",
            "arm",
        )
    }


def audit(args: argparse.Namespace) -> dict[str, Any]:
    repo = args.repo_root.resolve()
    terminal_path = args.stage1_terminal.resolve()
    old_root = args.old_stage1_root.resolve()
    terminal = _read(terminal_path)
    terminal_hash = _verify_self(
        terminal, "closure_payload_sha256", "Search Core V2 Stage-1 terminal"
    )
    if (
        terminal.get("status") != "SEARCH_CORE_V2_STAGE1_COMPLETE"
        or int(terminal.get("evaluated") or 0) != 672
        or int(terminal.get("evaluated_per_arm") or 0) != 336
        or any(int(v) != 0 for v in dict(terminal.get("restricted_reads") or {}).values())
        or bool(dict(terminal.get("decision") or {}).get("automatic_stage2_authorized"))
    ):
        raise RuntimeError("Stage-1 terminal contract drift")

    prefreeze_path = repo / "runtime/run_plans/cn_search_core_v2_stage1_prefreeze_20260824.json"
    stage_d_path = repo / "runtime/run_plans/cn_program_stage_d_primitive_confirmation_prefreeze_v1.json"
    stage_d_audit_path = repo / "runtime/run_plans/cn_program_stage_d_5e2dd3f_independent_audit_20260820.json"
    stage_d_builder_path = repo / "scripts/build_cn_program_stage_d_primitive_confirmation_prefreeze_v1.py"
    prefreeze = _read(prefreeze_path)
    _verify_self(prefreeze, "prefreeze_payload_sha256", "Stage-1 prefreeze")
    stage_d = _read(stage_d_path)
    _verify_self(stage_d, "prefreeze_payload_sha256", "Stage-D prefreeze")
    stage_d_audit = _read(stage_d_audit_path)
    _verify_self(stage_d_audit, "audit_payload_sha256", "Stage-D audit")

    # Stage-D cohort selection is hash-stratified before primitive scores are
    # computed. Bind the exact builder implementation so this audit cannot
    # silently reinterpret a later implementation.
    builder_source = stage_d_builder_path.read_text(encoding="utf-8-sig")
    hash_selection_marker = "stable_hash({'seed':COHORT_SEED,'template':t,'exact':r['exact_identity']})"
    score_after_marker = "for row in cohort:\n        primitive,novelty,_detail=primitive_score(row,stats)"
    stage_d_hash_split_verified = (
        hash_selection_marker in builder_source
        and score_after_marker in builder_source
        and builder_source.index(hash_selection_marker) < builder_source.index(score_after_marker)
    )
    if not stage_d_hash_split_verified:
        raise RuntimeError("Stage-D hash-before-score cohort selection source drift")

    stage_d_cohort_by_template: dict[str, set[str]] = {template: set() for template in TEMPLATES}
    for row in list(stage_d["cohort"]["candidates"]):
        stage_d_cohort_by_template[str(row["template_id"])].add(str(row["exact_identity"]))
    stage1_orders = dict(prefreeze["arm_a_primitive"]["eligible_orders_by_template"])
    partition_rows: dict[str, Any] = {}
    for template in TEMPLATES:
        stage_d_set = stage_d_cohort_by_template[template]
        remainder = set(map(str, stage1_orders[template]))
        partition_rows[template] = {
            "stage_d_hash_selected_count": len(stage_d_set),
            "stage1_unspent_remainder_count": len(remainder),
            "overlap": len(stage_d_set & remainder),
            "combined_count": len(stage_d_set | remainder),
        }
        if partition_rows[template] != {
            "stage_d_hash_selected_count": 288,
            "stage1_unspent_remainder_count": 224,
            "overlap": 0,
            "combined_count": 512,
        }:
            raise RuntimeError(f"Stage-D/Stage-1 partition drift for {template}")

    # The old failed run completed the exact first round before the Generator
    # failed at checkpoint 15. Primitive first-round parity is a stronger
    # evaluator/data sanity check than an unmatched historical yield band.
    checkpoint_metrics = {
        int(row["checkpoint_ordinal"]): dict(row)
        for row in list(terminal["checkpoint_metrics"])
    }
    parity_rows: dict[str, Any] = {}
    first_round_primitive_parity = True
    for checkpoint in range(0, 14, 2):
        old_metric = _read(old_root / f"checkpoint_{checkpoint:04d}" / "checkpoint_metric.json")
        new_metric = checkpoint_metrics[checkpoint]
        old_view = _metric_view(old_metric)
        new_view = _metric_view(new_metric)
        same = old_view == new_view
        first_round_primitive_parity &= same
        parity_rows[str(checkpoint)] = {
            "same": same,
            "old": old_view,
            "new": new_view,
        }
    if not first_round_primitive_parity:
        raise RuntimeError("Primitive first-round deterministic parity failed")

    a = dict(terminal["arm_metrics"][A])
    b = dict(terminal["arm_metrics"][B])
    a0 = dict(terminal["round_metrics"][A]["0"])
    a1 = dict(terminal["round_metrics"][A]["1"])
    b0 = dict(terminal["round_metrics"][B]["0"])
    b1 = dict(terminal["round_metrics"][B]["1"])
    d336 = dict(stage_d_audit["budget336"]["primitive_local"])
    d504 = dict(stage_d_audit["budget504"]["primitive_local"])
    current_a_rate = float(a["productive"]) / float(a["evaluated"])
    d336_rate = float(d336["productive"]) / float(d336["evaluated"])
    d504_rate = float(d504["productive"]) / float(d504["evaluated"])
    selection_fraction_stage_d_336 = 48.0 / 288.0
    selection_fraction_stage_d_504 = 72.0 / 288.0
    selection_fraction_stage1 = 48.0 / 224.0
    yield_bracket_pass = d504_rate <= current_a_rate <= d336_rate

    template_delta = {}
    positive_deltas = []
    generator_template_wins = 0
    for template in TEMPLATES:
        pa = int(terminal["per_template_metrics"][A][template]["productive"])
        pb = int(terminal["per_template_metrics"][B][template]["productive"])
        delta = pb - pa
        template_delta[template] = delta
        if delta > 0:
            positive_deltas.append(delta)
            generator_template_wins += 1
    positive_sum = sum(positive_deltas)
    max_positive_share = max(positive_deltas) / positive_sum if positive_sum else None

    payload = {
        "schema_version": SCHEMA,
        "status": STATUS,
        "audit_repo_sha": str(args.audit_repo_sha),
        "source_stage1": {
            "path": str(terminal_path),
            "file_sha256": _sha(terminal_path),
            "closure_payload_sha256": terminal_hash,
            "repo_sha": str(terminal["repo_sha"]),
            "evaluated": int(terminal["evaluated"]),
            "restricted_reads": dict(terminal["restricted_reads"]),
        },
        "generator_fix_status": "PRODUCTION_FAILURE_POINT_SURPASSED_AND_STAGE1_CLOSED",
        "first_round_primitive_deterministic_parity": {
            "status": "PASS",
            "old_run_root": str(old_root),
            "checkpoint_rows": parity_rows,
            "aggregate": dict(a0),
        },
        "historical_sanity_reference_audit": {
            "original_reference": dict(prefreeze["historical_336_sanity_band"]),
            "stage_d_cohort_selection": "HASH_STRATIFIED_BEFORE_PRIMITIVE_SCORING",
            "stage_d_builder_file_sha256": _sha(stage_d_builder_path),
            "hash_before_score_source_verified": stage_d_hash_split_verified,
            "partition_by_template": partition_rows,
            "selection_fraction_stage_d_336": selection_fraction_stage_d_336,
            "selection_fraction_stage_d_504": selection_fraction_stage_d_504,
            "selection_fraction_stage1_arm_a": selection_fraction_stage1,
            "stage_d_336_productive_rate": d336_rate,
            "stage_d_504_productive_rate": d504_rate,
            "stage1_arm_a_productive_rate": current_a_rate,
            "selection_intensity_yield_bracket_pass": yield_bracket_pass,
            "classification": "HISTORICAL_SANITY_BAND_NOT_SELECTION_INTENSITY_MATCHED",
            "interpretation": (
                "A=126/336 does not by itself establish evaluator drift: the exact first-round Primitive replay is deterministic, "
                "and the current 37.5% yield lies between Stage-D top-48/288 and top-72/288 Primitive yields while Core2 selects top-48/224."
            ),
        },
        "stage1_metrics": {
            "arm_a": a,
            "arm_b": b,
            "round0": {"arm_a": a0, "arm_b": b0},
            "round1": {"arm_a": a1, "arm_b": b1},
            "late_productive_delta_b_vs_a": int(b1["productive"]) - int(a1["productive"]),
            "late_stable_delta_b_vs_a": int(b1["stable"]) - int(a1["stable"]),
            "late_behavior_delta_b_vs_a": int(b1["distinct_behavior_pair_count"]) - int(a1["distinct_behavior_pair_count"]),
            "per_template_productive_delta_b_vs_a": template_delta,
            "generator_productive_template_win_count": generator_template_wins,
            "max_positive_template_delta_share": max_positive_share,
        },
        "decision_contract_review": {
            "terminal_status": str(terminal["decision"]["status"]),
            "terminal_baseline_historical_sanity_pass": bool(terminal["decision"]["baseline_historical_sanity_pass"]),
            "automatic_stage2_authorized": False,
            "finding": "FROZEN_TEXT_REQUIRED_BASELINE_AUDIT_BUT_47BC94F_DECISION_IMPLEMENTATION_ONLY_RECORDED_THE_FLAG",
            "required_repair": "FAIL_CLOSED_ON_BASELINE_SANITY_MISS_BEFORE_STAGE2_REVIEW_ELIGIBILITY",
        },
        "project_control_recommendation": {
            "verdict": "DO_NOT_LAUNCH_FULL_STAGE2",
            "next": "PREFREEZE_STAGE1_5_MATURE_STATE_JUMP_CONFIRMATION",
            "budget": {"per_arm": 168, "total": 336, "per_template_per_arm": 24},
            "primitive_source": "NEXT_UNSPENT_SLICE_48_TO_71_PER_TEMPLATE_FROM_STAGE1_FROZEN_ORDER",
            "generator_source": "RESTORE_EXACT_STAGE1_CHECKPOINT_0027_OPTIMIZER_STATE_AFTER",
            "purpose": "CONFIRM_OR_FALSIFY_LATE_ROUND_MATURE_GENERATOR_ADVANTAGE_AT_ONE_QUARTER_OF_FULL_STAGE2_COST",
            "automatic_stage2": False,
            "oos_authority": "NONE",
        },
    }
    payload["audit_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--audit-repo-sha", required=True)
    parser.add_argument("--stage1-terminal", type=Path, required=True)
    parser.add_argument("--old-stage1-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    audit_sha = str(args.audit_repo_sha).lower()
    if len(audit_sha) != 40 or any(ch not in "0123456789abcdef" for ch in audit_sha):
        parser.error("--audit-repo-sha must be a 40-character hex Git SHA")
    args.audit_repo_sha = audit_sha
    payload = audit(args)
    output = args.output if args.output.is_absolute() else args.repo_root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "status": payload["status"],
        "audit_payload_sha256": payload["audit_payload_sha256"],
        "output": str(output.resolve()),
        "selection_intensity_yield_bracket_pass": payload["historical_sanity_reference_audit"]["selection_intensity_yield_bracket_pass"],
        "late_productive_delta_b_vs_a": payload["stage1_metrics"]["late_productive_delta_b_vs_a"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
