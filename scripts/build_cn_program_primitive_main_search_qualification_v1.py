"""Freeze qualification evidence for Primitive-local as the Program main search policy."""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Mapping

from scripts.build_cn_program_stage_c_system_search_prefreeze_v1 import primitive_score
from scripts.run_cn_program_optimizer_large_fresh_v3 import _checkpoint_arm_v3
from our_system_phase2.runtime.cn_program_optimizer_large_fresh_v1 import ENHANCED_TEMPLATES
from our_system_phase2.services.program_search_optimizer_historical_v2 import (
    CATALOG_TYPED_EVOLUTION_PROGRAM_V2,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    UNIFORM_CONTROL,
    program_availability_entries_v1,
)
from our_system_phase2.services.program_search_primitive_credit_v1 import (
    PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1,
    HierarchicalPrimitiveCreditScorerV1,
)
from our_system_phase2.services.project_control_admission import sha256_file
from our_system_phase2.services.unified_capability_registry import stable_hash


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _verify_self(path: Path, field: str, label: str) -> dict[str, Any]:
    payload = _read(path)
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return payload


def _metadata(candidate: Mapping[str, Any], entry: Any) -> dict[str, Any]:
    components = {}
    for role, raw in sorted(dict(candidate["components"]).items()):
        binding = dict(raw)
        components[role] = {
            "component_id": str(binding["component_id"]),
            "route_id": str(binding["route_id"]),
            "skeleton_id": str(entry.genes[f"{role}__skeleton_id"]),
        }
    return {
        "template_id": str(candidate["template_id"]),
        "components": components,
        "tie_break_identity": str(candidate["exact_identity"]),
    }


def build(repo_root: Path) -> dict[str, Any]:
    root = repo_root.resolve()
    stats_path = root / "runtime/run_plans/cn_stage_c_primitive_credit_stats_20260820.json"
    stats = _verify_self(stats_path, "stats_payload_sha256", "primitive stats")
    if (
        stats.get("status") != "SPENT_DEVELOPMENT_PRIMITIVE_CREDIT_STATS_FROZEN"
        or int(stats.get("unique_program_exact_count") or 0) != 1392
        or dict(stats.get("source_counts") or {})
        != {"large": 840, "stage_a": 288, "stage_b": 264}
    ):
        raise RuntimeError("primitive bootstrap stats provenance drift")

    stage_d_path = root / "runtime/run_plans/cn_program_stage_d_5e2dd3f_independent_audit_20260820.json"
    stage_d = _verify_self(stage_d_path, "audit_payload_sha256", "Stage D audit")
    if (
        stage_d.get("status") != "PASS_INDEPENDENT_TERMINAL_AUDIT"
        or stage_d.get("confirmation_status")
        != "PRIMITIVE_LOCAL_LARGE_SCALE_CONFIRMATION_PASS_STAGE_D"
        or list(stage_d.get("checks_failed") or ())
    ):
        raise RuntimeError("Stage D qualification evidence drift")
    b1008 = dict(stage_d["budget1008"])
    primitive1008 = int(dict(b1008["primitive_local"])["productive"])
    evolution1008 = int(dict(b1008["typed_evolution"])["productive"])
    uniform1008 = float(b1008["uniform_seed_mean_productive_count"])
    if (
        primitive1008 != 355
        or evolution1008 != 296
        or uniform1008 != 254.0
        or int(b1008["primitive_templates_won_vs_uniform_mean"]) != 7
        or int(b1008["primitive_templates_won_vs_evolution"]) != 6
    ):
        raise RuntimeError("Stage D primary metrics drift")

    pre_path = root / "runtime/run_plans/cn_program_stage_d_primitive_confirmation_prefreeze_v1.json"
    pre = _verify_self(pre_path, "prefreeze_payload_sha256", "Stage D prefreeze")
    if (
        pre.get("no_stage_c_label_retraining_or_tuning") is not True
        or int(dict(pre["primitive_score_source"])["stage_c_results_in_stats"]) != 0
    ):
        raise RuntimeError("Stage D scorer contamination drift")
    candidates = list(dict(pre["cohort"])["candidates"])
    entries = program_availability_entries_v1(
        [{"genes": dict(row["program_genes"])} for row in candidates]
    )
    scorer = HierarchicalPrimitiveCreditScorerV1(
        primitive_stats=stats,
        primitive_stats_payload_sha256=str(stats["stats_payload_sha256"]),
    )
    max_abs_error = 0.0
    production_scores: dict[str, float] = {}
    physical_by_normalized: dict[str, str] = {}
    for candidate, entry in zip(candidates, entries, strict=True):
        expected_score, _novelty, _detail = primitive_score(candidate, stats)
        observed = float(scorer.score(_metadata(candidate, entry))["primitive_score"])
        max_abs_error = max(max_abs_error, abs(observed - float(expected_score)))
        production_scores[str(candidate["exact_identity"])] = observed
        physical_by_normalized[entry.exact_identity] = str(candidate["exact_identity"])
    if max_abs_error > 1e-15:
        raise RuntimeError("production primitive scorer numeric drift")
    expected_orders = dict(dict(pre["policy"])["primary_policy_order_by_template"])
    order_matches = {}
    for template_id in ENHANCED_TEMPLATES:
        physical = [
            str(row["exact_identity"])
            for row in candidates
            if str(row["template_id"]) == template_id
        ]
        observed = sorted(
            physical,
            key=lambda exact: (-production_scores[exact], exact),
        )
        order_matches[template_id] = observed == list(
            map(str, expected_orders[template_id])
        )
    if not all(order_matches.values()):
        raise RuntimeError("production primitive scorer Stage D order drift")

    scheduler = {}
    for macro in range(14):
        arms = [
            _checkpoint_arm_v3(macro, index)
            for index in range(len(ENHANCED_TEMPLATES))
        ]
        scheduler[str(macro)] = {
            "primitive": arms.count(PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1),
            "uniform": arms.count(UNIFORM_CONTROL),
            "evolution": arms.count(CATALOG_TYPED_EVOLUTION_PROGRAM_V2),
        }
        if scheduler[str(macro)] != {"primitive": 5, "uniform": 1, "evolution": 1}:
            raise RuntimeError("V3 scheduler allocation drift")

    rehearsal_path = root / "runtime/run_plans/cn_program_primitive_main_real_catalog_rehearsal_20260820.json"
    rehearsal = _verify_self(
        rehearsal_path,
        "rehearsal_payload_sha256",
        "primitive main real-catalog rehearsal",
    )
    if (
        rehearsal.get("status")
        != "ZERO_FINANCIAL_PRIMITIVE_MAIN_REAL_CATALOG_REHEARSAL_PASS"
        or int(rehearsal.get("program_space_entries") or 0) != 2274
        or int(rehearsal.get("metadata_entries") or 0) != 2274
        or int(rehearsal.get("tie_break_unique") or 0) != 2274
        or rehearsal.get("preview_state_unchanged") is not True
        or rehearsal.get("candidate_evaluation_executed") is not False
    ):
        raise RuntimeError("real-catalog rehearsal drift")

    scorer_path = root / "src/our_system_phase2/services/program_search_primitive_credit_v1.py"
    bandit_path = root / "src/our_system_phase2/services/program_optimizer_large_fresh_v3.py"
    runner_path = root / "scripts/run_cn_program_optimizer_large_fresh_v3.py"
    payload: dict[str, Any] = {
        "schema_version": "cn_program_primitive_main_search_qualification_v1",
        "status": "PRIMITIVE_LOCAL_MAIN_SEARCH_QUALIFIED_V1",
        "decision": "PRIMITIVE_PRIMARY_UNIFORM_RESERVE_EVOLUTION_CHALLENGER",
        "evidence_role": "DEVELOPMENT_SEARCH_POLICY_QUALIFICATION_ONLY",
        "stage_d_confirmation": {
            "relative_path": str(stage_d_path.relative_to(root)).replace("\\", "/"),
            "file_sha256": sha256_file(stage_d_path),
            "payload_sha256": str(stage_d["audit_payload_sha256"]),
            "confirmation_status": str(stage_d["confirmation_status"]),
            "primitive_productive_at_1008": primitive1008,
            "typed_evolution_productive_at_1008": evolution1008,
            "uniform_seed_mean_productive_at_1008": uniform1008,
            "primitive_vs_evolution_ratio_at_1008": float(
                b1008["primitive_vs_evolution_productive_ratio"]
            ),
            "primitive_vs_uniform_ratio_at_1008": float(
                b1008["primitive_vs_uniform_productive_ratio"]
            ),
            "templates_won_vs_uniform_at_1008": int(
                b1008["primitive_templates_won_vs_uniform_mean"]
            ),
            "templates_won_vs_evolution_at_1008": int(
                b1008["primitive_templates_won_vs_evolution"]
            ),
        },
        "bootstrap_primitive_stats": {
            "relative_path": str(stats_path.relative_to(root)).replace("\\", "/"),
            "file_sha256": sha256_file(stats_path),
            "payload_sha256": str(stats["stats_payload_sha256"]),
            "unique_program_exact_count": 1392,
            "source_counts": dict(stats["source_counts"]),
            "stage_c_results_imported": False,
            "stage_d_results_imported": False,
        },
        "production_equivalence": {
            "stage_d_candidate_count": len(candidates),
            "numeric_max_abs_error": max_abs_error,
            "template_order_match": order_matches,
            "all_template_orders_match": all(order_matches.values()),
        },
        "real_catalog_rehearsal": {
            "relative_path": str(rehearsal_path.relative_to(root)).replace("\\", "/"),
            "file_sha256": sha256_file(rehearsal_path),
            "payload_sha256": str(rehearsal["rehearsal_payload_sha256"]),
            "program_space_entries": int(rehearsal["program_space_entries"]),
            "preview_asks": int(rehearsal["preview_asks"]),
            "preview_state_unchanged": True,
            "candidate_evaluation_executed": False,
        },
        "implementation": {
            "primitive_scorer_relative_path": str(scorer_path.relative_to(root)).replace("\\", "/"),
            "primitive_scorer_file_sha256": sha256_file(scorer_path),
            "large_fresh_v3_bandit_relative_path": str(bandit_path.relative_to(root)).replace("\\", "/"),
            "large_fresh_v3_bandit_file_sha256": sha256_file(bandit_path),
            "large_fresh_v3_runner_relative_path": str(runner_path.relative_to(root)).replace("\\", "/"),
            "large_fresh_v3_runner_file_sha256": sha256_file(runner_path),
        },
        "v3_budget_scheduler": {
            "primitive_checkpoints_per_7_template_macro": 5,
            "uniform_reserve_checkpoints_per_macro": 1,
            "typed_evolution_challenger_checkpoints_per_macro": 1,
            "rotation_verified_macros": scheduler,
        },
        "online_feedback_contract": {
            "primitive_prior_mutated_within_campaign": False,
            "primitive_tell_mode": "FROZEN_PRIOR_AUDIT_ONLY",
            "uniform_learning": False,
            "typed_evolution_learning": True,
        },
        "execution_authorized_by_qualification": False,
        "old_v2_catalog_launch_authorized": False,
        "reason_old_v2_catalog_launch_not_authorized": "SPENT_AUTHORITY_MUST_BE_REFRESHED_BEFORE_NEXT_FINANCIAL_CAMPAIGN",
        "evaluation_data_role": "DEVELOPMENT_ONLY",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "oos_authority": "NONE",
        "promotion_authorized": False,
    }
    payload["qualification_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("runtime/run_plans/cn_program_primitive_main_search_qualification_v1.json"),
    )
    args = parser.parse_args(argv)
    payload = build(args.repo_root)
    output = args.output if args.output.is_absolute() else args.repo_root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "status": payload["status"],
                "decision": payload["decision"],
                "qualification_payload_sha256": payload["qualification_payload_sha256"],
                "execution_authorized_by_qualification": payload["execution_authorized_by_qualification"],
                "output": str(output.resolve()),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
