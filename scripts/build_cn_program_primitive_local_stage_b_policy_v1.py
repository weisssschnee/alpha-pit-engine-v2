from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping

from our_system_phase2.services.unified_capability_registry import stable_hash

SCHEMA = "cn_program_primitive_local_stage_b_policy_v1"
STATUS = "PRIMITIVE_LOCAL_STAGE_B_POLICY_FROZEN_BEFORE_STAGE_B_READ"
SEEDS = (1729, 2718, 31415, 65537, 104729, 130363, 155921, 196613)
BUDGETS_PER_TEMPORAL = (8, 12, 24)


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _self_hash(payload: Mapping[str, Any], field: str) -> dict[str, Any]:
    body = dict(payload)
    body[field] = stable_hash(body)
    return body


def _event_stats(audit: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    output = {}
    for row in audit["sibling_events_ranked"]:
        event = str(row["event_component_id"])
        n = int(row["evaluated"])
        y = int(row["productive"])
        output[event] = {
            "evaluated": n,
            "productive": y,
            "productive_rate": float(row["productive_rate"]),
            "beta_alpha": 1 + y,
            "beta_beta": 1 + n - y,
            "beta_mean": (1 + y) / (2 + n),
            "representation": str(row["representation"]),
            "pulse": str(row["pulse"]),
        }
    return output


def _family_means(audit: Mapping[str, Any]) -> tuple[dict[str, float], dict[str, float]]:
    reps = {
        str(key): (1 + int(value["productive"])) / (2 + int(value["evaluated"]))
        for key, value in dict(audit["representation_metrics"]).items()
    }
    pulses = {
        str(key): (1 + int(value["productive"])) / (2 + int(value["evaluated"]))
        for key, value in dict(audit["pulse_metrics"]).items()
    }
    return reps, pulses


def _uniform_key(seed: int, exact: str) -> str:
    return stable_hash({"policy": "UNIFORM_HASH", "seed": int(seed), "exact_identity": exact})


def _tie_key(seed: int, exact: str) -> str:
    return stable_hash({"policy": "PRIMITIVE_LOCAL_TIE", "seed": int(seed), "exact_identity": exact})


def build(repo: Path) -> dict[str, Any]:
    repo = repo.resolve()
    audit_path = repo / "runtime/run_plans/cn_program_disclosure_timing_mechanism_successor_retry3_cbf191f_independent_audit_20260819.json"
    prefreeze_path = repo / "runtime/run_plans/cn_disclosure_timing_mechanism_successor_prefreeze_20260819.json"
    old_spent_path = repo / "runtime/run_plans/cn_disclosure_timing_mechanism_spent_exact_freeze_20260819.json"
    audit = _read(audit_path)
    prefreeze = _read(prefreeze_path)
    old_spent = _read(old_spent_path)
    if audit.get("status") != "PASS_INDEPENDENT_TERMINAL_AUDIT" or audit.get("classification") != "LOCAL_PRIMITIVE_WIN_NOT_SYSTEMATIC_EVENT_FAMILY":
        raise ValueError("Stage A terminal audit is not the frozen failed-family result")
    if audit.get("stage_b_never_started") is not True:
        raise ValueError("Stage B was already touched")
    stage_a = list(prefreeze["candidates"]["stage_a"])
    stage_b = list(prefreeze["candidates"]["stage_b"])
    if len(stage_a) != 288 or len(stage_b) != 264:
        raise ValueError("prefrozen candidate counts drift")
    stage_b_fields = sorted(
        {str(field) for row in stage_b for field in row["physical_field_columns"]}
    )
    if len(stage_b_fields) != 38:
        raise ValueError("Stage B physical field geometry drift")

    old_spent_ids = list(map(str, old_spent["combined_spent_exact_identities"]))
    stage_a_ids = [str(row["exact_identity"]) for row in stage_a]
    stage_b_ids = [str(row["exact_identity"]) for row in stage_b]
    combined_spent = sorted(set(old_spent_ids).union(stage_a_ids))
    if len(old_spent_ids) != 2150 or len(set(stage_a_ids)) != 288 or len(combined_spent) != 2438:
        raise ValueError("spent identity count drift")
    if set(stage_b_ids).intersection(combined_spent):
        raise ValueError("Stage B is not fresh against Stage A spent set")

    events = _event_stats(audit)
    rep_means, pulse_means = _family_means(audit)
    if set(events) != {str(row["event_component_id"]) for row in stage_b}:
        raise ValueError("Stage B event coverage differs from Stage A sibling evidence")

    by_temporal: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in stage_b:
        by_temporal[str(row["temporal_component_id"])].append(dict(row))
    if len(by_temporal) != 6 or any(len(rows) != 44 for rows in by_temporal.values()):
        raise ValueError("Stage B temporal strata drift")

    primitive_orders: dict[str, list[str]] = {}
    family_orders: dict[str, list[str]] = {}
    uniform_orders: dict[str, dict[str, list[str]]] = {str(seed): {} for seed in SEEDS}
    primitive_scores = {}
    family_scores = {}

    for temporal, rows in sorted(by_temporal.items()):
        for row in rows:
            exact = str(row["exact_identity"])
            event = str(row["event_component_id"])
            rep = str(row["event_representation_family"])
            pulse = str(row["event_pulse_family"])
            primitive_scores[exact] = float(events[event]["beta_mean"])
            family_scores[exact] = 0.5 * float(rep_means[rep]) + 0.5 * float(pulse_means[pulse])
        primitive_orders[temporal] = [
            str(row["exact_identity"])
            for row in sorted(
                rows,
                key=lambda row: (
                    -primitive_scores[str(row["exact_identity"])],
                    _tie_key(20260819, str(row["exact_identity"])),
                ),
            )
        ]
        family_orders[temporal] = [
            str(row["exact_identity"])
            for row in sorted(
                rows,
                key=lambda row: (
                    -family_scores[str(row["exact_identity"])],
                    _tie_key(20260820, str(row["exact_identity"])),
                ),
            )
        ]
        for seed in SEEDS:
            uniform_orders[str(seed)][temporal] = [
                str(row["exact_identity"])
                for row in sorted(rows, key=lambda row: _uniform_key(seed, str(row["exact_identity"])))
            ]

    payload: dict[str, Any] = {
        "schema_version": SCHEMA,
        "status": STATUS,
        "source_stage_a_audit": {
            "relative_path": "runtime/run_plans/cn_program_disclosure_timing_mechanism_successor_retry3_cbf191f_independent_audit_20260819.json",
            "file_sha256": _sha(audit_path),
            "payload_sha256": str(audit["audit_payload_sha256"]),
            "classification": str(audit["classification"]),
            "stage_b_never_started": True,
        },
        "source_prefreeze": {
            "relative_path": "runtime/run_plans/cn_disclosure_timing_mechanism_successor_prefreeze_20260819.json",
            "file_sha256": _sha(prefreeze_path),
            "payload_sha256": str(prefreeze["prefreeze_payload_sha256"]),
            "stage_b_count": len(stage_b),
        },
        "stage_b_resource_preview": {
            "field_column_count": len(stage_b_fields),
            "field_columns": stage_b_fields,
            "field_columns_sha256": stable_hash(stage_b_fields),
            "candidate_evaluation_executed": False,
        },
        "spent_freeze": {
            "old_spent_count": len(old_spent_ids),
            "stage_a_spent_count": len(stage_a_ids),
            "combined_spent_exact_count": len(combined_spent),
            "combined_spent_exact_identities": combined_spent,
            "combined_spent_exact_identities_sha256": stable_hash(combined_spent),
            "stage_b_overlap_count": 0,
        },
        "training_evidence": {
            "event_exact_beta_posteriors": events,
            "representation_beta_means": rep_means,
            "pulse_beta_means": pulse_means,
            "stage_b_labels_read": False,
        },
        "policies": {
            "PRIMITIVE_LOCAL_BETA_TRANSFER_V1": {
                "description": "Rank within each unseen temporal stratum by Stage-A event-exact Beta(1,1) posterior mean; exact hash only breaks ties.",
                "orders_by_temporal": primitive_orders,
                "candidate_scores": primitive_scores,
                "uses_stage_b_feedback": False,
            },
            "FAMILY_ONLY_BACKOFF_V1": {
                "description": "Rank by equally weighted representation/pulse Beta means; deliberately excludes event-exact credit.",
                "orders_by_temporal": family_orders,
                "candidate_scores": family_scores,
                "uses_stage_b_feedback": False,
            },
            "UNIFORM_HASH_V1": {
                "seeds": list(SEEDS),
                "orders_by_seed_and_temporal": uniform_orders,
                "uses_stage_b_feedback": False,
            },
        },
        "evaluation": {
            "temporal_strata_count": 6,
            "candidates_per_temporal": 44,
            "budgets_per_temporal": list(BUDGETS_PER_TEMPORAL),
            "total_budgets": [6 * value for value in BUDGETS_PER_TEMPORAL],
            "primary_budget_per_temporal": 12,
            "primary_total_budget": 72,
            "primary_system_search_victory_gate": {
                "primitive_local_productive_count_vs_uniform_seed_mean_minimum_relative_ratio": 1.25,
                "primitive_local_productive_count_vs_uniform_seed_mean_minimum_absolute_delta": 5.0,
                "minimum_temporal_strata_won_vs_uniform_seed_mean": 4,
                "budget_144_minimum_relative_ratio_vs_uniform_seed_mean": 1.10,
                "all_stage_b_exact_unique": True,
                "all_stage_b_exact_fresh_vs_2438_spent": True,
                "restricted_reads_zero": True,
            },
            "interpretation": {
                "pass": "PRIMITIVE_LOCAL_SEARCH_TRANSFER_PASS_STAGE_C_LARGE_SCALE_AUTHORIZATION_ELIGIBLE",
                "fail": "PRIMITIVE_LOCAL_SEARCH_TRANSFER_FAIL_NO_LARGE_SCALE_CLAIM",
            },
        },
        "stage_b_candidate_set_frozen_before_stage_a_read": True,
        "stage_b_financial_labels_read_during_policy_freeze": False,
        "optimizer_selection_used_during_policy_freeze": False,
        "restricted_reads": {"validation": 0, "holdout": 0, "historical_2023": 0, "forward_b": 0, "forward_2026": 0},
        "promotion_authorized": False,
        "oos_authority": "NONE",
    }
    payload["policy_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("runtime/run_plans/cn_program_primitive_local_stage_b_policy_v1.json"),
    )
    args = parser.parse_args(argv)
    payload = build(args.repo_root)
    output = args.output if args.output.is_absolute() else args.repo_root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "status": payload["status"],
        "policy_payload_sha256": payload["policy_payload_sha256"],
        "spent": payload["spent_freeze"]["combined_spent_exact_count"],
        "stage_b": payload["source_prefreeze"]["stage_b_count"],
        "primary_budget": payload["evaluation"]["primary_total_budget"],
        "output": str(output.resolve()),
    }, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
