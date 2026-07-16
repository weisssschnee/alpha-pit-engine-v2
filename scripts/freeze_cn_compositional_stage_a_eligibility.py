from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping


STATUS = "CN_COMPOSITIONAL_STAGE_A_ROUTE_ELIGIBILITY_FROZEN"
FORMALLY_EVALUABLE = "FORMALLY_EVALUABLE"
NATURAL_UNDERFILL = "FORMALLY_EVALUABLE_WITH_NATURAL_UNDERFILL"
PARTIAL = "PARTIAL_NOT_FORMALLY_EVALUABLE"

DISCLOSURE_ROUTE = "DISCLOSURE_EVENT"
NATURAL_UNDERFILL_ROUTES = {"INTRADAY_STATE_TRANSITION"}
MIN_NATURAL_UNDERFILL_RATIO = 0.90


class StageAEligibilityError(RuntimeError):
    """Raised when existing frozen evidence cannot support a scoped Stage A pack."""


def _fail(message: str) -> None:
    raise StageAEligibilityError(message)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _stable_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _artifact(path: Path, repo_root: Path) -> dict[str, Any]:
    resolved = path.resolve()
    try:
        recorded_path = resolved.relative_to(repo_root.resolve()).as_posix()
    except ValueError:
        recorded_path = resolved.as_posix()
    return {
        "path": recorded_path,
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _atomic_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        _fail("eligible Stage A pack cannot be empty")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    os.replace(temporary, path)


def _unique_count(rows: Iterable[Mapping[str, Any]], key: str) -> int:
    values = [str(row.get(key) or "") for row in rows]
    if any(not value for value in values):
        _fail(f"admission evidence contains an empty {key}")
    return len(set(values))


def build_stage_a_eligibility(
    *,
    plan: Mapping[str, Any],
    waterfall_rows: list[dict[str, str]],
    admission_rows: list[dict[str, str]],
    expressivity: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Build a scoped Stage A eligibility decision from existing frozen evidence."""

    stage_a = dict(plan.get("stage_a") or {})
    route_quotas = {
        str(route): int(quota)
        for route, quota in dict(stage_a.get("route_pair_quotas") or {}).items()
    }
    if not route_quotas or sum(route_quotas.values()) != int(stage_a.get("pair_budget") or -1):
        _fail("Stage A route quotas do not match the frozen pair budget")

    forbidden = dict(plan.get("access_contract") or {})
    if (
        forbidden.get("validation") != "FORBIDDEN"
        or forbidden.get("holdout") != "FORBIDDEN"
        or forbidden.get("forward_2026") != "SEALED"
        or forbidden.get("promotion") != "FORBIDDEN"
        or forbidden.get("cross_sprint_memory") != "FORBIDDEN"
    ):
        _fail("source plan does not preserve the sealed development-only boundary")

    waterfall_by_route = {str(row["route_id"]): row for row in waterfall_rows}
    if set(waterfall_by_route) != set(route_quotas):
        _fail("waterfall routes do not exactly match the Stage A routes")

    admissions_by_route: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in admission_rows:
        route = str(row.get("route_id") or "")
        if route not in route_quotas:
            _fail(f"admission evidence contains an unexpected route: {route}")
        if str(row.get("performance_accessed_for_admission") or "").lower() != "false":
            _fail("Stage A eligibility cannot consume performance-selected admission rows")
        admissions_by_route[route].append(row)

    if set(admissions_by_route) != set(route_quotas):
        _fail("admission evidence does not cover every Stage A route")
    if _unique_count(admission_rows, "pair_id") != len(admission_rows):
        _fail("admission evidence contains duplicate pair identities")
    if _unique_count(admission_rows, "exact_identity") != len(admission_rows):
        _fail("admission evidence violates one exact identity, one vote")

    expressivity_routes = dict(
        dict(dict(expressivity.get("generators") or {}).get("compositional_v2") or {}).get("routes")
        or {}
    )
    disclosure_expressivity = dict(expressivity_routes.get(DISCLOSURE_ROUTE) or {})

    decisions: dict[str, dict[str, Any]] = {}
    selected: list[dict[str, Any]] = []
    for route in sorted(route_quotas):
        quota = route_quotas[route]
        rows = sorted(
            admissions_by_route[route],
            key=lambda row: (int(row["admission_rank"]), str(row["pair_id"])),
        )
        waterfall = waterfall_by_route[route]
        admitted = len(rows)
        exact_unique = int(waterfall["exact_unique"])
        behavior_unique = int(waterfall["behavior_unique"])
        materialization_pass = int(waterfall["materialization_pass"])
        support_pass = int(waterfall["support_pass"])
        diversity_admitted = int(waterfall["diversity_admitted"])

        if diversity_admitted != admitted:
            _fail(f"{route} admission row count does not match the waterfall")
        if _unique_count(rows, "exact_identity") != admitted:
            _fail(f"{route} contains duplicate exact identities")
        expected_policies = {str(value) for value in plan.get("policies") or []}
        expected_seeds = {int(value) for value in plan.get("seeds") or []}
        if {str(row["policy_id"]) for row in rows} != expected_policies:
            _fail(f"{route} does not cover every frozen proposal policy")
        if {int(row["seed"]) for row in rows} != expected_seeds:
            _fail(f"{route} does not cover every frozen seed")

        status: str
        eligible_quota: int
        reasons: list[str]
        if route == DISCLOSURE_ROUTE:
            if exact_unique != 40 or int(disclosure_expressivity.get("exact_identity_count") or -1) != 40:
                _fail("Disclosure exclusion is not bound to the observed 40-identity structural ceiling")
            status = PARTIAL
            eligible_quota = 0
            reasons = [
                "SOURCE_AND_AUDIT_CEILING_40",
                "FINAL_EXACT_SUPPLY_40",
                f"SUPPORT_QUALIFIED_ADMISSION_{admitted}",
                "CURRENT_GENERATOR_DOES_NOT_REPRESENT_BROAD_DISCLOSURE_CAPABILITY",
            ]
        elif admitted >= quota:
            status = FORMALLY_EVALUABLE
            eligible_quota = quota
            reasons = [
                "ADMITTED_EXACT_SUPPLY_MEETS_FROZEN_ROUTE_QUOTA",
                "ALL_FROZEN_POLICIES_AND_SEEDS_PRESENT",
            ]
        elif (
            route in NATURAL_UNDERFILL_ROUTES
            and admitted > 0
            and admitted / quota >= MIN_NATURAL_UNDERFILL_RATIO
        ):
            status = NATURAL_UNDERFILL
            eligible_quota = admitted
            reasons = [
                "NATURAL_UNDERFILL_REPORTED_WITHOUT_BUDGET_REALLOCATION",
                "ALL_FROZEN_POLICIES_AND_SEEDS_PRESENT",
            ]
        else:
            _fail(
                f"{route} has only {admitted}/{quota} admitted exact pairs and has no "
                "approved scoped underfill rule"
            )

        chosen = rows[:eligible_quota]
        if chosen and {str(row["policy_id"]) for row in chosen} != expected_policies:
            _fail(f"{route} selected pack does not preserve every frozen proposal policy")
        if chosen and {int(row["seed"]) for row in chosen} != expected_seeds:
            _fail(f"{route} selected pack does not preserve every frozen seed")
        decisions[route] = {
            "status": status,
            "original_pair_quota": quota,
            "eligible_pair_quota": eligible_quota,
            "unused_pair_quota": quota - eligible_quota,
            "generated_exact_unique": exact_unique,
            "generated_behavior_unique": behavior_unique,
            "materialization_pass": materialization_pass,
            "support_pass": support_pass,
            "admitted_exact_supply": admitted,
            "admitted_behavior_identity_supply": _unique_count(rows, "exact_behavior_identity"),
            "admitted_signal_cluster_supply": _unique_count(rows, "behavior_cluster_id"),
            "admitted_skeleton_count": _unique_count(rows, "skeleton_id"),
            "admitted_policy_count": _unique_count(rows, "policy_id"),
            "admitted_seed_count": _unique_count(rows, "seed"),
            "supply_to_quota_ratio": admitted / quota,
            "selection_used_performance": False,
            "reasons": reasons,
        }
        for row in chosen:
            selected.append(
                {
                    "stage_a_ordinal": 0,
                    "route_stage_a_ordinal": 0,
                    **row,
                }
            )

    selected.sort(
        key=lambda row: (
            str(row["route_id"]),
            int(row["admission_rank"]),
            str(row["pair_id"]),
        )
    )
    route_ordinals: dict[str, int] = defaultdict(int)
    for ordinal, row in enumerate(selected, start=1):
        route = str(row["route_id"])
        route_ordinals[route] += 1
        row["stage_a_ordinal"] = ordinal
        row["route_stage_a_ordinal"] = route_ordinals[route]

    eligible_routes = [
        route for route, decision in decisions.items() if decision["eligible_pair_quota"] > 0
    ]
    excluded_routes = [
        route for route, decision in decisions.items() if decision["eligible_pair_quota"] == 0
    ]
    frozen_pair_budget = len(selected)
    original_pair_budget = int(stage_a["pair_budget"])
    decision = {
        "schema_version": "cn_compositional_stage_a_route_eligibility_v1",
        "status": STATUS,
        "research_claim": "CURRENT_FORMALLY_EVALUABLE_ROUTES_DEVELOPMENT_INCREMENT",
        "excluded_claims": [
            "COMPLETE_SEVEN_ROUTE_COMPOSITIONAL_CAPABILITY",
            "BROAD_DISCLOSURE_EVENT_CAPABILITY",
            "VALIDATION_HOLDOUT_OR_FORWARD_PERFORMANCE",
            "CANDIDATE_PROMOTION",
        ],
        "source_stage_a_pair_budget": original_pair_budget,
        "frozen_stage_a_pair_budget": frozen_pair_budget,
        "unused_pair_budget_not_reallocated": original_pair_budget - frozen_pair_budget,
        "eligible_routes": eligible_routes,
        "excluded_routes": excluded_routes,
        "route_decisions": decisions,
        "selection_contract": {
            "source": "CN_DIVERSITY_ADMISSION.csv",
            "ordering": ["route_id", "admission_rank", "pair_id"],
            "one_exact_identity_one_vote": True,
            "performance_selection": "FORBIDDEN",
            "excluded_budget_reallocation": "FORBIDDEN",
            "natural_underfill_budget_reallocation": "FORBIDDEN",
        },
        "access_contract": {
            "development": "ONLY",
            "validation": "FORBIDDEN",
            "holdout": "FORBIDDEN",
            "forward_2026": "SEALED",
            "promotion": "FORBIDDEN",
            "cross_sprint_memory": "FORBIDDEN",
        },
        "strict_stage_a": "NOT_AUTHORIZED",
        "next_authorized_action": "PREPARE_FROZEN_STAGE_A_INPUTS_PENDING_SEPARATE_EXECUTION_AUTHORIZATION",
    }
    decision["decision_hash"] = _stable_hash(decision)
    return decision, selected


def freeze_stage_a_eligibility(
    *,
    repo_root: Path,
    plan_path: Path,
    runtime_root: Path,
    output_manifest: Path,
    output_pack: Path,
) -> dict[str, Any]:
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    waterfall_path = runtime_root / "CN_ADMISSION_WATERFALL.csv"
    admission_path = runtime_root / "CN_DIVERSITY_ADMISSION.csv"
    expressivity_path = runtime_root / "CN_GENERATOR_EXPRESSIVITY_AUDIT.json"
    expressivity = json.loads(expressivity_path.read_text(encoding="utf-8"))

    decision, selected = build_stage_a_eligibility(
        plan=plan,
        waterfall_rows=_read_csv(waterfall_path),
        admission_rows=_read_csv(admission_path),
        expressivity=expressivity,
    )
    _atomic_csv(output_pack, selected)
    decision["source_artifacts"] = [
        _artifact(plan_path, repo_root),
        _artifact(waterfall_path, repo_root),
        _artifact(admission_path, repo_root),
        _artifact(expressivity_path, repo_root),
    ]
    decision["stage_a_pack"] = {
        **_artifact(output_pack, repo_root),
        "pair_count": len(selected),
    }
    decision["manifest_hash"] = _stable_hash(decision)
    _atomic_json(output_manifest, decision)
    return decision


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Freeze a scoped Stage A route-eligibility manifest from existing evidence."
    )
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument(
        "--plan",
        type=Path,
        default=Path("runtime/run_plans/cn_compositional_nline_bounded_search_epoch1_v1.json"),
    )
    parser.add_argument(
        "--runtime-root",
        type=Path,
        default=Path("runtime/cn_compositional_nline_large_search_20260715"),
    )
    parser.add_argument(
        "--output-manifest",
        type=Path,
        default=Path("runtime/run_plans/cn_compositional_stage_a_route_eligibility_v1.json"),
    )
    parser.add_argument(
        "--output-pack",
        type=Path,
        default=Path(
            "runtime/cn_compositional_nline_large_search_20260715/"
            "CN_STAGE_A_ROUTE_ELIGIBLE_PACK.csv"
        ),
    )
    args = parser.parse_args()
    root = args.repo_root.resolve()

    def resolve(path: Path) -> Path:
        return path if path.is_absolute() else root / path

    decision = freeze_stage_a_eligibility(
        repo_root=root,
        plan_path=resolve(args.plan),
        runtime_root=resolve(args.runtime_root),
        output_manifest=resolve(args.output_manifest),
        output_pack=resolve(args.output_pack),
    )
    print(decision["status"])
    print(f"frozen_stage_a_pair_budget={decision['frozen_stage_a_pair_budget']}")
    print(f"excluded_routes={','.join(decision['excluded_routes'])}")
    print(f"strict_stage_a={decision['strict_stage_a']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
