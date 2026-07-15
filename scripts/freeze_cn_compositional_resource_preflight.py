"""Freeze a balanced, immutable 32-pair resource preflight pack.

This stage selects without labels or rewards.  It reconstructs every typed
pair from its proposal receipt, reauthorizes both members against the frozen
registry/data context, and writes separate native-clock evaluator packs.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from our_system_phase2.services.candidate_submission_receipt import (
    CandidateSubmissionAuthority,
    ReceiptContext,
    write_receipt_table,
)
from our_system_phase2.services.compositional_grammar import CompositionalGrammarV2
from our_system_phase2.services.fixed_split_authority import FixedSplitAuthority
from our_system_phase2.services.matched_control_pairs import (
    CandidatePairAuthority,
    write_pair_receipt_table,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)


REPO = Path(__file__).resolve().parents[1]
ACTIVE_ROUTES = frozenset(
    {
        "MINUTE_STATIC",
        "FIRSTN_PATH",
        "MARKET_REGIME_CONDITION",
        "INTRADAY_STATE_TRANSITION",
    }
)
SESSION_ROUTES = frozenset(
    {"SLOW_CROSS_SECTIONAL_LEVEL", "SLOW_TEMPORAL_CHANGE", "DISCLOSURE_EVENT"}
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _csv_value(value: Any) -> Any:
    if isinstance(value, (list, tuple, dict, set)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return value


def evaluator_expression_identity(candidate_id: str, expression: str) -> str:
    return hashlib.sha256((str(candidate_id) + "|" + str(expression)).encode("utf-8")).hexdigest()[:24]


def assign_evaluator_expression_identities(candidates: Sequence[dict[str, Any]]) -> None:
    """Keep legacy identities stable unless one expression belongs to several candidates."""
    expression_counts = Counter(str(row["expression"]) for row in candidates)
    for row in candidates:
        expression = str(row["expression"])
        row["expression_hash"] = (
            evaluator_expression_identity(str(row["candidate_id"]), expression)
            if expression_counts[expression] > 1
            else hashlib.sha256(expression.encode("utf-8")).hexdigest()[:24]
        )


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        raise ValueError(f"cannot write empty CSV: {path}")
    fields = sorted({str(key) for row in rows for key in row})
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: _csv_value(row.get(field, "")) for field in fields})


def representative_route_quotas(route_pair_quotas: Mapping[str, Any], total: int) -> dict[str, int]:
    routes = sorted(str(route) for route in route_pair_quotas)
    if total < len(routes):
        raise ValueError("resource preflight total must cover every route")
    base, remainder = divmod(int(total), len(routes))
    ranked = sorted(routes, key=lambda route: (-int(route_pair_quotas[route]), route))
    return {route: base + (1 if route in set(ranked[:remainder]) else 0) for route in routes}


def select_representative_pairs(
    rows: Iterable[Mapping[str, Any]],
    *,
    route_quotas: Mapping[str, int],
) -> list[dict[str, Any]]:
    pool = [dict(row) for row in rows]
    output: list[dict[str, Any]] = []
    for route_id in sorted(route_quotas):
        candidates = sorted(
            (row for row in pool if str(row.get("route_id") or "") == route_id),
            key=lambda row: str(row.get("candidate_id") or ""),
        )
        chosen: list[dict[str, Any]] = []
        seen: dict[str, set[str]] = {
            "policy_id": set(),
            "seed": set(),
            "skeleton_id": set(),
            "behavior_cluster_id": set(),
        }
        while len(chosen) < int(route_quotas[route_id]) and candidates:
            def score(row: Mapping[str, Any]) -> tuple[int, str]:
                novelty = sum(str(row.get(key) or "") not in seen[key] for key in seen)
                return (-novelty, str(row.get("candidate_id") or ""))

            winner = min(candidates, key=score)
            candidates.remove(winner)
            chosen.append(winner)
            for key in seen:
                seen[key].add(str(winner.get(key) or ""))
        if len(chosen) != int(route_quotas[route_id]):
            raise RuntimeError(f"natural underfill in resource preflight route {route_id}")
        output.extend(chosen)
    return output


def _candidate_rows(
    selected: Sequence[Mapping[str, Any]],
    *,
    compact_by_id: Mapping[str, Mapping[str, Any]],
    grammar: CompositionalGrammarV2,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates: list[dict[str, Any]] = []
    pack: list[dict[str, Any]] = []
    for ordinal, admission in enumerate(selected, 1):
        candidate_id = str(admission["candidate_id"])
        receipt = dict(compact_by_id[candidate_id])
        pair = grammar.propose(
            str(receipt["route_id"]),
            attempt_index=int(receipt["route_attempt_index"]),
            seed=int(receipt["seed"]),
        )
        if str(pair.primary["candidate_id"]) != candidate_id:
            raise RuntimeError(f"typed pair reconstruction candidate drift: {candidate_id}")
        if str(pair.primary["canonical_expression"]) != str(receipt["canonical_expression"]):
            raise RuntimeError(f"typed pair reconstruction expression drift: {candidate_id}")
        if str(pair.control["canonical_expression"]) != str(receipt["control_canonical_expression"]):
            raise RuntimeError(f"typed control reconstruction drift: {candidate_id}")
        for member in (pair.primary, pair.control):
            row = dict(member)
            # Formal evaluator identity is candidate-specific even when several
            # pairs reuse an identical neutral control.  Expression evaluation
            # remains shared by the expression-string cache, so this preserves
            # pair membership without duplicating materialization work.
            row["expression_hash"] = ""
            row["generator_arm"] = str(admission["policy_id"])
            row["run"] = "CN_COMPOSITIONAL_RESOURCE_PREFLIGHT"
            row["round_id"] = "RESOURCE_PREFLIGHT_32"
            candidates.append(row)
        pack.append(
            {
                "preflight_ordinal": ordinal,
                "candidate_id": candidate_id,
                "control_candidate_id": str(pair.control["candidate_id"]),
                "pair_id": str(pair.primary["pair_id"]),
                "route_id": str(admission["route_id"]),
                "skeleton_id": str(admission["skeleton_id"]),
                "policy_id": str(admission["policy_id"]),
                "seed": int(admission["seed"]),
                "behavior_cluster_id": int(admission["behavior_cluster_id"]),
                "clock_namespace": "active_bar" if str(admission["route_id"]) in ACTIVE_ROUTES else "stock_session",
                "performance_selected": False,
            }
        )
    assign_evaluator_expression_identities(candidates)
    return candidates, pack


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--release-manifest", type=Path, required=True)
    args = parser.parse_args()

    root = args.runtime_root.resolve()
    plan = json.loads(args.plan.read_text(encoding="utf-8"))
    release = json.loads(args.release_manifest.read_text(encoding="utf-8"))
    strict = dict(plan["strict_evaluation_contract"])
    if str(release.get("release_hash")) != str(strict["development_release_hash"]):
        raise RuntimeError("development release hash drift")
    if int(release["totals"]["rows"]) != int(strict["development_release_rows"]):
        raise RuntimeError("development release row-count drift")
    if release.get("forbidden_roles_present") or bool(release.get("forward_2026_present")):
        raise RuntimeError("development release contains a forbidden role")

    admissions = _read_csv(root / "CN_DIVERSITY_ADMISSION.csv")
    compact = _read_jsonl(root / "CN_PAIR_RECEIPTS.jsonl")
    compact_by_id = {str(row["candidate_id"]): row for row in compact}
    quotas = representative_route_quotas(plan["stage_a"]["route_pair_quotas"], int(plan["compute_contract"]["preflight_pairs"]))
    selected = select_representative_pairs(admissions, route_quotas=quotas)
    registry = UnifiedCapabilityRegistry.read(args.registry)
    candidates, pack = _candidate_rows(
        selected,
        compact_by_id=compact_by_id,
        grammar=CompositionalGrammarV2(registry),
    )

    split = FixedSplitAuthority.read(args.split_manifest)
    # ReceiptContext must match the formal evaluator's own authority envelope.
    # Its transitive implementation is pinned separately by the repo SHA and
    # artifact manifest; the evaluator receipt hash intentionally follows the
    # Phase3CM entrypoint contract used during validation.
    evaluator_paths = [
        REPO / "src/our_system_phase2/runtime/phase3cm_train_portfolio_sortino_reward_audit.py"
    ]
    authority = CandidateSubmissionAuthority(
        registry,
        ReceiptContext.build(
            registry=registry,
            split_authority=split,
            data_release_hash=str(release["release_hash"]),
            evaluator_paths=evaluator_paths,
        ),
    )
    candidate_receipts = authority.authorize_table(candidates)
    pair_receipts = CandidatePairAuthority().authorize_table(candidates, candidate_receipts)
    receipt_by_candidate = {str(row["candidate_id"]): row for row in candidate_receipts}
    pair_by_id = {str(row["pair_id"]): row for row in pair_receipts}

    _write_csv(root / "CN_RESOURCE_PREFLIGHT_PACK.csv", pack)
    artifacts: list[Path] = [root / "CN_RESOURCE_PREFLIGHT_PACK.csv"]
    for clock, routes in (("active", ACTIVE_ROUTES), ("session", SESSION_ROUTES)):
        members = [row for row in candidates if str(row["route_id"]) in routes]
        member_ids = {str(row["candidate_id"]) for row in members}
        group_receipts = [receipt_by_candidate[candidate_id] for candidate_id in sorted(member_ids)]
        group_pair_ids = {str(row["pair_id"]) for row in members}
        group_pair_receipts = [pair_by_id[pair_id] for pair_id in sorted(group_pair_ids)]
        candidate_path = root / f"preflight_{clock}_candidates.csv"
        receipt_path = root / f"preflight_{clock}_candidate_receipts.jsonl"
        pair_path = root / f"preflight_{clock}_pair_receipts.jsonl"
        _write_csv(candidate_path, members)
        write_receipt_table(receipt_path, group_receipts)
        write_pair_receipt_table(pair_path, group_pair_receipts)
        artifacts.extend((candidate_path, receipt_path, pair_path))

    freeze = {
        "status": "CN_COMPOSITIONAL_RESOURCE_PREFLIGHT_PACK_FROZEN",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "selection_used_performance": False,
        "data_role": "development",
        "validation_holdout_forward_read": False,
        "pair_count": len(pack),
        "evaluator_call_count": len(candidates),
        "route_quotas": quotas,
        "policy_counts": dict(Counter(row["policy_id"] for row in pack)),
        "seed_counts": dict(Counter(str(row["seed"]) for row in pack)),
        "clock_counts": dict(Counter(row["clock_namespace"] for row in pack)),
        "release_hash": str(release["release_hash"]),
        "release_rows": int(release["totals"]["rows"]),
        "source_rows_before_role_filter": int(release["totals"]["source_rows"]),
        "split_manifest_hash": split.manifest_hash,
        "registry_hash": registry.registry_hash,
        "pack_identity": stable_hash(pack),
        "artifacts": [
            {"path": str(path.relative_to(root)).replace("\\", "/"), "sha256": _sha256(path), "bytes": path.stat().st_size}
            for path in artifacts
        ],
    }
    _write_json(root / "CN_RESOURCE_PREFLIGHT_FREEZE.json", freeze)
    print(json.dumps(freeze, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
