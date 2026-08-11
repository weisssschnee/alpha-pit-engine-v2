"""Three-batch, development-only CN iterative-search V1 canary.

This is a bounded orchestration layer over the unified registry generator,
candidate/pair receipt authorities and the existing Phase3CM streaming
backend.  It is not a second search platform or evaluator.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import platform
import shutil
import statistics
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from our_system_phase2.services.project_control_admission import (
    consume_active_admission,
    verify_consumed_admission_target,
)

from our_system_phase2.runtime.phase3cn_feedback_memory_smoke import (
    build_iterative_feedback_views,
)
from our_system_phase2.services.candidate_submission_receipt import (
    CandidateSubmissionAuthority,
    ReceiptContext,
)
from our_system_phase2.services.evaluation_access_guard import GUARD_VERSION
from our_system_phase2.services.fixed_split_authority import FixedSplitAuthority, file_sha256
from our_system_phase2.services.matched_control_pairs import CandidatePairAuthority
from our_system_phase2.services.multi_arm_scheduler import build_route_schedule
from our_system_phase2.services.portfolio_behavior_archive import (
    PortfolioBehaviorArchive,
    bounded_label_free_behavior_probe,
)
from our_system_phase2.services.post_train_validation import (
    run_automatic_post_train_validation,
)
from our_system_phase2.services.unified_capability_registry import (
    ROUTE_IDS,
    UnifiedCapabilityRegistry,
    stable_hash,
)
from our_system_phase2.services.unified_discovery_generators import RegistryDrivenGenerator


REPO = Path(__file__).resolve().parents[3]
BATCH_COUNT = 3
PAIR_PROPOSAL_BUDGET = 48
PAIR_ADMISSION_BUDGET = 24
PER_ROUTE_CAP = 12
AUTHORIZED_HOST = "DESKTOP-77OPJ6F"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _json_value(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(list(value) if isinstance(value, (tuple, set)) else value, ensure_ascii=False, sort_keys=True)
    return value


def _write_json(path: Path, payload: Any) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(destination)
    return destination


def _write_jsonl(path: Path, rows: Sequence[Mapping[str, Any]]) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        "\n".join(json.dumps(dict(row), ensure_ascii=False, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )
    return destination


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)
    with destination.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _json_value(row.get(key, "")) for key in fields})
    return destination


def _write_parquet(path: Path, rows: Sequence[Mapping[str, Any]]) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    normalized = [
        {key: _json_value(value) for key, value in dict(row).items()}
        for row in rows
    ]
    frame = pd.DataFrame(normalized)
    for column in frame.select_dtypes(include=["object"]):
        inferred = pd.api.types.infer_dtype(frame[column], skipna=True)
        if not inferred.startswith("mixed"):
            continue
        frame[column] = frame[column].map(
            lambda value: (
                None
                if value is None or bool(pd.isna(value))
                else value
                if isinstance(value, str)
                else json.dumps(
                    value,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                )
            )
        )
    frame.to_parquet(destination, index=False)
    return destination


def _artifact(path: Path, *, root: Path | None = None) -> dict[str, Any]:
    source = Path(path)
    return {
        "path": str(source.relative_to(root)).replace("\\", "/") if root is not None else str(source),
        "sha256": _sha256(source),
        "bytes": source.stat().st_size,
    }


def _read_csv(path: Path) -> list[dict[str, str]]:
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _train_dates(split: FixedSplitAuthority) -> tuple[str, ...]:
    return tuple(row["trade_date"] for row in split.rows if row["split"] == "train")


def _clock_for_route(route_id: str) -> str:
    return (
        "active_bar"
        if route_id
        in {
            "MINUTE_STATIC",
            "FIRSTN_PATH",
            "MARKET_REGIME_CONDITION",
            "INTRADAY_STATE_TRANSITION",
        }
        else "stock_session"
    )


def _initial_prior_rows() -> list[dict[str, Any]]:
    weights = {
        "MINUTE_STATIC": 1.30,
        "SLOW_CROSS_SECTIONAL_LEVEL": 1.30,
        "INTRADAY_STATE_TRANSITION": 1.40,
        "FIRSTN_PATH": 0.75,
    }
    return [
        {
            "route_id": route_id,
            "actionable_support": 0,
            "initial_prior_weight": weights.get(route_id, 1.0),
        }
        for route_id in ROUTE_IDS
    ]


def _synthetic_feedback_proof() -> dict[str, Any]:
    neutral = [
        {
            "route_id": route_id,
            "actionable_support": 12,
            "positive_matched_density": 0.5,
            "median_matched_reward": 0.0,
            "new_signal_cluster_rate": 0.5,
            "new_portfolio_behavior_rate": 0.5,
            "cost_conversion_rate": 0.0,
            "turnover_killed_rate": 0.0,
            "behavior_duplicate_rate": 0.0,
            "semantic_wrong_lag_high_corr_rate": 0.0,
        }
        for route_id in ROUTE_IDS
    ]
    feedback = [dict(row) for row in neutral]
    feedback[0].update(
        positive_matched_density=0.9,
        median_matched_reward=0.2,
        new_signal_cluster_rate=0.9,
        new_portfolio_behavior_rate=0.9,
    )
    feedback[1].update(
        positive_matched_density=0.0,
        median_matched_reward=-0.2,
        cost_conversion_rate=0.8,
        turnover_killed_rate=0.8,
    )
    rows, _ = build_route_schedule(feedback, total_pairs=48, admission_pairs=24)
    by_route = {str(row["route_id"]): row for row in rows}
    proof = {
        "positive_rule": by_route["MINUTE_STATIC"]["scheduler_action"] == "EXPAND",
        "negative_rule": by_route["FIRSTN_PATH"]["scheduler_action"] in {"REPAIR", "DOWNWEIGHT"},
        "positive_budget_gt_negative_budget": int(by_route["MINUTE_STATIC"]["scheduled_pairs"])
        > int(by_route["FIRSTN_PATH"]["scheduled_pairs"]),
        "infrastructure_excluded_from_financial_route_health": True,
    }
    proof["status"] = "PASS" if all(bool(value) for value in proof.values()) else "FAIL"
    return proof


def _generate_master_stream(
    generator: RegistryDrivenGenerator,
    *,
    seed: int,
    historical_exact: set[str],
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    stream: dict[str, list[dict[str, Any]]] = {}
    funnel: list[dict[str, Any]] = []
    for route_ordinal, route_id in enumerate(ROUTE_IDS):
        rows, route_funnel = generator.generate_route_attempts(
            route_id,
            scheduled_pairs=PER_ROUTE_CAP,
            seed=int(seed + route_ordinal * 1009),
            attempt_limit=2000,
            existing_exact_identities=set(historical_exact),
        )
        stream[route_id] = rows
        funnel.append(route_funnel)
    return stream, funnel


def _select_proposal_pack(
    master_stream: Mapping[str, Sequence[Mapping[str, Any]]],
    schedule: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    pairs_by_route: dict[str, list[list[dict[str, Any]]]] = {}
    for route_id in ROUTE_IDS:
        rows = [dict(row) for row in master_stream.get(route_id, ())]
        pairs_by_route[route_id] = [rows[index : index + 2] for index in range(0, len(rows), 2)]
    selected: list[list[dict[str, Any]]] = []
    consumed: dict[str, int] = {}
    funnel = {
        route_id: {
            "route_id": route_id,
            "scheduled_pairs": int(next(row for row in schedule if row["route_id"] == route_id)["scheduled_pairs"]),
            "spillover_pairs": 0,
            "underfill_reason": "",
            "spillover_reason": "",
        }
        for route_id in ROUTE_IDS
    }
    for row in schedule:
        route_id = str(row["route_id"])
        wanted = int(row["scheduled_pairs"])
        available = pairs_by_route[route_id]
        take = min(wanted, len(available))
        selected.extend(available[:take])
        consumed[route_id] = take
        if take < wanted:
            funnel[route_id]["underfill_reason"] = "EXACT_UNIQUE_STREAM_UNDERFILL"
    shortfall = PAIR_PROPOSAL_BUDGET - len(selected)
    if shortfall > 0:
        for row in sorted(schedule, key=lambda item: (-int(item["scheduled_pairs"]), str(item["route_id"]))):
            route_id = str(row["route_id"])
            remaining = pairs_by_route[route_id][consumed.get(route_id, 0) :]
            take = min(shortfall, len(remaining))
            if take:
                selected.extend(remaining[:take])
                consumed[route_id] = consumed.get(route_id, 0) + take
                funnel[route_id]["spillover_pairs"] = take
                funnel[route_id]["spillover_reason"] = "CROSS_ROUTE_EXACT_UNIQUE_SHORTFALL_FILL"
                shortfall -= take
            if shortfall <= 0:
                break
    flat = [member for pair in selected for member in pair]
    return flat, funnel


def _legal_canonical_pair_distribution(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, int]:
    """Count actual legal/canonical matched pairs selected per registry route."""

    members_by_pair: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        members_by_pair[str(row.get("pair_id") or "")].append(row)
    counts = {route_id: 0 for route_id in ROUTE_IDS}
    for pair_id, members in members_by_pair.items():
        if not pair_id or len(members) != 2:
            raise RuntimeError(f"invalid selected matched pair membership: {pair_id!r}")
        routes = {str(member.get("route_id") or "") for member in members}
        if len(routes) != 1:
            raise RuntimeError(f"selected matched pair route drift: {pair_id}")
        legal = all(
            member.get("legal") is True
            or str(member.get("legal") or "").strip().lower() == "true"
            for member in members
        )
        canonical = all(str(member.get("canonical_identity") or "") for member in members)
        if legal and canonical:
            counts[next(iter(routes))] += 1
    return counts


def _causal_route_comparison(
    *,
    feedback_on_budgets: Mapping[str, int],
    feedback_off_budgets: Mapping[str, int],
    feedback_on_actual: Mapping[str, int],
    feedback_off_actual: Mapping[str, int],
    feedback_on_actions: Mapping[str, str],
    feedback_on_funnel: Mapping[str, Mapping[str, Any]],
    feedback_off_funnel: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    comparison: dict[str, dict[str, Any]] = {}
    applied_routes: list[str] = []
    clamped_routes: list[str] = []
    wrong_direction_routes: list[str] = []
    positive_statuses: list[str] = []
    negative_statuses: list[str] = []
    for route_id in ROUTE_IDS:
        budget_delta = int(feedback_on_budgets.get(route_id, 0)) - int(
            feedback_off_budgets.get(route_id, 0)
        )
        actual_delta = int(feedback_on_actual.get(route_id, 0)) - int(
            feedback_off_actual.get(route_id, 0)
        )
        action = str(feedback_on_actions.get(route_id) or "MAINTAIN")
        expected_direction = 1 if action == "EXPAND" else -1 if action == "DOWNWEIGHT" else 0
        on_funnel = dict(feedback_on_funnel.get(route_id) or {})
        off_funnel = dict(feedback_off_funnel.get(route_id) or {})
        supply_clamped = bool(
            on_funnel.get("underfill_reason")
            or off_funnel.get("underfill_reason")
            or int(feedback_on_actual.get(route_id, 0))
            < int(feedback_on_budgets.get(route_id, 0))
            or int(feedback_off_actual.get(route_id, 0))
            < int(feedback_off_budgets.get(route_id, 0))
        )
        if expected_direction == 0:
            exposure_status = (
                "SPILLOVER_ONLY_NOT_FEEDBACK" if actual_delta else "NO_FEEDBACK_ACTION"
            )
        elif actual_delta and (actual_delta > 0) == (expected_direction > 0):
            exposure_status = "APPLIED"
            applied_routes.append(route_id)
        elif actual_delta == 0 and supply_clamped:
            exposure_status = "ACTIONABLE_FEEDBACK_CLAMPED"
            clamped_routes.append(route_id)
        elif actual_delta == 0:
            exposure_status = "ACTIONABLE_FEEDBACK_NOT_EFFECTIVE"
            wrong_direction_routes.append(route_id)
        else:
            exposure_status = "ACTIONABLE_FEEDBACK_WRONG_DIRECTION"
            wrong_direction_routes.append(route_id)
        if expected_direction > 0:
            positive_statuses.append(exposure_status)
        elif expected_direction < 0:
            negative_statuses.append(exposure_status)
        comparison[route_id] = {
            "scheduler_action": action,
            "feedback_on_scheduled_pairs": int(feedback_on_budgets.get(route_id, 0)),
            "feedback_off_scheduled_pairs": int(feedback_off_budgets.get(route_id, 0)),
            "scheduled_pair_delta": budget_delta,
            "feedback_on_actual_legal_canonical_pairs": int(feedback_on_actual.get(route_id, 0)),
            "feedback_off_actual_legal_canonical_pairs": int(feedback_off_actual.get(route_id, 0)),
            "actual_pair_delta": actual_delta,
            "feedback_exposure_status": exposure_status,
            "supply_clamped": supply_clamped,
            "feedback_on_underfill_reason": str(on_funnel.get("underfill_reason") or ""),
            "feedback_off_underfill_reason": str(off_funnel.get("underfill_reason") or ""),
            "feedback_on_spillover_reason": str(on_funnel.get("spillover_reason") or ""),
            "feedback_off_spillover_reason": str(off_funnel.get("spillover_reason") or ""),
        }
    positive_direction_status = (
        "APPLIED"
        if "APPLIED" in positive_statuses
        else "ACTIONABLE_FEEDBACK_CLAMPED"
        if positive_statuses
        and all(status == "ACTIONABLE_FEEDBACK_CLAMPED" for status in positive_statuses)
        else "NO_ACTIONABLE_POSITIVE_FEEDBACK"
        if not positive_statuses
        else "ACTIONABLE_FEEDBACK_INVALID"
    )
    negative_direction_status = (
        "APPLIED"
        if "APPLIED" in negative_statuses
        else "ACTIONABLE_FEEDBACK_CLAMPED"
        if negative_statuses
        and all(status == "ACTIONABLE_FEEDBACK_CLAMPED" for status in negative_statuses)
        else "NO_ACTIONABLE_NEGATIVE_FEEDBACK"
        if not negative_statuses
        else "ACTIONABLE_FEEDBACK_INVALID"
    )
    gates: dict[str, Any] = {
        "scheduled_route_distribution_changed": any(
            row["scheduled_pair_delta"] != 0 for row in comparison.values()
        ),
        "actual_legal_canonical_route_distribution_changed": any(
            row["actual_pair_delta"] != 0 for row in comparison.values()
        ),
        "at_least_one_actionable_feedback_changes_actual_exposure": bool(applied_routes),
        "no_actionable_feedback_has_wrong_or_unexplained_direction": not wrong_direction_routes,
        "positive_direction_applied_or_clamped": positive_direction_status
        in {"APPLIED", "ACTIONABLE_FEEDBACK_CLAMPED", "NO_ACTIONABLE_POSITIVE_FEEDBACK"},
        "negative_direction_applied_or_clamped": negative_direction_status
        in {"APPLIED", "ACTIONABLE_FEEDBACK_CLAMPED", "NO_ACTIONABLE_NEGATIVE_FEEDBACK"},
        "maintain_spillover_excluded_from_feedback": all(
            row["scheduler_action"] != "MAINTAIN"
            or row["feedback_exposure_status"]
            in {"NO_FEEDBACK_ACTION", "SPILLOVER_ONLY_NOT_FEEDBACK"}
            for row in comparison.values()
        ),
    }
    gates["real_positive_direction_status"] = positive_direction_status
    gates["real_negative_direction_status"] = negative_direction_status
    return comparison, gates


def _probe_pack(
    *,
    candidate_rows: Sequence[Mapping[str, Any]],
    field_roots: Mapping[str, Path],
    train_dates: Sequence[str],
    coordinate_binding: str,
    batch_id: str,
    compute_threads: Mapping[str, int],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    records: list[dict[str, Any]] = []
    audits: list[dict[str, Any]] = []
    for route_id in ROUTE_IDS:
        backend = _clock_for_route(route_id)
        members = [
            dict(row)
            for row in candidate_rows
            if str(row.get("route_id") or "") == route_id
        ]
        if not members:
            continue
        if route_id == "DISCLOSURE_EVENT":
            max_trade_dates = 12
            max_trade_times = 1
            date_selection = "condition_activation"
            time_selection = "session_open_head"
        elif route_id == "FIRSTN_PATH":
            max_trade_dates = 4
            max_trade_times = 8
            date_selection = "calendar_stratified"
            time_selection = "intraday_stratified"
        else:
            max_trade_dates = 4
            max_trade_times = 8
            date_selection = "calendar_stratified"
            time_selection = "session_open_head"
        field_sidecars = tuple(sorted(Path(field_roots[backend]).glob("shard_*.parquet")))
        backend_records, audit = bounded_label_free_behavior_probe(
            candidates=members,
            field_sidecars=field_sidecars,
            eligible_trade_dates=train_dates,
            coordinate_binding=_stable_hash(
                {
                    "coordinate_binding": coordinate_binding,
                    "backend": backend,
                    "route_id": route_id,
                    "date_selection": date_selection,
                    "time_selection": time_selection,
                    "max_trade_dates": max_trade_dates,
                    "max_trade_times": max_trade_times,
                }
            ),
            batch_id=batch_id,
            compute_threads=int(compute_threads[backend]),
            max_trade_dates=max_trade_dates,
            max_trade_times=max_trade_times,
            date_selection=date_selection,
            time_selection=time_selection,
            pair_batch_size=8,
        )
        records.extend(backend_records)
        audits.append({"backend": backend, "route_id": route_id, **audit})
    order = {
        str(candidate_rows[index]["pair_id"]): index // 2
        for index in range(0, len(candidate_rows), 2)
    }
    records.sort(key=lambda row: order[str(row["pair_id"])])
    return records, audits


def _admit_pairs(
    *,
    candidate_rows: Sequence[Mapping[str, Any]],
    probe_rows: Sequence[Mapping[str, Any]],
    schedule: Sequence[Mapping[str, Any]],
    historical_archive: PortfolioBehaviorArchive,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    members_by_pair: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in candidate_rows:
        members_by_pair[str(row["pair_id"])].append(dict(row))
    route_budget = {str(row["route_id"]): int(row["admission_pair_budget"]) for row in schedule}
    admitted_ids: list[str] = []
    decisions: list[dict[str, Any]] = []
    seen_probe_ids: set[str] = set()
    eligible_by_route: dict[str, list[str]] = defaultdict(list)
    for raw in probe_rows:
        row = dict(raw)
        probe_id = str(row.get("behavior_probe_id") or "")
        member_duplicate = (
            bool(row.get("primary_behavior_probe_id"))
            and row.get("primary_behavior_probe_id") == row.get("control_behavior_probe_id")
        )
        if str(row.get("behavior_status")) != "RESOLVED":
            decision, reason = "REJECT", "BEHAVIOR_UNRESOLVED"
        elif member_duplicate or historical_archive.contains_probe(probe_id) or probe_id in seen_probe_ids:
            decision, reason = "REJECT", "EXACT_BEHAVIOR_DUPLICATE"
        else:
            decision, reason = "ELIGIBLE", "LABEL_FREE_BEHAVIOR_UNIQUE"
            seen_probe_ids.add(probe_id)
            eligible_by_route[str(row["route_id"])].append(str(row["pair_id"]))
        decisions.append({**row, "admission_decision": decision, "admission_reason": reason})
    for route_id in ROUTE_IDS:
        admitted_ids.extend(eligible_by_route[route_id][: route_budget.get(route_id, 0)])
    if len(admitted_ids) < PAIR_ADMISSION_BUDGET:
        already = set(admitted_ids)
        for row in probe_rows:
            pair_id = str(row["pair_id"])
            decision = next(item for item in decisions if item["pair_id"] == pair_id)
            if decision["admission_decision"] == "ELIGIBLE" and pair_id not in already:
                admitted_ids.append(pair_id)
                already.add(pair_id)
                decision["admission_reason"] = "BEHAVIOR_UNIQUE_SPILLOVER_FILL"
                if len(admitted_ids) >= PAIR_ADMISSION_BUDGET:
                    break
    admitted_set = set(admitted_ids[:PAIR_ADMISSION_BUDGET])
    for decision in decisions:
        if str(decision["pair_id"]) in admitted_set:
            decision["admission_decision"] = "ADMIT"
    admitted = [
        member
        for row in candidate_rows
        if str(row["pair_id"]) in admitted_set
        for member in [dict(row)]
    ]
    return admitted, decisions


def _context_and_binding(
    *,
    batch_root: Path,
    candidates: Sequence[Mapping[str, Any]],
    registry: UnifiedCapabilityRegistry,
    split: FixedSplitAuthority,
    data_release_hash: str,
    evaluation_role: str = "train",
) -> tuple[Path, dict[str, Path]]:
    if evaluation_role not in {"train", "validation", "holdout"}:
        raise ValueError(f"unsupported evaluation role: {evaluation_role}")
    evaluator_paths = (
        REPO / "scripts" / "run_cn_phase3cm_streaming_qualification.py",
        REPO / "src" / "our_system_phase2" / "services" / "phase3cm_streaming_portfolio.py",
        REPO / "src" / "our_system_phase2" / "services" / "portfolio_behavior_archive.py",
    )
    context = ReceiptContext.build(
        registry=registry,
        split_authority=split,
        data_release_hash=data_release_hash,
        evaluator_paths=evaluator_paths,
    )
    candidate_rows = [dict(row) for row in candidates]
    receipts = CandidateSubmissionAuthority(registry, context).authorize_table(candidate_rows)
    pair_receipts = CandidatePairAuthority().authorize_table(candidate_rows, receipts)
    receipt_by_id = {str(row["candidate_id"]): row for row in receipts}
    pair_receipt_by_id = {str(row["pair_id"]): row for row in pair_receipts}
    table_paths: dict[str, Path] = {}
    csv_rows_by_backend: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in candidate_rows:
        receipt = receipt_by_id[str(row["candidate_id"])]
        csv_rows_by_backend[_clock_for_route(str(row["route_id"]))].append(
            {
                **row,
                "canonical_expression": receipt["canonical_expression"],
                "field_ids": receipt["field_ids"],
                "clock_namespace": _clock_for_route(str(row["route_id"])),
            }
        )
    for backend, rows in csv_rows_by_backend.items():
        table_paths[backend] = _write_csv(batch_root / f"phase3cm_{backend}_candidates.csv", rows)
    receipt_path = _write_jsonl(batch_root / "candidate_receipts.jsonl", receipts)
    pair_receipt_path = _write_jsonl(batch_root / "pair_receipts.jsonl", pair_receipts)
    artifacts = [
        _artifact(path, root=batch_root)
        for path in [*table_paths.values(), receipt_path, pair_receipt_path]
    ]
    members = []
    for row in candidate_rows:
        receipt = receipt_by_id[str(row["candidate_id"])]
        members.append(
            {
                "pair_id": str(row["pair_id"]),
                "pair_member_role": str(row["pair_member_role"]),
                "candidate_id": str(row["candidate_id"]),
                "route_id": str(row["route_id"]),
                "clock_namespace": _clock_for_route(str(row["route_id"])),
                "expression": str(row["expression"]),
                "canonical_expression": str(receipt["canonical_expression"]),
                "receipt_hash": str(receipt["receipt_hash"]),
                "field_ids": list(receipt["field_ids"]),
            }
        )
    pairs = []
    for index in range(0, len(candidate_rows), 2):
        primary, control = candidate_rows[index], candidate_rows[index + 1]
        pair_receipt = pair_receipt_by_id[str(primary["pair_id"])]
        pairs.append(
            {
                "pair_id": str(primary["pair_id"]),
                "candidate_id": str(primary["candidate_id"]),
                "control_candidate_id": str(control["candidate_id"]),
                "route_id": str(primary["route_id"]),
                "clock_namespace": _clock_for_route(str(primary["route_id"])),
                "pair_receipt_hash": str(pair_receipt["pair_receipt_hash"]),
            }
        )
    binding: dict[str, Any] = {
        "status": "CN_STREAMING_REPAIR_FROZEN_INPUT_BOUND",
        "data_role": (
            "development" if evaluation_role == "train" else f"{evaluation_role}_report_only"
        ),
        "evaluation_role": evaluation_role,
        "source_closure_sha": data_release_hash,
        "development_release_hash": data_release_hash,
        "split_manifest_hash": split.manifest_hash,
        "registry_hash": registry.registry_hash,
        "pair_count": len(pairs),
        "candidate_member_count": len(members),
        "clock_counts": {
            backend: sum(1 for row in pairs if row["clock_namespace"] == backend)
            for backend in ("active_bar", "stock_session")
        },
        "artifacts": artifacts,
        "pairs": pairs,
        "candidate_members": members,
        "sealed_reads": {
            "train": {"validation": 0, "holdout": 0, "forward_2026": 0},
            "validation": {"holdout": 0, "forward_2026": 0},
            "holdout": {"forward_2026": 0},
        }[evaluation_role],
        "promotion": "FORBIDDEN",
        "cross_sprint_memory": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
        "evaluation_name": (
            "full-coordinate development Phase3CM pair evaluation"
            if evaluation_role == "train"
            else f"frozen-candidate report-only {evaluation_role}"
        ),
        "feedback_write": "FORBIDDEN" if evaluation_role != "train" else "TRAIN_ONLY",
        "scheduler_write": "FORBIDDEN" if evaluation_role != "train" else "CAMPAIGN_LOCAL",
        "archive_write": "FORBIDDEN" if evaluation_role != "train" else "TRAIN_ONLY",
    }
    binding["binding_hash"] = _stable_hash(binding)
    binding_path = _write_json(
        batch_root
        / (
            "phase3cm_input_binding.json"
            if evaluation_role == "train"
            else f"phase3cm_{evaluation_role}_input_binding.json"
        ),
        binding,
    )
    return binding_path, table_paths


def _run_phase3cm(
    *,
    batch_id: str,
    batch_root: Path,
    binding_path: Path,
    table_paths: Mapping[str, Path],
    split_manifest: Path,
    field_roots: Mapping[str, Path],
    label_roots: Mapping[str, Path],
    compute_threads: Mapping[str, int],
    evaluation_role: str = "train",
) -> list[dict[str, Any]]:
    receipts: list[dict[str, Any]] = []
    for backend in ("active_bar", "stock_session"):
        candidate_table = table_paths.get(backend)
        if candidate_table is None:
            continue
        pair_count = len({row["pair_id"] for row in _read_csv(candidate_table)})
        output_namespace = (
            "phase3cm"
            if evaluation_role == "train"
            else f"phase3cm_{evaluation_role}"
        )
        output_root = batch_root / output_namespace / backend
        result_path = output_root / "CN_STREAMING_BACKEND_RESULT.json"
        if result_path.exists():
            reused = json.loads(result_path.read_text(encoding="utf-8"))
            binding = json.loads(binding_path.read_text(encoding="utf-8"))
            if (
                str(reused.get("status")) != "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED"
                or str(reused.get("input_binding_hash")) != str(binding.get("binding_hash"))
                or int(reused.get("pair_count") or 0) != pair_count
                or str(reused.get("evaluation_role") or "train") != evaluation_role
            ):
                raise RuntimeError(f"existing Phase3CM result identity drift on {backend}")
            receipt = {
                "backend": backend,
                "command": "REUSED_COMPLETED_IDENTICAL_RESULT",
                "returncode": 0,
                "started_at": "REUSED",
                "completed_at": "REUSED",
                "result_path": str(result_path),
                "result_sha256": _sha256(result_path),
                "status": "COMPLETED_REUSED",
            }
            _write_json(output_root / "ITERATIVE_ACCESS_RECEIPT.json", receipt)
            receipts.append(receipt)
            continue
        command = [
            sys.executable,
            str(REPO / "scripts" / "run_cn_phase3cm_streaming_qualification.py"),
            "--backend", backend,
            "--evaluation-role", evaluation_role,
            "--phase", "D",
            "--pair-count", str(pair_count),
            "--candidate-table", str(candidate_table),
            "--binding", str(binding_path),
            "--split-manifest", str(split_manifest),
            "--artifact-root", str(batch_root),
            "--field-sidecar-root", str(field_roots[backend]),
            "--label-sidecar-root", str(label_roots[backend]),
            "--output-root", str(output_root),
            "--block-sessions", "10",
            "--pair-batch-size", "8",
            "--compute-threads", str(compute_threads[backend]),
            "--iterative-batch-id", batch_id,
        ]
        if (output_root / "CN_STREAMING_CHECKPOINT.json").is_file():
            command.append("--resume")
        environment = dict(os.environ)
        environment.update(
            {
                "PYTHONPATH": str(REPO / "src"),
                "NUMBA_NUM_THREADS": str(compute_threads[backend]),
                "ARROW_NUM_THREADS": "1",
                "OMP_NUM_THREADS": "1",
                "MKL_NUM_THREADS": "1",
                "OPENBLAS_NUM_THREADS": "1",
                "NUMEXPR_MAX_THREADS": "1",
                "POLARS_MAX_THREADS": "1",
            }
        )
        started = pd.Timestamp.now("UTC")
        process = subprocess.run(command, cwd=REPO, env=environment, text=True, capture_output=True)
        output_root.mkdir(parents=True, exist_ok=True)
        (output_root / "stdout.log").write_text(process.stdout or "", encoding="utf-8")
        (output_root / "stderr.log").write_text(process.stderr or "", encoding="utf-8")
        receipt = {
            "backend": backend,
            "command": command,
            "returncode": process.returncode,
            "started_at": started.isoformat(),
            "completed_at": pd.Timestamp.now("UTC").isoformat(),
            "result_path": str(result_path),
            "result_sha256": _sha256(result_path) if result_path.exists() else "",
            "status": "COMPLETED" if process.returncode == 0 and result_path.exists() else "INFRASTRUCTURE_FAILURE",
        }
        _write_json(output_root / "ITERATIVE_ACCESS_RECEIPT.json", receipt)
        receipts.append(receipt)
        if receipt["status"] != "COMPLETED":
            raise RuntimeError(f"Phase3CM infrastructure failure on {backend}; see {output_root}")
    return receipts


def _run_automatic_validation_after_train(
    *,
    train_manifest_path: Path,
    protected_train_artifacts: Sequence[Path],
    validation_root: Path,
    candidates: Sequence[Mapping[str, Any]],
    registry: UnifiedCapabilityRegistry,
    split: FixedSplitAuthority,
    validation_data_release_hash: str,
    split_manifest: Path,
    validation_field_roots: Mapping[str, Path],
    validation_label_roots: Mapping[str, Path],
    compute_threads: Mapping[str, int],
) -> dict[str, Any]:
    """Launch validation immediately after the immutable train closure."""

    validation_root = Path(validation_root).resolve()

    def runner() -> dict[str, Any]:
        binding_path, table_paths = _context_and_binding(
            batch_root=validation_root,
            candidates=candidates,
            registry=registry,
            split=split,
            data_release_hash=validation_data_release_hash,
            evaluation_role="validation",
        )
        receipts = _run_phase3cm(
            batch_id="post_train_validation",
            batch_root=validation_root,
            binding_path=binding_path,
            table_paths=table_paths,
            split_manifest=split_manifest,
            field_roots=validation_field_roots,
            label_roots=validation_label_roots,
            compute_threads=compute_threads,
            evaluation_role="validation",
        )
        results = []
        for backend in ("active_bar", "stock_session"):
            path = validation_root / "phase3cm_validation" / backend / "CN_STREAMING_BACKEND_RESULT.json"
            if path.is_file():
                results.append(json.loads(path.read_text(encoding="utf-8")))
        if not results:
            raise RuntimeError("AUTOMATIC_VALIDATION_PRODUCED_NO_BACKEND_RESULT")
        return {
            "status": "VALIDATION_COMPLETE",
            "evaluation_role": "validation",
            "validation_usage": "report_only",
            "validation_reads": sum(int(row.get("validation_reads") or 0) for row in results),
            "holdout_reads": sum(int(row.get("holdout_reads") or 0) for row in results),
            "forward_2026_reads": sum(int(row.get("forward_2026_reads") or 0) for row in results),
            "feedback_write": "FORBIDDEN",
            "scheduler_write": "FORBIDDEN",
            "archive_write": "FORBIDDEN",
            "promotion": "FORBIDDEN",
            "backend_results": [
                {
                    "backend": str(row.get("backend") or ""),
                    "pair_count": int(row.get("pair_count") or 0),
                    "result_path": str(
                        validation_root
                        / "phase3cm_validation"
                        / str(row.get("backend") or "")
                        / "CN_STREAMING_BACKEND_RESULT.json"
                    ),
                }
                for row in results
            ],
            "access_receipts": receipts,
        }

    return run_automatic_post_train_validation(
        train_manifest_path=train_manifest_path,
        validation_output_root=validation_root,
        protected_train_artifacts=protected_train_artifacts,
        validation_runner=runner,
    )


def _outcome_rows(batch_root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    outcomes: list[dict[str, Any]] = []
    full_behavior_rows: list[dict[str, Any]] = []
    for backend in ("active_bar", "stock_session"):
        result_path = batch_root / "phase3cm" / backend / "CN_STREAMING_BACKEND_RESULT.json"
        if not result_path.exists():
            continue
        result = json.loads(result_path.read_text(encoding="utf-8"))
        atom_rows = _read_csv(batch_root / "phase3cm" / backend / "CN_STREAMING_REWARD_ATOMS.csv")
        totals: dict[str, dict[str, float]] = defaultdict(lambda: {"raw": 0.0, "net": 0.0})
        for atom in atom_rows:
            candidate_id = str(atom["candidate_id"])
            totals[candidate_id]["raw"] += float(atom.get("raw_return_sum") or 0.0)
            totals[candidate_id]["net"] += float(atom.get("net_return_sum") or 0.0)
        reward_by_id = {str(row["candidate_id"]): dict(row) for row in result["candidate_rewards"]}
        for pair in result["pair_results"]:
            row = dict(pair)
            primary_id = str(row["primary_candidate_id"])
            control_id = str(row["control_candidate_id"])
            primary = totals[primary_id]
            control = totals[control_id]
            gross = primary["raw"] - control["raw"]
            net = primary["net"] - control["net"]
            cost_difference = (primary["raw"] - primary["net"]) - (control["raw"] - control["net"])
            reward = reward_by_id.get(primary_id, {})
            row.update(
                {
                    "matched_gross_increment": gross,
                    "matched_net_increment": net,
                    "matched_trading_cost_difference": cost_difference,
                    "matched_train_increment": row.get("pair_train_reward"),
                    "pair_turnover_metric": reward.get("pair_turnover_metric"),
                    "pair_train_reward_blockers": row.get("pair_evaluation_blockers"),
                    "feedback_data_role": "development",
                    "evaluation_access_guard": GUARD_VERSION,
                }
            )
            outcomes.append(row)
        behavior_path = Path(str(result["portfolio_behavior_archive"]["path"]))
        full_behavior_rows.extend(pd.read_parquet(behavior_path).fillna("").to_dict(orient="records"))
    return outcomes, full_behavior_rows


def _join_full_behavior_identities(
    full_behavior_rows: Sequence[Mapping[str, Any]],
    probe_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Close all four identities on the immutable post-Phase3CM behavior row."""

    probe_by_pair = {str(row.get("pair_id") or ""): row for row in probe_rows}
    joined: list[dict[str, Any]] = []
    for source in full_behavior_rows:
        row = dict(source)
        pair_id = str(row.get("pair_id") or "")
        probe = probe_by_pair.get(pair_id)
        if probe is None:
            raise RuntimeError(f"full behavior row has no bounded probe binding: {pair_id}")
        for identity_key in ("structural_family_id", "signal_cluster_id"):
            probe_value = str(probe.get(identity_key) or "")
            full_value = str(row.get(identity_key) or "")
            if full_value and probe_value and full_value != probe_value:
                raise RuntimeError(
                    f"full/probe {identity_key} drift for {pair_id}: "
                    f"{full_value} != {probe_value}"
                )
            row[identity_key] = full_value or probe_value
        if str(row.get("behavior_status") or "") == "RESOLVED":
            required = (
                "structural_family_id",
                "signal_cluster_id",
                "portfolio_behavior_signature_id",
                "portfolio_behavior_family_id",
            )
            missing = [key for key in required if not str(row.get(key) or "")]
            if missing:
                raise RuntimeError(
                    f"resolved full behavior row missing four-identity closure for "
                    f"{pair_id}: {missing}"
                )
        row["identity_join_authority"] = "PAIR_ID_BOUND_BOUNDED_PROBE_TO_FULL_PHASE3CM"
        joined.append(row)
    return joined


def _route_health(
    *,
    outcomes: Sequence[Mapping[str, Any]],
    ledger: Sequence[Mapping[str, Any]],
    positive: Sequence[Mapping[str, Any]],
    negative: Sequence[Mapping[str, Any]],
    admission_rows: Sequence[Mapping[str, Any]],
    full_behavior_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    positive_ids = {str(row["pair_id"]) for row in positive}
    negatives_by_pair = {str(row["pair_id"]): set(row.get("negative_labels") or ()) for row in negative}
    outcomes_by_route: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in outcomes:
        outcomes_by_route[str(row.get("route_id") or "")].append(row)
    admission_by_pair = {str(row["pair_id"]): row for row in admission_rows}
    full_by_pair = {str(row["pair_id"]): row for row in full_behavior_rows}
    total = max(1, len(outcomes))
    health: list[dict[str, Any]] = []
    for route_id in ROUTE_IDS:
        items = outcomes_by_route.get(route_id, [])
        rewards = [float(row["pair_train_reward"]) for row in items if row.get("pair_train_reward") not in (None, "")]
        pair_ids = [str(row["pair_id"]) for row in items]
        labels = [label for pair_id in pair_ids for label in negatives_by_pair.get(pair_id, set())]
        admitted_probe = [admission_by_pair[pair_id] for pair_id in pair_ids if pair_id in admission_by_pair]
        full = [full_by_pair[pair_id] for pair_id in pair_ids if pair_id in full_by_pair]
        support = len(items)
        health.append(
            {
                "route_id": route_id,
                "actionable_support": support,
                "positive_matched_density": sum(pair_id in positive_ids for pair_id in pair_ids) / max(1, support),
                "median_matched_reward": statistics.median(rewards) if rewards else 0.0,
                "new_signal_cluster_rate": len({str(row.get("signal_cluster_id") or "") for row in admitted_probe if row.get("signal_cluster_id")}) / max(1, len(admitted_probe)),
                "new_portfolio_behavior_rate": len({str(row.get("portfolio_behavior_family_id") or "") for row in full if row.get("portfolio_behavior_family_id")}) / max(1, len(full)),
                "cost_conversion_rate": labels.count("COST_KILLED") / max(1, support),
                "turnover_killed_rate": labels.count("TURNOVER_KILLED") / max(1, support),
                "behavior_duplicate_rate": sum(str(row.get("admission_reason")) == "EXACT_BEHAVIOR_DUPLICATE" for row in admission_rows if str(row.get("route_id")) == route_id) / max(1, sum(str(row.get("route_id")) == route_id for row in admission_rows)),
                "exact_behavior_duplicate_rate": labels.count("BEHAVIOR_DUPLICATE") / max(1, support),
                "semantic_wrong_lag_high_corr_rate": labels.count("SEMANTIC_BLOCKED") / max(1, support),
                "route_concentration": support / total,
            }
        )
    return health


def _batch_manifest(
    *,
    batch_root: Path,
    batch_id: str,
    input_hashes: Mapping[str, str],
    paths: Sequence[Path],
    access_receipts: Sequence[Mapping[str, Any]],
    evaluation_name: str = "full-coordinate development Phase3CM pair evaluation",
) -> Path:
    artifacts = [_artifact(path, root=batch_root) for path in paths]
    manifest: dict[str, Any] = {
        "schema_version": "cn_iterative_search_v1_batch_manifest_v1",
        "batch_id": batch_id,
        "status": "BATCH_CLOSED_IMMUTABLE",
        "input_hashes": dict(input_hashes),
        "artifacts": artifacts,
        "access_receipts": [dict(row) for row in access_receipts],
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "evaluation_name": str(evaluation_name),
    }
    manifest["manifest_payload_hash"] = _stable_hash(manifest)
    return _write_json(batch_root / "batch_manifest.json", manifest)


def run(args: argparse.Namespace) -> dict[str, Any]:
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError(
            f"full materialization and Phase3CM are authorized only on 77o ({AUTHORIZED_HOST}); host={platform.node()}"
        )
    registry = UnifiedCapabilityRegistry.read(args.registry.resolve())
    split = FixedSplitAuthority.read(args.split_manifest.resolve())
    train_dates = _train_dates(split)
    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    field_roots = {
        "active_bar": args.active_field_root.resolve(),
        "stock_session": args.session_field_root.resolve(),
    }
    label_roots = {
        "active_bar": args.active_label_root.resolve(),
        "stock_session": args.session_label_root.resolve(),
    }
    compute_threads = {"active_bar": int(args.active_threads), "stock_session": int(args.session_threads)}
    data_release_hash = _sha256(args.sidecar_closure.resolve())
    seeds = [int(args.seed_base + index * 100_003) for index in range(BATCH_COUNT)]
    synthetic_proof = _synthetic_feedback_proof()
    contract = {
        "schema_version": "cn_iterative_search_v1_frozen_contract_v1",
        "batch_count": BATCH_COUNT,
        "batch_seeds": seeds,
        "proposal_rows_per_batch_max": PAIR_PROPOSAL_BUDGET * 2,
        "proposal_pairs_per_batch_max": PAIR_PROPOSAL_BUDGET,
        "phase3cm_pairs_per_batch_max": PAIR_ADMISSION_BUDGET,
        "registry_path": str(args.registry.resolve()),
        "registry_hash": registry.registry_hash,
        "compiler_authority": "TypedRouteCompiler via CandidateSubmissionAuthority",
        "generator_authority": "RegistryDrivenGenerator",
        "scheduler_top_level_key": "unified registry route_id",
        "legacy_default_arm_profiles": "COMPATIBILITY_ONLY",
        "data_release_hash": data_release_hash,
        "split_manifest_hash": split.manifest_hash,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "cross_sprint_memory": "FORBIDDEN",
        "full_materialization_host": AUTHORIZED_HOST,
        "evaluation_name": "full-coordinate development Phase3CM pair evaluation",
    }
    frozen_contract_path = _write_json(output_root / "frozen_contract.json", contract)

    generator = RegistryDrivenGenerator(registry)
    behavior_archive = PortfolioBehaviorArchive()
    historical_exact: set[str] = set()
    schedule_history: list[dict[str, Any]] = []
    cumulative_ledger: list[dict[str, Any]] = []
    cumulative_positive: list[dict[str, Any]] = []
    cumulative_negative: list[dict[str, Any]] = []
    cumulative_health: list[dict[str, Any]] = []
    previous_health = _initial_prior_rows()
    previous_manifest: Path | None = None
    batch_summaries: list[dict[str, Any]] = []
    batch_manifests: list[Path] = []
    feedback_off_summary: dict[str, Any] = {}

    for batch_index in range(BATCH_COUNT):
        batch_id = f"batch_{batch_index:03d}"
        batch_root = output_root / batch_id
        batch_root.mkdir(parents=True, exist_ok=True)
        input_hashes = {
            "frozen_contract": _sha256(frozen_contract_path),
            "prior_batch_manifest": _sha256(previous_manifest) if previous_manifest else "GENESIS",
        }
        schedule, schedule_summary = build_route_schedule(
            previous_health,
            total_pairs=PAIR_PROPOSAL_BUDGET,
            admission_pairs=PAIR_ADMISSION_BUDGET,
        )
        for row in schedule:
            row["batch_id"] = batch_id
        schedule_path = _write_parquet(batch_root / "schedule.parquet", schedule)
        schedule_summary_path = _write_json(batch_root / "schedule_summary.json", schedule_summary)
        schedule_history.extend(schedule)

        master_stream, generation_funnel = _generate_master_stream(
            generator,
            seed=seeds[batch_index],
            historical_exact=historical_exact,
        )
        master_stream_identity = _stable_hash(
            {
                "seed": seeds[batch_index],
                "registry_hash": registry.registry_hash,
                "historical_exact_identity_hash": _stable_hash(sorted(historical_exact)),
                "routes": {
                    route_id: [
                        {
                            "candidate_id": str(row.get("candidate_id") or ""),
                            "pair_id": str(row.get("pair_id") or ""),
                            "pair_member_role": str(row.get("pair_member_role") or ""),
                            "generation_attempt_index": int(row.get("generation_attempt_index") or 0),
                            "exact_identity": str(row.get("exact_identity") or ""),
                        }
                        for row in master_stream[route_id]
                    ]
                    for route_id in ROUTE_IDS
                },
            }
        )
        master_stream_receipt_path = _write_json(
            batch_root / "generation_master_stream_receipt.json",
            {
                "schema_version": "cn_iterative_search_v1_master_attempt_stream_v1",
                "batch_id": batch_id,
                "seed": seeds[batch_index],
                "master_stream_hash": master_stream_identity,
                "historical_exact_identity_hash": _stable_hash(sorted(historical_exact)),
                "registry_hash": registry.registry_hash,
                "compiler_authority": "TypedRouteCompiler",
                "generator_authority": "RegistryDrivenGenerator",
                "route_member_counts": {
                    route_id: len(master_stream[route_id]) for route_id in ROUTE_IDS
                },
            },
        )
        proposal_rows, selection_funnel = _select_proposal_pack(master_stream, schedule)
        proposal_path = _write_csv(batch_root / "candidate_attempt_stream.csv", proposal_rows)
        archive_before = PortfolioBehaviorArchive(behavior_archive.rows)
        probe_binding = _stable_hash(
            {
                "batch_id": batch_id,
                "seed": seeds[batch_index],
                "registry_hash": registry.registry_hash,
                "split_manifest_hash": split.manifest_hash,
                "field_roots": {key: str(value) for key, value in field_roots.items()},
                "probe_trade_times": 30,
            }
        )
        probe_rows, probe_audits = _probe_pack(
            candidate_rows=proposal_rows,
            field_roots=field_roots,
            train_dates=train_dates,
            coordinate_binding=probe_binding,
            batch_id=batch_id,
            compute_threads=compute_threads,
        )
        admitted, admission_rows = _admit_pairs(
            candidate_rows=proposal_rows,
            probe_rows=probe_rows,
            schedule=schedule,
            historical_archive=archive_before,
        )
        control_contract = {
            "seed": seeds[batch_index],
            "master_stream_hash": master_stream_identity,
            "total_pair_budget": PAIR_PROPOSAL_BUDGET,
            "historical_exact_identity_hash": _stable_hash(sorted(historical_exact)),
            "historical_behavior_archive_hash": _stable_hash(archive_before.rows),
            "exact_dedupe_policy": "MASTER_STREAM_EXACT_IDENTITY_V1",
            "behavior_dedupe_policy": "LABEL_FREE_EXACT_PROBE_ID_V1",
            "registry_hash": registry.registry_hash,
            "compiler_authority": "TypedRouteCompiler",
            "generator_authority": "RegistryDrivenGenerator",
            "probe_coordinate_binding": probe_binding,
        }
        for row in probe_rows:
            if str(row.get("behavior_status") or "") == "RESOLVED":
                behavior_archive.add(dict(row))
        probe_path = _write_parquet(batch_root / "behavior_probe.parquet", probe_rows)
        admission_path = _write_parquet(batch_root / "admission_decisions.parquet", admission_rows)
        probe_audit_path = _write_json(batch_root / "behavior_probe_audit.json", probe_audits)

        if batch_index == 1:
            off_root = output_root / "batch_001_feedback_off"
            off_root.mkdir(parents=True, exist_ok=True)
            off_schedule, off_summary = build_route_schedule(
                _initial_prior_rows(),
                total_pairs=PAIR_PROPOSAL_BUDGET,
                admission_pairs=PAIR_ADMISSION_BUDGET,
            )
            off_proposals, off_selection_funnel = _select_proposal_pack(
                master_stream, off_schedule
            )
            off_probe, off_probe_audit = _probe_pack(
                candidate_rows=off_proposals,
                field_roots=field_roots,
                train_dates=train_dates,
                coordinate_binding=probe_binding,
                batch_id="batch_001_feedback_off",
                compute_threads=compute_threads,
            )
            off_admitted, off_decisions = _admit_pairs(
                candidate_rows=off_proposals,
                probe_rows=off_probe,
                schedule=off_schedule,
                historical_archive=archive_before,
            )
            _write_parquet(off_root / "schedule.parquet", off_schedule)
            _write_csv(off_root / "candidate_attempt_stream.csv", off_proposals)
            _write_parquet(off_root / "behavior_probe.parquet", off_probe)
            _write_parquet(off_root / "admission_decisions.parquet", off_decisions)
            off_manifest = {
                "status": "BATCH_1_FEEDBACK_OFF_PROPOSAL_CONTROL_COMPLETED",
                "seed": seeds[batch_index],
                "total_budget": PAIR_PROPOSAL_BUDGET,
                "registry_hash": registry.registry_hash,
                "compiler_authority": "SAME_AS_FEEDBACK_ON",
                "candidate_attempt_stream": "SAME_MASTER_STREAM_AS_FEEDBACK_ON",
                "master_stream_hash": master_stream_identity,
                "historical_behavior_archive_hash": _stable_hash(archive_before.rows),
                "historical_exact_identity_hash": _stable_hash(sorted(historical_exact)),
                "exact_behavior_dedupe": "SAME_AS_FEEDBACK_ON",
                "control_contract": control_contract,
                "phase3cm_evaluation": "NOT_RUN_BY_CONTRACT",
                "memory_update": "FORBIDDEN",
                "proposal_pairs": len(off_proposals) // 2,
                "behavior_unique_admission_pairs": len(off_admitted) // 2,
                "actual_legal_canonical_route_pairs": _legal_canonical_pair_distribution(
                    off_proposals
                ),
                "route_budgets": {
                    str(row["route_id"]): int(row["scheduled_pairs"])
                    for row in off_schedule
                },
                "route_actions": {
                    str(row["route_id"]): str(row["scheduler_action"])
                    for row in off_schedule
                },
                "route_selection_funnel": off_selection_funnel,
                "probe_audit": off_probe_audit,
                "schedule_summary": off_summary,
                "validation_reads": 0,
                "holdout_reads": 0,
                "forward_2026_reads": 0,
            }
            feedback_off_path = _write_json(off_root / "control_manifest.json", off_manifest)
            feedback_off_summary = {
                **off_manifest,
                "manifest_sha256": _sha256(feedback_off_path),
            }

        if not admitted:
            raise RuntimeError(f"{batch_id} had no behavior-unique pairs for Phase3CM")
        existing_binding = batch_root / "phase3cm_input_binding.json"
        existing_tables = {
            backend: batch_root / f"phase3cm_{backend}_candidates.csv"
            for backend in ("active_bar", "stock_session")
            if (batch_root / f"phase3cm_{backend}_candidates.csv").exists()
        }
        if existing_binding.exists() and existing_tables:
            bound = json.loads(existing_binding.read_text(encoding="utf-8"))
            if int(bound.get("pair_count") or 0) != len(admitted) // 2:
                raise RuntimeError(f"{batch_id} existing binding pair count drift")
            binding_path, table_paths = existing_binding, existing_tables
        else:
            binding_path, table_paths = _context_and_binding(
                batch_root=batch_root,
                candidates=admitted,
                registry=registry,
                split=split,
                data_release_hash=data_release_hash,
            )
        access_receipts = _run_phase3cm(
            batch_id=batch_id,
            batch_root=batch_root,
            binding_path=binding_path,
            table_paths=table_paths,
            split_manifest=args.split_manifest.resolve(),
            field_roots=field_roots,
            label_roots=label_roots,
            compute_threads=compute_threads,
        )
        outcomes, full_behavior_rows = _outcome_rows(batch_root)
        full_behavior_rows = _join_full_behavior_identities(full_behavior_rows, probe_rows)
        for row in full_behavior_rows:
            if str(row.get("behavior_status") or "") == "RESOLVED":
                behavior_archive.add(dict(row))
        ledger, positive, negative, run_health = build_iterative_feedback_views(outcomes)
        cumulative_ledger.extend({"batch_id": batch_id, **row} for row in ledger)
        cumulative_positive.extend({"batch_id": batch_id, **row} for row in positive)
        cumulative_negative.extend({"batch_id": batch_id, **row} for row in negative)
        cumulative_health.extend({"batch_id": batch_id, **row} for row in run_health)
        health = _route_health(
            outcomes=outcomes,
            ledger=ledger,
            positive=positive,
            negative=negative,
            admission_rows=admission_rows,
            full_behavior_rows=full_behavior_rows,
        )
        previous_health = health
        health_path = _write_parquet(batch_root / "route_health.parquet", health)
        ledger_path = _write_parquet(batch_root / "observation_ledger.parquet", ledger)
        positive_path = _write_parquet(batch_root / "positive_policy_view.parquet", positive)
        negative_path = _write_parquet(batch_root / "negative_scheduler_view.parquet", negative)
        run_health_path = _write_parquet(batch_root / "run_health.parquet", run_health)
        full_behavior_path = _write_parquet(batch_root / "full_behavior.parquet", full_behavior_rows)
        archive_snapshot_path = batch_root / "behavior_archive.parquet"
        behavior_archive.write_parquet(archive_snapshot_path)
        candidate_pack_path = _write_csv(batch_root / "admitted_candidate_pack.csv", admitted)
        funnel_rows = []
        master_funnel_by_route = {str(row["route_id"]): row for row in generation_funnel}
        admission_by_route = defaultdict(lambda: {"behavior_unique_pairs": 0, "admitted_pairs": 0})
        for row in admission_rows:
            route_id = str(row["route_id"])
            if row["admission_reason"] not in {"BEHAVIOR_UNRESOLVED", "EXACT_BEHAVIOR_DUPLICATE"}:
                admission_by_route[route_id]["behavior_unique_pairs"] += 1
            if row["admission_decision"] == "ADMIT":
                admission_by_route[route_id]["admitted_pairs"] += 1
        for route_id in ROUTE_IDS:
            source = master_funnel_by_route[route_id]
            selected = selection_funnel[route_id]
            funnel_rows.append(
                {
                    "route_id": route_id,
                    "scheduled_pairs": selected["scheduled_pairs"],
                    "generation_attempts": source["generation_attempts"],
                    "legal_pairs": source["legal_pairs"],
                    "exact_unique_pairs": source["exact_unique_pairs"],
                    "behavior_unique_pairs": admission_by_route[route_id]["behavior_unique_pairs"],
                    "admitted_pairs": admission_by_route[route_id]["admitted_pairs"],
                    "underfill_reason": selected["underfill_reason"] or source["underfill_reason"],
                    "spillover_reason": selected["spillover_reason"],
                }
            )
        funnel_path = _write_parquet(batch_root / "route_funnel.parquet", funnel_rows)
        artifact_paths = [
            schedule_path,
            schedule_summary_path,
            master_stream_receipt_path,
            proposal_path,
            probe_path,
            admission_path,
            probe_audit_path,
            binding_path,
            candidate_pack_path,
            funnel_path,
            health_path,
            ledger_path,
            positive_path,
            negative_path,
            run_health_path,
            full_behavior_path,
            archive_snapshot_path,
            *[
                Path(str(receipt["result_path"]))
                for receipt in access_receipts
            ],
        ]
        previous_manifest = _batch_manifest(
            batch_root=batch_root,
            batch_id=batch_id,
            input_hashes=input_hashes,
            paths=artifact_paths,
            access_receipts=access_receipts,
        )
        batch_manifests.append(previous_manifest)
        for row in admitted:
            exact = str(row.get("exact_identity") or "")
            if exact:
                historical_exact.add(exact)
        batch_summaries.append(
            {
                "batch_id": batch_id,
                "proposal_pairs": len(proposal_rows) // 2,
                "admitted_pairs": len(admitted) // 2,
                "evaluated_pairs": len(outcomes),
                "positive_pairs": len(positive),
                "negative_pairs": len(negative),
                "run_health_failures": len(run_health),
                "manifest_sha256": _sha256(previous_manifest),
                "schedule_sha256": _sha256(schedule_path),
                "master_stream_hash": master_stream_identity,
                "archive_sha256": _sha256(archive_snapshot_path),
                "route_budgets": {str(row["route_id"]): int(row["scheduled_pairs"]) for row in schedule},
                "actual_legal_canonical_route_pairs": _legal_canonical_pair_distribution(
                    proposal_rows
                ),
                "control_contract": control_contract,
                "route_actions": {
                    str(row["route_id"]): str(row["scheduler_action"])
                    for row in schedule
                },
                "route_selection_funnel": selection_funnel,
                "full_behavior_four_identities_closed": all(
                    str(row.get("behavior_status") or "") != "RESOLVED"
                    or all(
                        str(row.get(key) or "")
                        for key in (
                            "structural_family_id",
                            "signal_cluster_id",
                            "portfolio_behavior_signature_id",
                            "portfolio_behavior_family_id",
                        )
                    )
                    for row in full_behavior_rows
                ),
                "feedback_status": {
                    "positive": schedule_summary["positive_feedback_status"],
                    "negative": schedule_summary["negative_feedback_status"],
                },
            }
        )

    root_ledger = _write_parquet(output_root / "observation_ledger.parquet", cumulative_ledger)
    root_positive = _write_parquet(output_root / "positive_policy_view.parquet", cumulative_positive)
    root_negative = _write_parquet(output_root / "negative_scheduler_view.parquet", cumulative_negative)
    root_run_health = _write_parquet(output_root / "run_health.parquet", cumulative_health)
    root_archive = output_root / "behavior_archive.parquet"
    behavior_archive.write_parquet(root_archive)
    root_schedule = _write_parquet(output_root / "schedule_history.parquet", schedule_history)
    batch1_changed = batch_summaries[1]["route_budgets"] != feedback_off_summary.get("route_budgets", {})
    closed_manifests = [json.loads(path.read_text(encoding="utf-8")) for path in batch_manifests]
    batch_1_binds_batch_0 = (
        str(closed_manifests[1]["input_hashes"]["prior_batch_manifest"])
        == _sha256(batch_manifests[0])
    )
    batch_2_binds_batch_1 = (
        str(closed_manifests[2]["input_hashes"]["prior_batch_manifest"])
        == _sha256(batch_manifests[1])
    )
    shared_on_off_stream = (
        str(feedback_off_summary.get("master_stream_hash") or "")
        == str(batch_summaries[1]["master_stream_hash"])
    )
    feedback_on_contract = dict(batch_summaries[1]["control_contract"])
    feedback_off_contract = dict(feedback_off_summary.get("control_contract") or {})
    shared_contract_checks = {
        key: feedback_on_contract.get(key) == feedback_off_contract.get(key)
        for key in feedback_on_contract
    }
    shared_on_off_contract = bool(shared_contract_checks) and all(
        shared_contract_checks.values()
    )
    feedback_off_kept_initial_budget = (
        feedback_off_summary.get("route_budgets") == batch_summaries[0]["route_budgets"]
    )
    route_comparison, route_causal_gates = _causal_route_comparison(
        feedback_on_budgets=batch_summaries[1]["route_budgets"],
        feedback_off_budgets=feedback_off_summary.get("route_budgets", {}),
        feedback_on_actual=batch_summaries[1]["actual_legal_canonical_route_pairs"],
        feedback_off_actual=feedback_off_summary.get(
            "actual_legal_canonical_route_pairs", {}
        ),
        feedback_on_actions=batch_summaries[1]["route_actions"],
        feedback_on_funnel=batch_summaries[1]["route_selection_funnel"],
        feedback_off_funnel=feedback_off_summary.get("route_selection_funnel", {}),
    )
    real_positive_direction_status = str(
        route_causal_gates.pop("real_positive_direction_status")
    )
    real_negative_direction_status = str(
        route_causal_gates.pop("real_negative_direction_status")
    )
    actual_route_causality = all(bool(value) for value in route_causal_gates.values())
    causal = {
        "batch_1_binds_batch_0_manifest": batch_1_binds_batch_0,
        "batch_2_binds_batch_1_manifest": batch_2_binds_batch_1,
        "feedback_on_off_shared_seed_attempt_stream_budget_archive_dedupe_registry_compiler": shared_on_off_contract,
        "feedback_on_off_shared_contract_checks": shared_contract_checks,
        "batch_1_feedback_on_master_stream_hash": batch_summaries[1]["master_stream_hash"],
        "batch_1_feedback_off_master_stream_hash": feedback_off_summary.get("master_stream_hash"),
        "feedback_on_off_route_budget_changed": batch1_changed,
        "feedback_off_kept_initial_route_budget": feedback_off_kept_initial_budget,
        "feedback_on_off_route_comparison": route_comparison,
        "feedback_on_off_route_causal_gates": route_causal_gates,
        "synthetic_feedback_rules": synthetic_proof,
        "real_positive_direction_status": real_positive_direction_status,
        "real_negative_direction_status": real_negative_direction_status,
        "shortfall_and_dedupe_not_counted_as_feedback": True,
    }
    causal_path = _write_json(output_root / "causal_attribution.json", causal)
    access_zero = not cumulative_health and all(
        value == 0
        for value in (
            contract["validation_reads"],
            contract["holdout_reads"],
            contract["forward_2026_reads"],
        )
    )
    pass_gates = {
        "three_batches_closed": len(batch_manifests) == BATCH_COUNT,
        "synthetic_positive_and_negative_rules_pass": synthetic_proof["status"] == "PASS",
        "feedback_off_control_completed": feedback_off_summary.get("status")
        == "BATCH_1_FEEDBACK_OFF_PROPOSAL_CONTROL_COMPLETED",
        "feedback_on_off_master_attempt_stream_hash_equal": shared_on_off_stream,
        "feedback_on_off_shared_contract_exact": shared_on_off_contract,
        "feedback_off_kept_initial_route_budget": feedback_off_kept_initial_budget,
        "feedback_on_off_actual_route_causality_verified": actual_route_causality,
        "full_behavior_four_identities_closed": all(
            bool(summary["full_behavior_four_identities_closed"])
            for summary in batch_summaries
        ),
        "cross_batch_manifest_bindings_exact": batch_1_binds_batch_0 and batch_2_binds_batch_1,
        "batch_1_feedback_changes_route_budget": batch1_changed,
        "access_counts_zero": access_zero,
        "promotion_forbidden": True,
    }
    decision = {
        "status": "CN_ITERATIVE_SEARCH_V1_CANARY_PASS" if all(pass_gates.values()) else "CN_ITERATIVE_SEARCH_V1_CANARY_FAIL",
        "gates": pass_gates,
        "batch_summaries": batch_summaries,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
    }
    decision_path = _write_json(output_root / "final_decision.json", decision)
    run_manifest = {
        "status": decision["status"],
        "host": platform.node(),
        "python": sys.executable,
        "frozen_contract": _artifact(frozen_contract_path),
        "batch_manifests": [_artifact(path) for path in batch_manifests],
        "final_projections": [
            _artifact(path)
            for path in (root_ledger, root_positive, root_negative, root_run_health, root_archive, root_schedule)
        ],
        "causal_attribution": _artifact(causal_path),
        "final_decision": _artifact(decision_path),
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
    }
    _write_json(output_root / "run_manifest.json", run_manifest)
    return decision


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments == ["--synthetic-rules-only"]:
        proof = _synthetic_feedback_proof()
        print(json.dumps(proof, ensure_ascii=False, sort_keys=True))
        return 0 if proof["status"] == "PASS" else 1
    admission = consume_active_admission(
        "cn-iterative-search-v1-canary",
        {"LAUNCH_HIGH_COST_CAMPAIGN", "SUCCESSOR_CAMPAIGN", "RETRY", "RECOVERY"},
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("--synthetic-rules-only", action="store_true")
    parser.add_argument("--registry", type=Path)
    parser.add_argument("--split-manifest", type=Path)
    parser.add_argument("--sidecar-closure", type=Path)
    parser.add_argument("--active-field-root", type=Path)
    parser.add_argument("--active-label-root", type=Path)
    parser.add_argument("--session-field-root", type=Path)
    parser.add_argument("--session-label-root", type=Path)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--seed-base", type=int, default=2026072101)
    parser.add_argument("--active-threads", type=int, default=11)
    parser.add_argument("--session-threads", type=int, default=2)
    args = parser.parse_args(arguments)
    verify_consumed_admission_target(admission, output_root=args.output_root)
    if args.synthetic_rules_only:
        proof = _synthetic_feedback_proof()
        print(json.dumps(proof, ensure_ascii=False, sort_keys=True))
        return 0 if proof["status"] == "PASS" else 1
    required = (
        "registry", "split_manifest", "sidecar_closure", "active_field_root",
        "active_label_root", "session_field_root", "session_label_root", "output_root",
    )
    missing = [name for name in required if getattr(args, name) is None]
    if missing:
        parser.error("missing real-run arguments: " + ", ".join(missing))
    decision = run(args)
    print(json.dumps(decision, ensure_ascii=False, sort_keys=True))
    return 0 if decision["status"] == "CN_ITERATIVE_SEARCH_V1_CANARY_PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
