"""Phase3CO multi-arm scheduler utilities.

The scheduler consumes Phase3CN feedback tables and emits budgets for future
search arms. It does not generate candidates and does not use holdout metrics.
"""

from __future__ import annotations

import csv
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from our_system_phase2.services.candidate_schema import safe_float


@dataclass(frozen=True)
class ArmProfile:
    arm_id: str
    route_hint: str
    role: str
    base_share: float
    min_share: float
    max_share: float
    category: str


DEFAULT_ARM_PROFILES = [
    ArmProfile("rx_ucb_fresh", "phase3bs-adaptive-ucb-cem-practice", "broad fresh typed exploration", 0.26, 0.20, 0.42, "fresh"),
    ArmProfile("typed_ast_fresh", "phase3bt-ast-algorithm-bakeoff", "AST-aware fresh generation", 0.22, 0.18, 0.36, "fresh"),
    ArmProfile("challenger_repair", "future-phase3cr-repair", "repair/challenger branch", 0.16, 0.08, 0.24, "fresh"),
    ArmProfile("event_state", "future-event-state-generator", "typed event-state lane", 0.14, 0.06, 0.22, "event"),
    ArmProfile("cem_exploit", "phase3bs-adaptive-ucb-cem-practice", "guarded exploit around proven clean families", 0.14, 0.00, 0.22, "exploit"),
    ArmProfile("random_orthogonal", "control-random-orthogonal", "control and novelty baseline", 0.08, 0.04, 0.14, "control"),
]


def read_csv_rows(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _round(value: float, ndigits: int = 8) -> float:
    if not math.isfinite(float(value)):
        return 0.0
    return round(float(value), ndigits)


def _arm_health(row: dict[str, Any] | None) -> tuple[float, str, str]:
    if not row:
        return 0.0, "cold_start", "no CN feedback for this arm; use fresh/control floor only"
    clean = safe_float(row.get("clean_feedback_count"), 0.0)
    allowed = _truthy(row.get("feedback_update_allowed"))
    median_reward = safe_float(row.get("median_train_reward"), 0.0)
    rewardhack = safe_float(row.get("rewardhack_family_rate"), 0.0)
    wrong_lag = safe_float(row.get("wrong_lag_reject_rate"), 0.0)
    concentration = safe_float(row.get("top_family_concentration"), 0.0)
    exploit_families = safe_float(row.get("exploit_allowed_family_count"), 0.0)
    score = 0.0
    score += min(0.20, max(-0.20, median_reward)) * 0.45
    score += min(8.0, clean) * 0.01
    score += min(3.0, exploit_families) * 0.025
    score -= rewardhack * 0.24
    score -= wrong_lag * 0.35
    score -= max(0.0, concentration - 0.25) * 0.35
    if allowed:
        score += 0.06
        decision = "feedback_allowed"
        reason = "CN clean feedback threshold passed"
    else:
        score -= 0.14
        decision = "feedback_guarded"
        reason = "CN feedback_update_allowed=false; exploit update must stay capped"
    return max(-0.30, min(0.30, score)), decision, reason


def _fit_shares(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for _ in range(8):
        total = sum(float(row["target_share"]) for row in rows)
        if abs(total - 1.0) <= 1e-9:
            break
        if total > 1.0:
            surplus = total - 1.0
            slack = sum(max(0.0, float(row["target_share"]) - float(row["min_share"])) for row in rows)
            if slack <= 0:
                break
            for row in rows:
                available = max(0.0, float(row["target_share"]) - float(row["min_share"]))
                row["target_share"] = float(row["target_share"]) - surplus * (available / slack)
        else:
            deficit = 1.0 - total
            slack = sum(max(0.0, float(row["max_share"]) - float(row["target_share"])) for row in rows)
            if slack <= 0:
                break
            for row in rows:
                available = max(0.0, float(row["max_share"]) - float(row["target_share"]))
                row["target_share"] = float(row["target_share"]) + deficit * (available / slack)
        for row in rows:
            row["target_share"] = min(float(row["max_share"]), max(float(row["min_share"]), float(row["target_share"])))
    return rows


def _integer_budgets(rows: list[dict[str, Any]], total_budget: int) -> list[dict[str, Any]]:
    total_budget = max(1, int(total_budget))
    allocated = 0
    for row in rows:
        exact = float(row["target_share"]) * total_budget
        row["_exact_budget"] = exact
        row["candidate_budget"] = int(math.floor(exact))
        allocated += int(row["candidate_budget"])
    remainder = total_budget - allocated
    ranked = sorted(rows, key=lambda row: (float(row["_exact_budget"]) - int(row["candidate_budget"])), reverse=True)
    for row in ranked[: max(0, remainder)]:
        row["candidate_budget"] = int(row["candidate_budget"]) + 1
    for row in rows:
        row["target_share"] = _round(float(row["candidate_budget"]) / total_budget)
        row.pop("_exact_budget", None)
    return rows


def build_family_actions(
    family_rows: list[dict[str, Any]],
    blocked_rows: list[dict[str, Any]],
    exploit_rows: list[dict[str, Any]],
    *,
    total_budget: int,
    max_family_share: float,
) -> list[dict[str, Any]]:
    blocked_ids = {str(row.get("family_id") or "") for row in blocked_rows}
    exploit_ids = {str(row.get("family_id") or "") for row in exploit_rows}
    all_rows: dict[str, dict[str, Any]] = {}
    for row in family_rows + blocked_rows + exploit_rows:
        family_id = str(row.get("family_id") or "")
        if family_id:
            all_rows.setdefault(family_id, {}).update(row)
    out: list[dict[str, Any]] = []
    cap = max(0, int(math.floor(max(0.0, min(1.0, max_family_share)) * max(1, int(total_budget)))))
    for family_id, row in sorted(all_rows.items()):
        status = str(row.get("family_status") or "").lower()
        reasons = str(row.get("family_reasons") or "")
        if family_id in blocked_ids or status in {"block", "freeze"}:
            action = "block" if status == "block" else "freeze"
            budget_cap = 0
            scheduler_reason = reasons or "CN blocked/frozen family"
        elif family_id in exploit_ids or status == "exploit_allowed":
            action = "allow_followup"
            budget_cap = cap
            scheduler_reason = reasons or "CN exploit_allowed family"
        elif status == "downweight":
            action = "downweight"
            budget_cap = max(1, cap // 2)
            scheduler_reason = reasons or "family concentration cap"
        else:
            action = "observe"
            budget_cap = max(1, cap // 2)
            scheduler_reason = "normal family; no exploit permission"
        out.append(
            {
                "family_id": family_id,
                "motif_id": row.get("motif_id", ""),
                "field_family": row.get("field_family", ""),
                "primitive_family": row.get("primitive_family", ""),
                "event_state_family": row.get("event_state_family", ""),
                "family_status": status or "unknown",
                "family_reasons": reasons,
                "scheduler_action": action,
                "candidate_budget_cap": budget_cap,
                "scheduler_reason": scheduler_reason,
            }
        )
    return out


def build_arm_schedule(
    arm_rows: list[dict[str, Any]],
    family_rows: list[dict[str, Any]],
    blocked_rows: list[dict[str, Any]],
    exploit_rows: list[dict[str, Any]],
    *,
    total_budget: int,
    fresh_floor_share: float = 0.45,
    cem_probe_cap_share: float = 0.06,
    max_family_share: float = 0.25,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    arm_by_id = {str(row.get("generator_arm") or row.get("arm_id") or ""): row for row in arm_rows}
    exploit_allowed_count = len(exploit_rows) or sum(1 for row in family_rows if str(row.get("family_status") or "").lower() == "exploit_allowed")
    budget_rows: list[dict[str, Any]] = []
    for profile in DEFAULT_ARM_PROFILES:
        arm_row = arm_by_id.get(profile.arm_id)
        health, decision, reason = _arm_health(arm_row)
        min_share = profile.min_share
        max_share = profile.max_share
        target = profile.base_share + health
        action = "schedule"
        if profile.arm_id == "cem_exploit":
            feedback_allowed = bool(arm_row and _truthy(arm_row.get("feedback_update_allowed")))
            if (not feedback_allowed) or exploit_allowed_count <= 0:
                max_share = min(max_share, max(0.0, cem_probe_cap_share))
                min_share = 0.0
                target = min(target, max_share)
                action = "probe_only"
                reason = "CEM exploit capped until CN clean feedback and exploit families are sufficient"
            else:
                action = "guarded_exploit"
        elif decision == "cold_start" and profile.category == "fresh":
            action = "fresh_floor"
        elif decision == "cold_start" and profile.category == "event":
            action = "event_floor"
        budget_rows.append(
            {
                "arm_id": profile.arm_id,
                "route_hint": profile.route_hint,
                "role": profile.role,
                "category": profile.category,
                "base_share": profile.base_share,
                "min_share": min_share,
                "max_share": max_share,
                "health_score": _round(health),
                "feedback_decision": decision,
                "scheduler_action": action,
                "scheduler_reason": reason,
                "clean_feedback_count": int(safe_float((arm_row or {}).get("clean_feedback_count"), 0.0)),
                "feedback_update_allowed": str(_truthy((arm_row or {}).get("feedback_update_allowed"))).lower() if arm_row else "false",
                "target_share": min(max_share, max(min_share, target)),
            }
        )
    budget_rows = _fit_shares(budget_rows)
    fresh_share = sum(float(row["target_share"]) for row in budget_rows if row["category"] == "fresh")
    if fresh_share < fresh_floor_share:
        gap = fresh_floor_share - fresh_share
        fresh_rows = [row for row in budget_rows if row["category"] == "fresh"]
        nonfresh_rows = [row for row in budget_rows if row["category"] != "fresh"]
        fresh_slack = sum(max(0.0, float(row["max_share"]) - float(row["target_share"])) for row in fresh_rows)
        nonfresh_slack = sum(max(0.0, float(row["target_share"]) - float(row["min_share"])) for row in nonfresh_rows)
        shift = min(gap, fresh_slack, nonfresh_slack)
        if shift > 0:
            for row in fresh_rows:
                available = max(0.0, float(row["max_share"]) - float(row["target_share"]))
                row["target_share"] = float(row["target_share"]) + shift * (available / fresh_slack)
            for row in nonfresh_rows:
                available = max(0.0, float(row["target_share"]) - float(row["min_share"]))
                row["target_share"] = float(row["target_share"]) - shift * (available / nonfresh_slack)
    budget_rows = _fit_shares(budget_rows)
    budget_rows = _integer_budgets(budget_rows, total_budget)
    family_actions = build_family_actions(
        family_rows,
        blocked_rows,
        exploit_rows,
        total_budget=total_budget,
        max_family_share=max_family_share,
    )
    fresh_budget = sum(int(row["candidate_budget"]) for row in budget_rows if row["category"] == "fresh")
    cem_budget = sum(int(row["candidate_budget"]) for row in budget_rows if row["arm_id"] == "cem_exploit")
    summary = {
        "total_budget": int(total_budget),
        "allocated_budget": sum(int(row["candidate_budget"]) for row in budget_rows),
        "fresh_budget": fresh_budget,
        "fresh_share": _round(fresh_budget / max(1, int(total_budget))),
        "fresh_floor_share": _round(fresh_floor_share),
        "cem_exploit_budget": cem_budget,
        "cem_probe_cap_share": _round(cem_probe_cap_share),
        "cem_probe_cap_budget": int(math.ceil(max(1, int(total_budget)) * max(0.0, cem_probe_cap_share))),
        "exploit_allowed_family_count": exploit_allowed_count,
        "blocked_or_frozen_family_count": sum(1 for row in family_actions if row["scheduler_action"] in {"block", "freeze"}),
        "max_family_share": _round(max_family_share),
        "metric_boundary": "budget scheduler only; no search generation and no holdout optimization",
    }
    return budget_rows, family_actions, summary
