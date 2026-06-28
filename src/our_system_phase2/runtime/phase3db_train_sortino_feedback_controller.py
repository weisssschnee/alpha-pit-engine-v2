"""Phase3DB train-Sortino/rankIC feedback controller.

This route turns Phase3CM train-side composite optimizer rewards into the next
search budget. Proxy metrics have no positive scoring power here: they are only
used as cheap safety/cost veto metadata before expensive CM evaluation.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from our_system_phase2.runtime.phase3bl_bk_priority_signal_materialization import _write_csv, _write_json
from our_system_phase2.services.candidate_schema import OPTIMIZER_REWARD_METRIC, normalize_candidate_schema, safe_float
from our_system_phase2.services.multi_arm_scheduler import (
    DEFAULT_ARM_PROFILES,
    build_family_actions,
)


REPO = Path(__file__).resolve().parents[3]
DEFAULT_INPUT_ROOT = Path("reports/phase3cz_cn_feedback_after_cm_20260627")
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3db_train_sortino_feedback_controller_20260627")
DEFAULT_REPORT_ROOT = Path("reports/phase3db_train_sortino_feedback_controller_20260627")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _round(value: Any, ndigits: int = 8) -> float | None:
    out = safe_float(value)
    if not math.isfinite(out):
        return None
    return round(out, ndigits)


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _optimizer_reward(row: dict[str, Any]) -> float:
    reward = safe_float(row.get("optimizer_reward"), float("nan"))
    if not math.isfinite(reward):
        reward = safe_float(row.get("train_reward"), float("nan"))
    return reward


def _has_wrong_lag_or_corr(row: dict[str, Any]) -> bool:
    text = "|".join(
        str(row.get(name) or "")
        for name in ("blocker_flags", "train_reward_blockers", "inherited_blockers")
    ).lower()
    return "wrong_lag" in text or "future_signal_wrong_lag" in text or "high_corr" in text or "signal_corr_abs" in text


def _proxy_hacked(row: dict[str, Any]) -> bool:
    proxy = safe_float(row.get("phase3ca_proxy_quality") or row.get("proxy_quality"), float("nan"))
    train = _optimizer_reward(row)
    return math.isfinite(proxy) and proxy > 0.0 and math.isfinite(train) and train <= 0.0


def _load_feedback_rows(input_roots: list[Path], input_tables: list[Path]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    files: list[Path] = []
    for raw in input_tables:
        path = _resolve(raw)
        if path.exists():
            files.append(path)
    for raw in input_roots:
        root = _resolve(raw)
        if root.is_file():
            files.append(root)
        elif root.is_dir():
            for name in (
                "phase3cn_search_feedback_memory.csv",
                "phase3cm_train_reward.csv",
                "phase3cm_candidate_train_reward_summary.csv",
                "phase3cm_train_reward_partial.csv",
            ):
                files.extend(root.glob(f"**/{name}"))
    seen_files: set[Path] = set()
    seen_hashes: set[str] = set()
    rows: list[dict[str, Any]] = []
    source_rows: list[dict[str, Any]] = []
    for file_path in sorted({path.resolve() for path in files}):
        if file_path in seen_files:
            continue
        seen_files.add(file_path)
        raw_rows = _read_csv(file_path)
        added = 0
        for raw in raw_rows:
            row = dict(raw)
            row.update(normalize_candidate_schema(row))
            digest = str(row.get("expression_hash") or "")
            if not digest or digest in seen_hashes:
                continue
            reward = _optimizer_reward(row)
            if not math.isfinite(reward):
                continue
            seen_hashes.add(digest)
            rows.append(row)
            added += 1
        source_rows.append({"input_file": str(file_path), "raw_rows": len(raw_rows), "accepted_rows": added})
    return rows, source_rows


def _percentile(values: list[float], q: float) -> float:
    clean = sorted(value for value in values if math.isfinite(value))
    if not clean:
        return float("nan")
    if len(clean) == 1:
        return clean[0]
    pos = max(0.0, min(1.0, q)) * (len(clean) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return clean[lo]
    frac = pos - lo
    return clean[lo] * (1.0 - frac) + clean[hi] * frac


def _mean(values: list[float]) -> float:
    clean = [value for value in values if math.isfinite(value)]
    if not clean:
        return float("nan")
    return sum(clean) / len(clean)


def _clean(row: dict[str, Any], *, train_threshold: float, max_turnover: float) -> bool:
    reward = _optimizer_reward(row)
    turnover = safe_float(row.get("train_mean_one_way_turnover") or row.get("mean_one_way_turnover"), 0.0)
    blockers = str(row.get("train_reward_blockers") or row.get("blocker_flags") or "")
    if not math.isfinite(reward) or reward <= train_threshold:
        return False
    if blockers:
        return False
    if _has_wrong_lag_or_corr(row):
        return False
    if math.isfinite(turnover) and turnover > max_turnover:
        return False
    return True


def _group(rows: list[dict[str, Any]], key: str) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        out.setdefault(str(row.get(key) or "unknown"), []).append(row)
    return out


def _family_rows(
    rows: list[dict[str, Any]],
    *,
    train_threshold: float,
    max_turnover: float,
    max_family_share: float,
    min_clean_family: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    total = max(1, len(rows))
    family_rows: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    exploit: list[dict[str, Any]] = []
    for family_id, items in sorted(_group(rows, "family_id").items()):
        rewards = [_optimizer_reward(row) for row in items]
        clean_count = sum(1 for row in items if _clean(row, train_threshold=train_threshold, max_turnover=max_turnover))
        wrong_lag_count = sum(1 for row in items if _has_wrong_lag_or_corr(row))
        rewardhack_count = sum(1 for row in items if _proxy_hacked(row))
        high_turnover_count = sum(
            1
            for row in items
            if safe_float(row.get("train_mean_one_way_turnover") or row.get("mean_one_way_turnover"), 0.0) > max_turnover
        )
        share = len(items) / total
        status = "normal"
        reasons: list[str] = []
        if wrong_lag_count:
            status = "block"
            reasons.append("wrong_lag_or_high_corr")
        elif rewardhack_count:
            status = "freeze"
            reasons.append("proxy_positive_train_negative")
        elif high_turnover_count:
            status = "freeze"
            reasons.append("high_turnover")
        elif share > max_family_share:
            status = "downweight"
            reasons.append("family_concentration_cap")
        if clean_count >= min_clean_family and status in {"normal", "downweight"}:
            status = "exploit_allowed"
            reasons.append("train_composite_reward_clean_positive")
        exemplar = items[0]
        row = {
            "family_id": family_id,
            "motif_id": exemplar.get("motif_id", ""),
            "field_family": exemplar.get("field_family", ""),
            "primitive_family": exemplar.get("primitive_family", ""),
            "event_state_family": exemplar.get("event_state_family", ""),
            "horizon_bucket": exemplar.get("horizon_bucket", ""),
            "turnover_bucket": exemplar.get("turnover_bucket", ""),
            "candidate_count": len(items),
            "family_share": _round(share),
            "median_train_reward": _round(_percentile(rewards, 0.50)),
            "top_quartile_train_reward": _round(_mean([value for value in rewards if math.isfinite(value) and value >= _percentile(rewards, 0.75)])),
            "best_train_reward": _round(max((value for value in rewards if math.isfinite(value)), default=float("nan"))),
            "clean_count": clean_count,
            "rewardhack_count": rewardhack_count,
            "wrong_lag_or_corr_count": wrong_lag_count,
            "high_turnover_count": high_turnover_count,
            "family_status": status,
            "family_reasons": "|".join(reasons),
        }
        family_rows.append(row)
        if status in {"block", "freeze"}:
            blocked.append(row)
        if status == "exploit_allowed":
            exploit.append(row)
    family_rows.sort(key=lambda row: safe_float(row.get("best_train_reward"), -999.0), reverse=True)
    return family_rows, blocked, exploit


def _arm_rows(
    rows: list[dict[str, Any]],
    *,
    train_threshold: float,
    max_turnover: float,
    min_clean_arm: int,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for arm_id, items in sorted(_group(rows, "generator_arm").items()):
        rewards = [_optimizer_reward(row) for row in items]
        clean_count = sum(1 for row in items if _clean(row, train_threshold=train_threshold, max_turnover=max_turnover))
        wrong_lag_count = sum(1 for row in items if _has_wrong_lag_or_corr(row))
        rewardhack_count = sum(1 for row in items if _proxy_hacked(row))
        count = max(1, len(items))
        median_reward = _percentile(rewards, 0.50)
        top_quartile = _mean([value for value in rewards if math.isfinite(value) and value >= _percentile(rewards, 0.75)])
        best_reward = max((value for value in rewards if math.isfinite(value)), default=float("nan"))
        positive_rate = sum(1 for value in rewards if math.isfinite(value) and value > train_threshold) / count
        wrong_lag_rate = wrong_lag_count / count
        rewardhack_rate = rewardhack_count / count
        clean_rate = clean_count / count
        # Composite optimizer reward owns the score. Proxy contributes only
        # through the rewardhack penalty when it disagrees with train reward.
        train_score = (
            0.55 * max(-2.0, min(2.0, top_quartile if math.isfinite(top_quartile) else -2.0))
            + 0.30 * max(-2.0, min(2.0, median_reward if math.isfinite(median_reward) else -2.0))
            + 0.15 * positive_rate
            + 0.08 * clean_rate
            - 0.35 * wrong_lag_rate
            - 0.25 * rewardhack_rate
        )
        update_allowed = clean_count >= min_clean_arm
        out.append(
            {
                "generator_arm": arm_id,
                "candidate_count": len(items),
                "clean_feedback_count": clean_count,
                "min_clean_feedback": min_clean_arm,
                "feedback_update_allowed": str(update_allowed).lower(),
                "positive_train_reward_rate": _round(positive_rate),
                "median_train_reward": _round(median_reward),
                "top_quartile_train_reward": _round(top_quartile),
                "best_train_reward": _round(best_reward),
                "train_sortino_controller_score": _round(train_score),
                "optimizer_controller_score": _round(train_score),
                "arm_score": _round(train_score),
                "optimizer_reward_source": "train_only_phase3cm",
                "optimizer_reward_metric": OPTIMIZER_REWARD_METRIC,
                "proxy_positive_weight": 0.0,
                "proxy_usage": "veto_only",
                "rewardhack_family_rate": _round(rewardhack_rate),
                "wrong_lag_reject_rate": _round(wrong_lag_rate),
                "validation_usage": "report_only",
                "holdout_usage": "report_only",
            }
        )
    out.sort(key=lambda row: safe_float(row.get("train_sortino_controller_score"), -999.0), reverse=True)
    return out


def _fit_shares(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for _ in range(12):
        total = sum(float(row["target_share"]) for row in rows)
        if abs(total - 1.0) <= 1e-9:
            break
        if total > 1.0:
            slack = sum(max(0.0, float(row["target_share"]) - float(row["min_share"])) for row in rows)
            if slack <= 0.0:
                break
            for row in rows:
                available = max(0.0, float(row["target_share"]) - float(row["min_share"]))
                row["target_share"] = float(row["target_share"]) - (total - 1.0) * available / slack
        else:
            slack = sum(max(0.0, float(row["max_share"]) - float(row["target_share"])) for row in rows)
            if slack <= 0.0:
                break
            for row in rows:
                available = max(0.0, float(row["max_share"]) - float(row["target_share"]))
                row["target_share"] = float(row["target_share"]) + (1.0 - total) * available / slack
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
    for row in sorted(rows, key=lambda item: float(item["_exact_budget"]) - int(item["candidate_budget"]), reverse=True)[: total_budget - allocated]:
        row["candidate_budget"] = int(row["candidate_budget"]) + 1
    for row in rows:
        row["target_share"] = _round(int(row["candidate_budget"]) / total_budget)
        row.pop("_exact_budget", None)
    return rows


def _softmax_weights(arm_rows: list[dict[str, Any]], temperature: float) -> dict[str, float]:
    eligible_rows = [
        row
        for row in arm_rows
        if _truthy(row.get("feedback_update_allowed"))
        and safe_float(row.get("train_sortino_controller_score"), 0.0) > 0.0
    ]
    if not eligible_rows:
        return {}
    temp = max(0.05, float(temperature))
    scores = {str(row.get("generator_arm")): safe_float(row.get("train_sortino_controller_score"), 0.0) for row in eligible_rows}
    max_score = max(scores.values()) if scores else 0.0
    exp_scores = {arm: math.exp((score - max_score) / temp) for arm, score in scores.items()}
    total = sum(exp_scores.values()) or 1.0
    return {arm: value / total for arm, value in exp_scores.items()}


def _budget_rows(
    arm_rows: list[dict[str, Any]],
    family_rows: list[dict[str, Any]],
    blocked_rows: list[dict[str, Any]],
    exploit_rows: list[dict[str, Any]],
    *,
    total_budget: int,
    fresh_floor_share: float,
    cem_probe_cap_share: float,
    train_softmax_temperature: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    arm_by_id = {str(row.get("generator_arm")): row for row in arm_rows}
    weights = _softmax_weights(arm_rows, train_softmax_temperature)
    exploit_count = len(exploit_rows) or sum(1 for row in family_rows if str(row.get("family_status") or "") == "exploit_allowed")
    rows: list[dict[str, Any]] = []
    for profile in DEFAULT_ARM_PROFILES:
        feedback = arm_by_id.get(profile.arm_id)
        train_weight = weights.get(profile.arm_id, 0.0)
        min_share = profile.min_share
        max_share = profile.max_share
        train_score = safe_float((feedback or {}).get("train_sortino_controller_score"), 0.0)
        reward_eligible = bool(feedback and _truthy(feedback.get("feedback_update_allowed")) and train_score > 0.0)
        if reward_eligible:
            target = max(profile.min_share, min(profile.max_share, 0.35 * profile.base_share + 0.65 * train_weight))
        else:
            target = profile.base_share
        action = "train_reward_driven"
        reason = "budget weighted by Phase3CM train composite optimizer reward"
        if feedback is None:
            target = profile.min_share
            action = "cold_start_floor"
            reason = "no train reward labels yet"
        elif not reward_eligible:
            action = "fresh_or_probe_no_positive_train_reward"
            reason = "train composite optimizer reward is not positive enough to increase budget"
        if profile.arm_id == "cem_exploit":
            allowed = bool(feedback and _truthy(feedback.get("feedback_update_allowed")) and exploit_count > 0)
            if not allowed:
                min_share = 0.0
                max_share = min(max_share, max(0.0, cem_probe_cap_share))
                target = min(target, max_share)
                action = "probe_only"
                reason = "CEM cannot exploit until clean train composite reward families exist"
        rows.append(
            {
                "arm_id": profile.arm_id,
                "route_hint": profile.route_hint,
                "role": profile.role,
                "category": profile.category,
                "base_share": profile.base_share,
                "min_share": min_share,
                "max_share": max_share,
                "health_score": _round(safe_float((feedback or {}).get("train_sortino_controller_score"), 0.0)),
                "feedback_decision": "train_reward_feedback" if feedback else "cold_start",
                "scheduler_action": action,
                "scheduler_reason": reason,
                "clean_feedback_count": int(safe_float((feedback or {}).get("clean_feedback_count"), 0.0)),
                "feedback_update_allowed": str(_truthy((feedback or {}).get("feedback_update_allowed"))).lower() if feedback else "false",
                "target_share": target,
                "train_sortino_controller_score": _round(safe_float((feedback or {}).get("train_sortino_controller_score"), 0.0)),
                "optimizer_controller_score": _round(safe_float((feedback or {}).get("optimizer_controller_score") or (feedback or {}).get("train_sortino_controller_score"), 0.0)),
                "optimizer_reward_metric": OPTIMIZER_REWARD_METRIC,
                "proxy_positive_weight": 0.0,
                "proxy_usage": "veto_only",
            }
        )
    rows = _fit_shares(rows)
    fresh_share = sum(float(row["target_share"]) for row in rows if row["category"] == "fresh")
    if fresh_share < fresh_floor_share:
        gap = fresh_floor_share - fresh_share
        fresh_rows = [row for row in rows if row["category"] == "fresh"]
        other_rows = [row for row in rows if row["category"] != "fresh"]
        fresh_slack = sum(max(0.0, float(row["max_share"]) - float(row["target_share"])) for row in fresh_rows)
        other_slack = sum(max(0.0, float(row["target_share"]) - float(row["min_share"])) for row in other_rows)
        shift = min(gap, fresh_slack, other_slack)
        if shift > 0 and fresh_slack > 0 and other_slack > 0:
            for row in fresh_rows:
                row["target_share"] = float(row["target_share"]) + shift * max(0.0, float(row["max_share"]) - float(row["target_share"])) / fresh_slack
            for row in other_rows:
                row["target_share"] = float(row["target_share"]) - shift * max(0.0, float(row["target_share"]) - float(row["min_share"])) / other_slack
    rows = _fit_shares(rows)
    rows = _integer_budgets(rows, total_budget)
    family_actions = build_family_actions(
        family_rows,
        blocked_rows,
        exploit_rows,
        total_budget=total_budget,
        max_family_share=0.12,
    )
    fresh_budget = sum(int(row["candidate_budget"]) for row in rows if row["category"] == "fresh")
    cem_budget = sum(int(row["candidate_budget"]) for row in rows if row["arm_id"] == "cem_exploit")
    summary = {
        "total_budget": int(total_budget),
        "allocated_budget": sum(int(row["candidate_budget"]) for row in rows),
        "fresh_budget": fresh_budget,
        "fresh_share": _round(fresh_budget / max(1, total_budget)),
        "fresh_floor_share": _round(fresh_floor_share),
        "cem_exploit_budget": cem_budget,
        "cem_probe_cap_share": _round(cem_probe_cap_share),
        "cem_probe_cap_budget": int(math.ceil(max(1, total_budget) * max(0.0, cem_probe_cap_share))),
        "exploit_allowed_family_count": exploit_count,
        "blocked_or_frozen_family_count": sum(1 for row in family_actions if row.get("scheduler_action") in {"block", "freeze"}),
        "proxy_positive_weight": 0.0,
        "proxy_usage": "veto_only",
        "optimizer_reward_metric": OPTIMIZER_REWARD_METRIC,
        "metric_boundary": "train composite reward controller; validation and holdout are report-only; proxy cannot add positive score",
    }
    return rows, family_actions, summary


def _render_md(summary: dict[str, Any], arm_rows: list[dict[str, Any]], family_rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Phase3DB Train Composite Reward Feedback Controller 2026-06-27",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Reward Contract",
        "",
        "```text",
        f"optimizer_reward: Phase3CM train portfolio Sortino + bounded rank IC loss ({OPTIMIZER_REWARD_METRIC})",
        "proxy_positive_weight: 0.0",
        "proxy_usage: veto_only",
        "validation_usage: report_only",
        "holdout_usage: report_only",
        "```",
        "",
        "## Budget",
        "",
        "```text",
        f"input_candidates: {summary['input_candidate_count']}",
        f"total_budget: {summary['scheduler_summary']['total_budget']}",
        f"fresh_share: {summary['scheduler_summary']['fresh_share']}",
        f"cem_exploit_budget: {summary['scheduler_summary']['cem_exploit_budget']}",
        f"exploit_allowed_family_count: {summary['scheduler_summary']['exploit_allowed_family_count']}",
        "```",
        "",
        "## Arms",
        "",
        "| arm | budget | share | train score | clean | action | reason |",
        "|---|---:|---:|---:|---:|---|---|",
    ]
    for row in arm_rows:
        lines.append(
            f"| `{row.get('arm_id')}` | {row.get('candidate_budget')} | {row.get('target_share')} | "
            f"{row.get('train_sortino_controller_score')} | {row.get('clean_feedback_count')} | "
            f"`{row.get('scheduler_action')}` | {row.get('scheduler_reason')} |"
        )
    lines.extend(["", "## Families", "", "| family | status | median train | best train | clean | reasons |", "|---|---|---:|---:|---:|---|"])
    for row in family_rows[:30]:
        lines.append(
            f"| `{row.get('family_id')}` | `{row.get('family_status')}` | {row.get('median_train_reward')} | "
            f"{row.get('best_train_reward')} | {row.get('clean_count')} | `{row.get('family_reasons')}` |"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- This route emits search budgets only; it does not claim alpha validity.",
            "- Proxy metrics cannot increase score or budget.",
            "- Proxy may only reject, cap, or flag reward hacking when train composite reward disagrees.",
            "- Validation and holdout are not fed back into optimization.",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", type=Path, action="append", default=[DEFAULT_INPUT_ROOT])
    parser.add_argument("--input-table", type=Path, action="append", default=[])
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--total-budget", type=int, default=32768)
    parser.add_argument("--fresh-floor-share", type=float, default=0.70)
    parser.add_argument("--cem-probe-cap-share", type=float, default=0.025)
    parser.add_argument("--train-threshold", type=float, default=0.0)
    parser.add_argument("--max-turnover", type=float, default=0.75)
    parser.add_argument("--min-clean-arm", type=int, default=8)
    parser.add_argument("--min-clean-family", type=int, default=2)
    parser.add_argument("--max-family-share", type=float, default=0.12)
    parser.add_argument("--train-softmax-temperature", type=float, default=0.35)
    args = parser.parse_args(argv)

    output_root = _resolve(args.output_root)
    report_root = _resolve(args.report_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)

    rows, source_rows = _load_feedback_rows(args.input_root, args.input_table)
    family_rows, blocked_rows, exploit_rows = _family_rows(
        rows,
        train_threshold=args.train_threshold,
        max_turnover=args.max_turnover,
        max_family_share=args.max_family_share,
        min_clean_family=args.min_clean_family,
    )
    arm_score_rows = _arm_rows(
        rows,
        train_threshold=args.train_threshold,
        max_turnover=args.max_turnover,
        min_clean_arm=args.min_clean_arm,
    )
    arm_budget_rows, family_action_rows, scheduler_summary = _budget_rows(
        arm_score_rows,
        family_rows,
        blocked_rows,
        exploit_rows,
        total_budget=args.total_budget,
        fresh_floor_share=args.fresh_floor_share,
        cem_probe_cap_share=args.cem_probe_cap_share,
        train_softmax_temperature=args.train_softmax_temperature,
    )
    passed = bool(rows) and int(scheduler_summary["allocated_budget"]) == int(args.total_budget)
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260627_phase3db_train_sortino_feedback_controller",
        "decision": "PHASE3DB_TRAIN_SORTINO_FEEDBACK_CONTROLLER_READY_DIAGNOSTIC_ONLY" if passed else "PHASE3DB_TRAIN_SORTINO_FEEDBACK_CONTROLLER_FAIL",
        "input_candidate_count": len(rows),
        "input_sources": source_rows,
        "arm_count": len(arm_score_rows),
        "family_count": len(family_rows),
        "blocked_family_count": len(blocked_rows),
        "exploit_allowed_family_count": len(exploit_rows),
        "parameters": {
            "train_threshold": args.train_threshold,
            "max_turnover": args.max_turnover,
            "min_clean_arm": args.min_clean_arm,
            "min_clean_family": args.min_clean_family,
            "max_family_share": args.max_family_share,
            "train_softmax_temperature": args.train_softmax_temperature,
            "fresh_floor_share": args.fresh_floor_share,
            "cem_probe_cap_share": args.cem_probe_cap_share,
        },
        "scheduler_summary": scheduler_summary,
        "optimizer_reward_metric": OPTIMIZER_REWARD_METRIC,
        "metric_boundary": "train composite reward owns optimizer feedback; proxy has zero positive scoring weight",
    }
    for root in (output_root, report_root):
        _write_csv(root / "phase3db_input_sources.csv", source_rows)
        _write_csv(root / "phase3db_arm_train_score_table.csv", arm_score_rows)
        _write_csv(root / "phase3db_family_train_score_table.csv", family_rows)
        _write_csv(root / "phase3db_blocked_family_table.csv", blocked_rows)
        _write_csv(root / "phase3db_exploit_allowed_family_table.csv", exploit_rows)
        _write_csv(root / "phase3co_arm_budget_table.csv", arm_budget_rows)
        _write_csv(root / "phase3co_family_action_table.csv", family_action_rows)
        _write_json(root / "phase3db_train_sortino_feedback_controller_summary.json", summary)
    (report_root / "PHASE3DB_TRAIN_SORTINO_FEEDBACK_CONTROLLER_20260627.md").write_text(
        _render_md(summary, arm_budget_rows, family_rows),
        encoding="utf-8",
    )
    print(json.dumps({"status": "ok" if passed else "fail", **summary}, ensure_ascii=False))
    return 0 if passed else 2


if __name__ == "__main__":
    raise SystemExit(main())
