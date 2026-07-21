"""Build Phase3CN search feedback memory from Phase3CM reward outputs.

This is the first wiring step after Phase3CM: turn train portfolio Sortino
reward rows into arm/family feedback tables that future BS/BT/BU/CEM/UCB
searchers can consume. It does not run search.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import numpy as np

from our_system_phase2.services.candidate_schema import (
    TRAIN_ONLY_FEEDBACK_FIELDS,
    normalize_candidate_schema,
    safe_float,
)
from our_system_phase2.services.candidate_submission_receipt import (
    CandidateSubmissionAuthority,
    ReceiptContext,
    read_receipt_table,
)
from our_system_phase2.services.evaluation_access_guard import (
    DEVELOPMENT_ROLE,
    GUARD_VERSION,
    assert_train_only_feedback_rows,
    project_train_only_feedback_row,
)
from our_system_phase2.services.expression_semantics import analyze_expression
from our_system_phase2.services.fixed_split_authority import FixedSplitAuthority
from our_system_phase2.services.matched_control_pairs import (
    CandidatePairAuthority,
    MATCHED_OPTIMIZER_REWARD_METRIC,
    MATCHED_OPTIMIZER_REWARD_SOURCE,
    PAIR_TRAIN_FEEDBACK_READY,
    read_pair_receipt_table,
)
from our_system_phase2.services.unified_capability_registry import UnifiedCapabilityRegistry


REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3cn_feedback_memory_smoke_20260623")
DEFAULT_REPORT_ROOT = Path("reports/phase3cn_feedback_memory_smoke_20260623")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = []
        seen: set[str] = set()
        for row in rows:
            for key in row:
                if key not in seen:
                    fieldnames.append(key)
                    seen.add(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _round(value: Any, ndigits: int = 8) -> float | None:
    val = safe_float(value)
    if not math.isfinite(val):
        return None
    return round(val, ndigits)


def _median(values: list[float]) -> float | None:
    clean = [value for value in values if math.isfinite(value)]
    if not clean:
        return None
    return float(statistics.median(clean))


def _discover_cm_tables(paths: list[Path], roots: list[Path]) -> list[Path]:
    out: list[Path] = []
    for path in paths:
        resolved = _resolve(path)
        if resolved.exists():
            out.append(resolved)
    for root in roots:
        resolved_root = _resolve(root)
        if not resolved_root.exists():
            continue
        for name in ("phase3cm_train_reward.csv", "phase3cm_candidate_train_reward_summary.csv"):
            out.extend(sorted(resolved_root.rglob(name)))
    deduped: list[Path] = []
    seen: set[str] = set()
    for path in out:
        key = str(path).lower()
        if key not in seen:
            seen.add(key)
            deduped.append(path)
    return deduped


def _assert_raw_phase3cm_provenance(raw: dict[str, Any], *, source: Path) -> None:
    """Reject missing or contradictory provenance before schema normalization."""

    candidate = str(raw.get("candidate_id") or raw.get("expression_hash") or "<unknown>")
    split = str(raw.get("optimizer_reward_split") or "").strip().lower()
    reward_source = str(raw.get("optimizer_reward_source") or "").strip()
    metric = str(raw.get("optimizer_reward_metric") or "").strip()
    role = str(raw.get("feedback_data_role") or "").strip().lower()
    if split != "train":
        raise RuntimeError(
            f"raw Phase3CM source must declare optimizer_reward_split=train before normalization; "
            f"table={source} candidate={candidate} split={split or '<missing>'}"
        )
    if reward_source != MATCHED_OPTIMIZER_REWARD_SOURCE:
        raise RuntimeError(
            f"raw Phase3CM source must declare optimizer_reward_source={MATCHED_OPTIMIZER_REWARD_SOURCE} before normalization; "
            f"table={source} candidate={candidate} source={reward_source or '<missing>'}"
        )
    if metric != MATCHED_OPTIMIZER_REWARD_METRIC:
        raise RuntimeError(
            f"raw Phase3CM source metric mismatch before normalization; "
            f"table={source} candidate={candidate} metric={metric or '<missing>'}"
        )
    if role and role != DEVELOPMENT_ROLE:
        raise RuntimeError(
            f"raw Phase3CM source cannot relabel feedback_data_role={role} as development; "
            f"table={source} candidate={candidate}"
        )
    if str(raw.get("pair_evaluation_status") or "") != "PAIR_EVALUATED":
        raise RuntimeError(
            f"raw Phase3CM source is not a completed matched pair evaluation; "
            f"table={source} candidate={candidate}"
        )
    pair_decision = str(raw.get("pair_train_reward_decision") or "")
    pair_blockers = str(raw.get("pair_train_reward_blockers") or "")
    if pair_decision != PAIR_TRAIN_FEEDBACK_READY or pair_blockers:
        raise RuntimeError(
            "raw Phase3CM source is not pair-native feedback ready; "
            f"table={source} candidate={candidate} decision={pair_decision or '<missing>'} "
            f"blockers={pair_blockers or '<none>'}"
        )
    pair_reward = safe_float(raw.get("pair_train_reward"), float("nan"))
    matched_increment = safe_float(raw.get("matched_train_increment"), float("nan"))
    pair_turnover = safe_float(raw.get("pair_turnover_metric"), float("nan"))
    pair_support = safe_float(raw.get("pair_support_metric"), float("nan"))
    pair_rank_ic = safe_float(raw.get("pair_rank_ic_metric"), float("nan"))
    if not math.isfinite(pair_reward) or pair_reward <= 0.0:
        raise RuntimeError(f"raw Phase3CM pair_train_reward must be finite and positive; candidate={candidate}")
    if not math.isfinite(matched_increment) or not math.isclose(pair_reward, matched_increment, rel_tol=0.0, abs_tol=1e-12):
        raise RuntimeError(f"raw Phase3CM pair reward must equal matched_train_increment; candidate={candidate}")
    if not math.isfinite(pair_turnover):
        raise RuntimeError(f"raw Phase3CM pair_turnover_metric must be finite; candidate={candidate}")
    if pair_support != 1.0:
        raise RuntimeError(f"raw Phase3CM pair_support_metric must equal 1.0; candidate={candidate}")
    if not math.isfinite(pair_rank_ic):
        raise RuntimeError(f"raw Phase3CM pair_rank_ic_metric must be finite; candidate={candidate}")
    if int(safe_float(raw.get("primary_evaluator_invocation_count"), 0.0)) <= 0:
        raise RuntimeError(f"raw Phase3CM primary evaluator was not invoked; candidate={candidate}")
    if int(safe_float(raw.get("control_evaluator_invocation_count"), 0.0)) <= 0:
        raise RuntimeError(f"raw Phase3CM control evaluator was not invoked; candidate={candidate}")
    if str(raw.get("pair_member_role") or "") != "PRIMARY":
        raise RuntimeError(
            f"raw Phase3CM feedback may contain primary pair rows only; table={source} candidate={candidate}"
        )
    if str(raw.get("shard_chunk_reward_fallback") or "").strip().lower() in {"1", "true", "yes"}:
        raise RuntimeError("mean-of-shard reward fallback is forbidden from Phase3CN feedback")


def _load_rows(tables: list[Path]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    seen: set[str] = set()
    for table in tables:
        raw_rows = _read_csv(table)
        sources.append({"path": str(table), "rows": len(raw_rows)})
        for raw in raw_rows:
            _assert_raw_phase3cm_provenance(raw, source=table)
            normalized = dict(raw)
            normalized.update(normalize_candidate_schema(normalized))
            digest = str(normalized.get("expression_hash") or "")
            if not digest or digest in seen:
                continue
            seen.add(digest)
            rows.append(normalized)
    return rows, sources


def _has_wrong_lag_or_corr(row: dict[str, Any]) -> bool:
    text = "|".join(
        str(row.get(name) or "")
        for name in ("blocker_flags", "pair_train_reward_blockers", "inherited_blockers")
    ).lower()
    return "wrong_lag" in text or "future_signal_wrong_lag" in text or "high_corr" in text or "signal_corr_abs" in text


def _optimizer_reward(row: dict[str, Any]) -> float:
    reward = safe_float(row.get("pair_train_reward"), float("nan"))
    if not math.isfinite(reward):
        reward = safe_float(row.get("optimizer_reward"), float("nan"))
    return reward


def _has_semantic_degeneracy(row: dict[str, Any]) -> bool:
    return analyze_expression(str(row.get("expression") or "")).hard_blocked


def _is_clean(row: dict[str, Any], *, train_threshold: float, validation_floor: float, max_turnover: float) -> bool:
    del validation_floor
    if _has_semantic_degeneracy(row):
        return False
    train_reward = _optimizer_reward(row)
    turnover = safe_float(row.get("pair_turnover_metric"), float("nan"))
    decision = str(row.get("pair_train_reward_decision") or "")
    blockers = str(row.get("pair_train_reward_blockers") or "")
    support = safe_float(row.get("pair_support_metric"), float("nan"))
    rank_ic = safe_float(row.get("pair_rank_ic_metric"), float("nan"))
    if str(row.get("pair_evaluation_status") or "") != "PAIR_EVALUATED":
        return False
    if decision != PAIR_TRAIN_FEEDBACK_READY or blockers:
        return False
    if not math.isfinite(train_reward) or train_reward <= train_threshold:
        return False
    if not math.isfinite(turnover) or turnover > max_turnover:
        return False
    if support != 1.0 or not math.isfinite(rank_ic):
        return False
    if _has_wrong_lag_or_corr(row):
        return False
    return True


def _is_rewardhack(row: dict[str, Any]) -> bool:
    proxy = safe_float(row.get("phase3ca_proxy_quality") or row.get("proxy_quality"), float("nan"))
    train = _optimizer_reward(row)
    return math.isfinite(proxy) and proxy > 0.0 and math.isfinite(train) and train <= 0.0


def _group_rows(rows: list[dict[str, Any]], key: str) -> dict[str, list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(str(row.get(key) or "unknown"), []).append(row)
    return groups


def build_iterative_feedback_views(
    rows: list[dict[str, Any]],
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    """Project matched-pair outcomes into V1 feedback and run-health views.

    Infrastructure failures are intentionally excluded from financial route
    health.  Negative development evidence remains campaign-local; this view
    never emits a permanent freeze decision.
    """

    ledger: list[dict[str, Any]] = []
    positive: list[dict[str, Any]] = []
    negative: list[dict[str, Any]] = []
    run_health: list[dict[str, Any]] = []
    for source in rows:
        status = str(source.get("pair_evaluation_status") or "")
        pair_id = str(source.get("pair_id") or "")
        route_id = str(source.get("route_id") or source.get("generator_route") or "")
        if status not in {"PAIR_EVALUATED", "PAIR_EVALUATION_BLOCKED"}:
            run_health.append(
                {
                    "pair_id": pair_id,
                    "route_id": route_id,
                    "run_health_status": status or "UNKNOWN_INFRASTRUCTURE_STATUS",
                    "failure_reason": str(source.get("failure_reason") or source.get("error") or ""),
                    "feedback_data_role": "development",
                    "evaluation_access_guard": str(source.get("evaluation_access_guard") or GUARD_VERSION),
                }
            )
            continue

        gross = safe_float(source.get("matched_gross_increment"), float("nan"))
        net = safe_float(source.get("matched_net_increment"), float("nan"))
        if not math.isfinite(net):
            net = safe_float(source.get("matched_train_increment"), float("nan"))
        cost_difference = safe_float(source.get("matched_trading_cost_difference"), float("nan"))
        turnover = safe_float(source.get("pair_turnover_metric"), float("nan"))
        blockers = str(source.get("pair_train_reward_blockers") or "")
        blockers = "|".join(
            value for value in (blockers, str(source.get("pair_evaluation_blockers") or "")) if value
        )
        labels: list[str] = []
        if math.isfinite(gross) and gross > 0.0:
            labels.append("GROSS_POSITIVE")
        elif math.isfinite(gross) and gross < 0.0:
            labels.append("GROSS_NEGATIVE")
        if math.isfinite(net) and net > 0.0:
            labels.append("NET_POSITIVE")
        elif math.isfinite(net) and net < 0.0:
            labels.append("NET_NEGATIVE")
        cost_killed = (
            math.isfinite(gross)
            and math.isfinite(net)
            and math.isfinite(cost_difference)
            and gross > 0.0
            and net <= 0.0
            and cost_difference > 0.0
        )
        if cost_killed:
            labels = [label for label in labels if label not in {"NET_NEGATIVE"}]
            labels.append("COST_KILLED")
        lower_blockers = blockers.lower()
        if "turnover" in lower_blockers:
            labels.append("TURNOVER_KILLED")
        if any(token in lower_blockers for token in ("wrong_lag", "future_signal", "illegal_semantic", "high_corr")):
            labels.append("SEMANTIC_BLOCKED")
        if "empty_pair_support" in lower_blockers or "support" in lower_blockers:
            labels.append("SUPPORT_BLOCKED")
        if "control" in lower_blockers and any(
            token in lower_blockers for token in ("constant", "empty", "degenerate", "behavior_identity")
        ):
            labels.append("CONTROL_DEGENERATE")
        if "behavior_identity_equals" in lower_blockers:
            labels.append("BEHAVIOR_DUPLICATE")
        if "instability" in lower_blockers:
            labels.append("INSTABILITY")
        if not labels and status == "PAIR_EVALUATION_BLOCKED":
            labels.append("NO_INCREMENT")

        item = {
            "pair_id": pair_id,
            "route_id": route_id,
            "matched_gross_increment": _round(gross),
            "matched_net_increment": _round(net),
            "matched_trading_cost_difference": _round(cost_difference),
            "pair_turnover_metric": _round(turnover),
            "outcome_labels": labels,
            "feedback_scope": "CAMPAIGN_LOCAL_DEVELOPMENT",
            "feedback_data_role": str(source.get("feedback_data_role") or "development"),
            "evaluation_access_guard": str(source.get("evaluation_access_guard") or GUARD_VERSION),
        }
        ledger.append(item)
        negative_labels = [
            label
            for label in labels
            if label in {
                "GROSS_NEGATIVE", "NET_NEGATIVE", "COST_KILLED", "TURNOVER_KILLED",
                "SEMANTIC_BLOCKED", "SUPPORT_BLOCKED", "CONTROL_DEGENERATE",
                "BEHAVIOR_DUPLICATE", "INSTABILITY", "NO_INCREMENT",
            }
        ]
        if negative_labels:
            negative.append({**item, "negative_labels": negative_labels, "recommended_action": "REPAIR" if any(label in negative_labels for label in ("COST_KILLED", "TURNOVER_KILLED")) else "DOWNWEIGHT"})
        elif "GROSS_POSITIVE" in labels and "NET_POSITIVE" in labels:
            positive.append({**item, "positive_labels": ["GROSS_POSITIVE", "NET_POSITIVE"]})
    return ledger, positive, negative, run_health


def _family_tables(
    rows: list[dict[str, Any]],
    *,
    train_threshold: float,
    validation_floor: float,
    max_turnover: float,
    max_family_share: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    del validation_floor
    total = max(1, len(rows))
    family_rows: list[dict[str, Any]] = []
    blocked_rows: list[dict[str, Any]] = []
    exploit_rows: list[dict[str, Any]] = []
    for family_id, items in sorted(_group_rows(rows, "family_id").items()):
        semantic_degeneracy_count = sum(1 for row in items if _has_semantic_degeneracy(row))
        semantic_clean_items = [row for row in items if not _has_semantic_degeneracy(row)]
        semantic_degeneracy_rate = semantic_degeneracy_count / max(1, len(items))
        train_rewards = [_optimizer_reward(row) for row in semantic_clean_items]
        train_rewards = [value for value in train_rewards if math.isfinite(value)]
        clean_count = sum(1 for row in semantic_clean_items if _is_clean(row, train_threshold=train_threshold, validation_floor=0.0, max_turnover=max_turnover))
        rewardhack_count = sum(1 for row in semantic_clean_items if _is_rewardhack(row))
        wrong_lag_or_corr_count = sum(1 for row in items if _has_wrong_lag_or_corr(row))
        high_turnover_count = sum(1 for row in items if safe_float(row.get("pair_turnover_metric"), float("inf")) > max_turnover)
        family_share = len(items) / total
        status = "normal"
        reasons: list[str] = []
        if family_share > max_family_share:
            status = "downweight"
            reasons.append("top_family_share_cap")
        if rewardhack_count > 0:
            status = "freeze"
            reasons.append("proxy_high_cm_negative")
        if wrong_lag_or_corr_count > 0:
            status = "block"
            reasons.append("wrong_lag_or_high_corr")
        if semantic_degeneracy_count == len(items):
            status = "block"
            reasons.append("all_rows_semantic_degeneracy")
        elif semantic_degeneracy_rate >= 0.5 and status != "block":
            status = "freeze"
            reasons.append("semantic_degeneracy_rate_ge_50pct")
        elif semantic_degeneracy_count > 0:
            reasons.append("semantic_degenerate_rows_excluded_from_feedback")
        if high_turnover_count > 0 and status != "block":
            status = "freeze"
            reasons.append("high_turnover")
        if clean_count > 0 and status in {"normal", "downweight"}:
            status = "exploit_allowed"
            reasons.append("train_optimizer_reward_positive")
        exemplar = items[0]
        row = {
            "family_id": family_id,
            "motif_id": exemplar.get("motif_id"),
            "field_family": exemplar.get("field_family"),
            "primitive_family": exemplar.get("primitive_family"),
            "event_state_family": exemplar.get("event_state_family"),
            "horizon_bucket": exemplar.get("horizon_bucket"),
            "turnover_bucket": exemplar.get("turnover_bucket"),
            "candidate_count": len(items),
            "family_share": _round(family_share),
            "median_train_reward": _round(_median(train_rewards)),
            "positive_train_reward_count": sum(1 for value in train_rewards if value > train_threshold),
            "clean_count": clean_count,
            "rewardhack_count": rewardhack_count,
            "semantic_degeneracy_count": semantic_degeneracy_count,
            "semantic_degeneracy_rate": _round(semantic_degeneracy_rate),
            "semantic_clean_candidate_count": len(semantic_clean_items),
            "wrong_lag_or_corr_count": wrong_lag_or_corr_count,
            "high_turnover_count": high_turnover_count,
            "family_status": status,
            "family_reasons": "|".join(reasons),
            "feedback_data_role": "development",
            "evaluation_access_guard": GUARD_VERSION,
        }
        family_rows.append(row)
        if status in {"block", "freeze"}:
            blocked_rows.append(row)
        if status == "exploit_allowed":
            exploit_rows.append(row)
    family_rows.sort(key=lambda row: (str(row.get("family_status") == "exploit_allowed"), safe_float(row.get("median_train_reward"), -999.0)), reverse=True)
    return family_rows, blocked_rows, exploit_rows


def _arm_score_table(
    rows: list[dict[str, Any]],
    family_rows: list[dict[str, Any]],
    *,
    train_threshold: float,
    validation_floor: float,
    max_turnover: float,
    min_clean_feedback: int,
) -> list[dict[str, Any]]:
    del validation_floor
    family_by_id = {str(row.get("family_id")): row for row in family_rows}
    out: list[dict[str, Any]] = []
    for arm, items in sorted(_group_rows(rows, "generator_arm").items()):
        semantic_degeneracy_count = sum(1 for row in items if _has_semantic_degeneracy(row))
        semantic_clean_items = [row for row in items if not _has_semantic_degeneracy(row)]
        train_rewards = [_optimizer_reward(row) for row in semantic_clean_items]
        train_rewards = [value for value in train_rewards if math.isfinite(value)]
        clean_count = sum(1 for row in semantic_clean_items if _is_clean(row, train_threshold=train_threshold, validation_floor=0.0, max_turnover=max_turnover))
        rewardhack_count = sum(1 for row in semantic_clean_items if _is_rewardhack(row))
        wrong_lag_count = sum(1 for row in items if _has_wrong_lag_or_corr(row))
        low_turnover_count = sum(1 for row in semantic_clean_items if safe_float(row.get("pair_turnover_metric"), float("inf")) <= max_turnover)
        families = {str(row.get("family_id") or "") for row in semantic_clean_items}
        allowed_families = sum(1 for family in families if family_by_id.get(family, {}).get("family_status") == "exploit_allowed")
        top_family_count = max((sum(1 for row in semantic_clean_items if row.get("family_id") == family) for family in families), default=0)
        score_count = max(1, len(semantic_clean_items))
        total_count = max(1, len(items))
        positive_rate = sum(1 for value in train_rewards if value > train_threshold) / score_count
        new_family_rate = len(families) / score_count
        low_turnover_rate = low_turnover_count / score_count
        rewardhack_rate = rewardhack_count / score_count
        wrong_lag_rate = wrong_lag_count / total_count
        semantic_degeneracy_rate = semantic_degeneracy_count / total_count
        top_family_share = top_family_count / score_count
        median_reward = _median(train_rewards) or 0.0
        arm_score = (
            positive_rate
            + median_reward
            + new_family_rate
            + low_turnover_rate
            - rewardhack_rate
            - wrong_lag_rate
            - semantic_degeneracy_rate
            - top_family_share
        )
        update_allowed = clean_count >= min_clean_feedback
        out.append(
            {
                "generator_arm": arm,
                "candidate_count": len(items),
                "semantic_clean_candidate_count": len(semantic_clean_items),
                "semantic_degeneracy_count": semantic_degeneracy_count,
                "semantic_degeneracy_rate": _round(semantic_degeneracy_rate),
                "clean_feedback_count": clean_count,
                "min_clean_feedback": min_clean_feedback,
                "feedback_update_allowed": str(update_allowed).lower(),
                "positive_train_reward_rate": _round(positive_rate),
                "median_train_reward": _round(median_reward),
                "optimizer_reward_source": MATCHED_OPTIMIZER_REWARD_SOURCE,
                "optimizer_reward_metric": MATCHED_OPTIMIZER_REWARD_METRIC,
                "new_family_rate": _round(new_family_rate),
                "low_turnover_rate": _round(low_turnover_rate),
                "rewardhack_family_rate": _round(rewardhack_rate),
                "wrong_lag_reject_rate": _round(wrong_lag_rate),
                "top_family_concentration": _round(top_family_share),
                "exploit_allowed_family_count": allowed_families,
                "arm_score": _round(arm_score),
                "feedback_data_role": "development",
                "evaluation_access_guard": GUARD_VERSION,
            }
        )
    out.sort(key=lambda row: safe_float(row.get("arm_score"), -999.0), reverse=True)
    return out


def _render_md(summary: dict[str, Any], arm_rows: list[dict[str, Any]], family_rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Phase3CN Feedback Memory Smoke 2026-06-23",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Scope",
        "",
        "Reads Phase3CM train reward outputs and writes standardized search feedback memory. This route does not run search.",
        "",
        "## Summary",
        "",
        f"- input tables: `{len(summary['sources'])}`",
        f"- candidates: `{summary['candidate_count']}`",
        f"- families: `{summary['family_count']}`",
        f"- exploit-allowed families: `{summary['exploit_allowed_family_count']}`",
        f"- blocked/frozen families: `{summary['blocked_family_count']}`",
        "",
        "## Arm Scores",
        "",
        "| arm | rows | clean | update | median reward | wrong-lag/corr | rewardhack | top family | score |",
        "|---|---:|---:|---|---:|---:|---:|---:|---:|",
    ]
    for row in arm_rows:
        lines.append(
            f"| `{row.get('generator_arm')}` | {row.get('candidate_count')} | {row.get('clean_feedback_count')} | "
            f"`{row.get('feedback_update_allowed')}` | {row.get('median_train_reward')} | "
            f"{row.get('wrong_lag_reject_rate')} | {row.get('rewardhack_family_rate')} | {row.get('top_family_concentration')} | {row.get('arm_score')} |"
        )
    lines.extend(
        [
            "",
            "## Top Families",
            "",
            "| family | status | rows | median reward | clean | reasons |",
            "|---|---|---:|---:|---:|---|",
        ]
    )
    for row in family_rows[:30]:
        lines.append(
            f"| `{row.get('family_id')}` | `{row.get('family_status')}` | {row.get('candidate_count')} | "
            f"{row.get('median_train_reward')} | {row.get('clean_count')} | `{row.get('family_reasons')}` |"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- `optimizer_reward` is train-only Phase3CM composite reward: portfolio Sortino plus bounded rank IC loss component.",
            "- Candidate-level validation, holdout, sealed, and forward fields are physically absent from feedback artifacts.",
            "- `feedback_update_allowed=false` means CEM/UCB must not update from that arm.",
            "- Proxy-high but CM-negative families are frozen or blocked before exploit.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_feedback_memory(
    *,
    cm_tables: list[Path],
    cm_roots: list[Path],
    output_root: Path,
    report_root: Path,
    train_threshold: float,
    validation_floor: float,
    max_turnover: float,
    max_family_share: float,
    min_clean_feedback: int,
    authorized_receipt_hashes: Mapping[str, str] | None = None,
    authorized_pair_receipt_hashes: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    if authorized_receipt_hashes is None:
        raise RuntimeError("Phase3CN formal feedback requires candidate submission receipts")
    if authorized_pair_receipt_hashes is None:
        raise RuntimeError("Phase3CN formal feedback requires candidate pair receipts")
    tables = _discover_cm_tables(cm_tables, cm_roots)
    if not tables:
        raise RuntimeError("no Phase3CM reward tables found")
    rows, sources = _load_rows(tables)
    if not rows:
        raise RuntimeError("Phase3CM reward tables had no usable rows")
    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        if authorized_receipt_hashes is not None:
            candidate_id = str(item.get("candidate_id") or "")
            primary_id = str(item.get("primary_candidate_id") or "")
            control_id = str(item.get("control_candidate_id") or "")
            pair_id = str(item.get("pair_id") or "")
            expected_primary_hash = str(authorized_receipt_hashes.get(primary_id) or "")
            expected_control_hash = str(authorized_receipt_hashes.get(control_id) or "")
            expected_pair_hash = str(authorized_pair_receipt_hashes.get(pair_id) or "")
            if (
                not expected_primary_hash
                or candidate_id != primary_id
                or str(item.get("primary_receipt_hash") or "") != expected_primary_hash
            ):
                raise RuntimeError(
                    "Phase3CN feedback rejected primary without the exact evaluator authorization receipt; "
                    f"candidate={candidate_id or '<missing>'}"
                )
            if not expected_control_hash or str(item.get("control_receipt_hash") or "") != expected_control_hash:
                raise RuntimeError(
                    "Phase3CN feedback rejected pair without the exact control authorization receipt; "
                    f"candidate={candidate_id or '<missing>'}"
                )
            if not expected_pair_hash or str(item.get("pair_receipt_hash") or "") != expected_pair_hash:
                raise RuntimeError(
                    "Phase3CN feedback rejected pair without the exact immutable pair receipt; "
                    f"candidate={candidate_id or '<missing>'}"
                )
        source_split = str(item.get("optimizer_reward_split") or "").strip().lower()
        if source_split != "train":
            raise RuntimeError(
                "Phase3CM feedback source must explicitly declare optimizer_reward_split=train; "
                f"candidate={item.get('candidate_id')} split={source_split or '<missing>'}"
            )
        source_name = str(item.get("optimizer_reward_source") or "").strip()
        if source_name != MATCHED_OPTIMIZER_REWARD_SOURCE:
            raise RuntimeError(
                f"Phase3CM feedback source must explicitly declare optimizer_reward_source={MATCHED_OPTIMIZER_REWARD_SOURCE}; "
                f"candidate={item.get('candidate_id')} source={source_name or '<missing>'}"
            )
        source_metric = str(item.get("optimizer_reward_metric") or "").strip()
        if source_metric != MATCHED_OPTIMIZER_REWARD_METRIC:
            raise RuntimeError(
                "Phase3CM feedback source metric mismatch; "
                f"candidate={item.get('candidate_id')} metric={source_metric or '<missing>'}"
            )
        item.update(normalize_candidate_schema(item))
        reward = safe_float(item.get("pair_train_reward"), float("nan"))
        item["optimizer_reward"] = reward if math.isfinite(reward) else ""
        item["feedback_data_role"] = "development"
        normalized_rows.append(project_train_only_feedback_row(item))
    feedback_rows = [
        {field: row.get(field, "") for field in TRAIN_ONLY_FEEDBACK_FIELDS}
        for row in normalized_rows
    ]
    assert_train_only_feedback_rows(feedback_rows, source="Phase3CN feedback memory")
    family_rows, blocked_rows, exploit_rows = _family_tables(
        feedback_rows,
        train_threshold=train_threshold,
        validation_floor=validation_floor,
        max_turnover=max_turnover,
        max_family_share=max_family_share,
    )
    arm_rows = _arm_score_table(
        feedback_rows,
        family_rows,
        train_threshold=train_threshold,
        validation_floor=validation_floor,
        max_turnover=max_turnover,
        min_clean_feedback=min_clean_feedback,
    )
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260623_phase3cn_feedback_memory_smoke",
        "decision": "PHASE3CN_FEEDBACK_MEMORY_READY_DIAGNOSTIC_ONLY",
        "candidate_count": len(feedback_rows),
        "family_count": len(family_rows),
        "arm_count": len(arm_rows),
        "blocked_family_count": len(blocked_rows),
        "exploit_allowed_family_count": len(exploit_rows),
        "min_clean_feedback": min_clean_feedback,
        "train_threshold": train_threshold,
        "legacy_validation_floor_ignored": validation_floor,
        "candidate_level_oos_fields_stripped": True,
        "evaluation_access_guard": GUARD_VERSION,
        "optimizer_reward_source": MATCHED_OPTIMIZER_REWARD_SOURCE,
        "optimizer_reward_metric": MATCHED_OPTIMIZER_REWARD_METRIC,
        "optimizer_reward_split": "train",
        "max_turnover": max_turnover,
        "max_family_share": max_family_share,
        "sources": sources,
        "metric_boundary": "feedback memory only; payload is physically train/development-only; candidate-level non-development fields are forbidden",
    }
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    _write_csv(output_root / "phase3cn_search_feedback_memory.csv", feedback_rows, TRAIN_ONLY_FEEDBACK_FIELDS)
    _write_csv(output_root / "phase3cn_arm_score_table.csv", arm_rows)
    _write_csv(output_root / "phase3cn_family_score_table.csv", family_rows)
    _write_csv(output_root / "phase3cn_blocked_family_table.csv", blocked_rows)
    _write_csv(output_root / "phase3cn_exploit_allowed_family_table.csv", exploit_rows)
    _write_json(output_root / "phase3cn_feedback_memory_summary.json", summary)
    _write_csv(report_root / "phase3cn_search_feedback_memory.csv", feedback_rows, TRAIN_ONLY_FEEDBACK_FIELDS)
    _write_csv(report_root / "phase3cn_arm_score_table.csv", arm_rows)
    _write_csv(report_root / "phase3cn_family_score_table.csv", family_rows)
    _write_csv(report_root / "phase3cn_blocked_family_table.csv", blocked_rows)
    _write_csv(report_root / "phase3cn_exploit_allowed_family_table.csv", exploit_rows)
    _write_json(report_root / "phase3cn_feedback_memory_summary.json", summary)
    (report_root / "PHASE3CN_FEEDBACK_MEMORY_SMOKE_20260623.md").write_text(
        _render_md(summary, arm_rows, family_rows),
        encoding="utf-8",
    )
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cm-table", action="append", type=Path, default=[])
    parser.add_argument("--cm-root", action="append", type=Path, default=[])
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--train-threshold", type=float, default=0.0)
    parser.add_argument("--validation-floor", type=float, default=0.0)
    parser.add_argument("--max-turnover", type=float, default=0.75)
    parser.add_argument("--max-family-share", type=float, default=0.25)
    parser.add_argument("--min-clean-feedback", type=int, default=8)
    parser.add_argument("--candidate-receipt-table", type=Path, required=True)
    parser.add_argument("--candidate-pair-receipt-table", type=Path, required=True)
    parser.add_argument("--unified-registry", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--data-release-hash", required=True)
    args = parser.parse_args(argv)

    receipt_rows = read_receipt_table(_resolve(args.candidate_receipt_table))
    registry = UnifiedCapabilityRegistry.read(_resolve(args.unified_registry))
    split_authority = FixedSplitAuthority.read(_resolve(args.split_manifest), require_official=True)
    phase3cm_path = REPO / "src/our_system_phase2/runtime/phase3cm_train_portfolio_sortino_reward_audit.py"
    receipt_authority = CandidateSubmissionAuthority(
        registry,
        ReceiptContext.build(
            registry=registry,
            split_authority=split_authority,
            data_release_hash=str(args.data_release_hash),
            evaluator_paths=[phase3cm_path],
        ),
    )
    receipt_authority.validate_table(
        [dict(row.get("candidate_contract") or {}) for row in receipt_rows],
        receipt_rows,
    )
    pair_receipt_rows = read_pair_receipt_table(_resolve(args.candidate_pair_receipt_table))
    CandidatePairAuthority().validate_table(
        [dict(row.get("candidate_contract") or {}) for row in receipt_rows],
        receipt_rows,
        pair_receipt_rows,
    )
    authorized_receipt_hashes = {
        str(row.get("candidate_id") or ""): str(row.get("receipt_hash") or "")
        for row in receipt_rows
    }
    authorized_pair_receipt_hashes = {
        str(row.get("pair_id") or ""): str(row.get("pair_receipt_hash") or "")
        for row in pair_receipt_rows
    }

    summary = build_feedback_memory(
        cm_tables=args.cm_table,
        cm_roots=args.cm_root,
        output_root=_resolve(args.output_root),
        report_root=_resolve(args.report_root),
        train_threshold=args.train_threshold,
        validation_floor=args.validation_floor,
        max_turnover=args.max_turnover,
        max_family_share=args.max_family_share,
        min_clean_feedback=args.min_clean_feedback,
        authorized_receipt_hashes=authorized_receipt_hashes,
        authorized_pair_receipt_hashes=authorized_pair_receipt_hashes,
    )
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
