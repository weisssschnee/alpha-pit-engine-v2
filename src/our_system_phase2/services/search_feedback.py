"""Guarded search feedback helpers for Phase3CN.

This module is intentionally small: it lets BS/BT/BU read Phase3CN feedback
tables, but it only allows generator-policy updates when the feedback is clean
enough. Holdout columns are carried for reporting only and are never used in the
optimizer-side clean-feedback decision.
"""

from __future__ import annotations

import copy
import csv
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from our_system_phase2.services.candidate_schema import normalize_candidate_schema, safe_float


HOLDOUT_COLUMNS = {"holdout_day_sortino", "holdout_mcmc_prob_gt_0"}
VALIDATION_COLUMNS = {"validation_day_sortino", "validation_mcmc_prob_gt_0"}


@dataclass
class SearchFeedbackContext:
    provided: bool
    arm_id: str
    min_clean_feedback: int
    clean_feedback_count: int = 0
    feedback_update_allowed: bool = True
    exploit_allowed_family_count: int = 0
    blocked_family_count: int = 0
    feedback_row_count: int = 0
    arm_row_count: int = 0
    family_row_count: int = 0
    holdout_columns_present: bool = False
    validation_columns_present: bool = False
    validation_used_for_score: bool = False
    holdout_used_for_score: bool = False
    optimizer_reward_source: str = "train_only_phase3cm"
    optimizer_reward_metric: str = "train_portfolio_sortino_reward"
    optimizer_reward_split: str = "train"
    guardrail: str = ""
    eligible_source: str = ""
    source_tables: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "provided": self.provided,
            "arm_id": self.arm_id,
            "min_clean_feedback": self.min_clean_feedback,
            "clean_feedback_count": self.clean_feedback_count,
            "feedback_update_allowed": self.feedback_update_allowed,
            "exploit_allowed_family_count": self.exploit_allowed_family_count,
            "blocked_family_count": self.blocked_family_count,
            "feedback_row_count": self.feedback_row_count,
            "arm_row_count": self.arm_row_count,
            "family_row_count": self.family_row_count,
            "holdout_columns_present": self.holdout_columns_present,
            "validation_columns_present": self.validation_columns_present,
            "validation_used_for_score": self.validation_used_for_score,
            "holdout_used_for_score": self.holdout_used_for_score,
            "optimizer_reward_source": self.optimizer_reward_source,
            "optimizer_reward_metric": self.optimizer_reward_metric,
            "optimizer_reward_split": self.optimizer_reward_split,
            "guardrail": self.guardrail,
            "eligible_source": self.eligible_source,
            "source_tables": self.source_tables,
        }


def _read_csv(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _has_holdout_columns(rows: list[dict[str, Any]]) -> bool:
    for row in rows:
        for key in HOLDOUT_COLUMNS:
            if key in row and row.get(key) not in (None, ""):
                return True
    return False


def _has_validation_columns(rows: list[dict[str, Any]]) -> bool:
    for row in rows:
        for key in VALIDATION_COLUMNS:
            if key in row and row.get(key) not in (None, ""):
                return True
    return False


def _optimizer_reward(row: dict[str, Any]) -> float:
    reward = safe_float(row.get("optimizer_reward"), float("nan"))
    if not math.isfinite(reward):
        reward = safe_float(row.get("train_reward"), float("nan"))
    return reward


def _normalize_feedback_row(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    out.update(normalize_candidate_schema(out))
    reward = _optimizer_reward(out)
    out["optimizer_reward"] = reward if math.isfinite(reward) else ""
    out["optimizer_reward_source"] = str(out.get("optimizer_reward_source") or "train_only_phase3cm")
    out["optimizer_reward_metric"] = str(out.get("optimizer_reward_metric") or "train_portfolio_sortino_reward")
    out["optimizer_reward_split"] = str(out.get("optimizer_reward_split") or "train")
    out["validation_usage"] = str(out.get("validation_usage") or "report_only")
    out["holdout_usage"] = str(out.get("holdout_usage") or "report_only")
    return out


def _has_wrong_lag_or_corr(row: dict[str, Any]) -> bool:
    text = "|".join(
        str(row.get(name) or "")
        for name in ("blocker_flags", "phase3bp_blocker_flags", "train_reward_blockers")
    ).lower()
    return "wrong_lag" in text or "future_signal_wrong_lag" in text or "high_corr" in text or "signal_corr_abs" in text


def _clean_feedback_row(
    row: dict[str, Any],
    *,
    train_threshold: float,
    validation_floor: float,
    max_turnover: float,
) -> bool:
    del validation_floor
    train = _optimizer_reward(row)
    turnover = safe_float(row.get("train_mean_one_way_turnover") or row.get("mean_one_way_turnover"), float("nan"))
    blockers = str(row.get("train_reward_blockers") or "")
    decision = str(row.get("train_reward_decision") or "")
    turnover_ok = (not math.isfinite(turnover)) or turnover <= max_turnover
    return (
        math.isfinite(train)
        and train > train_threshold
        and turnover_ok
        and not blockers
        and (not decision or "FOLLOWUP_READY" in decision)
        and not _has_wrong_lag_or_corr(row)
    )


def clean_optimizer_feedback_rows(
    rows: list[dict[str, Any]],
    *,
    arm_id: str = "",
    train_threshold: float = 0.0,
    max_turnover: float = 0.75,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for raw in rows:
        row = _normalize_feedback_row(raw)
        if arm_id and row.get("generator_arm") != arm_id:
            continue
        if _clean_feedback_row(
            row,
            train_threshold=train_threshold,
            validation_floor=0.0,
            max_turnover=max_turnover,
        ):
            out.append(row)
    return out


def _count_exploit_allowed(
    family_rows: list[dict[str, Any]],
    exploit_rows: list[dict[str, Any]],
) -> int:
    if exploit_rows:
        return len(exploit_rows)
    return sum(1 for row in family_rows if str(row.get("family_status") or "").lower() == "exploit_allowed")


def _count_blocked(
    family_rows: list[dict[str, Any]],
    blocked_rows: list[dict[str, Any]],
) -> int:
    if blocked_rows:
        return len(blocked_rows)
    return sum(1 for row in family_rows if str(row.get("family_status") or "").lower() in {"block", "freeze"})


def build_search_feedback_context(
    *,
    feedback_rows: list[dict[str, Any]],
    arm_rows: list[dict[str, Any]] | None = None,
    family_rows: list[dict[str, Any]] | None = None,
    blocked_rows: list[dict[str, Any]] | None = None,
    exploit_rows: list[dict[str, Any]] | None = None,
    arm_id: str = "",
    min_clean_feedback: int = 8,
    train_threshold: float = 0.0,
    validation_floor: float = 0.0,
    max_turnover: float = 0.75,
    source_tables: dict[str, str] | None = None,
) -> SearchFeedbackContext:
    arm_rows = arm_rows or []
    family_rows = family_rows or []
    blocked_rows = blocked_rows or []
    exploit_rows = exploit_rows or []
    normalized_rows = [_normalize_feedback_row(row) for row in feedback_rows]
    arm_id = arm_id or "unknown_arm"
    provided = bool(feedback_rows or arm_rows or family_rows or blocked_rows or exploit_rows)
    clean_count = 0
    allowed_from_arm: bool | None = None
    eligible_source = "feedback_rows"
    matching_arms = [row for row in arm_rows if str(row.get("generator_arm") or row.get("arm_id") or "") == arm_id]
    if matching_arms:
        row = matching_arms[0]
        clean_count = int(safe_float(row.get("clean_feedback_count"), 0.0))
        allowed_from_arm = _truthy(row.get("feedback_update_allowed"))
        eligible_source = "arm_score_table"
        min_clean_feedback = int(safe_float(row.get("min_clean_feedback"), min_clean_feedback))
    else:
        clean_count = sum(
            1
            for row in normalized_rows
            if (not arm_id or row.get("generator_arm") == arm_id)
            and _clean_feedback_row(
                row,
                train_threshold=train_threshold,
                validation_floor=validation_floor,
                max_turnover=max_turnover,
            )
        )
    exploit_count = _count_exploit_allowed(family_rows, exploit_rows)
    blocked_count = _count_blocked(family_rows, blocked_rows)
    threshold_allowed = clean_count >= max(1, int(min_clean_feedback))
    if allowed_from_arm is not None:
        threshold_allowed = threshold_allowed and allowed_from_arm
    if provided and (exploit_rows or family_rows):
        threshold_allowed = threshold_allowed and exploit_count > 0
    if not provided:
        allowed = True
        guardrail = "no external Phase3CN feedback provided; legacy in-run feedback path unchanged"
    elif threshold_allowed:
        allowed = True
        guardrail = "external Phase3CN feedback passed clean threshold; generator may use guarded feedback"
    else:
        allowed = False
        guardrail = "external clean feedback below threshold or no exploit-allowed family; CEM/UCB feedback left unchanged"
    return SearchFeedbackContext(
        provided=provided,
        arm_id=arm_id,
        min_clean_feedback=int(min_clean_feedback),
        clean_feedback_count=clean_count,
        feedback_update_allowed=allowed,
        exploit_allowed_family_count=exploit_count,
        blocked_family_count=blocked_count,
        feedback_row_count=len(normalized_rows),
        arm_row_count=len(arm_rows),
        family_row_count=len(family_rows),
        holdout_columns_present=_has_holdout_columns(feedback_rows),
        validation_columns_present=_has_validation_columns(feedback_rows),
        validation_used_for_score=False,
        holdout_used_for_score=False,
        optimizer_reward_source="train_only_phase3cm",
        optimizer_reward_metric="train_portfolio_sortino_reward",
        optimizer_reward_split="train",
        guardrail=guardrail,
        eligible_source=eligible_source,
        source_tables=source_tables or {},
    )


def load_search_feedback_context(
    *,
    feedback_table: Path | None = None,
    arm_score_table: Path | None = None,
    family_memory: Path | None = None,
    blocked_family_table: Path | None = None,
    exploit_allowed_family_table: Path | None = None,
    arm_id: str = "",
    min_clean_feedback: int = 8,
    train_threshold: float = 0.0,
    validation_floor: float = 0.0,
    max_turnover: float = 0.75,
) -> SearchFeedbackContext:
    tables = {
        "feedback_table": str(feedback_table or ""),
        "arm_score_table": str(arm_score_table or ""),
        "family_memory": str(family_memory or ""),
        "blocked_family_table": str(blocked_family_table or ""),
        "exploit_allowed_family_table": str(exploit_allowed_family_table or ""),
    }
    return build_search_feedback_context(
        feedback_rows=_read_csv(feedback_table),
        arm_rows=_read_csv(arm_score_table),
        family_rows=_read_csv(family_memory),
        blocked_rows=_read_csv(blocked_family_table),
        exploit_rows=_read_csv(exploit_allowed_family_table),
        arm_id=arm_id,
        min_clean_feedback=min_clean_feedback,
        train_threshold=train_threshold,
        validation_floor=validation_floor,
        max_turnover=max_turnover,
        source_tables={key: value for key, value in tables.items() if value},
    )


def load_search_feedback_rows(feedback_table: Path | None = None) -> list[dict[str, Any]]:
    return [_normalize_feedback_row(row) for row in _read_csv(feedback_table)]


def policy_blocked_by_external_feedback(
    base_policy: dict[str, Any],
    context: SearchFeedbackContext,
) -> dict[str, Any]:
    policy = copy.deepcopy(base_policy)
    policy["policy_version"] = f"{policy.get('policy_version', 'policy')}_phase3cn_guarded"
    policy["phase3cn_external_feedback"] = context.to_dict()
    policy["feedback"] = {
        "decision_count": 0,
        "eligible_decision_count": context.clean_feedback_count,
        "min_eligible_decision_count": context.min_clean_feedback,
        "learning_rate": 0.0,
        "entropy_floor": None,
        "top_feedback": {},
        "updated": False,
        "guardrail": context.guardrail,
    }
    return policy


def annotate_policy_with_external_feedback(
    policy: dict[str, Any],
    context: SearchFeedbackContext,
) -> dict[str, Any]:
    if context.provided:
        policy["phase3cn_external_feedback"] = context.to_dict()
    return policy
