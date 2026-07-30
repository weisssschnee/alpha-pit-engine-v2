"""Machine-enforced evaluation-role boundaries for optimizer feedback.

Candidate-level challenge, sealed, spent-OOS, and forward metrics must never be
present in a payload consumed by search memory, CEM/UCB credit, or a scheduler.
Reporting code may retain those metrics in its own artifacts; feedback code must
first project an explicit development/train-only view.
"""

from __future__ import annotations

import math
from typing import Any, Iterable, Mapping

from our_system_phase2.services.matched_control_pairs import (
    MATCHED_OPTIMIZER_REWARD_CONTRACT,
    MATCHED_OPTIMIZER_REWARD_METRIC,
    MATCHED_OPTIMIZER_REWARD_SOURCE,
)
from our_system_phase2.services.time_series_uncertainty import (
    PAIRED_DELTA_UNCERTAINTY_CONTRACT,
)


GUARD_VERSION = "evalreset_feedback_guard_v2"
DEVELOPMENT_ROLE = "development"

FORBIDDEN_FEEDBACK_PREFIXES = (
    "validation_",
    "holdout_",
    "challenge_",
    "sealed_",
    "forward_",
    "oos_",
)


class EvaluationAccessViolation(RuntimeError):
    """Raised when non-development evaluation data reaches a feedback sink."""


def forbidden_feedback_fields(row: Mapping[str, Any]) -> list[str]:
    """Return forbidden candidate-level fields, even when their values are blank."""

    return sorted(
        str(key)
        for key in row
        if str(key).lower().startswith(FORBIDDEN_FEEDBACK_PREFIXES)
    )


def _has_reward(row: Mapping[str, Any]) -> bool:
    for key in ("optimizer_reward", "train_reward"):
        value = row.get(key)
        if value in (None, ""):
            continue
        try:
            if math.isfinite(float(value)):
                return True
        except (TypeError, ValueError):
            return True
    return False


def assert_train_only_feedback_row(
    row: Mapping[str, Any],
    *,
    source: str = "feedback payload",
) -> None:
    """Fail closed unless a feedback row is physically train/development-only."""

    forbidden = forbidden_feedback_fields(row)
    if forbidden:
        joined = ", ".join(forbidden)
        raise EvaluationAccessViolation(
            f"{source} contains candidate-level non-development fields: {joined}"
        )
    split = str(row.get("optimizer_reward_split") or "").strip().lower()
    if _has_reward(row) and split != "train":
        raise EvaluationAccessViolation(
            f"{source} optimizer_reward_split must be 'train'; observed {split or '<missing>'}"
        )
    if _has_reward(row):
        reward_source = str(row.get("optimizer_reward_source") or "").strip()
        reward_metric = str(row.get("optimizer_reward_metric") or "").strip()
        reward_contract = str(
            row.get("optimizer_reward_contract") or ""
        ).strip()
        uncertainty_contract = str(
            row.get("optimizer_reward_uncertainty_contract") or ""
        ).strip()
        if reward_source != MATCHED_OPTIMIZER_REWARD_SOURCE:
            raise EvaluationAccessViolation(
                f"{source} optimizer_reward_source must be '{MATCHED_OPTIMIZER_REWARD_SOURCE}'; observed {reward_source or '<missing>'}"
            )
        if reward_metric != MATCHED_OPTIMIZER_REWARD_METRIC:
            raise EvaluationAccessViolation(
                f"{source} optimizer_reward_metric must be '{MATCHED_OPTIMIZER_REWARD_METRIC}'; observed {reward_metric or '<missing>'}"
            )
        if reward_contract != MATCHED_OPTIMIZER_REWARD_CONTRACT:
            raise EvaluationAccessViolation(
                f"{source} optimizer_reward_contract must be '{MATCHED_OPTIMIZER_REWARD_CONTRACT}'; observed {reward_contract or '<missing>'}"
            )
        if uncertainty_contract != PAIRED_DELTA_UNCERTAINTY_CONTRACT:
            raise EvaluationAccessViolation(
                f"{source} optimizer_reward_uncertainty_contract must be '{PAIRED_DELTA_UNCERTAINTY_CONTRACT}'; observed {uncertainty_contract or '<missing>'}"
            )
    role = str(row.get("feedback_data_role") or "").strip().lower()
    if role != DEVELOPMENT_ROLE:
        raise EvaluationAccessViolation(
            f"{source} feedback_data_role must be '{DEVELOPMENT_ROLE}'; observed {role or '<missing>'}"
        )
    guard = str(row.get("evaluation_access_guard") or "").strip()
    if guard != GUARD_VERSION:
        raise EvaluationAccessViolation(
            f"{source} evaluation_access_guard must be '{GUARD_VERSION}'; observed {guard or '<missing>'}"
        )


def assert_train_only_feedback_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    source: str = "feedback payload",
) -> None:
    for index, row in enumerate(rows, 1):
        assert_train_only_feedback_row(row, source=f"{source} row {index}")


def project_train_only_feedback_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Create a train-only copy and prove that the resulting payload is safe."""

    split = str(row.get("optimizer_reward_split") or "").strip().lower()
    if _has_reward(row) and split != "train":
        raise EvaluationAccessViolation(
            f"cannot project optimizer reward from split {split or '<missing>'!r} into development feedback"
        )
    role = str(row.get("feedback_data_role") or "").strip().lower()
    if role != DEVELOPMENT_ROLE:
        raise EvaluationAccessViolation(
            f"cannot project feedback_data_role {role or '<missing>'!r}; explicit development provenance is required"
        )
    out = {
        str(key): value
        for key, value in row.items()
        if not str(key).lower().startswith(FORBIDDEN_FEEDBACK_PREFIXES)
    }
    out["feedback_data_role"] = DEVELOPMENT_ROLE
    out["evaluation_access_guard"] = GUARD_VERSION
    assert_train_only_feedback_row(out, source="projected train-only feedback")
    return out
