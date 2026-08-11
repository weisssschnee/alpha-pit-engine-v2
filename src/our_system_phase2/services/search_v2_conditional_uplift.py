"""Conditional Full-vs-Base uplift credit for admitted Search V2 programs."""

from __future__ import annotations

from dataclasses import dataclass
import math
from statistics import median
from types import MappingProxyType
from typing import Any, Mapping

from our_system_phase2.services.search_v2_admission import (
    AbsoluteEconomicAdmission,
)


CONDITIONAL_UPLIFT_POLICY_ID = "CN_SEARCH_V2_CONDITIONAL_FULL_VS_BASE_UPLIFT_V1"
MATCHED_CONTROL_CONTRACT_ID = "CANDIDATE_PROGRAM_V1_FULL_VS_BASE_MATCHED_CONTROL"
COMPONENT_ATTRIBUTION_UNIDENTIFIED = "COMPONENT_ATTRIBUTION_UNIDENTIFIED"


def _finite(value: Any, label: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"conditional uplift {label} is invalid") from exc
    if not math.isfinite(parsed):
        raise ValueError(f"conditional uplift {label} is invalid")
    return parsed


def _window_map(payload: Mapping[str, Any], label: str) -> dict[str, float]:
    windows = tuple(payload.get("development_subwindows") or ())
    if len(windows) != 3:
        raise ValueError(f"conditional uplift {label} window coverage drift")
    output: dict[str, float] = {}
    for row in windows:
        if not isinstance(row, Mapping):
            raise ValueError(f"conditional uplift {label} window shape drift")
        window_id = str(row.get("window_id") or "")
        if not window_id or window_id in output:
            raise ValueError(f"conditional uplift {label} window identity drift")
        output[window_id] = _finite(row.get("cumulative_net_return"), label)
    return output


@dataclass(frozen=True, slots=True)
class ProgramUpliftCredit:
    record_payload_sha256: str
    pair_id: str
    program_id: str
    control_program_id: str
    program_credit: Mapping[str, Any]
    component_attribution_status: str = COMPONENT_ATTRIBUTION_UNIDENTIFIED
    component_credits: None = None
    policy_id: str = CONDITIONAL_UPLIFT_POLICY_ID

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "program_credit", MappingProxyType(dict(self.program_credit))
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "policy_id": self.policy_id,
            "record_payload_sha256": self.record_payload_sha256,
            "pair_id": self.pair_id,
            "program_id": self.program_id,
            "control_program_id": self.control_program_id,
            "program_credit": dict(self.program_credit),
            "component_attribution_status": self.component_attribution_status,
            "component_credits": self.component_credits,
        }


def conditional_uplift_credit(
    record: Mapping[str, Any],
    admission: AbsoluteEconomicAdmission,
) -> ProgramUpliftCredit | None:
    """Return program-level uplift only after absolute admission.

    A failed candidate yields ``None`` rather than a large negative enhancer
    reward.  This prevents standalone failure from contaminating Head B.
    """

    if not admission.admitted:
        return None
    row_identity = (
        str(record.get("pair_id") or ""),
        str(record.get("program_id") or ""),
        str(record.get("control_program_id") or ""),
    )
    admitted_identity = (
        admission.pair_id,
        admission.program_id,
        admission.control_program_id,
    )
    if row_identity != admitted_identity:
        raise ValueError("conditional uplift Full/Base identity drift")
    if str(record.get("record_payload_sha256") or "") != admission.record_payload_sha256:
        raise ValueError("conditional uplift record identity drift")
    if (
        str(record.get("matched_control_contract_id") or "")
        != MATCHED_CONTROL_CONTRACT_ID
    ):
        raise ValueError("conditional uplift matched-control semantics drift")

    primary = record.get("primary")
    control = record.get("base_control")
    if not isinstance(primary, Mapping) or not isinstance(control, Mapping):
        raise ValueError("conditional uplift primary/base payload missing")

    reward_increment = _finite(
        record.get("matched_net_reward_increment"), "matched reward increment"
    )
    return_increment = _finite(
        record.get("matched_cumulative_return_increment"),
        "matched cumulative return increment",
    )
    recomputed_reward = _finite(
        primary.get("continuous_book_net_reward"), "primary reward"
    ) - _finite(control.get("continuous_book_net_reward"), "control reward")
    recomputed_return = _finite(
        primary.get("cumulative_net_return"), "primary return"
    ) - _finite(control.get("cumulative_net_return"), "control return")
    if not math.isclose(reward_increment, recomputed_reward, rel_tol=1e-12, abs_tol=1e-12):
        raise ValueError("conditional uplift matched reward increment drift")
    if not math.isclose(return_increment, recomputed_return, rel_tol=1e-12, abs_tol=1e-12):
        raise ValueError("conditional uplift matched return increment drift")

    primary_windows = _window_map(primary, "primary")
    control_windows = _window_map(control, "control")
    if tuple(primary_windows) != tuple(control_windows):
        raise ValueError("conditional uplift matched window identity drift")
    window_increments = tuple(
        primary_windows[window_id] - control_windows[window_id]
        for window_id in primary_windows
    )
    positive_windows = sum(value > 0.0 for value in window_increments)
    primary_turnover = _finite(
        primary.get("mean_one_way_turnover"), "primary turnover"
    )
    control_turnover = _finite(
        control.get("mean_one_way_turnover"), "control turnover"
    )
    credit = {
        "matched_net_reward_increment": reward_increment,
        "matched_cumulative_net_return_increment": return_increment,
        "cross_window_matched_increment_count": len(window_increments),
        "cross_window_positive_increment_count": positive_windows,
        "cross_window_matched_consistency": positive_windows / len(window_increments),
        "robust_median_window_return_increment": float(median(window_increments)),
        "lower_tail_window_return_increment": float(min(window_increments)),
        "turnover_differential": primary_turnover - control_turnover,
        "window_return_increments": list(window_increments),
        "credit_level": "PROGRAM_OR_TEMPLATE_ONLY",
    }
    return ProgramUpliftCredit(
        record_payload_sha256=admission.record_payload_sha256,
        pair_id=admission.pair_id,
        program_id=admission.program_id,
        control_program_id=admission.control_program_id,
        program_credit=credit,
    )
