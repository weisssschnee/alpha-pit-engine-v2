"""Absolute economic admission for Search Engine V2.

This is a search-control gate over fields already emitted by the accepted
Joint Program evaluator.  It does not evaluate a candidate and it deliberately
does not inspect matched Full-vs-Base uplift.
"""

from __future__ import annotations

from dataclasses import dataclass
import math
from types import MappingProxyType
from typing import Any, Mapping

from our_system_phase2.services.unified_capability_registry import stable_hash


ABSOLUTE_ADMISSION_POLICY_ID = "CN_SEARCH_V2_ABSOLUTE_ECONOMIC_ADMISSION_V1"
EXPECTED_REPLAY_STATUS = "PAIR_REPLAY_COMPLETE"
EXPECTED_RECORD_KIND = "ENHANCED_FULL_BASE_PAIR"
EXPECTED_WINDOW_COUNT = 3
MINIMUM_POSITIVE_WINDOWS = 2


def _finite(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


@dataclass(frozen=True, slots=True)
class AbsoluteEconomicAdmission:
    """One immutable admission decision, separate from optimizer credit."""

    record_payload_sha256: str
    pair_id: str
    program_id: str
    control_program_id: str
    admitted: bool
    failure_reasons: tuple[str, ...]
    metrics: Mapping[str, Any]
    policy_id: str = ABSOLUTE_ADMISSION_POLICY_ID

    def __post_init__(self) -> None:
        object.__setattr__(self, "metrics", MappingProxyType(dict(self.metrics)))

    @classmethod
    def evaluate(
        cls,
        record: Mapping[str, Any],
        *,
        expected_pair_id: str,
        expected_program_id: str,
        expected_control_program_id: str,
    ) -> "AbsoluteEconomicAdmission":
        row = dict(record)
        claimed_hash = str(row.pop("record_payload_sha256", ""))
        reasons: list[str] = []
        if not claimed_hash or stable_hash(row) != claimed_hash:
            reasons.append("RECORD_SELF_HASH_DRIFT")

        pair_id = str(record.get("pair_id") or "")
        program_id = str(record.get("program_id") or "")
        control_program_id = str(record.get("control_program_id") or "")
        if pair_id != str(expected_pair_id):
            reasons.append("PAIR_IDENTITY_MISMATCH")
        if program_id != str(expected_program_id):
            reasons.append("PROGRAM_IDENTITY_MISMATCH")
        if control_program_id != str(expected_control_program_id):
            reasons.append("CONTROL_PROGRAM_IDENTITY_MISMATCH")
        if str(record.get("record_kind") or "") != EXPECTED_RECORD_KIND:
            reasons.append("FULL_BASE_RECORD_KIND_INVALID")
        if str(record.get("replay_status") or "") != EXPECTED_REPLAY_STATUS:
            reasons.append("REPLAY_NOT_COMPLETE")
        if record.get("blockers"):
            reasons.append("CANDIDATE_LOCAL_BLOCKER")
        if not bool(record.get("control_contract_valid")):
            reasons.append("MATCHED_CONTROL_CONTRACT_INVALID")
        if str(record.get("compile_status") or "") != "PASS":
            reasons.append("COMPILE_NOT_COMPLETE")
        if not bool(record.get("physical_ready")) or not bool(record.get("dag_ready")):
            reasons.append("EXECUTION_NOT_READY")
        if bool(record.get("semantic_noop")):
            reasons.append("SEMANTIC_NOOP")

        primary = record.get("primary")
        base_control = record.get("base_control")
        if not isinstance(primary, Mapping) or not isinstance(base_control, Mapping):
            reasons.append("PRIMARY_OR_BASE_CONTROL_MISSING")
            primary = {}

        reward = _finite(primary.get("continuous_book_net_reward"))
        cumulative_return = _finite(primary.get("cumulative_net_return"))
        turnover_efficiency = _finite(primary.get("net_return_per_turnover"))
        if reward is None or reward <= 0.0:
            reasons.append("PRIMARY_NET_REWARD_NOT_POSITIVE")
        if cumulative_return is None or cumulative_return <= 0.0:
            reasons.append("PRIMARY_CUMULATIVE_RETURN_NOT_POSITIVE")
        if turnover_efficiency is None:
            reasons.append("TURNOVER_EFFICIENCY_INVALID")
        elif turnover_efficiency <= 0.0:
            reasons.append("TURNOVER_EFFICIENCY_NOT_POSITIVE")

        fill_count_value = primary.get("fill_count")
        try:
            fill_count = int(fill_count_value)
        except (TypeError, ValueError):
            fill_count = 0
        windows = tuple(primary.get("development_subwindows") or ())
        window_ids: list[str] = []
        window_returns: list[float] = []
        windows_valid = len(windows) == EXPECTED_WINDOW_COUNT
        if windows_valid:
            for window in windows:
                if not isinstance(window, Mapping):
                    windows_valid = False
                    break
                window_id = str(window.get("window_id") or "")
                value = _finite(window.get("cumulative_net_return"))
                if not window_id or value is None:
                    windows_valid = False
                    break
                window_ids.append(window_id)
                window_returns.append(value)
        if len(set(window_ids)) != len(window_ids):
            windows_valid = False
        if fill_count <= 0 or not windows_valid:
            reasons.append("SUPPORT_OR_COVERAGE_INVALID")
        positive_windows = sum(value > 0.0 for value in window_returns)
        if windows_valid and positive_windows < MINIMUM_POSITIVE_WINDOWS:
            reasons.append("CROSS_WINDOW_STABILITY_FAILED")

        metrics = {
            "primary_continuous_book_net_reward": reward,
            "primary_cumulative_net_return": cumulative_return,
            "primary_net_return_per_turnover": turnover_efficiency,
            "fill_count": fill_count,
            "development_window_count": len(window_returns),
            "positive_development_window_count": positive_windows,
            "development_window_ids": tuple(window_ids),
        }
        return cls(
            record_payload_sha256=claimed_hash,
            pair_id=pair_id,
            program_id=program_id,
            control_program_id=control_program_id,
            admitted=not reasons,
            failure_reasons=tuple(dict.fromkeys(reasons)),
            metrics=metrics,
        )

    def to_record(self) -> dict[str, Any]:
        metrics = dict(self.metrics)
        metrics["development_window_ids"] = list(
            metrics.get("development_window_ids") or ()
        )
        return {
            "policy_id": self.policy_id,
            "record_payload_sha256": self.record_payload_sha256,
            "pair_id": self.pair_id,
            "program_id": self.program_id,
            "control_program_id": self.control_program_id,
            "admitted": self.admitted,
            "failure_reasons": list(self.failure_reasons),
            "metrics": metrics,
            "enhancer_credit": None if not self.admitted else "SEPARATE_STAGE_REQUIRED",
        }
