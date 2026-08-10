"""Campaign-local allocator credit that puts absolute economics before uplift."""

from __future__ import annotations

from dataclasses import dataclass, field
import math
from statistics import median
from typing import Any, Mapping, Sequence

from our_system_phase2.services.candidate_program_proposal_v0 import (
    ProgramProposalReceiptV0,
)
from our_system_phase2.services.program_factorized_bandit_v0 import (
    ProgramFactorizedBanditV0,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


BANDIT_VERSION = "cn_program_allocator_repair_bandit_v0"
BANDIT_POLICY_ID = "ABSOLUTE_FIRST_REPLAY_SAFE_FACTORIZED_V0"
OUTCOME_FIELDS = (
    "replay_admissible",
    "primary_reward_positive",
    "primary_return_positive",
    "stable_two_of_three",
    "turnover_efficiency_positive",
    "turnover_efficiency",
    "matched_reward_positive",
    "matched_return_positive",
    "matched_reward_increment",
    "matched_return_increment",
)


def _winsorized_mean(values: Sequence[float]) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return 0.0
    if len(ordered) < 5:
        return float(median(ordered))
    lower = ordered[int((len(ordered) - 1) * 0.1)]
    upper = ordered[int((len(ordered) - 1) * 0.9)]
    return sum(min(max(value, lower), upper) for value in ordered) / len(ordered)


def allocator_outcome_from_record_v0(row: Mapping[str, Any]) -> dict[str, float]:
    replay_admissible = bool(
        str(row.get("replay_status") or "") == "PAIR_REPLAY_COMPLETE"
        and not row.get("blockers")
        and row.get("primary") is not None
        and row.get("base_control") is not None
    )
    if not replay_admissible:
        return {field: 0.0 for field in OUTCOME_FIELDS}

    primary = dict(row["primary"])
    windows = tuple(primary.get("development_subwindows") or ())
    positive_windows = sum(
        float(item.get("cumulative_net_return") or 0.0) > 0.0 for item in windows
    )
    stable_two_of_three = len(windows) == 3 and positive_windows >= 2
    turnover_efficiency_value = primary.get("net_return_per_turnover")
    turnover_efficiency = (
        float(turnover_efficiency_value)
        if turnover_efficiency_value is not None
        and math.isfinite(float(turnover_efficiency_value))
        else 0.0
    )
    matched_reward = float(row.get("matched_net_reward_increment") or 0.0)
    matched_return = float(row.get("matched_cumulative_return_increment") or 0.0)
    return {
        "replay_admissible": 1.0,
        "primary_reward_positive": float(
            float(primary["continuous_book_net_reward"]) > 0.0
        ),
        "primary_return_positive": float(
            float(primary["cumulative_net_return"]) > 0.0
        ),
        "stable_two_of_three": float(stable_two_of_three),
        "turnover_efficiency_positive": float(turnover_efficiency > 0.0),
        "turnover_efficiency": turnover_efficiency,
        "matched_reward_positive": float(matched_reward > 0.0),
        "matched_return_positive": float(matched_return > 0.0),
        "matched_reward_increment": matched_reward,
        "matched_return_increment": matched_return,
    }


@dataclass(slots=True)
class ProgramAllocatorRepairBanditV0:
    campaign_id: str
    outcomes_by_factor: dict[str, list[dict[str, float]]] = field(default_factory=dict)
    observations: int = 0
    version: str = BANDIT_VERSION
    policy_id: str = BANDIT_POLICY_ID

    def __post_init__(self) -> None:
        if not self.campaign_id:
            raise ValueError("allocator-repair bandit requires a campaign_id")
        if self.version != BANDIT_VERSION or self.policy_id != BANDIT_POLICY_ID:
            raise ValueError("allocator-repair bandit authority/version drift")

    @staticmethod
    def _factor_keys(receipt: ProgramProposalReceiptV0) -> tuple[str, ...]:
        return ProgramFactorizedBanditV0._factor_keys(receipt)

    def observe(
        self,
        receipt: ProgramProposalReceiptV0,
        *,
        outcome: Mapping[str, float],
    ) -> None:
        if receipt.program_template_id == "BASE":
            raise ValueError("BASE parity proposals are not allocator-credit eligible")
        normalized = {field: float(outcome[field]) for field in OUTCOME_FIELDS}
        if any(not math.isfinite(value) for value in normalized.values()):
            raise ValueError("allocator-repair outcome contains a non-finite value")
        for field in (
            "replay_admissible",
            "primary_reward_positive",
            "primary_return_positive",
            "stable_two_of_three",
            "turnover_efficiency_positive",
            "matched_reward_positive",
            "matched_return_positive",
        ):
            if normalized[field] not in (0.0, 1.0):
                raise ValueError(f"allocator-repair binary outcome drift: {field}")
        for key in self._factor_keys(receipt):
            self.outcomes_by_factor.setdefault(key, []).append(dict(normalized))
        self.observations += 1

    def observe_record(
        self,
        receipt: ProgramProposalReceiptV0,
        row: Mapping[str, Any],
    ) -> dict[str, float]:
        outcome = allocator_outcome_from_record_v0(row)
        self.observe(receipt, outcome=outcome)
        return outcome

    def factor_score(self, factor_key: str) -> dict[str, Any]:
        outcomes = list(self.outcomes_by_factor.get(str(factor_key), ()))
        support = len(outcomes)
        exploit_eligible = support >= 4
        support_weight = (
            0.0
            if support < 4
            else 0.25
            if support < 8
            else support / (support + 8.0)
        )
        means = {
            field: _winsorized_mean([row[field] for row in outcomes])
            for field in OUTCOME_FIELDS
        }
        selection_rank = [
            means["replay_admissible"] * support_weight,
            means["primary_reward_positive"] * support_weight,
            means["primary_return_positive"] * support_weight,
            means["stable_two_of_three"] * support_weight,
            means["turnover_efficiency_positive"] * support_weight,
            math.tanh(means["turnover_efficiency"]) * support_weight,
            means["matched_reward_positive"] * support_weight,
            means["matched_return_positive"] * support_weight,
            math.tanh(means["matched_reward_increment"]) * support_weight,
            math.tanh(means["matched_return_increment"]) * support_weight,
        ]
        return {
            "factor_key": str(factor_key),
            "support": support,
            "support_weight": support_weight,
            "exploit_eligible": exploit_eligible,
            "outcome_means": means,
            "selection_rank": selection_rank,
        }

    def score_receipt(self, receipt: ProgramProposalReceiptV0) -> dict[str, Any]:
        factor_rows = [self.factor_score(key) for key in self._factor_keys(receipt)]
        eligible = [row for row in factor_rows if bool(row["exploit_eligible"])]
        selection_rank = [
            sum(float(row["selection_rank"][index]) for row in eligible) / len(eligible)
            if eligible
            else 0.0
            for index in range(10)
        ]
        return {
            "program_template_id": receipt.program_template_id,
            "semantic_program_hash": receipt.semantic_program_hash,
            "eligible_factor_count": len(eligible),
            "factor_count": len(factor_rows),
            "allocator_signal_available": bool(eligible),
            "selection_rank": selection_rank,
            "factors": factor_rows,
        }

    def snapshot(self) -> dict[str, Any]:
        payload = {
            "version": self.version,
            "policy_id": self.policy_id,
            "campaign_id": self.campaign_id,
            "observations": int(self.observations),
            "outcomes_by_factor": {
                key: [dict(outcome) for outcome in outcomes]
                for key, outcomes in sorted(self.outcomes_by_factor.items())
            },
            "selection_hierarchy": list(OUTCOME_FIELDS),
            "blocked_positive_credit": False,
            "cross_campaign_import_allowed": False,
            "formal_optimizer_authority": False,
            "formal_scheduler_authority": False,
            "validation_feedback_allowed": False,
        }
        payload["bandit_state_sha256"] = stable_hash(payload)
        return payload

    @classmethod
    def restore(
        cls, snapshot: Mapping[str, Any]
    ) -> "ProgramAllocatorRepairBanditV0":
        payload = dict(snapshot)
        expected = str(payload.pop("bandit_state_sha256", ""))
        if expected != stable_hash(payload):
            raise ValueError("allocator-repair bandit state self-hash mismatch")
        if list(payload.get("selection_hierarchy") or ()) != list(OUTCOME_FIELDS):
            raise ValueError("allocator-repair selection hierarchy drift")
        if bool(payload.get("blocked_positive_credit")) or any(
            bool(payload.get(key))
            for key in (
                "cross_campaign_import_allowed",
                "formal_optimizer_authority",
                "formal_scheduler_authority",
                "validation_feedback_allowed",
            )
        ):
            raise ValueError("allocator-repair bandit enables forbidden authority")
        bandit = cls(
            campaign_id=str(payload.get("campaign_id") or ""),
            outcomes_by_factor={
                str(key): [
                    {field: float(outcome[field]) for field in OUTCOME_FIELDS}
                    for outcome in outcomes
                ]
                for key, outcomes in dict(
                    payload.get("outcomes_by_factor") or {}
                ).items()
            },
            observations=int(payload.get("observations") or 0),
            version=str(payload.get("version") or ""),
            policy_id=str(payload.get("policy_id") or ""),
        )
        if bandit.snapshot()["bandit_state_sha256"] != expected:
            raise ValueError("allocator-repair bandit exact replay drift")
        return bandit
