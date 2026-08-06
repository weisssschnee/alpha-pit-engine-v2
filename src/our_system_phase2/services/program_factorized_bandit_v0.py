"""Campaign-local, auditable factorized credit for joint CN programs."""

from __future__ import annotations

from dataclasses import dataclass, field
from statistics import median
from typing import Any, Mapping, Sequence

from our_system_phase2.services.candidate_program_proposal_v0 import (
    ProgramProposalReceiptV0,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


BANDIT_VERSION = "cn_program_factorized_bandit_v0"
BANDIT_POLICY_ID = "ROBUST_SHRUNK_MATCHED_INCREMENT_FACTORIZED_V0"
ARM_PATTERN = (
    "UNIFORM_FRESH",
    "UNIFORM_FRESH",
    "FACTORIZED_EXPLOIT",
    "FACTORIZED_EXPLOIT",
    "NOVELTY_RESERVE",
)


def generation_arm_v0(ask_ordinal: int) -> str:
    if int(ask_ordinal) < 0:
        raise ValueError("ask ordinal must be non-negative")
    return ARM_PATTERN[int(ask_ordinal) % len(ARM_PATTERN)]


def _winsorized_mean(values: Sequence[float]) -> float:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return 0.0
    if len(ordered) < 5:
        return float(median(ordered))
    lower_index = int((len(ordered) - 1) * 0.1)
    upper_index = int((len(ordered) - 1) * 0.9)
    lower = ordered[lower_index]
    upper = ordered[upper_index]
    clipped = [min(max(value, lower), upper) for value in ordered]
    return sum(clipped) / len(clipped)


@dataclass(slots=True)
class ProgramFactorizedBanditV0:
    campaign_id: str
    values_by_factor: dict[str, list[float]] = field(default_factory=dict)
    observations: int = 0
    version: str = BANDIT_VERSION
    policy_id: str = BANDIT_POLICY_ID

    def __post_init__(self) -> None:
        if not self.campaign_id:
            raise ValueError("factorized bandit requires a campaign_id")
        if self.version != BANDIT_VERSION or self.policy_id != BANDIT_POLICY_ID:
            raise ValueError("factorized bandit authority/version drift")

    @staticmethod
    def _factor_keys(receipt: ProgramProposalReceiptV0) -> tuple[str, ...]:
        if receipt.program_template_id == "BASE":
            return ()
        keys = [f"template:{receipt.program_template_id}"]
        candidate_ids = dict(receipt.component_candidate_ids)
        for role, component_id in candidate_ids.items():
            if bool(receipt.component_credit_eligible[role]):
                keys.append(f"{role}:{component_id}")
        combination_id = stable_hash(dict(receipt.combination_policy))[:24]
        keys.append(f"combination:{combination_id}")
        base_id = candidate_ids["base"]
        for role in ("temporal", "market", "event"):
            component_id = candidate_ids.get(role)
            if component_id is None or not bool(
                receipt.component_credit_eligible[role]
            ):
                continue
            keys.append(f"base_x_{role}:{base_id}:{component_id}")
            keys.append(
                f"template_x_{role}:{receipt.program_template_id}:{component_id}"
            )
        return tuple(sorted(set(keys)))

    def observe(
        self,
        receipt: ProgramProposalReceiptV0,
        *,
        matched_increment: float,
        behavior_cluster_repeat_count: int = 0,
        turnover_excess_ratio: float = 0.0,
        single_window_concentration: float = 0.0,
    ) -> None:
        if receipt.program_template_id == "BASE":
            raise ValueError("BASE parity proposals are not bandit-reward eligible")
        repeat = max(int(behavior_cluster_repeat_count), 0)
        turnover = max(float(turnover_excess_ratio), 0.0)
        concentration = min(max(float(single_window_concentration), 0.0), 1.0)
        credit = float(matched_increment)
        if credit < 0.0:
            credit *= 1.25
        quality_weight = (
            (1.0 / (1.0 + 0.25 * repeat))
            * (1.0 / (1.0 + turnover))
            * (1.0 - 0.5 * concentration)
        )
        adjusted = credit * quality_weight
        for key in self._factor_keys(receipt):
            self.values_by_factor.setdefault(key, []).append(adjusted)
        self.observations += 1

    def factor_score(self, factor_key: str) -> dict[str, Any]:
        values = list(self.values_by_factor.get(str(factor_key), ()))
        support = len(values)
        robust_mean = _winsorized_mean(values)
        if support < 4:
            exploit_eligible = False
            support_weight = 0.0
        elif support < 8:
            exploit_eligible = True
            support_weight = 0.25
        else:
            exploit_eligible = True
            support_weight = support / (support + 8.0)
        return {
            "factor_key": str(factor_key),
            "support": support,
            "robust_mean": robust_mean,
            "support_weight": support_weight,
            "exploit_eligible": exploit_eligible,
            "weighted_score": robust_mean * support_weight,
        }

    def rank_options(
        self, namespace: str, option_ids: Sequence[str]
    ) -> tuple[dict[str, Any], ...]:
        rows = [
            self.factor_score(f"{namespace}:{str(option_id)}")
            | {"option_id": str(option_id)}
            for option_id in option_ids
        ]
        return tuple(
            sorted(
                rows,
                key=lambda row: (
                    not bool(row["exploit_eligible"]),
                    -float(row["weighted_score"]),
                    str(row["option_id"]),
                ),
            )
        )

    def snapshot(self) -> dict[str, Any]:
        payload = {
            "version": self.version,
            "policy_id": self.policy_id,
            "campaign_id": self.campaign_id,
            "observations": int(self.observations),
            "values_by_factor": {
                key: list(values)
                for key, values in sorted(self.values_by_factor.items())
            },
            "cross_campaign_import_allowed": False,
            "formal_optimizer_authority": False,
            "formal_scheduler_authority": False,
            "validation_feedback_allowed": False,
        }
        payload["bandit_state_sha256"] = stable_hash(payload)
        return payload

    @classmethod
    def restore(cls, snapshot: Mapping[str, Any]) -> "ProgramFactorizedBanditV0":
        payload = dict(snapshot)
        expected = str(payload.pop("bandit_state_sha256", ""))
        if expected != stable_hash(payload):
            raise ValueError("factorized bandit state self-hash mismatch")
        if any(
            bool(payload.get(key))
            for key in (
                "cross_campaign_import_allowed",
                "formal_optimizer_authority",
                "formal_scheduler_authority",
                "validation_feedback_allowed",
            )
        ):
            raise ValueError("factorized bandit state enables forbidden authority")
        bandit = cls(
            campaign_id=str(payload.get("campaign_id") or ""),
            values_by_factor={
                str(key): [float(value) for value in values]
                for key, values in dict(
                    payload.get("values_by_factor") or {}
                ).items()
            },
            observations=int(payload.get("observations") or 0),
            version=str(payload.get("version") or ""),
            policy_id=str(payload.get("policy_id") or ""),
        )
        if bandit.snapshot()["bandit_state_sha256"] != expected:
            raise ValueError("factorized bandit exact replay drift")
        return bandit
