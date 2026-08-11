"""Fresh, campaign-local hierarchical scheduler state for Search Engine V2."""

from __future__ import annotations

from dataclasses import dataclass, field
from statistics import median
from typing import Any, Mapping

from our_system_phase2.services.candidate_program_proposal_v0 import (
    ProgramProposalReceiptV0,
)
from our_system_phase2.services.search_v2_admission import (
    AbsoluteEconomicAdmission,
)
from our_system_phase2.services.search_v2_conditional_uplift import (
    ProgramUpliftCredit,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


SCHEDULER_VERSION = "cn_search_v2_scheduler_v1"
SCHEDULER_POLICY_ID = "PROGRAM_TEMPLATE_HIERARCHICAL_TWO_HEAD_V1"
MINIMUM_HEAD_SUPPORT = 4


def _verify_fresh_provenance(provenance: Mapping[str, Any]) -> dict[str, Any]:
    payload = dict(provenance)
    false_fields = (
        "serialized_optimizer_state_imported",
        "development_financial_observations_imported",
        "candidate_results_imported",
        "factor_statistics_imported",
        "behavior_statistics_imported",
        "template_classification_imported",
    )
    if any(bool(payload.get(field)) for field in false_fields):
        raise ValueError("Search V2 fresh state imports prohibited parent state or results")
    if int(payload.get("development_observation_count", -1)) != 0:
        raise ValueError("Search V2 fresh state requires zero development observations")
    if not bool(payload.get("manual_diagnosis_imported")):
        raise ValueError("Search V2 must disclose its manual diagnosis input")
    if not bool(payload.get("objective_designed_after_parent_results")):
        raise ValueError("Search V2 must disclose post-parent objective design")
    if not bool(payload.get("cross_campaign_development_feedback")):
        raise ValueError("Search V2 adaptive-design provenance cannot claim independence")
    return payload


def _admission_key(receipt: ProgramProposalReceiptV0, kind: str) -> str:
    if kind == "program":
        return f"program:{receipt.semantic_program_hash}"
    if kind == "template":
        return f"template:{receipt.program_template_id}"
    raise ValueError(f"unknown Search V2 scheduler key kind: {kind}")


@dataclass(slots=True)
class SearchV2SchedulerV1:
    campaign_id: str
    provenance: dict[str, Any]
    admission_by_key: dict[str, list[bool]] = field(default_factory=dict)
    uplift_by_key: dict[str, list[dict[str, float]]] = field(default_factory=dict)
    observations: int = 0
    conditional_uplift_observations: int = 0
    version: str = SCHEDULER_VERSION
    policy_id: str = SCHEDULER_POLICY_ID

    def __post_init__(self) -> None:
        if not self.campaign_id:
            raise ValueError("Search V2 scheduler requires a campaign id")
        if self.version != SCHEDULER_VERSION or self.policy_id != SCHEDULER_POLICY_ID:
            raise ValueError("Search V2 scheduler version or policy drift")
        self.provenance = _verify_fresh_provenance(self.provenance)

    @classmethod
    def fresh(
        cls, campaign_id: str, provenance: Mapping[str, Any]
    ) -> "SearchV2SchedulerV1":
        return cls(campaign_id=str(campaign_id), provenance=dict(provenance))

    def observe(
        self,
        receipt: ProgramProposalReceiptV0,
        admission: AbsoluteEconomicAdmission,
        credit: ProgramUpliftCredit | None,
    ) -> None:
        if receipt.program_template_id == "BASE":
            raise ValueError("BASE parity is not Search V2 enhancer-credit eligible")
        if receipt.program_id != admission.program_id:
            raise ValueError("Search V2 receipt/admission program identity drift")
        if admission.admitted != (credit is not None):
            raise ValueError("Search V2 admission and conditional-credit domain drift")
        for kind in ("template", "program"):
            key = _admission_key(receipt, kind)
            self.admission_by_key.setdefault(key, []).append(bool(admission.admitted))
            if credit is not None:
                program_credit = credit.program_credit
                self.uplift_by_key.setdefault(key, []).append(
                    {
                        "matched_net_reward_increment": float(
                            program_credit["matched_net_reward_increment"]
                        ),
                        "matched_cumulative_net_return_increment": float(
                            program_credit[
                                "matched_cumulative_net_return_increment"
                            ]
                        ),
                        "cross_window_matched_consistency": float(
                            program_credit["cross_window_matched_consistency"]
                        ),
                        "robust_median_window_return_increment": float(
                            program_credit["robust_median_window_return_increment"]
                        ),
                        "lower_tail_window_return_increment": float(
                            program_credit["lower_tail_window_return_increment"]
                        ),
                        "turnover_differential": float(
                            program_credit["turnover_differential"]
                        ),
                    }
                )
        self.observations += 1
        if credit is not None:
            self.conditional_uplift_observations += 1

    def _selected_key(self, receipt: ProgramProposalReceiptV0) -> str:
        program_key = _admission_key(receipt, "program")
        if len(self.admission_by_key.get(program_key, ())) >= MINIMUM_HEAD_SUPPORT:
            return program_key
        return _admission_key(receipt, "template")

    def score_receipt(self, receipt: ProgramProposalReceiptV0) -> dict[str, Any]:
        key = self._selected_key(receipt)
        admissions = list(self.admission_by_key.get(key, ()))
        admitted_count = sum(admissions)
        admission_probability = (admitted_count + 1.0) / (len(admissions) + 2.0)
        uplifts = list(self.uplift_by_key.get(key, ()))
        if uplifts:
            reward_median = float(
                median(row["matched_net_reward_increment"] for row in uplifts)
            )
            return_median = float(
                median(
                    row["matched_cumulative_net_return_increment"] for row in uplifts
                )
            )
            consistency_median = float(
                median(row["cross_window_matched_consistency"] for row in uplifts)
            )
            robust_window_median = float(
                median(
                    row["robust_median_window_return_increment"] for row in uplifts
                )
            )
            lower_tail_median = float(
                median(row["lower_tail_window_return_increment"] for row in uplifts)
            )
            turnover_differential_median = float(
                median(row["turnover_differential"] for row in uplifts)
            )
        else:
            reward_median = 0.0
            return_median = 0.0
            consistency_median = 0.0
            robust_window_median = 0.0
            lower_tail_median = 0.0
            turnover_differential_median = 0.0
        positive_uplift = reward_median > 0.0 and return_median > 0.0
        eligible = bool(
            len(admissions) >= MINIMUM_HEAD_SUPPORT
            and len(uplifts) >= MINIMUM_HEAD_SUPPORT
            and admission_probability >= 0.5
            and positive_uplift
        )
        return {
            "program_template_id": receipt.program_template_id,
            "semantic_program_hash": receipt.semantic_program_hash,
            "head_key": key,
            "admission_head": {
                "support": len(admissions),
                "admitted_count": admitted_count,
                "probability": admission_probability,
            },
            "conditional_uplift_head": {
                "support": len(uplifts),
                "matched_reward_increment_median": reward_median,
                "matched_return_increment_median": return_median,
                "cross_window_consistency_median": consistency_median,
                "robust_window_return_increment_median": robust_window_median,
                "lower_tail_window_return_increment_median": lower_tail_median,
                "turnover_differential_median": turnover_differential_median,
                "positive_uplift": positive_uplift,
            },
            "conditional_uplift_exploit_eligible": eligible,
            "acquisition_order": [
                "positive_conditional_uplift",
                "matched_return_increment_median",
                "matched_reward_increment_median",
                "lower_tail_window_return_increment_median",
                "cross_window_consistency_median",
                "admission_probability",
            ],
            "acquisition_rank": [
                float(positive_uplift),
                return_median,
                reward_median,
                lower_tail_median,
                consistency_median,
                admission_probability,
            ],
            "scalar_absolute_plus_uplift_reward": None,
        }

    def snapshot(self) -> dict[str, Any]:
        payload = {
            "version": self.version,
            "policy_id": self.policy_id,
            "campaign_id": self.campaign_id,
            "provenance": dict(self.provenance),
            "observations": int(self.observations),
            "conditional_uplift_observations": int(
                self.conditional_uplift_observations
            ),
            "admission_by_key": {
                key: list(values) for key, values in sorted(self.admission_by_key.items())
            },
            "uplift_by_key": {
                key: [dict(row) for row in values]
                for key, values in sorted(self.uplift_by_key.items())
            },
            "selection_hierarchy": [
                "ABSOLUTE_ADMISSION_HEAD",
                "CONDITIONAL_PROGRAM_UPLIFT_HEAD",
                "TEMPLATE_THEN_PROGRAM_SCHEDULER",
            ],
            "serialized_optimizer_state_imported": False,
            "development_financial_observations_imported": False,
            "cross_campaign_state_import_allowed": False,
            "component_credit_authoritative": False,
            "formal_optimizer_authority": False,
            "formal_scheduler_authority": False,
            "validation_feedback_allowed": False,
        }
        payload["bandit_state_sha256"] = stable_hash(payload)
        return payload

    @classmethod
    def restore(
        cls,
        snapshot: Mapping[str, Any],
        *,
        expected_campaign_id: str | None = None,
    ) -> "SearchV2SchedulerV1":
        payload = dict(snapshot)
        expected_hash = str(payload.pop("bandit_state_sha256", ""))
        if not expected_hash or stable_hash(payload) != expected_hash:
            raise ValueError("Search V2 scheduler state self-hash drift")
        campaign_id = str(payload.get("campaign_id") or "")
        if expected_campaign_id is not None and campaign_id != str(expected_campaign_id):
            raise ValueError("Search V2 scheduler campaign identity drift")
        if any(
            bool(payload.get(field))
            for field in (
                "serialized_optimizer_state_imported",
                "development_financial_observations_imported",
                "cross_campaign_state_import_allowed",
                "component_credit_authoritative",
                "formal_optimizer_authority",
                "formal_scheduler_authority",
                "validation_feedback_allowed",
            )
        ):
            raise ValueError("Search V2 scheduler state grants forbidden authority")
        scheduler = cls(
            campaign_id=campaign_id,
            provenance=dict(payload.get("provenance") or {}),
            admission_by_key={
                str(key): [bool(value) for value in values]
                for key, values in dict(payload.get("admission_by_key") or {}).items()
            },
            uplift_by_key={
                str(key): [
                    {str(field): float(value) for field, value in row.items()}
                    for row in values
                ]
                for key, values in dict(payload.get("uplift_by_key") or {}).items()
            },
            observations=int(payload.get("observations") or 0),
            conditional_uplift_observations=int(
                payload.get("conditional_uplift_observations") or 0
            ),
            version=str(payload.get("version") or ""),
            policy_id=str(payload.get("policy_id") or ""),
        )
        if scheduler.snapshot()["bandit_state_sha256"] != expected_hash:
            raise ValueError("Search V2 scheduler exact restore drift")
        return scheduler
