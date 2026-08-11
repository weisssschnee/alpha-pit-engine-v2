"""Honest provenance for development feedback reused across campaigns."""

from __future__ import annotations

from typing import Any


PROVENANCE_SCHEMA_VERSION = "cn_development_feedback_provenance_v1"


def build_development_feedback_provenance(
    *,
    serialized_optimizer_state_imported: bool,
    development_financial_observations_imported: bool,
    development_observation_count: int,
    candidate_results_imported: bool,
    factor_statistics_imported: bool,
    behavior_statistics_imported: bool,
    template_classification_imported: bool,
    manual_diagnosis_imported: bool,
    objective_designed_after_parent_results: bool,
) -> dict[str, Any]:
    """Build a complete provenance record without conflating state and observations."""

    count = int(development_observation_count)
    if count < 0:
        raise ValueError("development observation count cannot be negative")
    observations = bool(development_financial_observations_imported)
    if observations != (count > 0):
        raise ValueError(
            "development observation count must be positive exactly when financial "
            "observations were imported"
        )
    facts = {
        "serialized_optimizer_state_imported": bool(
            serialized_optimizer_state_imported
        ),
        "development_financial_observations_imported": observations,
        "development_observation_count": count,
        "candidate_results_imported": bool(candidate_results_imported),
        "factor_statistics_imported": bool(factor_statistics_imported),
        "behavior_statistics_imported": bool(behavior_statistics_imported),
        "template_classification_imported": bool(
            template_classification_imported
        ),
        "manual_diagnosis_imported": bool(manual_diagnosis_imported),
        "objective_designed_after_parent_results": bool(
            objective_designed_after_parent_results
        ),
    }
    cross_campaign = any(
        bool(value)
        for key, value in facts.items()
        if key != "development_observation_count"
    )
    return {
        "schema_version": PROVENANCE_SCHEMA_VERSION,
        **facts,
        "cross_campaign_development_feedback": cross_campaign,
    }


def classify_legacy_feedback_record(record: dict[str, Any]) -> dict[str, Any]:
    """Read a legacy seed row without treating state_imported=false as no feedback."""

    observation_applied = bool(
        record.get("bandit_update_applied")
        or record.get("bandit_observation_applied")
    )
    return build_development_feedback_provenance(
        serialized_optimizer_state_imported=bool(
            record.get("cross_campaign_state_imported", False)
        ),
        development_financial_observations_imported=observation_applied,
        development_observation_count=int(observation_applied),
        candidate_results_imported=True,
        factor_statistics_imported=False,
        behavior_statistics_imported=bool(record.get("behavior_statistics_imported")),
        template_classification_imported=bool(record.get("template_id")),
        manual_diagnosis_imported=False,
        objective_designed_after_parent_results=False,
    )
