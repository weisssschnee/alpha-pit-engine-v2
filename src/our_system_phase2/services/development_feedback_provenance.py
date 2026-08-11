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


def verify_or_classify_development_feedback(
    *,
    feedback_rows: list[dict[str, Any]],
    initial_bandit_observations: int,
    contract_provenance: dict[str, Any] | None,
    access_provenance: dict[str, Any] | None,
    behavior_statistics_imported: bool,
    manual_diagnosis_imported: bool,
    objective_designed_after_parent_results: bool,
) -> dict[str, Any]:
    """Verify V1 provenance or derive an explicit non-authoritative legacy view."""

    embedded_flags = ["development_feedback_provenance" in row for row in feedback_rows]
    if any(embedded_flags) and not all(embedded_flags):
        raise ValueError("mixed embedded and legacy development feedback provenance")
    embedded = bool(feedback_rows) and all(embedded_flags)
    classified_rows: list[dict[str, Any]] = []
    for row in feedback_rows:
        if embedded:
            observed = bool(
                row.get("bandit_update_applied")
                or row.get("bandit_observation_applied")
            )
            expected = build_development_feedback_provenance(
                serialized_optimizer_state_imported=False,
                development_financial_observations_imported=observed,
                development_observation_count=int(observed),
                candidate_results_imported=True,
                factor_statistics_imported=False,
                behavior_statistics_imported=(
                    bool(row.get("development_feedback_provenance", {}).get(
                        "behavior_statistics_imported"
                    ))
                ),
                template_classification_imported=True,
                manual_diagnosis_imported=manual_diagnosis_imported,
                objective_designed_after_parent_results=(
                    objective_designed_after_parent_results
                ),
            )
            if dict(row["development_feedback_provenance"]) != expected:
                raise ValueError("development feedback row provenance drift")
            classified_rows.append(expected)
        else:
            legacy = classify_legacy_feedback_record(row)
            legacy["manual_diagnosis_imported"] = manual_diagnosis_imported
            legacy["objective_designed_after_parent_results"] = (
                objective_designed_after_parent_results
            )
            legacy["cross_campaign_development_feedback"] = any(
                bool(value)
                for key, value in legacy.items()
                if key
                not in {
                    "schema_version",
                    "development_observation_count",
                    "cross_campaign_development_feedback",
                }
            )
            classified_rows.append(legacy)
    observed_count = sum(
        int(row["development_observation_count"]) for row in classified_rows
    )
    if observed_count != int(initial_bandit_observations):
        raise ValueError("development observation count does not match bandit state")
    expected_aggregate = build_development_feedback_provenance(
        serialized_optimizer_state_imported=False,
        development_financial_observations_imported=observed_count > 0,
        development_observation_count=observed_count,
        candidate_results_imported=bool(feedback_rows),
        factor_statistics_imported=False,
        behavior_statistics_imported=behavior_statistics_imported,
        template_classification_imported=bool(feedback_rows),
        manual_diagnosis_imported=manual_diagnosis_imported,
        objective_designed_after_parent_results=(
            objective_designed_after_parent_results
        ),
    )
    aggregate_presence = (
        contract_provenance is not None,
        access_provenance is not None,
    )
    if aggregate_presence == (True, True):
        if (
            dict(contract_provenance or {}) != expected_aggregate
            or dict(access_provenance or {}) != expected_aggregate
        ):
            raise ValueError("aggregate development feedback provenance drift")
        classification = "EMBEDDED_V1_VERIFIED"
    elif aggregate_presence == (False, False) and not embedded:
        classification = "LEGACY_DERIVED_NON_AUTHORITATIVE"
    else:
        raise ValueError("partial development feedback provenance is forbidden")
    return {
        "status": classification,
        "initial_bandit_observations": observed_count,
        "aggregate": expected_aggregate,
        "feedback_record_count": len(feedback_rows),
    }
