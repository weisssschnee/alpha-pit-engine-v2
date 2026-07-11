from __future__ import annotations

import pytest

from our_system_phase2.services.admission_diversity import AdmissionConfig, admit_candidates
from our_system_phase2.services.benchmark_competitor_harness import (
    CompetitorReproductionAdapter,
    default_benchmark_harness,
)
from our_system_phase2.services.coverage_metrics import compute_coverage_metrics
from our_system_phase2.services.hypothesis_lanes import default_nextgen_lane_registry


def _candidate(index: int, lane_id: str, *, exact: str | None = None, parent: str = "", fresh: bool = False) -> dict[str, object]:
    lanes = default_nextgen_lane_registry()
    lane = lanes.get(lane_id)
    return {
        "candidate_id": f"c{index:02d}",
        "lane_id": lane_id,
        "exact_identity": exact or f"exact-{index}",
        "semantic_key": f"semantic-{index % 4}",
        "lineage_key": f"{lane.lineage_namespace}:line-{index}",
        "expression": f"CSRank($field_{index})",
        "data_role": "development",
        "family_id": f"family-{index % 3}",
        "semantic_bucket": f"bucket-{index % 4}",
        "parent_id": parent,
        "fresh": fresh,
        "field_family": f"field-{index % 3}",
        "primitive_family": f"primitive-{index % 2}",
        "temporal_cell": f"temporal-{index % 3}",
        "event_state_family": f"event-{index % 2}",
        "plate_industry_family": "pit-group" if index % 2 else "none",
        "economic_hypothesis": f"economic-{index % 4}",
        "grammar_cell": f"grammar-{index % 5}",
    }


def test_lane_registry_has_isolated_contracts_and_no_performance_route() -> None:
    lanes = default_nextgen_lane_registry()
    contract = lanes.contract()

    assert contract["lane_count"] == 7
    assert contract["formal_performance_search_allowed"] is False
    assert contract["adaptive_policy_update_allowed"] is False
    assert len({row["archive_namespace"] for row in contract["lanes"]}) == 7
    assert all(row["policy_frozen"] for row in contract["lanes"])


def test_candidate_submission_rejects_performance_and_non_development_data() -> None:
    lanes = default_nextgen_lane_registry()
    row = _candidate(1, "temporal_program")
    row["reward"] = 1.0
    with pytest.raises(ValueError, match="forbidden"):
        lanes.validate_submission(row)
    row.pop("reward")
    row["data_role"] = "forward"
    with pytest.raises(ValueError, match="development-only"):
        lanes.validate_submission(row)


def test_stratified_admission_is_deterministic_and_one_identity_one_vote() -> None:
    lanes = default_nextgen_lane_registry()
    rows = [
        _candidate(0, "static_cross_sectional", fresh=True),
        _candidate(1, "temporal_program", fresh=True),
        _candidate(2, "event_conditioned", parent="p1"),
        _candidate(3, "state_transition", parent="p1"),
        _candidate(4, "orthogonal_exile", parent="p2"),
        _candidate(5, "competitor_reproduction", exact="exact-2", parent="p3"),
        _candidate(6, "mcts_evolutionary_challenger", parent="p4"),
    ]
    config = AdmissionConfig(5, family_cap=3, bucket_cap=2, parent_descendant_cap=1, fresh_budget_floor=2, exile_quota=1)

    first = admit_candidates(rows, lanes, config)
    second = admit_candidates(list(reversed(rows)), lanes, config)

    assert [row["candidate_id"] for row in first["selected"]] == [row["candidate_id"] for row in second["selected"]]
    assert first["duplicate_vote_count"] == 1
    assert first["fresh_selected_count"] >= 2
    assert first["exile_selected_count"] == 1
    assert first["performance_used"] is False


def test_benchmark_harness_is_budgeted_frozen_and_not_run() -> None:
    lanes = default_nextgen_lane_registry()
    harness = default_benchmark_harness(lanes)
    plan = harness.plan()

    assert len(harness.specs) == 16
    assert plan["proposal_budget"] > plan["strict_eval_budget"] > 0
    assert plan["policy_freeze"] is True
    assert plan["performance_comparison_executed"] is False
    assert all(spec.execution_state == "planned_not_run" for spec in harness.specs)


def test_external_competitor_adapter_requires_provenance_and_isolated_lane() -> None:
    lanes = default_nextgen_lane_registry()
    adapter = CompetitorReproductionAdapter("ext-1", "vendor", "v2", "abc123", "internal-test-only")
    submission = adapter.submit(_candidate(9, "competitor_reproduction"), lanes)

    assert submission["external_performance_imported"] is False
    assert submission["archive_namespace"] == "nextgen/competitor_reproduction"
    with pytest.raises(ValueError, match="competitor_reproduction lane"):
        adapter.submit(_candidate(9, "temporal_program"), lanes)


def test_coverage_metrics_are_non_performance_and_complete() -> None:
    rows = [_candidate(index, "temporal_program") for index in range(8)]
    metrics = compute_coverage_metrics(rows)

    assert metrics["field_family_coverage"]["distinct"] == 3
    assert metrics["primitive_family_coverage"]["distinct"] == 2
    assert metrics["grammar_cell_entropy"] > 0
    assert metrics["lineage_entropy"] > 0
    assert metrics["signal_cluster_potential"]["status"] == "structural_potential_only_no_signal_materialization"
    assert metrics["performance_used"] is False
    rows[0]["validation_score"] = 1.0
    with pytest.raises(ValueError, match="performance fields"):
        compute_coverage_metrics(rows)
