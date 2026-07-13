from __future__ import annotations

import json
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
ACTIVE_STATUSES = [
    "BROAD_EVENT_CAPABILITY_NOT_EVALUATED",
    "CURRENT_EVENT_TRIGGER_SEMANTICS_MISMATCH",
    "EVENT_AUDIT_CONTRACT_INVALID",
    "EVENT_LANE_ZERO_BUDGET_NOT_EXECUTED",
    "EVENT_DATA_NOT_INVALIDATED",
]


def test_broad_event_status_supersedes_only_active_interpretation() -> None:
    status = json.loads(
        (REPO / "runtime/run_plans/cn_broad_event_recovery_status_v1.json").read_text(
            encoding="utf-8"
        )
    )

    assert status["active_status"] == ACTIVE_STATUSES
    assert status["execution_state"] == "GOVERNANCE_MAINTENANCE_ONLY_NOT_STARTED"
    historical = status["historical_evidence_contract"]
    assert historical["proposal_tables_immutable"] is True
    assert historical["strict_tables_immutable"] is True
    assert historical["performance_tables_immutable"] is True
    assert historical["epoch_c_event_proposal_budget"] == 0
    assert historical["epoch_c_event_admission_budget"] == 0
    assert historical["epoch_c_event_strict_budget"] == 0
    assert historical["interpretation"].startswith("zero event clusters means NOT_EXECUTED")


def test_active_architecture_does_not_repeat_retracted_event_denial() -> None:
    retracted = "EVENT_" + "GENERATOR_NOT_OPERATIONAL"
    active_assets = [
        ".planning/STATE.md",
        ".planning/architecture/CURRENT_ARCHITECTURE.md",
        ".planning/architecture/ARCHITECTURE_BOUNDARY.md",
        ".planning/architecture/EVOLUTION_MAP.md",
        ".planning/architecture/architecture_graph.json",
        ".planning/architecture/architecture_registry.json",
        "reports/cn_search_selection_event_state_sprint2_20260713/DECISION_CHANGE_LOG.md",
        "reports/cn_broad_event_recovery_20260713/DECISION_CHANGE_LOG.md",
    ]
    for relative in active_assets:
        assert retracted not in (REPO / relative).read_text(encoding="utf-8"), relative


def test_graph_records_legacy_path_and_completed_broad_recovery_separately() -> None:
    graph = json.loads((REPO / ".planning/architecture/architecture_graph.json").read_text(encoding="utf-8"))
    nodes = {row["id"]: row for row in graph["nodes"]}
    links = {(row["source"], row["target"], row["relation"]): row for row in graph["links"]}

    assert graph["graph"]["phase"] == "CN_BROAD_EVENT_SYSTEM_RECOVERY_COMPLETED_DISCOVERY_ELIGIBLE"
    assert nodes["sprint2_event_generator"]["status"] == "DEPRECATED"
    assert nodes["broad_event_recovery"]["status"] == "IMPLEMENTED"
    assert links[
        ("sprint2_event_generator", "broad_event_recovery", "superseded_by_semantic_recovery")
    ]["permission"] == "ALLOWED"
    assert links[("forward_2026", "broad_event_recovery", "forward_to_broad_event_policy")][
        "permission"
    ] == "FORBIDDEN"


def test_broad_event_closure_preserves_boundaries_and_reproducible_entry_pack() -> None:
    status = json.loads(
        (REPO / "runtime/run_plans/cn_broad_event_recovery_status_v2.json").read_text(
            encoding="utf-8"
        )
    )
    pack = json.loads(
        (REPO / "reports/cn_broad_event_recovery_20260713/DISCOVERY_ENTRY_PACK.json").read_text(
            encoding="utf-8"
        )
    )
    assert status["reproduced_mechanism_count"] == 11
    assert status["reproduced_new_behavior_cluster_count"] == 10
    assert pack["mechanism_count"] == 11
    assert pack["behavior_cluster_count"] == 10
    assert pack["boundaries"]["candidate_promotion"] is False
    assert pack["boundaries"]["forward_2026_access"] is False
    assert pack["boundaries"]["cross_sprint_memory"] is False
