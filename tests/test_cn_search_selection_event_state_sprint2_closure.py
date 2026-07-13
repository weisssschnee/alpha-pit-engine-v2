from __future__ import annotations

import json
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]


def test_sprint2_manifest_records_partial_contract_and_sealed_boundaries() -> None:
    manifest = json.loads(
        (REPO / "runtime/run_plans/cn_search_selection_event_state_sprint2_manifest_v1.json")
        .read_text(encoding="utf-8")
    )

    assert manifest["status"] == "CN_SEARCH_SELECTION_EVENT_STATE_SPRINT2_PARTIALLY_COMPLETED"
    epoch = manifest["epoch_c"]
    assert epoch["global_unique_exact_actual"] == 32739
    assert epoch["global_unique_exact_expected"] == 32768
    assert epoch["unique_underfill"] == 29
    assert epoch["shared_exact_backbone_actual"] == epoch["shared_exact_backbone_expected"] == 8192
    assert epoch["strict_actual"] == epoch["strict_expected"] == 1024
    assert epoch["selector_realized_wins_vs_scalar"] == 3
    assert epoch["rx_ucb_strict_wins_vs_control"] == 3
    assert epoch["zero_forbidden_access_all_seeds"] is True
    assert manifest["event_state_support"]["event_decision"] == "EVENT_GENERATOR_NOT_OPERATIONAL"
    assert manifest["epoch_d"]["state"] == "NOT_RUN_CONDITIONS_NOT_MET"
    assert manifest["closure"]["candidate_pack_ready_for_separate_forward_authorization"] is False
    assert set(manifest["immutable_boundaries"].values()) == {False}


def test_sprint2_joint_analysis_is_research_positive_but_contract_partial() -> None:
    result = json.loads(
        (REPO / "reports/cn_search_selection_event_state_sprint2_20260713/epoch_c_joint_analysis_v1.json")
        .read_text(encoding="utf-8")
    )

    assert result["final_status"] == "CN_SEARCH_SELECTION_EVENT_STATE_SPRINT2_PARTIALLY_COMPLETED"
    assert result["success_gate_pass_count"] == 10
    assert result["access_safety_pass"] is True
    assert result["contract_fidelity_pass"] is False
    assert result["proposal_union"]["unique_exact_identity_union_count"] == 32739
    assert result["strict_budget"] == {"actual": 1024, "expected": 1024, "met": True}
    assert result["selector_realized_win_seed_count_vs_scalar"] == 3
    assert result["rx_ucb_strict_win_seed_count"] == 3
    assert result["state_independent_economic_increment"]["gate_passed"] is False
    assert result["event_decision"] == "EVENT_GENERATOR_NOT_OPERATIONAL"
    assert result["candidate_pack_ready_for_separate_forward_authorization"] is False
    assert result["epoch_d"]["authorized"] is False
    assert result["boundaries"] == {
        "candidate_promotion_made": False,
        "cross_sprint_adaptive_memory_written": False,
        "forward_2026_sealed": True,
    }


def test_sprint2_graph_contains_selector_generators_epoch_and_frozen_pack() -> None:
    graph = json.loads((REPO / ".planning/graphs/graph.json").read_text(encoding="utf-8"))
    nodes = {row["id"]: row for row in graph["nodes"]}
    links = {(row["source"], row["target"], row["relation"]): row for row in graph["links"]}

    assert graph["graph"]["phase"] == "CN_BROAD_EVENT_SYSTEM_RECOVERY_AUTHORIZED_NOT_STARTED"
    assert nodes["sprint2_strict_priority_selector"]["status"] == "IMPLEMENTED"
    assert nodes["sprint2_event_generator"]["status"] == "DEPRECATED"
    assert nodes["broad_event_recovery"]["status"] == "PLANNED"
    assert nodes["sprint2_state_generator"]["status"] == "PARTIAL"
    assert nodes["sprint2_rx_ucb"]["status"] == "IMPLEMENTED"
    assert nodes["sprint2_epoch_c"]["status"] == "PARTIAL"
    assert nodes["sprint2_research_pack"]["status"] == "FROZEN"
    assert links[("forward_2026", "sprint2_epoch_c", "forward_to_sprint2_search_policy")]["permission"] == "FORBIDDEN"
    assert links[("sprint2_research_pack", "scheduler_memory_feedback", "report_only_to_positive_memory")]["permission"] == "FORBIDDEN"
