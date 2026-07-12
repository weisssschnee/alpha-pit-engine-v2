from __future__ import annotations

import json
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]


def test_sprint1_manifest_closes_formal_canary_and_keeps_sealed_boundaries() -> None:
    manifest = json.loads(
        (REPO / "runtime/run_plans/cn_generator_research_sprint1_manifest_v1.json").read_text(
            encoding="utf-8"
        )
    )
    closure = manifest["formal_canary_closure"]
    ledger = closure["read_ledger"]

    assert manifest["status"] == "CN_GENERATOR_RESEARCH_SPRINT1_PARTIALLY_COMPLETED"
    assert closure["status"] == "CN_B1S_CANARY_COMPLETED_WITH_NATURAL_UNDERFILL"
    assert closure["tests_passed"] == 213
    assert (
        closure["proposal_count"],
        closure["legal_count"],
        closure["exact_identity_count"],
        closure["signal_cluster_count"],
        closure["development_survivor_count"],
        closure["strict_candidate_count"],
    ) == (1344, 1187, 1094, 279, 863, 64)
    assert set(ledger.values()) == {0}
    assert manifest["authorization"]["maximum_experiment_rounds"] == 3
    assert manifest["authorization"]["experiment_rounds_used"] == 3
    assert [row["state"] for row in manifest["authorization"]["rounds"]] == [
        "COMPLETED", "COMPLETED", "COMPLETED"
    ]
    assert manifest["authorization"]["epoch_b"]["state"] == "NOT_RUN_ROUND_BUDGET_EXHAUSTED"
    assert manifest["data_boundary"]["forward_2026_allowed"] is False
    assert manifest["data_boundary"]["candidate_promotion_allowed"] is False
    assert manifest["data_boundary"]["cross_sprint_adaptive_memory_allowed"] is False


def test_sprint1_epoch_a_closure_records_partial_result_without_forward_pack() -> None:
    result = json.loads(
        (REPO / "reports/cn_generator_research_sprint1_20260712/epoch_a_result_v1.json").read_text(
            encoding="utf-8"
        )
    )
    assert result["sprint_status"] == "CN_GENERATOR_RESEARCH_SPRINT1_PARTIALLY_COMPLETED"
    assert result["recommendation"] == "CONTINUE_GENERATOR_RESEARCH_SPRINT"
    assert result["epoch_budget"] == {
        "seed_count": 2,
        "proposal_total": 16384,
        "admission_total": 2048,
        "strict_eval_total": 512,
    }
    assert all(row["strict_count"] == 256 for row in result["seed_results"].values())
    assert result["acceptance"]["at_least_one_adaptive_lane_beats_control_across_seeds"] is True
    assert result["acceptance"]["new_clusters_per_strict_improved"] is False
    assert result["acceptance"]["final_forward_pack_authorized"] is False
    assert set(result["boundaries"].values()) <= {0, False, True}
    assert result["boundaries"]["forward_2026_sealed"] is True


def test_sprint1_current_graph_contains_release_canary_and_research_nodes() -> None:
    graph = json.loads((REPO / ".planning/graphs/graph.json").read_text(encoding="utf-8"))
    nodes = {row["id"]: row for row in graph["nodes"]}

    assert graph["graph"]["phase"] == "CN_GENERATOR_RESEARCH_SPRINT1_PARTIALLY_COMPLETED"
    assert graph["graph"]["graph_type"] == (
        "CN_GENERATOR_RESEARCH_SPRINT1_CURRENT_ARCHITECTURE_CONTRACT"
    )
    assert nodes["development_only_release"]["status"] == "IMPLEMENTED"
    assert nodes["formal_b1s_canary"]["status"] == "IMPLEMENTED"
    assert nodes["generator_research_sprint1"]["status"] == "PARTIAL"
    assert nodes["formal_search_frozen"]["status"] == "FROZEN"
