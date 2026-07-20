from __future__ import annotations

import csv
import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from our_system_phase2.services.phase3cm_streaming_resource_contract import (
    FrozenExecutionPlan,
)
from our_system_phase2.services.phase3cm_streaming_dag import SharedMultiCandidateDAGPlan
from scripts import freeze_cn_phase3cm_phase_e_plans as freezer
from scripts.run_cn_phase3cm_streaming_qualification import _candidate_pairs


def _source_plan(*, phase: str, compute_threads: int) -> FrozenExecutionPlan:
    return FrozenExecutionPlan.create(
        phase=phase,
        block_size=5,
        block_boundaries=(("2024-01-02", "2024-01-08"),),
        pair_batches=(("template",),),
        heavy_processes=2,
        compute_threads=compute_threads,
        primary_thread_pool="numba",
        cache_caps={"dag_block_cache_bytes": 1024},
        checkpoint_every_blocks=1,
        rss_soft_bytes=10,
        rss_hard_bytes=20,
        global_rss_hard_bytes=40,
    )


def _binding() -> dict[str, object]:
    pairs: list[dict[str, object]] = []
    members: list[dict[str, object]] = []
    specifications = (
        (
            "active.1",
            "active_bar",
            "CSRank(Persistence($x,5))",
            "Persistence($x,5)",
        ),
        ("active.2", "active_bar", "CSRank(Delta($x,5))", "Delta($x,5)"),
        (
            "active.3",
            "active_bar",
            "CSRank(Persistence($y,5))",
            "Persistence($y,5)",
        ),
        ("session.1", "stock_session", "CSRank(Delta($fund,1))", "$fund"),
        ("session.2", "stock_session", "CSRank(Slope($fund,4))", "$fund"),
    )
    for ordinal, (pair_id, clock, primary_expression, control_expression) in enumerate(
        specifications, start=1
    ):
        primary_id = f"{pair_id}.primary"
        control_id = f"{pair_id}.control"
        pairs.append(
            {
                "preflight_ordinal": ordinal,
                "pair_id": pair_id,
                "candidate_id": primary_id,
                "control_candidate_id": control_id,
                "clock_namespace": clock,
                "support_unit": "symbol_day",
                "mapping_portfolio_contract": "long_only_top",
            }
        )
        field = "fund" if clock == "stock_session" else primary_expression.split("$")[1].split(",")[0].split(")")[0]
        for role, candidate_id, expression in (
            ("PRIMARY", primary_id, primary_expression),
            ("CONTROL", control_id, control_expression),
        ):
            members.append(
                {
                    "pair_id": pair_id,
                    "pair_member_role": role,
                    "candidate_id": candidate_id,
                    "clock_namespace": clock,
                    "expression": expression,
                    "canonical_expression": expression,
                    "field_ids": [field],
                    "maturity_contract": "BAR_CLOSE" if clock == "active_bar" else "SESSION_OPEN",
                    "clock_contract": clock,
                    "outer_mapping": "cross_sectional",
                    "pair_support_alignment_policy": "PRIMARY_CONTROL_FINITE_INTERSECTION",
                    "portfolio_mode": "long_only_top",
                }
            )
    binding: dict[str, object] = {
        "status": "CN_STREAMING_REPAIR_FROZEN_INPUT_BOUND",
        "pair_count": len(pairs),
        "sealed_reads": {"validation": 0, "holdout": 0, "forward_2026": 0},
        "pairs": pairs,
        "candidate_members": members,
    }
    binding["binding_hash"] = freezer._stable_hash(binding)
    return binding


def test_pair_union_lifetime_order_is_deterministic_exactly_adjudicated_and_ignores_diagnostics() -> None:
    binding = _binding()
    first = freezer._dag_cache_greedy_order(
        pairs=binding["pairs"],
        candidate_rows=binding["candidate_members"],
        clock_namespace="active_bar",
        maximum_batch_size=2,
    )
    pairs = [dict(row, reward=999.0, label="future") for row in binding["pairs"]]
    members = [
        dict(row, performance_rank=-1, selector_decision="KEEP")
        for row in binding["candidate_members"]
    ]
    second = freezer._dag_cache_greedy_order(
        pairs=pairs,
        candidate_rows=members,
        clock_namespace="active_bar",
        maximum_batch_size=2,
    )
    assert first["strategy"] == "pair_union_lifetime_greedy_exact_adjudicated"
    assert first["proposal_model"] == "matched_pair_union_atomic_release"
    assert first["exact_adjudication_authority"] == (
        "predict_dag_cache_peak_primary_then_control_singleton_release"
    )
    assert first["heuristic_pair_union_peak_owned_nodes"] > 0
    assert first["exact_predicted_peak_owned_arrays"] > 0
    assert first["exact_candidate_order_hash"] == first[
        "capacity_candidate_order_hash"
    ]
    assert len(first["exact_capacity_preflight_hash_at_one_row"]) == 64
    assert len(first["capacity_candidate_order_hash"]) == 64
    assert first["pair_batches"] == second["pair_batches"]
    assert first["candidate_member_order_hash"] == second["candidate_member_order_hash"]
    assert first["ordering_contract_hash"] == second["ordering_contract_hash"]
    reversed_input = freezer._dag_cache_greedy_order(
        pairs=list(reversed(binding["pairs"])),
        candidate_rows=list(reversed(binding["candidate_members"])),
        clock_namespace="active_bar",
        maximum_batch_size=2,
    )
    assert first["pair_batches"] == reversed_input["pair_batches"]
    assert first["input_pair_order_hash"] != reversed_input["input_pair_order_hash"]
    assert {
        pair_id for batch in first["pair_batches"] for pair_id in batch
    } == {"active.1", "active.2", "active.3"}
    for row in first["pair_member_order"]:
        assert row[1] == "PRIMARY"
        assert row[3] == "CONTROL"


def test_dag_cache_greedy_order_rejects_pair_identity_drift() -> None:
    binding = _binding()
    members = [dict(row) for row in binding["candidate_members"]]
    members[0]["candidate_id"] = "wrong.primary"
    with pytest.raises(ValueError, match="primary candidate identity drift"):
        freezer._dag_cache_greedy_order(
            pairs=binding["pairs"],
            candidate_rows=members,
            clock_namespace="active_bar",
            maximum_batch_size=2,
        )


def test_pair_union_heuristic_peak_is_not_treated_as_exact_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binding = _binding()
    observed_candidate_order: list[tuple[str, ...]] = []

    def exact_adjudicator(_dag, *, ordered_candidate_ids, **_kwargs):
        observed_candidate_order.append(tuple(ordered_candidate_ids))
        return SimpleNamespace(
            candidate_order_hash="c" * 64,
            predicted_peak_owned_arrays=97,
            preflight_hash="d" * 64,
        )

    monkeypatch.setattr(freezer, "predict_dag_cache_peak", exact_adjudicator)
    result = freezer._dag_cache_greedy_order(
        pairs=binding["pairs"],
        candidate_rows=binding["candidate_members"],
        clock_namespace="active_bar",
        maximum_batch_size=2,
    )

    assert len(observed_candidate_order) == 1
    assert result["heuristic_pair_union_peak_owned_nodes"] != 97
    assert result["exact_predicted_peak_owned_arrays"] == 97
    assert result["exact_candidate_order_hash"] == "c" * 64
    assert result["exact_capacity_preflight_hash_at_one_row"] == "d" * 64


def test_phase_e_template_accepts_phase_d_or_e_and_preserves_11_2_threads() -> None:
    active = freezer._phase_e(
        _source_plan(phase="E", compute_threads=11),
        heavy_processes=2,
        pair_batches=(("active.1", "active.2"),),
    )
    session = freezer._phase_e(
        _source_plan(phase="D", compute_threads=2),
        heavy_processes=2,
        pair_batches=(("session.1",),),
    )
    assert (active.phase, active.compute_threads) == ("E", 11)
    assert (session.phase, session.compute_threads) == ("E", 2)


def test_runtime_dag_relevant_contracts_change_the_frozen_dag_identity() -> None:
    binding = _binding()
    baseline = freezer._dag_cache_greedy_order(
        pairs=binding["pairs"],
        candidate_rows=binding["candidate_members"],
        clock_namespace="active_bar",
        maximum_batch_size=2,
    )
    active_rows = [
        row for row in binding["candidate_members"] if row["clock_namespace"] == "active_bar"
    ]
    assert baseline["dag_plan_hash"] == SharedMultiCandidateDAGPlan.build(
        active_rows
    ).plan_hash
    changed = [dict(row) for row in binding["candidate_members"]]
    changed[0]["maturity_contract"] = "FIRST_N_END"
    observed = freezer._dag_cache_greedy_order(
        pairs=binding["pairs"],
        candidate_rows=changed,
        clock_namespace="active_bar",
        maximum_batch_size=2,
    )
    assert observed["dag_plan_hash"] != baseline["dag_plan_hash"]


def test_freezer_candidate_normalization_matches_runtime_phase_e_parser() -> None:
    binding = _binding()
    rows = [
        row for row in binding["candidate_members"] if row["clock_namespace"] == "active_bar"
    ]
    assert freezer._runtime_candidate_pairs(
        rows,
        pair_count=3,
        clock_namespace="active_bar",
    ) == _candidate_pairs(
        rows,
        pair_limit=3,
        clock="active_bar",
        require_exact_count=True,
    )


def test_source_plan_aliases_write_hashed_dag_order_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    active_path = tmp_path / "active.json"
    session_path = tmp_path / "session.json"
    binding_path = tmp_path / "binding.json"
    output_root = tmp_path / "out"
    active_path.write_text(
        json.dumps(_source_plan(phase="E", compute_threads=11).to_dict()),
        encoding="utf-8",
    )
    session_path.write_text(
        json.dumps(_source_plan(phase="D", compute_threads=2).to_dict()),
        encoding="utf-8",
    )
    binding = _binding()
    for clock, name in (
        ("active_bar", "preflight_active_candidates.csv"),
        ("stock_session", "preflight_session_candidates.csv"),
    ):
        path = tmp_path / name
        rows = [
            {
                **row,
                "field_ids": json.dumps(row["field_ids"]),
            }
            for row in binding["candidate_members"]
            if row["clock_namespace"] == clock
        ]
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        binding.setdefault("artifacts", []).append(
            {
                "path": name,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    binding.pop("binding_hash", None)
    binding["binding_hash"] = freezer._stable_hash(binding)
    binding_path.write_text(json.dumps(binding), encoding="utf-8")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "freeze",
            "--active-source-plan",
            str(active_path),
            "--session-source-plan",
            str(session_path),
            "--binding",
            str(binding_path),
            "--repo-sha",
            "b" * 40,
            "--output-root",
            str(output_root),
            "--pair-batch-size",
            "2",
        ],
    )
    assert freezer.main() == 0
    contract = json.loads(
        (output_root / "CN_PHASE_E_COMBINED_EXECUTION_CONTRACT.json").read_text(
            encoding="utf-8"
        )
    )
    assert contract["ordering_strategy"] == (
        "pair_union_lifetime_greedy_exact_adjudicated"
    )
    assert contract["pair_member_execution_order"] == ["PRIMARY", "CONTROL"]
    assert contract["compute_threads_by_backend"] == {
        "active_bar": 11,
        "stock_session": 2,
    }
    assert contract["plans"]["active_bar"]["source_execution_plan_phase"] == "E"
    assert contract["plans"]["stock_session"]["source_execution_plan_phase"] == "D"
    for backend in ("active_bar", "stock_session"):
        row = contract["plans"][backend]
        assert len(row["source_execution_plan_sha256"]) == 64
        assert len(row["input_pair_order_hash"]) == 64
        assert len(row["execution_pair_order_hash"]) == 64
        assert len(row["ordering_contract_hash"]) == 64
        assert len(row["exact_candidate_order_hash"]) == 64
        assert row["heuristic_pair_union_peak_owned_nodes"] > 0
        assert row["exact_predicted_peak_owned_arrays"] > 0
