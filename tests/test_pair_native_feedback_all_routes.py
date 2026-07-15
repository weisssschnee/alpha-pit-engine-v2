from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from our_system_phase2.runtime.phase3cn_feedback_memory_smoke import _is_clean
from our_system_phase2.services.candidate_submission_receipt import (
    CandidateSubmissionAuthority,
    ReceiptContext,
)
from our_system_phase2.services.fixed_split_authority import FixedSplitAuthority
from our_system_phase2.services.matched_control_pairs import (
    CandidatePairAuthority,
    build_pair_evaluation_rows,
    group_candidate_pairs,
)
from our_system_phase2.services.real_market_validation import (
    UnsupportedExpressionError,
    evaluate_panel_expression,
    frozen_replay_channel,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)
from our_system_phase2.services.unified_discovery_generators import RegistryDrivenGenerator
from scripts.build_cn_matched_control_candidate_parallel_artifacts import (
    ALL_RUNTIME_ROUTES,
    run_all_route_runtime_qualification,
)


REPO = Path(__file__).resolve().parents[1]
SPLIT = REPO / "runtime/run_plans/phase3ga_true1min_2024_2025_global_split_manifest.csv"
REGISTRY = (
    REPO
    / "reports/cn_unified_capability_discovery_20260714/completed_f8169e1/registry"
    / "unified_capability_registry.json"
)
EVALUATOR = REPO / "src/our_system_phase2/runtime/phase3cm_train_portfolio_sortino_reward_audit.py"
DATA_RELEASE_HASH = "cfb2742d975f2f6f1dcdf78d011f6d471b8d0e444164bae1d1816ba1fdcc5827"


def _pair_inputs(
    *,
    primary_reward: float = 0.3,
    control_reward: float = 0.1,
    primary_standalone_blockers: str = "",
) -> dict[str, object]:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    candidates = RegistryDrivenGenerator(registry).generate_route(
        "MINUTE_STATIC", proposal_budget=2, seed=811
    )
    for candidate in candidates:
        candidate["expression_hash"] = stable_hash(candidate["expression"])
    authority = CandidateSubmissionAuthority(
        registry,
        ReceiptContext.build(
            registry=registry,
            split_authority=FixedSplitAuthority.read(SPLIT),
            data_release_hash=DATA_RELEASE_HASH,
            evaluator_paths=[EVALUATOR],
        ),
    )
    receipts = authority.authorize_table(candidates)
    pair_receipts = CandidatePairAuthority().authorize_table(candidates, receipts)
    primary, control = group_candidate_pairs(candidates)[0]
    rewards = [
        {
            "candidate_id": primary["candidate_id"],
            "optimizer_reward": primary_reward,
            "train_mean_one_way_turnover": 0.4,
            "train_rank_ic_mean": 0.03,
            "train_reward_decision": "TRAIN_REWARD_FOLLOWUP_READY",
            "train_reward_blockers": primary_standalone_blockers,
        },
        {
            "candidate_id": control["candidate_id"],
            "optimizer_reward": control_reward,
            "train_mean_one_way_turnover": 0.2,
            "train_rank_ic_mean": 0.01,
        },
    ]
    support = {
        "shard_index": 0,
        "trade_time": "2024-01-02 09:35:00",
        "horizon_min": 1,
        "split": "train",
        "eligible_code_count": 4,
        "eligible_code_identity": stable_hash(["S1", "S2", "S3", "S4"]),
        "long_count": 2,
        "short_count": 2,
        "one_way_turnover": 0.2,
    }
    rows_by_hash = {
        str(primary["expression_hash"]): [
            {
                **support,
                "top_signal_mean": 1.0,
                "bottom_signal_mean": -1.0,
                "portfolio_weight_identity": "primary-weights",
            }
        ],
        str(control["expression_hash"]): [
            {
                **support,
                "top_signal_mean": 0.5,
                "bottom_signal_mean": -0.5,
                "portfolio_weight_identity": "control-weights",
            }
        ],
    }
    return {
        "candidates": candidates,
        "candidate_receipts": receipts,
        "pair_receipts": pair_receipts,
        "reward_rows": rewards,
        "portfolio_rows_by_expression_hash": rows_by_hash,
        "evaluator_invocation_counts": {
            str(primary["candidate_id"]): 1,
            str(control["candidate_id"]): 1,
        },
    }


def test_pair_feedback_blocks_nonpositive_matched_increment_even_when_primary_is_ready() -> None:
    row = build_pair_evaluation_rows(
        **_pair_inputs(primary_reward=0.3, control_reward=0.4)
    )[0]

    assert row["pair_evaluation_status"] == "PAIR_EVALUATED"
    assert row["matched_train_increment"] == pytest.approx(-0.1)
    assert row["pair_train_reward_decision"] == "PAIR_TRAIN_FEEDBACK_BLOCKED"
    assert "matched_train_increment_nonpositive" in row["pair_train_reward_blockers"]
    assert row["optimizer_reward"] == ""


def test_phase3cn_clean_gate_uses_pair_native_fields_not_primary_standalone_diagnostics() -> None:
    row = {
        "expression": "Sign(ZScore($close))",
        "pair_evaluation_status": "PAIR_EVALUATED",
        "pair_train_reward": 0.2,
        "pair_train_reward_decision": "PAIR_TRAIN_FEEDBACK_READY",
        "pair_train_reward_blockers": "",
        "pair_turnover_metric": 0.4,
        "pair_support_metric": 1.0,
        "pair_rank_ic_metric": 0.01,
        "optimizer_reward": 0.2,
        "primary_standalone_train_reward_decision": "TRAIN_REWARD_BLOCKED",
        "primary_standalone_train_reward_blockers": "legacy_primary_blocker",
        "train_reward_decision": "TRAIN_REWARD_BLOCKED",
        "train_reward_blockers": "legacy_primary_blocker",
        "train_mean_one_way_turnover": 99.0,
    }

    assert _is_clean(
        row,
        train_threshold=0.0,
        validation_floor=0.0,
        max_turnover=0.75,
    ) is True


def test_pair_feedback_blocks_support_mismatch_and_missing_control_invocation() -> None:
    support_mismatch = _pair_inputs()
    control_hash = next(
        key
        for key in support_mismatch["portfolio_rows_by_expression_hash"]
        if key
        != next(iter(support_mismatch["portfolio_rows_by_expression_hash"]))
    )
    support_mismatch["portfolio_rows_by_expression_hash"][control_hash][0][
        "eligible_code_identity"
    ] = stable_hash(["S1", "S2", "S3", "S9"])
    mismatch_row = build_pair_evaluation_rows(**support_mismatch)[0]
    assert mismatch_row["pair_train_reward_decision"] == "PAIR_TRAIN_FEEDBACK_BLOCKED"
    assert "pair_support_mismatch" in mismatch_row["pair_train_reward_blockers"]

    not_invoked = _pair_inputs()
    control_id = str(not_invoked["candidates"][1]["candidate_id"])
    not_invoked["evaluator_invocation_counts"][control_id] = 0
    not_invoked_row = build_pair_evaluation_rows(**not_invoked)[0]
    assert not_invoked_row["pair_train_reward_decision"] == "PAIR_TRAIN_FEEDBACK_BLOCKED"
    assert "control_evaluator_not_invoked" in not_invoked_row["pair_train_reward_blockers"]


def test_pair_feedback_ready_ignores_primary_standalone_blocker_but_keeps_diagnostic() -> None:
    row = build_pair_evaluation_rows(
        **_pair_inputs(primary_standalone_blockers="legacy_primary_blocker")
    )[0]

    assert row["pair_train_reward_decision"] == "PAIR_TRAIN_FEEDBACK_READY"
    assert row["pair_train_reward_blockers"] == ""
    assert row["primary_standalone_train_reward_blockers"] == "legacy_primary_blocker"
    assert "train_reward_decision" not in row
    assert "train_reward_blockers" not in row
    assert "train_mean_one_way_turnover" not in row


def test_frozen_replay_uses_independent_materialized_primary_and_control_channels() -> None:
    field_id = "broad_event_test_mechanism"
    primary_channel = frozen_replay_channel(field_id, is_control=False)
    control_channel = frozen_replay_channel(field_id, is_control=True)
    frame = pd.DataFrame(
        {
            "code": ["S1", "S2", "S1", "S2"],
            "trade_time": pd.to_datetime(
                [
                    "2024-01-02 09:31:00",
                    "2024-01-02 09:31:00",
                    "2024-01-02 09:32:00",
                    "2024-01-02 09:32:00",
                ]
            ),
            field_id: [1.0, 1.0, 1.0, 1.0],
            primary_channel: [3.0, -2.0, 4.0, -1.0],
            control_channel: [-0.5, 0.5, -0.25, 0.25],
        }
    )

    primary = evaluate_panel_expression(
        frame,
        f"FrozenMechanismReplay(${field_id})",
        data_role="development",
    )
    control = evaluate_panel_expression(
        frame,
        f"MatchedControlReplay(${field_id})",
        data_role="development",
    )

    assert primary.tolist() == [3.0, -2.0, 4.0, -1.0]
    assert control.tolist() == [-0.5, 0.5, -0.25, 0.25]
    with pytest.raises(UnsupportedExpressionError, match="missing_frozen_replay_channel"):
        evaluate_panel_expression(
            frame.drop(columns=[control_channel]),
            f"MatchedControlReplay(${field_id})",
            data_role="development",
        )


def test_all_eight_routes_reach_real_phase3cm_and_pair_native_phase3cn_validation(
    tmp_path: Path,
) -> None:
    result = run_all_route_runtime_qualification(
        registry_path=REGISTRY,
        split_manifest_path=SPLIT,
        run_root=tmp_path,
        repo_sha="synthetic-test-repo-sha",
    )

    assert result["status"] == "PASS"
    assert result["constructor_authority"] == "8/8"
    assert result["synthetic_end_to_end_runtime"] == "8/8"
    assert {row["route_id"] for row in result["routes"]} == set(ALL_RUNTIME_ROUTES)
    for row in result["routes"]:
        assert row["primary_evaluator_invocation_count"] == 1
        assert row["control_evaluator_invocation_count"] == 1
        assert row["pair_evaluation_status"] == "PAIR_EVALUATED"
        assert row["matched_train_increment_finite"] is True
        assert row["pair_support_overlap"] == pytest.approx(1.0)
        assert row["pair_train_reward_decision"] in {
            "PAIR_TRAIN_FEEDBACK_READY",
            "PAIR_TRAIN_FEEDBACK_BLOCKED",
        }
        assert row["primary_control_exact_equivalent"] is False
        assert row["primary_control_behavior_equivalent"] is False
        assert row["phase3cn_pair_feedback_validation"] in {
            "ACCEPTED_READY_PAIR",
            "REJECTED_BLOCKED_PAIR_AS_DESIGNED",
        }
    assert result["data_access"] == {
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
