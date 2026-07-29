from __future__ import annotations

import json
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
from our_system_phase2.services.a_share_tradability_guard import (
    A_SHARE_EXECUTABLE_REWARD_READY,
    A_SHARE_TRADABILITY_EVIDENCE_CLASS,
    A_SHARE_TRADABILITY_READY,
    REQUIRED_TRADABILITY_PROOFS,
    build_a_share_tradability_receipt,
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
    primary_executable_reward: float = 0.8,
    control_executable_reward: float = 0.7,
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
    receipt_by_id = {
        str(row["candidate_id"]): row for row in receipts
    }
    def replay_receipt(candidate_id: str, reward: float, turnover: float) -> dict[str, object]:
        return build_a_share_tradability_receipt(
            candidate_id=candidate_id,
            candidate_exact_identity=str(
                receipt_by_id[candidate_id]["exact_identity"]
            ),
            replay_code_sha256="a" * 64,
            input_data_sha256="b" * 64,
            universe_manifest_sha256="c" * 64,
            fee_schedule_sha256="d" * 64,
            execution_policy_sha256="e" * 64,
            corporate_action_policy_sha256="f" * 64,
            executable_net_reward=reward,
            train_read_count=10,
            trade_count=2,
            fill_count=2,
            blocked_buy_count=0,
            blocked_sell_count=0,
            extra={"a_share_mean_one_way_turnover": turnover},
        )
    rewards = [
        {
            **replay_receipt(
                str(primary["candidate_id"]),
                primary_executable_reward,
                0.4,
            ),
            "candidate_id": primary["candidate_id"],
            "optimizer_reward": primary_reward,
            "train_mean_one_way_turnover": 0.4,
            "train_rank_ic_mean": 0.03,
            "train_reward_decision": "TRAIN_REWARD_FOLLOWUP_READY",
            "train_reward_blockers": primary_standalone_blockers,
        },
        {
            **replay_receipt(
                str(control["candidate_id"]),
                control_executable_reward,
                0.2,
            ),
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


def test_phase3cn_clean_gate_blocks_primary_standalone_failure() -> None:
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
        "primary_executable_reward_decision": "",
        "primary_standalone_train_reward_blockers": "legacy_primary_blocker",
        "train_reward_decision": "TRAIN_REWARD_BLOCKED",
        "train_reward_blockers": "legacy_primary_blocker",
        "train_mean_one_way_turnover": 99.0,
        "evaluation_evidence_class": A_SHARE_TRADABILITY_EVIDENCE_CLASS,
        "a_share_tradability_decision": A_SHARE_TRADABILITY_READY,
        **{field: True for field in REQUIRED_TRADABILITY_PROOFS},
    }

    assert _is_clean(
        row,
        train_threshold=0.0,
        validation_floor=0.0,
        max_turnover=0.75,
    ) is False


def test_pair_feedback_does_not_require_finalist_tradability_evidence() -> None:
    inputs = _pair_inputs()
    for reward in inputs["reward_rows"]:
        reward.pop("replay_receipt_canonical_json")
        reward.pop("t_plus_one_enforced")

    row = build_pair_evaluation_rows(**inputs)[0]

    assert row["pair_evaluation_status"] == "PAIR_EVALUATED"
    assert row["pair_train_reward_decision"] == "PAIR_TRAIN_FEEDBACK_READY"
    assert "t_plus_one_enforced_not_proven" in row[
        "pair_a_share_tradability_blockers"
    ]
    assert row["pair_train_reward_blockers"] == ""
    assert row["optimizer_reward"] == pytest.approx(0.2)
    assert row["finalist_execution_eligible"] is False


def test_pair_feedback_binds_separate_immutable_replay_receipts() -> None:
    inputs = _pair_inputs()
    predictive_rows: list[dict[str, object]] = []
    replay_rows: list[dict[str, object]] = []
    for reward in inputs["reward_rows"]:
        canonical = str(reward["replay_receipt_canonical_json"])
        replay = json.loads(canonical)
        replay["replay_receipt_canonical_json"] = canonical
        replay["replay_receipt_payload_sha256"] = reward[
            "replay_receipt_payload_sha256"
        ]
        replay_rows.append(replay)
        predictive_rows.append(
            {
                "candidate_id": reward["candidate_id"],
                "optimizer_reward": reward["optimizer_reward"],
                "train_mean_one_way_turnover": reward[
                    "train_mean_one_way_turnover"
                ],
                "train_rank_ic_mean": reward["train_rank_ic_mean"],
                "train_reward_decision": reward.get(
                    "train_reward_decision", ""
                ),
                "train_reward_blockers": reward.get(
                    "train_reward_blockers", ""
                ),
            }
        )
    inputs["reward_rows"] = predictive_rows
    inputs["replay_receipt_rows"] = replay_rows

    row = build_pair_evaluation_rows(**inputs)[0]

    assert row["pair_evaluation_status"] == "PAIR_EVALUATED"
    assert row["pair_train_reward_decision"] == "PAIR_TRAIN_FEEDBACK_READY"
    assert (
        row["primary_executable_reward_decision"]
        == A_SHARE_EXECUTABLE_REWARD_READY
    )
    assert row["matched_train_increment"] == pytest.approx(0.2)
    assert row["a_share_matched_executable_increment"] == pytest.approx(0.1)
    assert row["optimizer_reward"] == pytest.approx(0.2)
    assert row["finalist_execution_eligible"] is True


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
    assert (
        row["primary_executable_reward_decision"]
        == A_SHARE_EXECUTABLE_REWARD_READY
    )
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

    # The synthetic Phase3CM run proves the development feedback boundary and
    # all route constructors. It deliberately remains ineligible for finalist
    # execution evidence because no replay receipt is fabricated here.
    assert result["status"] == "PASS"
    assert result["constructor_authority"] == "8/8"
    assert result["synthetic_end_to_end_runtime"] == "8/8"
    assert result["pair_native_phase3cn_feedback"] == "QUALIFIED"
    assert result["finalist_execution_evidence"] == (
        "NOT_QUALIFIED_MISSING_EXECUTABLE_REPLAY"
    )
    assert {row["route_id"] for row in result["routes"]} == set(ALL_RUNTIME_ROUTES)
    for row in result["routes"]:
        assert row["primary_evaluator_invocation_count"] == 1
        assert row["control_evaluator_invocation_count"] == 1
        assert row["pair_evaluation_status"] == "PAIR_EVALUATED"
        assert row["matched_train_increment_finite"] is True
        assert row["predictive_matched_increment_finite"] is True
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
        assert row["finalist_execution_eligible"] is False
        assert "replay_receipt_schema_version_not_ready" in row[
            "finalist_execution_blockers"
        ]
    assert result["data_access"] == {
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
