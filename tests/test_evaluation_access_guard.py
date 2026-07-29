from __future__ import annotations

import csv
from pathlib import Path

import pytest

from our_system_phase2.runtime.phase3cn_feedback_memory_smoke import build_feedback_memory
from our_system_phase2.services.evaluation_access_guard import (
    EvaluationAccessViolation,
    assert_train_only_feedback_rows,
    project_train_only_feedback_row,
)
from our_system_phase2.services.multi_arm_scheduler import build_arm_schedule
from our_system_phase2.services.search_feedback import build_search_feedback_context
from our_system_phase2.services.search_feedback import clean_optimizer_feedback_rows
from our_system_phase2.services.a_share_tradability_guard import (
    A_SHARE_EXECUTABLE_REWARD_READY,
    A_SHARE_TRADABILITY_EVIDENCE_CLASS,
    A_SHARE_TRADABILITY_READY,
    REQUIRED_TRADABILITY_PROOFS,
    a_share_tradability_blockers,
    build_a_share_tradability_receipt,
    pair_tradability_evidence,
    prefixed_tradability_evidence,
)
from our_system_phase2.services.matched_control_pairs import (
    MATCHED_OPTIMIZER_REWARD_METRIC,
    MATCHED_OPTIMIZER_REWARD_SOURCE,
)


def _reward_row(**overrides: str) -> dict[str, object]:
    receipt = build_a_share_tradability_receipt(
        candidate_id="c1",
        candidate_exact_identity="exact-c1",
        replay_code_sha256="a" * 64,
        input_data_sha256="b" * 64,
        universe_manifest_sha256="c" * 64,
        fee_schedule_sha256="d" * 64,
        execution_policy_sha256="e" * 64,
        corporate_action_policy_sha256="f" * 64,
        executable_net_reward=0.25,
        train_read_count=10,
        trade_count=2,
        fill_count=2,
        blocked_buy_count=0,
        blocked_sell_count=0,
        extra={"a_share_mean_one_way_turnover": 0.2},
    )
    control_receipt = build_a_share_tradability_receipt(
        candidate_id="c1-control",
        candidate_exact_identity="exact-c1-control",
        replay_code_sha256="a" * 64,
        input_data_sha256="b" * 64,
        universe_manifest_sha256="c" * 64,
        fee_schedule_sha256="d" * 64,
        execution_policy_sha256="e" * 64,
        corporate_action_policy_sha256="f" * 64,
        executable_net_reward=0.0,
        train_read_count=10,
        trade_count=2,
        fill_count=2,
        blocked_buy_count=0,
        blocked_sell_count=0,
        extra={"a_share_mean_one_way_turnover": 0.1},
    )
    pair_evidence = pair_tradability_evidence(receipt, control_receipt)
    row = {
        **receipt,
        **pair_evidence,
        **prefixed_tradability_evidence(receipt, prefix="primary_"),
        **prefixed_tradability_evidence(
            control_receipt, prefix="control_"
        ),
        "candidate_id": "c1",
        "pair_id": "p1",
        "pair_member_role": "PRIMARY",
        "primary_candidate_id": "c1",
        "control_candidate_id": "c1-control",
        "primary_receipt_hash": "receipt-hash-c1",
        "control_receipt_hash": "receipt-hash-c1-control",
        "pair_receipt_hash": "pair-receipt-hash-p1",
        "pair_evaluation_status": "PAIR_EVALUATED",
        "expression_hash": "h1",
        "expression": "CSRank($close)",
        "generator_arm": "rx_ucb_fresh",
        "optimizer_reward": "0.25",
        "train_reward": "0.25",
        "matched_train_increment": "0.25",
        "a_share_matched_executable_increment": "0.25",
        "pair_train_reward": "0.25",
        "pair_train_reward_decision": "PAIR_TRAIN_FEEDBACK_READY",
        "pair_train_reward_blockers": "",
        "pair_turnover_metric": "0.2",
        "pair_support_metric": "1.0",
        "pair_rank_ic_metric": "0.01",
        "primary_evaluator_invocation_count": "1",
        "control_evaluator_invocation_count": "1",
        "optimizer_reward_source": MATCHED_OPTIMIZER_REWARD_SOURCE,
        "optimizer_reward_metric": MATCHED_OPTIMIZER_REWARD_METRIC,
        "optimizer_reward_split": "train",
        "feedback_data_role": "development",
        "primary_standalone_train_reward_decision": "TRAIN_REWARD_FOLLOWUP_READY",
        "primary_executable_reward_decision": A_SHARE_EXECUTABLE_REWARD_READY,
        "primary_standalone_train_reward_blockers": "",
        "evaluation_evidence_class": A_SHARE_TRADABILITY_EVIDENCE_CLASS,
        "a_share_tradability_decision": A_SHARE_TRADABILITY_READY,
        "pair_a_share_tradability_decision": A_SHARE_TRADABILITY_READY,
        **{field: "true" for field in REQUIRED_TRADABILITY_PROOFS},
        "validation_day_sortino": "9.9",
        "holdout_day_sortino": "8.8",
    }
    row.update(overrides)
    return row


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def test_projection_physically_removes_candidate_level_oos_fields() -> None:
    projected = project_train_only_feedback_row(_reward_row())

    assert "validation_day_sortino" not in projected
    assert "holdout_day_sortino" not in projected
    assert projected["optimizer_reward_split"] == "train"
    assert projected["feedback_data_role"] == "development"
    assert_train_only_feedback_rows([projected])


def test_search_feedback_rejects_raw_candidate_level_oos_payload() -> None:
    with pytest.raises(EvaluationAccessViolation, match="validation_day_sortino"):
        build_search_feedback_context(feedback_rows=[_reward_row()], arm_id="rx_ucb_fresh")


def test_projection_rejects_non_train_optimizer_reward() -> None:
    with pytest.raises(EvaluationAccessViolation, match="split 'holdout'"):
        project_train_only_feedback_row(_reward_row(optimizer_reward_split="holdout"))


def test_projection_allows_development_reward_without_finalist_replay() -> None:
    row = _reward_row(t_plus_one_enforced="false")
    row.pop("replay_receipt_canonical_json")
    projected = project_train_only_feedback_row(row)

    assert projected["optimizer_reward"] == "0.25"
    assert "t_plus_one_enforced_not_proven" in (
        a_share_tradability_blockers(projected)
    )


def test_projection_rejects_missing_split_and_role() -> None:
    with pytest.raises(EvaluationAccessViolation, match="split '<missing>'"):
        project_train_only_feedback_row(_reward_row(optimizer_reward_split=""))
    with pytest.raises(EvaluationAccessViolation, match="explicit development provenance"):
        project_train_only_feedback_row(_reward_row(feedback_data_role="spent"))


def test_clean_feedback_helper_has_no_raw_oos_bypass() -> None:
    row = _reward_row()
    row["evaluation_access_guard"] = "evalreset_feedback_guard_v1"
    with pytest.raises(EvaluationAccessViolation, match="validation_day_sortino"):
        clean_optimizer_feedback_rows([row])


def test_scheduler_rejects_candidate_level_oos_columns() -> None:
    with pytest.raises(EvaluationAccessViolation, match="validation_survival_rate"):
        build_arm_schedule(
            arm_rows=[
                {
                    "generator_arm": "rx_ucb_fresh",
                    "clean_feedback_count": "3",
                    "feedback_update_allowed": "true",
                    "validation_survival_rate": "0.9",
                }
            ],
            family_rows=[],
            blocked_rows=[],
            exploit_rows=[],
            total_budget=100,
        )


def test_feedback_memory_writes_train_only_schema(tmp_path: Path) -> None:
    cm_table = tmp_path / "cm.csv"
    output_root = tmp_path / "runtime"
    report_root = tmp_path / "reports"
    _write_csv(cm_table, [_reward_row()])

    build_feedback_memory(
        cm_tables=[cm_table],
        cm_roots=[],
        output_root=output_root,
        report_root=report_root,
        train_threshold=0.0,
        validation_floor=0.0,
        max_turnover=0.75,
        max_family_share=1.0,
        min_clean_feedback=1,
        authorized_receipt_hashes={"c1": "receipt-hash-c1", "c1-control": "receipt-hash-c1-control"},
        authorized_pair_receipt_hashes={"p1": "pair-receipt-hash-p1"},
    )

    with (output_root / "phase3cn_search_feedback_memory.csv").open(
        encoding="utf-8-sig", newline=""
    ) as fh:
        rows = list(csv.DictReader(fh))
        fields = list(rows[0])

    assert not any(name.startswith(("validation_", "holdout_")) for name in fields)
    assert rows[0]["feedback_data_role"] == "development"
    assert_train_only_feedback_rows(rows)


def test_feedback_memory_rejects_missing_candidate_receipt_authority(tmp_path: Path) -> None:
    cm_table = tmp_path / "cm.csv"
    _write_csv(cm_table, [_reward_row()])
    with pytest.raises(RuntimeError, match="requires candidate submission receipts"):
        build_feedback_memory(
            cm_tables=[cm_table],
            cm_roots=[],
            output_root=tmp_path / "runtime",
            report_root=tmp_path / "reports",
            train_threshold=0.0,
            validation_floor=0.0,
            max_turnover=0.75,
            max_family_share=1.0,
            min_clean_feedback=1,
        )


@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"optimizer_reward_split": ""}, "split=<missing>"),
        ({"optimizer_reward_source": ""}, "source=<missing>"),
        ({"optimizer_reward_metric": ""}, "metric=<missing>"),
        ({"feedback_data_role": "spent"}, "cannot relabel feedback_data_role=spent"),
    ],
)
def test_feedback_memory_rejects_raw_provenance_laundering(
    tmp_path: Path,
    overrides: dict[str, str],
    match: str,
) -> None:
    cm_table = tmp_path / "cm.csv"
    _write_csv(cm_table, [_reward_row(**overrides)])

    with pytest.raises(RuntimeError, match=match):
        build_feedback_memory(
            cm_tables=[cm_table],
            cm_roots=[],
            output_root=tmp_path / "runtime",
            report_root=tmp_path / "reports",
            train_threshold=0.0,
            validation_floor=0.0,
            max_turnover=0.75,
            max_family_share=1.0,
            min_clean_feedback=1,
            authorized_receipt_hashes={"c1": "receipt-hash-c1", "c1-control": "receipt-hash-c1-control"},
            authorized_pair_receipt_hashes={"p1": "pair-receipt-hash-p1"},
        )


@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"control_receipt_hash": "wrong"}, "exact control authorization receipt"),
        ({"pair_receipt_hash": "wrong"}, "exact immutable pair receipt"),
        ({"pair_evaluation_status": "PAIR_EVALUATION_BLOCKED"}, "not a completed matched pair"),
        ({"pair_member_role": "CONTROL"}, "primary pair rows only"),
        ({"shard_chunk_reward_fallback": "true"}, "mean-of-shard reward fallback"),
    ],
)
def test_phase3cn_rejects_invalid_pair_or_shard_fallback(
    tmp_path: Path,
    overrides: dict[str, str],
    match: str,
) -> None:
    cm_table = tmp_path / "cm.csv"
    _write_csv(cm_table, [_reward_row(**overrides)])
    with pytest.raises(RuntimeError, match=match):
        build_feedback_memory(
            cm_tables=[cm_table],
            cm_roots=[],
            output_root=tmp_path / "runtime",
            report_root=tmp_path / "reports",
            train_threshold=0.0,
            validation_floor=0.0,
            max_turnover=0.75,
            max_family_share=1.0,
            min_clean_feedback=1,
            authorized_receipt_hashes={
                "c1": "receipt-hash-c1",
                "c1-control": "receipt-hash-c1-control",
            },
            authorized_pair_receipt_hashes={"p1": "pair-receipt-hash-p1"},
        )
