from __future__ import annotations

import copy
import csv
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from our_system_phase2.runtime import phase3cm_train_portfolio_sortino_reward_audit as phase3cm
from our_system_phase2.services.candidate_submission_receipt import (
    CandidateReceiptError,
    CandidateSubmissionAuthority,
    LegacyCandidateSubmissionAdapter,
    ReceiptContext,
    write_receipt_table,
)
from our_system_phase2.services.fixed_split_authority import FixedSplitAuthority
from our_system_phase2.services.matched_control_pairs import (
    CONTROL_CONSTRUCTOR_MATRIX,
    CandidatePairAuthority,
    CandidatePairError,
    build_pair_evaluation_rows,
    group_candidate_pairs,
    partition_candidate_pairs,
    write_pair_receipt_table,
)
from our_system_phase2.services.unified_capability_registry import (
    ROUTE_IDS,
    UnifiedCapabilityRegistry,
    stable_hash,
)
from our_system_phase2.services.unified_discovery_generators import RegistryDrivenGenerator


REPO = Path(__file__).resolve().parents[1]
SPLIT = REPO / "runtime/run_plans/phase3ga_true1min_2024_2025_global_split_manifest.csv"
REGISTRY = (
    REPO
    / "reports/cn_unified_capability_discovery_20260714/completed_f8169e1/registry"
    / "unified_capability_registry.json"
)
EVALUATOR = REPO / "src/our_system_phase2/runtime/phase3cm_train_portfolio_sortino_reward_audit.py"
DATA_RELEASE_HASH = "cfb2742d975f2f6f1dcdf78d011f6d471b8d0e444164bae1d1816ba1fdcc5827"


def _candidate_authority(registry: UnifiedCapabilityRegistry) -> CandidateSubmissionAuthority:
    return CandidateSubmissionAuthority(
        registry,
        ReceiptContext.build(
            registry=registry,
            split_authority=FixedSplitAuthority.read(SPLIT),
            data_release_hash=DATA_RELEASE_HASH,
            evaluator_paths=[EVALUATOR],
        ),
    )


def _four_route_pairs() -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    generator = RegistryDrivenGenerator(registry)
    candidates: list[dict[str, object]] = []
    for seed, route_id in enumerate(
        ("MINUTE_STATIC", "FIRSTN_PATH", "SLOW_CROSS_SECTIONAL_LEVEL", "SLOW_TEMPORAL_CHANGE"),
        101,
    ):
        candidates.extend(generator.generate_route(route_id, proposal_budget=2, seed=seed))
    receipts = _candidate_authority(registry).authorize_table(candidates)
    pair_receipts = CandidatePairAuthority().authorize_table(candidates, receipts)
    return candidates, receipts, pair_receipts


def test_all_routes_have_authoritative_constructor_and_immutable_pair_receipt() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    generator = RegistryDrivenGenerator(registry)
    authority = _candidate_authority(registry)
    assert set(CONTROL_CONSTRUCTOR_MATRIX) == set(ROUTE_IDS)
    for index, route_id in enumerate(ROUTE_IDS, 1):
        candidates = generator.generate_route(route_id, proposal_budget=2, seed=200 + index)
        assert {row["pair_member_role"] for row in candidates} == {"PRIMARY", "CONTROL"}
        assert len({row["pair_id"] for row in candidates}) == 1
        receipts = authority.authorize_table(candidates)
        pair_receipts = CandidatePairAuthority().authorize_table(candidates, receipts)
        assert len(pair_receipts) == 1
        assert pair_receipts[0]["control_constructor_id"] == CONTROL_CONSTRUCTOR_MATRIX[route_id][
            "control_constructor_id"
        ]
        assert CandidatePairAuthority().validate_table(candidates, receipts, pair_receipts) == pair_receipts


def test_legacy_adapter_fails_closed_without_verified_route_control() -> None:
    adapter = LegacyCandidateSubmissionAdapter(UnifiedCapabilityRegistry.read(REGISTRY))
    with pytest.raises(CandidateReceiptError, match="FAIL_CLOSED_NO_MATCHED_CONTROL_CONSTRUCTOR"):
        adapter.adapt_pair({"candidate_id": "single_static", "expression": "CSRank($close)"})
    with pytest.raises(CandidateReceiptError, match="cannot be conservatively mapped"):
        adapter.adapt_pair({"candidate_id": "metadata", "expression": "CSRank($code)"})


def test_pair_authority_rejects_pointer_route_context_and_exact_drift() -> None:
    candidates, receipts, _ = _four_route_pairs()
    first_pair = candidates[:2]
    first_receipts = receipts[:2]

    wrong_pointer = copy.deepcopy(first_pair)
    wrong_pointer[1]["matched_control_id"] = "some-other-primary"
    with pytest.raises(CandidatePairError, match="point"):
        CandidatePairAuthority().authorize_table(wrong_pointer, first_receipts)

    route_drift = copy.deepcopy(first_receipts)
    route_drift[1]["route_id"] = "FIRSTN_PATH"
    with pytest.raises(CandidatePairError, match="context mismatch"):
        CandidatePairAuthority().authorize_table(first_pair, route_drift)

    context_drift = copy.deepcopy(first_receipts)
    context_drift[1]["data_release_hash"] = "0" * 64
    with pytest.raises(CandidatePairError, match="context mismatch"):
        CandidatePairAuthority().authorize_table(first_pair, context_drift)

    support_drift = copy.deepcopy(first_receipts)
    support_drift[1]["support_unit"] = "different-support-unit"
    with pytest.raises(CandidatePairError, match="context mismatch"):
        CandidatePairAuthority().authorize_table(first_pair, support_drift)

    split_drift = copy.deepcopy(first_receipts)
    split_drift[1]["split_manifest_hash"] = "f" * 64
    with pytest.raises(CandidatePairError, match="context mismatch"):
        CandidatePairAuthority().authorize_table(first_pair, split_drift)

    exact_drift = copy.deepcopy(first_receipts)
    exact_drift[1]["exact_identity"] = exact_drift[0]["exact_identity"]
    with pytest.raises(CandidatePairError, match="exact identity"):
        CandidatePairAuthority().authorize_table(first_pair, exact_drift)

    observable_drift = copy.deepcopy(first_receipts)
    observable_drift[1]["observable_time_contract"][0] += "|FUTURE_CLOCK"
    with pytest.raises(CandidatePairError, match="observable time contract mismatch"):
        CandidatePairAuthority().authorize_table(first_pair, observable_drift)

    pit_lag_drift = copy.deepcopy(first_receipts)
    pit_lag_drift[1]["pit_source_lag_contract"][0] += "|FUTURE_LAG"
    with pytest.raises(CandidatePairError, match="PIT/source-lag contract mismatch"):
        CandidatePairAuthority().authorize_table(first_pair, pit_lag_drift)

    with pytest.raises(CandidatePairError, match="candidate receipt missing"):
        CandidatePairAuthority().authorize_table(first_pair, first_receipts[:1])


def test_pair_receipt_hash_and_clock_contract_are_immutable() -> None:
    candidates, receipts, pair_receipts = _four_route_pairs()
    first_pair = candidates[:2]
    receipt_by_id = {str(row["candidate_id"]): row for row in receipts}
    first_receipts = [receipt_by_id[str(row["candidate_id"])] for row in first_pair]
    supplied = [next(row for row in pair_receipts if row["pair_id"] == first_pair[0]["pair_id"])]
    assert supplied[0]["pair_clock_alignment_policy"] == "PRIMARY_CLOCK_AND_EXACT_ELIGIBLE_SUPPORT"
    assert supplied[0]["primary_observable_time_contract"]
    assert supplied[0]["primary_pit_source_lag_contract"]
    tampered = copy.deepcopy(supplied)
    tampered[0]["pair_clock_alignment_policy"] = "WEAK_CLOCK"
    with pytest.raises(CandidatePairError, match="hash does not match"):
        CandidatePairAuthority().validate_table(first_pair, first_receipts, tampered)


def test_candidate_parallel_partition_never_splits_a_pair_and_is_deterministic() -> None:
    candidates, _, _ = _four_route_pairs()
    for worker_count in (1, 2, 4):
        first = partition_candidate_pairs(candidates, worker_count=worker_count)
        second = partition_candidate_pairs(list(reversed(candidates)), worker_count=worker_count)
        assert first == second
        assert sum(len(chunk) for chunk in first) == len(candidates)
        seen: set[str] = set()
        for chunk in first:
            for primary, control in group_candidate_pairs(chunk):
                assert primary["pair_id"] == control["pair_id"]
                assert primary["pair_id"] not in seen
                seen.add(str(primary["pair_id"]))
        assert len(seen) == len(candidates) // 2


def test_pair_evaluation_computes_matched_increment_and_blocks_degenerate_control() -> None:
    candidates, receipts, pair_receipts = _four_route_pairs()
    primary, control = group_candidate_pairs(candidates)[0]
    candidates = [primary, control]
    receipt_by_id = {str(row["candidate_id"]): row for row in receipts}
    receipts = [receipt_by_id[str(primary["candidate_id"])], receipt_by_id[str(control["candidate_id"])]]
    pair_receipt = next(row for row in pair_receipts if row["pair_id"] == primary["pair_id"])
    for row in candidates:
        row["expression_hash"] = stable_hash(row["expression"])
    reward_rows = [
        {
            "candidate_id": primary["candidate_id"],
            "optimizer_reward": 0.3,
            "train_mean_one_way_turnover": 0.4,
            "train_rank_ic_mean": 0.02,
        },
        {
            "candidate_id": control["candidate_id"],
            "optimizer_reward": 0.1,
            "train_mean_one_way_turnover": 0.3,
            "train_rank_ic_mean": 0.01,
        },
    ]
    base = {
        "shard_index": 1,
        "trade_time": "2024-01-02 09:35:00",
        "horizon_min": 5,
        "split": "train",
        "long_count": 2,
        "short_count": 2,
        "one_way_turnover": 0.2,
        "eligible_code_count": 4,
        "eligible_code_identity": stable_hash(["S1", "S2", "S3", "S4"]),
        "portfolio_weight_identity": "weights-primary",
    }
    primary_rows = [{**base, "top_signal_mean": 1.0, "bottom_signal_mean": -1.0}]
    control_rows = [
        {
            **base,
            "top_signal_mean": 0.5,
            "bottom_signal_mean": -0.5,
            "portfolio_weight_identity": "weights-control",
        }
    ]
    result = build_pair_evaluation_rows(
        candidates=candidates,
        candidate_receipts=receipts,
        pair_receipts=[pair_receipt],
        reward_rows=reward_rows,
        portfolio_rows_by_expression_hash={
            str(primary["expression_hash"]): primary_rows,
            str(control["expression_hash"]): control_rows,
        },
        evaluator_invocation_counts={str(primary["candidate_id"]): 1, str(control["candidate_id"]): 1},
    )
    assert result[0]["pair_evaluation_status"] == "PAIR_EVALUATED"
    assert result[0]["matched_train_increment"] == pytest.approx(0.2)
    assert result[0]["matched_turnover_increment"] == pytest.approx(0.1)
    assert result[0]["matched_rank_ic_increment"] == pytest.approx(0.01)

    blocked = build_pair_evaluation_rows(
        candidates=candidates,
        candidate_receipts=receipts,
        pair_receipts=[pair_receipt],
        reward_rows=reward_rows,
        portfolio_rows_by_expression_hash={
            str(primary["expression_hash"]): primary_rows,
            str(control["expression_hash"]): [{**base, "top_signal_mean": 0.0, "bottom_signal_mean": 0.0}],
        },
        evaluator_invocation_counts={str(primary["candidate_id"]): 1, str(control["candidate_id"]): 1},
    )
    assert blocked[0]["pair_evaluation_status"] == "PAIR_EVALUATION_BLOCKED"
    assert "control_signal_empty_or_constant" in blocked[0]["pair_evaluation_blockers"]
    assert blocked[0]["optimizer_reward"] == ""

    support_mismatch = build_pair_evaluation_rows(
        candidates=candidates,
        candidate_receipts=receipts,
        pair_receipts=[pair_receipt],
        reward_rows=reward_rows,
        portfolio_rows_by_expression_hash={
            str(primary["expression_hash"]): primary_rows,
            str(control["expression_hash"]): [
                {
                    **control_rows[0],
                    "eligible_code_identity": stable_hash(["S1", "S2", "S3", "S9"]),
                }
            ],
        },
        evaluator_invocation_counts={str(primary["candidate_id"]): 1, str(control["candidate_id"]): 1},
    )
    assert support_mismatch[0]["pair_evaluation_status"] == "PAIR_EVALUATION_BLOCKED"
    assert "pair_support_coordinates_mismatch" in support_mismatch[0]["pair_evaluation_blockers"]
    assert support_mismatch[0]["pair_support_overlap"] == 0.0


def test_pair_evaluation_fails_closed_for_missing_receipts_and_control_invocation() -> None:
    candidates, receipts, pair_receipts = _four_route_pairs()
    primary, control = group_candidate_pairs(candidates)[0]
    for row in (primary, control):
        row["expression_hash"] = stable_hash(row["expression"])
    receipt_by_id = {str(row["candidate_id"]): row for row in receipts}
    pair_receipt = next(row for row in pair_receipts if row["pair_id"] == primary["pair_id"])
    pair_receipts_subset = [pair_receipt]
    reward_rows = [
        {"candidate_id": primary["candidate_id"], "optimizer_reward": 0.2},
        {"candidate_id": control["candidate_id"], "optimizer_reward": 0.1},
    ]
    support = {
        "shard_index": 0,
        "trade_time": "2024-01-02T09:35:00",
        "horizon_min": 1,
        "split": "train",
        "eligible_code_count": 2,
        "eligible_code_identity": stable_hash(["S1", "S2"]),
        "long_count": 1,
        "short_count": 1,
        "one_way_turnover": 0.1,
    }
    rows_by_hash = {
        str(primary["expression_hash"]): [
            {**support, "top_signal_mean": 1.0, "bottom_signal_mean": -1.0, "portfolio_weight_identity": "p"}
        ],
        str(control["expression_hash"]): [
            {**support, "top_signal_mean": 0.5, "bottom_signal_mean": -0.5, "portfolio_weight_identity": "c"}
        ],
    }

    cases = (
        ([], pair_receipts_subset, "primary_receipt_missing"),
        ([receipt_by_id[str(primary["candidate_id"])]], pair_receipts_subset, "control_receipt_missing"),
        (
            [receipt_by_id[str(primary["candidate_id"])], receipt_by_id[str(control["candidate_id"])]],
            [],
            "pair_receipt_missing",
        ),
    )
    for candidate_receipts, supplied_pair_receipts, blocker in cases:
        result = build_pair_evaluation_rows(
            candidates=[primary, control],
            candidate_receipts=candidate_receipts,
            pair_receipts=supplied_pair_receipts,
            reward_rows=reward_rows,
            portfolio_rows_by_expression_hash=rows_by_hash,
            evaluator_invocation_counts={str(primary["candidate_id"]): 1, str(control["candidate_id"]): 1},
        )
        assert result[0]["pair_evaluation_status"] == "PAIR_EVALUATION_BLOCKED"
        assert blocker in result[0]["pair_evaluation_blockers"]

    not_invoked = build_pair_evaluation_rows(
        candidates=[primary, control],
        candidate_receipts=[receipt_by_id[str(primary["candidate_id"])], receipt_by_id[str(control["candidate_id"])]],
        pair_receipts=pair_receipts_subset,
        reward_rows=reward_rows,
        portfolio_rows_by_expression_hash=rows_by_hash,
        evaluator_invocation_counts={str(primary["candidate_id"]): 1, str(control["candidate_id"]): 0},
    )
    assert "control_evaluator_not_invoked" in not_invoked[0]["pair_evaluation_blockers"]


def test_phase3cm_formally_evaluates_both_pair_members_and_writes_matched_increment(
    tmp_path: Path,
) -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    candidates = RegistryDrivenGenerator(registry).generate_route(
        "MINUTE_STATIC", proposal_budget=2, seed=409
    )
    for candidate in candidates:
        candidate["expression_hash"] = stable_hash(candidate["expression"])[:24]
        candidate["generator_arm"] = "matched_pair_integration"

    candidate_table = tmp_path / "candidate-pairs.csv"
    with candidate_table.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(candidates[0]))
        writer.writeheader()
        writer.writerows(candidates)

    candidate_receipts = _candidate_authority(registry).authorize_table(candidates)
    pair_receipts = CandidatePairAuthority().authorize_table(candidates, candidate_receipts)
    receipt_table = tmp_path / "candidate-receipts.jsonl"
    pair_receipt_table = tmp_path / "pair-receipts.jsonl"
    write_receipt_table(receipt_table, candidate_receipts)
    write_pair_receipt_table(pair_receipt_table, pair_receipts)

    shard_root = tmp_path / "shards"
    panel_dir = shard_root / "shard_00" / "phase3aq_wide_true1min" / "canary"
    panel_dir.mkdir(parents=True)
    rows: list[dict[str, object]] = []
    field_ids = sorted(
        {str(field) for candidate in candidates for field in candidate["declared_field_ids"]}
    )
    for day_offset, date in enumerate(("2024-01-02", "2024-01-03", "2024-01-04")):
        for minute in range(8):
            trade_time = pd.Timestamp(date) + pd.Timedelta(hours=9, minutes=30 + minute)
            for symbol_index in range(30):
                row: dict[str, object] = {
                    "code": f"S{symbol_index:03d}",
                    "trade_time": trade_time,
                    "date": pd.Timestamp(date),
                    "close": 20.0
                    + symbol_index * 0.02
                    + minute * (0.005 if symbol_index % 2 else -0.004)
                    + day_offset * 0.01,
                }
                for field_index, field_id in enumerate(field_ids, 1):
                    row[field_id] = float(
                        np.sin((symbol_index + 1) * (field_index + 1) * 0.31)
                        + np.cos((minute + 1) * (field_index + 2) * 0.23)
                    )
                rows.append(row)
    pd.DataFrame(rows).to_parquet(
        panel_dir / "phase3aq_true_1min_formula_canary.parquet", index=False
    )

    output_root = tmp_path / "output"
    report_root = tmp_path / "report"
    result = phase3cm.main(
        [
            "--candidate-audit",
            str(candidate_table),
            "--shard-root",
            str(shard_root),
            "--output-root",
            str(output_root),
            "--report-root",
            str(report_root),
            "--candidate-limit",
            "1",
            "--max-shards",
            "1",
            "--sample-trade-times-per-shard",
            "24",
            "--horizons",
            "1",
            "--split-manifest",
            str(SPLIT),
            "--candidate-receipt-table",
            str(receipt_table),
            "--candidate-pair-receipt-table",
            str(pair_receipt_table),
            "--unified-registry",
            str(REGISTRY),
            "--data-release-hash",
            DATA_RELEASE_HASH,
            "--min-obs-per-time",
            "10",
            "--write-pnl-rows",
            "--write-reward-atoms",
            "--disable-incremental-checkpoints",
            "--disable-persistent-expression-cache",
            "--disable-persistent-operator-cache",
            "--disable-persistent-feature-matrix-cache",
            "--persistent-cache-mode",
            "off",
        ]
    )
    with (output_root / "phase3cm_candidate_pair_evaluation.csv").open(
        encoding="utf-8-sig", newline=""
    ) as handle:
        pair_rows = list(csv.DictReader(handle))
    with (output_root / "phase3cm_train_reward.csv").open(
        encoding="utf-8-sig", newline=""
    ) as handle:
        evaluator_rows = list(csv.DictReader(handle))
    assert result == 0
    assert len(evaluator_rows) == 2
    assert {row["candidate_id"] for row in evaluator_rows} == {
        str(candidate["candidate_id"]) for candidate in candidates
    }
    assert len(pair_rows) == 1
    assert pair_rows[0]["pair_evaluation_status"] == "PAIR_EVALUATED"
    assert pair_rows[0]["matched_train_increment"] != ""
    assert int(pair_rows[0]["primary_evaluator_invocation_count"]) == 1
    assert int(pair_rows[0]["control_evaluator_invocation_count"]) == 1
