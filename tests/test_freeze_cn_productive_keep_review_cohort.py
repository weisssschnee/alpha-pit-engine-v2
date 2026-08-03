from __future__ import annotations

import math
import json
import hashlib
from pathlib import Path

import pandas as pd

from scripts.freeze_cn_productive_keep_review_cohort import (
    HIGHER_IS_BETTER,
    LOWER_IS_BETTER,
    _deduplicate_behavior_candidates,
    _deduplicate_mechanism_candidates,
    _finalist_funnel_identities,
    _load_excluded_cohort_pairs,
    _payload_sha256,
    _productive_authority,
    _productive_pair_outcomes,
    _rank_candidates,
    _screen_reason,
    _select_finalist_funnel,
    _select_with_caps,
    _source_paths,
)
from scripts.verify_cn_productive_keep_review_cohort import (
    _recompute_finalist_identities,
    _recompute_finalist_selection,
)


def _ranking_frame() -> pd.DataFrame:
    rows = []
    groups = [
        ("ROUTE_A", "STRUCT_A", "SIGNAL_A", 60),
        ("ROUTE_B", "STRUCT_B", "SIGNAL_B", 20),
        ("ROUTE_C", "STRUCT_C", "SIGNAL_C", 20),
    ]
    ordinal = 0
    for route_id, structural_id, signal_id, count in groups:
        for _ in range(count):
            ordinal += 1
            row = {
                "pair_id": f"pair-{ordinal:03d}",
                "route_id": route_id,
                "structural_family_id": structural_id,
                "signal_cluster_id": signal_id,
            }
            for offset, column in enumerate(HIGHER_IS_BETTER):
                row[column] = 1000.0 - ordinal - offset * 0.01
            for offset, column in enumerate(LOWER_IS_BETTER):
                row[column] = ordinal + offset * 0.01
            rows.append(row)
    return pd.DataFrame(rows)


def test_cap_constrained_selection_is_deterministic_and_diverse() -> None:
    ranked = _rank_candidates(_ranking_frame())
    first = _select_with_caps(ranked)
    second = _select_with_caps(ranked)
    assert first == second
    assert len(first) == 64
    selected = ranked[ranked["pair_id"].isin(first)]
    assert selected["route_id"].value_counts().max() <= 48
    assert selected["structural_family_id"].value_counts().max() <= 32
    assert selected["signal_cluster_id"].value_counts().max() <= 48
    assert set(selected["structural_family_id"]) == {
        "STRUCT_A",
        "STRUCT_B",
        "STRUCT_C",
    }
    assert set(selected["signal_cluster_id"]) == {
        "SIGNAL_A",
        "SIGNAL_B",
        "SIGNAL_C",
    }


def test_small_cohort_does_not_overshoot_diversity_anchors() -> None:
    ranked = _rank_candidates(_ranking_frame())
    selected_ids = _select_with_caps(ranked, cohort_pairs=2)
    assert len(selected_ids) == 2
    assert selected_ids == _select_with_caps(ranked, cohort_pairs=2)


def test_behavior_dedup_keeps_highest_ranked_representative() -> None:
    frame = _ranking_frame().head(4).copy()
    frame["portfolio_behavior_family_id"] = [
        "FAMILY_DUPLICATE",
        "FAMILY_DUPLICATE",
        "FAMILY_3",
        "FAMILY_4",
    ]
    frame["portfolio_behavior_signature_id"] = [
        "SIGNATURE_1",
        "SIGNATURE_2",
        "SIGNATURE_3",
        "SIGNATURE_4",
    ]
    ranked = _rank_candidates(frame)
    deduped, rejected = _deduplicate_behavior_candidates(ranked)
    assert deduped["pair_id"].tolist() == [
        "pair-001",
        "pair-003",
        "pair-004",
    ]
    assert rejected == {
        "pair-002": "BEHAVIOR_FAMILY_DUPLICATE_LOWER_RANK"
    }


def test_train_stability_screen_is_fail_closed() -> None:
    clean = {
        "search_score": 0.1,
        "primary_composite_reward": 0.2,
        "matched_train_increment": 0.1,
        "matched_gross_increment": 1.0,
        "matched_net_increment": 0.9,
        "matched_trading_cost_difference": 0.1,
        "train_day_sortino": 0.4,
        "train_worst_horizon_day_sortino": 0.04,
        "train_median_horizon_day_sortino": 0.2,
        "train_horizon_sortino_stdev": 0.1,
        "train_day_mcmc_p25": 0.2,
        "train_day_mcmc_prob_gt_0": 1.0,
        "train_regime_positive_share": 0.5,
        "train_regime_stability_score": -0.5,
        "train_rank_ic_mean": 0.01,
        "train_rank_ic_hit_rate": 0.6,
        "train_mean_one_way_turnover": 0.02,
    }
    assert _screen_reason(clean) == ""

    broken = dict(clean)
    broken["matched_net_increment"] = -0.01
    assert _screen_reason(broken) == "NONPOSITIVE_MATCHED_NET_INCREMENT"

    broken = dict(clean)
    broken["train_day_mcmc_p25"] = float("nan")
    assert _screen_reason(broken).startswith(
        "MISSING_FINITE_TRAIN_METRICS:"
    )


def test_payload_hash_is_canonical() -> None:
    left = {"b": 2, "a": {"x": 1}}
    right = {"a": {"x": 1}, "b": 2}
    assert _payload_sha256(left) == _payload_sha256(right)


def test_excluded_cohort_is_hash_verified_and_loaded(
    tmp_path: Path,
) -> None:
    root = tmp_path / "prior"
    root.mkdir()
    pairs = root / "keep_review_pairs.parquet"
    pd.DataFrame({"pair_id": ["pair-old-1", "pair-old-2"]}).to_parquet(
        pairs, index=False
    )
    artifact = {
        "path": pairs.name,
        "bytes": pairs.stat().st_size,
        "sha256": hashlib.sha256(pairs.read_bytes()).hexdigest(),
    }
    manifest = {
        "status": "KEEP_REVIEW_COHORT_CLOSED_IMMUTABLE",
        "selection_payload_sha256": "selection",
        "artifacts": [artifact],
    }
    manifest["manifest_payload_sha256"] = _payload_sha256(manifest)
    (root / "keep_review_manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    pair_ids, bindings = _load_excluded_cohort_pairs((root,))
    assert pair_ids == {"pair-old-1", "pair-old-2"}
    assert bindings[0]["excluded_pair_count"] == 2


def test_finalist_funnel_allows_single_compatible_route_and_signal() -> None:
    rows = []
    for ordinal in range(320):
        rows.append(
            {
                "pair_id": f"pair-final-{ordinal:03d}",
                "route_id": "SLOW_TEMPORAL_CHANGE",
                "structural_family_id": f"STRUCT_{ordinal % 2}",
                "signal_cluster_id": "ONLY_SIGNAL",
                "portfolio_exposure_family_id": f"EXPOSURE_{ordinal % 8}",
                "economic_mechanism_id": f"MECHANISM_{ordinal:03d}",
            }
        )
    ranked = pd.DataFrame(rows)
    selected_ids, caps = _select_finalist_funnel(
        ranked,
        cohort_pairs=256,
    )
    assert len(selected_ids) == 256
    selected = ranked[ranked["pair_id"].isin(selected_ids)]
    assert caps["route_cap"] is None
    assert caps["signal_cap"] is None
    assert selected["structural_family_id"].value_counts().max() <= 154
    assert (
        selected["portfolio_exposure_family_id"].value_counts().max()
        <= 64
    )


def test_finalist_funnel_does_not_fake_structural_diversity() -> None:
    rows = []
    for ordinal in range(96):
        rows.append(
            {
                "pair_id": f"pair-single-structure-{ordinal:03d}",
                "route_id": "SLOW_TEMPORAL_CHANGE",
                "structural_family_id": "ONLY_STRUCTURE",
                "signal_cluster_id": "ONLY_SIGNAL",
                "portfolio_exposure_family_id": f"EXPOSURE_{ordinal % 4}",
                "economic_mechanism_id": f"MECHANISM_{ordinal:03d}",
            }
        )
    selected, caps = _select_finalist_funnel(
        pd.DataFrame(rows), cohort_pairs=64
    )
    assert len(selected) == 64
    assert caps["structural_group_count"] == 1
    assert caps["structural_cap"] is None


def test_finalist_mechanism_identity_and_dedup_are_deterministic() -> None:
    rows = []
    for ordinal in range(2):
        row = {
            "pair_id": f"pair-{ordinal}",
            "route_id": "SLOW_TEMPORAL_CHANGE",
            "financial_hypothesis": "same",
            "operator_family": "difference",
            "skeleton_id": "skeleton",
            "source_field_ids": ["b", "a"],
            "operator_paths": ["x", "y"],
            "event_state_family": "none",
            "primitive_family": "time_series",
            "horizon_bucket": "multi_or_unknown",
            "primary_support_rate": 0.91,
            "primary_mean_turnover": 0.08,
        }
        rows.append(row)
    identified = _finalist_funnel_identities(pd.DataFrame(rows))
    assert identified["economic_mechanism_id"].nunique() == 1
    assert identified["portfolio_exposure_family_id"].nunique() == 1
    deduped, rejected = _deduplicate_mechanism_candidates(identified)
    assert deduped["pair_id"].tolist() == ["pair-0"]
    assert rejected == {
        "pair-1": "ECONOMIC_MECHANISM_DUPLICATE_LOWER_RANK"
    }
    independently_recomputed = _recompute_finalist_identities(identified)
    assert independently_recomputed[
        "verify_economic_mechanism_id"
    ].tolist() == identified["economic_mechanism_id"].tolist()
    assert independently_recomputed[
        "verify_portfolio_exposure_family_id"
    ].tolist() == identified["portfolio_exposure_family_id"].tolist()


def test_finalist_verifier_recomputes_same_selection() -> None:
    rows = []
    for ordinal in range(320):
        rows.append(
            {
                "pair_id": f"pair-verify-{ordinal:03d}",
                "route_id": "SLOW_TEMPORAL_CHANGE",
                "structural_family_id": f"STRUCT_{ordinal % 2}",
                "signal_cluster_id": "ONLY_SIGNAL",
                "portfolio_exposure_family_id": f"EXPOSURE_{ordinal % 8}",
                "economic_mechanism_id": f"MECHANISM_{ordinal:03d}",
            }
        )
    ranked = pd.DataFrame(rows)
    selected, caps = _select_finalist_funnel(ranked, cohort_pairs=256)
    verified, verified_caps = _recompute_finalist_selection(
        ranked,
        cohort_pairs=256,
    )
    assert verified == selected
    assert verified_caps == caps


def test_productive_pair_outcomes_are_not_limited_to_optimizer_observations(
    tmp_path: Path,
) -> None:
    checkpoints = tmp_path / "checkpoints"
    for ordinal, score in enumerate((0.2, -0.1), start=1):
        checkpoint = checkpoints / f"checkpoint_{ordinal:03d}"
        checkpoint.mkdir(parents=True)
        pd.DataFrame(
            [
                {
                    "pair_id": f"pair-{ordinal}",
                    "pair_evaluation_status": "PAIR_EVALUATED",
                    "primary_composite_reward": score,
                    "matched_train_increment": score,
                    "primary_standalone_train_reward_decision": (
                        "TRAIN_REWARD_FOLLOWUP_READY"
                    ),
                }
            ]
        ).to_parquet(checkpoint / "pair_outcomes.parquet", index=False)
    productive = _productive_pair_outcomes(tmp_path)
    assert productive["pair_id"].tolist() == ["pair-1"]
    assert productive["search_score"].tolist() == [0.2]


def test_productive_authority_prefers_closed_pair_outcomes(
    tmp_path: Path,
) -> None:
    checkpoint = tmp_path / "checkpoints" / "checkpoint_001"
    checkpoint.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "pair_id": "pair-complete",
                "pair_evaluation_status": "PAIR_EVALUATED",
                "primary_composite_reward": 0.3,
                "matched_train_increment": 0.2,
                "primary_standalone_train_reward_decision": (
                    "TRAIN_REWARD_FOLLOWUP_READY"
                ),
            }
        ]
    ).to_parquet(checkpoint / "pair_outcomes.parquet", index=False)
    observations = pd.DataFrame(
        [
            {
                "pair_id": "pair-complete",
                "pair_evaluation_status": "PAIR_EVALUATED",
                "search_score": float("nan"),
                "matched_train_increment": 0.2,
                "primary_standalone_train_reward_decision": (
                    "TRAIN_REWARD_FOLLOWUP_READY"
                ),
            }
        ]
    )

    productive, authority, bind_pair_outcomes = _productive_authority(
        root=tmp_path,
        observations=observations,
    )

    assert productive["pair_id"].tolist() == ["pair-complete"]
    assert authority == "IMMUTABLE_CHECKPOINT_PAIR_OUTCOMES"
    assert bind_pair_outcomes is True


def test_source_paths_bind_checkpoint_count_from_closed_train_manifest(
    tmp_path: Path,
) -> None:
    (tmp_path / "train_complete_manifest.json").write_text(
        json.dumps({"checkpoint_count": 4}), encoding="utf-8"
    )
    for name in (
        "run_manifest.json",
        "final_decision.json",
        "frozen_contract.json",
        "candidate_ledger.parquet",
        "observation_ledger.parquet",
    ):
        (tmp_path / name).write_bytes(b"source")
    for index in range(1, 5):
        checkpoint = tmp_path / "checkpoints" / f"checkpoint_{index:03d}"
        phase3cm = checkpoint / "phase3cm"
        for backend in ("active_bar", "stock_session"):
            result = phase3cm / backend / "CN_STREAMING_BACKEND_RESULT.json"
            result.parent.mkdir(parents=True, exist_ok=True)
            result.write_text("{}", encoding="utf-8")
        for name in ("batch_manifest.json", "full_behavior.parquet"):
            (checkpoint / name).write_bytes(b"checkpoint")

    paths = _source_paths(tmp_path)

    assert sum(path.name == "batch_manifest.json" for path in paths) == 4
    assert sum(path.name == "CN_STREAMING_BACKEND_RESULT.json" for path in paths) == 8


def test_source_paths_reject_checkpoint_set_drift(tmp_path: Path) -> None:
    (tmp_path / "train_complete_manifest.json").write_text(
        json.dumps({"checkpoint_count": 4}), encoding="utf-8"
    )
    for index in range(1, 4):
        (tmp_path / "checkpoints" / f"checkpoint_{index:03d}").mkdir(
            parents=True
        )

    try:
        _source_paths(tmp_path)
    except RuntimeError as exc:
        assert "does not match train manifest" in str(exc)
    else:
        raise AssertionError("checkpoint set drift was accepted")
