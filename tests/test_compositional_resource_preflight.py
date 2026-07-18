from __future__ import annotations

import hashlib

import pytest

from scripts.freeze_cn_compositional_resource_preflight import (
    _candidate_rows,
    assert_strict_materialization_ready,
    assign_evaluator_expression_identities,
    evaluator_expression_identity,
    representative_route_quotas,
    select_representative_pairs,
)


def test_strict_materialization_gate_fails_closed_without_required_receipt() -> None:
    with pytest.raises(RuntimeError, match="cannot enter strict evaluation"):
        assert_strict_materialization_ready(
            {
                "candidate_id": "state-close-range-location-sign",
                "declared_field_ids": ["state_close_range_location_sign"],
                "materialization_status": "NOT_MATERIALIZED",
                "strict_evaluation_allowed": False,
                "required_materialization_receipt": (
                    "state_close_range_location_sign_materialization_receipt"
                ),
            }
        )


def test_strict_materialization_gate_requires_verified_catalog_not_hash_shape() -> None:
    candidate = {
        "candidate_id": "state-close-range-location-sign",
        "declared_field_ids": ["state_close_range_location_sign"],
        "runtime_ready": True,
        "materialization_status": "MATERIALIZED_DEVELOPMENT_ONLY",
        "strict_evaluation_allowed": True,
        "materialization_support_receipt_hashes": {
            "state_close_range_location_sign": "b" * 64,
        },
    }
    with pytest.raises(RuntimeError, match="cannot enter strict evaluation"):
        assert_strict_materialization_ready(candidate)
    with pytest.raises(RuntimeError, match="cannot enter strict evaluation"):
        assert_strict_materialization_ready(
            candidate,
            verified_receipt_hashes={"state_close_range_location_sign": "c" * 64},
        )
    assert_strict_materialization_ready(
        candidate,
        verified_receipt_hashes={"state_close_range_location_sign": "b" * 64},
    )


def test_evaluator_identity_keeps_duplicate_controls_in_distinct_pairs() -> None:
    expression = "CSRank(Sign($x))"

    assert evaluator_expression_identity("control-a", expression) != evaluator_expression_identity(
        "control-b", expression
    )


def test_evaluator_identity_changes_only_duplicate_expressions() -> None:
    rows = [
        {"candidate_id": "primary-a", "expression": "CSRank($x)"},
        {"candidate_id": "control-a", "expression": "CSRank(Sign($y))"},
        {"candidate_id": "control-b", "expression": "CSRank(Sign($y))"},
    ]

    assign_evaluator_expression_identities(rows)

    assert rows[0]["expression_hash"] == hashlib.sha256(b"CSRank($x)").hexdigest()[:24]
    assert rows[1]["expression_hash"] != rows[2]["expression_hash"]


def test_resource_preflight_quotas_cover_every_route_and_preserve_stage_weight_ties() -> None:
    quotas = representative_route_quotas(
        {"A": 592, "B": 576, "C": 592, "D": 576, "E": 592, "F": 576, "G": 592},
        32,
    )

    assert sum(quotas.values()) == 32
    assert {route for route, count in quotas.items() if count == 5} == {"A", "C", "E", "G"}
    assert set(quotas.values()) == {4, 5}


def test_resource_preflight_selection_is_deterministic_and_covers_dimensions() -> None:
    rows = []
    for index in range(12):
        rows.append(
            {
                "candidate_id": f"c{index:02d}",
                "route_id": "A",
                "policy_id": f"p{index % 4}",
                "seed": str(index % 4),
                "skeleton_id": f"s{index % 3}",
                "behavior_cluster_id": str(index),
            }
        )

    first = select_representative_pairs(rows, route_quotas={"A": 4})
    second = select_representative_pairs(reversed(rows), route_quotas={"A": 4})

    assert [row["candidate_id"] for row in first] == [row["candidate_id"] for row in second]
    assert len({row["policy_id"] for row in first}) == 4
    assert len({row["seed"] for row in first}) == 4
    assert len({row["behavior_cluster_id"] for row in first}) == 4


def test_candidate_rows_accepts_explicit_evaluation_round_id() -> None:
    class _Pair:
        primary = {"candidate_id": "c1", "canonical_expression": "CSRank($x)", "expression": "CSRank($x)"}
        control = {"candidate_id": "k1", "canonical_expression": "CSRank(Sign($x))", "expression": "CSRank(Sign($x))"}

    class _Grammar:
        def propose(self, route_id: str, *, attempt_index: int, seed: int) -> _Pair:
            _Pair.primary = dict(_Pair.primary, pair_id="p1", route_id=route_id)
            _Pair.control = dict(_Pair.control, pair_id="p1", route_id=route_id)
            return _Pair()

    selected = [{
        "candidate_id": "c1", "route_id": "MINUTE_STATIC", "policy_id": "p",
        "seed": "1", "skeleton_id": "s", "behavior_cluster_id": "1",
    }]
    compact = {"c1": {
        "candidate_id": "c1", "route_id": "MINUTE_STATIC", "route_attempt_index": 0,
        "seed": 1, "canonical_expression": "CSRank($x)",
        "control_canonical_expression": "CSRank(Sign($x))",
    }}

    rows, _ = _candidate_rows(
        selected,
        compact_by_id=compact,
        grammar=_Grammar(),
        round_id="STRICT_WAVE_00064",
    )

    assert {row["round_id"] for row in rows} == {"STRICT_WAVE_00064"}


def test_candidate_rows_reconstructs_supplemental_pair_and_propagates_receipt_hash() -> None:
    class _Pair:
        primary = {
            "candidate_id": "supplemental-c1",
            "canonical_expression": "CSRank($state_close_range_location_sign)",
            "expression": "CSRank($state_close_range_location_sign)",
            "pair_id": "supplemental-p1",
            "route_id": "INTRADAY_STATE_TRANSITION",
        }
        control = {
            "candidate_id": "supplemental-k1",
            "canonical_expression": "CSRank(Mul(0,$state_close_range_location_sign))",
            "expression": "CSRank(Mul(0,$state_close_range_location_sign))",
            "pair_id": "supplemental-p1",
            "route_id": "INTRADAY_STATE_TRANSITION",
        }

    class _Grammar:
        def propose(self, *args: object, **kwargs: object) -> _Pair:
            raise AssertionError("base proposer must not reconstruct supplemental receipt")

        def propose_supplemental(self, *args: object, **kwargs: object) -> _Pair:
            return _Pair()

    selected = [
        {
            "candidate_id": "supplemental-c1",
            "route_id": "INTRADAY_STATE_TRANSITION",
            "policy_id": "supplemental",
            "seed": "1",
            "skeleton_id": "s",
            "behavior_cluster_id": "1",
        }
    ]
    binding = {"state_close_range_location_sign": "c" * 64}
    compact = {
        "supplemental-c1": {
            "candidate_id": "supplemental-c1",
            "route_id": "INTRADAY_STATE_TRANSITION",
            "route_attempt_index": 0,
            "seed": 1,
            "generator_version": "cn_typed_compositional_supplemental_v1",
            "canonical_expression": "CSRank($state_close_range_location_sign)",
            "control_canonical_expression": (
                "CSRank(Mul(0,$state_close_range_location_sign))"
            ),
            "declared_field_ids": ["state_close_range_location_sign"],
            "runtime_ready": True,
            "materialization_status": "MATERIALIZED_DEVELOPMENT_ONLY",
            "strict_evaluation_allowed": True,
            "materialization_support_receipt_hashes": binding,
        }
    }

    rows, _ = _candidate_rows(
        selected,
        compact_by_id=compact,
        grammar=_Grammar(),
        verified_receipt_hashes=binding,
    )

    assert len(rows) == 2
    assert all(row["materialization_support_receipt_hashes"] == binding for row in rows)
