from __future__ import annotations

import pytest

from scripts.run_cn_tpe_availability_yield_qualification import (
    BEHAVIOR_SAMPLE_SIZE,
    MINIMUM_ROUTE_BEHAVIOR_SAMPLE,
    _select_behavior_sample,
    _target_weighted_quotas,
    _wilson_interval,
)
from our_system_phase2.runtime.cn_large_tpe_search_campaign import ROUTES

SUCCESSOR_TARGETS = {
    "SLOW_TEMPORAL_CHANGE": 14_000,
    "FIRSTN_PATH": 1_300,
    "SLOW_CROSS_SECTIONAL_LEVEL": 900,
    "MARKET_REGIME_CONDITION": 380,
    "DISCLOSURE_EVENT": 300,
}


def test_behavior_qualification_quotas_preserve_route_floor_and_total() -> None:
    quotas = _target_weighted_quotas(
        BEHAVIOR_SAMPLE_SIZE,
        SUCCESSOR_TARGETS,
    )

    assert set(quotas) == set(ROUTES)
    assert sum(quotas.values()) == BEHAVIOR_SAMPLE_SIZE
    assert min(quotas.values()) >= MINIMUM_ROUTE_BEHAVIOR_SAMPLE
    assert quotas["SLOW_TEMPORAL_CHANGE"] == max(quotas.values())


def test_wilson_interval_is_conservative_and_not_a_fixed_yield_floor() -> None:
    lower, upper = _wilson_interval(14, 1_000)

    assert lower < 0.014 < upper
    assert lower < 0.01
    assert upper < 0.03


def test_behavior_sample_is_deterministic_unique_and_route_stratified() -> None:
    modes = (
        "TPE_DIRECT_FRESH",
        "TPE_BUCKET_REPLACEMENT",
        "GLOBAL_AVAILABILITY_FALLBACK",
    )
    emissions = []
    for route_index, route_id in enumerate(ROUTES):
        for index in range(1_300):
            emissions.append(
                {
                    "route_id": route_id,
                    "original_checkpoint": f"checkpoint_{index % 24 + 1:03d}",
                    "original_checkpoint_index": index % 24 + 1,
                    "bucket_key": f"bucket-{route_index}-{index % 17}",
                    "exact_identity": f"exact-{route_index}-{index}",
                    "emission_mode": modes[index % len(modes)],
                    "genes": {
                        "skeleton_id": f"skeleton-{route_index}",
                        "primary_field_id": f"field-{index}",
                    },
                }
            )

    first = _select_behavior_sample(
        emissions=emissions,
        qualification_seed=91,
        sample_size=BEHAVIOR_SAMPLE_SIZE,
        route_targets=SUCCESSOR_TARGETS,
    )
    second = _select_behavior_sample(
        emissions=emissions,
        qualification_seed=91,
        sample_size=BEHAVIOR_SAMPLE_SIZE,
        route_targets=SUCCESSOR_TARGETS,
    )

    assert first == second
    assert len(first) == BEHAVIOR_SAMPLE_SIZE
    assert len({row["exact_identity"] for row in first}) == len(first)
    for route_id in ROUTES:
        route_rows = [
            row for row in first if row["route_id"] == route_id
        ]
        assert len(route_rows) >= MINIMUM_ROUTE_BEHAVIOR_SAMPLE
        assert {row["replay_stage"] for row in route_rows} == {
            "EARLY",
            "MIDDLE",
            "LATE",
        }
        assert {
            row["emission_mode"] for row in route_rows
        } == set(modes)
