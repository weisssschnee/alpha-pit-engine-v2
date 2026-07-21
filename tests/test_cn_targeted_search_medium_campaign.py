from our_system_phase2.runtime.cn_targeted_search_medium_campaign import (
    CHECKPOINT_BASE_TARGETS,
    CHECKPOINT_COUNT,
    MAX_RAW_ATTEMPTS,
    SEARCH_ROUTES,
    TOTAL_SCHEDULED_MATCHED_PAIR_BUDGET,
    build_seed_attempt_manifest,
)


def test_checkpoint_base_targets_close_exact_campaign_schedule() -> None:
    assert len(CHECKPOINT_BASE_TARGETS) == CHECKPOINT_COUNT == 6
    assert all(sum(row.values()) == 256 for row in CHECKPOINT_BASE_TARGETS)
    totals = {
        route: sum(row[route] for row in CHECKPOINT_BASE_TARGETS)
        for route in SEARCH_ROUTES
    }
    assert totals == {
        "MINUTE_STATIC": 224,
        "FIRSTN_PATH": 192,
        "SLOW_CROSS_SECTIONAL_LEVEL": 256,
        "SLOW_TEMPORAL_CHANGE": 224,
        "DISCLOSURE_EVENT": 160,
        "MARKET_REGIME_CONDITION": 192,
        "INTRADAY_STATE_TRANSITION": 288,
    }
    assert sum(totals.values()) == TOTAL_SCHEDULED_MATCHED_PAIR_BUDGET


def test_seed_attempt_ranges_are_disjoint_and_bounded() -> None:
    manifest = build_seed_attempt_manifest(
        registry_hash="r" * 64,
        schema_hash_by_backend={"active_bar": "a" * 64, "stock_session": "s" * 64},
        grammar_hash="g" * 64,
        seed_base=1729,
    )

    assert manifest["frozen_route_attempt_capacity"] <= MAX_RAW_ATTEMPTS
    for route in SEARCH_ROUTES:
        ranges = [
            (int(row["attempt_start"]), int(row["attempt_stop"]))
            for row in manifest["rows"]
            if row["route_id"] == route
        ]
        assert ranges == [(index * 2300, (index + 1) * 2300) for index in range(6)]
