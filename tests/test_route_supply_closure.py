from __future__ import annotations

from pathlib import Path

import pandas as pd

from our_system_phase2.services.phase3cm_streaming_expression import (
    unsupported_streaming_operators,
)
from our_system_phase2.services.portfolio_behavior_archive import (
    bounded_label_free_behavior_probe,
)
from our_system_phase2.services.route_supply_closure import (
    FROZEN_REFERENCE_ONLY,
    PRIMARY_SEARCH_ROUTES,
    classify_clamped_route,
    diagnose_exact_supply,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)
from our_system_phase2.services.unified_discovery_generators import (
    COMPOSITIONAL_V2_PROFILE,
    LEGACY_V1_PROFILE,
    RegistryDrivenGenerator,
)
from scripts.run_cn_route_supply_closure import TARGET_BEHAVIOR_ROUTES


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "runtime/field_registry/cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)


def test_bounded_behavior_qualification_covers_prior_clamped_routes() -> None:
    assert {
        "INTRADAY_STATE_TRANSITION",
        "MINUTE_STATIC",
    }.issubset(TARGET_BEHAVIOR_ROUTES)


def test_compositional_profile_repairs_disclosure_exact_supply_without_second_authority() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    legacy = RegistryDrivenGenerator(registry, constructor_profile=LEGACY_V1_PROFILE)
    upgraded = RegistryDrivenGenerator(
        registry, constructor_profile=COMPOSITIONAL_V2_PROFILE
    )

    legacy_rows, legacy_funnel = legacy.generate_route_attempts(
        "DISCLOSURE_EVENT",
        scheduled_pairs=24,
        seed=20260721,
        attempt_limit=256,
    )
    upgraded_rows, upgraded_funnel = upgraded.generate_route_attempts(
        "DISCLOSURE_EVENT",
        scheduled_pairs=24,
        seed=20260721,
        attempt_limit=256,
    )

    assert legacy_funnel["exact_unique_pairs"] == 4
    assert len(legacy_rows) == 8
    assert upgraded_funnel["exact_unique_pairs"] == 24
    assert len(upgraded_rows) == 48
    assert upgraded_funnel["top_level_scheduling_key"] == "unified_registry_route_id"
    assert upgraded_funnel["constructor_profile"] == COMPOSITIONAL_V2_PROFILE
    assert any(len(row["declared_field_ids"]) >= 2 for row in upgraded_rows)
    assert not unsupported_streaming_operators(
        row["expression"] for row in upgraded_rows
    )


def test_registry_attempt_stream_can_require_current_materialized_fields() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    generator = RegistryDrivenGenerator(
        registry, constructor_profile=COMPOSITIONAL_V2_PROFILE
    )
    baseline, _ = generator.generate_route_attempts(
        "DISCLOSURE_EVENT",
        scheduled_pairs=12,
        seed=2026072201,
        attempt_limit=256,
    )
    first_pair_fields = {
        str(field)
        for row in baseline[:2]
        for field in (row.get("declared_field_ids") or ())
    }
    available = {
        row.field_id for row in registry.fields_for_route("DISCLOSURE_EVENT")
    } - first_pair_fields

    rows, funnel = generator.generate_route_attempts(
        "DISCLOSURE_EVENT",
        scheduled_pairs=8,
        seed=2026072201,
        attempt_limit=512,
        available_field_ids=available,
    )

    assert len(rows) == 16
    assert funnel["materialization_missing_field_pairs"] > 0
    assert all(
        set(map(str, row.get("declared_field_ids") or ())).issubset(available)
        for row in rows
    )


def test_supply_diagnosis_excludes_frozen_broad_event_and_requires_headroom() -> None:
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    report = diagnose_exact_supply(
        RegistryDrivenGenerator(
            registry, constructor_profile=COMPOSITIONAL_V2_PROFILE
        ),
        route_ids=("INTRADAY_STATE_TRANSITION", "BROAD_EVENT_FROZEN_ENTRY"),
        seeds=(1729, 2718),
        attempt_caps=(32, 128),
        required_pairs=12,
        headroom_multiplier=2,
        historical_exact=set(),
    )

    state = report["routes"]["INTRADAY_STATE_TRANSITION"]
    broad = report["routes"]["BROAD_EVENT_FROZEN_ENTRY"]
    assert state["search_role"] == "PRIMARY_SEARCH"
    assert state["minimum_final_exact_unique_pairs"] >= 24
    assert state["classification"] == "SEARCHABLE_SUPPLY_READY_WITH_HEADROOM"
    assert broad["search_role"] == FROZEN_REFERENCE_ONLY
    assert broad["classification"] == "FROZEN_REFERENCE_ONLY_NOT_SEARCH_SUPPLY"
    assert "BROAD_EVENT_FROZEN_ENTRY" not in PRIMARY_SEARCH_ROUTES


def test_clamp_classification_does_not_turn_repaired_legacy_cycle_into_phase_blocker() -> None:
    diagnosis = classify_clamped_route(
        route_id="INTRADAY_STATE_TRANSITION",
        legacy_classification="STABLE_EXACT_SUPPLY_BOTTLENECK",
        upgraded_classification="SEARCHABLE_SUPPLY_READY_WITH_HEADROOM",
        prior_feedback_status="ACTIONABLE_FEEDBACK_CLAMPED",
    )

    assert diagnosis["root_cause"] == "LEGACY_CONSTRUCTOR_CYCLE_EXHAUSTION"
    assert diagnosis["repair_status"] == "REPAIRED_BY_REGISTRY_COMPOSITIONAL_PROFILE"
    assert diagnosis["blocks_next_development_phase"] is False


def _pair(route_id: str, primary: str, control: str, *, condition: str) -> list[dict[str, object]]:
    pair_id = f"pair.{route_id.lower()}"
    common = {
        "pair_id": pair_id,
        "route_id": route_id,
        "operator_family": "ConditionGate",
        "condition_field_ids": [condition],
    }
    return [
        {
            **common,
            "candidate_id": pair_id + ".primary",
            "expression": primary,
        },
        {
            **common,
            "candidate_id": pair_id + ".control",
            "expression": control,
        },
    ]


def test_behavior_probe_uses_multiple_stratified_development_dates(tmp_path: Path) -> None:
    rows: list[dict[str, object]] = []
    for day_index, date in enumerate(("2024-01-02", "2024-01-03")):
        for minute in range(2):
            for code_index in range(10):
                rows.append(
                    {
                        "code": f"S{code_index:02d}",
                        "trade_time": pd.Timestamp(date) + pd.Timedelta(
                            hours=9, minutes=31 + minute
                        ),
                        "close": 10.0 + code_index,
                        "payload": float(code_index - 5),
                        "regime": 1.0 if day_index == 0 else -1.0,
                    }
                )
    sidecar = tmp_path / "fields.parquet"
    frame = pd.DataFrame(rows)
    frame["source_shard"] = 0
    frame["source_row_identity"] = [f"row-{index}" for index in range(len(frame))]
    frame["duplicate_ordinal"] = 0
    frame.to_parquet(sidecar, index=False)

    records, audit = bounded_label_free_behavior_probe(
        candidates=_pair(
            "MARKET_REGIME_CONDITION",
            "CSRank(Mul(Sign($regime),$payload))",
            "CSRank($payload)",
            condition="regime",
        ),
        field_sidecars=(sidecar,),
        eligible_trade_dates=("2024-01-02", "2024-01-03"),
        coordinate_binding="multi-date-test",
        batch_id="probe",
        compute_threads=1,
        max_trade_dates=2,
        max_trade_times=2,
        min_obs=10,
    )

    assert audit["probe_dates"] == ["2024-01-02", "2024-01-03"]
    assert audit["trade_time_count"] == 4
    assert records[0]["behavior_status"] == "RESOLVED"


def test_behavior_probe_can_select_label_free_condition_activation_date(
    tmp_path: Path,
) -> None:
    rows: list[dict[str, object]] = []
    dates = ("2024-01-02", "2024-01-03", "2024-01-04")
    for date in dates:
        for code_index in range(20):
            rows.append(
                {
                    "code": f"S{code_index:02d}",
                    "trade_time": pd.Timestamp(date) + pd.Timedelta(hours=15),
                    "close": 10.0 + code_index,
                    "event": 1.0
                    if date == "2024-01-03" and code_index % 4 == 0
                    else 0.0,
                }
            )
    sidecar = tmp_path / "sessions.parquet"
    frame = pd.DataFrame(rows)
    frame["source_shard"] = 0
    frame["source_row_identity"] = [f"row-{index}" for index in range(len(frame))]
    frame["duplicate_ordinal"] = 0
    frame.to_parquet(sidecar, index=False)

    _, audit = bounded_label_free_behavior_probe(
        candidates=_pair(
            "DISCLOSURE_EVENT",
            "EventCount($event,1)",
            "TimeSince($event)",
            condition="event",
        ),
        field_sidecars=(sidecar,),
        eligible_trade_dates=dates,
        coordinate_binding="activation-test",
        batch_id="probe",
        compute_threads=1,
        max_trade_dates=1,
        max_trade_times=1,
        min_obs=10,
        date_selection="condition_activation",
    )

    assert audit["probe_dates"] == ["2024-01-03"]
    assert audit["date_selection"] == "condition_activation"
    assert audit["label_sidecar_paths_accepted"] == 0
