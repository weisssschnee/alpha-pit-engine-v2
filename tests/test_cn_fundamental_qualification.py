from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts.build_cn_fundamental_qualification import build
from our_system_phase2.services.fundamental_representations import (
    FUNDAMENTAL_SOURCE_RELEASE,
    PROVIDER_BY_TABLE,
)
from our_system_phase2.services.unified_capability_registry import source_field_id


REPO = Path(__file__).resolve().parents[1]


def _rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_fundamental_qualification_is_stable_and_fail_closed(tmp_path: Path) -> None:
    runtime_root = tmp_path / "runtime"
    report_root = tmp_path / "reports"
    summary = build(
        source=REPO / "runtime/cn_pit_fundamental_fabric_v1/fundamental_source_universe.csv",
        policy_path=REPO / "runtime/run_plans/cn_fundamental_field_qualification_policy_v1.json",
        merge_contract_path=REPO / "runtime/run_plans/cn_unified_registry_merge_contract_v1.json",
        runtime_root=runtime_root,
        report_root=report_root,
    )
    matrix = _rows(runtime_root / "fundamental_field_qualification_matrix.csv")
    registry = json.loads(
        (runtime_root / "fundamental_canonical_representation_registry.json").read_text(
            encoding="utf-8"
        )
    )

    assert len(matrix) == 1227
    assert len({row["source_field_id"] for row in matrix}) == 1227
    total_assets = next(
        row
        for row in matrix
        if row["source_table"] == "balance_sheet_report_em"
        and row["source_field"] == "TOTAL_ASSETS"
    )
    assert total_assets["source_field_id"] == source_field_id(
        provider=PROVIDER_BY_TABLE["balance_sheet_report_em"],
        source_release_version=FUNDAMENTAL_SOURCE_RELEASE,
        source_table="balance_sheet_report_em",
        source_field="TOTAL_ASSETS",
    )
    assert total_assets["qualification_state"] == "DERIVED_ONLY"
    assert total_assets["raw_generator_exposure"] == "False"

    fail_closed = {
        "INDUSTRY_SPECIFIC",
        "LOW_COVERAGE_ARCHIVE",
        "SEMANTICS_UNRESOLVED",
        "PIT_UNRESOLVED",
        "BLOCKED_METADATA",
    }
    assert all(
        not row["allowed_routes"]
        for row in matrix
        if row["qualification_state"] in fail_closed
    )
    zygc = [row for row in matrix if row["source_table"] == "zygc_em"]
    assert len(zygc) == 17
    assert {row["qualification_state"] for row in zygc} == {"PIT_UNRESOLVED"}

    assert registry["source_universe_count"] == 1227
    assert registry["representation_count"] < 384
    assert not registry["raw_1227_generator_exposure"]
    assert not registry["current_snapshot_revision_replay_supported"]
    assert all(
        not row["historical_revision_replay_supported"]
        and not row["performance_used"]
        for row in registry["representations"]
    )
    searchable_representation_ids = {
        row["representation_id"]
        for row in registry["representations"]
        if row["search_eligible"]
    }
    assert all(
        not searchable_representation_ids.intersection(
            value for value in row["representation_ids"].split("|") if value
        )
        for row in matrix
        if row["qualification_state"] in fail_closed
    )

    source_yoy = {
        row["source_fields"][0]["source_field"][:-4]: row["representation_id"]
        for row in registry["representations"]
        if row["representation_type"] == "source_reported_yoy"
    }
    internal_yoy = {
        row["source_fields"][0]["source_field"]: row["representation_id"]
        for row in registry["representations"]
        if row["representation_type"] == "internally_derived_yoy"
    }
    shared = set(source_yoy).intersection(internal_yoy)
    assert shared
    assert all(source_yoy[name] != internal_yoy[name] for name in shared)

    representations = registry["representations"]
    payloads = {
        row["parameters"]["payload_source_representation_id"]: row
        for row in representations
        if row["route_id"] == "DISCLOSURE_EVENT"
        and row["temporal_semantics"]
        in {"DISCLOSURE_LEVEL_PAYLOAD", "DISCLOSURE_CHANGE_PAYLOAD"}
    }
    eligible_payload_sources = {
        row["representation_id"]
        for row in representations
        if row["search_eligible"]
        and row["route_id"] in {"SLOW_CROSS_SECTIONAL_LEVEL", "SLOW_TEMPORAL_CHANGE"}
        and row["operation"] != "staleness_sessions"
        and row["field_role"] in {"primary", "interaction-only"}
    }
    assert set(payloads) == eligible_payload_sources
    assert all(row["support_unit"] == "disclosure episode" for row in payloads.values())
    assert not any(
        any(source["source_table"] == "zygc_em" for source in row["source_fields"])
        for row in payloads.values()
    )

    assert summary["qualification_state_counts"]["PIT_UNRESOLVED"] == 17
    assert not summary["performance_used"]
    assert not summary["validation_accessed"]
    assert not summary["holdout_accessed"]
    assert not summary["forward_2026_accessed"]
    assert not summary["candidate_promotion"]
