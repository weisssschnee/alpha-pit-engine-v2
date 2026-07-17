from __future__ import annotations

import json
from pathlib import Path

from our_system_phase2.services.field_information_v0 import (
    apply_information_census,
    compile_tokens,
    summarize,
)
from our_system_phase2.services.unified_capability_registry import UnifiedCapabilityRegistry


REPO = Path(__file__).resolve().parents[1]
MASTER = REPO / "runtime/field_registry/cn_field_master_registry_v1/cn_field_master_registry_v1.json"
REGISTRY = REPO / "runtime/field_registry/cn_unified_capability_registry_v3_20260717/unified_capability_registry.json"


def test_tokens_trace_authority_and_exposure_without_information_claims() -> None:
    master = json.loads(MASTER.read_text(encoding="utf-8"))
    tokens = compile_tokens(
        master, UnifiedCapabilityRegistry.read(REGISTRY),
        master_path=MASTER, registry_path=REGISTRY, attempts_per_route=64,
    )
    assert len(tokens) == 1683
    assert len({row["field_token_id"] for row in tokens}) == 1683
    assert all(row["source_registry_sha256"] == master["content_sha256"] for row in tokens)
    assert not any(row["information_qualified"] or row["core_pack_selected"] for row in tokens)
    chip = next(row for row in tokens if row["field_id"] == "chip_cost_p50")
    assert chip["context"] == "CHIP_PLATE_STATE"
    assert chip["registry_present"] is True
    assert chip["search_allowed"] is True
    assert set(chip["allowed_routes"]) == {"SLOW_CROSS_SECTIONAL_LEVEL", "SLOW_TEMPORAL_CHANGE"}
    assert any(
        row["family"] == "chip_distribution" and row["generator_exposed"]
        for row in tokens
    )
    chip_benchmark = next(row for row in tokens if row["field_id"] == "chip_historical_low")
    assert chip_benchmark["benchmark_only"] is True
    assert chip_benchmark["search_allowed"] is False
    plate_peer = next(row for row in tokens if row["field_id"] == "plate_peer_return_mean")
    assert plate_peer["allowed_routes"] == ["MINUTE_STATIC"]
    assert any(
        row["family"] == "true1min_plate_sparse" and row["generator_exposed"]
        for row in tokens
    )
    plate_condition = next(row for row in tokens if row["field_id"] == "plate_coverage")
    assert plate_condition["condition_only"] is True
    assert plate_condition["search_allowed"] is False
    disclosure = next(
        row for row in tokens
        if row["field_id"].endswith("_at_disclosure") and row["generator_exposed"]
    )
    assert disclosure["dependencies"]
    assert disclosure["episode_payload_role"] in {"DISCLOSURE_LEVEL_PAYLOAD", "DISCLOSURE_CHANGE_PAYLOAD"}
    result = summarize(tokens)
    assert result["forward_2026_accessed"] is False
    assert result["information_qualified_count"] == 0


def test_census_evidence_updates_only_evaluated_tokens(tmp_path: Path) -> None:
    master = json.loads(MASTER.read_text(encoding="utf-8"))
    tokens = compile_tokens(
        master, UnifiedCapabilityRegistry.read(REGISTRY),
        master_path=MASTER, registry_path=REGISTRY, attempts_per_route=8,
    )
    evidence = tmp_path / "run_manifest.json"
    evidence.write_text('{"status":"completed"}\n', encoding="utf-8")
    updated = apply_information_census(
        tokens,
        metrics=[{
            "field_id": "chip_cost_p50",
            "coverage": 0.99,
            "sample_unique": 100,
            "temporal_change_rate": 0.5,
        }],
        core_pack={"selected_field_ids": ["chip_cost_p50"]},
        evidence_path=evidence,
    )
    chip = next(row for row in updated if row["field_id"] == "chip_cost_p50")
    plate = next(row for row in updated if row["field_id"] == "plate_peer_return_mean")
    assert chip["information_qualified"] is True
    assert chip["core_pack_selected"] is True
    assert plate["information_status"] == "NOT_EVALUATED"
    assert summarize(updated)["core_pack_selected_count"] == 1
