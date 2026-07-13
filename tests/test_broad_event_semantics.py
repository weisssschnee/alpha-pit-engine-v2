from __future__ import annotations

import json
from pathlib import Path

from our_system_phase2.services.broad_event_semantics import SEMANTIC_TYPES, validate_semantic_registry


REPO = Path(__file__).resolve().parents[1]


def test_semantic_registry_scans_121_fields_and_separates_event_kinds() -> None:
    registry = json.loads(
        (REPO / "runtime/event_registry/cn_broad_event_semantic_registry_v1.json").read_text(encoding="utf-8")
    )
    validate_semantic_registry(registry)
    assert registry["scanned_field_count"] == 121
    assert set(registry["semantic_types"]) == SEMANTIC_TYPES
    rows = {row["field_name"]: row for row in registry["field_inventory"]}
    active = rows["evt_uplimit_active"]
    assert active["semantic_type"] == "LATCHED_OBSERVATION"
    assert active["latched"] is True
    assert active["exit_detectable"] is False
    market = rows["ctx_sent_uplimit_num"]
    assert market["semantic_type"] == "REGIME_STATE"
    assert market["entity_scope"] == "MARKET"
    assert market["cross_sectional_rank_allowed"] is False
    assert market["search_role"] == "condition-only"


def test_semantic_registry_does_not_promote_carried_context_to_disclosure_pulse() -> None:
    registry = json.loads(
        (REPO / "runtime/event_registry/cn_broad_event_semantic_registry_v1.json").read_text(encoding="utf-8")
    )
    rows = {row["field_name"]: row for row in registry["field_inventory"]}
    for name in ("ctx_billboard_billboard_net_amt", "ctx_holder_holder_num", "ctx_rzrq_rzye"):
        row = rows[name]
        assert row["semantic_type"] == "LAGGED_CONTEXT"
        assert row["entry_detectable"] is False
        assert "source-date" in row["episode_definition"]

