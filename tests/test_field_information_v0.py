from __future__ import annotations

import json
from pathlib import Path

from our_system_phase2.services.field_information_v0 import compile_tokens, summarize
from our_system_phase2.services.unified_capability_registry import UnifiedCapabilityRegistry


REPO = Path(__file__).resolve().parents[1]
MASTER = REPO / "runtime/field_registry/cn_field_master_registry_v1/cn_field_master_registry_v1.json"
REGISTRY = REPO / "runtime/field_registry/cn_unified_capability_registry_v2_20260717/unified_capability_registry.json"


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
    assert chip["registry_present"] is False
    assert chip["generator_exposed"] is False
    disclosure = next(
        row for row in tokens
        if row["field_id"].endswith("_at_disclosure") and row["generator_exposed"]
    )
    assert disclosure["dependencies"]
    assert disclosure["episode_payload_role"] in {"DISCLOSURE_LEVEL_PAYLOAD", "DISCLOSURE_CHANGE_PAYLOAD"}
    result = summarize(tokens)
    assert result["forward_2026_accessed"] is False
    assert result["information_qualified_count"] == 0
