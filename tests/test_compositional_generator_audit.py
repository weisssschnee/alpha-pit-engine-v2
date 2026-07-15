from __future__ import annotations

from pathlib import Path

from our_system_phase2.services.compositional_generator_audit import (
    audit_generator_expressivity,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "reports/cn_unified_capability_discovery_20260714/completed_f8169e1/registry"
    / "unified_capability_registry.json"
)


def test_structural_audit_separates_legacy_templates_from_compositional_grammar() -> None:
    report = audit_generator_expressivity(
        UnifiedCapabilityRegistry.read(REGISTRY),
        attempts_per_generator=256,
        seed=1729,
    )

    assert report["economic_evaluator_accessed"] is False
    assert report["behavior_identity_status"] == "NOT_EVALUATED"
    assert set(report["generators"]) == {"legacy_registry_v1", "compositional_v2"}
    assert report["generators"]["legacy_registry_v1"]["attempts"] == 256
    assert report["generators"]["compositional_v2"]["attempts"] == 256
    assert report["generators"]["legacy_registry_v1"]["routes"]["MINUTE_STATIC"][
        "distinct_expression_skeleton_count"
    ] < 8
    for route_id, row in report["generators"]["compositional_v2"]["routes"].items():
        if route_id == "BROAD_EVENT_FROZEN_ENTRY":
            assert row["search_role"] == "FROZEN_REFERENCE_ONLY"
        else:
            assert row["distinct_expression_skeleton_count"] >= 8
        assert row["failure_categories_are_mutually_exclusive"] is True
