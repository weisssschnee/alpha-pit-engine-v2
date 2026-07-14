from __future__ import annotations

from pathlib import Path

from scripts.build_cn_feature_runtime_wiring_audit import (
    OUTPUT_NAMES,
    _split_audit,
    _synthetic_capability_tests,
    parse_list,
)


REPO = Path(__file__).resolve().parents[1]
REGISTRY = (
    REPO
    / "reports/cn_unified_capability_discovery_20260714/completed_f8169e1/registry/unified_capability_registry.json"
)


def test_parse_list_accepts_legacy_and_unified_encodings() -> None:
    assert parse_list("['high', 'low']") == ["high", "low"]
    assert parse_list("high|low") == ["high", "low"]
    assert parse_list("") == []


def test_all_nine_synthetic_capability_routes_fail_closed() -> None:
    result = _synthetic_capability_tests(REGISTRY)
    assert result["case_count"] == 9
    assert result["passed_count"] == 9
    assert not result["performance_used"]
    assert not result["validation_accessed"]
    assert not result["holdout_accessed"]
    assert not result["forward_2026_accessed"]
    for case in result["cases"]:
        assert case["status"] == "PASS"
        assert case["wrong_lag_rejection_code"] == "PIT_UNQUALIFIED"
        assert case["metadata_misuse_rejection_code"] in {
            "PIT_UNQUALIFIED",
            "ROUTE_NOT_ALLOWED",
        }


def test_split_reachability_reports_worker_mismatches() -> None:
    rows = _split_audit(REPO)
    by_component = {row["component"]: row for row in rows}
    assert by_component["FIXED_GLOBAL_MANIFEST"]["status"] == "PASS"
    assert by_component["PHASE3CM_WORKER_LOCAL_SPLIT"]["status"] == "FAIL_REACHABLE"
    assert by_component["CHUNK04_RECOVERY_WORKERS"]["status"] == "FAIL_REACHABLE"
    assert by_component["FINAL_EXACT_NORMALIZATION"]["status"] == "PASS_WITH_UPSTREAM_CAVEAT"


def test_required_deliverable_names_are_stable() -> None:
    assert len(OUTPUT_NAMES) == 15
    assert "CN_FEATURE_RUNTIME_WIRING_INDEPENDENT_AUDIT.md" in OUTPUT_NAMES
    assert "CN_FEATURE_WIRING_CAPABILITY_TEST.json" in OUTPUT_NAMES
