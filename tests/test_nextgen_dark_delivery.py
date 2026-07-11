from __future__ import annotations

from pathlib import Path

from scripts.validate_nextgen_dark_delivery import validate_nextgen_dark


REPO = Path(__file__).resolve().parents[1]


def test_nextgen_dark_closure_is_ready_with_plate_scope_deferred() -> None:
    result = validate_nextgen_dark(REPO)

    assert result["status"] == "NEXTGEN_DARK_INFRASTRUCTURE_READY"
    assert result["field_count"] == 121
    assert result["temporal_primitive_count"] == 15
    assert result["event_feature_count"] == 16
    assert result["lane_count"] == 7
    assert result["benchmark_count"] == 16
    assert result["partial_artifacts"] == []
    assert result["deferred_artifacts"] == ["pit_historical_membership_release"]
    assert result["plate_scope"] == "USER_DEFERRED_EXCLUDED_FROM_CLOSURE"
    assert result["canary_execution_state"] == "PREPARED_NOT_STARTED_REQUIRES_INDEPENDENT_AUTHORIZATION"
