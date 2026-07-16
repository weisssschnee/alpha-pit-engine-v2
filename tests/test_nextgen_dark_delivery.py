from __future__ import annotations

from pathlib import Path

from scripts.refresh_nextgen_dark_artifact_index import _sha256 as refresh_sha256
from scripts.validate_nextgen_dark_delivery import _sha256 as validate_sha256
from scripts.validate_nextgen_dark_delivery import validate_nextgen_dark


REPO = Path(__file__).resolve().parents[1]


def test_nextgen_text_hash_is_newline_portable(tmp_path: Path) -> None:
    lf = tmp_path / "artifact_lf.py"
    crlf = tmp_path / "artifact_crlf.py"
    lf.write_bytes(b"alpha = 1\nbeta = 2\n")
    crlf.write_bytes(b"alpha = 1\r\nbeta = 2\r\n")

    assert refresh_sha256(lf) == refresh_sha256(crlf)
    assert validate_sha256(lf) == validate_sha256(crlf)


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
