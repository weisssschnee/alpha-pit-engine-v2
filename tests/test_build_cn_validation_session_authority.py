from pathlib import Path

from scripts import build_cn_validation_session_authority as authority
from scripts import verify_cn_validation_session_authority as verify


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_validation_authority_contract_is_report_only_and_bounded() -> None:
    assert authority.STATUS == "VALIDATION_SESSION_AUTHORITY_CLOSED_IMMUTABLE"
    assert authority.DATE_MIN == "2025-07-08"
    assert authority.DATE_MAX == "2025-10-24"
    assert authority.ALLOWED_EXCHANGES == ("SSE", "SZSE")


def test_validation_authority_builder_has_explicit_fail_closed_boundaries() -> None:
    source = (PROJECT_ROOT / "scripts" / "build_cn_validation_session_authority.py").read_text(
        encoding="utf-8"
    )
    assert "EXCLUDE_IF_ABSENT_FROM_IMMUTABLE_SSE_SZSE_SECURITY_MASTER" in source
    assert "fillna(True)" in source
    assert '"holdout_reads": 0' in source
    assert '"forward_2026_reads": 0' in source
    assert '"promotion": "FORBIDDEN"' in source


def test_validation_authority_independent_verifier_checks_sources_and_st() -> None:
    source = (PROJECT_ROOT / "scripts" / "verify_cn_validation_session_authority.py").read_text(
        encoding="utf-8"
    )
    assert "verify_source_snapshot" in source
    assert "observed ST state parity failure" in source
    assert verify.build.STATUS == authority.STATUS
