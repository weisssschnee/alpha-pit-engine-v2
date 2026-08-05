from pathlib import Path

import pandas as pd
import pytest

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
    assert "CORPORATE_ACTION_RAW_SNAPSHOT" in source
    assert "excluded_missing_st_coordinates.parquet" in source
    assert "FAIL_CLOSED_COORDINATE_EXCLUSION" in source
    assert "fillna(True)" not in source
    assert "--historical-daily-st-source" in source
    assert "--expected-daily-st-source-sha256" in source
    assert ".sort_by(" not in source
    assert '"holdout_reads"' in source
    assert 'evaluation_role: str = "validation"' in source
    assert 'if evaluation_role == "forward_2026"' in source
    assert '"historical_challenge_reads"' in source
    assert '"--evaluation-role"' in source
    assert '"promotion": "FORBIDDEN"' in source


def test_validation_authority_independent_verifier_checks_sources_and_st() -> None:
    source = (PROJECT_ROOT / "scripts" / "verify_cn_validation_session_authority.py").read_text(
        encoding="utf-8"
    )
    assert "verify_source_snapshot" in source
    assert "observed ST state parity failure" in source
    assert "validation session authority has missing ST states" in source
    assert "validation daily ST source binding drift" in source
    assert "validation observed ST source coverage failure" in source
    assert "validation ST coordinate exclusion has an exact source" in source
    assert verify.build.STATUS == authority.STATUS


def test_exact_st_source_gaps_remain_explicit_for_fail_closed_merge() -> None:
    result = authority._normalize_exact_st_allowing_gaps(
        pd.Series([0, 1, None])
    )
    assert result.iloc[0] == False  # noqa: E712
    assert result.iloc[1] == True  # noqa: E712
    assert pd.isna(result.iloc[2])


def test_exact_st_source_is_hash_bound_complete_and_normalized(tmp_path: Path) -> None:
    source_path = tmp_path / "daily_st.parquet"
    pd.DataFrame(
        {
            "date": ["2025-07-08", "2025-07-08", "2025-07-09", "2025-07-09"],
            "code": ["000001", "600000", "000001", "600000"],
            "name": ["平安银行", "ST浦发", "平安银行", "ST浦发"],
            "is_st": ["否", "是", "否", "是"],
        }
    ).to_parquet(source_path, index=False)
    source_sha256 = authority.v1._sha256(source_path)
    dates = tuple(pd.to_datetime(["2025-07-08", "2025-07-09"]).date)

    exact, receipt, source_rows = authority._extract_exact_st(
        source_path=source_path,
        expected_source_sha256=source_sha256,
        validation_dates=dates,
    )

    assert source_rows == 4
    assert exact["is_st"].tolist() == [False, True, False, True]
    assert receipt["selected_st_session_count"] == 2
    assert receipt["selected_non_st_session_count"] == 2
    assert receipt["source_sha256"] == source_sha256

    with pytest.raises(RuntimeError, match="source hash drift"):
        authority._extract_exact_st(
            source_path=source_path,
            expected_source_sha256="0" * 64,
            validation_dates=dates,
        )
