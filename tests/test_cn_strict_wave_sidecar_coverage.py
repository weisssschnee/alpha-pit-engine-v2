from __future__ import annotations

from scripts.audit_cn_strict_wave_sidecar_coverage import required_raw_fields


def test_required_raw_fields_extracts_unique_typed_roots_and_close() -> None:
    rows = [
        {"expression": "Rank(Delta($fund_profit, 5) + $close)"},
        {"expression": "Winsorize($fund_profit)"},
    ]

    assert required_raw_fields(rows) == ["close", "fund_profit"]
