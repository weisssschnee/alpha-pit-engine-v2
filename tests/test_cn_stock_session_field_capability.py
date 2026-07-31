from __future__ import annotations

from pathlib import Path

import pandas as pd

from scripts.build_cn_stock_session_field_capability import (
    _direct_field_variation,
    _required_fields,
)


def test_required_fields_accept_existing_candidate_ledger_column(
    tmp_path: Path,
) -> None:
    ledger = tmp_path / "candidate_ledger.csv"
    ledger.write_text(
        "candidate_id,canonical_expression\n"
        'a,"Add($field_b,$field_a)"\n',
        encoding="utf-8",
    )
    assert _required_fields(ledger) == ("field_a", "field_b")


def test_direct_field_capability_tracks_variation_and_shard_coverage(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first.parquet"
    second = tmp_path / "second.parquet"
    pd.DataFrame(
        {
            "code": ["000001", "000001"],
            "trade_time": pd.to_datetime(
                ["2025-01-02 09:31:00", "2025-01-02 09:32:00"]
            ),
            "constant_field": [1.0, 1.0],
            "varying_field": [1.0, 2.0],
        }
    ).to_parquet(first, index=False)
    pd.DataFrame(
        {
            "code": ["000002", "000002"],
            "trade_time": pd.to_datetime(
                ["2025-01-02 09:31:00", "2025-01-02 09:32:00"]
            ),
            "varying_field": [3.0, 3.0],
        }
    ).to_parquet(second, index=False)

    variation, coverage, scanned = _direct_field_variation(
        sources=(first, second),
        fields=("constant_field", "varying_field"),
        eligible_dates=("2025-01-02",),
    )

    assert variation == {"constant_field": 1, "varying_field": 2}
    assert coverage == {"constant_field": 1, "varying_field": 2}
    assert scanned == 4
