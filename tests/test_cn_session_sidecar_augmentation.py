from __future__ import annotations

import json

import pandas as pd
import pytest

from scripts.augment_cn_phase3cm_session_time_major_sidecar import (
    _materialize_incremental_chip_context,
    _materialize_lagged_daily_context,
)


def _bar_root(tmp_path, values: list[float]):
    root = tmp_path / "release"
    shard = root / "shard_00"
    shard.mkdir(parents=True)
    (root / "development_only_release_manifest.json").write_text(
        json.dumps({"forbidden_roles_present": [], "forward_2026_present": False}),
        encoding="utf-8",
    )
    pd.DataFrame(
        {
            "code": ["000001.SZ", "000001.SZ"],
            "trade_time": pd.to_datetime(["2024-01-02 09:31", "2024-01-02 15:00"]),
            "ctx_hfq_pe_ttm": values,
        }
    ).to_parquet(shard / "raw.parquet", index=False)
    return root


def test_lagged_daily_context_joins_pre_lagged_session_constant(tmp_path) -> None:
    frame = pd.DataFrame(
        {
            "code": ["000001.SZ"],
            "trade_time": pd.to_datetime(["2024-01-02 15:00"]),
        }
    )

    output, evidence = _materialize_lagged_daily_context(
        frame,
        fields=["ctx_hfq_pe_ttm"],
        bar_source_root=_bar_root(tmp_path, [12.5, 12.5]),
        shard_index=0,
    )

    assert output["ctx_hfq_pe_ttm"].tolist() == [12.5]
    assert evidence["maximum_intraday_unique_values"] == {"ctx_hfq_pe_ttm": 1}


def test_lagged_daily_context_fails_when_value_changes_inside_session(tmp_path) -> None:
    frame = pd.DataFrame(
        {
            "code": ["000001.SZ"],
            "trade_time": pd.to_datetime(["2024-01-02 15:00"]),
        }
    )

    with pytest.raises(ValueError, match="changes within a session"):
        _materialize_lagged_daily_context(
            frame,
            fields=["ctx_hfq_pe_ttm"],
            bar_source_root=_bar_root(tmp_path, [12.5, 13.0]),
            shard_index=0,
        )


def test_incremental_chip_context_preserves_existing_source_session() -> None:
    frame = pd.DataFrame(
        {
            "code": ["000001.SZ"],
            "trade_time": pd.to_datetime(["2024-01-03 15:00"]),
            "chip_source_session": pd.to_datetime(["2024-01-02"]),
            "chip_cost_p50": [10.0],
        }
    )
    chip = pd.DataFrame(
        {
            "code": ["000001.SZ"],
            "source_session": pd.to_datetime(["2024-01-02"]),
            "chip_cost_p95": [12.0],
        }
    )

    output = _materialize_incremental_chip_context(
        frame,
        chip,
        fields=["chip_cost_p95"],
    )

    pd.testing.assert_series_equal(
        output["chip_source_session"], frame["chip_source_session"], check_names=False
    )
    assert output["chip_cost_p50"].tolist() == [10.0]
    assert output["chip_cost_p95"].tolist() == [12.0]


def test_incremental_chip_context_fails_on_source_session_drift() -> None:
    frame = pd.DataFrame(
        {
            "code": ["000001.SZ"],
            "trade_time": pd.to_datetime(["2024-01-04 15:00"]),
            "chip_source_session": pd.to_datetime(["2024-01-01"]),
        }
    )
    chip = pd.DataFrame(
        {
            "code": ["000001.SZ"],
            "source_session": pd.to_datetime(["2024-01-02"]),
            "chip_cost_p95": [12.0],
        }
    )

    with pytest.raises(RuntimeError, match="source-session drift"):
        _materialize_incremental_chip_context(
            frame,
            chip,
            fields=["chip_cost_p95"],
        )
