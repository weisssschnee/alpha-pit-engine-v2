from __future__ import annotations

import pandas as pd

from our_system_phase2.services.compositional_session_signal_panel import (
    attach_coordinate_row_indices,
    build_session_coordinate_rows,
    field_partition,
)


def test_session_coordinates_collapse_intraday_slots_without_mixing_ab() -> None:
    active = [
        {
            "coordinate_set": coordinate_set,
            "code": "000001.SZ",
            "trade_date": "2025-01-02",
            "trade_time": f"2025-01-02 {clock}",
        }
        for coordinate_set in ("A", "B")
        for clock in ("09:35:00", "14:30:00")
    ]
    rows = build_session_coordinate_rows(
        active,
        available_keys={("000001.SZ", pd.Timestamp("2025-01-02"))},
    )

    assert len(rows) == 2
    assert {row["coordinate_set"] for row in rows} == {"A", "B"}
    assert all(row["trade_time"].startswith("2025-01-02T15:00:00") for row in rows)
    assert all(row["data_role"] == "development" for row in rows)
    assert all(row["row_index"] == 0 for row in rows)


def test_coordinate_indices_and_field_partitions_are_deterministic() -> None:
    panel = pd.DataFrame(
        {
            "code": ["000001.SZ", "000002.SZ"],
            "trade_time": pd.to_datetime(["2025-01-02 15:00", "2025-01-02 15:00"]),
        }
    )
    rows = [
        {"code": "000002.SZ", "trade_time": "2025-01-02T15:00:00", "coordinate_set": "A"},
        {"code": "000003.SZ", "trade_time": "2025-01-02T15:00:00", "coordinate_set": "B"},
    ]

    indexed = attach_coordinate_row_indices(rows, panel)

    assert [row["row_index"] for row in indexed] == [1, -1]
    assert field_partition("fund_example", 7) == field_partition("fund_example", 7)
    assert 0 <= field_partition("fund_example", 7) < 7
