from __future__ import annotations

import pandas as pd

from our_system_phase2.services.real_market_validation import evaluate_panel_expression


def test_expression_evaluation_keeps_runtime_layouts_out_of_dataframe_attrs() -> None:
    frame = pd.DataFrame(
        {
            "code": ["A", "A", "B", "B"],
            "trade_time": pd.to_datetime(
                [
                    "2026-01-05 09:30:00",
                    "2026-01-05 09:31:00",
                    "2026-01-05 09:30:00",
                    "2026-01-05 09:31:00",
                ]
            ),
            "x": [1.0, 3.0, 2.0, 1.0],
        }
    )
    frame.attrs["owner"] = "caller_metadata"

    actual = evaluate_panel_expression(frame, "CSRank($x)", cache={})

    assert actual.tolist() == [0.5, 1.0, 1.0, 0.5]
    assert frame.attrs == {"owner": "caller_metadata"}


def test_expression_evaluation_reports_division_tail_diagnostics_on_request() -> None:
    frame = pd.DataFrame(
        {
            "code": ["A", "A", "B", "B"],
            "trade_time": pd.to_datetime(
                [
                    "2026-01-05 09:30:00",
                    "2026-01-05 09:31:00",
                    "2026-01-05 09:30:00",
                    "2026-01-05 09:31:00",
                ]
            ),
            "x": [1.0, 2.0, 3.0, 4.0],
            "y": [0.0, 1.0, 0.0, 2.0],
        }
    )
    frame.attrs["owner"] = "caller_metadata"
    diagnostics: dict[str, object] = {}

    actual = evaluate_panel_expression(
        frame,
        "Div($x,Add(Abs($y),0.000001))",
        diagnostics=diagnostics,
    )

    assert actual.iloc[0] == 1_000_000.0
    assert diagnostics["division_node_count"] == 1
    assert diagnostics["division_min_abs"] == 0.000001
    assert float(diagnostics["division_floor_hit_ratio_max"]) == 0.5
    assert diagnostics["division_denominator_examples"]
    assert frame.attrs == {"owner": "caller_metadata"}
