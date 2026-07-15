from __future__ import annotations

import numpy as np
import pandas as pd

from our_system_phase2.services.expression_semantics import analyze_expression
from our_system_phase2.services.real_market_validation import evaluate_panel_expression


def test_safe_div_uses_sign_preserving_floor_and_keeps_nan_missing() -> None:
    frame = pd.DataFrame(
        {
            "code": ["000001", "000001", "000001", "000001", "000001"],
            "date": pd.to_datetime(["2025-01-02"] * 5),
            "left": [1.0, 1.0, 1.0, 1.0, 1.0],
            "right": [0.0, 0.01, -0.01, 0.20, np.nan],
        }
    )

    result = evaluate_panel_expression(frame, "SafeDiv($left,$right,0.05)")

    assert result.iloc[:4].tolist() == [20.0, 20.0, -20.0, 5.0]
    assert np.isnan(result.iloc[4])


def test_safe_div_floor_must_be_positive_and_finite() -> None:
    for expression in (
        "SafeDiv($left,$right,0)",
        "SafeDiv($left,$right,-0.1)",
        "SafeDiv($left,$right,nan)",
    ):
        analysis = analyze_expression(expression)
        assert analysis.hard_blocked
        assert "INVALID_SAFEDIV_FLOOR" in analysis.issue_codes


def test_winsorize_clips_within_each_observable_cross_section() -> None:
    frame = pd.DataFrame(
        {
            "code": [f"{index:06d}" for index in range(101)],
            "date": pd.to_datetime(["2025-01-02"] * 101),
            "value": [*map(float, range(100)), 10_000.0],
        }
    )

    result = evaluate_panel_expression(frame, "Winsorize($value)")

    assert result.max() < 10_000.0
    assert result.min() > 0.0
    assert result.iloc[50] == 50.0
