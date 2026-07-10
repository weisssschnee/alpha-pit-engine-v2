from __future__ import annotations

from our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit import _bootstrap_days


def test_bootstrap_days_is_deterministic_and_reports_vectorized_engine() -> None:
    day_values = [0.01, -0.02, 0.03, -0.01, 0.015] * 20

    first = _bootstrap_days(day_values, iterations=600, seed=20260710)
    second = _bootstrap_days(day_values, iterations=600, seed=20260710)

    assert first == second
    assert first["iterations"] == 600
    assert first["day_count"] == 100
    assert first["engine"] == "numpy_vectorized_v1"
    assert 0.0 <= first["prob_gt_0"] <= 1.0


def test_bootstrap_days_preserves_no_downside_behavior() -> None:
    result = _bootstrap_days([0.01, 0.02, 0.03], iterations=100, seed=7)

    assert result["iterations"] == 0
    assert result["day_count"] == 3
    assert result["engine"] == "numpy_vectorized_v1"
