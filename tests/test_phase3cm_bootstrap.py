from __future__ import annotations

import numpy as np

from our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit import (
    DAY_UNCERTAINTY_CONTRACT,
    DAY_UNCERTAINTY_ENGINE,
    DAY_UNCERTAINTY_METHOD,
    _bootstrap_days,
    _day_uncertainty_fields,
    _stationary_bootstrap_block_length,
    _stationary_bootstrap_indices,
)
from our_system_phase2.services.time_series_uncertainty import (
    PAIRED_DELTA_UNCERTAINTY_CONTRACT,
    bootstrap_paired_delta_days,
)


def test_bootstrap_days_is_deterministic_and_reports_stationary_engine() -> None:
    day_values = [0.01, -0.02, 0.03, -0.01, 0.015] * 20

    first = _bootstrap_days(day_values, iterations=600, seed=20260710)
    second = _bootstrap_days(day_values, iterations=600, seed=20260710)

    assert first == second
    assert first["iterations"] == 600
    assert first["requested_iterations"] == 600
    assert first["valid_iterations"] == 600
    assert first["invalid_iterations"] == 0
    assert first["valid_fraction"] == 1.0
    assert first["day_count"] == 100
    assert first["engine"] == DAY_UNCERTAINTY_ENGINE
    assert first["method"] == DAY_UNCERTAINTY_METHOD
    assert first["contract"] == DAY_UNCERTAINTY_CONTRACT
    assert first["resampling_unit"] == "ordered_trade_day"
    assert first["expected_block_length"] == 5
    assert first["block_length_rule"] == "round_cuberoot_day_count_clamped_2_20"
    assert first["sufficient_day_count"] is True
    assert 0.0 <= first["prob_gt_0"] <= 1.0
    assert 0.0 <= first["prob_gt_0_mcse"] <= 0.5
    assert first["iid_reference_valid_iterations"] == 600
    assert first["dependence_p25_delta"] is not None


def test_stationary_bootstrap_indices_preserve_order_inside_blocks() -> None:
    indices = _stationary_bootstrap_indices(
        rng=np.random.default_rng(11),
        iterations=64,
        day_count=100,
        expected_block_length=10,
    )

    continued = indices[:, 1:] == ((indices[:, :-1] + 1) % 100)
    assert indices.shape == (64, 100)
    assert float(np.mean(continued)) > 0.80


def test_stationary_bootstrap_block_length_is_frozen_and_bounded() -> None:
    assert _stationary_bootstrap_block_length(0) == 0
    assert _stationary_bootstrap_block_length(1) == 1
    assert _stationary_bootstrap_block_length(8) == 2
    assert _stationary_bootstrap_block_length(100) == 5
    assert _stationary_bootstrap_block_length(100_000) == 20


def test_stationary_bootstrap_exposes_dependence_against_iid_reference() -> None:
    clustered_returns = ([0.012] * 8 + [-0.018] * 8) * 10
    result = _bootstrap_days(clustered_returns, iterations=1_000, seed=17)

    assert result["dependence_p25_delta"] < 0.0
    assert result["prob_gt_0"] > result["iid_reference_prob_gt_0"]


def test_uncertainty_fields_keep_legacy_mcmc_names_as_exact_aliases() -> None:
    bootstrap = _bootstrap_days(
        [0.01, -0.02, 0.03, -0.01, 0.015] * 20,
        iterations=128,
        seed=17,
    )
    fields = _day_uncertainty_fields(bootstrap)

    assert fields["day_uncertainty_sortino_p25"] == fields["day_mcmc_sortino_p25"]
    assert (
        fields["day_uncertainty_support_sortino_gt_0"]
        == fields["day_mcmc_prob_sortino_gt_0"]
    )
    assert fields["day_uncertainty_engine"] == fields["day_mcmc_engine"]
    assert fields["day_uncertainty_valid_iterations"] == fields["day_mcmc_iterations"]


def test_bootstrap_days_preserves_no_downside_behavior() -> None:
    result = _bootstrap_days([0.01, 0.02, 0.03], iterations=100, seed=7)

    assert result["iterations"] == 0
    assert result["requested_iterations"] == 100
    assert result["valid_iterations"] == 0
    assert result["invalid_iterations"] == 100
    assert result["valid_fraction"] == 0.0
    assert result["day_count"] == 3
    assert result["engine"] == DAY_UNCERTAINTY_ENGINE
    assert result["sufficient_day_count"] is False


def test_paired_delta_bootstrap_uses_one_shared_stationary_path() -> None:
    deltas = [0.01, -0.002, 0.006, 0.004] * 25

    first = bootstrap_paired_delta_days(
        deltas,
        iterations=600,
        seed=41,
    )
    second = bootstrap_paired_delta_days(
        deltas,
        iterations=600,
        seed=41,
    )

    assert first == second
    assert first["contract"] == PAIRED_DELTA_UNCERTAINTY_CONTRACT
    assert first["shared_block_indices"] is True
    assert first["resampling_unit"] == "ordered_trade_day_paired_delta"
    assert first["valid_iterations"] == 600
    assert first["invalid_iterations"] == 0
    assert first["observed_mean_delta"] > 0.0
    assert first["support_mean_delta_gt_0"] > 0.60


def test_paired_delta_bootstrap_handles_all_positive_days() -> None:
    result = bootstrap_paired_delta_days(
        [0.01] * 20,
        iterations=128,
        seed=7,
    )

    assert result["valid_iterations"] == 128
    assert result["support_mean_delta_gt_0"] == 1.0
    assert result["mean_delta_p25"] == 0.01
