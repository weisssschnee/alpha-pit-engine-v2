"""Frozen time-series resampling primitives shared by reward evaluators.

The estimator is a non-parametric stationary bootstrap.  Historical Phase3CM
artifacts retain ``day_mcmc_*`` compatibility columns, but no Markov chain or
Bayesian posterior is constructed here.
"""

from __future__ import annotations

import math
from typing import Any, Sequence

import numpy as np


DAY_UNCERTAINTY_ENGINE = "stationary_block_bootstrap_v2"
DAY_UNCERTAINTY_METHOD = "politis_romano_stationary_bootstrap"
DAY_UNCERTAINTY_CONTRACT = (
    "phase3cm_ordered_day_stationary_block_uncertainty_v2"
)
DAY_UNCERTAINTY_REFERENCE = "doi:10.1080/01621459.1994.10476870"
DAY_UNCERTAINTY_BLOCK_LENGTH_RULE = (
    "round_cuberoot_day_count_clamped_2_20"
)
DAY_UNCERTAINTY_MIN_DAYS = 20
DAY_UNCERTAINTY_MIN_VALID_FRACTION = 0.90

PAIRED_DELTA_UNCERTAINTY_CONTRACT = (
    "phase3cm_paired_daily_net_delta_stationary_bootstrap_v1"
)
PAIRED_DELTA_UNCERTAINTY_ITERATIONS = 600
PAIRED_DELTA_UNCERTAINTY_MIN_SUPPORT = 0.60
PAIRED_DELTA_AGGREGATION_CONTRACT = (
    "shared_coordinate_equal_horizon_mean_then_ordered_day_sum_v1"
)
def _round(value: Any, ndigits: int = 8) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return round(number, ndigits)


def stationary_bootstrap_block_length(day_count: int) -> int:
    """Return the frozen expected block length for ordered daily observations."""

    day_count = max(0, int(day_count))
    if day_count <= 1:
        return day_count
    return min(
        day_count,
        20,
        max(2, int(round(day_count ** (1.0 / 3.0)))),
    )


def stationary_bootstrap_indices(
    *,
    rng: np.random.Generator,
    iterations: int,
    day_count: int,
    expected_block_length: int,
) -> np.ndarray:
    """Generate circular stationary-bootstrap indices."""

    if iterations <= 0 or day_count <= 0:
        return np.empty((0, max(0, day_count)), dtype=np.int64)
    restart_probability = 1.0 / max(1, int(expected_block_length))
    indices = np.empty(
        (int(iterations), int(day_count)),
        dtype=np.int64,
    )
    indices[:, 0] = rng.integers(
        0,
        day_count,
        size=iterations,
        dtype=np.int64,
    )
    if day_count == 1:
        return indices
    restarts = (
        rng.random((iterations, day_count - 1)) < restart_probability
    )
    fresh_starts = rng.integers(
        0,
        day_count,
        size=(iterations, day_count - 1),
        dtype=np.int64,
    )
    for offset in range(1, day_count):
        continuation = (indices[:, offset - 1] + 1) % day_count
        indices[:, offset] = np.where(
            restarts[:, offset - 1],
            fresh_starts[:, offset - 1],
            continuation,
        )
    return indices


def _sortino_draws(samples: np.ndarray) -> tuple[np.ndarray, int]:
    if samples.ndim != 2:
        raise ValueError(
            "bootstrap samples must be a two-dimensional matrix"
        )
    means = np.mean(samples, axis=1)
    downside = np.minimum(samples, 0.0)
    downside_scale = np.sqrt(np.mean(downside * downside, axis=1))
    valid = (
        np.isfinite(means)
        & np.isfinite(downside_scale)
        & (downside_scale > 1e-18)
    )
    return means[valid] / downside_scale[valid], int(np.sum(~valid))


def bootstrap_sortino_days(
    day_values: Sequence[float],
    *,
    iterations: int,
    seed: int,
) -> dict[str, Any]:
    """Estimate daily Sortino uncertainty with a stationary bootstrap."""

    clean = np.asarray(
        [
            float(value)
            for value in day_values
            if math.isfinite(float(value))
        ],
        dtype=np.float64,
    )
    requested_iterations = max(0, int(iterations))
    day_count = int(len(clean))
    block_length = stationary_bootstrap_block_length(day_count)
    base = {
        "iterations": 0,
        "requested_iterations": requested_iterations,
        "valid_iterations": 0,
        "invalid_iterations": requested_iterations,
        "valid_fraction": (
            0.0 if requested_iterations else None
        ),
        "day_count": day_count,
        "downside_day_count": int(np.sum(clean < 0.0)),
        "downside_day_fraction": _round(
            float(np.mean(clean < 0.0)) if day_count else None
        ),
        "engine": DAY_UNCERTAINTY_ENGINE,
        "method": DAY_UNCERTAINTY_METHOD,
        "contract": DAY_UNCERTAINTY_CONTRACT,
        "reference": DAY_UNCERTAINTY_REFERENCE,
        "resampling_unit": "ordered_trade_day",
        "seed": int(seed),
        "expected_block_length": block_length,
        "restart_probability": _round(
            1.0 / block_length if block_length else None
        ),
        "block_length_rule": DAY_UNCERTAINTY_BLOCK_LENGTH_RULE,
        "minimum_day_count": DAY_UNCERTAINTY_MIN_DAYS,
        "sufficient_day_count": bool(
            day_count >= DAY_UNCERTAINTY_MIN_DAYS
        ),
        "minimum_valid_fraction": DAY_UNCERTAINTY_MIN_VALID_FRACTION,
    }
    if day_count == 0 or requested_iterations == 0:
        return base

    rng = np.random.default_rng(int(seed))
    indices = stationary_bootstrap_indices(
        rng=rng,
        iterations=requested_iterations,
        day_count=day_count,
        expected_block_length=block_length,
    )
    draws, invalid_iterations = _sortino_draws(clean[indices])
    valid_iterations = int(len(draws))
    positives = int(np.sum(draws > 0.0))
    probability = (
        positives / valid_iterations if valid_iterations else None
    )

    iid_rng = np.random.default_rng(int(seed) + 104_729)
    iid_indices = iid_rng.integers(
        0,
        day_count,
        size=(requested_iterations, day_count),
        dtype=np.int64,
    )
    iid_draws, iid_invalid_iterations = _sortino_draws(
        clean[iid_indices]
    )
    iid_p25 = (
        float(np.quantile(iid_draws, 0.25))
        if len(iid_draws)
        else None
    )
    p25 = (
        float(np.quantile(draws, 0.25))
        if valid_iterations
        else None
    )
    return {
        **base,
        "iterations": valid_iterations,
        "valid_iterations": valid_iterations,
        "invalid_iterations": invalid_iterations,
        "valid_fraction": _round(
            valid_iterations / requested_iterations
        ),
        "p05": _round(
            float(np.quantile(draws, 0.05)) if len(draws) else None
        ),
        "p25": _round(p25),
        "median": _round(
            float(np.quantile(draws, 0.50)) if len(draws) else None
        ),
        "p75": _round(
            float(np.quantile(draws, 0.75)) if len(draws) else None
        ),
        "p95": _round(
            float(np.quantile(draws, 0.95)) if len(draws) else None
        ),
        "prob_gt_0": _round(probability),
        "prob_gt_0_mcse": _round(
            math.sqrt(
                probability
                * (1.0 - probability)
                / valid_iterations
            )
            if probability is not None and valid_iterations
            else None
        ),
        "iid_reference_valid_iterations": int(len(iid_draws)),
        "iid_reference_invalid_iterations": int(
            iid_invalid_iterations
        ),
        "iid_reference_p25": _round(iid_p25),
        "iid_reference_prob_gt_0": _round(
            float(np.mean(iid_draws > 0.0))
            if len(iid_draws)
            else None
        ),
        "dependence_p25_delta": _round(
            p25 - iid_p25
            if p25 is not None and iid_p25 is not None
            else None
        ),
    }


def bootstrap_paired_delta_days(
    day_deltas: Sequence[float],
    *,
    iterations: int = PAIRED_DELTA_UNCERTAINTY_ITERATIONS,
    seed: int,
) -> dict[str, Any]:
    """Bootstrap the mean paired daily net-return delta.

    Pairing happens before resampling, so each bootstrap path uses one shared
    block-index sequence for the primary and control observations.
    """

    clean = np.asarray(
        [
            float(value)
            for value in day_deltas
            if math.isfinite(float(value))
        ],
        dtype=np.float64,
    )
    requested_iterations = max(0, int(iterations))
    day_count = int(len(clean))
    block_length = stationary_bootstrap_block_length(day_count)
    base = {
        "engine": DAY_UNCERTAINTY_ENGINE,
        "method": DAY_UNCERTAINTY_METHOD,
        "contract": PAIRED_DELTA_UNCERTAINTY_CONTRACT,
        "reference": DAY_UNCERTAINTY_REFERENCE,
        "aggregation_contract": (
            PAIRED_DELTA_AGGREGATION_CONTRACT
        ),
        "resampling_unit": "ordered_trade_day_paired_delta",
        "pairing": "primary_net_return_minus_control_net_return",
        "shared_block_indices": True,
        "seed": int(seed),
        "day_count": day_count,
        "requested_iterations": requested_iterations,
        "valid_iterations": 0,
        "invalid_iterations": requested_iterations,
        "valid_fraction": (
            0.0 if requested_iterations else None
        ),
        "expected_block_length": block_length,
        "restart_probability": _round(
            1.0 / block_length if block_length else None
        ),
        "block_length_rule": DAY_UNCERTAINTY_BLOCK_LENGTH_RULE,
        "minimum_support": PAIRED_DELTA_UNCERTAINTY_MIN_SUPPORT,
    }
    if day_count == 0 or requested_iterations == 0:
        return base

    rng = np.random.default_rng(int(seed))
    indices = stationary_bootstrap_indices(
        rng=rng,
        iterations=requested_iterations,
        day_count=day_count,
        expected_block_length=block_length,
    )
    draws = np.mean(clean[indices], axis=1)
    valid = np.isfinite(draws)
    valid_draws = draws[valid]
    valid_iterations = int(len(valid_draws))
    invalid_iterations = int(requested_iterations - valid_iterations)
    probability = (
        float(np.mean(valid_draws > 0.0))
        if valid_iterations
        else None
    )
    return {
        **base,
        "valid_iterations": valid_iterations,
        "invalid_iterations": invalid_iterations,
        "valid_fraction": _round(
            valid_iterations / requested_iterations
        ),
        "observed_mean_delta": _round(
            float(np.mean(clean))
        ),
        "mean_delta_p05": _round(
            float(np.quantile(valid_draws, 0.05))
            if valid_iterations
            else None
        ),
        "mean_delta_p25": _round(
            float(np.quantile(valid_draws, 0.25))
            if valid_iterations
            else None
        ),
        "mean_delta_median": _round(
            float(np.quantile(valid_draws, 0.50))
            if valid_iterations
            else None
        ),
        "mean_delta_p75": _round(
            float(np.quantile(valid_draws, 0.75))
            if valid_iterations
            else None
        ),
        "mean_delta_p95": _round(
            float(np.quantile(valid_draws, 0.95))
            if valid_iterations
            else None
        ),
        "support_mean_delta_gt_0": _round(probability),
        "support_mean_delta_gt_0_mcse": _round(
            math.sqrt(
                probability
                * (1.0 - probability)
                / valid_iterations
            )
            if probability is not None and valid_iterations
            else None
        ),
    }
