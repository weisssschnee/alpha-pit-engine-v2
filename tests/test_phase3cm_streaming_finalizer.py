from __future__ import annotations

from scripts.finalize_cn_phase3cm_streaming_repair import _linear_fit, _resource_projection


def _backend(pair_count: int, wall: float, cpu: float) -> dict[str, object]:
    return {
        "pair_count": pair_count,
        "wall_seconds": wall,
        "cpu_seconds": cpu,
    }


def test_resource_projection_separates_startup_marginal_cpu_and_io_bounds() -> None:
    scale = [
        {"pair_count": 1, "wall_seconds": 360.0},
        {"pair_count": 4, "wall_seconds": 870.0},
        {"pair_count": 8, "wall_seconds": 1_550.0},
        {"pair_count": 16, "wall_seconds": 2_910.0},
        {"pair_count": 32, "wall_seconds": 3_500.0},
    ]
    phase_d = {
        "active_bar": _backend(18, 3_300.0, 8_000.0),
        "stock_session": _backend(14, 20.0, 45.0),
        "global_peak_rss_bytes": 12 * 1024**3,
    }
    phase_e = {"global_peak_rss_bytes": 13 * 1024**3}

    projection = _resource_projection(
        scale,
        phase_d,
        phase_e,
        sidecar_build_seconds=120.0,
        sidecar_bytes=20 * 1024**3,
        retained_checkpoint_bytes_per_candidate=1_000_000.0,
        device_bandwidth=9_000_000_000.0,
    )

    model = projection["model"]
    target = projection["targets"]["4096"]
    assert model["active_startup_intercept_seconds"] > 0.0
    assert model["active_measured_marginal_seconds_per_pair"] > 0.0
    assert target["cpu_lower_bound_wall_seconds"] > target["io_lower_bound_wall_seconds"]
    assert target["sidecar_build_inclusive_wall_seconds"] == (
        target["sidecar_ready_wall_seconds"] + 120.0
    )
    assert target["two_worker_partition"]["duplicate_active_sidecar_scan"] is True
    assert target["projected_retained_checkpoint_disk_bytes"] == 4_096_000_000


def test_linear_fit_is_deterministic() -> None:
    points = ((1, 10.0), (4, 25.0), (8, 45.0), (16, 85.0))
    assert _linear_fit(points) == _linear_fit(tuple(reversed(points)))
