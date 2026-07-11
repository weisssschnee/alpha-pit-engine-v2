from __future__ import annotations

import numpy as np

from our_system_phase2.services.deterministic_signal_sketch import (
    build_signal_sketch,
    cluster_distribution,
    cluster_sketches,
    decode_bits,
    decode_i8,
    projection_matrix,
    sketch_similarity,
)


def _coordinates(size: int) -> list[dict[str, str]]:
    return [
        {
            "coordinate_id": f"c{index}",
            "trade_month": f"2024-{index % 2 + 1:02d}",
            "intraday_period": "open" if index % 2 == 0 else "close",
            "stock_coverage_interval": str(index % 4),
            "listing_age_bucket": "mature",
            "activation_density_bucket": "mid",
        }
        for index in range(size)
    ]


def _sketch(candidate_id: str, values: np.ndarray) -> dict[str, object]:
    coordinates = _coordinates(len(values))
    valid = np.isfinite(values)
    ranks = np.full(len(values), np.nan)
    ranks[valid] = np.argsort(np.argsort(values[valid])).astype(float) / max(1, int(valid.sum()) - 1)
    return {
        "candidate_id": candidate_id,
        **build_signal_sketch(
            values,
            ranks,
            coordinates,
            coordinate_set="A",
            projection=projection_matrix([row["coordinate_id"] for row in coordinates], seed=7),
        ),
    }


def test_sketch_roundtrip_and_missingness() -> None:
    sketch = _sketch("a", np.asarray([1.0, np.nan, -2.0, 0.0]))

    assert len(decode_i8(str(sketch["rank_quantized_sketch"]))) == 4
    missing = decode_bits(str(sketch["missingness_pattern"]), 4)
    assert missing.tolist() == [False, True, False, False]
    assert sketch["finite_ratio"] == 0.75


def test_equivalent_scale_signals_cluster_without_labels() -> None:
    base = np.linspace(-2.0, 2.0, 64)
    rows = [_sketch("a", base), _sketch("b", base * 10.0), _sketch("c", base[::-1].copy())]

    similarity = sketch_similarity(rows[0], rows[1])
    labels = cluster_sketches(rows)
    metrics = cluster_distribution(labels.values())

    assert similarity["rank_corr"] > 0.99
    assert labels["a"] == labels["b"]
    assert metrics["cluster_count"] >= 1


def test_projection_is_deterministic() -> None:
    ids = ["x", "y", "z"]
    assert np.array_equal(projection_matrix(ids, seed=1), projection_matrix(ids, seed=1))
    assert not np.array_equal(projection_matrix(ids, seed=1), projection_matrix(ids, seed=2))


def test_opposite_direction_signals_share_equivalence_cluster() -> None:
    base = np.linspace(-3.0, 3.0, 128)
    rows = [_sketch("long", base), _sketch("short", -base)]

    labels = cluster_sketches(rows)

    assert labels["long"] == labels["short"]
