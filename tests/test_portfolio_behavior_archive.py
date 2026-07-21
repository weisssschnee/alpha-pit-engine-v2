from __future__ import annotations

import numpy as np

from our_system_phase2.services.portfolio_behavior_archive import (
    PortfolioBehaviorArchive,
    StreamingLabelFreeBehavior,
    pair_behavior_record,
)


def _behavior(signals: np.ndarray) -> list[dict[str, object]]:
    accumulator = StreamingLabelFreeBehavior(
        candidate_ids=("primary", "control"),
        code_count=4,
        coordinate_binding="probe-contract-a",
        scope="probe",
        min_obs=2,
        top_quantile=0.5,
    )
    accumulator.update_block(
        signals=signals,
        time_ids=np.array([0, 0, 0, 0, 1, 1, 1, 1], dtype=np.int64),
        code_ids=np.array([0, 1, 2, 3, 0, 1, 2, 3], dtype=np.int32),
        trade_times_ns=np.array([1, 1, 1, 1, 2, 2, 2, 2], dtype=np.int64),
        directions=np.array([1.0, 1.0]),
    )
    return accumulator.rows()


def test_behavior_probe_is_label_free_and_exact() -> None:
    signals = np.array(
        [
            [1.0, 2.0, 3.0, 4.0, 4.0, 3.0, 2.0, 1.0],
            [4.0, 3.0, 2.0, 1.0, 1.0, 2.0, 3.0, 4.0],
        ],
        dtype=np.float64,
    )
    first = _behavior(signals)
    second = _behavior(signals.copy())

    assert first == second
    assert all(row["behavior_status"] == "RESOLVED" for row in first)
    assert all(str(row["behavior_probe_id"]) for row in first)
    forbidden = {"return", "gross", "net", "reward", "rank_ic", "cost_outcome"}
    assert not forbidden.intersection(first[0])


def test_behavior_identity_changes_with_selection_not_outcome() -> None:
    baseline = _behavior(
        np.array(
            [
                [1, 2, 3, 4, 1, 2, 3, 4],
                [4, 3, 2, 1, 4, 3, 2, 1],
            ],
            dtype=np.float64,
        )
    )
    changed = _behavior(
        np.array(
            [
                [4, 3, 2, 1, 4, 3, 2, 1],
                [4, 3, 2, 1, 4, 3, 2, 1],
            ],
            dtype=np.float64,
        )
    )
    assert baseline[0]["behavior_probe_id"] != changed[0]["behavior_probe_id"]


def test_unresolved_behavior_never_falls_back_to_structure() -> None:
    rows = _behavior(np.full((2, 8), np.nan, dtype=np.float64))
    record = pair_behavior_record(
        batch_id="batch_000",
        pair_id="pair-a",
        route_id="MINUTE_STATIC",
        primary_candidate_id="primary",
        control_candidate_id="control",
        structural_family_id="structural-a",
        primary_behavior=rows[0],
        control_behavior=rows[1],
    )

    assert record["behavior_status"] == "BEHAVIOR_UNRESOLVED"
    assert record["portfolio_behavior_signature_id"] == ""
    assert record["portfolio_behavior_family_id"] == ""


def test_archive_dedupes_probe_and_full_signature_separately(tmp_path) -> None:
    rows = _behavior(
        np.array(
            [
                [1, 2, 3, 4, 1, 2, 3, 4],
                [4, 3, 2, 1, 4, 3, 2, 1],
            ],
            dtype=np.float64,
        )
    )
    record = pair_behavior_record(
        batch_id="batch_000",
        pair_id="pair-a",
        route_id="MINUTE_STATIC",
        primary_candidate_id="primary",
        control_candidate_id="control",
        structural_family_id="structural-a",
        primary_behavior=rows[0],
        control_behavior=rows[1],
    )
    archive = PortfolioBehaviorArchive()
    archive.add(record)
    assert archive.contains_probe(str(record["behavior_probe_id"]))

    path = tmp_path / "behavior_archive.parquet"
    archive.write_parquet(path)
    restored = PortfolioBehaviorArchive.read_parquet(path)
    assert restored.rows == archive.rows

