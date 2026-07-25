from __future__ import annotations

import numpy as np

from our_system_phase2.services.portfolio_behavior_archive import (
    PortfolioBehaviorArchive,
    StreamingLabelFreeBehavior,
    _label_free_mapping_kernel,
    pair_behavior_record,
)


def _serial_label_free_mapping(
    signals: np.ndarray,
    starts: np.ndarray,
    ends: np.ndarray,
    code_ids: np.ndarray,
    directions: np.ndarray,
    min_obs: int,
    top_quantile: float,
    previous_weights: np.ndarray,
    selected_frequency: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    candidate_count, row_count = signals.shape
    time_count = len(starts)
    selected = np.zeros((candidate_count, row_count), dtype=np.bool_)
    support = np.zeros((candidate_count, time_count), dtype=np.float64)
    turnover = np.zeros((candidate_count, time_count), dtype=np.float64)
    selected_counts = np.zeros(
        (candidate_count, time_count), dtype=np.int32
    )
    for candidate in range(candidate_count):
        chosen_codes = np.zeros(
            previous_weights.shape[1], dtype=np.bool_
        )
        for time_index in range(time_count):
            start = starts[time_index]
            end = ends[time_index]
            size = end - start
            finite_rows = np.flatnonzero(
                np.isfinite(signals[candidate, start:end])
            ) + start
            finite_count = len(finite_rows)
            support[candidate, time_index] = finite_count / max(1, size)
            chosen_codes[:] = False
            if finite_count >= min_obs:
                direction = (
                    1.0 if directions[candidate] >= 0.0 else -1.0
                )
                scores = signals[candidate, finite_rows] * direction
                take = max(
                    1,
                    int(np.ceil(finite_count * top_quantile)),
                )
                threshold = np.sort(scores)[finite_count - take]
                selected_rows = finite_rows[scores > threshold]
                selected[candidate, selected_rows] = True
                chosen_codes[code_ids[selected_rows]] = True
                remaining = take - len(selected_rows)
                if remaining > 0:
                    tied_rows = finite_rows[scores == threshold]
                    tie_order = np.argsort(code_ids[tied_rows])
                    selected_rows = tied_rows[tie_order[:remaining]]
                    selected[candidate, selected_rows] = True
                    chosen_codes[code_ids[selected_rows]] = True
                selected_counts[candidate, time_index] = take
            selected_code_count = int(chosen_codes.sum())
            equal_weight = (
                1.0 / selected_code_count
                if selected_code_count
                else 0.0
            )
            absolute_change = 0.0
            for code in range(previous_weights.shape[1]):
                new_weight = (
                    equal_weight if chosen_codes[code] else 0.0
                )
                absolute_change += abs(
                    new_weight - previous_weights[candidate, code]
                )
                previous_weights[candidate, code] = new_weight
                if chosen_codes[code]:
                    selected_frequency[candidate, code] += 1
            turnover[candidate, time_index] = 0.5 * absolute_change
    return selected, support, turnover, selected_counts


def test_parallel_label_free_mapping_matches_serial_reference_exactly() -> None:
    rng = np.random.default_rng(20260726)
    candidate_count = 8
    code_count = 31
    time_count = 12
    rows_per_time = 31
    signals = rng.normal(
        size=(candidate_count, time_count * rows_per_time)
    )
    signals[rng.random(signals.shape) < 0.08] = np.nan
    starts = np.arange(
        0,
        time_count * rows_per_time,
        rows_per_time,
        dtype=np.int64,
    )
    ends = starts + rows_per_time
    code_ids = np.tile(
        np.arange(code_count, dtype=np.int32),
        time_count,
    )
    directions = np.where(
        np.arange(candidate_count) % 2 == 0,
        1.0,
        -1.0,
    )
    expected_weights = np.zeros(
        (candidate_count, code_count), dtype=np.float64
    )
    expected_frequency = np.zeros(
        (candidate_count, code_count), dtype=np.int64
    )
    expected = _serial_label_free_mapping(
        signals,
        starts,
        ends,
        code_ids,
        directions,
        2,
        0.2,
        expected_weights,
        expected_frequency,
    )
    actual_weights = np.zeros_like(expected_weights)
    actual_frequency = np.zeros_like(expected_frequency)
    actual = _label_free_mapping_kernel(
        signals,
        starts,
        ends,
        code_ids,
        directions,
        2,
        0.2,
        actual_weights,
        actual_frequency,
    )
    for actual_value, expected_value in zip(actual, expected):
        np.testing.assert_array_equal(actual_value, expected_value)
    np.testing.assert_array_equal(actual_weights, expected_weights)
    np.testing.assert_array_equal(
        actual_frequency,
        expected_frequency,
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
