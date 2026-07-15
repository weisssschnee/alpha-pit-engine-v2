from __future__ import annotations

from our_system_phase2.services.compositional_behavior_admission import (
    select_diversity_admission,
)


def _rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for route in ("A", "B", "C"):
        for index in range(7 if route != "C" else 2):
            rows.append(
                {
                    "candidate_id": f"{route}{index}",
                    "exact_identity": f"exact-{route}-{index}",
                    "route_id": route,
                    "skeleton_id": f"s{index % 2}",
                    "source_family_signature": f"f{index % 3}",
                    "operator_path_hash": f"o{index % 2}",
                    "expression_depth": 2 + index % 2,
                    "behavior_cluster_id": 1 + index % 3,
                    "seed": index % 2,
                    "reward": 1_000_000 - index,
                }
            )
    return rows


def test_diversity_admission_is_deterministic_balanced_and_reward_dark() -> None:
    forward = select_diversity_admission(_rows(), maximum_pairs=12)
    reverse = select_diversity_admission(list(reversed(_rows())), maximum_pairs=12)

    assert [row["candidate_id"] for row in forward] == [
        row["candidate_id"] for row in reverse
    ]
    counts = {route: sum(row["route_id"] == route for row in forward) for route in "ABC"}
    assert counts == {"A": 5, "B": 5, "C": 2}
    assert all(row["performance_accessed_for_admission"] is False for row in forward)
    assert len({row["exact_identity"] for row in forward}) == len(forward)


def test_coverage_limited_rows_cannot_enter_admission() -> None:
    rows = _rows()
    rows[0]["behavior_cluster_id"] = 0

    admitted = select_diversity_admission(rows, maximum_pairs=len(rows))

    assert rows[0]["candidate_id"] not in {row["candidate_id"] for row in admitted}
