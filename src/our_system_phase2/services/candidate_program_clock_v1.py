"""Coordinate-level joint PIT clock resolution for candidate programs."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

import pandas as pd

from our_system_phase2.services.candidate_program_v1 import JointClockContractV1


def resolve_joint_clock_coordinates_v1(
    contract: JointClockContractV1,
    component_rows: Mapping[str, Sequence[Mapping[str, Any]]],
) -> list[dict[str, Any]]:
    """Intersect required component support and take row-wise latest maturity."""

    required = tuple(contract.component_clock_node_ids)
    unknown = sorted(set(component_rows) - set(required))
    if unknown:
        raise ValueError(f"joint clock received undeclared components: {unknown}")
    indexed: dict[str, dict[str, dict[str, Any]]] = {}
    all_coordinates: set[str] = set()
    for node_id in required:
        rows = component_rows.get(node_id) or ()
        by_coordinate: dict[str, dict[str, Any]] = {}
        for source in rows:
            row = dict(source)
            coordinate_id = str(row.get("coordinate_id") or "")
            if not coordinate_id or coordinate_id in by_coordinate:
                raise ValueError("joint clock coordinates must be unique and non-empty")
            by_coordinate[coordinate_id] = row
            all_coordinates.add(coordinate_id)
        indexed[node_id] = by_coordinate

    output = []
    for coordinate_id in sorted(all_coordinates):
        missing = [
            node_id for node_id in required if coordinate_id not in indexed[node_id]
        ]
        rows = [indexed[node_id][coordinate_id] for node_id in required if node_id not in missing]
        eligible = not missing and all(
            bool(row.get("eligible")) and bool(row.get("support_present"))
            for row in rows
        )
        observable_values = [pd.Timestamp(row["observable_at"]) for row in rows]
        maturity_values = [pd.Timestamp(row["mature_at"]) for row in rows]
        action_sessions = [str(row.get("action_session") or "") for row in rows]
        if rows and any(not value for value in action_sessions):
            raise ValueError("joint clock component lacks action_session")
        joint_observable = max(observable_values) if observable_values else None
        joint_maturity = max(maturity_values) if maturity_values else None
        action_session = max(action_sessions) if action_sessions else None
        if joint_maturity is not None and action_session is not None:
            action_timestamp = pd.Timestamp(action_session)
            eligible = eligible and joint_maturity <= action_timestamp
        output.append(
            {
                "coordinate_id": coordinate_id,
                "eligible": bool(eligible),
                "joint_observable_at": (
                    joint_observable.isoformat() if joint_observable is not None else None
                ),
                "joint_eligible_from": (
                    joint_maturity.isoformat() if joint_maturity is not None else None
                ),
                "action_session": action_session,
                "missing_component_node_ids": missing,
                "component_count": len(rows),
                "component_clocks": [
                    {
                        "node_id": node_id,
                        "observable_at": indexed[node_id][coordinate_id].get("observable_at"),
                        "mature_at": indexed[node_id][coordinate_id].get("mature_at"),
                        "source_lag": indexed[node_id][coordinate_id].get("source_lag"),
                        "revision_policy": indexed[node_id][coordinate_id].get("revision_policy"),
                        "support_present": bool(indexed[node_id][coordinate_id].get("support_present")),
                        "eligible": bool(indexed[node_id][coordinate_id].get("eligible")),
                    }
                    for node_id in required
                    if coordinate_id in indexed[node_id]
                ],
                "joint_eligible_from_policy": contract.joint_eligible_from_policy,
                "joint_support_policy": contract.joint_support_policy,
                "pit_guard_version": contract.pit_guard_version,
            }
        )
    return output
