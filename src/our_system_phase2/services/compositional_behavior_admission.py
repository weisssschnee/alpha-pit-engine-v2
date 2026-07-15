"""Label-free behavior registry and deterministic diversity admission helpers."""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict, deque
from typing import Any, Iterable, Mapping


ADMISSION_VERSION = "cn_compositional_behavior_admission_v1"


def stable_order_key(payload: Any) -> str:
    return hashlib.sha256(
        (ADMISSION_VERSION + "|" + json.dumps(payload, sort_keys=True, separators=(",", ":"))).encode(
            "utf-8"
        )
    ).hexdigest()


def stratified_route_order(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for raw in rows:
        row = dict(raw)
        stratum = (
            str(row["skeleton_id"]),
            str(row.get("source_family_signature") or ""),
            str(row.get("operator_path_hash") or ""),
            str(row.get("expression_depth") or ""),
            str(row["behavior_cluster_id"]),
            str(row["seed"]),
        )
        groups[stratum].append(row)
    queues: list[tuple[str, deque[dict[str, Any]]]] = []
    for stratum, members in groups.items():
        ordered = sorted(
            members,
            key=lambda row: stable_order_key(
                {"exact_identity": row["exact_identity"], "candidate_id": row["candidate_id"]}
            ),
        )
        queues.append((stable_order_key(stratum), deque(ordered)))
    queues.sort(key=lambda item: item[0])
    output: list[dict[str, Any]] = []
    while queues:
        remaining: list[tuple[str, deque[dict[str, Any]]]] = []
        for key, queue in queues:
            output.append(queue.popleft())
            if queue:
                remaining.append((key, queue))
        queues = remaining
    return output


def select_diversity_admission(
    rows: Iterable[Mapping[str, Any]], *, maximum_pairs: int
) -> list[dict[str, Any]]:
    if maximum_pairs < 0:
        raise ValueError("maximum_pairs must be non-negative")
    materialized = list(rows) if not isinstance(rows, list) else rows
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for raw in materialized:
        if int(raw.get("behavior_cluster_id") or 0) > 0:
            grouped[str(raw["route_id"])].append(raw)
    by_route = {
        route_id: deque(stratified_route_order(members))
        for route_id, members in grouped.items()
    }
    selected: list[dict[str, Any]] = []
    route_order = sorted(by_route, key=lambda value: stable_order_key({"route": value}))
    while route_order and len(selected) < maximum_pairs:
        remaining: list[str] = []
        for route_id in route_order:
            queue = by_route[route_id]
            if queue and len(selected) < maximum_pairs:
                row = queue.popleft()
                row["admission_rank"] = len(selected) + 1
                row["admission_policy"] = "DETERMINISTIC_ROUTE_AND_BEHAVIOR_STRATIFIED"
                row["performance_accessed_for_admission"] = False
                selected.append(row)
            if queue:
                remaining.append(route_id)
        route_order = remaining
    return selected
