"""Deterministic, non-performance admission and diversity controls."""

from __future__ import annotations

import hashlib
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from our_system_phase2.services.hypothesis_lanes import HypothesisLaneRegistry


@dataclass(frozen=True, slots=True)
class AdmissionConfig:
    total_quota: int
    family_cap: int
    bucket_cap: int
    parent_descendant_cap: int
    fresh_budget_floor: int
    exile_quota: int
    seed: int = 20260711

    def validate(self) -> None:
        values = (
            self.total_quota, self.family_cap, self.bucket_cap,
            self.parent_descendant_cap, self.fresh_budget_floor, self.exile_quota,
        )
        if self.total_quota <= 0 or any(value < 0 for value in values[1:]):
            raise ValueError("invalid admission limits")
        if self.fresh_budget_floor + self.exile_quota > self.total_quota:
            raise ValueError("fresh plus exile quota exceeds total")


def _stable_priority(row: Mapping[str, Any], seed: int) -> str:
    return hashlib.sha256(f"{seed}|{row.get('candidate_id')}|{row.get('exact_identity')}".encode()).hexdigest()


def _dedupe_exact(rows: Iterable[dict[str, Any]], seed: int) -> list[dict[str, Any]]:
    owners: dict[str, dict[str, Any]] = {}
    for row in rows:
        identity = str(row["exact_identity"])
        current = owners.get(identity)
        if current is None or _stable_priority(row, seed) < _stable_priority(current, seed):
            owners[identity] = row
    return sorted(owners.values(), key=lambda row: _stable_priority(row, seed))


def _semantic_volume(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    counts = Counter(
        "|".join(
            str(row.get(key) or "unknown")
            for key in ("field_family", "primitive_family", "temporal_cell", "event_state_family")
        )
        for row in rows
    )
    values = list(counts.values())
    return {
        "cell_count": len(counts),
        "candidate_count": sum(values),
        "max_cell_count": max(values, default=0),
        "min_cell_count": min(values, default=0),
        "per_cell": dict(sorted(counts.items())),
    }


def admit_candidates(
    candidates: Iterable[Mapping[str, Any]],
    lanes: HypothesisLaneRegistry,
    config: AdmissionConfig,
) -> dict[str, Any]:
    config.validate()
    validated = [lanes.validate_submission(row) for row in candidates]
    candidate_id_counts = Counter(str(row["candidate_id"]) for row in validated)
    duplicate_candidate_ids = sorted(key for key, count in candidate_id_counts.items() if count > 1)
    if duplicate_candidate_ids:
        raise ValueError(f"duplicate candidate_id values: {duplicate_candidate_ids}")
    proposal_count = Counter(str(row["lane_id"]) for row in validated)
    proposal_overflow = {
        lane_id: {"observed": count, "quota": lanes.get(lane_id).proposal_quota}
        for lane_id, count in proposal_count.items()
        if count > lanes.get(lane_id).proposal_quota
    }
    if proposal_overflow:
        raise ValueError(f"lane proposal quota exceeded: {proposal_overflow}")
    deduped = _dedupe_exact(validated, config.seed)
    global_topk_baseline = deduped[: config.total_quota]

    fresh = [row for row in deduped if bool(row.get("fresh")) or not str(row.get("parent_id") or "")]
    exile = [row for row in deduped if lanes.get(str(row["lane_id"])).exile]
    regular = [row for row in deduped if row not in exile]
    buckets: dict[tuple[str, str, str], deque[dict[str, Any]]] = defaultdict(deque)
    for row in regular:
        key = (str(row["lane_id"]), str(row.get("family_id") or "unknown"), str(row.get("semantic_bucket") or "unknown"))
        buckets[key].append(row)

    selected: list[dict[str, Any]] = []
    selected_ids: set[str] = set()
    family_count: Counter[str] = Counter()
    bucket_count: Counter[str] = Counter()
    parent_count: Counter[str] = Counter()
    lane_count: Counter[str] = Counter()

    def try_add(row: dict[str, Any]) -> bool:
        candidate_id = str(row["candidate_id"])
        family = str(row.get("family_id") or "unknown")
        bucket = str(row.get("semantic_bucket") or "unknown")
        parent = str(row.get("parent_id") or "")
        lane_id = str(row["lane_id"])
        lane = lanes.get(lane_id)
        if candidate_id in selected_ids or len(selected) >= config.total_quota:
            return False
        if family_count[family] >= config.family_cap or bucket_count[bucket] >= config.bucket_cap:
            return False
        if parent and parent_count[parent] >= config.parent_descendant_cap:
            return False
        if lane_count[lane_id] >= lane.admission_quota:
            return False
        selected.append(row)
        selected_ids.add(candidate_id)
        family_count[family] += 1
        bucket_count[bucket] += 1
        parent_count[parent] += int(bool(parent))
        lane_count[lane_id] += 1
        return True

    for row in fresh:
        if sum(bool(item.get("fresh")) or not str(item.get("parent_id") or "") for item in selected) >= config.fresh_budget_floor:
            break
        try_add(row)
    for row in exile:
        if sum(lanes.get(str(item["lane_id"])).exile for item in selected) >= config.exile_quota:
            break
        try_add(row)

    active = deque(sorted(buckets))
    while active and len(selected) < config.total_quota:
        key = active.popleft()
        queue = buckets[key]
        added = False
        while queue and not added:
            added = try_add(queue.popleft())
        if queue:
            active.append(key)

    fresh_selected_count = sum(
        bool(row.get("fresh")) or not str(row.get("parent_id") or "") for row in selected
    )
    exile_selected_count = sum(lanes.get(str(row["lane_id"])).exile for row in selected)
    if fresh_selected_count < config.fresh_budget_floor:
        raise ValueError(
            "fresh budget floor was not met: "
            f"selected={fresh_selected_count}, floor={config.fresh_budget_floor}"
        )
    return {
        "selected": selected,
        "global_topk_baseline": global_topk_baseline,
        "input_count": len(validated),
        "exact_identity_count": len(deduped),
        "duplicate_vote_count": len(validated) - len(deduped),
        "selected_count": len(selected),
        "fresh_selected_count": fresh_selected_count,
        "fresh_floor_satisfied": fresh_selected_count >= config.fresh_budget_floor,
        "fresh_floor_shortfall": max(0, config.fresh_budget_floor - fresh_selected_count),
        "exile_selected_count": exile_selected_count,
        "exile_quota_satisfied": exile_selected_count >= config.exile_quota,
        "exile_quota_shortfall": max(0, config.exile_quota - exile_selected_count),
        "per_lane_proposal_distribution": dict(sorted(proposal_count.items())),
        "per_lane_proposal_quota": {
            spec.lane_id: spec.proposal_quota for spec in lanes.specs
        },
        "per_lane_admission_distribution": dict(sorted(lane_count.items())),
        "semantic_volume": _semantic_volume(deduped),
        "performance_used": False,
        "deterministic_seed": config.seed,
    }
