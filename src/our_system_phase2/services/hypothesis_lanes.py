"""Isolated NEXTGEN-DARK hypothesis lanes and candidate submission contract."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping


LANE_REGISTRY_VERSION = "nextgen_dark_hypothesis_lanes_v1"
FORBIDDEN_SUBMISSION_TOKENS = (
    "reward", "validation", "holdout", "forward", "oos", "label", "winner",
    "sortino", "sharpe", "performance", "return",
)
FORBIDDEN_EXPRESSION_TOKENS = (
    "reward", "validation", "holdout", "forward", "oos", "label", "winner",
    "sortino", "sharpe", "performance",
)


@dataclass(frozen=True, slots=True)
class HypothesisLaneSpec:
    lane_id: str
    root_distribution: tuple[tuple[str, float], ...]
    proposal_quota: int
    admission_quota: int
    archive_namespace: str
    lineage_namespace: str
    seed: int
    candidate_contract: str
    no_memory: bool = False
    exile: bool = False
    policy_frozen: bool = True
    data_access_role: str = "development"
    performance_search_allowed: bool = False

    def validate(self) -> None:
        if not self.lane_id or self.proposal_quota <= 0 or self.admission_quota < 0:
            raise ValueError(f"invalid lane quota: {self.lane_id}")
        if self.admission_quota > self.proposal_quota:
            raise ValueError(f"admission exceeds proposal quota: {self.lane_id}")
        if not self.archive_namespace or not self.lineage_namespace or not self.candidate_contract:
            raise ValueError(f"lane namespaces/contract missing: {self.lane_id}")
        if self.data_access_role != "development" or self.performance_search_allowed:
            raise ValueError(f"NEXTGEN-DARK lane must remain development-only and no-performance: {self.lane_id}")
        total = sum(weight for _, weight in self.root_distribution)
        if abs(total - 1.0) > 1e-9 or any(weight <= 0 for _, weight in self.root_distribution):
            raise ValueError(f"root distribution must be positive and sum to one: {self.lane_id}")

    def canonical(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["root_distribution"] = {key: value for key, value in self.root_distribution}
        return payload


class HypothesisLaneRegistry:
    def __init__(self, specs: Iterable[HypothesisLaneSpec]) -> None:
        self._specs: dict[str, HypothesisLaneSpec] = {}
        archives: set[str] = set()
        lineages: set[str] = set()
        for spec in specs:
            spec.validate()
            if spec.lane_id in self._specs:
                raise ValueError(f"duplicate lane: {spec.lane_id}")
            if spec.archive_namespace in archives or spec.lineage_namespace in lineages:
                raise ValueError(f"lane namespace collision: {spec.lane_id}")
            self._specs[spec.lane_id] = spec
            archives.add(spec.archive_namespace)
            lineages.add(spec.lineage_namespace)
        if not self._specs:
            raise ValueError("lane registry cannot be empty")

    @property
    def specs(self) -> tuple[HypothesisLaneSpec, ...]:
        return tuple(self._specs[key] for key in sorted(self._specs))

    def get(self, lane_id: str) -> HypothesisLaneSpec:
        try:
            return self._specs[lane_id]
        except KeyError as exc:
            raise KeyError(f"unknown hypothesis lane: {lane_id}") from exc

    def contract(self) -> dict[str, Any]:
        payload = {
            "registry_version": LANE_REGISTRY_VERSION,
            "lane_count": len(self._specs),
            "lanes": [spec.canonical() for spec in self.specs],
            "formal_performance_search_allowed": False,
            "adaptive_policy_update_allowed": False,
        }
        payload["registry_hash"] = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return payload

    def validate_submission(self, row: Mapping[str, Any]) -> dict[str, Any]:
        required = {
            "candidate_id", "lane_id", "exact_identity", "semantic_key",
            "lineage_key", "expression", "data_role", "root_cell",
        }
        missing = sorted(required - set(row))
        if missing:
            raise ValueError(f"candidate submission missing fields: {missing}")
        lane = self.get(str(row["lane_id"]))
        blank = sorted(key for key in required if not str(row[key]).strip())
        if blank:
            raise ValueError(f"candidate submission has blank required fields: {blank}")
        if str(row["data_role"]) != "development":
            raise ValueError("candidate submission must be development-only")
        forbidden = sorted(
            key
            for key in row
            if any(token in str(key).lower() for token in FORBIDDEN_SUBMISSION_TOKENS)
        )
        if forbidden:
            raise ValueError(f"performance/evaluation fields forbidden in submission: {forbidden}")
        expression = str(row["expression"]).lower()
        expression_forbidden = sorted(
            token for token in FORBIDDEN_EXPRESSION_TOKENS if token in expression
        )
        if expression_forbidden or "2026" in expression:
            raise ValueError(
                "candidate expression references forbidden evaluation/forward fields: "
                f"{expression_forbidden or ['2026']}"
            )
        root_cell = str(row["root_cell"])
        allowed_root_cells = {cell for cell, _ in lane.root_distribution}
        if root_cell not in allowed_root_cells:
            raise ValueError(
                f"candidate root cell {root_cell!r} is outside lane distribution {sorted(allowed_root_cells)}"
            )
        lineage = str(row["lineage_key"])
        if not lineage.startswith(f"{lane.lineage_namespace}:"):
            raise ValueError(f"lineage key must use lane namespace {lane.lineage_namespace}")
        output = dict(row)
        output["archive_namespace"] = lane.archive_namespace
        output["candidate_contract"] = lane.candidate_contract
        output["lane_seed"] = lane.seed
        output["lane_proposal_quota"] = lane.proposal_quota
        output["lane_admission_quota"] = lane.admission_quota
        output["policy_frozen"] = lane.policy_frozen
        output["no_memory"] = lane.no_memory
        output["exile"] = lane.exile
        return output


def default_nextgen_lane_registry() -> HypothesisLaneRegistry:
    contract = "nextgen_dark_candidate_submission_v1"
    rows = [
        ("static_cross_sectional", (("raw", 0.6), ("context", 0.4)), 256, 32, False, False),
        ("temporal_program", (("single_scale", 0.5), ("multi_scale", 0.5)), 256, 32, False, False),
        ("event_conditioned", (("limit", 0.5), ("firstN", 0.25), ("context", 0.25)), 192, 24, False, False),
        ("state_transition", (("duration", 0.4), ("transition", 0.4), ("confirmation", 0.2)), 192, 24, False, False),
        ("orthogonal_exile", (("orthogonal", 0.5), ("exile", 0.5)), 128, 16, True, True),
        ("competitor_reproduction", (("strategy", 0.5), ("external", 0.5)), 128, 16, True, False),
        ("mcts_evolutionary_challenger", (("mcts", 0.5), ("evolutionary", 0.5)), 192, 24, True, False),
    ]
    return HypothesisLaneRegistry(
        HypothesisLaneSpec(
            lane_id=lane_id,
            root_distribution=distribution,
            proposal_quota=proposal,
            admission_quota=admission,
            archive_namespace=f"nextgen/{lane_id}",
            lineage_namespace=f"ngd_{lane_id}",
            seed=9100 + index,
            candidate_contract=contract,
            no_memory=no_memory,
            exile=exile,
        )
        for index, (lane_id, distribution, proposal, admission, no_memory, exile) in enumerate(rows)
    )
