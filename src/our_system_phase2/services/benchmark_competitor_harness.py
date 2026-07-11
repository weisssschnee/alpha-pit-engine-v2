"""Frozen-budget benchmark and competitor plan; execution is intentionally absent."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping

from our_system_phase2.services.hypothesis_lanes import HypothesisLaneRegistry


HARNESS_VERSION = "nextgen_dark_benchmark_competitor_harness_v1"


@dataclass(frozen=True, slots=True)
class BenchmarkSpec:
    benchmark_id: str
    benchmark_kind: str
    proposal_budget: int
    strict_eval_budget: int
    archive_namespace: str
    policy_frozen: bool = True
    data_access_contract: str = "development_only_no_forward"
    execution_state: str = "planned_not_run"

    def validate(self) -> None:
        if self.benchmark_kind not in {"strategy", "algorithm"}:
            raise ValueError(f"invalid benchmark kind: {self.benchmark_id}")
        if self.proposal_budget < 0 or self.strict_eval_budget < 0 or self.strict_eval_budget > self.proposal_budget:
            raise ValueError(f"invalid benchmark budget: {self.benchmark_id}")
        if not self.policy_frozen or self.execution_state != "planned_not_run":
            raise ValueError(f"NEXTGEN benchmark must remain frozen and not run: {self.benchmark_id}")
        if self.data_access_contract != "development_only_no_forward":
            raise ValueError(f"invalid data access contract: {self.benchmark_id}")


@dataclass(frozen=True, slots=True)
class CompetitorReproductionAdapter:
    adapter_id: str
    source_system: str
    source_version: str
    source_hash: str
    license_note: str
    lane_id: str = "competitor_reproduction"
    policy_frozen: bool = True

    def submit(self, row: Mapping[str, Any], lanes: HypothesisLaneRegistry) -> dict[str, Any]:
        if not all((self.adapter_id, self.source_system, self.source_version, self.source_hash, self.license_note)):
            raise ValueError("competitor reproduction requires complete source provenance")
        if not re.fullmatch(r"[0-9a-fA-F]{64}", self.source_hash):
            raise ValueError("competitor reproduction source_hash must be a SHA-256 hex digest")
        if not self.policy_frozen or str(row.get("lane_id")) != self.lane_id:
            raise ValueError("competitor adapter is frozen to the competitor_reproduction lane")
        output = lanes.validate_submission(row)
        output["competitor_adapter_id"] = self.adapter_id
        output["competitor_source_system"] = self.source_system
        output["competitor_source_version"] = self.source_version
        output["competitor_source_hash"] = self.source_hash
        output["competitor_license_note"] = self.license_note
        output["external_performance_imported"] = False
        return output


class BenchmarkHarness:
    def __init__(self, specs: Iterable[BenchmarkSpec], lanes: HypothesisLaneRegistry) -> None:
        self.lanes = lanes
        self._specs: dict[str, BenchmarkSpec] = {}
        archives: set[str] = set()
        for spec in specs:
            spec.validate()
            if spec.benchmark_id in self._specs or spec.archive_namespace in archives:
                raise ValueError(f"benchmark id/archive collision: {spec.benchmark_id}")
            self._specs[spec.benchmark_id] = spec
            archives.add(spec.archive_namespace)

    @property
    def specs(self) -> tuple[BenchmarkSpec, ...]:
        return tuple(self._specs[key] for key in sorted(self._specs))

    def validate_submission(self, benchmark_id: str, row: Mapping[str, Any]) -> dict[str, Any]:
        spec = self._specs[benchmark_id]
        candidate = self.lanes.validate_submission(row)
        candidate["benchmark_id"] = benchmark_id
        candidate["benchmark_archive_namespace"] = spec.archive_namespace
        candidate["benchmark_policy_frozen"] = True
        return candidate

    def plan(self) -> dict[str, Any]:
        payload = {
            "harness_version": HARNESS_VERSION,
            "benchmarks": [asdict(spec) for spec in self.specs],
            "proposal_budget": sum(spec.proposal_budget for spec in self.specs),
            "strict_eval_budget": sum(spec.strict_eval_budget for spec in self.specs),
            "candidate_submission_contract": "nextgen_dark_candidate_submission_v1",
            "independent_archives": True,
            "policy_freeze": True,
            "data_access_contract": "development_only_no_forward",
            "performance_comparison_executed": False,
        }
        payload["plan_hash"] = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return payload


def default_benchmark_harness(lanes: HypothesisLaneRegistry) -> BenchmarkHarness:
    strategies = (
        "x0_r3", "simple_momentum", "simple_reversal", "volatility", "liquidity",
        "firstN", "event_state", "plate_industry_linkage",
    )
    algorithms = (
        "cem", "typed_ast", "rx_ucb", "uct_mcts", "evolutionary_search",
        "surrogate", "llm_proposal", "external_competitor_reproduction",
    )
    specs = [
        BenchmarkSpec(name, "strategy", 32, 4, f"benchmark/strategy/{name}")
        for name in strategies
    ] + [
        BenchmarkSpec(name, "algorithm", 64, 8, f"benchmark/algorithm/{name}")
        for name in algorithms
    ]
    return BenchmarkHarness(specs, lanes)
