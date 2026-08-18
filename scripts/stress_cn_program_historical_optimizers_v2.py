"""Synthetic equal-budget stress for Program optimizer policies; no financial reads."""
from __future__ import annotations

import argparse
import json
from itertools import product
from pathlib import Path
from statistics import mean

from our_system_phase2.services.program_search_optimizer_historical_v2 import (
    CatalogTypedEvolutionProgramV2,
    HierarchicalProgramCEMV2,
)
from our_system_phase2.services.program_search_optimizer_v1 import (
    HybridTPEProgramSearchAdapter,
    ProgramOptimizerObservationV1,
    UniformProgramSearchAdapter,
    program_availability_entries_v1,
)
from our_system_phase2.services.search_v2_admission import AbsoluteEconomicAdmission
from our_system_phase2.services.search_v2_conditional_uplift import ProgramUpliftCredit

PRODUCTIVE_REWARD_THRESHOLD = 0.45


def _space():
    rows = []
    for base_route, base_skeleton, temporal_skeleton, combine, base_field in product(
        ("MINUTE_STATIC", "SLOW_CROSS_SECTIONAL_LEVEL"),
        ("base.0", "base.1", "base.2", "base.3"),
        ("temporal.0", "temporal.1", "temporal.2", "temporal.3"),
        ("ADD", "MAX"),
        ("field.0", "field.1"),
    ):
        rows.append(
            {
                "genes": {
                    "skeleton_id": "CN_TYPED_PROGRAM_GENE_SPACE_V1",
                    "gene_surface_id": "CN_TYPED_PROGRAM_GENE_SPACE_V1",
                    "program_template_id": "BASE_TEMPORAL",
                    "active_component_roles": "base+temporal",
                    "composition_topology": "base>temporal",
                    "combination_temporal": combine,
                    "combination_market": "FILTER",
                    "combination_event_episode": "SOURCE_ROUTE_EPISODE",
                    "combination_event_application": "FILTER",
                    "joint_clock_class": "derived-clock",
                    "lag_class": f"derived::{base_skeleton}::{temporal_skeleton}",
                    "structural_complexity_class": f"derived::{base_route}",
                    "raw_field_count": "2",
                    "rolling_node_count": "1",
                    "interaction_topology": "derived-interaction",
                    "base__route_id": base_route,
                    "base__skeleton_id": base_skeleton,
                    "base__primary_field_id": base_field,
                    "temporal__route_id": "SLOW_TEMPORAL_CHANGE",
                    "temporal__skeleton_id": temporal_skeleton,
                    "temporal__primary_field_id": "field.temporal",
                }
            }
        )
    return program_availability_entries_v1(rows)


def _score(genes) -> float:
    # Deliberately contains both marginal and interaction structure.  The exact
    # identity itself carries no useful ordering information.
    score = 0.08
    if genes["base__route_id"] == "MINUTE_STATIC":
        score += 0.10
    if genes["base__skeleton_id"] == "base.2":
        score += 0.12
    if genes["temporal__skeleton_id"] == "temporal.3":
        score += 0.12
    if genes["combination_temporal"] == "MAX":
        score += 0.10
    if genes["base__primary_field_id"] == "field.1":
        score += 0.08
    if (
        genes["base__route_id"] == "MINUTE_STATIC"
        and genes["base__skeleton_id"] == "base.2"
    ):
        score += 0.14
    if (
        genes["temporal__skeleton_id"] == "temporal.3"
        and genes["combination_temporal"] == "MAX"
    ):
        score += 0.12
    if (
        genes["base__skeleton_id"] == "base.2"
        and genes["base__primary_field_id"] == "field.1"
    ):
        score += 0.08
    if (
        genes["base__route_id"] == "MINUTE_STATIC"
        and genes["base__skeleton_id"] == "base.2"
        and genes["temporal__skeleton_id"] == "temporal.3"
        and genes["combination_temporal"] == "MAX"
        and genes["base__primary_field_id"] == "field.1"
    ):
        score += 0.06
    return min(1.0, score)


def _observation(ask, score: float) -> ProgramOptimizerObservationV1:
    identity = str(ask["exact_identity"])
    admission = AbsoluteEconomicAdmission(
        record_payload_sha256=f"record-{identity}",
        pair_id=f"pair-{identity}",
        program_id=f"program-{identity}",
        control_program_id=f"control-{identity}",
        admitted=True,
        failure_reasons=(),
        metrics={"synthetic": True},
    )
    uplift = ProgramUpliftCredit(
        record_payload_sha256=admission.record_payload_sha256,
        pair_id=admission.pair_id,
        program_id=admission.program_id,
        control_program_id=admission.control_program_id,
        program_credit={
            "matched_cumulative_net_return_increment": score,
            "matched_net_reward_increment": score - PRODUCTIVE_REWARD_THRESHOLD,
        },
    )
    return ProgramOptimizerObservationV1(
        proposal_id=str(ask["proposal_id"]),
        exact_identity=identity,
        admission=admission,
        uplift=uplift,
    )


def _run(policy, *, entries, seed: int, budget: int, batch: int):
    common = dict(entries=entries, seen_exact_identities=(), seed=seed)
    if policy is HybridTPEProgramSearchAdapter:
        adapter = policy(**common, n_startup_trials=16, n_ei_candidates=64)
    elif policy is HierarchicalProgramCEMV2:
        adapter = policy(
            **common,
            elite_fraction=0.20,
            smoothing=0.35,
            minimum_probability=0.002,
            entropy_floor_ratio=0.60,
            minimum_observation_count=4,
            count_pseudocount=0.50,
        )
    elif policy is CatalogTypedEvolutionProgramV2:
        adapter = policy(**common, warmup=16, tournament_size=4, population_limit=64)
    else:
        adapter = policy(**common)
    scores = []
    exacts = []
    checkpoint = 0
    while len(scores) < budget:
        asks = adapter.ask(
            checkpoint_id=f"stress_{checkpoint:03d}",
            count=min(batch, budget - len(scores)),
            required_program_template_id="BASE_TEMPORAL",
        )
        if not asks:
            break
        local = [_score(row["program_genes"]) for row in asks]
        adapter.tell(
            [
                _observation(row, value)
                for row, value in zip(asks, local, strict=True)
            ]
        )
        scores.extend(local)
        exacts.extend(str(row["exact_identity"]) for row in asks)
        checkpoint += 1
    half = len(scores) // 2
    return {
        "evaluated": len(scores),
        "unique_exact": len(set(exacts)),
        "best": max(scores, default=0.0),
        "mean": mean(scores) if scores else 0.0,
        "last_half_mean": mean(scores[half:]) if scores[half:] else 0.0,
        "productive_count": sum(value > PRODUCTIVE_REWARD_THRESHOLD for value in scores),
        "high_score_ge_0_8": sum(value >= 0.8 for value in scores),
        "top8_mean": mean(sorted(scores, reverse=True)[:8]) if scores else 0.0,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seeds", type=int, default=16)
    parser.add_argument("--budget", type=int, default=64)
    parser.add_argument("--batch", type=int, default=8)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    entries = _space()
    policies = {
        "UNIFORM": UniformProgramSearchAdapter,
        "EXACT_ID_TPE_V1": HybridTPEProgramSearchAdapter,
        "HIERARCHICAL_CEM_V2": HierarchicalProgramCEMV2,
        "CATALOG_TYPED_EVOLUTION_V2": CatalogTypedEvolutionProgramV2,
    }
    runs = {name: [] for name in policies}
    for offset in range(args.seeds):
        for ordinal, (name, policy) in enumerate(policies.items()):
            runs[name].append(
                _run(
                    policy,
                    entries=entries,
                    seed=8261800 + offset * 17 + ordinal,
                    budget=args.budget,
                    batch=args.batch,
                )
            )
    aggregate = {}
    for name, rows in runs.items():
        aggregate[name] = {
            "mean_best": mean(row["best"] for row in rows),
            "mean_score": mean(row["mean"] for row in rows),
            "mean_last_half": mean(row["last_half_mean"] for row in rows),
            "mean_productive_count": mean(row["productive_count"] for row in rows),
            "mean_high_score_ge_0_8": mean(row["high_score_ge_0_8"] for row in rows),
            "mean_top8": mean(row["top8_mean"] for row in rows),
            "all_runs_unique_exact": all(row["evaluated"] == row["unique_exact"] for row in rows),
        }
    payload = {
        "schema_version": "cn_program_historical_optimizer_synthetic_stress_v2",
        "candidate_count": len(entries),
        "productive_reward_threshold": PRODUCTIVE_REWARD_THRESHOLD,
        "seed_count": args.seeds,
        "budget": args.budget,
        "batch": args.batch,
        "aggregate": aggregate,
        "runs": runs,
    }
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
