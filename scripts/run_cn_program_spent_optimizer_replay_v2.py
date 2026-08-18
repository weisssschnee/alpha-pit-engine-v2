"""Development-only online replay of Program optimizer policies on spent evidence.

No evaluator is called.  Every selected exact identity is looked up in the
canonical 1310-row spent-development dataset, then fed back through the normal
ProgramOptimizerObservationV1 contract.  The fixed pool is retrospective and
therefore report-only search-policy evidence.
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from statistics import mean
from typing import Any, Mapping, Sequence

from our_system_phase2.services.candidate_program_proposal_v0 import (
    PROGRAM_TEMPLATE_COMPONENTS,
)
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
from our_system_phase2.services.unified_capability_registry import stable_hash

TEMPLATES = tuple(
    template for template in PROGRAM_TEMPLATE_COMPONENTS if template != "BASE"
)
POLICIES = (
    "UNIFORM",
    "EXACT_ID_TPE_V1",
    "HIERARCHICAL_CEM_V2",
    "CATALOG_TYPED_EVOLUTION_V2",
)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _verify_dataset(dataset: Mapping[str, Any]) -> None:
    if dataset.get("status") != "FROZEN_SPENT_DEVELOPMENT_DATASET_READY":
        raise ValueError("SPENT_REPLAY_DATASET_STATUS_INVALID")
    if int(dataset.get("post_C_prior_exact_count") or 0) != 1310:
        raise ValueError("SPENT_REPLAY_DATASET_COUNT_DRIFT")
    if any(
        int(dataset.get(key) or 0) != 0
        for key in (
            "validation_reads",
            "holdout_reads",
            "historical_challenge_reads",
            "forward_B_reads",
            "forward_2026_reads",
        )
    ):
        raise ValueError("SPENT_REPLAY_RESTRICTED_READ_DRIFT")
    rows = list(dataset.get("rows") or ())
    if len(rows) != 1310 or stable_hash(rows) != str(dataset.get("rows_sha256")):
        raise ValueError("SPENT_REPLAY_ROWS_HASH_DRIFT")


def _entries_and_rows(dataset: Mapping[str, Any]):
    rows = list(dataset["rows"])
    entries = program_availability_entries_v1(
        [{"genes": dict(row["structural_genes"])} for row in rows]
    )
    by_exact = {str(row["exact_identity"]): dict(row) for row in rows}
    if len(by_exact) != len(rows):
        raise ValueError("SPENT_REPLAY_EXACT_DUPLICATE")
    if {entry.exact_identity for entry in entries} != set(by_exact):
        raise ValueError("SPENT_REPLAY_STRUCTURAL_IDENTITY_DRIFT")
    return entries, by_exact


def _observation(
    ask: Mapping[str, Any], source: Mapping[str, Any]
) -> ProgramOptimizerObservationV1:
    exact = str(ask["exact_identity"])
    admitted = bool(source["admitted"])
    admission = AbsoluteEconomicAdmission(
        record_payload_sha256=f"spent-replay::{source['source_record_sha256']}",
        pair_id=f"spent-pair::{exact}",
        program_id=f"spent-program::{exact}",
        control_program_id=f"spent-control::{exact}",
        admitted=admitted,
        failure_reasons=() if admitted else ("SPENT_DEVELOPMENT_ADMISSION_FAILED",),
        metrics={
            "offline_replay": True,
            "source_cohort": str(source["source_cohort"]),
        },
    )
    uplift = None
    if admitted:
        return_increment = float(source["matched_cumulative_net_return_increment"])
        reward_increment = float(source["matched_net_reward_increment"])
        uplift = ProgramUpliftCredit(
            record_payload_sha256=admission.record_payload_sha256,
            pair_id=admission.pair_id,
            program_id=admission.program_id,
            control_program_id=admission.control_program_id,
            program_credit={
                "matched_cumulative_net_return_increment": return_increment,
                "matched_net_reward_increment": reward_increment,
            },
        )
    return ProgramOptimizerObservationV1(
        proposal_id=str(ask["proposal_id"]),
        exact_identity=exact,
        admission=admission,
        uplift=uplift,
    )


def _new_policy(name: str, *, entries, seed: int):
    common = dict(entries=entries, seen_exact_identities=(), seed=seed)
    if name == "UNIFORM":
        return UniformProgramSearchAdapter(**common)
    if name == "EXACT_ID_TPE_V1":
        return HybridTPEProgramSearchAdapter(
            **common, n_startup_trials=24, n_ei_candidates=64
        )
    if name == "HIERARCHICAL_CEM_V2":
        return HierarchicalProgramCEMV2(
            **common,
            elite_fraction=0.20,
            smoothing=0.35,
            minimum_probability=0.002,
            entropy_floor_ratio=0.60,
            minimum_observation_count=4,
            count_pseudocount=0.50,
        )
    if name == "CATALOG_TYPED_EVOLUTION_V2":
        return CatalogTypedEvolutionProgramV2(
            **common,
            warmup=32,
            tournament_size=4,
            population_limit=256,
        )
    raise ValueError(name)


def _metric(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    admitted = sum(bool(row["admitted"]) for row in rows)
    productive = sum(bool(row["productive"]) for row in rows)
    returns = [
        float(row["matched_cumulative_net_return_increment"])
        for row in rows
        if row["matched_cumulative_net_return_increment"] is not None
    ]
    rewards = [
        float(row["matched_net_reward_increment"])
        for row in rows
        if row["matched_net_reward_increment"] is not None
    ]
    behaviors = {
        str(row["behavior_pair_identity"])
        for row in rows
        if row.get("behavior_pair_identity")
    }
    return {
        "evaluated": len(rows),
        "admitted": admitted,
        "productive": productive,
        "productive_efficiency": productive / len(rows) if rows else 0.0,
        "admission_rate": admitted / len(rows) if rows else 0.0,
        "mean_matched_return_increment": mean(returns) if returns else None,
        "mean_matched_reward_increment": mean(rewards) if rewards else None,
        "behavior_pair_count": len(behaviors),
        "source_cohorts": dict(Counter(str(row["source_cohort"]) for row in rows)),
    }


def _run_policy(
    name: str,
    *,
    entries,
    by_exact: Mapping[str, Mapping[str, Any]],
    seed: int,
    macro_count: int,
    checkpoint_size: int,
) -> dict[str, Any]:
    adapter = _new_policy(name, entries=entries, seed=seed)
    eligible_by_template = {
        template: tuple(
            sorted(
                exact
                for exact, row in by_exact.items()
                if str(row["template_id"]) == template
            )
        )
        for template in TEMPLATES
    }
    required_per_template = macro_count * checkpoint_size
    supply = {template: len(values) for template, values in eligible_by_template.items()}
    if any(count < required_per_template for count in supply.values()):
        raise RuntimeError(
            "SPENT_REPLAY_TEMPLATE_SUPPLY_INSUFFICIENT:"
            + json.dumps(supply, sort_keys=True)
        )
    selected_rows: list[dict[str, Any]] = []
    checkpoints = []
    selected_exacts: set[str] = set()
    ordinal = 0
    for macro_index in range(macro_count):
        macro_start = len(selected_rows)
        for template in TEMPLATES:
            asks = adapter.ask(
                checkpoint_id=f"replay_{ordinal:04d}",
                count=checkpoint_size,
                required_program_template_id=template,
                eligible_exact_identities=eligible_by_template[template],
            )
            if len(asks) != checkpoint_size:
                raise RuntimeError(
                    f"SPENT_REPLAY_ASK_COUNT_DRIFT:{name}:{template}:{len(asks)}"
                )
            exacts = [str(row["exact_identity"]) for row in asks]
            if len(set(exacts)) != checkpoint_size or set(exacts) & selected_exacts:
                raise RuntimeError("SPENT_REPLAY_EXACT_REUSE")
            selected_exacts.update(exacts)
            sources = [dict(by_exact[exact]) for exact in exacts]
            adapter.tell(
                [
                    _observation(ask, source)
                    for ask, source in zip(asks, sources, strict=True)
                ]
            )
            selected_rows.extend(sources)
            checkpoints.append(
                {
                    "checkpoint_ordinal": ordinal,
                    "macro_index": macro_index,
                    "template_id": template,
                    **_metric(sources),
                }
            )
            ordinal += 1
        checkpoints.append(
            {
                "checkpoint_ordinal": None,
                "macro_index": macro_index,
                "template_id": "__MACRO__",
                **_metric(selected_rows[macro_start:]),
            }
        )
    budgets = {}
    for budget in (168, 336, 504, 672, 840):
        if budget <= len(selected_rows):
            budgets[str(budget)] = _metric(selected_rows[:budget])
    return {
        "policy": name,
        "seed": seed,
        "template_supply": supply,
        "selected_exact_count": len(selected_exacts),
        "selected_exact_sha256": stable_hash(sorted(selected_exacts)),
        "budgets": budgets,
        "final": _metric(selected_rows),
        "checkpoints": checkpoints,
        "optimizer_metadata": adapter.optimizer_metadata(),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--seeds", type=int, default=4)
    parser.add_argument("--macros", type=int, default=5)
    parser.add_argument("--checkpoint-size", type=int, default=24)
    args = parser.parse_args()
    dataset = _read_json(args.dataset.resolve())
    _verify_dataset(dataset)
    entries, by_exact = _entries_and_rows(dataset)
    runs: dict[str, list[dict[str, Any]]] = {policy: [] for policy in POLICIES}
    for seed_offset in range(args.seeds):
        for policy_index, policy in enumerate(POLICIES):
            runs[policy].append(
                _run_policy(
                    policy,
                    entries=entries,
                    by_exact=by_exact,
                    seed=82618000 + seed_offset * 101 + policy_index,
                    macro_count=args.macros,
                    checkpoint_size=args.checkpoint_size,
                )
            )
    aggregate = {}
    budget_labels = (168, 336, 504, 672, 840)
    for policy, policy_runs in runs.items():
        aggregate[policy] = {
            "runs": len(policy_runs),
            "budgets": {
                str(budget): {
                    "mean_productive_efficiency": mean(
                        run["budgets"][str(budget)]["productive_efficiency"]
                        for run in policy_runs
                    ),
                    "mean_productive_count": mean(
                        run["budgets"][str(budget)]["productive"]
                        for run in policy_runs
                    ),
                    "mean_admission_rate": mean(
                        run["budgets"][str(budget)]["admission_rate"]
                        for run in policy_runs
                    ),
                    "mean_behavior_pair_count": mean(
                        run["budgets"][str(budget)]["behavior_pair_count"]
                        for run in policy_runs
                    ),
                }
                for budget in budget_labels
                if str(budget) in policy_runs[0]["budgets"]
            },
        }
    payload = {
        "schema_version": "cn_program_spent_optimizer_online_replay_v2",
        "status": "SPENT_DEVELOPMENT_OPTIMIZER_REPLAY_COMPLETE",
        "evidence_role": "REPORT_ONLY_FIXED_RETROSPECTIVE_SEARCH_POLICY_COMPARISON",
        "dataset_payload_sha256": dataset["dataset_payload_sha256"],
        "financial_evaluation_performed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_reads": 0,
        "policy_feedback_source": "SELECTED_SPENT_DEVELOPMENT_OUTCOME_ONLY",
        "prior_optimizer_state_imported": False,
        "macro_count": args.macros,
        "checkpoint_size": args.checkpoint_size,
        "seed_count": args.seeds,
        "aggregate": aggregate,
        "runs": runs,
    }
    payload["replay_payload_sha256"] = stable_hash(payload)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "status": payload["status"],
        "dataset_payload_sha256": payload["dataset_payload_sha256"],
        "replay_payload_sha256": payload["replay_payload_sha256"],
        "aggregate": aggregate,
    }, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
