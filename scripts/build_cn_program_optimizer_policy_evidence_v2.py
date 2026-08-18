"""Freeze report-only optimizer evidence used to authorize Large Fresh V2."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from statistics import mean
from typing import Any, Mapping

from our_system_phase2.services.unified_capability_registry import stable_hash

POLICIES = (
    "UNIFORM",
    "HIERARCHICAL_CEM_V2",
    "CATALOG_TYPED_EVOLUTION_V2",
)
BUDGETS = (168, 336, 504, 672, 840)


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _write(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(dict(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def build(args: argparse.Namespace) -> dict[str, Any]:
    dataset = _read(args.dataset)
    if (
        dataset.get("status") != "FROZEN_SPENT_DEVELOPMENT_DATASET_READY"
        or len(dataset.get("rows") or ()) != 1310
        or dataset.get("financial_evaluation_performed_by_builder") is not False
        or any(
            int(dataset.get(key) or 0) != 0
            for key in (
                "validation_reads",
                "holdout_reads",
                "forward_B_reads",
                "forward_2026_reads",
            )
        )
    ):
        raise ValueError("optimizer evidence dataset boundary drift")
    dataset_payload = str(dataset["dataset_payload_sha256"])

    paired_runs = []
    deltas_by_budget = {budget: {"cem": [], "evolution": []} for budget in BUDGETS}
    for seed_offset in range(4):
        path = args.paired_root / f"CORE_s{seed_offset}.json"
        payload = _read(path)
        if (
            payload.get("status") != "SPENT_DEVELOPMENT_OPTIMIZER_REPLAY_COMPLETE"
            or payload.get("dataset_payload_sha256") != dataset_payload
            or payload.get("financial_evaluation_performed") is not False
            or list(payload.get("policies") or ()) != list(POLICIES)
        ):
            raise ValueError(f"paired replay boundary drift: {path}")
        runs = {policy: payload["runs"][policy][0] for policy in POLICIES}
        seeds = {int(run["seed"]) for run in runs.values()}
        if len(seeds) != 1:
            raise ValueError(f"paired replay seed drift: {path}")
        seed = next(iter(seeds))
        budget_rows = {}
        for budget in BUDGETS:
            label = str(budget)
            counts = {
                policy: int(runs[policy]["budgets"][label]["productive"])
                for policy in POLICIES
            }
            cem_delta = counts["HIERARCHICAL_CEM_V2"] - counts["UNIFORM"]
            evo_delta = counts["CATALOG_TYPED_EVOLUTION_V2"] - counts["UNIFORM"]
            deltas_by_budget[budget]["cem"].append(cem_delta)
            deltas_by_budget[budget]["evolution"].append(evo_delta)
            budget_rows[label] = {
                "productive": counts,
                "cem_delta_vs_uniform": cem_delta,
                "evolution_delta_vs_uniform": evo_delta,
            }
        paired_runs.append(
            {
                "seed_offset": seed_offset,
                "seed": seed,
                "file": str(path.resolve()),
                "file_sha256": _sha256(path),
                "replay_payload_sha256": str(payload["replay_payload_sha256"]),
                "budgets": budget_rows,
            }
        )

    paired_summary = {
        str(budget): {
            "cem_deltas": deltas_by_budget[budget]["cem"],
            "cem_mean_delta": mean(deltas_by_budget[budget]["cem"]),
            "cem_positive_seed_count": sum(x > 0 for x in deltas_by_budget[budget]["cem"]),
            "evolution_deltas": deltas_by_budget[budget]["evolution"],
            "evolution_mean_delta": mean(deltas_by_budget[budget]["evolution"]),
            "evolution_positive_seed_count": sum(x > 0 for x in deltas_by_budget[budget]["evolution"]),
        }
        for budget in BUDGETS
    }
    if (
        paired_summary["840"]["evolution_mean_delta"] != 24.25
        or paired_summary["840"]["evolution_positive_seed_count"] != 4
    ):
        raise ValueError("Evolution paired evidence qualification drift")

    tpe_completed = []
    for seed_offset in (1, 2):
        path = args.paired_root / f"TPE_s{seed_offset}.json"
        payload = _read(path)
        run = payload["runs"]["EXACT_ID_TPE_V1"][0]
        if (
            payload.get("dataset_payload_sha256") != dataset_payload
            or int(run["seed"]) != paired_runs[seed_offset]["seed"]
        ):
            raise ValueError("paired TPE replay drift")
        tpe = int(run["budgets"]["168"]["productive"])
        uniform = int(
            _read(args.paired_root / f"CORE_s{seed_offset}.json")["runs"]["UNIFORM"][0]["budgets"]["168"]["productive"]
        )
        tpe_completed.append(
            {
                "seed_offset": seed_offset,
                "seed": int(run["seed"]),
                "productive_168": tpe,
                "uniform_productive_168": uniform,
                "delta_vs_uniform": tpe - uniform,
                "file_sha256": _sha256(path),
            }
        )
    kill = _read(args.tpe_kill_receipt)
    if kill.get("status") != "PASS" or int(kill.get("remaining_count", -1)) != 0:
        raise ValueError("TPE slow-path cleanup receipt drift")

    payload = {
        "schema_version": "cn_program_optimizer_policy_evidence_v2",
        "status": "PASS_REPORT_ONLY_OPTIMIZER_POLICY_EVIDENCE",
        "decision": "EVOLUTION_PRIMARY_UNIFORM_RESERVE",
        "evidence_role": "REPORT_ONLY_FIXED_RETROSPECTIVE_SEARCH_POLICY_COMPARISON",
        "dataset": {
            "file": str(args.dataset.resolve()),
            "file_sha256": _sha256(args.dataset),
            "dataset_payload_sha256": dataset_payload,
            "row_count": 1310,
            "financial_evaluation_performed_by_builder": False,
        },
        "paired_seed_count": 4,
        "paired_runs": paired_runs,
        "paired_summary": paired_summary,
        "evolution_mean_productive_delta_at_840": 24.25,
        "evolution_positive_seed_count_at_840": 4,
        "evolution_budget_role": "PRIMARY_6_OF_7_CHECKPOINTS_PER_MACRO",
        "uniform_budget_role": "ROTATING_1_OF_7_CHECKPOINTS_PER_MACRO",
        "cem_budget_role": "IMPLEMENTED_CHALLENGER_NOT_ALLOCATED_IN_THIS_840",
        "tpe_budget_role": "BASELINE_NOT_ALLOCATED_IN_THIS_840",
        "tpe_completed_paired_168": tpe_completed,
        "tpe_slow_path_seed_offsets": [0, 3],
        "tpe_slow_path_cleanup_receipt_sha256": _sha256(args.tpe_kill_receipt),
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_reads": 0,
        "promotion_authorized": False,
    }
    payload["evidence_payload_sha256"] = stable_hash(payload)
    _write(args.output, payload)
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--paired-root", type=Path, required=True)
    parser.add_argument("--tpe-kill-receipt", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    payload = build(args)
    print(json.dumps({
        "status": payload["status"],
        "decision": payload["decision"],
        "evidence_payload_sha256": payload["evidence_payload_sha256"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
