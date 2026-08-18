"""Freeze exact-SHA report-only optimizer evidence for Large Fresh V2."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from statistics import mean
from typing import Any, Mapping

from our_system_phase2.services.unified_capability_registry import stable_hash

IMPLEMENTATION_REPO_SHA = "c8ff15049d29da00085cc7ce1e7b76b09ad58a37"
EXPECTED_SEEDS = tuple(82618000 + offset * 101 for offset in range(4))


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
    dataset_file_sha = _sha256(args.dataset)

    receipt = _read(args.pair_replay_receipt)
    if (
        receipt.get("schema_version") != "cn_program_optimizer_pair_replay_receipt_v1"
        or receipt.get("status") != "PASS"
        or receipt.get("repo_sha") != IMPLEMENTATION_REPO_SHA
        or int(receipt.get("dirty_count") or 0) != 0
        or receipt.get("dataset_payload_sha256") != dataset_payload
        or receipt.get("dataset_file_sha256") != dataset_file_sha
        or receipt.get("financial_evaluation_performed") is not False
        or any(int(receipt.get(key) or 0) != 0 for key in ("validation_reads", "holdout_reads", "forward_reads"))
    ):
        raise ValueError("pair replay receipt boundary drift")

    runs = [dict(row) for row in receipt.get("paired_runs") or ()]
    if len(runs) != 4:
        raise ValueError("pair replay seed cardinality drift")
    normalized: list[dict[str, Any]] = []
    for offset, row in enumerate(runs):
        seed = int(row.get("seed") or -1)
        u168 = int(row.get("uniform_168") or 0)
        e168 = int(row.get("evolution_168") or 0)
        d168 = int(row.get("delta_168") or 0)
        u840 = int(row.get("uniform_840") or 0)
        e840 = int(row.get("evolution_840") or 0)
        d840 = int(row.get("delta_840") or 0)
        if (
            int(row.get("seed_offset") or 0) != offset
            or seed != EXPECTED_SEEDS[offset]
            or d168 != e168 - u168
            or d840 != e840 - u840
            or d168 <= 0
            or d840 <= 0
            or len(str(row.get("file_sha256") or "")) != 64
            or len(str(row.get("replay_payload_sha256") or "")) != 64
        ):
            raise ValueError(f"paired replay row drift: {offset}")
        normalized.append(
            {
                "seed_offset": offset,
                "seed": seed,
                "uniform_productive_168": u168,
                "evolution_productive_168": e168,
                "delta_168": d168,
                "uniform_productive_840": u840,
                "evolution_productive_840": e840,
                "delta_840": d840,
                "replay_file_sha256": str(row["file_sha256"]),
                "replay_payload_sha256": str(row["replay_payload_sha256"]),
            }
        )

    mean_delta_168 = mean(row["delta_168"] for row in normalized)
    mean_delta_840 = mean(row["delta_840"] for row in normalized)
    mean_uniform_840 = mean(row["uniform_productive_840"] for row in normalized)
    mean_evolution_840 = mean(row["evolution_productive_840"] for row in normalized)
    if mean_delta_168 != 3.0 or mean_delta_840 != 23.5:
        raise ValueError("paired replay aggregate drift")

    payload = {
        "schema_version": "cn_program_optimizer_policy_evidence_v2",
        "status": "PASS_REPORT_ONLY_OPTIMIZER_POLICY_EVIDENCE",
        "decision": "EVOLUTION_PRIMARY_UNIFORM_RESERVE",
        "evidence_role": "REPORT_ONLY_FIXED_RETROSPECTIVE_SEARCH_POLICY_COMPARISON",
        "implementation_repo_sha": IMPLEMENTATION_REPO_SHA,
        "algorithm_contract": {
            "optimizer_arm": "CATALOG_TYPED_EVOLUTION_PROGRAM_V2",
            "algorithm_origin": "CRYPTO_TYPED_EVOLUTION_V2_PORT",
            "population_limit": 256,
            "template_cell_limit": 64,
            "warmup": 32,
            "tournament_size": 4,
            "gene_mutation_probability": 0.55,
            "compatible_skeleton_mutation_probability": 0.25,
            "homologous_crossover_probability": 0.20,
        },
        "dataset": {
            "file_sha256": dataset_file_sha,
            "dataset_payload_sha256": dataset_payload,
            "row_count": 1310,
            "financial_evaluation_performed_by_builder": False,
        },
        "pair_replay_receipt": {
            "file_sha256": _sha256(args.pair_replay_receipt),
            "repo_sha": IMPLEMENTATION_REPO_SHA,
            "source_role": "77O_EXACT_SHA_REPLAY_RECEIPT",
        },
        "paired_seed_count": 4,
        "paired_runs": normalized,
        "evolution_mean_productive_delta_at_168": mean_delta_168,
        "evolution_positive_seed_count_at_168": 4,
        "evolution_mean_productive_delta_at_840": mean_delta_840,
        "evolution_positive_seed_count_at_840": 4,
        "mean_uniform_productive_at_840": mean_uniform_840,
        "mean_evolution_productive_at_840": mean_evolution_840,
        "evolution_budget_role": "PRIMARY_6_OF_7_CHECKPOINTS_PER_MACRO",
        "uniform_budget_role": "ROTATING_1_OF_7_CHECKPOINTS_PER_MACRO",
        "cem_budget_role": "IMPLEMENTED_CHALLENGER_NOT_ALLOCATED_IN_THIS_840",
        "tpe_budget_role": "BASELINE_NOT_ALLOCATED_IN_THIS_840",
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
    parser.add_argument("--pair-replay-receipt", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    payload = build(args)
    print(json.dumps({
        "status": payload["status"],
        "decision": payload["decision"],
        "implementation_repo_sha": payload["implementation_repo_sha"],
        "evolution_mean_productive_delta_at_168": payload["evolution_mean_productive_delta_at_168"],
        "evolution_mean_productive_delta_at_840": payload["evolution_mean_productive_delta_at_840"],
        "evidence_payload_sha256": payload["evidence_payload_sha256"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
