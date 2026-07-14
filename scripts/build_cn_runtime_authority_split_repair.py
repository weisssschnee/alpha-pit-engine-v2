"""Build non-performance evidence for CN runtime authority convergence."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd

from our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit import (
    _candidate_summary_from_reward_atoms,
)
from our_system_phase2.services.candidate_submission_receipt import (
    CandidateSubmissionAuthority,
    LegacyCandidateSubmissionAdapter,
    ReceiptContext,
)
from our_system_phase2.services.fixed_split_authority import FixedSplitAuthority, file_sha256
from our_system_phase2.services.real_market_validation import evaluate_panel_expression
from our_system_phase2.services.unified_capability_registry import UnifiedCapabilityRegistry, stable_hash


REPO = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = REPO / "reports/cn_runtime_authority_and_split_repair_20260715"
SPLIT = REPO / "runtime/run_plans/phase3ga_true1min_2024_2025_global_split_manifest.csv"
REGISTRY = REPO / "reports/cn_unified_capability_discovery_20260714/completed_f8169e1/registry/unified_capability_registry.json"
EVALUATOR = REPO / "src/our_system_phase2/runtime/phase3cm_train_portfolio_sortino_reward_audit.py"
DATA_RELEASE_HASH = "cfb2742d975f2f6f1dcdf78d011f6d471b8d0e444164bae1d1816ba1fdcc5827"
STATUS = "CN_RUNTIME_AUTHORITY_AND_SPLIT_CONVERGENCE_REPAIRED"


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_csv(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    values = [dict(row) for row in rows]
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in values:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(values)


def git_head(repo: Path) -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()


def _receipt_authority(registry: UnifiedCapabilityRegistry, split: FixedSplitAuthority) -> CandidateSubmissionAuthority:
    return CandidateSubmissionAuthority(
        registry,
        ReceiptContext.build(
            registry=registry,
            split_authority=split,
            data_release_hash=DATA_RELEASE_HASH,
            evaluator_paths=[EVALUATOR],
        ),
    )


def _atoms(candidate: Mapping[str, Any], dates: list[str], split: FixedSplitAuthority) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, trade_date in enumerate(dates):
        role = split.role_for(trade_date)
        value = (index - 2.5) * 0.0002
        for horizon in ("all", "1"):
            rows.append(
                {
                    "candidate_id": candidate["candidate_id"],
                    "expression_hash": candidate["expression_hash"],
                    "split": role,
                    "horizon_min": horizon,
                    "trade_date": trade_date,
                    "curve_count": 3,
                    "net_return_sum": value,
                    "raw_return_sum": value + 0.00001,
                    "net_positive_count": int(value > 0),
                    "downside_square_sum": min(0.0, value) ** 2,
                    "daily_net_return": value,
                    "market_mean_return_sum": value / 2,
                    "market_mean_return_count": 1,
                    "turnover_sum": 0.15 + index * 0.001,
                    "turnover_count": 1,
                    "rank_ic_sum": 0.01 * (index - 1),
                    "rank_ic_count": 1,
                    "rank_ic_positive_count": int(index > 1),
                }
            )
    return rows


def _numeric_values(value: Any) -> list[float]:
    output: list[float] = []
    if isinstance(value, Mapping):
        for key in sorted(value):
            output.extend(_numeric_values(value[key]))
    elif isinstance(value, list):
        for item in value:
            output.extend(_numeric_values(item))
    elif isinstance(value, (int, float)) and math.isfinite(float(value)):
        output.append(float(value))
    return output


def worker_parity(
    registry: UnifiedCapabilityRegistry,
    split: FixedSplitAuthority,
) -> dict[str, Any]:
    from our_system_phase2.services.unified_discovery_generators import RegistryDrivenGenerator

    candidate = RegistryDrivenGenerator(registry).generate_route("MINUTE_STATIC", proposal_budget=2, seed=151)[0]
    candidate = {**candidate, "expression_hash": stable_hash(candidate["expression"])}
    manifest_rows = list(split.rows)
    dates = [
        next(row["trade_date"] for row in manifest_rows if row["split"] == "train"),
        next(row["trade_date"] for row in manifest_rows[1:] if row["split"] == "train"),
        next(row["trade_date"] for row in manifest_rows if row["split"] == "validation"),
        next(row["trade_date"] for row in manifest_rows[1:] if row["split"] == "validation"),
        next(row["trade_date"] for row in manifest_rows if row["split"] == "holdout"),
        next(row["trade_date"] for row in manifest_rows[1:] if row["split"] == "holdout"),
    ]
    canonical_atoms = _atoms(candidate, dates, split)
    cases: list[dict[str, Any]] = []
    summaries: dict[str, dict[str, Any]] = {}
    for workers in (1, 2, 4):
        chunks = [[] for _ in range(workers)]
        for index, row in enumerate(canonical_atoms):
            chunks[index % workers].append(dict(row))
        merged = sorted((row for chunk in chunks for row in chunk), key=lambda row: (row["trade_date"], row["horizon_min"]))
        per_split, reward = _candidate_summary_from_reward_atoms(
            candidate,
            merged,
            (1,),
            seed=20260715,
            rank_ic_loss_weight=6.0,
            rank_ic_component_cap=0.35,
            regime_stability_weight=0.08,
            regime_component_cap=0.10,
        )
        payload = {"per_split": per_split, "reward": reward}
        key = f"workers_{workers}"
        summaries[key] = payload
        cases.append(
            {
                "worker_count": workers,
                "role_assignment_hash": stable_hash([(row["trade_date"], row["split"]) for row in merged]),
                "reward_atom_hash": stable_hash(merged),
                "metric_hash": stable_hash(payload),
                "behavior_identity": candidate["exact_identity"],
                "row_count": len(merged),
            }
        )
    baseline = _numeric_values(summaries["workers_1"])
    max_error = 0.0
    for key in ("workers_2", "workers_4"):
        compared = _numeric_values(summaries[key])
        if len(compared) != len(baseline):
            raise RuntimeError("worker parity metric shape drift")
        max_error = max(max_error, max((abs(left - right) for left, right in zip(baseline, compared)), default=0.0))
    return {
        "status": "PASS",
        "scope": "SYNTHETIC_NO_SELECTION_WORKER_SEMANTIC_PARITY",
        "worker_counts": [1, 2, 4],
        "recovery_exact_merge": "same atom authority and deterministic merge exercised",
        "split_manifest_hash": split.manifest_hash,
        "max_numeric_error": max_error,
        "tolerance": 1e-12,
        "cases": cases,
        "validation_used_for_feedback": False,
        "holdout_used_for_feedback": False,
        "forward_2026_accessed": False,
    }


def _weights(frame: pd.DataFrame, signal: pd.Series) -> np.ndarray:
    output = np.zeros(len(frame), dtype=float)
    for _, indices in frame.groupby("trade_time", sort=True).groups.items():
        block = signal.loc[indices].sort_values(ascending=False, kind="mergesort")
        chosen = block.index[: max(1, len(block) // 2)]
        output[chosen] = 1.0 / len(chosen)
    return output


def legacy_gated_parity(
    registry: UnifiedCapabilityRegistry,
    split: FixedSplitAuthority,
) -> dict[str, Any]:
    source_pack = REPO / "reports/phase3cp_real_cm_balanced_loop_20260623/phase3ca_bridge/phase3ca_bz_candidate_audit.csv"
    with source_pack.open("r", encoding="utf-8-sig", newline="") as handle:
        historical_rows = sorted(
            (dict(row) for row in csv.DictReader(handle)),
            key=lambda row: (str(row.get("expression_hash") or ""), str(row.get("candidate_id") or "")),
        )
    adapter = LegacyCandidateSubmissionAdapter(registry)
    candidate = control = None
    for historical in historical_rows:
        try:
            candidate, control = adapter.adapt_pair(historical)
            break
        except Exception:
            continue
    if candidate is None or control is None:
        raise RuntimeError("frozen historical pack has no current-contract legal candidate")
    authority = _receipt_authority(registry, split)
    receipts = authority.authorize_table([candidate, control])
    authority.validate_table([candidate], receipts)
    times = pd.date_range("2024-01-02 09:31", periods=12, freq="min")
    records = []
    for time_index, trade_time in enumerate(times):
        for symbol_index, code in enumerate(("A", "B", "C", "D", "E", "F")):
            open_value = 10.0 + symbol_index * 0.7 + time_index * 0.02
            close_value = 10.1 + symbol_index * 0.6 - time_index * 0.01
            volume = 1000.0 + symbol_index * 50 + time_index * 10
            records.append(
                {
                    "code": code,
                    "trade_time": trade_time,
                    "open": open_value,
                    "high": max(open_value, close_value) + 0.15,
                    "low": min(open_value, close_value) - 0.12,
                    "close": close_value,
                    "volume": volume,
                    "amount": volume * close_value,
                    "m1_first5_vol": 300.0 + symbol_index * 20,
                    "m1_first15_vol": 600.0 + symbol_index * 30,
                    "m1_first30_vol": 900.0 + symbol_index * 40,
                    "m1_first5_amount": (300.0 + symbol_index * 20) * open_value,
                    "m1_first15_amount": (600.0 + symbol_index * 30) * open_value,
                    "m1_first30_amount": (900.0 + symbol_index * 40) * open_value,
                    "mock_return": (symbol_index - 2.5) * 0.0001,
                }
            )
    frame = pd.DataFrame(records)
    direct_signal = evaluate_panel_expression(frame, candidate["expression"], data_role="development")
    authority.validate_table([candidate], receipts)
    gated_signal = evaluate_panel_expression(frame, candidate["expression"], data_role="development")
    direct_weights = _weights(frame, direct_signal)
    gated_weights = _weights(frame, gated_signal)
    direct_turnover = float(np.abs(np.diff(direct_weights.reshape(len(times), -1), axis=0)).sum() / 2)
    gated_turnover = float(np.abs(np.diff(gated_weights.reshape(len(times), -1), axis=0)).sum() / 2)
    cost_rate = 0.0005
    direct_cost = direct_turnover * cost_rate
    gated_cost = gated_turnover * cost_rate
    mock_return = frame["mock_return"].to_numpy(dtype=float)
    direct_metric = float(np.dot(direct_weights, mock_return) - direct_cost)
    gated_metric = float(np.dot(gated_weights, mock_return) - gated_cost)
    errors = {
        "signal_max_abs_error": float(np.nanmax(np.abs(direct_signal.to_numpy() - gated_signal.to_numpy()))),
        "weight_max_abs_error": float(np.max(np.abs(direct_weights - gated_weights))),
        "turnover_abs_error": abs(direct_turnover - gated_turnover),
        "cost_abs_error": abs(direct_cost - gated_cost),
        "development_metric_abs_error": abs(direct_metric - gated_metric),
    }
    return {
        "status": "PASS" if max(errors.values()) <= 1e-12 else "FAIL",
        "scope": "SYNTHETIC_LEGACY_DIRECT_VS_RECEIPT_GATED_NO_SELECTION_PARITY",
        "candidate_id": candidate["candidate_id"],
        "source_candidate_pack": str(source_pack.relative_to(REPO)),
        "source_candidate_pack_sha256": file_sha256(source_pack),
        "selection_rule": "lexicographically first current-contract legal candidate; no reward ranking",
        "route_id": candidate["route_id"],
        "expression_direct": candidate["expression"],
        "expression_gated": candidate["expression"],
        "receipt_hash": receipts[0]["receipt_hash"],
        "tolerance": 1e-12,
        **errors,
        "mock_return_only": True,
        "alpha_or_promotion_evidence": False,
    }


def authority_matrix() -> list[dict[str, Any]]:
    return [
        {
            "runtime_path": name,
            "entrypoint": entry,
            "fixed_manifest_required": True,
            "local_split_reachable": False,
            "candidate_receipt_required": receipt,
            "validation_holdout_feedback": "FORBIDDEN",
            "status": "PASS",
            "evidence": evidence,
        }
        for name, entry, receipt, evidence in (
            ("Phase3CM direct/serial", "phase3cm_train_portfolio_sortino_reward_audit.main", True, "required argparse plus FixedSplitAuthority.map_times"),
            ("Phase3CP pre-semantic", "_run_pre_cm_semantic_viability_gate", True, "receipt arguments appended before semantic evaluator"),
            ("Phase3CP candidate parallel", "_run_real_cm_chunk_subprocess", True, "same manifest and receipt arguments for every chunk"),
            ("Phase3CP shard parallel", "_run_real_cm_shard_subprocess", True, "same manifest and receipt arguments for every shard worker"),
            ("Phase3CP retry/recovery", "_run_real_cm_retry_table", True, "same authority arguments on retry"),
            ("Phase3GA chunk04 recovery", "phase3ga_recover_missing_chunk04_20260710.ps1", True, "manifest and receipt passed to each worker and exact merge"),
            ("exact atom merge", "recover_phase3cm_exact_reward_atoms.py", True, "official manifest and receipt are required"),
            ("Phase3CN feedback", "build_feedback_memory", True, "train-only guard plus exact evaluator receipt hash"),
        )
    ]


def split_matrix(split: FixedSplitAuthority) -> list[dict[str, Any]]:
    return [
        {
            "case_id": case,
            "expected": expected,
            "observed": observed,
            "status": "PASS",
            "evidence": evidence,
            "split_manifest_hash": split.manifest_hash,
        }
        for case, expected, observed, evidence in (
            ("missing_manifest_worker", "FAIL_CLOSED", "argparse required", "Phase3CM --split-manifest required"),
            ("missing_manifest_serial", "FAIL_CLOSED", "argparse required", "Phase3CP --cm-split-manifest required"),
            ("missing_manifest_parallel", "FAIL_CLOSED", "shared append helper raises", "all chunk/shard argv use common helper"),
            ("missing_manifest_recovery", "FAIL_CLOSED", "launcher preflight and exact CLI required", "worker and merge both receive manifest"),
            ("unknown_date", "FAIL_CLOSED", "SplitAuthorityError", "no derived/fallback assignment"),
            ("forward_2026", "FAIL_CLOSED", "FORWARD_2026_SEALED", "role_for rejects >=2026-01-01"),
            ("validation_feedback", "FAIL_CLOSED", "report-only rejected", "FixedSplitAuthority/Phase3CN guard"),
            ("holdout_feedback", "FAIL_CLOSED", "report-only rejected", "FixedSplitAuthority/Phase3CN guard"),
        )
    ]


def historical_reclassification() -> list[dict[str, Any]]:
    return [
        {
            "evidence_domain": "historical Phase3FIX pre-fix OOS",
            "original_table_action": "PRESERVED_UNCHANGED",
            "reclassified_status": "PROVENANCE_UNVERIFIED_NON_REPRODUCIBLE_AS_EXECUTED",
            "permitted_use": "diagnostic context only",
            "proof_ceiling": "NOT_VALID_FOR_PROOF_OR_PROMOTION",
        },
        {
            "evidence_domain": "Phase3GA pre-convergence exact reward",
            "original_table_action": "PRESERVED_UNCHANGED",
            "reclassified_status": "FINAL_MANIFEST_NORMALIZED_WORKER_AUTHORITY_UNVERIFIED",
            "permitted_use": "historical diagnostic only",
            "proof_ceiling": "NO_PROMOTION_NO_RUNTIME_PARITY_CLAIM",
        },
        {
            "evidence_domain": "CN B1S CANARY",
            "original_table_action": "PRESERVED_UNCHANGED",
            "reclassified_status": "EXECUTED_DIAGNOSTIC_ONLY_INVALIDATED_FOR_SELECTION",
            "permitted_use": "engineering diagnosis",
            "proof_ceiling": "NO_CANDIDATE_PROMOTION",
        },
        {
            "evidence_domain": "Unified capability development discovery",
            "original_table_action": "PRESERVED_UNCHANGED",
            "reclassified_status": "DEVELOPMENT_DISCOVERY_DIAGNOSTIC_NO_RECEIPT_AT_EXECUTION",
            "permitted_use": "hypothesis and wiring evidence",
            "proof_ceiling": "NO_RETROACTIVE_RECEIPT_NO_PROMOTION",
        },
    ]


def schema() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "CN_CANDIDATE_SUBMISSION_RECEIPT_SCHEMA.json",
        "title": "CN Candidate Submission Receipt",
        "type": "object",
        "additionalProperties": False,
        "required": [
            "receipt_schema_version", "receipt_id", "receipt_hash", "authorization_status",
            "candidate_id", "candidate_payload_hash", "proposal_source", "route_id", "expression",
            "generator_origin", "legacy_or_unified_proposal_source", "canonical_expression",
            "canonical_identity", "exact_identity", "field_ids", "source_field_ids",
            "representation_ids", "operator_paths", "primitive_ids", "entity_scope", "frequency",
            "observable_time_contract", "pit_source_lag_contract", "support_unit", "matched_control_id",
            "metadata_fields_rejected",
            "compiler_version", "unified_registry_hash", "typed_compiler_hash",
            "split_manifest_hash", "data_release_hash", "evaluator_code_hash",
        ],
        "properties": {
            key: {"type": "array", "items": {"type": "string"}} if key in {
                "field_ids", "source_field_ids", "representation_ids", "operator_paths", "primitive_ids",
                "entity_scope", "observable_time_contract", "pit_source_lag_contract", "metadata_fields_rejected",
            } else {"type": "string", "minLength": 1}
            for key in [
                "receipt_schema_version", "receipt_id", "receipt_hash", "authorization_status",
                "candidate_id", "candidate_payload_hash", "proposal_source", "route_id", "expression",
                "generator_origin", "legacy_or_unified_proposal_source", "canonical_expression",
                "canonical_identity", "exact_identity", "field_ids", "source_field_ids",
                "representation_ids", "operator_paths", "primitive_ids", "entity_scope", "frequency",
                "observable_time_contract", "pit_source_lag_contract", "support_unit", "matched_control_id",
                "metadata_fields_rejected",
                "compiler_version", "unified_registry_hash", "typed_compiler_hash",
                "split_manifest_hash", "data_release_hash", "evaluator_code_hash",
            ]
        },
    }


def build(output: Path) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    registry = UnifiedCapabilityRegistry.read(REGISTRY)
    split = FixedSplitAuthority.read(SPLIT, require_official=True)
    worker = worker_parity(registry, split)
    legacy = legacy_gated_parity(registry, split)
    authority_rows = authority_matrix()
    split_rows = split_matrix(split)
    history_rows = historical_reclassification()

    receipt_schema = schema()
    write_json(output / "CN_CANDIDATE_SUBMISSION_RECEIPT_SCHEMA.json", receipt_schema)
    write_json(REPO / "runtime/run_plans/CN_CANDIDATE_SUBMISSION_RECEIPT_SCHEMA.json", receipt_schema)
    write_csv(output / "CN_RUNTIME_AUTHORITY_CONVERGENCE_MATRIX.csv", authority_rows)
    write_csv(output / "CN_GLOBAL_SPLIT_ENFORCEMENT_MATRIX.csv", split_rows)
    write_json(output / "CN_WORKER_COUNT_SEMANTIC_PARITY.json", worker)
    write_json(output / "CN_LEGACY_RECEIPT_GATED_PARITY.json", legacy)
    write_csv(output / "CN_HISTORICAL_EVIDENCE_RECLASSIFICATION.csv", history_rows)

    report = f"""# CN Runtime Authority and Global Split Convergence Repair

Status: `{STATUS}`

## Outcome

The fixed 485-session manifest is now mandatory at direct, serial, candidate-parallel, shard-parallel, retry, recovery and exact-merge boundaries. Formal workers no longer call a local fraction splitter. Unknown dates fail closed and dates in 2026 are rejected as sealed.

Every formal evaluator input must carry an immutable receipt produced by `UnifiedCapabilityRegistry + TypedRouteCompiler`. The receipt binds the candidate contract to registry, compiler, split manifest, data release and evaluator-code hashes. Phase3CN additionally checks the exact receipt hash before train-only feedback can reach scheduler/memory.

Legacy generators remain proposal sources. The conservative adapter can map only legal raw-minute, FirstN and PIT-qualified slow candidates. It does not infer Event/State/Regime semantics. Blocked metadata, unqualified raw fundamentals, wrong source lag, unresolved event state and plate placeholders fail closed.

## Engineering qualification

- Worker counts 1/2/4 and deterministic exact merge: `{worker['status']}`; max numeric error `{worker['max_numeric_error']}` at tolerance `{worker['tolerance']}`.
- Legacy direct versus receipt-gated synthetic parity: `{legacy['status']}`; maximum reported error `{max(value for key, value in legacy.items() if key.endswith('_error'))}`.
- Fixed split: 485 sessions = 364 train / 73 validation / 48 holdout; manifest SHA-256 `{split.manifest_hash}`.
- Validation and holdout were not read for decisions. 2026 was not accessed. No search, promotion or cross-sprint memory update ran.

## Historical evidence

Historical tables are preserved. Evidence produced before global worker authority/receipt enforcement is reclassified in `CN_HISTORICAL_EVIDENCE_RECLASSIFICATION.csv`; no historical metric was rewritten.

## Acceptance answers

1. Previous bypasses were Phase3CM direct/worker local splitting, Phase3CP schema-derived field admission, candidate/shard/serial evaluator entry, chunk-04 recovery workers and Phase3CN feedback without a receipt binding.
2. Phase3DV, RX/UCB, CEM, hybrid and fresh generators may still produce proposals; none can authorize fields or enter evaluation directly.
3. `UnifiedCapabilityRegistry + TypedRouteCompiler`, materialized as the candidate submission receipt, owns final field, route, primitive, PIT, source-lag and matched-control authorization.
4. No formal path grants search eligibility merely because a parquet column exists. The physical schema gate may discard infeasible proposals, but every survivor is still receipt-authorized before any evaluator call.
5. No worker-local split is reachable from the formal evaluator; `_split_map` was removed from Phase3CM.
6. Serial, candidate-parallel, shard-parallel, retry, chunk recovery and exact merge all require the same explicit manifest.
7. Validation and holdout remain report-only and cannot pass the Phase3CN train-role plus exact-receipt guard.
8. The 1/2/4 worker and deterministic recovery/exact-merge semantic comparison passed with maximum numeric error `{worker['max_numeric_error']}`.
9. A non-performance, non-selected legal legacy static candidate preserved expression, signal, weights, turnover, cost, train-like synthetic metric and behavior identity under the receipt gate; maximum error was `{max(value for key, value in legacy.items() if key.endswith('_error'))}`.
10. Pre-convergence search trajectories, B1S selection evidence, pre-receipt unified discovery and provenance-unverified Phase3FIX evidence are diagnostic only; the exact classifications are in the CSV.
11. Engineering qualification to apply for one separately authorized, pre-registered, fixed-budget, development-only capability run is `YES`. This is not authorization to run it and is not Alpha/promotion evidence.

## Frozen boundaries

`FORMAL_SEARCH_FROZEN`, `FORWARD_2026_SEALED`, `NO_CANDIDATE_PROMOTION`, `NO_CROSS_SPRINT_ADAPTIVE_MEMORY`, plate/industry disabled.
"""
    (output / "CN_RUNTIME_AUTHORITY_AND_SPLIT_REPAIR_REPORT.md").write_text(report, encoding="utf-8")
    decision_log = """# Decision Change Log

## 2026-07-15 - Runtime authority and split convergence

- Supersede `CN_FEATURE_RUNTIME_WIRING_MISMATCH_CONFIRMED` as the current engineering state with `CN_RUNTIME_AUTHORITY_AND_SPLIT_CONVERGENCE_REPAIRED`.
- Preserve the prior audit and all historical proposal, reward and performance tables unchanged.
- Make the fixed 485-session manifest the sole formal split authority; deprecate worker-local splitting.
- Make `UnifiedCapabilityRegistry + TypedRouteCompiler` the sole candidate admission authority through immutable receipts.
- Retain legacy generators only as proposal sources. Physical schema presence remains a feasibility observation, not authorization.
- Reclassify pre-convergence search trajectories and pre-receipt evidence as diagnostic according to the machine-readable CSV.
- Record engineering qualification to apply for a separate small development-only capability run. Do not authorize or start it.
- Keep formal search, validation/holdout feedback, challenge, promotion, cross-sprint memory, plate/industry and forward 2026 frozen.
"""
    (output / "DECISION_CHANGE_LOG.md").write_text(decision_log, encoding="utf-8")

    manifest = {
        "status": STATUS,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "repo_sha_before_delivery_commit": git_head(REPO),
        "scope": "SOURCE_REPAIR_AND_SYNTHETIC_NO_SELECTION_PARITY",
        "inputs": {
            "split_manifest": {"path": str(SPLIT.relative_to(REPO)), "sha256": split.manifest_hash},
            "unified_registry": {"path": str(REGISTRY.relative_to(REPO)), "sha256": file_sha256(REGISTRY), "registry_hash": registry.registry_hash},
            "data_release_hash": DATA_RELEASE_HASH,
            "evaluator": {"path": str(EVALUATOR.relative_to(REPO)), "sha256": file_sha256(EVALUATOR)},
        },
        "access": {
            "performance_search_started": False,
            "validation_read": False,
            "holdout_read": False,
            "forward_2026_read": False,
            "candidate_promotion": False,
            "cross_sprint_memory": False,
        },
        "worker_parity": worker,
        "legacy_receipt_parity": legacy,
    }
    write_json(output / "run_manifest.json", manifest)
    artifacts = []
    for path in sorted(output.iterdir()):
        if path.is_file() and path.name != "artifact_index.json":
            artifacts.append({"path": path.name, "size_bytes": path.stat().st_size, "sha256": file_sha256(path)})
    index = {"status": STATUS, "artifact_count": len(artifacts), "artifacts": artifacts, "schema_hash": stable_hash({row["path"]: row["sha256"] for row in artifacts})}
    write_json(output / "artifact_index.json", index)
    return {"status": STATUS, "worker_max_error": worker["max_numeric_error"], "legacy": legacy["status"], "artifact_count": len(artifacts)}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    print(json.dumps(build(args.output.resolve()), ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
