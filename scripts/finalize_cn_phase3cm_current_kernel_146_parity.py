from __future__ import annotations

import argparse
import hashlib
import json
import os
import uuid
from pathlib import Path
from typing import Any, Mapping, Sequence


PARTITION_COUNT = 2
PAIR_COUNT = 146
CANDIDATE_COUNT = 292
RESULT_FILE = "CN_STREAMING_BACKEND_RESULT.json"
CHECKPOINT_CATEGORIES = ("temporal", "state", "support", "portfolio", "reducer")
ACCESS_FIELDS = ("validation_reads", "holdout_reads", "forward_2026_reads")
COMPARABILITY_KEYS = ("candidate_identity", "pair_identity", "required_artifacts_present")
SEMANTIC_KEYS = (
    "result:candidate_rewards",
    "result:pair_results",
    "result:reward_atoms",
    "result:support_identities",
    "artifact:CN_STREAMING_REWARD_ATOMS.csv",
    "artifact:CN_STREAMING_REDUCER_CONTRACT.json",
)


class EvidenceError(ValueError):
    pass


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise EvidenceError(f"JSON object required: {path}")
    return value


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, destination)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _object(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise EvidenceError(f"{label} must be an object")
    return value


def _text(value: Any, label: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise EvidenceError(f"{label} is missing")
    return normalized


def _ids(value: Any, label: str) -> list[str]:
    if not isinstance(value, list):
        raise EvidenceError(f"{label} must be an array")
    rows = [_text(item, label) for item in value]
    if len(rows) != len(set(rows)):
        raise EvidenceError(f"{label} contains duplicates")
    return sorted(rows)


def _access(value: Mapping[str, Any], label: str, *, promotion: bool) -> None:
    for field in ACCESS_FIELDS:
        if value.get(field) != 0:
            raise EvidenceError(f"{label} does not prove {field}=0")
    role = value.get("data_role")
    if role is not None and role not in {"development", "development_train_only"}:
        raise EvidenceError(f"{label} is not development-only")
    if promotion:
        if value.get("promotion") != "FORBIDDEN":
            raise EvidenceError(f"{label} does not forbid promotion")
        if value.get("strict_stage_a") != "NOT_AUTHORIZED":
            raise EvidenceError(f"{label} authorizes strict Stage A")


def _partitions(contract: Mapping[str, Any], contract_path: Path) -> dict[str, dict[str, Any]]:
    raw = contract.get("partitions")
    if not isinstance(raw, list) or len(raw) != PARTITION_COUNT:
        raise EvidenceError("contract must contain exactly two partitions")
    output: dict[str, dict[str, Any]] = {}
    for ordinal, item in enumerate(raw):
        row = _object(item, f"partitions[{ordinal}]")
        partition_id = _text(row.get("partition_id"), "partition_id")
        if partition_id in output:
            raise EvidenceError(f"duplicate partition_id: {partition_id}")
        pair_source = row.get("expected_pair_ids", row.get("pair_ids"))
        candidate_source = row.get("expected_candidate_ids", row.get("candidate_ids"))
        pairs = _ids(pair_source, f"{partition_id} pair_ids")
        candidates = _ids(candidate_source, f"{partition_id} candidate_ids")
        if not pairs or len(candidates) != 2 * len(pairs):
            raise EvidenceError(f"{partition_id} does not bind two candidates per pair")
        backend = _text(row.get("backend") or "active_bar", "backend")
        output[partition_id] = {
            "pair_ids": pairs,
            "candidate_ids": candidates,
            "backend": backend,
            "route_cohort": str(row.get("route_cohort") or f"{backend}:{len(pairs)}pairs"),
            "checkpoint_required_fields": list(
                row.get("checkpoint_required_fields")
                or contract.get("checkpoint_required_fields")
                or []
            ),
        }
    all_pairs = [value for row in output.values() for value in row["pair_ids"]]
    all_candidates = [value for row in output.values() for value in row["candidate_ids"]]
    binding = _object(contract.get("historical_input_binding") or {}, "historical_input_binding")
    declared_pair_count = contract.get("pair_count", binding.get("expected_pair_count"))
    declared_candidate_count = contract.get(
        "candidate_count", binding.get("expected_candidate_count")
    )
    if int(declared_pair_count or -1) != PAIR_COUNT or len(all_pairs) != PAIR_COUNT:
        raise EvidenceError("contract union must contain exactly 146 pairs")
    if int(declared_candidate_count or -1) != CANDIDATE_COUNT or len(all_candidates) != CANDIDATE_COUNT:
        raise EvidenceError("contract union must contain exactly 292 candidates")
    if len(set(all_pairs)) != PAIR_COUNT or len(set(all_candidates)) != CANDIDATE_COUNT:
        raise EvidenceError("contract partitions overlap")
    if binding.get("expected_pair_ids") is not None and _ids(
        binding["expected_pair_ids"], "expected_pair_ids"
    ) != sorted(all_pairs):
        raise EvidenceError("partition pair union drifts from historical_input_binding")
    if binding.get("expected_candidate_ids") is not None and _ids(
        binding["expected_candidate_ids"], "expected_candidate_ids"
    ) != sorted(all_candidates):
        raise EvidenceError("partition candidate union drifts from historical_input_binding")
    return output


def _result_evidence(result: Mapping[str, Any], partition: Mapping[str, Any]) -> dict[str, Any]:
    _access(result, "candidate backend result", promotion=True)
    if result.get("backend") != partition["backend"]:
        raise EvidenceError("candidate backend drift")
    pair_rows = result.get("pair_results")
    reward_rows = result.get("candidate_rewards")
    if not isinstance(pair_rows, list) or not isinstance(reward_rows, list):
        raise EvidenceError("candidate result lacks pair_results/candidate_rewards")
    members: list[dict[str, str]] = []
    behavior: list[dict[str, str]] = []
    support: list[dict[str, Any]] = []
    blocker_reward: list[dict[str, Any]] = []
    for raw in pair_rows:
        row = _object(raw, "pair result")
        pair_id = _text(row.get("pair_id"), "pair_id")
        primary = _text(row.get("primary_candidate_id"), "primary_candidate_id")
        control = _text(row.get("control_candidate_id"), "control_candidate_id")
        if row.get("streaming_identity_schema") != "block_composable_v1":
            raise EvidenceError(f"behavior digest schema drift: {pair_id}")
        required = (
            "pair_support_identity",
            "pair_support_count",
            "pair_support_overlap",
            "primary_behavior_identity",
            "control_behavior_identity",
            "pair_evaluation_status",
            "pair_evaluation_blockers",
            "primary_train_reward",
            "control_train_reward",
            "pair_train_reward",
        )
        missing = [field for field in required if field not in row]
        if missing:
            raise EvidenceError(f"pair {pair_id} lacks evidence: {missing}")
        members.append({"pair_id": pair_id, "primary": primary, "control": control})
        behavior.append(
            {
                "pair_id": pair_id,
                "primary": str(row["primary_behavior_identity"]),
                "control": str(row["control_behavior_identity"]),
            }
        )
        support.append(
            {
                "pair_id": pair_id,
                "identity": str(row["pair_support_identity"]),
                "count": row["pair_support_count"],
                "overlap": row["pair_support_overlap"],
            }
        )
        blocker_reward.append(
            {
                "pair_id": pair_id,
                "status": row["pair_evaluation_status"],
                "blockers": row["pair_evaluation_blockers"],
                "rewards": [
                    row["primary_train_reward"],
                    row["control_train_reward"],
                    row["pair_train_reward"],
                ],
            }
        )
    pair_ids = sorted(row["pair_id"] for row in members)
    candidate_ids = sorted(
        _text(_object(row, "candidate reward").get("candidate_id"), "candidate_id")
        for row in reward_rows
    )
    if len(pair_ids) != len(set(pair_ids)) or len(candidate_ids) != len(set(candidate_ids)):
        raise EvidenceError("candidate result contains duplicate identities")
    pair_members = {value for row in members for value in (row["primary"], row["control"])}
    if (
        pair_ids != partition["pair_ids"]
        or candidate_ids != partition["candidate_ids"]
        or pair_members != set(candidate_ids)
    ):
        raise EvidenceError("candidate result identity differs from frozen partition")
    return {
        "pair_ids": pair_ids,
        "candidate_ids": candidate_ids,
        "pair_membership_digest": _digest(sorted(members, key=lambda row: row["pair_id"])),
        "behavior_digest": _digest(sorted(behavior, key=lambda row: row["pair_id"])),
        "support_digest": _digest(sorted(support, key=lambda row: row["pair_id"])),
        "blocker_reward_digest": _digest(sorted(blocker_reward, key=lambda row: row["pair_id"])),
    }


def _qualification(path: Path, partition: Mapping[str, Any]) -> dict[str, Any]:
    receipt = _read_json(path)
    _access(receipt, "qualification receipt", promotion=True)
    if receipt.get("comparable") is not True or receipt.get("semantic_parity_exact") is not True:
        raise EvidenceError("qualification is not comparable with exact semantic parity")
    comparability = _object(receipt.get("comparability"), "comparability")
    semantic = _object(receipt.get("semantic_parity"), "semantic_parity")
    failed = [key for key in COMPARABILITY_KEYS if comparability.get(key) is not True]
    failed += [key for key in SEMANTIC_KEYS if semantic.get(key) is not True]
    if failed:
        raise EvidenceError(f"qualification lacks exact evidence: {failed}")
    candidate_root = Path(_text(receipt.get("candidate_root"), "candidate_root"))
    evidence = _result_evidence(_read_json(candidate_root / RESULT_FILE), partition)
    artifact_hashes = _object(receipt.get("candidate_artifact_hashes"), "candidate_artifact_hashes")
    for name in ("CN_STREAMING_REWARD_ATOMS.csv", "CN_STREAMING_REDUCER_CONTRACT.json"):
        _text(artifact_hashes.get(name), f"candidate artifact hash {name}")
    return {
        "receipt": str(path.resolve()),
        "receipt_sha256": _sha256(path),
        "status_observed_not_gated": receipt.get("status"),
        "performance_observed_not_gated": receipt.get("performance"),
        "comparable": True,
        "semantic_parity_exact": True,
        "identity_evidence": evidence,
        "candidate_artifact_hashes": dict(artifact_hashes),
    }


def _checkpoint(path: Path, partition: Mapping[str, Any]) -> dict[str, Any]:
    receipt = _read_json(path)
    _access(receipt, "checkpoint receipt", promotion=False)
    if receipt.get("status") != "CN_PHASE3CM_SCALING_PROBE_PARITY_PASS":
        raise EvidenceError("checkpoint receipt is not a parity pass")
    if receipt.get("backend") != partition["backend"]:
        raise EvidenceError("checkpoint backend drift")
    parity = _object(receipt.get("parity"), "checkpoint parity")
    for category in CHECKPOINT_CATEGORIES:
        row = _object(parity.get(category), f"checkpoint {category}")
        if row.get("exact") is not True or list(row.get("mismatches") or []):
            raise EvidenceError(f"checkpoint {category} is not exact")
    declared = _object(receipt.get("contract_parity") or {}, "checkpoint contract_parity")
    checked: dict[str, bool] = {"backend": True}
    required = list(partition["checkpoint_required_fields"])
    for field in sorted(set(required) | set(declared)):
        row = _object(declared.get(field), f"checkpoint contract {field}")
        exact = row.get("exact") is True and not list(row.get("mismatches") or [])
        if not exact:
            raise EvidenceError(f"checkpoint contract {field} is not exact")
        checked[field] = True
    return {
        "receipt": str(path.resolve()),
        "receipt_sha256": _sha256(path),
        "continuation_parity": {name: True for name in CHECKPOINT_CATEGORIES},
        "contract_parity": checked,
        "required_contract_fields": required,
        "performance_observed_not_gated": {
            "mapping_speedup": receipt.get("mapping_speedup"),
            "compute_speedup": receipt.get("compute_speedup"),
        },
    }


def finalize_replay(
    *, contract_path: Path, qualification_paths: Mapping[str, Path], checkpoint_paths: Mapping[str, Path]
) -> dict[str, Any]:
    contract_path = Path(contract_path).resolve()
    errors: list[str] = []
    rows: list[dict[str, Any]] = []
    try:
        contract = _read_json(contract_path)
        partitions = _partitions(contract, contract_path)
    except (OSError, ValueError, TypeError) as exc:
        partitions = {}
        errors.append(str(exc))
    if partitions and set(qualification_paths) != set(partitions):
        errors.append("qualification receipts do not map exactly to frozen partitions")
    if partitions and set(checkpoint_paths) != set(partitions):
        errors.append("checkpoint receipts do not map exactly to frozen partitions")
    if partitions and not errors:
        for partition_id, partition in partitions.items():
            try:
                qualification = _qualification(Path(qualification_paths[partition_id]), partition)
                checkpoint = _checkpoint(Path(checkpoint_paths[partition_id]), partition)
                rows.append(
                    {
                        "partition_id": partition_id,
                        "status": "EXACT_PARITY_PASS",
                        "pair_count": len(partition["pair_ids"]),
                        "candidate_count": len(partition["candidate_ids"]),
                        "qualification": qualification,
                        "checkpoint": checkpoint,
                    }
                )
            except (OSError, ValueError, TypeError) as exc:
                errors.append(f"{partition_id}: {exc}")
                rows.append({"partition_id": partition_id, "status": "FAIL_CLOSED", "error": str(exc)})
    observed_pairs = [
        value
        for row in rows
        if row["status"] == "EXACT_PARITY_PASS"
        for value in row["qualification"]["identity_evidence"]["pair_ids"]
    ]
    observed_candidates = [
        value
        for row in rows
        if row["status"] == "EXACT_PARITY_PASS"
        for value in row["qualification"]["identity_evidence"]["candidate_ids"]
    ]
    identity_exact = (
        len(observed_pairs) == len(set(observed_pairs)) == PAIR_COUNT
        and len(observed_candidates)
        == len(set(observed_candidates))
        == CANDIDATE_COUNT
    )
    if not identity_exact and not errors:
        errors.append("observed union is not exactly 146 pairs / 292 candidates")
    passed = not errors and len(rows) == PARTITION_COUNT and identity_exact
    return {
        "schema_version": "cn_phase3cm_current_kernel_146_parity_final_v1",
        "status": (
            "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_PASS"
            if passed
            else "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_FAIL_CLOSED"
        ),
        "contract": str(contract_path),
        "contract_sha256": _sha256(contract_path) if contract_path.is_file() else None,
        "kernel_state": "PARTIALLY_QUALIFIED",
        "backend_authority": "EXPERIMENTAL_BACKEND",
        "formal_evaluator_authority": "UNCHANGED",
        "partitions": rows,
        "identity_coverage": {
            "expected_pair_count": PAIR_COUNT,
            "observed_pair_count": len(set(observed_pairs)),
            "expected_candidate_count": CANDIDATE_COUNT,
            "observed_candidate_count": len(set(observed_candidates)),
            "pair_union_digest": _digest(sorted(observed_pairs)),
            "candidate_union_digest": _digest(sorted(observed_candidates)),
        },
        "identity_coverage_exact": identity_exact,
        "semantic_evidence_coverage": {
            "candidate_identity": "frozen IDs and pair membership digest",
            "support": "support identity/count/overlap digest plus exact reward atoms",
            "rank_and_weights": (
                "block_composable_v1 selected-bitmap and mapping-metrics behavior "
                "digest; no raw coordinates retained"
            ),
            "turnover_cost_reward_rankic": "byte-exact reward atoms plus exact candidate/pair result fields",
            "blockers": "exact pair blocker/status/reward digest",
            "checkpoint": {
                "continuation_categories": list(CHECKPOINT_CATEGORIES),
                "contract_fields": (
                    sorted(
                        {
                            field
                            for row in partitions.values()
                            for field in row["checkpoint_required_fields"]
                        }
                    )
                    if partitions
                    else []
                ),
            },
        },
        "boundaries": {
            "data_role": "development_train_only",
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "promotion": "FORBIDDEN",
            "strict_stage_a": "NOT_AUTHORIZED",
        },
        "performance_threshold_gate": "NOT_USED",
        "two_x_speedup_required": False,
        "errors": errors,
        "next_decision": (
            "FREEZE_1024_RESOURCE_AND_EXECUTION_CONTRACT"
            if passed
            else "STOP_BEFORE_1024_ROUTE_ASYMMETRIC_CONFIRMATION"
        ),
    }


def _paths(values: Sequence[str], label: str) -> dict[str, Path]:
    if len(values) != PARTITION_COUNT:
        raise EvidenceError(f"{label} must be supplied exactly twice")
    output: dict[str, Path] = {}
    for value in values:
        partition_id, separator, path = value.partition("=")
        if not separator or not partition_id.strip() or not path.strip() or partition_id in output:
            raise EvidenceError(f"{label} must use two unique PARTITION_ID=PATH values")
        output[partition_id.strip()] = Path(path.strip())
    return output


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Finalize frozen current-kernel 146-pair semantic parity.")
    parser.add_argument("--contract", type=Path, required=True)
    parser.add_argument("--qualification", action="append", default=[], metavar="PARTITION_ID=PATH")
    parser.add_argument("--checkpoint", action="append", default=[], metavar="PARTITION_ID=PATH")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        result = finalize_replay(
            contract_path=args.contract,
            qualification_paths=_paths(args.qualification, "--qualification"),
            checkpoint_paths=_paths(args.checkpoint, "--checkpoint"),
        )
    except (OSError, ValueError, TypeError) as exc:
        result = {
            "schema_version": "cn_phase3cm_current_kernel_146_parity_final_v1",
            "status": "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_FAIL_CLOSED",
            "errors": [str(exc)],
            "next_decision": "STOP_BEFORE_1024_ROUTE_ASYMMETRIC_CONFIRMATION",
            "performance_threshold_gate": "NOT_USED",
        }
    _write_json_atomic(args.output.resolve(), result)
    print(
        json.dumps(
            {
                "status": result["status"],
                "identity_coverage_exact": result.get("identity_coverage_exact", False),
                "next_decision": result["next_decision"],
            },
            sort_keys=True,
        )
    )
    return 0 if result["status"] == "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
