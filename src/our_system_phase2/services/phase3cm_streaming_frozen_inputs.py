"""Fail-closed binding for the frozen Phase3CM streaming-repair inputs."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping


BOUND_STATUS = "CN_STREAMING_REPAIR_FROZEN_INPUT_BOUND"
DRIFT_STATUS = "CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT"


class FrozenInputDriftError(RuntimeError):
    """Raised when an immutable resource-preflight input no longer matches."""


def _fail(message: str) -> None:
    raise FrozenInputDriftError(f"{DRIFT_STATUS}: {message}")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _git_blob_hash(path: Path) -> str:
    payload = path.read_bytes()
    header = f"blob {len(payload)}\0".encode("ascii")
    return hashlib.sha1(header + payload).hexdigest()  # noqa: S324 - Git object identity


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _unique_by(rows: Iterable[Mapping[str, Any]], key: str, label: str) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        identity = str(row.get(key) or "")
        if not identity:
            _fail(f"{label} has an empty {key}")
        if identity in output:
            _fail(f"duplicate {label} {key}: {identity}")
        output[identity] = row
    return output


def _artifact_record(root: Path, expected: Mapping[str, Any]) -> dict[str, Any]:
    relative = str(expected.get("path") or "")
    path = root / relative
    if not path.is_file():
        _fail(f"missing frozen artifact: {relative}")
    content_sha = _sha256(path)
    expected_sha = str(expected.get("sha256") or "").lower()
    if content_sha != expected_sha:
        _fail(f"content hash drift for {relative}: {content_sha} != {expected_sha}")
    byte_count = path.stat().st_size
    if byte_count != int(expected.get("bytes") or -1):
        _fail(f"byte-count drift for {relative}: {byte_count} != {expected.get('bytes')}")
    rows = _read_csv(path) if path.suffix.lower() == ".csv" else _read_jsonl(path)
    return {
        "path": relative.replace("\\", "/"),
        "bytes": byte_count,
        "row_count": len(rows),
        "sha256": content_sha,
        "git_blob_hash": _git_blob_hash(path),
    }


def _stable_payload_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_frozen_input_binding(
    *,
    runtime_root: Path,
    freeze_manifest: Path,
    pack_path: Path,
    source_closure_sha: str,
) -> dict[str, Any]:
    """Validate and describe the exact 32-pair resource-preflight input pack."""

    root = runtime_root.resolve()
    manifest_path = freeze_manifest.resolve()
    actual_pack_path = pack_path.resolve()
    if len(str(source_closure_sha)) != 40:
        _fail("source closure SHA must be a full 40-character Git SHA")
    if not manifest_path.is_file() or not actual_pack_path.is_file():
        _fail("freeze manifest or pack is missing")

    freeze = json.loads(manifest_path.read_text(encoding="utf-8"))
    if str(freeze.get("status")) != "CN_COMPOSITIONAL_RESOURCE_PREFLIGHT_PACK_FROZEN":
        _fail("resource-preflight freeze status is not authoritative")
    if bool(freeze.get("selection_used_performance")):
        _fail("frozen pack was performance-selected")
    if bool(freeze.get("validation_holdout_forward_read")):
        _fail("frozen pack records forbidden data access")

    expected_artifacts = [dict(row) for row in freeze.get("artifacts", [])]
    artifact_by_name = {str(row.get("path")): row for row in expected_artifacts}
    if len(artifact_by_name) != 7:
        _fail("freeze manifest must bind exactly seven input artifacts")
    artifacts = [_artifact_record(root, row) for row in expected_artifacts]

    expected_pack = artifact_by_name.get(actual_pack_path.name)
    if expected_pack is None or _sha256(actual_pack_path) != str(expected_pack.get("sha256")):
        _fail("explicit pack path does not match the frozen pack artifact")

    pack = sorted(_read_csv(actual_pack_path), key=lambda row: int(row["preflight_ordinal"]))
    if [int(row["preflight_ordinal"]) for row in pack] != list(range(1, len(pack) + 1)):
        _fail("preflight candidate ordering is not contiguous and deterministic")
    if len(pack) != int(freeze.get("pair_count") or -1):
        _fail("pack pair count drift")
    _unique_by(pack, "pair_id", "pack pair")

    candidates: list[dict[str, Any]] = []
    candidate_receipts: list[dict[str, Any]] = []
    pair_receipts: list[dict[str, Any]] = []
    for namespace in ("active", "session"):
        candidates.extend(_read_csv(root / f"preflight_{namespace}_candidates.csv"))
        candidate_receipts.extend(_read_jsonl(root / f"preflight_{namespace}_candidate_receipts.jsonl"))
        pair_receipts.extend(_read_jsonl(root / f"preflight_{namespace}_pair_receipts.jsonl"))

    candidate_by_id = _unique_by(candidates, "candidate_id", "candidate")
    receipt_by_candidate = _unique_by(candidate_receipts, "candidate_id", "candidate receipt")
    pair_receipt_by_id = _unique_by(pair_receipts, "pair_id", "pair receipt")
    if len(candidate_by_id) != int(freeze.get("evaluator_call_count") or -1):
        _fail("candidate member count drift")
    if set(candidate_by_id) != set(receipt_by_candidate):
        _fail("candidate and candidate-receipt membership differ")

    bound_pairs: list[dict[str, Any]] = []
    bound_members: list[dict[str, Any]] = []
    clock_counts: dict[str, int] = {}
    for pack_row in pack:
        pair_id = str(pack_row["pair_id"])
        primary_id = str(pack_row["candidate_id"])
        control_id = str(pack_row["control_candidate_id"])
        receipt = pair_receipt_by_id.get(pair_id)
        if receipt is None:
            _fail(f"missing pair receipt: {pair_id}")
        if str(receipt.get("primary_candidate_id")) != primary_id:
            _fail(f"primary candidate drift for pair {pair_id}")
        if str(receipt.get("control_candidate_id")) != control_id:
            _fail(f"control candidate drift for pair {pair_id}")
        clock = str(pack_row["clock_namespace"])
        clock_counts[clock] = clock_counts.get(clock, 0) + 1

        for role, candidate_id in (("PRIMARY", primary_id), ("CONTROL", control_id)):
            candidate = candidate_by_id.get(candidate_id)
            candidate_receipt = receipt_by_candidate.get(candidate_id)
            if candidate is None or candidate_receipt is None:
                _fail(f"missing {role.lower()} member or receipt: {candidate_id}")
            if str(candidate.get("pair_id")) != pair_id or str(candidate_receipt.get("pair_id")) != pair_id:
                _fail(f"pair membership drift for candidate {candidate_id}")
            expression = str(candidate.get("expression") or "")
            if expression != str(candidate_receipt.get("expression") or ""):
                _fail(f"expression drift for candidate {candidate_id}")
            bound_members.append(
                {
                    "preflight_ordinal": int(pack_row["preflight_ordinal"]),
                    "pair_id": pair_id,
                    "pair_member_role": role,
                    "candidate_id": candidate_id,
                    "route_id": str(pack_row["route_id"]),
                    "clock_namespace": clock,
                    "expression": expression,
                    "canonical_expression": str(candidate_receipt.get("canonical_expression") or ""),
                    "candidate_receipt_id": str(candidate_receipt.get("receipt_id") or ""),
                    "receipt_hash": str(candidate_receipt.get("receipt_hash") or ""),
                    "field_ids": list(candidate_receipt.get("field_ids") or []),
                    "observable_time_contract": list(candidate_receipt.get("observable_time_contract") or []),
                    "pit_source_lag_contract": list(candidate_receipt.get("pit_source_lag_contract") or []),
                }
            )

        bound_pairs.append(
            {
                "preflight_ordinal": int(pack_row["preflight_ordinal"]),
                "pair_id": pair_id,
                "candidate_id": primary_id,
                "control_candidate_id": control_id,
                "route_id": str(pack_row["route_id"]),
                "clock_namespace": clock,
                "policy_id": str(pack_row["policy_id"]),
                "seed": int(pack_row["seed"]),
                "skeleton_id": str(pack_row["skeleton_id"]),
                "primary_expression": str(candidate_by_id[primary_id]["expression"]),
                "control_expression": str(candidate_by_id[control_id]["expression"]),
                "pair_receipt_id": str(receipt.get("pair_receipt_id") or ""),
                "pair_receipt_hash": str(receipt.get("pair_receipt_hash") or ""),
                "support_unit": str(receipt.get("support_unit") or ""),
                "mapping_portfolio_contract": str(receipt.get("mapping_portfolio_contract") or ""),
            }
        )

    if clock_counts != {str(k): int(v) for k, v in dict(freeze.get("clock_counts") or {}).items()}:
        _fail("clock namespace counts drift")
    if set(pair_receipt_by_id) != {str(row["pair_id"]) for row in pack}:
        _fail("pair receipt table contains missing or extra pairs")

    binding: dict[str, Any] = {
        "status": BOUND_STATUS,
        "source_closure_sha": str(source_closure_sha),
        "pack_identity": str(freeze["pack_identity"]),
        "data_role": str(freeze["data_role"]),
        "development_release_hash": str(freeze["release_hash"]),
        "development_release_rows": int(freeze["release_rows"]),
        "split_manifest_hash": str(freeze["split_manifest_hash"]),
        "registry_hash": str(freeze["registry_hash"]),
        "pair_count": len(bound_pairs),
        "candidate_member_count": len(bound_members),
        "clock_counts": clock_counts,
        "artifacts": artifacts,
        "pairs": bound_pairs,
        "candidate_members": bound_members,
        "sealed_reads": {"validation": 0, "holdout": 0, "forward_2026": 0},
        "promotion": "FORBIDDEN",
        "cross_sprint_memory": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
    }
    binding["binding_hash"] = _stable_payload_hash(binding)
    return binding

