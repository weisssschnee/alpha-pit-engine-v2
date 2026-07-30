"""Independently verify a frozen zero-financial finalist input binding."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping


def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _payload_sha256(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _verify_payload_hash(
    payload: Mapping[str, Any],
    field: str,
) -> None:
    expected = str(payload.get(field) or "")
    candidate = dict(payload)
    candidate.pop(field, None)
    observed = _payload_sha256(candidate)
    if expected != observed:
        raise ValueError(
            f"{field} mismatch: declared={expected} observed={observed}"
        )


def verify(
    *,
    output_root: Path,
    verification_root: Path,
) -> dict[str, Any]:
    output_root = output_root.resolve()
    verification_root = verification_root.resolve()
    verification_root.mkdir(parents=True, exist_ok=False)
    manifest_path = output_root / "finalist_input_authority_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    _verify_payload_hash(manifest, "manifest_payload_sha256")
    artifact_count = 0
    for artifact in manifest.get("artifacts") or []:
        path = output_root / str(artifact["path"])
        if not path.is_file():
            raise FileNotFoundError(path)
        if path.stat().st_size != int(artifact["bytes"]):
            raise ValueError(f"artifact size mismatch: {path}")
        if _sha256(path) != str(artifact["sha256"]):
            raise ValueError(f"artifact hash mismatch: {path}")
        artifact_count += 1
    if artifact_count != 2:
        raise ValueError(f"expected 2 declared artifacts, got {artifact_count}")
    input_manifests = list(manifest.get("input_manifests") or [])
    if len(input_manifests) not in {2, 3}:
        raise ValueError(
            "expected 2 legacy or 3 session-bound frozen input manifests, "
            f"got {len(input_manifests)}"
        )
    for source in input_manifests:
        path = Path(str(source["path"])).resolve()
        if not path.is_file():
            raise FileNotFoundError(path)
        if _sha256(path) != str(source["sha256"]):
            raise ValueError(f"input manifest hash mismatch: {path}")
    for field in ("validation_reads", "holdout_reads", "forward_2026_reads"):
        if int(manifest.get(field, -1)) != 0:
            raise ValueError(f"root manifest {field} must remain zero")
    for field in (
        "financial_replay_authorized",
        "report_only_validation_authorized",
        "financial_result_recomputed",
    ):
        if bool(manifest.get(field)):
            raise ValueError(f"root manifest {field} must remain false")

    binding_path = output_root / "finalist_input_authority_binding.json"
    binding = json.loads(binding_path.read_text(encoding="utf-8"))
    _verify_payload_hash(binding, "binding_payload_sha256")
    if (
        str(binding["binding_payload_sha256"])
        != str(manifest["binding_payload_sha256"])
    ):
        raise ValueError("binding payload hash differs from root manifest")
    if binding["status"] != manifest["status"]:
        raise ValueError("binding/root status mismatch")
    if bool(binding.get("new_authority_node_created")):
        raise ValueError("binding illegally declares a new authority node")
    zero_fields = (
        "optimizer_feedback_writes",
        "scheduler_writes",
        "archive_writes",
        "validation_reads",
        "holdout_reads",
        "forward_2026_reads",
    )
    for field in zero_fields:
        if int(binding.get(field, -1)) != 0:
            raise ValueError(f"{field} must remain zero")
    for field in (
        "financial_replay_authorized",
        "report_only_validation_authorized",
        "promotion_authorized",
        "successor_search_authorized",
        "financial_result_recomputed",
    ):
        if bool(binding.get(field)):
            raise ValueError(f"{field} must remain false")
    blockers = list(binding.get("blockers") or [])
    status = str(binding["status"])
    if status == "FINALIST_INPUT_AUTHORITY_READY" and blockers:
        raise ValueError("READY binding carries blockers")
    if status == "FINALIST_INPUT_AUTHORITY_READY":
        if len(input_manifests) != 3:
            raise ValueError(
                "READY binding requires cohort, release, and session "
                "authority manifests"
            )
        session = binding.get("session_authority")
        if not isinstance(session, dict):
            raise ValueError("READY binding lacks session authority")
        if int(session.get("security_count") or 0) <= 0:
            raise ValueError("READY session authority security count is invalid")
        if int(session.get("session_row_count") or 0) <= 0:
            raise ValueError("READY session authority row count is invalid")
    if status == "HOLD_RESEARCH_FINALIST_INPUTS_INCOMPLETE" and not blockers:
        raise ValueError("HOLD binding has no blocker")
    if status not in {
        "FINALIST_INPUT_AUTHORITY_READY",
        "HOLD_RESEARCH_FINALIST_INPUTS_INCOMPLETE",
    }:
        raise ValueError(f"unsupported binding status: {status}")

    receipt = {
        "schema_version": "cn_finalist_input_authority_verification_v1",
        "status": "INDEPENDENT_VERIFICATION_PASS",
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_status": status,
        "source_manifest": str(manifest_path),
        "source_manifest_file_sha256": _sha256(manifest_path),
        "source_manifest_payload_sha256": manifest[
            "manifest_payload_sha256"
        ],
        "binding_payload_sha256": binding["binding_payload_sha256"],
        "declared_artifacts_verified": artifact_count,
        "input_manifests_verified": len(input_manifests),
        "blocker_count": len(blockers),
        "blockers": blockers,
        "financial_replay_authorized": False,
        "report_only_validation_authorized": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "financial_result_recomputed": False,
    }
    receipt["verification_payload_sha256"] = _payload_sha256(receipt)
    receipt_path = verification_root / "verification_receipt.json"
    receipt_path.write_text(
        json.dumps(
            receipt,
            ensure_ascii=False,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    result = {
        "status": receipt["status"],
        "source_status": status,
        "verification_receipt": str(receipt_path),
        "verification_receipt_sha256": _sha256(receipt_path),
        "verification_payload_sha256": receipt[
            "verification_payload_sha256"
        ],
        "blocker_count": len(blockers),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--verification-root", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    verify(
        output_root=args.output_root,
        verification_root=args.verification_root,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
