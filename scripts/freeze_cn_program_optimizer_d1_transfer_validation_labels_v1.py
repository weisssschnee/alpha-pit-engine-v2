"""Freeze per-exact labels from an already-completed D1 transfer validation run.

The source validation has already consumed its authorized validation evidence.
This script performs no financial evaluation: it reads only immutable result
records and the candidate freeze/members, verifies their hashes and exact set,
and writes a compact durable label artifact suitable for later reproducibility
audits.  Holdout and Forward-2026 access remain forbidden.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from our_system_phase2.services.unified_capability_registry import stable_hash

TRANSFER_CLOSURE = "CN_PROGRAM_OPTIMIZER_D1_TRANSFER_PROSPECTIVE_VALIDATION_COMPLETE.json"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify_self_hash(payload: Mapping[str, Any], field: str, label: str) -> None:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")


def freeze_labels(
    *,
    formal_root: Path,
    candidate_freeze_path: Path,
    candidate_members_path: Path,
    output_path: Path,
    closure_name: str = TRANSFER_CLOSURE,
) -> dict[str, Any]:
    root = formal_root.resolve()
    freeze_path = candidate_freeze_path.resolve()
    members_path = candidate_members_path.resolve()
    closure_path = root / closure_name
    records_root = root / "records"
    if not closure_path.is_file():
        raise FileNotFoundError(closure_path)
    if not records_root.is_dir():
        raise FileNotFoundError(records_root)

    freeze = _read_json(freeze_path)
    members = _read_jsonl(members_path)
    closure = _read_json(closure_path)
    _verify_self_hash(freeze, "freeze_payload_sha256", "candidate freeze")
    _verify_self_hash(closure, "closure_payload_sha256", "validation closure")

    candidate_count = int(freeze.get("candidate_count") or 0)
    if candidate_count <= 0 or len(members) != candidate_count:
        raise RuntimeError("candidate freeze/member cardinality drift")
    if stable_hash(members) != str(freeze.get("candidate_members_payload_sha256") or ""):
        raise RuntimeError("candidate members payload drift")
    member_ids = sorted(str(row.get("exact_identity") or "") for row in members)
    if any(not exact for exact in member_ids) or len(set(member_ids)) != len(member_ids):
        raise RuntimeError("candidate member exact identity drift")
    if stable_hash(member_ids) != str(freeze.get("candidate_exact_identities_sha256") or ""):
        raise RuntimeError("candidate exact identity set drift")
    if int(closure.get("candidate_count") or 0) != candidate_count:
        raise RuntimeError("validation closure candidate count drift")
    if str(closure.get("candidate_exact_identities_sha256") or "") != str(freeze.get("candidate_exact_identities_sha256") or ""):
        raise RuntimeError("validation closure candidate identity drift")
    if int(closure.get("holdout_reads") or 0) != 0 or int(closure.get("forward_2026_reads") or 0) != 0:
        raise RuntimeError("validation closure restricted-read drift")
    if bool(closure.get("promotion_authorized")):
        raise RuntimeError("validation closure unexpectedly authorizes promotion")

    label_rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for path in sorted(records_root.glob("candidate_*.json")):
        wrapper = _read_json(path)
        result = dict(wrapper.get("validation_result") or {})
        if not result:
            raise RuntimeError(f"missing validation_result in {path}")
        _verify_self_hash(result, "result_payload_sha256", f"validation result {path.name}")
        exact = str(result.get("exact_identity") or "")
        if exact not in set(member_ids) or exact in seen:
            raise RuntimeError(f"validation result exact identity drift: {exact}")
        seen.add(exact)
        if int(result.get("holdout_reads") or 0) != 0 or int(result.get("forward_2026_reads") or 0) != 0:
            raise RuntimeError(f"validation result restricted-read drift: {exact}")
        admission = dict(result.get("admission") or {})
        label_rows.append({
            "exact_identity": exact,
            "source_cohort": str(result.get("source_cohort") or ""),
            "validation_productive": bool(result.get("validation_productive")),
            "validation_admitted": bool(admission.get("admitted")),
            "validation_result_payload_sha256": str(result["result_payload_sha256"]),
        })

    label_rows.sort(key=lambda row: str(row["exact_identity"]))
    if len(label_rows) != candidate_count or seen != set(member_ids):
        missing = sorted(set(member_ids) - seen)
        extra = sorted(seen - set(member_ids))
        raise RuntimeError(f"validation label coverage drift: missing={missing[:3]} extra={extra[:3]}")

    positive_count = sum(bool(row["validation_productive"]) for row in label_rows)
    admitted_count = sum(bool(row["validation_admitted"]) for row in label_rows)
    payload: dict[str, Any] = {
        "schema_version": "cn_program_optimizer_d1_transfer_validation_label_evidence_v1",
        "status": "FROZEN_DURABLE_REPORT_ONLY_VALIDATION_LABEL_EVIDENCE",
        "source_formal_root": str(root),
        "source_validation_closure_name": closure_name,
        "source_validation_closure_file_sha256": _sha256(closure_path),
        "source_validation_closure_payload_sha256": str(closure["closure_payload_sha256"]),
        "source_candidate_freeze_file_sha256": _sha256(freeze_path),
        "source_candidate_freeze_payload_sha256": str(freeze["freeze_payload_sha256"]),
        "source_candidate_members_file_sha256": _sha256(members_path),
        "source_candidate_members_payload_sha256": str(freeze["candidate_members_payload_sha256"]),
        "candidate_count": candidate_count,
        "candidate_exact_identities_sha256": str(freeze["candidate_exact_identities_sha256"]),
        "validation_productive_count": positive_count,
        "validation_admitted_count": admitted_count,
        "label_rows": label_rows,
        "label_rows_sha256": stable_hash(label_rows),
        "evidence_usage": "REPRODUCIBILITY_AND_REPORT_ONLY_ANALYSIS_ONLY",
        "optimizer_feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion_authorized": False,
        "financial_evaluation_performed_by_this_freeze": False,
        "holdout_reads_by_this_freeze": 0,
        "forward_2026_reads_by_this_freeze": 0,
    }
    payload["evidence_payload_sha256"] = stable_hash(payload)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--formal-root", type=Path, required=True)
    parser.add_argument("--candidate-freeze", type=Path, required=True)
    parser.add_argument("--candidate-members", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--closure-name", default=TRANSFER_CLOSURE)
    args = parser.parse_args(argv)
    payload = freeze_labels(
        formal_root=args.formal_root,
        candidate_freeze_path=args.candidate_freeze,
        candidate_members_path=args.candidate_members,
        output_path=args.output,
        closure_name=args.closure_name,
    )
    print(json.dumps({
        "status": payload["status"],
        "candidate_count": payload["candidate_count"],
        "validation_productive_count": payload["validation_productive_count"],
        "label_rows_sha256": payload["label_rows_sha256"],
        "evidence_payload_sha256": payload["evidence_payload_sha256"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
