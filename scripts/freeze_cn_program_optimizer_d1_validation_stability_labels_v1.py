"""Recover durable D1 validation stability labels from immutable consumed results.

This is a no-financial-read evidence recovery.  It reads only the two already-
consumed D1 validation formal roots, verifies closure/result/pair self-hashes
against durable Git authority, and freezes per-exact productive/stable labels.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from our_system_phase2.services.unified_capability_registry import stable_hash

STATUS = "D1_VALIDATION_STABILITY_LABELS_RECOVERED_FROM_IMMUTABLE_CONSUMED_RESULTS"


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def _recover_root(
    root: Path,
    *,
    expected_closure_payload_sha256: str,
    expected_count: int,
    cohort_label: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    root = root.resolve()
    closure_candidates = [path for path in root.glob("*_COMPLETE.json") if path.is_file()]
    if len(closure_candidates) != 1:
        raise RuntimeError(f"{cohort_label} closure cardinality drift")
    closure_path = closure_candidates[0]
    closure = _read(closure_path)
    closure_hash = _verify(closure, "closure_payload_sha256", f"{cohort_label} closure")
    if closure_hash != str(expected_closure_payload_sha256):
        raise RuntimeError(f"{cohort_label} closure authority drift")
    record_root = root / "records"
    record_files = sorted(record_root.glob("candidate_*.json"))
    if len(record_files) != int(expected_count):
        raise RuntimeError(f"{cohort_label} record count drift")
    rows: list[dict[str, Any]] = []
    for path in record_files:
        payload = _read(path)
        pair = dict(payload["pair_record"])
        result = dict(payload["validation_result"])
        pair_hash = _verify(pair, "record_payload_sha256", f"{cohort_label} pair")
        result_hash = _verify(result, "result_payload_sha256", f"{cohort_label} result")
        if str(result.get("pair_record_sha256") or "") != pair_hash:
            raise RuntimeError(f"{cohort_label} result/pair binding drift")
        uplift = result.get("uplift")
        credit = dict(dict(uplift or {}).get("program_credit") or {})
        positive_windows = int(credit.get("cross_window_positive_increment_count") or 0)
        productive = bool(result["validation_productive"])
        window_increments = list(map(float, credit.get("window_return_increments") or ()))
        if uplift is not None and len(window_increments) != 3:
            raise RuntimeError(f"{cohort_label} validation window increment drift")
        row = {
            "schema_version": "cn_program_optimizer_d1_validation_stability_label_v1",
            "exact_identity": str(result["exact_identity"]),
            "source_cohort": str(result["source_cohort"]),
            "template_id": str(result["template_id"]),
            "selection_kind": str(result["selection_kind"]),
            "validation_admitted": bool(dict(result["admission"])["admitted"]),
            "validation_productive": productive,
            "cross_window_positive_increment_count": positive_windows,
            "validation_stable_2of3": productive and positive_windows >= 2,
            "validation_stable_3of3": productive and positive_windows >= 3,
            "validation_window_return_increments": window_increments,
            "source_result_payload_sha256": result_hash,
            "source_pair_record_payload_sha256": pair_hash,
            "source_record_file_sha256": _sha(path),
        }
        row["label_row_sha256"] = stable_hash(row)
        rows.append(row)
    exacts = [str(row["exact_identity"]) for row in rows]
    if len(set(exacts)) != int(expected_count):
        raise RuntimeError(f"{cohort_label} exact identity duplicate")
    source = {
        "source_root": str(root),
        "closure_relative_name": closure_path.name,
        "closure_file_sha256": _sha(closure_path),
        "closure_payload_sha256": closure_hash,
        "record_count": len(rows),
        "exact_identities_sha256": stable_hash(sorted(exacts)),
    }
    return source, rows


def freeze(repo: Path, *, old120_root: Path, b63_root: Path) -> dict[str, Any]:
    repo = repo.resolve()
    rp = repo / "runtime/run_plans"
    old_outcome = _read(rp / "cn_program_optimizer_d1_validation_outcome_20260817.json")
    old_survivors = _read(rp / "cn_program_optimizer_d1_validation_survivor_freeze_20260817.json")
    b_labels = _read(rp / "cn_program_optimizer_d1_transfer_v2_recovery_20260817/B63_validation_labels.json")
    _verify(old_outcome, "outcome_payload_sha256", "old120 validation outcome")
    old_survivor_hash = _verify(old_survivors, "freeze_payload_sha256", "old120 survivor freeze")
    b_label_hash = _verify(b_labels, "evidence_payload_sha256", "B63 validation labels")

    old_source, old_rows = _recover_root(
        old120_root,
        expected_closure_payload_sha256=str(old_outcome["closure_payload_sha256"]),
        expected_count=120,
        cohort_label="old120",
    )
    b_source, b_rows = _recover_root(
        b63_root,
        expected_closure_payload_sha256=str(b_labels["source_validation_closure_payload_sha256"]),
        expected_count=63,
        cohort_label="B63",
    )
    old_exact = {str(row["exact_identity"]) for row in old_rows}
    old_productive = {str(row["exact_identity"]) for row in old_rows if bool(row["validation_productive"])}
    old_members = _read_jsonl(rp / "cn_program_optimizer_d1_validation_candidate_members_20260816.jsonl")
    if len(old_members) != 120:
        raise RuntimeError("old120 candidate member cardinality drift")
    if old_exact != {str(row["exact_identity"]) for row in old_members}:
        raise RuntimeError("old120 candidate membership drift")
    if old_productive != set(map(str, old_survivors["exact_identities"])):
        raise RuntimeError("old120 productive survivor drift")
    b_by_exact = {str(row["exact_identity"]): row for row in b_rows}
    b_label_rows = list(b_labels["label_rows"])
    if set(b_by_exact) != {str(row["exact_identity"]) for row in b_label_rows}:
        raise RuntimeError("B63 label membership drift")
    for label in b_label_rows:
        exact = str(label["exact_identity"])
        if bool(label["validation_productive"]) != bool(b_by_exact[exact]["validation_productive"]):
            raise RuntimeError("B63 productive label drift")

    rows = sorted(old_rows + b_rows, key=lambda row: str(row["exact_identity"]))
    if len(rows) != 183 or len({str(row["exact_identity"]) for row in rows}) != 183:
        raise RuntimeError("combined historical stability label geometry drift")
    cohort_counts: dict[str, dict[str, int]] = {}
    template_counts: dict[str, dict[str, int]] = {}
    for row in rows:
        for key, container in ((str(row["source_cohort"]), cohort_counts), (str(row["template_id"]), template_counts)):
            stats = container.setdefault(key, {"count": 0, "productive": 0, "stable_2of3": 0, "stable_3of3": 0})
            stats["count"] += 1
            stats["productive"] += int(bool(row["validation_productive"]))
            stats["stable_2of3"] += int(bool(row["validation_stable_2of3"]))
            stats["stable_3of3"] += int(bool(row["validation_stable_3of3"]))
    payload = {
        "schema_version": "cn_program_optimizer_d1_validation_stability_labels_v1",
        "status": STATUS,
        "source_authority": {
            "old120": old_source,
            "B63": b_source,
            "old120_survivor_freeze_payload_sha256": old_survivor_hash,
            "B63_label_evidence_payload_sha256": b_label_hash,
        },
        "candidate_count": len(rows),
        "validation_productive_count": sum(bool(row["validation_productive"]) for row in rows),
        "validation_stable_2of3_count": sum(bool(row["validation_stable_2of3"]) for row in rows),
        "validation_stable_3of3_count": sum(bool(row["validation_stable_3of3"]) for row in rows),
        "cohort_counts": dict(sorted(cohort_counts.items())),
        "template_counts": dict(sorted(template_counts.items())),
        "label_rows": rows,
        "research_boundaries": {
            "financial_evaluation_performed": False,
            "source_validation_domains_already_consumed": True,
            "holdout_read": False,
            "forward_read": False,
            "promotion_authorized": False,
        },
    }
    payload["evidence_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--old120-root", type=Path, required=True)
    parser.add_argument("--b63-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("runtime/run_plans/cn_program_optimizer_d1_validation_stability_labels_20260825.json"))
    args = parser.parse_args(argv)
    payload = freeze(args.repo_root, old120_root=args.old120_root, b63_root=args.b63_root)
    output = args.output if args.output.is_absolute() else args.repo_root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": payload["status"], "count": payload["candidate_count"], "productive": payload["validation_productive_count"], "stable2": payload["validation_stable_2of3_count"], "stable3": payload["validation_stable_3of3_count"], "payload": payload["evidence_payload_sha256"], "output": str(output.resolve())}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
