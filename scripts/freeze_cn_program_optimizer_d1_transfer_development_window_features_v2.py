"""Freeze chronological development-window features for D1 transfer cohorts.

Run this on a host that still has the immutable development run roots referenced
by candidate-member freezes.  It performs no financial evaluation and opens no
field/price/validation/holdout data.  It only re-reads already-closed
``physical_results.jsonl`` files, verifies each candidate's frozen provenance,
and persists the ordered development_1/2/3 return increments needed to rebuild
Transfer Filter V2.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from our_system_phase2.services.unified_capability_registry import stable_hash

EXPECTED_WINDOWS = ("development_1", "development_2", "development_3")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def freeze_features(*, member_paths: Sequence[Path], output_path: Path) -> dict[str, Any]:
    if not member_paths:
        raise ValueError("at least one candidate-member file is required")
    members: list[dict[str, Any]] = []
    source_files: list[dict[str, Any]] = []
    for raw_path in member_paths:
        path = raw_path.resolve()
        rows = _read_jsonl(path)
        members.extend(rows)
        source_files.append({"path": str(path), "file_sha256": _sha256(path), "candidate_count": len(rows)})

    exacts = [str(row.get("exact_identity") or "") for row in members]
    if any(not exact for exact in exacts) or len(set(exacts)) != len(exacts):
        raise RuntimeError("candidate-member exact identity drift")

    grouped: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for member in members:
        root = str(member.get("source_root") or "")
        wave = int(member.get("source_wave"))
        if not root:
            raise RuntimeError("candidate member missing source_root")
        grouped[(root, wave)].append(member)

    feature_rows: list[dict[str, Any]] = []
    source_result_files: list[dict[str, Any]] = []
    for (root_text, wave), group in sorted(grouped.items(), key=lambda item: (item[0][0], item[0][1])):
        run_root = Path(root_text)
        result_path = run_root / f"wave_{wave:03d}" / "physical_results.jsonl"
        if not result_path.is_file():
            raise FileNotFoundError(result_path)
        result_rows = _read_jsonl(result_path)
        by_exact = {str(row.get("exact_identity") or ""): row for row in result_rows}
        if len(by_exact) != len(result_rows):
            raise RuntimeError(f"duplicate exact identity in {result_path}")
        source_result_files.append({"path": str(result_path.resolve()), "file_sha256": _sha256(result_path), "wave": wave})

        for member in group:
            exact = str(member["exact_identity"])
            result = by_exact.get(exact)
            if result is None:
                raise RuntimeError(f"missing frozen development result for {exact}")
            if str(result.get("source_record_sha256") or "") != str(member.get("source_record_sha256") or ""):
                raise RuntimeError(f"source_record_sha256 drift for {exact}")
            if str(result.get("physical_result_hash") or "") != str(member.get("physical_result_hash") or ""):
                raise RuntimeError(f"physical_result_hash drift for {exact}")
            admission = dict(result.get("admission") or {})
            if not bool(admission.get("admitted")):
                raise RuntimeError(f"frozen transfer member is not development-admitted: {exact}")
            ids = tuple(map(str, dict(admission.get("metrics") or {}).get("development_window_ids") or ()))
            if ids != EXPECTED_WINDOWS:
                raise RuntimeError(f"development window identity/order drift for {exact}: {ids}")
            credit = dict((result.get("uplift") or {}).get("program_credit") or {})
            wins = tuple(map(float, credit.get("window_return_increments") or ()))
            if len(wins) != 3 or not all(math.isfinite(value) for value in wins):
                raise RuntimeError(f"development window increment drift for {exact}")
            ret = float(credit.get("matched_cumulative_net_return_increment"))
            reward = float(credit.get("matched_net_reward_increment"))
            if not math.isclose(ret, float(member["development_matched_cumulative_net_return_increment"]), rel_tol=0.0, abs_tol=1e-15):
                raise RuntimeError(f"development matched return drift for {exact}")
            if not math.isclose(reward, float(member["development_matched_net_reward_increment"]), rel_tol=0.0, abs_tol=1e-15):
                raise RuntimeError(f"development matched reward drift for {exact}")
            feature_rows.append({
                "exact_identity": exact,
                "source_cohort": str(member.get("source_cohort") or ""),
                "source_wave": wave,
                "development_window_ids": list(ids),
                "development_window_return_increments": list(wins),
                "source_record_sha256": str(result["source_record_sha256"]),
                "physical_result_hash": str(result["physical_result_hash"]),
            })

    feature_rows.sort(key=lambda row: str(row["exact_identity"]))
    if len(feature_rows) != len(members):
        raise RuntimeError("development feature evidence coverage drift")

    payload: dict[str, Any] = {
        "schema_version": "cn_program_optimizer_d1_transfer_development_window_feature_evidence_v2",
        "status": "FROZEN_DURABLE_DEVELOPMENT_WINDOW_FEATURE_EVIDENCE",
        "candidate_count": len(feature_rows),
        "candidate_exact_identities_sha256": stable_hash(sorted(exacts)),
        "development_window_ids": list(EXPECTED_WINDOWS),
        "feature_rows": feature_rows,
        "feature_rows_sha256": stable_hash(feature_rows),
        "source_candidate_member_files": source_files,
        "source_physical_result_files": source_result_files,
        "financial_evaluation_performed_by_this_freeze": False,
        "validation_reads_by_this_freeze": 0,
        "holdout_reads_by_this_freeze": 0,
        "forward_2026_reads_by_this_freeze": 0,
        "optimizer_feedback_write": "FORBIDDEN",
        "promotion_authorized": False,
    }
    payload["evidence_payload_sha256"] = stable_hash(payload)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--members", type=Path, action="append", required=True, help="Repeat for each frozen cohort member JSONL")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    payload = freeze_features(member_paths=args.members, output_path=args.output)
    print(json.dumps({
        "status": payload["status"],
        "candidate_count": payload["candidate_count"],
        "feature_rows_sha256": payload["feature_rows_sha256"],
        "evidence_payload_sha256": payload["evidence_payload_sha256"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
