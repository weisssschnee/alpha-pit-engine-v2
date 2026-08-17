"""Build the deterministic 183-row Transfer Filter V2 reconstruction dataset.

The builder combines durable candidate-member freezes with separately frozen
chronological development-window evidence and B per-exact validation labels.
It never opens financial datasets.  The old 120 validation labels come from the
already committed survivor freeze.  The output is suitable for independent V2
model/LOCO reproduction once the missing legacy evidence has been recovered.
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from our_system_phase2.services.unified_capability_registry import stable_hash

OLD_MEMBERS_REL = Path("runtime/run_plans/cn_program_optimizer_d1_validation_candidate_members_20260816.jsonl")
OLD_SURVIVORS_REL = Path("runtime/run_plans/cn_program_optimizer_d1_validation_survivor_freeze_20260817.json")
B_MEMBERS_REL = Path("runtime/run_plans/cn_program_optimizer_d1_transfer_B_validation_freeze_20260817/validation_candidate_members.jsonl")
V2_AUTOPSY_REL = Path("runtime/run_plans/cn_program_optimizer_d1_transfer_v2_autopsy_20260817.json")
EXPECTED_WINDOWS = ("development_1", "development_2", "development_3")
EXPECTED_COHORTS = {"D1_CONTINUATION_B": 63, "D1_FRESH": 64, "SUCCESSOR_D1": 56}


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def _verify_self_hash(payload: Mapping[str, Any], field: str, label: str) -> None:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")


def build_dataset(
    *,
    repo_root: Path,
    feature_evidence_path: Path,
    b_label_evidence_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    root = repo_root.resolve()
    old_members = _read_jsonl(root / OLD_MEMBERS_REL)
    b_members = _read_jsonl(root / B_MEMBERS_REL)
    survivors = _read_json(root / OLD_SURVIVORS_REL)
    autopsy = _read_json(root / V2_AUTOPSY_REL)
    features = _read_json(feature_evidence_path.resolve())
    b_labels = _read_json(b_label_evidence_path.resolve())

    _verify_self_hash(survivors, "freeze_payload_sha256", "old survivor freeze")
    _verify_self_hash(autopsy, "autopsy_payload_sha256", "V2 autopsy")
    _verify_self_hash(features, "evidence_payload_sha256", "development window feature evidence")
    _verify_self_hash(b_labels, "evidence_payload_sha256", "B validation label evidence")

    members = old_members + b_members
    if len(old_members) != 120 or len(b_members) != 63 or len(members) != 183:
        raise RuntimeError("reconstruction candidate cardinality drift")
    member_by = {str(row["exact_identity"]): dict(row) for row in members}
    if len(member_by) != 183:
        raise RuntimeError("reconstruction exact identity duplication")

    feature_rows = list(features.get("feature_rows") or ())
    if int(features.get("candidate_count") or 0) != 183 or len(feature_rows) != 183:
        raise RuntimeError("development feature evidence cardinality drift")
    if tuple(features.get("development_window_ids") or ()) != EXPECTED_WINDOWS:
        raise RuntimeError("development feature evidence window contract drift")
    if stable_hash(feature_rows) != str(features.get("feature_rows_sha256") or ""):
        raise RuntimeError("development feature rows hash drift")
    feature_by = {str(row.get("exact_identity") or ""): dict(row) for row in feature_rows}
    if set(feature_by) != set(member_by):
        raise RuntimeError("development feature evidence exact-set drift")

    survivor_ids = set(map(str, survivors.get("exact_identities") or ()))
    old_ids = {str(row["exact_identity"]) for row in old_members}
    if len(survivor_ids) != 30 or not survivor_ids.issubset(old_ids):
        raise RuntimeError("old survivor label drift")
    label_by: dict[str, bool] = {exact: exact in survivor_ids for exact in old_ids}

    b_label_rows = list(b_labels.get("label_rows") or ())
    if int(b_labels.get("candidate_count") or 0) != 63 or len(b_label_rows) != 63:
        raise RuntimeError("B validation label evidence cardinality drift")
    if int(b_labels.get("validation_productive_count") or -1) != 20:
        raise RuntimeError("B validation productive count drift")
    if stable_hash(b_label_rows) != str(b_labels.get("label_rows_sha256") or ""):
        raise RuntimeError("B validation label rows hash drift")
    b_ids = {str(row["exact_identity"]) for row in b_members}
    if {str(row.get("exact_identity") or "") for row in b_label_rows} != b_ids:
        raise RuntimeError("B validation label evidence exact-set drift")
    for row in b_label_rows:
        label_by[str(row["exact_identity"])] = bool(row["validation_productive"])
    if set(label_by) != set(member_by):
        raise RuntimeError("combined validation label coverage drift")

    rows: list[dict[str, Any]] = []
    for exact, member in member_by.items():
        feat = feature_by[exact]
        ids = tuple(map(str, feat.get("development_window_ids") or ()))
        wins = tuple(map(float, feat.get("development_window_return_increments") or ()))
        if ids != EXPECTED_WINDOWS or len(wins) != 3 or not all(math.isfinite(value) for value in wins):
            raise RuntimeError(f"chronological development feature drift: {exact}")
        if str(feat.get("source_cohort") or "") != str(member.get("source_cohort") or ""):
            raise RuntimeError(f"development feature cohort drift: {exact}")
        rows.append({
            "exact_identity": exact,
            "source_cohort": str(member["source_cohort"]),
            "dev_matched_return": float(member["development_matched_cumulative_net_return_increment"]),
            "dev_matched_reward": float(member["development_matched_net_reward_increment"]),
            "dev_window_1_increment": wins[0],
            "dev_window_2_increment": wins[1],
            "dev_window_3_increment": wins[2],
            "validation_productive": bool(label_by[exact]),
        })
    rows.sort(key=lambda row: str(row["exact_identity"]))

    cohort_counts = {cohort: sum(str(row["source_cohort"]) == cohort for row in rows) for cohort in sorted(EXPECTED_COHORTS)}
    positive_count = sum(bool(row["validation_productive"]) for row in rows)
    if cohort_counts != EXPECTED_COHORTS or positive_count != 50:
        raise RuntimeError("combined V2 reconstruction population drift")

    frozen_dataset_sha = str(dict(autopsy.get("tuning_population") or {}).get("combined_183_window_dataset_sha256") or "")
    rows_sha = stable_hash(rows)
    payload: dict[str, Any] = {
        "schema_version": "cn_program_optimizer_d1_transfer_v2_repro_dataset_v1",
        "status": "V2_REPRO_DATASET_BUILT_FROM_DURABLE_EVIDENCE",
        "candidate_count": len(rows),
        "validation_productive_count": positive_count,
        "cohort_counts": cohort_counts,
        "development_window_ids": list(EXPECTED_WINDOWS),
        "rows": rows,
        "rows_sha256": rows_sha,
        "frozen_V2_dataset_sha256_claim": frozen_dataset_sha,
        "rows_sha256_matches_frozen_claim": rows_sha == frozen_dataset_sha,
        "source_feature_evidence_payload_sha256": str(features["evidence_payload_sha256"]),
        "source_B_label_evidence_payload_sha256": str(b_labels["evidence_payload_sha256"]),
        "financial_evaluation_performed_by_this_build": False,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "optimizer_feedback_write": "FORBIDDEN",
        "promotion_authorized": False,
    }
    payload["dataset_payload_sha256"] = stable_hash(payload)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--feature-evidence", type=Path, required=True)
    parser.add_argument("--b-label-evidence", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    payload = build_dataset(
        repo_root=args.repo_root,
        feature_evidence_path=args.feature_evidence,
        b_label_evidence_path=args.b_label_evidence,
        output_path=args.output,
    )
    print(json.dumps({
        "status": payload["status"],
        "candidate_count": payload["candidate_count"],
        "validation_productive_count": payload["validation_productive_count"],
        "rows_sha256": payload["rows_sha256"],
        "rows_sha256_matches_frozen_claim": payload["rows_sha256_matches_frozen_claim"],
        "dataset_payload_sha256": payload["dataset_payload_sha256"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
