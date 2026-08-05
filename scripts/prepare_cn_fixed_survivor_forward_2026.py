"""Zero-forward-read preparation for the fixed ten-pair 2026 confirmation."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from scripts import build_cn_portfolio_decoder_autopsy_v1 as v1
from scripts import run_cn_fixed_survivor_forward_2026 as forward


def prepare(
    *,
    confirmation_root: Path,
    authorization_path: Path,
    output_root: Path,
    expected_authorization_sha256: str,
    expected_selection_payload_sha256: str,
    expected_forward_split_sha256: str,
    builder_commit_sha: str,
) -> dict:
    authorization = forward._verify_authorization(
        authorization_path,
        expected_sha256=expected_authorization_sha256,
        expected_selection_payload_sha256=expected_selection_payload_sha256,
        expected_forward_split_sha256=expected_forward_split_sha256,
    )
    manifest, contract, pairs, candidates = forward._load_confirmation_cohort(
        confirmation_root,
        expected_selection_payload_sha256=expected_selection_payload_sha256,
    )
    output_root = output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=False)
    candidate_csv = output_root / "confirmation_candidates.csv"
    pair_path = output_root / "confirmation_pairs.parquet"
    candidates.to_csv(candidate_csv, index=False)
    pairs.to_parquet(pair_path, index=False)
    receipt = {
        "schema_version": "cn_fixed10_forward_2026_preparation_v1",
        "status": "FIXED10_FORWARD_2026_ZERO_READ_PREPARED",
        "prepared_at_utc": datetime.now(timezone.utc).isoformat(),
        "builder_commit_sha": builder_commit_sha,
        "selection_payload_sha256": expected_selection_payload_sha256,
        "authorization_file_sha256": expected_authorization_sha256,
        "forward_split_manifest_sha256": expected_forward_split_sha256,
        "cohort_manifest_payload_sha256": manifest["manifest_payload_sha256"],
        "cohort_contract_payload_sha256": contract["contract_payload_sha256"],
        "pair_count": len(pairs),
        "candidate_member_count": len(candidates),
        "source_finalist_orders": pairs["finalist_order"].astype(int).tolist(),
        "candidate_csv_sha256": v1._sha256(candidate_csv),
        "pair_artifact_sha256": v1._sha256(pair_path),
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "replacement": "FORBIDDEN",
        "backfill": "FORBIDDEN",
        "post_freeze_filtering": "FORBIDDEN",
        "optimizer_feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "authorization_status": authorization["status"],
    }
    receipt["receipt_payload_sha256"] = v1._stable_hash(receipt)
    receipt_path = v1._write_json(output_root / "PREPARED_ZERO_READ.json", receipt)
    return {
        "status": receipt["status"],
        "receipt": str(receipt_path),
        "receipt_sha256": v1._sha256(receipt_path),
        "pair_count": len(pairs),
        "candidate_member_count": len(candidates),
        "forward_2026_reads": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--confirmation-root", type=Path, required=True)
    parser.add_argument("--authorization-path", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--expected-authorization-sha256", required=True)
    parser.add_argument("--expected-selection-payload-sha256", required=True)
    parser.add_argument("--expected-forward-split-sha256", required=True)
    parser.add_argument("--builder-commit-sha", required=True)
    args = parser.parse_args()
    result = prepare(
        confirmation_root=args.confirmation_root,
        authorization_path=args.authorization_path,
        output_root=args.output_root,
        expected_authorization_sha256=args.expected_authorization_sha256,
        expected_selection_payload_sha256=args.expected_selection_payload_sha256,
        expected_forward_split_sha256=args.expected_forward_split_sha256,
        builder_commit_sha=args.builder_commit_sha,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
