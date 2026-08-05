"""Freeze the exact report-only calendar for a historical challenge archive."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts import build_cn_portfolio_decoder_autopsy_v1 as v1


SCHEMA_VERSION = "cn_historical_challenge_split_manifest_v1"
STATUS = "HISTORICAL_CHALLENGE_SPLIT_FROZEN_IMMUTABLE"


def build(*, sessionized_root: Path, output_root: Path) -> dict:
    sessionized_root = sessionized_root.resolve()
    source_manifest_path = sessionized_root / "SESSIONIZED_ARCHIVE_COMPLETE.json"
    source_manifest = v1._read_json(source_manifest_path)
    body = dict(source_manifest)
    declared = str(body.pop("manifest_payload_sha256", ""))
    if not declared or v1._stable_hash(body) != declared:
        raise RuntimeError("sessionized archive manifest self-hash drift")
    if (
        source_manifest.get("status") != "SESSIONIZED_ARCHIVE_COMPLETE"
        or int(source_manifest.get("expected_year") or 0) != 2023
        or int(source_manifest.get("shard_count") or 0) != 12
    ):
        raise RuntimeError("historical sessionized archive contract drift")

    shard_paths = sorted(sessionized_root.glob("shard_*.parquet"))
    if len(shard_paths) != 12:
        raise RuntimeError("historical session shard cardinality drift")
    dates: set[pd.Timestamp] = set()
    for path in shard_paths:
        frame = pd.read_parquet(path, columns=["date"])
        dates.update(pd.to_datetime(frame["date"], errors="raise").dt.normalize())
    ordered_dates = sorted(dates)
    if not 200 <= len(ordered_dates) < 250:
        raise RuntimeError("historical challenge calendar evidence ceiling drift")

    output_root = output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=False)
    split_path = output_root / "historical_challenge_split_manifest.csv"
    pd.DataFrame(
        {
            "trade_date": [value.strftime("%Y-%m-%d") for value in ordered_dates],
            "split": "historical_challenge",
            "optimizer_usage": "report_only",
        }
    ).to_csv(split_path, index=False)
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "status": STATUS,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_manifest_sha256": v1._sha256(source_manifest_path),
        "source_manifest_payload_sha256": declared,
        "split_manifest_sha256": v1._sha256(split_path),
        "date_count": len(ordered_dates),
        "date_min": ordered_dates[0].strftime("%Y-%m-%d"),
        "date_max": ordered_dates[-1].strftime("%Y-%m-%d"),
        "split": "historical_challenge",
        "optimizer_usage": "report_only",
        "outcome_based_filtering": "FORBIDDEN",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "forward_b_reads": 0,
        "optimizer_feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "artifacts": [v1._artifact(split_path, root=output_root)],
    }
    receipt["receipt_payload_sha256"] = v1._stable_hash(receipt)
    receipt_path = v1._write_json(
        output_root / "HISTORICAL_CHALLENGE_SPLIT_FROZEN.json", receipt
    )
    return {
        "status": STATUS,
        "receipt": str(receipt_path),
        "receipt_file_sha256": v1._sha256(receipt_path),
        "receipt_payload_sha256": receipt["receipt_payload_sha256"],
        "split_manifest": str(split_path),
        "split_manifest_sha256": receipt["split_manifest_sha256"],
        "date_count": receipt["date_count"],
        "date_min": receipt["date_min"],
        "date_max": receipt["date_max"],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sessionized-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    result = build(
        sessionized_root=args.sessionized_root,
        output_root=args.output_root,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
