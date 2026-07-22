"""Build one immutable exact/behavior dedupe snapshot from prior campaigns."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from our_system_phase2.services.portfolio_behavior_archive import (
    PortfolioBehaviorArchive,
)


BEHAVIOR_IDENTITY_FIELDS = {
    "pair_id",
    "route_id",
    "structural_family_id",
    "signal_cluster_id",
    "behavior_probe_id",
    "primary_behavior_probe_id",
    "control_behavior_probe_id",
    "portfolio_behavior_signature_id",
    "portfolio_behavior_family_id",
    "behavior_status",
    "primary_support_rate",
    "control_support_rate",
    "primary_mean_turnover",
    "control_mean_turnover",
    "coordinate_binding",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _artifact(path: Path, *, role: str | None = None) -> dict[str, Any]:
    path = Path(path).resolve()
    row: dict[str, Any] = {
        "path": str(path),
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }
    if role is not None:
        row["role"] = role
    return row


def _read_candidate_rows(path: Path) -> list[dict[str, Any]]:
    path = Path(path).resolve()
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path).fillna("").to_dict(orient="records")
    if suffix == ".csv":
        return pd.read_csv(path).fillna("").to_dict(orient="records")
    if suffix == ".jsonl":
        return [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    raise ValueError(f"unsupported candidate archive: {path}")


def build_snapshot(
    *,
    candidate_sources: Sequence[Path],
    behavior_sources: Sequence[Path],
    candidate_output: Path,
    behavior_output: Path,
    manifest_output: Path,
) -> dict[str, Any]:
    candidate_sources = tuple(Path(path).resolve() for path in candidate_sources)
    behavior_sources = tuple(Path(path).resolve() for path in behavior_sources)
    if not candidate_sources or not behavior_sources:
        raise ValueError("candidate and behavior sources are both required")

    exact_identities = sorted(
        {
            str(row.get("exact_identity") or "")
            for path in candidate_sources
            for row in _read_candidate_rows(path)
            if str(row.get("exact_identity") or "")
        }
    )
    if not exact_identities:
        raise RuntimeError("historical candidate sources contain no exact identities")

    behavior_rows: list[dict[str, Any]] = []
    seen_behavior_rows: set[str] = set()
    for path in behavior_sources:
        for source in PortfolioBehaviorArchive.read_parquet(path).rows:
            if str(source.get("behavior_status") or "RESOLVED") != "RESOLVED":
                continue
            row = {
                key: value
                for key, value in source.items()
                if key in BEHAVIOR_IDENTITY_FIELDS
            }
            identity = json.dumps(row, ensure_ascii=False, sort_keys=True, default=str)
            if identity in seen_behavior_rows:
                continue
            seen_behavior_rows.add(identity)
            behavior_rows.append(row)
    if not behavior_rows:
        raise RuntimeError("historical behavior sources contain no resolved identities")

    candidate_output = Path(candidate_output).resolve()
    behavior_output = Path(behavior_output).resolve()
    manifest_output = Path(manifest_output).resolve()
    candidate_output.parent.mkdir(parents=True, exist_ok=True)
    behavior_output.parent.mkdir(parents=True, exist_ok=True)
    manifest_output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"exact_identity": exact_identities}).to_parquet(
        candidate_output, index=False
    )
    PortfolioBehaviorArchive(behavior_rows).write_parquet(behavior_output)

    sources = [
        *[_artifact(path, role="candidate_exact") for path in candidate_sources],
        *[_artifact(path, role="portfolio_behavior") for path in behavior_sources],
    ]
    manifest = {
        "schema_version": "cn_campaign_history_snapshot_v1",
        "status": "PASS",
        "merge_policy": "IDENTITY_ONLY_NO_REWARD_NO_SCHEDULER_STATE",
        "sources": sources,
        "candidate_exact_archive": _artifact(candidate_output),
        "behavior_archive": _artifact(behavior_output),
        "exact_identity_count": len(exact_identities),
        "behavior_identity_row_count": len(behavior_rows),
        "reward_columns_imported": [],
        "scheduler_state_imported": False,
    }
    manifest_output.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-source", type=Path, action="append", required=True)
    parser.add_argument("--behavior-source", type=Path, action="append", required=True)
    parser.add_argument("--candidate-output", type=Path, required=True)
    parser.add_argument("--behavior-output", type=Path, required=True)
    parser.add_argument("--manifest-output", type=Path, required=True)
    args = parser.parse_args(argv)
    result = build_snapshot(
        candidate_sources=args.candidate_source,
        behavior_sources=args.behavior_source,
        candidate_output=args.candidate_output,
        behavior_output=args.behavior_output,
        manifest_output=args.manifest_output,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
