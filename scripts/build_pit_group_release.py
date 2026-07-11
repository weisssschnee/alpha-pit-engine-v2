"""Normalize a user-supplied historical plate/industry artifact into a PIT release."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd

from our_system_phase2.services.pit_group_release import (
    PITGroupReleaseSpec,
    build_interval_release,
    build_snapshot_release,
    write_release,
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_csv(path: Path) -> pd.DataFrame:
    failures: list[str] = []
    for encoding in ("utf-8-sig", "gb18030"):
        try:
            return pd.read_csv(path, encoding=encoding, dtype=str)
        except UnicodeDecodeError as exc:
            failures.append(f"{encoding}: {exc}")
    raise ValueError(f"cannot decode CSV {path}: {'; '.join(failures)}")


def read_source(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix in {".csv", ".txt"}:
        return _read_csv(path)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix in {".jsonl", ".ndjson"}:
        return pd.read_json(path, lines=True)
    if suffix == ".json":
        return pd.read_json(path)
    raise ValueError(f"unsupported source format: {suffix}; use CSV, Parquet, JSON, or JSONL")


def _column_mapping(value: str | None) -> dict[str, str]:
    if not value:
        return {}
    candidate = Path(value)
    payload = candidate.read_text(encoding="utf-8") if candidate.exists() else value
    mapping = json.loads(payload)
    if not isinstance(mapping, dict) or not all(isinstance(key, str) and isinstance(item, str) for key, item in mapping.items()):
        raise ValueError("column map must be a JSON object of source_name: canonical_name")
    return mapping


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--input-mode", choices=("intervals", "snapshots"), required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--column-map", help="JSON path or inline source-to-canonical mapping")
    parser.add_argument("--source-name", required=True)
    parser.add_argument("--source-version", required=True)
    parser.add_argument("--source-uri", required=True)
    parser.add_argument("--retrieved-at", required=True)
    parser.add_argument("--group-type", choices=("plate", "industry"), required=True)
    parser.add_argument("--membership-policy", choices=("single", "multi"), required=True)
    parser.add_argument("--required-start", required=True)
    parser.add_argument("--required-end", required=True)
    parser.add_argument("--maximum-observable-time", required=True)
    args = parser.parse_args(argv)

    if not args.input.is_file():
        parser.error(f"input is not a file: {args.input}")
    source = read_source(args.input).rename(columns=_column_mapping(args.column_map))
    spec = PITGroupReleaseSpec(
        source_name=args.source_name,
        source_version=args.source_version,
        source_uri=args.source_uri,
        source_artifact_sha256=_sha256(args.input),
        retrieved_at=args.retrieved_at,
        group_type=args.group_type,
        membership_policy=args.membership_policy,
        required_start=args.required_start,
        required_end=args.required_end,
        maximum_observable_time=args.maximum_observable_time,
    )
    if args.input_mode == "intervals":
        membership, manifest = build_interval_release(source, spec)
    else:
        membership, manifest = build_snapshot_release(source, spec)
    paths = write_release(membership, manifest, args.output_root, source_path=args.input)
    print(
        json.dumps(
            {
                "status": "PIT_GROUP_RELEASE_BUILT",
                "membership": str(paths["membership"]),
                "manifest": str(paths["manifest"]),
                "membership_sha256": manifest["membership_sha256"],
                "source_artifact_sha256": manifest["source_artifact_sha256"],
                "row_count": manifest["row_count"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
