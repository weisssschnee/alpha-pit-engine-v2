from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any, Iterable, Mapping

import pyarrow.parquet as pq


FIELD_PATTERN = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")
EXPECTED_LABELS = frozenset(
    {"fwd_ret_1m", "fwd_ret_5m", "fwd_ret_15m", "fwd_ret_30m"}
)


def _read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def required_raw_fields(rows: Iterable[Mapping[str, Any]]) -> list[str]:
    return sorted(
        {
            field
            for row in rows
            for field in FIELD_PATTERN.findall(str(row.get("expression") or ""))
        }
        | {"close"}
    )


def _schema_audit(root: Path) -> dict[str, Any]:
    paths = sorted(root.glob("shard_*.parquet"))
    schemas = [set(pq.ParquetFile(path).schema_arrow.names) for path in paths]
    return {
        "root": str(root),
        "shards": len(paths),
        "intersection": sorted(set.intersection(*schemas)) if schemas else [],
        "union": sorted(set.union(*schemas)) if schemas else [],
    }


def _backend_audit(*, candidate_path: Path, field_root: Path, label_root: Path) -> dict[str, Any]:
    rows = _read_rows(candidate_path)
    required = required_raw_fields(rows)
    fields = _schema_audit(field_root)
    labels = _schema_audit(label_root)
    return {
        "candidate_path": str(candidate_path),
        "candidate_members": len(rows),
        "required_raw_fields": required,
        "required_raw_field_count": len(required),
        "field_sidecar": fields,
        "missing_raw_fields": sorted(set(required) - set(fields["intersection"])),
        "label_sidecar": labels,
        "missing_label_fields": sorted(EXPECTED_LABELS - set(labels["intersection"])),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--active-field-root", type=Path, required=True)
    parser.add_argument("--active-label-root", type=Path, required=True)
    parser.add_argument("--session-field-root", type=Path, required=True)
    parser.add_argument("--session-label-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--wave-id", required=True)
    args = parser.parse_args()

    backends = {
        "active": _backend_audit(
            candidate_path=args.candidate_root / "preflight_active_candidates.csv",
            field_root=args.active_field_root,
            label_root=args.active_label_root,
        ),
        "session": _backend_audit(
            candidate_path=args.candidate_root / "preflight_session_candidates.csv",
            field_root=args.session_field_root,
            label_root=args.session_label_root,
        ),
    }
    payload = {
        "schema_version": "cn_strict_wave_sidecar_coverage_v1",
        "status": f"CN_STRICT_WAVE_{args.wave_id}_SIDECAR_COVERAGE_AUDITED",
        "wave_id": args.wave_id,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "backends": backends,
        "all_required_fields_present": all(
            not row["missing_raw_fields"]
            and not row["missing_label_fields"]
            and row["field_sidecar"]["shards"] == 16
            and row["label_sidecar"]["shards"] == 16
            for row in backends.values()
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_name(args.output.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(args.output)
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload["all_required_fields_present"] else 3


if __name__ == "__main__":
    raise SystemExit(main())
