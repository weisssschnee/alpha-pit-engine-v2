"""Audit true-1min field usage across data, generation, and evaluation lifecycle.

Phase3CT is a hard prelaunch/acceptance audit. It answers a narrow question:
given a concrete true-1min shard root and optional candidate outputs, are the
available fields routed, materialized, split-covered, and actually consumed in a
way that prevents accidental fallback to old 1D data or silent sidecar omission?
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.compute as pc
import pyarrow.parquet as pq

from our_system_phase2.runtime.phase3bl_bk_priority_signal_materialization import (
    DEFAULT_SHARD_ROOT,
    _discover_panels,
    _fields,
    _write_csv,
    _write_json,
)
from our_system_phase2.services.atom_lane_manifest import atom_inventory_summary, field_lane


REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3ct_true1min_lifecycle_field_usage_audit_20260625")
DEFAULT_REPORT_ROOT = Path("reports/phase3ct_true1min_lifecycle_field_usage_audit_20260625")
FORBIDDEN_PATH_TOKENS = ("tdxofficial", "old_1d", "1d_old", "daily_stock_pit", "stock_pit_1d")
FUTURE_OR_LABEL_TOKENS = ("next_open", "next_close", "next_return", "future", "label")
BASE_COLUMNS = ("code", "trade_time", "exec_date", "date")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_md(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Phase3CT True1min Lifecycle Field Usage Audit",
        "",
        f"- created_at: {summary['created_at']}",
        f"- decision: `{summary['decision']}`",
        f"- shard_root: `{summary['shard_root']}`",
        f"- panel_count: {summary['panel_count']}",
        f"- schema_intersection_field_count: {summary['schema_intersection_field_count']}",
        f"- candidate_file_count: {summary['candidate_file_count']}",
        f"- blocker_count: {len(summary['blockers'])}",
        "",
        "## Lane Coverage",
        "",
        "```json",
        json.dumps(summary["lane_counts"], ensure_ascii=False, indent=2, sort_keys=True),
        "```",
        "",
        "## Split Coverage",
        "",
        "```json",
        json.dumps(summary["split_lane_coverage"], ensure_ascii=False, indent=2, sort_keys=True),
        "```",
        "",
        "## Candidate Usage",
        "",
        "```json",
        json.dumps(summary["candidate_usage_summary"], ensure_ascii=False, indent=2, sort_keys=True),
        "```",
        "",
        "## Blockers",
        "",
    ]
    if summary["blockers"]:
        lines.extend(f"- {item}" for item in summary["blockers"])
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- This audit is evidence plumbing, not alpha proof.",
            "- `train/validation/test` here is a chronological field-coverage audit split, not a promotion split.",
            "- X0/R3 are read-only.",
            "- Candidate files are checked for schema/lane usage and empty effective signal symptoms.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _schema_fields(panels: list[Path]) -> tuple[set[str], set[str], list[dict[str, Any]]]:
    intersection: set[str] | None = None
    union: set[str] = set()
    rows: list[dict[str, Any]] = []
    for panel in panels:
        fields = set(pq.ParquetFile(panel).schema_arrow.names)
        union |= fields
        intersection = fields if intersection is None else intersection & fields
        rows.append(
            {
                "panel": str(panel),
                "column_count": len(fields),
                "has_code": "code" in fields,
                "has_trade_time": "trade_time" in fields,
                "has_exec_date": "exec_date" in fields,
                "has_open": "open" in fields,
                "has_close": "close" in fields,
                "has_amount": "amount" in fields,
            }
        )
    return intersection or set(), union, rows


def _collect_dates(panels: list[Path]) -> list[str]:
    dates: set[str] = set()
    for panel in panels:
        parquet = pq.ParquetFile(panel)
        names = set(parquet.schema_arrow.names)
        col = "exec_date" if "exec_date" in names else "date"
        for row_group in range(parquet.num_row_groups):
            table = parquet.read_row_group(row_group, columns=[col])
            values = pc.unique(table[col].combine_chunks()).to_pylist()
            dates.update(str(item)[:10] for item in values if item is not None and str(item)[:10])
    return sorted(dates)


def _split_map(dates: list[str]) -> dict[str, str]:
    if not dates:
        return {}
    n = len(dates)
    train_end = max(1, int(math.floor(n * 0.60)))
    valid_end = max(train_end + 1, int(math.floor(n * 0.80))) if n >= 3 else train_end
    out: dict[str, str] = {}
    for idx, date in enumerate(dates):
        if idx < train_end:
            out[date] = "train"
        elif idx < valid_end:
            out[date] = "validation"
        else:
            out[date] = "test"
    return out


def _route_counts(fields: set[str]) -> dict[str, int]:
    return dict(sorted(Counter(field_lane(field) for field in fields).items()))


def _coverage_audit(panels: list[Path], fields: set[str], split_by_date: dict[str, str]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    audited_fields = sorted(
        field
        for field in fields
        if field_lane(field) in {"direct_formula", "event_state", "lagged_context", "membership_context", "unknown_review"}
    )
    rows_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for panel in panels:
        parquet = pq.ParquetFile(panel)
        names = set(parquet.schema_arrow.names)
        date_col = "exec_date" if "exec_date" in names else "date"
        columns = [col for col in [date_col, *audited_fields] if col in names]
        if date_col not in columns:
            continue
        for row_group in range(parquet.num_row_groups):
            frame = parquet.read_row_group(row_group, columns=columns).to_pandas()
            dates = frame[date_col].astype(str).str.slice(0, 10)
            split = dates.map(split_by_date).fillna("unknown")
            for split_name, idx in split.groupby(split, sort=False).groups.items():
                part = frame.loc[idx]
                total = int(len(part))
                for field in audited_fields:
                    if field not in part.columns:
                        continue
                    value = pd.to_numeric(part[field], errors="coerce")
                    valid = value.notna()
                    key = (field, str(split_name))
                    row = rows_by_key.setdefault(
                        key,
                        {
                            "field": field,
                            "field_lane": field_lane(field),
                            "split": str(split_name),
                            "rows": 0,
                            "nonnull": 0,
                            "nonzero": 0,
                        },
                    )
                    row["rows"] += total
                    row["nonnull"] += int(valid.sum())
                    row["nonzero"] += int((valid & (value != 0.0)).sum())
    rows: list[dict[str, Any]] = []
    for row in rows_by_key.values():
        total = max(1, int(row["rows"]))
        row["nonnull_ratio"] = round(float(row["nonnull"]) / total, 8)
        row["nonzero_ratio"] = round(float(row["nonzero"]) / total, 8)
        rows.append(row)
    rows.sort(key=lambda row: (row["field_lane"], row["field"], row["split"]))

    split_lane: dict[str, dict[str, Any]] = defaultdict(lambda: defaultdict(int))
    for row in rows:
        lane = str(row["field_lane"])
        split = str(row["split"])
        if float(row["nonnull_ratio"]) > 0.0:
            split_lane[lane][f"{split}_nonnull_fields"] += 1
        if float(row["nonzero_ratio"]) > 0.0:
            split_lane[lane][f"{split}_nonzero_fields"] += 1
    return rows, {lane: dict(values) for lane, values in sorted(split_lane.items())}


def _candidate_usage(candidate_files: list[Path], schema_fields: set[str]) -> tuple[list[dict[str, Any]], dict[str, Any], list[str]]:
    rows: list[dict[str, Any]] = []
    blockers: list[str] = []
    used_fields: Counter[str] = Counter()
    effective_fields: Counter[str] = Counter()
    lane_counts: Counter[str] = Counter()
    effective_lane_counts: Counter[str] = Counter()
    file_summaries: list[dict[str, Any]] = []
    for path in candidate_files:
        path = _resolve(path)
        data = _read_csv(path)
        missing_rows = 0
        blocked_rows = 0
        effective_rows = 0
        zero_signal_rows = 0
        for idx, item in enumerate(data, 1):
            expr = str(item.get("expression") or "")
            fields = _fields(expr)
            missing = [field for field in fields if field not in schema_fields]
            blocked = [field for field in fields if field_lane(field) == "blocked_key_or_label" or any(token in field.lower() for token in FUTURE_OR_LABEL_TOKENS)]
            signal_nonnull = item.get("signal_nonnull_sum")
            effective = signal_nonnull in (None, "")
            if signal_nonnull not in (None, ""):
                try:
                    effective = float(signal_nonnull) > 0.0
                except Exception:
                    effective = False
            if missing:
                missing_rows += 1
                blockers.append(f"candidate_missing_schema_fields:{path.name}:row{idx}:{'|'.join(missing)}")
            if blocked:
                blocked_rows += 1
                blockers.append(f"candidate_uses_blocked_or_future_fields:{path.name}:row{idx}:{'|'.join(blocked)}")
            if not effective:
                zero_signal_rows += 1
            else:
                effective_rows += 1
            for field in fields:
                used_fields[field] += 1
                lane = field_lane(field)
                lane_counts[lane] += 1
                if effective:
                    effective_fields[field] += 1
                    effective_lane_counts[lane] += 1
            rows.append(
                {
                    "candidate_file": str(path),
                    "row_number": idx,
                    "candidate_id": item.get("candidate_id", ""),
                    "expression_hash": item.get("expression_hash", ""),
                    "fields": "|".join(fields),
                    "field_lanes": "|".join(sorted({field_lane(field) for field in fields})),
                    "missing_schema_fields": "|".join(missing),
                    "blocked_or_future_fields": "|".join(blocked),
                    "signal_nonnull_sum": signal_nonnull or "",
                    "effective_signal_used": bool(effective),
                    "decision": item.get("phase3bp_decision") or item.get("train_reward_decision") or "",
                    "blockers": item.get("phase3bp_blocker_flags") or item.get("train_reward_blockers") or item.get("blocker_flags") or "",
                }
            )
        file_summaries.append(
            {
                "candidate_file": str(path),
                "row_count": len(data),
                "missing_schema_rows": missing_rows,
                "blocked_or_future_rows": blocked_rows,
                "effective_signal_rows": effective_rows,
                "zero_signal_rows": zero_signal_rows,
            }
        )
    summary = {
        "files": file_summaries,
        "used_field_count": len(used_fields),
        "effective_used_field_count": len(effective_fields),
        "used_lane_counts": dict(sorted(lane_counts.items())),
        "effective_used_lane_counts": dict(sorted(effective_lane_counts.items())),
        "top_used_fields": used_fields.most_common(30),
        "top_effective_fields": effective_fields.most_common(30),
    }
    return rows, summary, blockers


def _sidecar_summary(sidecar_root: Path | None) -> dict[str, Any]:
    if sidecar_root is None:
        return {"provided": False}
    root = _resolve(sidecar_root)
    out: dict[str, Any] = {"provided": True, "root": str(root), "exists": root.exists(), "files": []}
    for name in (
        "phase3cs_stock_lagged_context.parquet",
        "phase3cs_market_lagged_context.parquet",
        "phase3cs_stock_event_context.parquet",
        "phase3cs_sidecar_field_contract.csv",
        "phase3cs_sidecar_pack_summary.json",
    ):
        path = root / name
        item: dict[str, Any] = {"name": name, "exists": path.exists(), "path": str(path)}
        if path.exists() and path.suffix == ".parquet":
            pf = pq.ParquetFile(path)
            item["rows"] = pf.metadata.num_rows
            item["columns"] = len(pf.schema_arrow.names)
            item["schema"] = pf.schema_arrow.names
        out["files"].append(item)
    return out


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shard-root", type=Path, default=DEFAULT_SHARD_ROOT)
    parser.add_argument("--sidecar-root", type=Path, default=None)
    parser.add_argument("--candidate-file", action="append", type=Path, default=[])
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--max-shards", type=int, default=4)
    parser.add_argument("--min-schema-fields", type=int, default=40)
    parser.add_argument("--require-sidecar-lanes", action="store_true")
    args = parser.parse_args(argv)

    shard_root = _resolve(args.shard_root)
    output_root = _resolve(args.output_root)
    report_root = _resolve(args.report_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)

    panels = _discover_panels(shard_root, args.max_shards)
    schema_intersection, schema_union, panel_rows = _schema_fields(panels)
    dates = _collect_dates(panels)
    split_by_date = _split_map(dates)
    coverage_rows, split_lane_coverage = _coverage_audit(panels, schema_intersection, split_by_date)
    field_route_rows = [{"field": field, "field_lane": field_lane(field)} for field in sorted(schema_intersection)]
    lane_counts = _route_counts(schema_intersection)
    atom_summary = atom_inventory_summary(schema_intersection)
    candidate_rows, candidate_usage_summary, candidate_blockers = _candidate_usage(args.candidate_file, schema_intersection)
    sidecar = _sidecar_summary(args.sidecar_root)

    blockers: list[str] = []
    shard_root_lower = str(shard_root).lower()
    if any(token in shard_root_lower for token in FORBIDDEN_PATH_TOKENS):
        blockers.append(f"forbidden_shard_root_token:{shard_root}")
    if len(schema_intersection) < int(args.min_schema_fields):
        blockers.append(f"schema_field_count_below_min:{len(schema_intersection)}<{args.min_schema_fields}")
    if any(not row["has_trade_time"] or not row["has_code"] for row in panel_rows):
        blockers.append("panel_missing_trade_time_or_code")
    if args.require_sidecar_lanes:
        if lane_counts.get("event_state", 0) <= 0:
            blockers.append("required_event_state_fields_missing")
        if lane_counts.get("lagged_context", 0) <= 0:
            blockers.append("required_lagged_context_fields_missing")
    if lane_counts.get("event_state", 0) > 0 and int(atom_summary["by_field_class"].get("event_state", 0)) <= 0:
        blockers.append("event_fields_available_but_no_event_atoms")
    if lane_counts.get("lagged_context", 0) > 0 and int(atom_summary["by_field_class"].get("lagged_context", 0)) <= 0:
        blockers.append("context_fields_available_but_no_context_atoms")
    if args.candidate_file:
        blockers.extend(candidate_blockers[:50])
        used_lanes = candidate_usage_summary["used_lane_counts"]
        effective_lanes = candidate_usage_summary["effective_used_lane_counts"]
        if args.require_sidecar_lanes and used_lanes.get("event_state", 0) <= 0:
            blockers.append("candidate_files_do_not_use_available_event_state_fields")
        if args.require_sidecar_lanes and used_lanes.get("lagged_context", 0) <= 0:
            blockers.append("candidate_files_do_not_use_available_lagged_context_fields")
        if used_lanes.get("event_state", 0) > 0 and effective_lanes.get("event_state", 0) <= 0:
            blockers.append("event_state_candidates_present_but_zero_effective_signal")
        if used_lanes.get("lagged_context", 0) > 0 and effective_lanes.get("lagged_context", 0) <= 0:
            blockers.append("lagged_context_candidates_present_but_zero_effective_signal")

    decision = "PASS_TRUE1MIN_LIFECYCLE_FIELD_USAGE_AUDIT" if not blockers else "HOLD_TRUE1MIN_LIFECYCLE_FIELD_USAGE_AUDIT"
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260625_phase3ct_true1min_lifecycle_field_usage_audit",
        "decision": decision,
        "shard_root": str(shard_root),
        "sidecar_root": str(_resolve(args.sidecar_root)) if args.sidecar_root else None,
        "candidate_files": [str(_resolve(path)) for path in args.candidate_file],
        "candidate_file_count": len(args.candidate_file),
        "panel_count": len(panels),
        "panel_paths": [str(path) for path in panels],
        "schema_intersection_field_count": len(schema_intersection),
        "schema_union_field_count": len(schema_union),
        "lane_counts": lane_counts,
        "split_policy": {"kind": "chronological_60_20_20_field_coverage", "date_count": len(dates)},
        "split_ranges": {
            split: {"start": min([date for date, value in split_by_date.items() if value == split], default=None), "end": max([date for date, value in split_by_date.items() if value == split], default=None)}
            for split in ("train", "validation", "test")
        },
        "split_lane_coverage": split_lane_coverage,
        "atom_summary": atom_summary,
        "candidate_usage_summary": candidate_usage_summary,
        "sidecar_summary": sidecar,
        "blockers": blockers,
        "hard_boundary": [
            "true trade_time shard panels only",
            "field availability must be schema-bound",
            "candidate fields must be subset of schema intersection",
            "event/context fields must have typed atoms and effective signal usage before success can be claimed",
            "old 1D paths and future/label fields are blocked",
        ],
    }
    _write_csv(output_root / "phase3ct_panel_schema.csv", panel_rows)
    _write_csv(output_root / "phase3ct_field_routes.csv", field_route_rows)
    _write_csv(output_root / "phase3ct_split_field_coverage.csv", coverage_rows)
    _write_csv(output_root / "phase3ct_candidate_usage.csv", candidate_rows)
    _write_json(output_root / "phase3ct_true1min_lifecycle_field_usage_summary.json", summary)
    _write_csv(report_root / "phase3ct_field_routes.csv", field_route_rows)
    _write_csv(report_root / "phase3ct_split_field_coverage.csv", coverage_rows)
    _write_csv(report_root / "phase3ct_candidate_usage.csv", candidate_rows)
    _write_json(report_root / "phase3ct_true1min_lifecycle_field_usage_summary.json", summary)
    _write_md(report_root / "PHASE3CT_TRUE1MIN_LIFECYCLE_FIELD_USAGE_AUDIT_20260625.md", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if decision.startswith("PASS") else 2


if __name__ == "__main__":
    raise SystemExit(main())
