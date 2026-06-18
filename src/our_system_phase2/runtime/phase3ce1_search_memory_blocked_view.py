"""Build Phase3CE1 search-memory blocked view.

This script does not delete or rewrite historical memory. It reads inherited
memory artifacts, applies the shared typed primitive gate, and emits a view
where unsafe expression/skeleton keys are preserved for duplicate/block checks
but excluded from positive attribution.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from our_system_phase2.services.search_memory import expression_memory_key, skeleton_memory_key
from our_system_phase2.services.typed_primitive_gate import validate_expression


DEFAULT_MEMORY_ROOT = Path("runtime/search_memory")
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3ce1_search_memory_blocked_view_20260618")
DEFAULT_REPORT_ROOT = Path("reports/phase3ce1_search_memory_blocked_view_20260618")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _memory_json_paths(memory_root: Path) -> list[Path]:
    if not memory_root.exists():
        return []
    if memory_root.is_file():
        return [memory_root] if memory_root.suffix.lower() == ".json" else []
    out: list[Path] = []
    out.extend(memory_root.rglob("phase3aj_search_memory_ledger.json"))
    out.extend(memory_root.rglob("search_memory.json"))
    return sorted(set(out))


def _rows_from_memory_json(path: Path) -> list[dict[str, Any]]:
    payload = _read_json(path)
    if not isinstance(payload, dict):
        return []
    rows: list[dict[str, Any]] = []
    if isinstance(payload.get("memory_entries"), list):
        for row in payload["memory_entries"]:
            if isinstance(row, dict):
                rows.append(
                    {
                        **row,
                        "memory_artifact_schema": str(payload.get("schema_version") or "phase3aj_search_memory_ledger"),
                        "memory_generated_at": str(payload.get("generated_at") or ""),
                    }
                )
    elif isinstance(payload.get("records"), list):
        for row in payload["records"]:
            if isinstance(row, dict):
                rows.append(
                    {
                        **row,
                        "memory_artifact_schema": str(payload.get("schema_version") or "local_search_memory"),
                        "memory_generated_at": str(payload.get("created_at") or ""),
                    }
                )
    return rows


def _expression(row: dict[str, Any]) -> str:
    for key in ("expression", "formula", "signal_expression", "canonical_rank_validation_expression"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _key_value(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return str(value)
    return ""


def _view_rows(memory_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for path in _memory_json_paths(memory_root):
        for row in _rows_from_memory_json(path):
            expression = _expression(row)
            expr_key = _key_value(row, "expression_key", "search_memory_key", "expression_hash")
            skel_key = _key_value(row, "skeleton_key", "skeleton_hash")
            if expression:
                expr_key = expr_key or expression_memory_key(expression)
                skel_key = skel_key or skeleton_memory_key(expression)
            key = (str(path), expr_key, expression)
            if key in seen:
                continue
            seen.add(key)
            if expression:
                verdict = validate_expression(
                    expression,
                    entry_lineage="search_memory_inheritance",
                    materialization_stage="memory_load",
                    candidate_role="blocked_memory_key",
                ).to_row()
            else:
                verdict = {
                    "typed_gate_decision": "allow",
                    "typed_gate_reason": "memory row has no expression; key preserved without positive formula attribution",
                    "blocked_fields": "",
                    "blocked_primitives": "",
                    "required_rewrite": "",
                    "registry_version": "phase3ce1_typed_primitive_gate_v1_20260618",
                    "entry_lineage": "search_memory_inheritance",
                    "materialization_stage": "memory_load",
                    "candidate_role": "blocked_memory_key",
                }
            decision = str(verdict.get("typed_gate_decision") or "")
            rows.append(
                {
                    "source_memory_path": str(path),
                    "memory_artifact_schema": row.get("memory_artifact_schema", ""),
                    "memory_generated_at": row.get("memory_generated_at", ""),
                    "candidate_id": row.get("candidate_id", ""),
                    "search_memory_key": row.get("search_memory_key", ""),
                    "expression_key": expr_key,
                    "skeleton_key": skel_key,
                    "expression_hash": row.get("expression_hash", ""),
                    "skeleton_hash": row.get("skeleton_hash", ""),
                    "field_set_hash": row.get("field_set_hash", ""),
                    "source_label": row.get("source_label", ""),
                    "source_lane": row.get("source_lane", ""),
                    "frontier_lane": row.get("frontier_lane", ""),
                    "phase3_budget_bucket": row.get("phase3_budget_bucket", ""),
                    "field_family": row.get("field_family", ""),
                    "field_name": row.get("field_name", ""),
                    "expression": expression,
                    **verdict,
                    "memory_block_policy": "preserve_key_for_duplicate_block" if decision != "allow" else "positive_memory_allowed",
                    "positive_attribution_allowed": str(decision == "allow").lower(),
                    "duplicate_block_key_preserved": "true",
                }
            )
    return rows


def _compact_positive_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if row.get("typed_gate_decision") == "allow" and row.get("expression")
    ]


def _compact_blocked_records(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if row.get("typed_gate_decision") != "allow"]


def run(*, memory_root: Path, output_root: Path, report_root: Path) -> dict[str, Any]:
    rows = _view_rows(memory_root)
    blocked_rows = _compact_blocked_records(rows)
    positive_rows = _compact_positive_records(rows)
    all_expression_keys = sorted({str(row.get("expression_key") or "") for row in rows if row.get("expression_key")})
    all_skeleton_keys = sorted({str(row.get("skeleton_key") or "") for row in rows if row.get("skeleton_key")})
    blocked_expression_keys = sorted({str(row.get("expression_key") or "") for row in blocked_rows if row.get("expression_key")})
    blocked_skeleton_keys = sorted({str(row.get("skeleton_key") or "") for row in blocked_rows if row.get("skeleton_key")})
    safe_expression_keys = sorted({str(row.get("expression_key") or "") for row in positive_rows if row.get("expression_key")})
    safe_skeleton_keys = sorted({str(row.get("skeleton_key") or "") for row in positive_rows if row.get("skeleton_key")})

    decision_counts = Counter(str(row.get("typed_gate_decision") or "") for row in rows)
    source_counts = Counter(str(row.get("source_memory_path") or "") for row in rows)
    payload = {
        "schema_version": "phase3ce1_search_memory_blocked_view_v1",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "decision": "PASS_SEARCH_MEMORY_BLOCKED_VIEW_CREATED",
        "official_chain_mutation": False,
        "search_launched": False,
        "memory_root": str(memory_root),
        "source_memory_file_count": len(source_counts),
        "memory_entry_count": len(rows),
        "positive_record_count": len(positive_rows),
        "blocked_record_count": len(blocked_rows),
        "typed_gate_decision_counts": dict(sorted(decision_counts.items())),
        "all_expression_key_count": len(all_expression_keys),
        "all_skeleton_key_count": len(all_skeleton_keys),
        "safe_expression_key_count": len(safe_expression_keys),
        "safe_skeleton_key_count": len(safe_skeleton_keys),
        "blocked_expression_key_count": len(blocked_expression_keys),
        "blocked_skeleton_key_count": len(blocked_skeleton_keys),
        "policy": {
            "do_not_delete_historical_memory": True,
            "blocked_keys_preserved_for_duplicate_block": True,
            "blocked_records_excluded_from_positive_attribution": True,
            "construction_time_validator_remains_primary_safety_gate": True,
        },
        "active_duplicate_block_keys": {
            "expression_keys": all_expression_keys,
            "skeleton_keys": all_skeleton_keys,
        },
        "positive_memory_view": {
            "expression_keys": safe_expression_keys,
            "skeleton_keys": safe_skeleton_keys,
            "records": positive_rows,
        },
        "blocked_memory_view": {
            "expression_keys": blocked_expression_keys,
            "skeleton_keys": blocked_skeleton_keys,
            "records": blocked_rows,
        },
        "outputs": {
            "search_memory_blocked_view_json": str(output_root / "search_memory_blocked_view.json"),
            "search_memory_blocked_view_csv": str(output_root / "search_memory_blocked_view.csv"),
            "search_memory_blocked_keys_csv": str(output_root / "search_memory_blocked_keys.csv"),
            "search_memory_positive_records_csv": str(output_root / "search_memory_positive_records.csv"),
            "report": str(report_root / "PHASE3CE1_SEARCH_MEMORY_BLOCKED_VIEW_20260618.md"),
        },
    }

    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    _write_json(output_root / "search_memory_blocked_view.json", payload)
    _write_csv(output_root / "search_memory_blocked_view.csv", rows)
    _write_csv(output_root / "search_memory_blocked_keys.csv", blocked_rows)
    _write_csv(output_root / "search_memory_positive_records.csv", positive_rows)
    _write_json(report_root / "phase3ce1_search_memory_blocked_view_summary.json", payload)
    _write_csv(report_root / "search_memory_blocked_view.csv", rows)
    _write_csv(report_root / "search_memory_blocked_keys.csv", blocked_rows)
    _write_csv(report_root / "search_memory_positive_records.csv", positive_rows)
    _write_report(report_root / "PHASE3CE1_SEARCH_MEMORY_BLOCKED_VIEW_20260618.md", payload)
    return payload


def _write_report(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Phase3CE1 Search Memory Blocked View",
        "",
        "Status: diagnostic/runtime view. No historical memory deletion. No search launched. No official X0/R3 mutation.",
        "",
        "## Counts",
        "",
        f"- source_memory_file_count: `{payload['source_memory_file_count']}`",
        f"- memory_entry_count: `{payload['memory_entry_count']}`",
        f"- positive_record_count: `{payload['positive_record_count']}`",
        f"- blocked_record_count: `{payload['blocked_record_count']}`",
        f"- blocked_expression_key_count: `{payload['blocked_expression_key_count']}`",
        f"- blocked_skeleton_key_count: `{payload['blocked_skeleton_key_count']}`",
        "",
        "## Typed Gate Decisions",
        "",
    ]
    for decision, count in payload["typed_gate_decision_counts"].items():
        lines.append(f"- `{decision}`: `{count}`")
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- Unsafe memory keys are not deleted.",
            "- Unsafe expression/skeleton keys remain in `active_duplicate_block_keys`.",
            "- Unsafe records are excluded from positive attribution.",
            "- Construction-time validator remains the primary safety gate.",
            "",
            "## Outputs",
            "",
            "- `search_memory_blocked_view.json`",
            "- `search_memory_blocked_view.csv`",
            "- `search_memory_blocked_keys.csv`",
            "- `search_memory_positive_records.csv`",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--memory-root", type=Path, default=DEFAULT_MEMORY_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    payload = run(memory_root=args.memory_root, output_root=args.output_root, report_root=args.report_root)
    print(
        json.dumps(
            {
                "decision": payload["decision"],
                "memory_entry_count": payload["memory_entry_count"],
                "positive_record_count": payload["positive_record_count"],
                "blocked_record_count": payload["blocked_record_count"],
                "typed_gate_decision_counts": payload["typed_gate_decision_counts"],
                "outputs": payload["outputs"],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
