"""Phase3CR atom/lane inventory audit.

This audit checks whether the true-1min generator has a complete, typed field
entry plan before a new search is launched. It is a prelaunch gate, not alpha
proof.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from our_system_phase2.runtime.phase3bl_bk_priority_signal_materialization import (
    DEFAULT_SHARD_ROOT,
    _discover_panels,
    _write_csv,
    _write_json,
)
from our_system_phase2.services.atom_lane_manifest import atom_inventory_summary, build_search_atoms, field_lane
from our_system_phase2.services.typed_primitive_gate import validate_expression


REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3cr_atom_lane_inventory_audit_20260624")
DEFAULT_REPORT_ROOT = Path("reports/phase3cr_atom_lane_inventory_audit_20260624")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _read_available_fields(shard_root: Path, max_shards: int) -> tuple[set[str], list[dict[str, Any]]]:
    panels = _discover_panels(shard_root, max_shards)
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
                "has_trade_time": "trade_time" in fields,
                "has_code": "code" in fields,
                "has_open": "open" in fields,
                "has_close": "close" in fields,
                "has_amount": "amount" in fields,
                "has_volume": "volume" in fields,
            }
        )
    return intersection or union, rows


def _candidate_expressions(atom: dict[str, Any]) -> list[dict[str, Any]]:
    expr = str(atom["expr"])
    if str(atom.get("transform_mode") or "") == "typed_rank":
        rank = f"CSRank({expr})"
    else:
        rank = f"CSRank(ZScore({expr}))"
    return [
        {**atom, "candidate_expression": rank, "candidate_transform": "rank"},
        {**atom, "candidate_expression": f"Neg({rank})", "candidate_transform": "inverted"},
    ]


def _write_markdown(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Phase3CR Atom Lane Inventory Audit",
        "",
        f"- created_at: {summary['created_at']}",
        f"- decision: `{summary['decision']}`",
        f"- shard_root: `{summary['shard_root']}`",
        f"- schema_field_count: {summary['schema_field_count']}",
        f"- atom_count: {summary['atom_count']}",
        f"- candidate_expression_count: {summary['candidate_expression_count']}",
        f"- typed_gate_reject_count: {summary['typed_gate_reject_count']}",
        "",
        "## Atom Counts",
        "",
        "```json",
        json.dumps(summary["atom_counts"], ensure_ascii=False, indent=2, sort_keys=True),
        "```",
        "",
        "## Field Routing Counts",
        "",
        "```json",
        json.dumps(summary["field_route_counts"], ensure_ascii=False, indent=2, sort_keys=True),
        "```",
        "",
        "## Boundary",
        "",
        "- `direct_formula` fields may enter ordinary formula atoms.",
        "- `event_state` fields must enter typed event/state primitives.",
        "- `lagged_context` fields must enter coverage-aware primitives.",
        "- key/timestamp/future label fields are blocked.",
        "- This audit is not alpha proof and does not modify X0/R3.",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _counter(values: list[str]) -> dict[str, int]:
    out: dict[str, int] = {}
    for value in values:
        out[value] = out.get(value, 0) + 1
    return dict(sorted(out.items()))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard-root", type=Path, default=DEFAULT_SHARD_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--max-shards", type=int, default=4)
    args = parser.parse_args(argv)

    shard_root = _resolve(args.shard_root)
    output_root = _resolve(args.output_root)
    report_root = _resolve(args.report_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)

    available_fields, panel_rows = _read_available_fields(shard_root, args.max_shards)
    atoms = build_search_atoms(available_fields)
    candidate_rows: list[dict[str, Any]] = []
    reject_rows: list[dict[str, Any]] = []
    for atom in atoms:
        for row in _candidate_expressions(atom):
            verdict = validate_expression(
                str(row["candidate_expression"]),
                entry_lineage="phase3cr_atom_lane_inventory_audit",
                materialization_stage="candidate_expression_prelaunch",
                candidate_role=str(row.get("role") or "unknown"),
            )
            item = {**row, **verdict.to_row()}
            candidate_rows.append(item)
            if verdict.typed_gate_decision != "allow":
                reject_rows.append(item)

    field_route_rows = [
        {
            "field": field,
            "field_route": field_lane(field),
            "consumed_by_atom": any(field in (atom.get("required_fields") or ()) for atom in atoms),
        }
        for field in sorted(available_fields)
    ]
    inv = atom_inventory_summary(available_fields)
    atom_counts = {
        "by_role": inv["by_role"],
        "by_field_class": inv["by_field_class"],
        "by_lane": inv["by_lane"],
    }
    field_route_counts = _counter([row["field_route"] for row in field_route_rows])
    event_fields_available = field_route_counts.get("event_state", 0) > 0
    event_atoms_present = int(inv["by_field_class"].get("event_state", 0)) > 0
    context_fields_available = field_route_counts.get("lagged_context", 0) > 0
    context_atoms_present = int(inv["by_field_class"].get("lagged_context", 0)) > 0
    if reject_rows:
        decision = "FAIL_ATOM_LANE_TYPED_GATE_REJECTS"
    elif event_fields_available and not event_atoms_present:
        decision = "FAIL_EVENT_FIELDS_AVAILABLE_BUT_NO_EVENT_ATOMS"
    elif context_fields_available and not context_atoms_present:
        decision = "FAIL_CONTEXT_FIELDS_AVAILABLE_BUT_NO_CONTEXT_ATOMS"
    else:
        decision = "PASS_ATOM_LANE_PRELAUNCH_GATE_DIAGNOSTIC_ONLY"

    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260624_phase3cr_atom_lane_inventory_audit",
        "decision": decision,
        "shard_root": str(shard_root),
        "max_shards": int(args.max_shards),
        "schema_field_count": len(available_fields),
        "panel_count": len(panel_rows),
        "atom_count": len(atoms),
        "candidate_expression_count": len(candidate_rows),
        "typed_gate_reject_count": len(reject_rows),
        "atom_counts": atom_counts,
        "field_route_counts": field_route_counts,
        "unconsumed_available_fields": inv["unconsumed_available_fields"],
        "hard_boundary": [
            "direct formula fields, event-state fields, and lagged context fields have separate entry paths",
            "event fields cannot enter ordinary continuous primitives",
            "lagged/coverage context cannot enter unguarded Corr/Cov/ZScore/CSResidual",
            "this is prelaunch plumbing audit only",
        ],
    }
    _write_csv(output_root / "phase3cr_panel_schema.csv", panel_rows)
    _write_csv(output_root / "phase3cr_field_routes.csv", field_route_rows)
    _write_csv(output_root / "phase3cr_atom_inventory.csv", atoms)
    _write_csv(output_root / "phase3cr_candidate_expression_gate.csv", candidate_rows)
    _write_csv(output_root / "phase3cr_typed_gate_rejects.csv", reject_rows)
    _write_json(output_root / "phase3cr_atom_lane_inventory_summary.json", summary)
    _write_csv(report_root / "phase3cr_field_routes.csv", field_route_rows)
    _write_csv(report_root / "phase3cr_atom_inventory.csv", atoms)
    _write_csv(report_root / "phase3cr_typed_gate_rejects.csv", reject_rows)
    _write_json(report_root / "phase3cr_atom_lane_inventory_summary.json", summary)
    _write_markdown(report_root / "PHASE3CR_ATOM_LANE_INVENTORY_AUDIT_20260624.md", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if decision.startswith("PASS") else 2


if __name__ == "__main__":
    raise SystemExit(main())

