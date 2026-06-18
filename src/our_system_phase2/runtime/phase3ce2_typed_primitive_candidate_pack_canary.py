"""Build and gate a typed-primitive canary candidate pack.

This is an entry-path canary, not alpha proof. It verifies that sparse event,
discrete state, and coverage-sensitive fields can enter candidate materialization
through typed primitives and pass the CE1/G2 input gate.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any

from our_system_phase2.domain.models import make_candidate_id, utc_now_iso
from our_system_phase2.services.artifact_schema import write_json_artifact
from our_system_phase2.services.real_market_validation import TYPED_PRIMITIVE_OPERATORS
from our_system_phase2.services.typed_primitive_gate import (
    REGISTRY_VERSION,
    field_category,
    gate_g2_input_rows,
    validate_row,
)


DEFAULT_BLOCKED_EVENT_ROWS = Path(
    "reports/phase3ce1_factor_pack_preflight_gate_smoke_20260618/typed_gate_violation.csv"
)
DEFAULT_ADVISORY_ROWS = Path(
    "reports/phase3ce1_field_registry_gate_smoke_20260618/advisory_registry_gate_audit.csv"
)
DEFAULT_OUTPUT = Path("reports/phase3ce2_typed_primitive_candidate_pack_canary_20260618")
CANARY_VERSION = "phase3ce2-typed-primitive-candidate-pack-canary-v1-2026-06-18"


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    keys: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key in seen:
                continue
            seen.add(key)
            keys.append(key)
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def _split_fields(value: Any) -> list[str]:
    fields: list[str] = []
    seen: set[str] = set()
    for part in str(value or "").replace(",", "|").split("|"):
        field = part.strip().lstrip("$")
        if not field or field in seen:
            continue
        seen.add(field)
        fields.append(field)
    return fields


def _candidate(expression: str, *, field: str, category: str, primitive: str, lane: str) -> dict[str, Any]:
    return {
        "candidate_id": make_candidate_id(expression),
        "expression": expression,
        "source_lane": "phase3ce2_typed_primitive_canary",
        "factor_lane": lane,
        "field": field,
        "field_category": category,
        "typed_primitive": primitive,
        "official_book_eligible": False,
        "canary_only": True,
    }


def _collect_fields(rows: list[dict[str, Any]], limit_per_category: int) -> dict[str, list[str]]:
    by_category: dict[str, list[str]] = {
        "sparse_event": [],
        "discrete_state": [],
        "coverage_sensitive": [],
    }
    seen: set[str] = set()
    for row in rows:
        for field in _split_fields(row.get("blocked_fields") or row.get("field") or row.get("field_name")):
            category = field_category(field)
            if category not in by_category or field in seen:
                continue
            seen.add(field)
            if len(by_category[category]) < limit_per_category:
                by_category[category].append(field)
    return by_category


def _build_candidates(fields_by_category: dict[str, list[str]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for field in fields_by_category.get("sparse_event", []):
        rows.append(
            _candidate(
                f"CSRank(EventCount(${field},5))",
                field=field,
                category="sparse_event",
                primitive="EventCount",
                lane="event_state",
            )
        )
        rows.append(
            _candidate(
                f"CSRank(EventAge(${field}))",
                field=field,
                category="sparse_event",
                primitive="EventAge",
                lane="event_state",
            )
        )
    for field in fields_by_category.get("discrete_state", []):
        rows.append(
            _candidate(
                f"CSRank(StateDwell(${field},5))",
                field=field,
                category="discrete_state",
                primitive="StateDwell",
                lane="state_lifecycle",
            )
        )
        rows.append(
            _candidate(
                f"CSRank(WindowStateCount(${field},10))",
                field=field,
                category="discrete_state",
                primitive="WindowStateCount",
                lane="state_lifecycle",
            )
        )
    for field in fields_by_category.get("coverage_sensitive", []):
        rows.append(
            _candidate(
                f"CSRank(ValidRatioGate(${field},60,0.8))",
                field=field,
                category="coverage_sensitive",
                primitive="ValidRatioGate",
                lane="coverage_guarded",
            )
        )
        rows.append(
            _candidate(
                f"CSRank(MaskedZScore(${field},60,0.8))",
                field=field,
                category="coverage_sensitive",
                primitive="MaskedZScore",
                lane="coverage_guarded",
            )
        )
        rows.append(
            _candidate(
                f"CSRank(SafeCSResidual(${field},$amount,20,5,0.8))",
                field=field,
                category="coverage_sensitive",
                primitive="SafeCSResidual",
                lane="coverage_guarded",
            )
        )
    return rows


def _counter(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    return dict(sorted(Counter(str(row.get(key) or "unknown") for row in rows).items()))


def _write_markdown(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Phase3CE2 Typed Primitive Candidate Pack Canary",
        "",
        f"- created_at: {summary['created_at']}",
        f"- registry_version: `{summary['registry_version']}`",
        f"- candidate_rows: {summary['candidate_rows']}",
        f"- typed_gate_allowed_rows: {summary['typed_gate_allowed_rows']}",
        f"- typed_gate_rejected_rows: {summary['typed_gate_rejected_rows']}",
        f"- g2_input_allowed_rows: {summary['g2_input_allowed_rows']}",
        f"- g2_input_rejected_rows: {summary['g2_input_rejected_rows']}",
        f"- evaluator_support_status: {summary['evaluator_support_status']}",
        f"- decision: {summary['decision']}",
        "",
        "## Field Category Attribution",
        "",
    ]
    for key, value in summary["field_category_counts"].items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Stop Condition Status", ""])
    for row in summary["stop_condition_status"]:
        lines.append(f"- {row['check_id']}: {row['status']} - {row['evidence']}")
    lines.extend(
        [
            "",
            "This canary does not run alpha scoring, selector scoring, replay, or official promotion.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--blocked-event-rows", type=Path, default=DEFAULT_BLOCKED_EVENT_ROWS)
    parser.add_argument("--advisory-rows", type=Path, default=DEFAULT_ADVISORY_ROWS)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--limit-per-category", type=int, default=16)
    args = parser.parse_args()

    source_rows = _read_csv(args.blocked_event_rows) + _read_csv(args.advisory_rows)
    fields_by_category = _collect_fields(source_rows, max(1, int(args.limit_per_category)))
    candidates = _build_candidates(fields_by_category)
    typed_rows = [
        validate_row(
            row,
            entry_lineage="phase3ce2_typed_primitive_canary",
            materialization_stage="candidate_pack_materialization",
            candidate_role="typed_primitive_canary",
        )
        for row in candidates
    ]
    typed_allowed = [row for row in typed_rows if str(row.get("typed_gate_decision") or "") == "allow"]
    typed_rejected = [row for row in typed_rows if str(row.get("typed_gate_decision") or "") != "allow"]
    g2_allowed, g2_rejected = gate_g2_input_rows(
        typed_allowed,
        entry_lineage="phase3ce2_typed_primitive_canary",
        materialization_stage="g2_selector_input_canary",
        candidate_role="mature_g2_candidate",
    )
    output_root = args.output_root
    output_root.mkdir(parents=True, exist_ok=True)
    _write_csv(output_root / "typed_primitive_canary_candidates.csv", typed_rows)
    _write_csv(output_root / "typed_primitive_canary_gate_rejects.csv", typed_rejected)
    _write_csv(output_root / "typed_primitive_canary_g2_allowed.csv", g2_allowed)
    _write_csv(output_root / "typed_primitive_canary_g2_rejected.csv", g2_rejected)

    required_runtime_ops = {
        "eventage",
        "eventcount",
        "statedwell",
        "windowstatecount",
        "validratiogate",
        "maskedzscore",
        "safecsresidual",
    }
    evaluator_support_status = (
        "implemented_in_real_market_validation"
        if required_runtime_ops <= set(TYPED_PRIMITIVE_OPERATORS)
        else "not_detected_in_runtime_evaluator"
    )
    field_category_counts = _counter(candidates, "field_category")
    stop_condition_status = [
        {
            "check_id": "ce2_01_entry_path",
            "status": "pass" if all(field_category_counts.get(key, 0) > 0 for key in ("sparse_event", "coverage_sensitive")) else "hold",
            "evidence": json.dumps(field_category_counts, ensure_ascii=False, sort_keys=True),
        },
        {
            "check_id": "ce2_02_old_primitive_leak",
            "status": "pass" if not typed_rejected and not g2_rejected else "fail",
            "evidence": f"typed_rejected={len(typed_rejected)}, g2_rejected={len(g2_rejected)}",
        },
        {
            "check_id": "ce2_03_to_ce2_07_runtime_semantic_tests",
            "status": "ready_for_semantic_smoke" if evaluator_support_status == "implemented_in_real_market_validation" else "pending",
            "evidence": "runtime evaluator exposes typed primitive operators; still requires semantic smoke, placebo/lag/fragment replay",
        },
    ]
    if not candidates or typed_rejected or g2_rejected:
        decision = "HOLD_TYPED_PRIMITIVE_CANARY_GATE_FAILURE"
    elif evaluator_support_status == "implemented_in_real_market_validation":
        decision = "PASS_TYPED_PRIMITIVE_ENTRY_PATH_EVALUATOR_READY_HOLD_SEMANTIC_PROOF"
    else:
        decision = "PASS_TYPED_PRIMITIVE_ENTRY_PATH_HOLD_EVALUATOR_IMPLEMENTATION"
    summary = {
        "phase3_version": CANARY_VERSION,
        "created_at": utc_now_iso(),
        "output_root": str(output_root),
        "registry_version": REGISTRY_VERSION,
        "source_rows": len(source_rows),
        "fields_by_category": fields_by_category,
        "field_category_counts": field_category_counts,
        "primitive_counts": _counter(candidates, "typed_primitive"),
        "candidate_rows": len(candidates),
        "typed_gate_allowed_rows": len(typed_allowed),
        "typed_gate_rejected_rows": len(typed_rejected),
        "g2_input_allowed_rows": len(g2_allowed),
        "g2_input_rejected_rows": len(g2_rejected),
        "evaluator_support_status": evaluator_support_status,
        "stop_condition_status": stop_condition_status,
        "decision": decision,
    }
    write_json_artifact(output_root / "typed_primitive_canary_summary.json", summary)
    _write_markdown(output_root / "PHASE3CE2_TYPED_PRIMITIVE_CANARY_20260618.md", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
