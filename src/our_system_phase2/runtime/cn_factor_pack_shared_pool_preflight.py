from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from our_system_phase2.services.typed_primitive_gate import validate_row


DEFAULT_ENRICHED_POOL = Path("runtime/cn_factor_pack_phase3aa_preflight_light_20260531/shared_candidate_pool_event_enriched.json")
DEFAULT_FACTOR_PACK = Path("runtime/factor_packs/cn_event_factor_candidate_pack_v1_20260531.json")
DEFAULT_FIELD_PACK = Path("runtime/field_registry/cn_field_factor_field_pack_v1_20260531.json")
DEFAULT_DERIVED_PANEL = Path("runtime/derived_features/cn_event_daily_features_v1_20260531.parquet")
DEFAULT_MATURE_PANEL = Path("G:/Project_V7_Rotation/scripts/data/phase2_stock_tdx_official_20250806_to_20260508_maxopt.parquet")
DEFAULT_SELECTOR_REPORT = Path("runtime/cn_factor_pack_phase3aa_micro_selector_20260531/selector/aa/phase3_selection_only_report.json")
DEFAULT_SELECTOR_PREFLIGHT = Path("runtime/cn_factor_pack_phase3aa_micro_selector_20260531/selector/aa/phase3e_selector_feature_preflight.json")
DEFAULT_OUTPUT_DIR = Path("reports/cn_factor_pack_shared_pool_preflight_20260531")


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


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


def _parquet_columns(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return set(pq.ParquetFile(path).schema_arrow.names)


def _expr_fields(expression: str) -> list[str]:
    seen: set[str] = set()
    fields: list[str] = []
    for token in re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", expression or ""):
        field = token.lower()
        if field not in seen:
            seen.add(field)
            fields.append(field)
    return fields


def _counter_to_dict(counter: Counter[str]) -> dict[str, int]:
    return {key: int(counter[key]) for key in sorted(counter)}


def _field_pack_lookup(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    payload = _read_json(path)
    return {str(row.get("field")): dict(row) for row in payload.get("records") or [] if row.get("field")}


def _factor_pack_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = _read_json(path)
    return [dict(row) for row in payload.get("candidate_rows") or [] if row.get("expression")]


def _selector_artifact(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return dict(_read_json(path))
    except Exception:
        return None


def _build_field_rows(
    *,
    factor_rows: list[dict[str, Any]],
    field_lookup: dict[str, dict[str, Any]],
    derived_columns: set[str],
    mature_columns: set[str],
) -> list[dict[str, Any]]:
    field_to_lanes: dict[str, set[str]] = defaultdict(set)
    field_to_expr_count: Counter[str] = Counter()
    for row in factor_rows:
        lane = str(row.get("factor_lane") or row.get("diagnostic_role") or "unknown")
        for field in _expr_fields(str(row.get("expression") or "")):
            field_to_lanes[field].add(lane)
            field_to_expr_count[field] += 1

    rows: list[dict[str, Any]] = []
    for field in sorted(field_to_expr_count):
        meta = field_lookup.get(field, {})
        rows.append(
            {
                "field": field,
                "factor_expression_count": int(field_to_expr_count[field]),
                "factor_lanes": "|".join(sorted(field_to_lanes[field])),
                "family": meta.get("family"),
                "role": meta.get("role"),
                "status": meta.get("status"),
                "lag_policy": meta.get("lag_policy"),
                "leakage_flag": meta.get("leakage_flag"),
                "in_derived_feature_panel": field in derived_columns,
                "in_mature_signal_vector_panel": field in mature_columns,
            }
        )
    return rows


def _build_candidate_rows(factor_rows: list[dict[str, Any]], enriched_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched_by_expr = {str(row.get("expression") or ""): row for row in enriched_rows if row.get("expression")}
    rows: list[dict[str, Any]] = []
    for row in factor_rows:
        expression = str(row.get("expression") or "")
        enriched = enriched_by_expr.get(expression, {})
        candidate = {
                "candidate_id": row.get("candidate_id"),
                "expression": expression,
                "factor_lane": row.get("factor_lane"),
                "diagnostic_role": row.get("diagnostic_role"),
                "event_fields": row.get("event_fields"),
                "in_enriched_shared_pool": bool(enriched),
                "enriched_candidate_id": enriched.get("candidate_id"),
                "enriched_source_lane": enriched.get("source_lane"),
                "enriched_phase3aa_factor_candidate": enriched.get("phase3aa_factor_candidate"),
                "official_book_eligible": row.get("official_book_eligible"),
        }
        rows.append(
            validate_row(
                candidate,
                entry_lineage="factor_pack_preflight",
                materialization_stage="preflight_write",
                candidate_role="selector_candidate",
            )
        )
    return rows


def build_report(
    *,
    enriched_pool_path: Path,
    factor_pack_path: Path,
    field_pack_path: Path,
    derived_panel_path: Path,
    mature_panel_path: Path,
    selector_report_path: Path,
    selector_preflight_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)

    pool = _read_json(enriched_pool_path)
    factor_rows = _factor_pack_rows(factor_pack_path)
    field_lookup = _field_pack_lookup(field_pack_path)
    enriched_rows = [dict(row) for row in pool.get("candidate_pool") or []]
    enrichment = dict(pool.get("phase3aa_enrichment") or {})

    derived_columns = _parquet_columns(derived_panel_path)
    mature_columns = _parquet_columns(mature_panel_path)

    field_rows = _build_field_rows(
        factor_rows=factor_rows,
        field_lookup=field_lookup,
        derived_columns=derived_columns,
        mature_columns=mature_columns,
    )
    candidate_rows = _build_candidate_rows(factor_rows, enriched_rows)
    typed_blocked_rows = [
        row for row in candidate_rows if str(row.get("typed_gate_decision") or "") != "allow"
    ]

    missing_derived = [row["field"] for row in field_rows if not row["in_derived_feature_panel"]]
    missing_mature = [row["field"] for row in field_rows if not row["in_mature_signal_vector_panel"]]
    factor_pack_integrated = sum(1 for row in candidate_rows if row["in_enriched_shared_pool"])

    source_counts = Counter(str(row.get("source_lane") or "unknown") for row in enriched_rows)
    factor_lane_counts = Counter(str(row.get("factor_lane") or "unknown") for row in factor_rows)
    field_status_counts = Counter(str(row.get("status") or "unknown") for row in field_rows)

    selector_report = _selector_artifact(selector_report_path)
    selector_preflight = _selector_artifact(selector_preflight_path)
    selector_summary = {
        "selector_report_path": str(selector_report_path),
        "selector_report_present": selector_report is not None,
        "selected_count": None,
        "event_candidates_selected": None,
        "selector_uses_forbidden_fields": None,
        "signal_vector_proxy_requirement_pass": None,
        "signal_vector_store_ready": None,
    }
    if selector_report:
        checks = selector_report.get("selector_checks") or {}
        selector_summary.update(
            {
                "selected_count": (selector_report.get("parameters") or {}).get("selected_count"),
                "event_candidates_selected": checks.get("event_candidates_selected"),
                "selector_uses_forbidden_fields": (checks.get("forbidden_label_guard") or {}).get("selector_uses_forbidden_fields"),
                "signal_vector_proxy_requirement_pass": checks.get("signal_vector_proxy_requirement_pass"),
            }
        )
    if selector_preflight:
        selector_summary["signal_vector_store_ready"] = selector_preflight.get("signal_vector_store_ready")
        selector_summary["signal_vector_proxy_requirement_pass"] = selector_preflight.get(
            "signal_vector_proxy_requirement_pass",
            selector_summary["signal_vector_proxy_requirement_pass"],
        )

    if typed_blocked_rows:
        decision = "HOLD_TYPED_PRIMITIVE_GATE_BLOCKS"
    elif missing_derived:
        decision = "HOLD_FEATURE_PANEL_MISSING_FIELDS"
    elif missing_mature:
        decision = "PASS_FACTOR_PACK_ENRICHMENT_HOLD_SIGNAL_VECTOR_DATASET_JOIN"
    elif selector_summary["signal_vector_proxy_requirement_pass"] is False:
        decision = "PASS_FACTOR_PACK_ENRICHMENT_HOLD_SIGNAL_VECTOR_STORE"
    else:
        decision = "PASS_FACTOR_PACK_READY_FOR_G2_SELECTOR"

    payload = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "decision": decision,
        "scope": "field_factor_pack_to_mature_shared_pool_preflight_no_replay",
        "inputs": {
            "enriched_pool_path": str(enriched_pool_path),
            "factor_pack_path": str(factor_pack_path),
            "field_pack_path": str(field_pack_path),
            "derived_panel_path": str(derived_panel_path),
            "mature_panel_path": str(mature_panel_path),
        },
        "counts": {
            "enriched_pool_rows": len(enriched_rows),
            "factor_pack_rows": len(factor_rows),
            "typed_gate_blocked_rows": len(typed_blocked_rows),
            "factor_pack_rows_in_enriched_pool": factor_pack_integrated,
            "factor_pack_rows_not_in_enriched_pool": len(factor_rows) - factor_pack_integrated,
            "factor_fields_used": len(field_rows),
            "factor_fields_missing_from_derived_panel": len(missing_derived),
            "factor_fields_missing_from_mature_signal_vector_panel": len(missing_mature),
            "memory_duplicate_skipped": int(enrichment.get("duplicate_memory_skipped") or 0),
            "event_rows_added": int(enrichment.get("event_rows_added") or 0),
            "pre_dedup_event_source_rows": int(enrichment.get("pre_dedup_event_source_rows") or 0),
            "factor_pack_source_rows": int(enrichment.get("factor_pack_source_rows") or 0),
        },
        "by_source_lane": _counter_to_dict(source_counts),
        "by_factor_lane": _counter_to_dict(factor_lane_counts),
        "by_field_status": _counter_to_dict(field_status_counts),
        "missing_from_derived_panel": missing_derived,
        "missing_from_mature_signal_vector_panel": missing_mature,
        "selector_micro_smoke": selector_summary,
        "policy": {
            "official_x0_r3": "read_only",
            "promotion": "not_allowed_from_this_preflight",
            "allowed_next_step": "join derived feature panel into mature evaluator/signal-vector dataset, then rerun frozen G2 selector",
        },
    }

    _write_json(output_dir / "cn_factor_pack_shared_pool_preflight.json", payload)
    _write_csv(output_dir / "cn_factor_pack_field_coverage.csv", field_rows)
    _write_csv(output_dir / "cn_factor_pack_candidate_integration.csv", candidate_rows)
    _write_csv(output_dir / "typed_gate_violation.csv", typed_blocked_rows)
    _write_markdown(output_dir / "CN_FACTOR_PACK_SHARED_POOL_PREFLIGHT_2026-05-31.md", payload)
    return payload


def _write_markdown(path: Path, payload: dict[str, Any]) -> None:
    counts = payload["counts"]
    lines = [
        "# CN Factor Pack Shared Pool Preflight",
        "",
        f"decision: `{payload['decision']}`",
        "",
        "## Scope",
        "",
        "This is a no-replay, no-promotion preflight for field/factor pack integration into the mature shared pool.",
        "",
        "## Counts",
        "",
        f"- enriched pool rows: {counts['enriched_pool_rows']}",
        f"- factor pack rows: {counts['factor_pack_rows']}",
        f"- typed gate blocked rows: {counts.get('typed_gate_blocked_rows', 0)}",
        f"- factor pack rows found in enriched pool: {counts['factor_pack_rows_in_enriched_pool']}",
        f"- event rows added after search-memory dedupe: {counts['event_rows_added']}",
        f"- memory duplicates skipped: {counts['memory_duplicate_skipped']}",
        f"- factor fields used: {counts['factor_fields_used']}",
        f"- missing from derived feature panel: {counts['factor_fields_missing_from_derived_panel']}",
        f"- missing from mature signal-vector panel: {counts['factor_fields_missing_from_mature_signal_vector_panel']}",
        "",
        "## Factor Lanes",
        "",
    ]
    for lane, count in payload["by_factor_lane"].items():
        lines.append(f"- {lane}: {count}")
    lines.extend(["", "## Selector Micro Smoke", ""])
    for key, value in payload["selector_micro_smoke"].items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Interpretation", ""])
    decision = payload["decision"]
    if decision == "PASS_FACTOR_PACK_ENRICHMENT_HOLD_SIGNAL_VECTOR_DATASET_JOIN":
        lines.append(
            "The factor pack is visible in the mature shared pool and search-memory dedupe is active, "
            "but the mature signal-vector panel does not yet contain the new derived event fields."
        )
        lines.append(
            "Do not rerun large G2 selection until the derived feature panel is joined into the evaluator/signal-vector dataset."
        )
    elif decision.startswith("HOLD_"):
        lines.append("The field/factor layer is not ready for mature-chain selector use. Fix the blocker before selector runs.")
    else:
        lines.append("The field/factor layer passed this preflight. Selector/replay gates are still separate.")
    lines.extend(["", "## Outputs", ""])
    lines.append("- cn_factor_pack_shared_pool_preflight.json")
    lines.append("- cn_factor_pack_field_coverage.csv")
    lines.append("- cn_factor_pack_candidate_integration.csv")
    lines.append("- typed_gate_violation.csv")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--enriched-pool", type=Path, default=DEFAULT_ENRICHED_POOL)
    parser.add_argument("--factor-pack", type=Path, default=DEFAULT_FACTOR_PACK)
    parser.add_argument("--field-pack", type=Path, default=DEFAULT_FIELD_PACK)
    parser.add_argument("--derived-panel", type=Path, default=DEFAULT_DERIVED_PANEL)
    parser.add_argument("--mature-panel", type=Path, default=DEFAULT_MATURE_PANEL)
    parser.add_argument("--selector-report", type=Path, default=DEFAULT_SELECTOR_REPORT)
    parser.add_argument("--selector-preflight", type=Path, default=DEFAULT_SELECTOR_PREFLIGHT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    payload = build_report(
        enriched_pool_path=args.enriched_pool,
        factor_pack_path=args.factor_pack,
        field_pack_path=args.field_pack,
        derived_panel_path=args.derived_panel,
        mature_panel_path=args.mature_panel,
        selector_report_path=args.selector_report,
        selector_preflight_path=args.selector_preflight,
        output_dir=args.output_dir,
    )
    print(json.dumps({"decision": payload["decision"], "counts": payload["counts"]}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
