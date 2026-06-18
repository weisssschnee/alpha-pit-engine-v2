"""Create a diagnostic-only limit motif pack report.

The output is a candidate template and audit plan, not a replay result. It
explicitly keeps limit diagnostics outside the locked X0/R3 shadow object.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from our_system_phase2.services.candidate_pool_priority import enrich_candidate_pool_priority
from our_system_phase2.services.event_derived_features import event_derived_feature_contract, event_feature_spec
from our_system_phase2.services.search_memory import (
    LocalSearchMemory,
    expression_memory_key,
    production_rule_key,
    skeleton_memory_key,
)
from our_system_phase2.services.typed_primitive_gate import is_allowed, validate_row


DEFAULT_MOTIF_PACK = Path("src/our_system_phase2/formula_gen_v2/motif_pack_limit_diagnostic.yaml")
DEFAULT_O7_SUMMARY = Path("reports/phase3o7_limit_factor_chain_audit_20260517/phase3o7_limit_factor_chain_audit.json")
DEFAULT_OUTPUT_ROOT = Path("reports/phase3r_limit_motif_pack_diagnostic_20260528")
REPORT_FILENAME = "PHASE3R_LIMIT_MOTIF_PACK_DIAGNOSTIC_2026-05-28.md"


LIMIT_FIELDS = {
    "limit_event": [
        "$limit_up_event",
        "$limit_down_event",
        "$limit_up_break",
        "$limit_down_repair",
        "$limit_flip_up_to_down",
        "$limit_flip_down_to_up",
    ],
    "limit_close": ["$limit_up_close_event", "$limit_down_close_event"],
    "limit_open": ["$limit_up_open_event", "$limit_down_open_event"],
    "limit_touch": ["$limit_up_touch_event", "$limit_down_touch_event"],
    "limit_break": [
        "$limit_up_touch_not_close",
        "$limit_down_touch_not_close",
        "$limit_up_open_not_close",
        "$limit_down_open_not_close",
        "$break_board_after_streak_ge_2",
        "$break_board_after_streak_ge_3",
        "$limit_down_rebound_after_streak_ge_2",
        "$limit_down_rebound_after_streak_ge_3",
    ],
    "limit_streak": ["$limit_up_streak", "$limit_down_streak"],
    "limit_streak_ge": [
        "$limit_up_streak_ge_2",
        "$limit_up_streak_ge_3",
        "$limit_up_streak_ge_4",
        "$limit_up_touch_streak_ge_2",
        "$limit_up_touch_streak_ge_3",
        "$limit_up_touch_streak_ge_4",
        "$limit_down_streak_ge_2",
        "$limit_down_streak_ge_3",
    ],
    "high_board": [
        "$high_board_rank",
        "$market_high_board",
        "$is_market_high_board",
        "$streak_gap_to_market_high",
        "$post_market_high_board_tplus_1",
        "$post_market_high_board_tplus_2",
        "$break_after_high_board_tplus_1",
        "$break_after_high_board_tplus_2",
    ],
    "price": ["$close", "$open", "$vwap"],
    "flow": ["$amount", "$volume", "$turnover_rate"],
}

WINDOWS = {"w1": [2, 3, 5], "w2": [8, 10, 20]}

TEMPLATES = [
    ("event_factor", "CSRank(Mean({limit_close},{w1}))"),
    ("event_factor", "CSRank(Mean({limit_open},{w1}))"),
    ("event_factor", "CSRank(Mean({limit_touch},{w1}))"),
    ("event_factor", "CSRank(Mean({limit_break},{w1}))"),
    ("event_factor", "CSRank(Mean({limit_streak_ge},{w1}))"),
    ("event_factor", "CSRank(Mean({high_board},{w1}))"),
    ("event_factor", "CSRank(Mean({limit_event},{w1}))"),
    ("event_factor", "CSRank(Mean({limit_streak},{w1}))"),
    ("event_factor", "CSRank(Sub(Mean({limit_event},{w1}),Mean({limit_event},{w2})))"),
    ("interaction_factor", "CSRank(Mul(ZScore(Mean(Abs(Delta({price},1)),{w2})),ZScore(Mean({limit_event},{w1}))))"),
    ("interaction_factor", "CSRank(Mul(ZScore(Mean({flow},{w2})),ZScore(Mean({limit_event},{w1}))))"),
    ("interaction_factor", "CSRank(CSResidual(ZScore(Mean({limit_event},{w1})),CSRank(Log($final_float_market_cap))))"),
    ("interaction_factor", "CSRank(Mul(ZScore(Mean({flow},{w2})),ZScore(Mean({limit_break},{w1}))))"),
    ("interaction_factor", "CSRank(Mul(ZScore(Mean({flow},{w2})),ZScore(Mean({limit_streak_ge},{w1}))))"),
    ("interaction_factor", "CSRank(Mul(ZScore(Mean({flow},{w2})),ZScore(Mean({high_board},{w1}))))"),
    ("interaction_factor", "CSRank(CSResidual(ZScore(Mean({high_board},{w1})),CSRank(Log($final_float_market_cap))))"),
]


def _now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


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


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_json(path: Path) -> Any:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _fill_template(template: str, role: str) -> list[str]:
    expressions = [template]
    for key, values in LIMIT_FIELDS.items():
        if "{" + key + "}" not in template:
            continue
        expressions = [expr.replace("{" + key + "}", value) for expr in expressions for value in values]
    for key, values in WINDOWS.items():
        if "{" + key + "}" not in template:
            continue
        expressions = [expr.replace("{" + key + "}", str(value)) for expr in expressions for value in values]
    # Keep the first version compact. Direct event coverage is more important
    # than enumerating every redundant window pair.
    deduped = []
    seen = set()
    for expression in expressions:
        if "{w" in expression:
            continue
        if expression in seen:
            continue
        seen.add(expression)
        deduped.append(expression)
    return deduped


def _expression_fields(expression: str) -> list[str]:
    fields = []
    seen = set()
    for token in re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", expression):
        normalized = token.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        fields.append(normalized)
    return fields


def _take_field_breadth(expressions: list[str], cap: int) -> list[str]:
    selected: list[str] = []
    seen_field_keys: set[tuple[str, ...]] = set()
    for expression in expressions:
        key = tuple(_expression_fields(expression))
        if key in seen_field_keys:
            continue
        seen_field_keys.add(key)
        selected.append(expression)
        if len(selected) >= cap:
            return selected
    for expression in expressions:
        if expression in selected:
            continue
        selected.append(expression)
        if len(selected) >= cap:
            return selected
    return selected


def _event_metadata_for_expression(expression: str) -> dict[str, Any]:
    event_specs = []
    for field in _expression_fields(expression):
        spec = event_feature_spec(field)
        if spec is not None:
            event_specs.append(spec)
    event_fields = sorted({spec.field_name for spec in event_specs})
    families = sorted({spec.family for spec in event_specs})
    lag_rules = sorted({spec.lag_rule for spec in event_specs})
    tradability_rules = sorted({spec.tradability_rule for spec in event_specs})
    leakage_flags = sorted({spec.leakage_flag for spec in event_specs})
    digest_source = "|".join([*families, *event_fields]) or expression
    digest = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()[:16]
    return {
        "feature_adapter": "event_derived_feature_layer" if event_specs else "legacy_or_non_event",
        "event_fields": "|".join(event_fields),
        "event_family": "|".join(families),
        "lag_rule": "|".join(lag_rules) if lag_rules else "standard_market_field_lag_policy",
        "tradability_rule": "|".join(tradability_rules) if tradability_rules else "standard_tradability_policy",
        "leakage_flag": "|".join(leakage_flags) if leakage_flags else "none_detected",
        "search_memory_key": f"event_adapter:{digest}",
        "contains_new_event_adapter_field": any(
            field
            not in {
                "limit_up_event",
                "limit_down_event",
                "limit_up_streak",
                "limit_down_streak",
                "limit_up_break",
                "limit_down_repair",
                "limit_flip_up_to_down",
                "limit_flip_down_to_up",
            }
            for field in event_fields
        ),
    }


def _candidate_rows(max_per_role: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    role_counts: dict[str, int] = {}
    per_template_cap = max(1, min(3, int(max_per_role)))
    for role, template in TEMPLATES:
        for expression in _take_field_breadth(_fill_template(template, role), per_template_cap):
            count = role_counts.get(role, 0)
            if count >= max_per_role:
                break
            role_counts[role] = count + 1
            metadata = _event_metadata_for_expression(expression)
            enriched = (
                enrich_candidate_pool_priority(
                    {
                    "candidate_id": f"limit_diag_{role}_{role_counts[role]:03d}",
                    "diagnostic_role": role,
                    "expression": expression,
                    "uses_limit_token": bool(re.search(r"(limit_|high_board|market_high)", expression)),
                    "official_book_eligible": False,
                    "required_lag_days": 1,
                    "required_audits": "gate_lag_check|tradability_exclusion_check|same_day_leakage_check",
                    **metadata,
                    }
                )
            )
            rows.append(
                validate_row(
                    enriched,
                    entry_lineage="phase3r_limit_diagnostic",
                    materialization_stage="candidate_materialization",
                    candidate_role="diagnostic",
                )
            )
    rows.extend(
        [
            validate_row(
                enrich_candidate_pool_priority(
                    {
                    "candidate_id": "limit_diag_r3_gate_001",
                    "diagnostic_role": "r3_secondary_gate",
                    "expression": "R3_liquidity_low AND limit_density_high",
                    "uses_limit_token": True,
                    "official_book_eligible": False,
                    "required_lag_days": 1,
                    "required_audits": "R3_2x2|random_active_day_placebo|inverted_gate",
                    "feature_adapter": "event_derived_feature_layer",
                    "event_fields": "limit_density_high",
                    "event_family": "limit_density",
                    "lag_rule": "gate_definition_must_use_lagged_regime_state",
                    "tradability_rule": "secondary_gate_not_direct_tradeability_filter",
                    "leakage_flag": "requires_gate_lag_audit",
                    "search_memory_key": "event_adapter:r3_limit_density_high",
                    "contains_new_event_adapter_field": False,
                    }
                ),
                entry_lineage="phase3r_limit_diagnostic",
                materialization_stage="candidate_materialization",
                candidate_role="diagnostic_gate",
            ),
            validate_row(
                enrich_candidate_pool_priority(
                    {
                    "candidate_id": "limit_diag_r3_gate_002",
                    "diagnostic_role": "r3_secondary_gate",
                    "expression": "R3_liquidity_low AND limit_density_not_high",
                    "uses_limit_token": True,
                    "official_book_eligible": False,
                    "required_lag_days": 1,
                    "required_audits": "R3_2x2|random_active_day_placebo|inverted_gate",
                    "feature_adapter": "event_derived_feature_layer",
                    "event_fields": "limit_density_not_high",
                    "event_family": "limit_density",
                    "lag_rule": "gate_definition_must_use_lagged_regime_state",
                    "tradability_rule": "secondary_gate_not_direct_tradeability_filter",
                    "leakage_flag": "requires_gate_lag_audit",
                    "search_memory_key": "event_adapter:r3_limit_density_not_high",
                    "contains_new_event_adapter_field": False,
                    }
                ),
                entry_lineage="phase3r_limit_diagnostic",
                materialization_stage="candidate_materialization",
                candidate_role="diagnostic_gate",
            ),
        ]
    )
    return rows


def _memory_filter_rows(
    rows: list[dict[str, Any]],
    *,
    previous_memory_root: Path | None,
    dataset_role: str | None,
    run_id: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    memory = LocalSearchMemory.from_previous_run(previous_memory_root, expected_dataset_role=dataset_role)
    kept: list[dict[str, Any]] = []
    duplicate_count = 0
    for row in rows:
        expression = str(row.get("expression") or "")
        if not expression:
            kept.append(row)
            continue
        is_gate = row.get("diagnostic_role") == "r3_secondary_gate"
        if not is_gate and memory.has_seen_expression(expression):
            duplicate_count += 1
            memory.record_duplicate_skip(
                expression=expression,
                run_id=run_id,
                round_index=0,
                lane=str(row.get("diagnostic_role") or "limit_event_diagnostic"),
                source_mode="event_derived_feature_layer",
                reason="event_adapter_search_memory_duplicate_expression",
            )
            continue
        kept.append(row)
        if not is_gate:
            expression_key = expression_memory_key(expression)
            skeleton_key = skeleton_memory_key(expression)
            memory.expression_keys.add(expression_key)
            memory.skeleton_keys.add(skeleton_key)
            memory.records.append(
                {
                    "run_id": run_id,
                    "candidate_id": row.get("candidate_id") or expression_key,
                    "expression": expression,
                    "expression_key": expression_key,
                    "skeleton_key": skeleton_key,
                    "production_rule_key": production_rule_key(
                        source_mode="event_derived_feature_layer",
                        frontier_lane=str(row.get("diagnostic_role") or "limit_event_diagnostic"),
                        generation_context={
                            "source": "phase3r_limit_motif_pack_diagnostic",
                            "feature_adapter": row.get("feature_adapter"),
                            "event_family": row.get("event_family"),
                        },
                    ),
                    "source_mode": "event_derived_feature_layer",
                    "frontier_lane": row.get("diagnostic_role"),
                    "retained": False,
                    "label": "diagnostic_candidate_template",
                    "real_replay_dataset_role": dataset_role,
                    "feature_adapter": row.get("feature_adapter"),
                    "event_fields": row.get("event_fields"),
                    "event_family": row.get("event_family"),
                    "search_memory_key": row.get("search_memory_key"),
                }
            )
    return kept, {
        "active": True,
        "run_id": run_id,
        "previous_memory_root": str(previous_memory_root) if previous_memory_root else None,
        "dataset_role": dataset_role,
        "input_candidate_count": len(rows),
        "kept_candidate_count": len(kept),
        "duplicate_skip_count": duplicate_count,
        "search_memory": memory.report(run_id=run_id),
    }


def run(
    *,
    motif_pack: Path,
    o7_summary_path: Path,
    output_root: Path,
    max_per_role: int,
    previous_memory_root: Path | None = None,
    dataset_role: str = "stock_pit_panel",
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    o7 = _read_json(o7_summary_path)
    run_id = "phase3r_limit_motif_pack_diagnostic_v2_event_adapter_memory"
    all_rows = _candidate_rows(max_per_role=max_per_role)
    rejected_rows = [row for row in all_rows if not is_allowed(row)]
    allowed_rows = [row for row in all_rows if is_allowed(row)]
    rows, memory_report = _memory_filter_rows(
        allowed_rows,
        previous_memory_root=previous_memory_root,
        dataset_role=dataset_role,
        run_id=run_id,
    )
    _write_csv(output_root / "phase3r_limit_diagnostic_candidate_templates.csv", rows)
    _write_csv(output_root / "phase3r_limit_diagnostic_reject_ledger.csv", rejected_rows)
    _write_json(output_root / "search_memory.json", memory_report["search_memory"])
    candidate_ledger = {
        "run_id": run_id,
        "created_at": _now(),
        "scope": "diagnostic_only_no_retraining_no_X0_R3_changes",
        "feature_adapter_contract": event_derived_feature_contract(max_streak_n=10),
        "proof_variant": "limit_motif_pack_diagnostic",
        "record_count": len(rows),
        "typed_gate_reject_count": len(rejected_rows),
        "records": rows,
        "reject_records": rejected_rows,
        "search_memory_report": {
            key: value for key, value in memory_report.items() if key != "search_memory"
        },
        "schema_version": "phase3r_limit_diagnostic_ledger_v2_event_adapter_metadata",
    }
    _write_json(output_root / "phase3r_limit_diagnostic_candidate_ledger.json", candidate_ledger)
    summary = {
        "created_at": _now(),
        "decision": "PASS_LIMIT_MOTIF_DIAGNOSTIC_SCAFFOLD_CREATED_WITH_TYPED_GATE",
        "scope": "diagnostic_only_no_retraining_no_X0_R3_changes",
        "motif_pack": str(motif_pack),
        "o7_prior_decision": o7.get("decision"),
        "candidate_template_count": len(rows),
        "pre_memory_candidate_template_count": len(all_rows),
        "typed_gate_allowed_count": len(allowed_rows),
        "typed_gate_reject_count": len(rejected_rows),
        "new_event_adapter_candidate_count": int(sum(bool(row.get("contains_new_event_adapter_field")) for row in rows)),
        "search_memory": {
            key: value for key, value in memory_report.items() if key != "search_memory"
        },
        "roles": sorted({row["diagnostic_role"] for row in rows}),
        "hard_boundaries": [
            "not_official_budget",
            "not_X0_book_eligible",
            "same_day_limit_status_disallowed",
            "must_use_lagged_features",
            "requires_tradability_failure_audit_before_replay",
        ],
        "next_action": "Run cheap diagnostic evaluation only if the locked X0/R3 shadow continues unchanged.",
        "outputs": {
            "candidate_templates_csv": str(output_root / "phase3r_limit_diagnostic_candidate_templates.csv"),
            "reject_ledger_csv": str(output_root / "phase3r_limit_diagnostic_reject_ledger.csv"),
            "candidate_ledger_json": str(output_root / "phase3r_limit_diagnostic_candidate_ledger.json"),
            "search_memory_json": str(output_root / "search_memory.json"),
            "summary_json": str(output_root / "phase3r_limit_motif_pack_diagnostic.json"),
            "summary_md": str(output_root / REPORT_FILENAME),
        },
    }
    _write_json(output_root / "phase3r_limit_motif_pack_diagnostic.json", summary)
    md = [
        "# Phase3R Limit Motif Pack Diagnostic",
        "",
        f"- decision: `{summary['decision']}`",
        f"- prior O7 decision: `{summary['o7_prior_decision']}`",
        f"- candidate_template_count: `{summary['candidate_template_count']}`",
        f"- pre_memory_candidate_template_count: `{summary['pre_memory_candidate_template_count']}`",
        f"- duplicate_skip_count: `{summary['search_memory']['duplicate_skip_count']}`",
        f"- search_memory_json: `{summary['outputs']['search_memory_json']}`",
        "- status: diagnostic only; not official book budget.",
        "",
        "## Roles",
        "",
        "- event_factor",
        "- interaction_factor",
        "- r3_secondary_gate",
        "",
        "## Hard Boundaries",
        "",
    ]
    md.extend(f"- `{item}`" for item in summary["hard_boundaries"])
    md.extend(
        [
            "",
            "## Required Interpretation",
            "",
            "- A good limit diagnostic result may justify a future diagnostic replay.",
            "- It does not change `X0_official_6_R3_liquidity_low_v1`.",
            "- Same-day limit status is not allowed as a signal feature.",
            "",
        ]
    )
    (output_root / REPORT_FILENAME).write_text("\n".join(md), encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--motif-pack", type=Path, default=DEFAULT_MOTIF_PACK)
    parser.add_argument("--o7-summary", type=Path, default=DEFAULT_O7_SUMMARY)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--max-per-role", type=int, default=24)
    parser.add_argument("--previous-memory-root", type=Path, default=None)
    parser.add_argument("--dataset-role", default="stock_pit_panel")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = run(
        motif_pack=args.motif_pack,
        o7_summary_path=args.o7_summary,
        output_root=args.output_root,
        max_per_role=args.max_per_role,
        previous_memory_root=args.previous_memory_root,
        dataset_role=args.dataset_role,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
