"""Phase3CD AST primitive assumption audit for new CN field integration.

This is a design/audit artifact, not an alpha proof and not a search launcher.
It records where the mature AST primitive assumptions conflict with newly
introduced sparse event, discrete state, coverage-sensitive, and context fields.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = Path("reports/phase3cd_ast_primitive_assumption_audit_20260618")

EVALUATOR_PATH = Path("src/our_system_phase2/services/real_market_validation.py")
FREEFORM_PATH = Path("src/our_system_phase2/formula_gen_v2/freeform_sampler.py")
CORE_MOTIF_PATH = Path("src/our_system_phase2/formula_gen_v2/motif_pack_core.yaml")
LIMIT_MOTIF_PATH = Path("src/our_system_phase2/formula_gen_v2/motif_pack_limit_diagnostic.yaml")
TRUE1MIN_ADAPTER_PATH = Path("src/our_system_phase2/runtime/phase3aq_true_1min_formula_adapter.py")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _read_text(path: Path) -> str:
    full = _resolve(path)
    if not full.exists():
        return ""
    return full.read_text(encoding="utf-8", errors="ignore")


def _primitive_rows() -> list[dict[str, Any]]:
    return [
        {
            "primitive": "CSRank/Rank",
            "status": "existing",
            "assumption_class": "cross_section_geometry",
            "implicit_assumption": "same timestamp cross-section has enough comparable numeric values and non-degenerate ordering",
            "highest_risk_field_categories": "sparse_event|discrete_state|coverage_sensitive|key_or_membership",
            "failure_mode": "ranks availability, event sparsity, or group membership instead of signal strength",
            "default_action": "allow_continuous_only; typed_event_or_state_required_for_sparse_inputs",
        },
        {
            "primitive": "ZScore",
            "status": "existing",
            "assumption_class": "cross_section_scale",
            "implicit_assumption": "cross-section variance is meaningful and not mostly zero, missing, or discrete",
            "highest_risk_field_categories": "sparse_event|discrete_state|coverage_sensitive",
            "failure_mode": "turns rare events or coverage masks into extreme standardized values",
            "default_action": "allow_continuous_only; use MaskedZScore or state/event primitives otherwise",
        },
        {
            "primitive": "Mean/Std/WMA/Med/Kurt/Skew",
            "status": "existing",
            "assumption_class": "time_continuity",
            "implicit_assumption": "per-code series is regularly sampled and semantically continuous",
            "highest_risk_field_categories": "sparse_event|daily_context|pit_fundamental|disclosure_event",
            "failure_mode": "rolling window measures update cadence or stale step functions rather than economics",
            "default_action": "allow_continuous_minute; typed window count/state dwell for events and states",
        },
        {
            "primitive": "Delta/Delay",
            "status": "existing",
            "assumption_class": "time_ordering",
            "implicit_assumption": "lagged observations are comparable and field availability is not the signal",
            "highest_risk_field_categories": "sparse_event|daily_context|pit_fundamental|timestamped_event",
            "failure_mode": "captures publication/update timing, event absence, or stale context jumps",
            "default_action": "use explicit LaggedContext, EventTransition, StateTransition, or SinceLastEvent",
        },
        {
            "primitive": "Mom",
            "status": "existing",
            "assumption_class": "ratio_scale",
            "implicit_assumption": "lagged denominator is non-zero, continuous, and ratio-comparable",
            "highest_risk_field_categories": "sparse_event|discrete_state|fundamental_step|coverage_sensitive",
            "failure_mode": "explodes around zeros or turns step changes into false momentum",
            "default_action": "block for sparse/discrete/context fields; use delta or bounded ratio pack only after scale audit",
        },
        {
            "primitive": "Corr/Cov",
            "status": "existing",
            "assumption_class": "window_joint_variation",
            "implicit_assumption": "rolling window has enough joint valid continuous variation",
            "highest_risk_field_categories": "sparse_event|limit_state|coverage_sensitive|daily_context",
            "failure_mode": "measures availability, limit-lock trading freeze, or coverage mask co-movement",
            "default_action": "require ValidRatioGate and MaskedCorr; otherwise block",
        },
        {
            "primitive": "CSResidual",
            "status": "existing",
            "assumption_class": "cross_section_linear_model",
            "implicit_assumption": "regressor and target are continuous with stable cross-sectional variance",
            "highest_risk_field_categories": "membership|sector|coverage_sensitive|sparse_event|discrete_state",
            "failure_mode": "residualizes group membership, universe masks, or availability rather than risk exposure",
            "default_action": "allow only audited continuous controls; use SafeCSResidual with coverage and category bans",
        },
        {
            "primitive": "Div",
            "status": "existing",
            "assumption_class": "numeric_safety_vs_semantic_safety",
            "implicit_assumption": "denominator is meaningful ratio scale; zero guard only handles arithmetic safety",
            "highest_risk_field_categories": "sparse_event|discrete_state|coverage_sensitive",
            "failure_mode": "creates extreme buckets from sparse denominators despite eps guard",
            "default_action": "require denominator family audit; block sparse/discrete denominator fields",
        },
        {
            "primitive": "Log",
            "status": "existing",
            "assumption_class": "positive_magnitude",
            "implicit_assumption": "magnitude is meaningful on log scale",
            "highest_risk_field_categories": "binary_state|event_flag|signed_return|timestamp",
            "failure_mode": "collapses binary flags or hides sign semantics",
            "default_action": "allow positive continuous magnitudes only",
        },
        {
            "primitive": "Sign",
            "status": "existing",
            "assumption_class": "directional_semantics",
            "implicit_assumption": "sign encodes meaningful up/down direction",
            "highest_risk_field_categories": "event_count|state_flag|coverage_sensitive",
            "failure_mode": "turns presence, absence, or update timing into artificial direction",
            "default_action": "allow on returns/deltas; prefer EventTransition or StateTransition for events",
        },
        {
            "primitive": "Mul/Add/Sub",
            "status": "existing",
            "assumption_class": "composition_geometry",
            "implicit_assumption": "inputs share compatible scale or were normalized with valid geometry",
            "highest_risk_field_categories": "mixed_event_continuous|coverage_sensitive",
            "failure_mode": "multiplies degenerate zscores or availability masks into apparently fresh signals",
            "default_action": "require input route compatibility and source attribution",
        },
        {
            "primitive": "EventAge/SinceLastEvent/EventCount",
            "status": "proposed_typed",
            "assumption_class": "event_state_semantics",
            "implicit_assumption": "event field is a timestamped or lagged sparse occurrence indicator",
            "highest_risk_field_categories": "sparse_event|timestamped_event|limit_event",
            "failure_mode": "not applicable; designed to replace direct Mean/Delta/Mom on event fields",
            "default_action": "preferred for limit/open-board/high-board/uplimit fields",
        },
        {
            "primitive": "StateAge/StateDwell/StateTransition/WindowStateCount",
            "status": "proposed_typed",
            "assumption_class": "discrete_state_semantics",
            "implicit_assumption": "state has finite labels or binary flags and should be treated as dwell/transition",
            "highest_risk_field_categories": "discrete_state|limit_state|st_state|board_state",
            "failure_mode": "not applicable; designed to replace direct ZScore/Mom on states",
            "default_action": "preferred for is_limit_up, is_st, streak_ge, board state, hotness buckets",
        },
        {
            "primitive": "ValidRatioGate/MaskedCorr/MaskedZScore/SafeCSResidual",
            "status": "proposed_typed",
            "assumption_class": "coverage_sensitive_semantics",
            "implicit_assumption": "coverage and joint valid sample size are first-class constraints",
            "highest_risk_field_categories": "coverage_sensitive|pit_fundamental|rzrq|billboard|holder",
            "failure_mode": "not applicable; designed to prevent availability-mask reward hacking",
            "default_action": "mandatory before Corr/Cov/ZScore/CSResidual on coverage-sensitive fields",
        },
    ]


FIELD_CATEGORY_EXAMPLES = {
    "continuous_minute": [
        "open",
        "high",
        "low",
        "close",
        "vwap",
        "amount",
        "volume",
        "ret_1m",
        "intraday_ret_from_open",
    ],
    "firstn_minute_derived": [
        "m1_first5_amount",
        "m1_first15_amount",
        "m1_first30_range",
        "m1_first30_vwap_return_vs_open",
    ],
    "sparse_event": [
        "up_limit_time",
        "limit_up_event",
        "limit_up_touch_not_close",
        "open_board_record",
        "break_board_after_streak_ge_3",
    ],
    "discrete_state": [
        "is_limit_up",
        "is_st",
        "limit_up_streak_ge_2",
        "market_high_board",
        "lb_2_num",
        "max_lb_num",
    ],
    "coverage_sensitive": [
        "RZRQ",
        "rzye",
        "billboard_net_amount",
        "holder_num",
        "pe_ttm",
        "pb",
        "float_market_cap_yuan",
    ],
    "membership_or_group_key": [
        "industry",
        "sector",
        "sector_code",
        "plate_code",
        "concept",
        "board",
    ],
    "timestamp_or_key": [
        "date",
        "exec_date",
        "trade_time",
        "notice_date",
        "update_time",
        "report_date",
        "code",
        "stock_code",
    ],
    "text_or_label": [
        "reason",
        "stock_name",
        "plate_name",
        "next_open_pct",
        "future_return",
        "label_1m",
    ],
}


def _field_category_rows() -> list[dict[str, Any]]:
    route = {
        "continuous_minute": "ordinary_formula_allowed",
        "firstn_minute_derived": "ordinary_formula_allowed_with_open_cutoff_contract",
        "sparse_event": "typed_event_primitive_only",
        "discrete_state": "typed_state_primitive_only",
        "coverage_sensitive": "bounded_factor_pack_or_coverage_guarded_primitive",
        "membership_or_group_key": "context_or_group_key_only_not_formula_input",
        "timestamp_or_key": "blocked_key_or_cutoff_only",
        "text_or_label": "blocked_or_nlp_diagnostic_only",
    }
    risk = {
        "continuous_minute": "low",
        "firstn_minute_derived": "medium_cutoff_sensitive",
        "sparse_event": "high",
        "discrete_state": "high",
        "coverage_sensitive": "high",
        "membership_or_group_key": "high_group_geometry",
        "timestamp_or_key": "critical_leakage",
        "text_or_label": "critical_leakage_or_non_numeric",
    }
    return [
        {
            "field_category": category,
            "examples": "|".join(examples),
            "risk_level": risk[category],
            "selector_route": route[category],
        }
        for category, examples in FIELD_CATEGORY_EXAMPLES.items()
    ]


def _matrix_decision(primitive: str, category: str) -> tuple[str, str]:
    p = primitive.lower()
    if category in {"timestamp_or_key", "text_or_label"}:
        return "BLOCK", "key/text/label/cutoff fields are not numeric alpha inputs"
    if category == "membership_or_group_key":
        if "csresidual" in p or "group" in p:
            return "REQUIRES_SEPARATE_GROUP_AUDIT", "membership geometry must be PIT/churn audited first"
        return "BLOCK_AS_FORMULA_INPUT", "may be context key only; not direct numeric input"
    if category == "continuous_minute":
        return "ALLOW_WITH_EXISTING_GUARDS", "ordinary 1min continuous field"
    if category == "firstn_minute_derived":
        return "ALLOW_WITH_CUTOFF_CONTRACT", "field is valid only after its first-N minute cutoff"
    if category == "sparse_event":
        if any(token in p for token in ("eventage", "sincelast", "eventcount")):
            return "PREFERRED_TYPED_ROUTE", "event primitive matches sparse-event semantics"
        if any(token in p for token in ("mom", "corr", "cov", "csresidual", "zscore", "mean", "std", "delta", "div")):
            return "BLOCK_OR_TYPED_REWRITE", "ordinary continuous primitive mismatches sparse-event semantics"
        return "REQUIRES_TYPED_ROUTE", "event semantics must be preserved"
    if category == "discrete_state":
        if any(token in p for token in ("stateage", "statedwell", "statetransition", "windowstatecount")):
            return "PREFERRED_TYPED_ROUTE", "state primitive matches discrete-state semantics"
        if any(token in p for token in ("zscore", "mom", "corr", "cov", "csresidual", "mean", "std", "div")):
            return "BLOCK_OR_TYPED_REWRITE", "ordinary primitive treats state as continuous"
        return "REQUIRES_TYPED_ROUTE", "state semantics must be preserved"
    if category == "coverage_sensitive":
        if any(token in p for token in ("validratiogate", "maskedcorr", "maskedzscore", "safecsresidual")):
            return "PREFERRED_TYPED_ROUTE", "coverage is an explicit guard"
        if any(token in p for token in ("corr", "cov", "csresidual", "zscore", "rank", "mom", "div")):
            return "REQUIRES_COVERAGE_GATE", "coverage masks can dominate output geometry"
        return "BOUNDED_FACTOR_PACK_FIRST", "coverage and PIT availability must be audited"
    return "REVIEW", "no default route"


def _primitive_category_matrix(primitive_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for primitive in primitive_rows:
        for category in FIELD_CATEGORY_EXAMPLES:
            decision, reason = _matrix_decision(str(primitive["primitive"]), category)
            rows.append(
                {
                    "primitive": primitive["primitive"],
                    "primitive_status": primitive["status"],
                    "field_category": category,
                    "decision": decision,
                    "reason": reason,
                }
            )
    return rows


def _scan_group_key_risk() -> list[dict[str, Any]]:
    files = [
        EVALUATOR_PATH,
        FREEFORM_PATH,
        CORE_MOTIF_PATH,
        LIMIT_MOTIF_PATH,
        TRUE1MIN_ADAPTER_PATH,
        Path("src/our_system_phase2/runtime/cn_integrated_field_utility_map_v1.py"),
        Path("src/our_system_phase2/runtime/cn_new_data_asset_integration_v1.py"),
    ]
    pattern = re.compile(
        r"GroupRank|GroupMean|GroupNeutral|GroupZ|GroupResidual|group_key|sector_residual|industry|sector|plate|concept|board",
        re.I,
    )
    rows: list[dict[str, Any]] = []
    for rel in files:
        text = _read_text(rel)
        if not text:
            rows.append({"path": str(rel), "line": "", "match": "", "risk": "missing_file"})
            continue
        for idx, line in enumerate(text.splitlines(), start=1):
            if pattern.search(line):
                rows.append(
                    {
                        "path": str(rel),
                        "line": idx,
                        "match": line.strip()[:500],
                        "risk": _classify_group_line(line),
                    }
                )
    return rows


def _classify_group_line(line: str) -> str:
    lower = line.lower()
    if any(token.lower() in lower for token in ("grouprank", "groupmean", "groupneutral", "groupz", "groupresidual")):
        return "formula_group_primitive_present"
    if "sector_residual" in lower:
        return "planned_transform_requires_group_membership_audit"
    if any(token in lower for token in ("role", "cross_section_group_key", "key_only")):
        return "key_declared_not_formula_input"
    if any(token in lower for token in ("industry", "sector", "plate", "concept", "board")):
        return "membership_or_event_context_reference"
    return "context_reference"


def _unsafe_limit_motif_rows() -> list[dict[str, Any]]:
    text = _read_text(LIMIT_MOTIF_PATH)
    rows: list[dict[str, Any]] = []
    unsafe = re.compile(r"(Mean|ZScore|CSResidual|Corr|Cov|Mom|Std|Div)\([^#]*(limit_|high_board|market_high|streak|break_board)", re.I)
    for idx, line in enumerate(text.splitlines(), start=1):
        if unsafe.search(line):
            rows.append(
                {
                    "path": str(LIMIT_MOTIF_PATH),
                    "line": idx,
                    "template": line.strip(),
                    "risk": "event_or_state_field_consumed_by_continuous_primitive",
                    "recommended_rewrite": _limit_rewrite_hint(line),
                }
            )
    return rows


def _limit_rewrite_hint(line: str) -> str:
    lower = line.lower()
    if "csresidual" in lower:
        return "SafeCSResidual(EventIntensity(...), continuous_control) only after ValidRatioGate and membership audit"
    if "mean" in lower:
        return "EventCount/WindowStateCount or StateDwell instead of Mean on sparse/state fields"
    if "zscore" in lower:
        return "MaskedZScore only after event/state transform and valid cross-section count gate"
    return "typed event/state primitive required before ordinary composition"


def _scan_registry_fields() -> list[dict[str, Any]]:
    roots = [
        REPO / "runtime/field_registry",
        REPO / "runtime/nonminute_context_panels",
        REPO / "runtime/minute_feature_panels",
    ]
    field_counter: Counter[str] = Counter()
    source_counter: Counter[str] = Counter()
    route_counter: Counter[str] = Counter()
    rows: list[dict[str, Any]] = []
    for root in roots:
        if not root.exists():
            continue
        for path in sorted(root.rglob("*.csv")):
            if path.stat().st_size > 20_000_000:
                continue
            try:
                with path.open("r", encoding="utf-8-sig", newline="") as handle:
                    reader = csv.DictReader(handle)
                    headers = list(reader.fieldnames or [])
                    if not any(h in headers for h in ("field_name", "field", "column", "selector_allowed", "route", "role")):
                        continue
                    for idx, row in enumerate(reader):
                        if idx > 5000:
                            break
                        field = str(row.get("field_name") or row.get("field") or row.get("column") or "").strip()
                        if not field:
                            continue
                        category = _classify_field_name(field, row)
                        route = str(row.get("route") or row.get("selector_role") or row.get("role") or "")
                        field_counter[category] += 1
                        source_counter[path.name] += 1
                        if route:
                            route_counter[route] += 1
                        if len(rows) < 500:
                            rows.append(
                                {
                                    "source_file": str(path.relative_to(REPO)),
                                    "field": field,
                                    "category": category,
                                    "route_or_role": route,
                                    "selector_allowed": row.get("selector_allowed") or row.get("formula_allowed") or "",
                                    "recommended_formula_route": _category_route(category),
                                }
                            )
            except Exception:
                continue
    summary_rows = [
        {"source_file": "__category_summary__", "field": key, "category": key, "route_or_role": "", "selector_allowed": "", "recommended_formula_route": str(value)}
        for key, value in sorted(field_counter.items())
    ]
    return summary_rows + rows


def _classify_field_name(field: str, row: dict[str, Any] | None = None) -> str:
    name = field.lower()
    row_text = " ".join(str(value).lower() for value in (row or {}).values())
    if name.startswith(("label_", "future_", "next_")):
        return "text_or_label"
    if name in {"code", "stock_code", "source_code6", "symbol", "date", "exec_date", "trade_time"}:
        return "timestamp_or_key"
    if any(token in name for token in ("date", "time", "notice", "update", "report")):
        return "timestamp_or_key"
    if any(token in name for token in ("name", "reason", "text", "desc", "tag")):
        return "text_or_label"
    if any(token in name for token in ("industry", "sector", "plate", "concept", "board_code", "theme")):
        return "membership_or_group_key"
    if any(token in name for token in ("limit", "uplimit", "open_board", "break_board", "fengdan", "auction", "seal")):
        return "sparse_event"
    if any(token in name for token in ("is_", "streak", "lb_", "max_lb", "hotness", "rank")):
        return "discrete_state"
    if any(token in name for token in ("rzrq", "rzye", "billboard", "holder", "pe", "pb", "ps", "market_cap", "float", "share", "fundamental")) or any(
        token in row_text for token in ("announcement", "pit", "disclosure", "coverage")
    ):
        return "coverage_sensitive"
    if name.startswith("m1_first"):
        return "firstn_minute_derived"
    if any(token in name for token in ("open", "high", "low", "close", "vwap", "amount", "volume", "ret", "range")):
        return "continuous_minute"
    return "coverage_sensitive"


def _category_route(category: str) -> str:
    return {
        "continuous_minute": "ordinary_formula_allowed",
        "firstn_minute_derived": "ordinary_formula_allowed_with_cutoff",
        "sparse_event": "typed_event_primitive_only",
        "discrete_state": "typed_state_primitive_only",
        "coverage_sensitive": "coverage_guard_or_bounded_factor_pack",
        "membership_or_group_key": "context_key_only_until_group_audit",
        "timestamp_or_key": "blocked_key_or_cutoff_only",
        "text_or_label": "blocked_or_nlp_diagnostic_only",
    }.get(category, "manual_review")


def _typed_primitive_spec_rows() -> list[dict[str, Any]]:
    return [
        {
            "primitive": "EventAge(field)",
            "input_category": "sparse_event|timestamped_event",
            "output_semantics": "minutes_or_bars_since_event_or_nan_before_event",
            "required_guards": "observable_time_contract|lag_if_no_intraday_timestamp",
            "replaces": "Delta/Mom on event flag",
        },
        {
            "primitive": "EventCount(field, window)",
            "input_category": "sparse_event",
            "output_semantics": "count of events in rolling per-code or cross-section window",
            "required_guards": "min_event_count|min_valid_ratio",
            "replaces": "Mean(event_flag, window)",
        },
        {
            "primitive": "EventTransition(prev_state, next_state)",
            "input_category": "sparse_event|discrete_state",
            "output_semantics": "typed transition such as touch_not_close or break_after_board",
            "required_guards": "state_lifecycle_definition|tradability_check",
            "replaces": "Sign(Delta(state))",
        },
        {
            "primitive": "StateDwell(state, window)",
            "input_category": "discrete_state",
            "output_semantics": "duration or count of staying in a finite state",
            "required_guards": "finite_state_contract|missing_state_policy",
            "replaces": "Mean(state, window)|ZScore(state)",
        },
        {
            "primitive": "WindowStateCount(state, window)",
            "input_category": "discrete_state",
            "output_semantics": "rolling count of a target state",
            "required_guards": "state_value_contract|min_observation_count",
            "replaces": "Mean(binary_state, window)",
        },
        {
            "primitive": "ValidRatioGate(x, window, min_ratio)",
            "input_category": "coverage_sensitive",
            "output_semantics": "mask or gated signal only when coverage is sufficient",
            "required_guards": "min_ratio >= 0.8 default unless explicitly justified",
            "replaces": "blind Corr/Cov/ZScore on sparse coverage",
        },
        {
            "primitive": "MaskedCorr(x, y, window, min_ratio)",
            "input_category": "coverage_sensitive|continuous_minute",
            "output_semantics": "rolling corr with joint-valid coverage gate",
            "required_guards": "joint_valid_ratio|min_unique_values|block_limit_freeze_windows_if_needed",
            "replaces": "Corr(x,y,window) on mixed coverage fields",
        },
        {
            "primitive": "SafeCSResidual(y, x, min_n, min_x_unique, min_valid_ratio)",
            "input_category": "coverage_sensitive|continuous_control",
            "output_semantics": "cross-sectional residual only when regression geometry is valid",
            "required_guards": "block membership keys|min_n>=20|min_valid_ratio",
            "replaces": "CSResidual on event/state/membership-sensitive fields",
        },
    ]


def _write_report(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Phase3CD AST Primitive Assumption Audit",
        "",
        "Status: diagnostic design artifact. No search was launched and no official X0/R3 state was modified.",
        "",
        "## Decision",
        "",
        "- Fresh search validation showed current true1min runs are fresh but still concentrated in base minute fields.",
        "- Current AST evaluator does not expose GroupRank/GroupMean/GroupResidual primitives, so new fields are not currently entering formula group-key geometry.",
        "- The immediate blocker is ordinary continuous primitives consuming sparse event, discrete state, and coverage-sensitive fields.",
        "- Limit diagnostic motifs already contain examples that would be unsafe if promoted into main AST budget without typed rewrites.",
        "",
        "## Key Counts",
        "",
        f"- primitive assumptions: {summary['primitive_count']}",
        f"- primitive/category matrix rows: {summary['matrix_rows']}",
        f"- group-key scan hits: {summary['group_key_scan_hits']}",
        f"- unsafe limit diagnostic motif rows: {summary['unsafe_limit_motif_rows']}",
        f"- registry sample rows: {summary['registry_sample_rows']}",
        "",
        "## Required Next Gate Before Large Search",
        "",
        "1. Block sparse event, discrete state, membership, timestamp, text, and label fields from ordinary AST inputs by default.",
        "2. Add typed event/state/coverage primitive registry and require it for those fields.",
        "3. Keep plate/industry/concept membership as audited context until PIT membership/churn and group geometry are tested.",
        "4. Run a small new-field fresh canary only after candidate generation proves that new fields actually enter the AST candidate pack.",
        "",
        "## Outputs",
        "",
        "- `primitive_assumption_matrix.csv`",
        "- `field_category_route_matrix.csv`",
        "- `primitive_x_field_category_decision_matrix.csv`",
        "- `group_key_usage_audit.csv`",
        "- `unsafe_limit_motif_rewrite_queue.csv`",
        "- `typed_primitive_spec.csv`",
        "- `typed_primitive_registry.json`",
        "- `registry_field_route_sample.csv`",
        "- `phase3cd_ast_primitive_assumption_audit_summary.json`",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _typed_registry_payload(typed_rows: list[dict[str, Any]], field_rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "phase3cd_typed_primitive_registry_v1",
        "status": "diagnostic_gate_ready_not_evaluator_enabled",
        "official_chain_mutation": False,
        "ordinary_ast_allowed_categories": [
            "continuous_minute",
            "firstn_minute_derived",
        ],
        "ordinary_ast_blocked_categories": [
            "sparse_event",
            "discrete_state",
            "coverage_sensitive",
            "membership_or_group_key",
            "timestamp_or_key",
            "text_or_label",
        ],
        "field_category_policy": {
            row["field_category"]: {
                "risk_level": row["risk_level"],
                "selector_route": row["selector_route"],
                "examples": str(row["examples"]).split("|") if row.get("examples") else [],
            }
            for row in field_rows
        },
        "typed_primitives": typed_rows,
        "pre_search_gates": [
            {
                "gate": "candidate_pack_new_field_presence",
                "requirement": "new-field canary must show nonzero sparse_event/discrete_state/coverage_sensitive fields when that lane is requested",
            },
            {
                "gate": "ordinary_ast_category_block",
                "requirement": "ordinary AST generator must reject blocked categories before candidate materialization",
            },
            {
                "gate": "event_state_typed_rewrite",
                "requirement": "limit/open-board/high-board fields must enter through EventAge/EventCount/EventTransition/StateDwell/WindowStateCount",
            },
            {
                "gate": "coverage_guard",
                "requirement": "fundamental/RZRQ/billboard/holder/capacity fields require ValidRatioGate before Corr/Cov/ZScore/CSResidual",
            },
            {
                "gate": "membership_group_audit",
                "requirement": "industry/sector/plate/concept fields remain context keys until PIT membership and group churn are audited",
            },
        ],
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_root = _resolve(Path(args.output_root))
    primitive_rows = _primitive_rows()
    field_rows = _field_category_rows()
    matrix_rows = _primitive_category_matrix(primitive_rows)
    group_rows = _scan_group_key_risk()
    unsafe_limit_rows = _unsafe_limit_motif_rows()
    registry_rows = _scan_registry_fields()
    typed_rows = _typed_primitive_spec_rows()

    _write_csv(output_root / "primitive_assumption_matrix.csv", primitive_rows)
    _write_csv(output_root / "field_category_route_matrix.csv", field_rows)
    _write_csv(output_root / "primitive_x_field_category_decision_matrix.csv", matrix_rows)
    _write_csv(output_root / "group_key_usage_audit.csv", group_rows)
    _write_csv(output_root / "unsafe_limit_motif_rewrite_queue.csv", unsafe_limit_rows)
    _write_csv(output_root / "typed_primitive_spec.csv", typed_rows)
    _write_csv(output_root / "registry_field_route_sample.csv", registry_rows)
    _write_json(output_root / "typed_primitive_registry.json", _typed_registry_payload(typed_rows, field_rows))

    group_risk_counts = Counter(str(row.get("risk")) for row in group_rows)
    matrix_decision_counts = Counter(str(row.get("decision")) for row in matrix_rows)
    summary = {
        "status": "ok",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260618_phase3cd_ast_primitive_assumption_audit",
        "decision": "PHASE3CD_TYPED_PRIMITIVE_GATE_READY_DIAGNOSTIC_ONLY",
        "official_chain_mutation": False,
        "search_launched": False,
        "primitive_count": len(primitive_rows),
        "field_category_count": len(field_rows),
        "matrix_rows": len(matrix_rows),
        "group_key_scan_hits": len(group_rows),
        "group_key_risk_counts": dict(sorted(group_risk_counts.items())),
        "unsafe_limit_motif_rows": len(unsafe_limit_rows),
        "registry_sample_rows": len(registry_rows),
        "matrix_decision_counts": dict(sorted(matrix_decision_counts.items())),
        "outputs": {
            "primitive_assumption_matrix": str(output_root / "primitive_assumption_matrix.csv"),
            "field_category_route_matrix": str(output_root / "field_category_route_matrix.csv"),
            "primitive_x_field_category_decision_matrix": str(output_root / "primitive_x_field_category_decision_matrix.csv"),
            "group_key_usage_audit": str(output_root / "group_key_usage_audit.csv"),
            "unsafe_limit_motif_rewrite_queue": str(output_root / "unsafe_limit_motif_rewrite_queue.csv"),
            "typed_primitive_spec": str(output_root / "typed_primitive_spec.csv"),
            "typed_primitive_registry": str(output_root / "typed_primitive_registry.json"),
            "registry_field_route_sample": str(output_root / "registry_field_route_sample.csv"),
            "report": str(output_root / "PHASE3CD_AST_PRIMITIVE_ASSUMPTION_AUDIT_20260618.md"),
        },
        "hard_boundary": [
            "diagnostic design artifact only",
            "no official X0/R3 changes",
            "no large search until new-field typed primitive gate exists",
            "ordinary AST primitives remain allowed only for audited continuous minute fields",
        ],
    }
    _write_json(output_root / "phase3cd_ast_primitive_assumption_audit_summary.json", summary)
    _write_report(output_root / "PHASE3CD_AST_PRIMITIVE_ASSUMPTION_AUDIT_20260618.md", summary)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = run(args)
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
