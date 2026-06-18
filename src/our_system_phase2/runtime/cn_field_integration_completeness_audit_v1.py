"""Audit end-to-end CN field integration completeness.

This is a no-replay governance audit. It tracks public-enrichment fields from
field registry/contract assets through panel materialization, factor-pack
expressions, selector admission, and field-availability evidence.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from our_system_phase2.services.typed_primitive_gate import validate_row


DEFAULT_GLOBAL_ROUTE = Path("runtime/field_registry/cn_public_enrichment_global_field_route_v1_20260602/global_field_route.csv")
DEFAULT_NEW_DATA_ROUTE = Path("runtime/field_registry/cn_new_data_asset_integration_v1_20260603/field_route_registry.csv")
DEFAULT_NONMINUTE_INTEGRATION = Path("reports/cn_nonminute_system_integration_audit_20260602/nonminute_field_system_integration.csv")
DEFAULT_FACTOR_PACK_DIR = Path("runtime/factor_packs")
DEFAULT_OUTPUT_ROOT = Path("reports/cn_field_integration_completeness_audit_v1_20260603")
DEFAULT_RUNTIME_ROOT = Path("runtime/field_registry/cn_field_integration_completeness_audit_v1_20260603")

DEFAULT_PANEL_PATHS = [
    Path(r"G:\Project_V7_Rotation\data\company_phase2_panels\phase2_stock_tdx_official_20250806_to_20260410_cn_integrated_v2_all_fields_local.parquet"),
    Path(r"G:\Project_V7_Rotation\data\company_phase2_panels\phase2_stock_tdx_official_20250806_to_20260410_cn_integrated_v2_phase3ad_newdata_v1_20260603.parquet"),
    Path(r"G:\Project_V7_Rotation\data\company_phase2_panels\phase2_stock_tdx_official_20250806_to_20260410_cn_integrated_v2_phase3ad_newdata_hfqvaluation_v1_20260603.parquet"),
    Path("runtime/cn_phase3ad_full_index_sidecar_v1_20260603/phase3ad_selected_sidecar.parquet"),
    Path("runtime/cn_phase3ad_hfq_valuation_sidecar_v1_20260603/hfq_valuation_lag1_sidecar.parquet"),
    Path("runtime/nonminute_context_panels/cn_nonminute_pit_context_panel_v1_20260602"),
    Path("runtime/minute_feature_panels/cn_minute_feature_panel_v2_20260602_full_retry1"),
    Path("runtime/minute_feature_panels/cn_minute_limit_event_alignment_v2_20260602/cn_minute_limit_event_alignment_v1.parquet"),
]

FIELD_RE = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")
TOKEN_RE = re.compile(r"[A-Za-z0-9]+")
HIGH_VALUE_FAMILIES = {
    "flow_liquidity",
    "capacity_size",
    "limit_event_sentiment",
    "leverage_flow",
    "fundamental_quality_risk",
    "holder_corporate_action",
    "disclosure_event_flow",
    "group_market_context",
    "price_state",
    "fundamental",
    "disclosure_event",
}
WATCHLIST_PATTERNS = {
    "valuation_pe_pb_ps": ["pe_ttm", "ps_ttm", "inv_pe", "inv_pb", "inv_ps", "ctx_hfq_pb"],
    "volume_turnover_ratio": ["volume_ratio", "turnover_ratio", "volume_ration"],
    "daily_amount_volume": ["amount_yuan", "volume_shares", "amount", "volume"],
    "limit_event_core": ["up_limit_time", "uplimit", "up_limit", "limit_up", "open_board", "fd_", "auction"],
    "limit_board_count": ["lb_2", "lb_3", "max_lb", "up_limit_keep_times", "uplimit_count", "open_board_count"],
    "rzrq_margin": ["rzrq", "rzmre", "rzche", "rzjme", "rqmcl", "rqchl"],
    "billboard_flow": ["billboard", "accum_amount", "deal_net", "buy_ratio", "sell_ratio"],
    "plate_moneyflow": ["money_leader", "trade_money", "sum_leader_money", "sum_score", "plate"],
    "capacity_size": ["market_cap", "float", "circulation", "shares"],
}
BLOCKED_ROUTES = {
    "blocked_future_label",
    "metadata",
    "join_key",
    "manual_review_required",
    "text_diagnostic",
}


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    for encoding in ("utf-8-sig", "utf-8", "gbk"):
        try:
            return pd.read_csv(path, encoding=encoding)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, encoding="utf-8", encoding_errors="ignore")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _schema_columns(path: Path) -> set[str]:
    out: set[str] = set()
    if not path.exists():
        return out
    if path.is_file() and path.suffix.lower() == ".parquet":
        return set(pq.ParquetFile(path).schema_arrow.names)
    if path.is_dir():
        for item in path.rglob("*.parquet"):
            try:
                out.update(pq.ParquetFile(item).schema_arrow.names)
            except Exception:
                continue
    return out


def _tokens(value: str) -> list[str]:
    return TOKEN_RE.findall(str(value).lower())


def _norm(value: Any) -> str:
    return str(value or "").strip().lower()


def _split_pipe(value: Any) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    return [item.strip() for item in re.split(r"[|,]", str(value)) if item.strip()]


def _extract_expr_fields(expression: Any) -> set[str]:
    return set(FIELD_RE.findall(str(expression or "")))


def _raw_matches_alias(raw_field: str, alias_field: str) -> bool:
    raw = _norm(raw_field)
    alias = _norm(alias_field)
    if not raw or not alias:
        return False
    if raw == alias:
        return True
    short_finance_tokens = {"pe", "pb", "ps", "roe", "roa", "eps"}
    raw_tokens_all = _tokens(raw)
    alias_tokens_all = _tokens(alias)
    raw_tokens = [
        token
        for token in raw_tokens_all
        if (len(token) > 2 and not token.isdigit()) or token in short_finance_tokens
    ]
    alias_tokens = [
        token
        for token in alias_tokens_all
        if (len(token) > 2 and not token.isdigit()) or token in short_finance_tokens
    ]
    if not raw_tokens or not alias_tokens:
        return False
    if len(raw) <= 3:
        return raw in alias_tokens
    joined_alias = "_".join(alias_tokens)
    joined_raw = "_".join(raw_tokens)
    if joined_raw in joined_alias:
        return True
    if len(raw_tokens) == 1 and raw_tokens[0] in alias_tokens:
        return True
    for idx in range(0, len(alias_tokens) - len(raw_tokens) + 1):
        if alias_tokens[idx : idx + len(raw_tokens)] == raw_tokens:
            return True
    return False


def _load_assets(global_route: Path, new_data_route: Path, nonminute_integration: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    global_df = _read_csv(global_route)
    for rec in global_df.to_dict("records"):
        rows.append(
            {
                "source_registry": "global_field_route",
                "source_group": rec.get("source_root", ""),
                "dataset": rec.get("dataset", ""),
                "field_name": rec.get("field_name", ""),
                "field_family": rec.get("field_family", ""),
                "route": rec.get("route", ""),
                "selector_allowed": rec.get("selector_role", ""),
                "pit_rule": rec.get("pit_rule", ""),
                "priority": rec.get("priority", ""),
                "asset_status": "registered",
                "integration_hint": "",
            }
        )
    new_df = _read_csv(new_data_route)
    for rec in new_df.to_dict("records"):
        rows.append(
            {
                "source_registry": "new_data_asset_integration",
                "source_group": rec.get("source_group", ""),
                "dataset": rec.get("dataset", ""),
                "field_name": rec.get("field", ""),
                "field_family": rec.get("field_family", ""),
                "route": rec.get("route", ""),
                "selector_allowed": rec.get("selector_allowed", ""),
                "pit_rule": rec.get("pit_rule", ""),
                "priority": rec.get("priority", ""),
                "asset_status": rec.get("asset_status", ""),
                "integration_hint": rec.get("next_action", ""),
            }
        )
    nonminute_df = _read_csv(nonminute_integration)
    for rec in nonminute_df.to_dict("records"):
        rows.append(
            {
                "source_registry": "nonminute_system_integration",
                "source_group": rec.get("path", ""),
                "dataset": rec.get("table_id", ""),
                "field_name": rec.get("field_name", ""),
                "field_family": rec.get("field_family", ""),
                "route": rec.get("route", ""),
                "selector_allowed": rec.get("selector_allowed", ""),
                "pit_rule": rec.get("pit_rule", ""),
                "priority": "",
                "asset_status": "registered",
                "integration_hint": rec.get("integration_status", ""),
            }
        )
    dedup: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        field = str(row.get("field_name") or "").strip()
        if not field:
            continue
        key = (_norm(row.get("source_group")), _norm(row.get("dataset")), _norm(field))
        if key not in dedup:
            dedup[key] = row
        else:
            existing = dedup[key]
            for col in ("source_registry", "route", "selector_allowed", "integration_hint", "asset_status"):
                old = str(existing.get(col) or "")
                new = str(row.get(col) or "")
                if new and new not in old.split("|"):
                    existing[col] = "|".join([part for part in [old, new] if part])
    return list(dedup.values())


def _load_factor_refs(factor_pack_dir: Path) -> tuple[dict[str, set[str]], list[dict[str, Any]]]:
    refs: dict[str, set[str]] = defaultdict(set)
    candidate_rows: list[dict[str, Any]] = []
    for path in sorted(factor_pack_dir.glob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except Exception:
            continue
        rows = payload.get("candidate_rows") or payload.get("rows") or payload.get("candidates") or []
        for row in rows:
            expression = row.get("expression") or row.get("formula") or ""
            fields = set(_extract_expr_fields(expression))
            for meta_col in ("input_fields", "event_fields", "flow_liquidity_fields", "fundamental_fields", "capacity_fields"):
                fields.update(_split_pipe(row.get(meta_col)))
            fields = {field for field in fields if field}
            candidate_rows.append(
                validate_row(
                    {
                    "factor_pack": path.name,
                    "candidate_id": row.get("candidate_id", ""),
                    "source_generator": row.get("source_generator", ""),
                    "source_lane": row.get("source_lane", ""),
                    "factor_lane": row.get("factor_lane", ""),
                    "expression": expression,
                    "fields": "|".join(sorted(fields)),
                    },
                    entry_lineage="advisory_runtime_registry",
                    materialization_stage="advisory_registry_write",
                    candidate_role="selector_candidate",
                )
            )
            for field in fields:
                refs[field].add(path.name)
    return refs, candidate_rows


def _load_selector_refs() -> tuple[dict[str, set[str]], list[dict[str, Any]]]:
    refs: dict[str, set[str]] = defaultdict(set)
    selected_rows: list[dict[str, Any]] = []
    search_roots = [
        Path("runtime"),
        Path("reports"),
    ]
    audit_paths: list[Path] = []
    for root in search_roots:
        if root.exists():
            audit_paths.extend(root.rglob("phase3e_selector_audit.csv"))
            audit_paths.extend(root.rglob("*selected_candidates.csv"))
    for path in sorted(set(audit_paths)):
        try:
            frame = pd.read_csv(path, encoding="utf-8-sig")
        except Exception:
            continue
        if "selected_for_audit" in frame.columns:
            frame = frame[frame["selected_for_audit"].astype(str).str.lower().isin({"true", "1", "yes"})]
        for rec in frame.to_dict("records"):
            expression = rec.get("expression", "")
            fields = set(_extract_expr_fields(expression))
            if not fields and rec.get("fields"):
                fields.update(_split_pipe(rec.get("fields")))
            if not fields:
                continue
            selected_rows.append(
                validate_row(
                    {
                    "source_file": str(path),
                    "candidate_id": rec.get("candidate_id", ""),
                    "source_generator": rec.get("source_generator", ""),
                    "source_lane": rec.get("source_lane", ""),
                    "selection_rank": rec.get("selection_rank", ""),
                    "expression": expression,
                    "fields": "|".join(sorted(fields)),
                    },
                    entry_lineage="advisory_runtime_registry",
                    materialization_stage="advisory_registry_write",
                    candidate_role="selector_candidate",
                )
            )
            for field in fields:
                refs[field].add(str(path))
    return refs, selected_rows


def _load_field_availability_refs() -> dict[str, set[str]]:
    refs: dict[str, set[str]] = defaultdict(set)
    for path in sorted(Path("reports").rglob("selected_field_availability.csv")):
        try:
            frame = pd.read_csv(path, encoding="utf-8-sig")
        except Exception:
            continue
        for rec in frame.to_dict("records"):
            status = str(rec.get("status") or "")
            for field in _split_pipe(rec.get("fields")):
                if status == "executable":
                    refs[field].add(str(path))
    return refs


def _best_alias_matches(raw_field: str, aliases: set[str], limit: int = 8) -> list[str]:
    matches = [alias for alias in aliases if _raw_matches_alias(raw_field, alias)]
    return sorted(matches)[:limit]


def _classify(row: dict[str, Any]) -> str:
    if row["blocked_or_metadata"]:
        return "blocked_or_metadata"
    if row["selector_selected"]:
        return "integrated_selector_selected"
    if row["factor_pack_referenced"] and row["panel_materialized"]:
        return "integrated_selector_ready"
    if row["panel_materialized"]:
        return "panel_only"
    if row["factor_pack_referenced"]:
        return "factor_pack_only_missing_panel"
    if row["asset_status"] in {"blocked", "backlog"} or "probe" in _norm(row["source_registry"]) or "probe" in _norm(row["source_group"]):
        return "probe_or_backlog"
    return "contract_only"


def _repair_action(row: dict[str, Any]) -> str:
    status = row["integration_status"]
    if status == "panel_only":
        return "build_factor_pack_candidates_then_selector_canary"
    if status == "factor_pack_only_missing_panel":
        return "materialize_sidecar_or_block_before_selector"
    if status == "contract_only":
        return "materialize_pit_sidecar_before_formula_search"
    if status == "probe_or_backlog":
        return "finish_ingestion_and_pit_contract_first"
    if status in {"integrated_selector_ready", "integrated_selector_selected"}:
        return "replay_style_cost_audit_if_selected"
    return "no_alpha_action"


def _priority_score(row: dict[str, Any]) -> float:
    score = 0.0
    status_weight = {
        "panel_only": 5.0,
        "factor_pack_only_missing_panel": 4.5,
        "contract_only": 3.0,
        "probe_or_backlog": 2.0,
        "integrated_selector_ready": 1.0,
        "integrated_selector_selected": 0.5,
    }
    family_weight = {
        "limit_event_sentiment": 3.0,
        "flow_liquidity": 2.8,
        "capacity_size": 2.6,
        "leverage_flow": 2.4,
        "group_market_context": 2.3,
        "disclosure_event_flow": 2.1,
        "fundamental_quality_risk": 1.8,
        "holder_corporate_action": 1.6,
        "price_state": 1.2,
        "fundamental": 1.2,
    }
    priority_weight = {
        "very_high": 3.0,
        "high": 2.0,
        "medium": 1.0,
        "low": 0.2,
    }
    score += status_weight.get(str(row.get("integration_status") or ""), 0.0)
    score += family_weight.get(str(row.get("field_family") or ""), 0.0)
    score += priority_weight.get(str(row.get("priority") or "").lower(), 0.0)
    if row.get("panel_materialized") and not row.get("factor_pack_referenced"):
        score += 1.0
    if row.get("factor_pack_referenced") and not row.get("panel_materialized"):
        score += 1.0
    return score


def _watchlist_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for label, patterns in WATCHLIST_PATTERNS.items():
        for row in rows:
            haystack = " ".join(
                str(row.get(col) or "")
                for col in (
                    "field_name",
                    "dataset",
                    "source_group",
                    "panel_aliases",
                    "factor_pack_aliases",
                    "selector_aliases",
                )
            ).lower()
            field = _norm(row.get("field_name"))
            valuation_exact = label == "valuation_pe_pb_ps" and field in {"pe", "pb", "ps", "pe_ttm", "ps_ttm"}
            if valuation_exact or any(pattern.lower() in haystack for pattern in patterns):
                out.append(
                    {
                        "watchlist_group": label,
                        "field_name": row.get("field_name"),
                        "dataset": row.get("dataset"),
                        "field_family": row.get("field_family"),
                        "integration_status": row.get("integration_status"),
                        "panel_aliases": row.get("panel_aliases"),
                        "factor_pack_aliases": row.get("factor_pack_aliases"),
                        "selector_aliases": row.get("selector_aliases"),
                        "recommended_repair_action": row.get("recommended_repair_action"),
                        "repair_priority_score": row.get("repair_priority_score"),
                    }
                )
    return out


def run(
    *,
    global_route: Path,
    new_data_route: Path,
    nonminute_integration: Path,
    factor_pack_dir: Path,
    panel_paths: list[Path],
    output_root: Path,
    runtime_root: Path,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    runtime_root.mkdir(parents=True, exist_ok=True)

    assets = _load_assets(global_route, new_data_route, nonminute_integration)
    panel_fields: set[str] = set()
    panel_field_sources: dict[str, set[str]] = defaultdict(set)
    for path in panel_paths:
        cols = _schema_columns(path)
        panel_fields.update(cols)
        for col in cols:
            panel_field_sources[col].add(str(path))

    factor_refs, factor_candidates = _load_factor_refs(factor_pack_dir)
    selector_refs, selected_candidates = _load_selector_refs()
    availability_refs = _load_field_availability_refs()
    factor_aliases = set(factor_refs)
    selector_aliases = set(selector_refs)
    availability_aliases = set(availability_refs)

    rows: list[dict[str, Any]] = []
    for asset in assets:
        field = str(asset["field_name"])
        panel_matches = _best_alias_matches(field, panel_fields)
        factor_matches = _best_alias_matches(field, factor_aliases)
        selector_matches = _best_alias_matches(field, selector_aliases)
        availability_matches = _best_alias_matches(field, availability_aliases)
        route = _norm(asset.get("route"))
        selector_allowed = _norm(asset.get("selector_allowed"))
        family = _norm(asset.get("field_family"))
        blocked = (
            route in BLOCKED_ROUTES
            or selector_allowed in {"false", "false_key_only", "not_alpha_feature", "blocked"}
            or family in {"metadata", "instrument_key", "time_key_or_cutoff", "future_label", "text_or_description"}
        )
        row = {
            **asset,
            "panel_materialized": bool(panel_matches),
            "panel_aliases": "|".join(panel_matches),
            "factor_pack_referenced": bool(factor_matches),
            "factor_pack_aliases": "|".join(factor_matches),
            "factor_pack_names": "|".join(sorted({pack for alias in factor_matches for pack in factor_refs.get(alias, set())})[:8]),
            "selector_selected": bool(selector_matches),
            "selector_aliases": "|".join(selector_matches),
            "availability_executable_seen": bool(availability_matches),
            "availability_aliases": "|".join(availability_matches),
            "blocked_or_metadata": blocked,
            "high_value_family": family in HIGH_VALUE_FAMILIES,
        }
        row["integration_status"] = _classify(row)
        row["recommended_repair_action"] = _repair_action(row)
        row["repair_priority_score"] = round(_priority_score(row), 3)
        rows.append(row)

    alias_unmapped_rows = []
    asset_fields = [str(row["field_name"]) for row in assets]
    for alias in sorted(factor_aliases | selector_aliases):
        if not any(_raw_matches_alias(raw, alias) for raw in asset_fields):
            alias_unmapped_rows.append(
                {
                    "alias_field": alias,
                    "in_panel": alias in panel_fields,
                    "factor_packs": "|".join(sorted(factor_refs.get(alias, set()))[:8]),
                    "selector_selected": alias in selector_aliases,
                    "selector_sources": "|".join(sorted(selector_refs.get(alias, set()))[:5]),
                }
            )

    status_counts = Counter(row["integration_status"] for row in rows)
    family_status_counts = Counter((row["field_family"], row["integration_status"]) for row in rows)
    high_value_gaps = [
        row
        for row in rows
        if row["high_value_family"]
        and row["integration_status"]
        in {"contract_only", "panel_only", "factor_pack_only_missing_panel", "probe_or_backlog"}
    ]
    factor_only_gaps = [row for row in rows if row["integration_status"] == "factor_pack_only_missing_panel"]
    panel_only_gaps = [row for row in rows if row["integration_status"] == "panel_only" and row["high_value_family"]]
    repair_priority_gaps = sorted(
        [
            row
            for row in rows
            if row["high_value_family"]
            and row["integration_status"] in {"panel_only", "factor_pack_only_missing_panel", "contract_only", "probe_or_backlog"}
        ],
        key=lambda row: (-float(row.get("repair_priority_score") or 0.0), str(row.get("field_family") or ""), str(row.get("field_name") or "")),
    )
    watchlist = _watchlist_rows(rows)
    factor_candidate_blocked = [
        row for row in factor_candidates if str(row.get("typed_gate_decision") or "") != "allow"
    ]
    selector_selected_blocked = [
        row for row in selected_candidates if str(row.get("typed_gate_decision") or "") != "allow"
    ]

    _write_csv(runtime_root / "field_integration_completeness.csv", rows)
    _write_csv(runtime_root / "high_value_field_gaps.csv", high_value_gaps)
    _write_csv(runtime_root / "factor_pack_only_missing_panel.csv", factor_only_gaps)
    _write_csv(runtime_root / "panel_only_high_value_fields.csv", panel_only_gaps)
    _write_csv(runtime_root / "repair_priority_field_gaps.csv", repair_priority_gaps)
    _write_csv(runtime_root / "key_field_watchlist_status.csv", watchlist)
    _write_csv(runtime_root / "unmapped_factor_aliases.csv", alias_unmapped_rows)
    _write_csv(runtime_root / "factor_candidate_field_refs.csv", factor_candidates)
    _write_csv(runtime_root / "selector_selected_field_refs.csv", selected_candidates)
    _write_csv(runtime_root / "advisory_registry_gate_audit.csv", [*factor_candidate_blocked, *selector_selected_blocked])

    summary = {
        "version": "cn-field-integration-completeness-audit-v1-2026-06-03",
        "decision": "HOLD_FIELD_INTEGRATION_NOT_FULLY_CLOSED",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "asset_field_count": len(rows),
        "panel_field_count": len(panel_fields),
        "factor_alias_count": len(factor_aliases),
        "selector_alias_count": len(selector_aliases),
        "status_counts": dict(status_counts),
        "high_value_gap_count": len(high_value_gaps),
        "factor_pack_only_missing_panel_count": len(factor_only_gaps),
        "panel_only_high_value_count": len(panel_only_gaps),
        "unmapped_factor_alias_count": len(alias_unmapped_rows),
        "repair_priority_top_count": len(repair_priority_gaps),
        "key_field_watchlist_row_count": len(watchlist),
        "typed_gate_factor_candidate_blocked_count": len(factor_candidate_blocked),
        "typed_gate_selector_selected_blocked_count": len(selector_selected_blocked),
        "family_status_counts": [
            {"field_family": family, "integration_status": status, "count": count}
            for (family, status), count in sorted(family_status_counts.items(), key=lambda item: (str(item[0][0]), str(item[0][1])))
        ],
        "key_findings": [
            "Field assets are broad, but end-to-end field integration is not fully closed.",
            "A field can be present in raw assets or contracts while still absent from factor-pack/search space.",
            "A field can be panelized while still lacking selector/replay evidence.",
            "HFQ PE/PB/PS/volume_ratio/turnover_ratio are now wired as a lag1 selector-only route, but replay/style evidence is still missing.",
        ],
        "outputs": {
            "field_integration_completeness": str(runtime_root / "field_integration_completeness.csv"),
            "high_value_field_gaps": str(runtime_root / "high_value_field_gaps.csv"),
            "factor_pack_only_missing_panel": str(runtime_root / "factor_pack_only_missing_panel.csv"),
            "panel_only_high_value_fields": str(runtime_root / "panel_only_high_value_fields.csv"),
            "repair_priority_field_gaps": str(runtime_root / "repair_priority_field_gaps.csv"),
            "key_field_watchlist_status": str(runtime_root / "key_field_watchlist_status.csv"),
            "unmapped_factor_aliases": str(runtime_root / "unmapped_factor_aliases.csv"),
            "factor_candidate_field_refs": str(runtime_root / "factor_candidate_field_refs.csv"),
            "selector_selected_field_refs": str(runtime_root / "selector_selected_field_refs.csv"),
            "advisory_registry_gate_audit": str(runtime_root / "advisory_registry_gate_audit.csv"),
        },
    }
    _write_json(runtime_root / "field_integration_completeness_audit.json", summary)
    _write_json(output_root / "field_integration_completeness_audit.json", summary)
    _write_csv(output_root / "high_value_field_gaps_top.csv", high_value_gaps[:250])
    _write_csv(output_root / "panel_only_high_value_fields.csv", panel_only_gaps[:250])
    _write_csv(output_root / "factor_pack_only_missing_panel.csv", factor_only_gaps[:250])
    _write_csv(output_root / "repair_priority_field_gaps_top.csv", repair_priority_gaps[:300])
    _write_csv(output_root / "key_field_watchlist_status.csv", watchlist)

    lines = [
        "# CN Field Integration Completeness Audit v1",
        "",
        f"decision: `{summary['decision']}`",
        "",
        "## Summary",
        "",
        f"- asset_field_count: `{summary['asset_field_count']}`",
        f"- panel_field_count: `{summary['panel_field_count']}`",
        f"- factor_alias_count: `{summary['factor_alias_count']}`",
        f"- selector_alias_count: `{summary['selector_alias_count']}`",
        f"- high_value_gap_count: `{summary['high_value_gap_count']}`",
        f"- factor_pack_only_missing_panel_count: `{summary['factor_pack_only_missing_panel_count']}`",
        f"- panel_only_high_value_count: `{summary['panel_only_high_value_count']}`",
        f"- key_field_watchlist_row_count: `{summary['key_field_watchlist_row_count']}`",
        "",
        "## Integration Status Counts",
        "",
    ]
    for status, count in sorted(status_counts.items()):
        lines.append(f"- `{status}`: `{count}`")
    lines.extend(
        [
            "",
            "## Interpretation",
            "",
            "This audit confirms the project has substantial data assets, but field integration is not globally closed. The failure mode is not data absence; it is route incompleteness between field contracts, panel aliases, factor-pack expressions, selector admission, and replay availability.",
            "",
            "The PE/PB case is now fixed as an HFQ lag1 valuation selector-only route, but it is the pattern we need to watch for: fields can exist in silver data and still be invisible to the mature search chain.",
            "",
            "## Next Repair Order",
            "",
            "1. Convert `panel_only_high_value_fields` into bounded factor-pack candidates.",
            "2. Convert `factor_pack_only_missing_panel` into selected sidecars or block those candidates before selector/replay.",
            "3. Keep timestamped event fields separate from daily context fields; event time fields need cutoff proofs, not simple T+1 context.",
            "4. Run replay/style audit only after field availability is closed for the selected queue.",
        ]
    )
    (output_root / "CN_FIELD_INTEGRATION_COMPLETENESS_AUDIT_V1_2026-06-03.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--global-route", type=Path, default=DEFAULT_GLOBAL_ROUTE)
    parser.add_argument("--new-data-route", type=Path, default=DEFAULT_NEW_DATA_ROUTE)
    parser.add_argument("--nonminute-integration", type=Path, default=DEFAULT_NONMINUTE_INTEGRATION)
    parser.add_argument("--factor-pack-dir", type=Path, default=DEFAULT_FACTOR_PACK_DIR)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--runtime-root", type=Path, default=DEFAULT_RUNTIME_ROOT)
    parser.add_argument("--panel-path", action="append", type=Path, default=[])
    args = parser.parse_args()
    panel_paths = args.panel_path or DEFAULT_PANEL_PATHS
    summary = run(
        global_route=args.global_route,
        new_data_route=args.new_data_route,
        nonminute_integration=args.nonminute_integration,
        factor_pack_dir=args.factor_pack_dir,
        panel_paths=panel_paths,
        output_root=args.output_root,
        runtime_root=args.runtime_root,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
