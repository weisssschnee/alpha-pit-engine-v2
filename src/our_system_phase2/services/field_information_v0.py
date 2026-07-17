"""Compile field tokens and observed grammar exposure from existing authorities."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

from our_system_phase2.services.compositional_grammar import CompositionalGrammarV2
from our_system_phase2.services.unified_capability_registry import ROUTE_IDS, UnifiedCapabilityRegistry


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _context(row: dict[str, Any]) -> str:
    family = str(row.get("data_family", ""))
    routes = set(str(row.get("recommended_routes", "")).split("|"))
    if "chip" in family or "plate" in family:
        return "CHIP_PLATE_STATE"
    if family.startswith("fundamental") or row.get("source_table") in {
        "balance_sheet_report_em", "profit_sheet_report_em", "cash_flow_sheet_report_em",
        "main_stock_holder_sina", "zygc_em",
    }:
        return "FUNDAMENTAL_DISCLOSURE"
    if routes.intersection({"FIRSTN_PATH", "MINUTE_STATIC", "INTRADAY_STATE_TRANSITION", "MARKET_REGIME_CONDITION"}):
        return "FIRSTN_INTRADAY"
    return "UNASSIGNED"


def observed_exposure(
    registry: UnifiedCapabilityRegistry,
    *,
    attempts_per_route: int = 2048,
    seed: int = 20260717,
) -> tuple[dict[str, set[str]], dict[str, int]]:
    grammar = CompositionalGrammarV2(registry)
    routes_by_field: dict[str, set[str]] = {}
    counts: dict[str, int] = {}
    for route_id in ROUTE_IDS:
        skeleton_count = 1 if route_id == "BROAD_EVENT_FROZEN_ENTRY" else attempts_per_route
        for attempt in range(skeleton_count):
            pair = grammar.propose(route_id, attempt_index=attempt, seed=seed)
            for field_id in pair.primary["declared_field_ids"]:
                routes_by_field.setdefault(field_id, set()).add(route_id)
                counts[field_id] = counts.get(field_id, 0) + 1
    return routes_by_field, counts


def compile_tokens(
    master: dict[str, Any],
    registry: UnifiedCapabilityRegistry,
    *,
    master_path: Path,
    registry_path: Path,
    attempts_per_route: int = 2048,
) -> list[dict[str, Any]]:
    exposed_routes, exposure_counts = observed_exposure(
        registry, attempts_per_route=attempts_per_route
    )
    capability_by_id = {row.field_id: row for row in registry.fields}
    raw_registry = json.loads(registry_path.read_text(encoding="utf-8"))
    raw_by_id = {row["field_id"]: row for row in raw_registry["fields"]}
    tokens: list[dict[str, Any]] = []
    for row in master["rows"]:
        field_id = str(row["field_name"])
        capability = capability_by_id.get(field_id)
        raw = raw_by_id.get(field_id, {})
        canonical = dict(raw.get("metadata", {}).get("canonical_representation", {}))
        dependencies = canonical.get("source_field_ids", [])
        params = canonical.get("parameters", {})
        token = {
            "field_token_id": "cn.ft." + hashlib.sha256(str(row["field_uid"]).encode()).hexdigest()[:24],
            "field_uid": row["field_uid"],
            "field_id": field_id,
            "base_field": row["source_field"],
            "source": row["source_table"],
            "family": row["data_family"],
            "semantic_role": row["semantic_role"],
            "value_type": row["dtype"],
            "unit": raw.get("unit_status", "UNREGISTERED"),
            "scope": row["entity_scope"],
            "clock": row["observable_time"],
            "observable_time_rule": row["observable_time"],
            "maturity_rule": row["maturity"],
            "pit_status": row["pit_status"],
            "materialization_status": row["materialization_status"],
            "search_allowed": bool(capability and capability.search_eligible),
            "condition_only": row["field_role"] == "condition-only",
            "benchmark_only": row["field_role"] == "benchmark-only",
            "metadata_only": row["field_role"] == "blocked" or row["semantic_role"] == "METADATA_BLOCKED",
            "allowed_routes": list(capability.allowed_routes) if capability else [],
            "blocker": row["blocked_reason"],
            "dependencies": dependencies,
            "transform": canonical.get("operation", "identity"),
            "window": params.get("window", ""),
            "lag": raw.get("source_lag", ""),
            "episode_payload_role": raw.get("temporal_semantics", ""),
            "context": _context(row),
            "registry_present": capability is not None,
            "route_compatible": bool(capability and capability.allowed_routes),
            "representation_available": bool(raw.get("representation_id")),
            "generator_exposed": field_id in exposed_routes,
            "generator_exposed_routes": sorted(exposed_routes.get(field_id, set())),
            "generator_observation_count": exposure_counts.get(field_id, 0),
            "information_qualified": False,
            "core_pack_selected": False,
            "information_status": "NOT_EVALUATED",
            "source_registry_path": str(master_path.as_posix()),
            "source_registry_sha256": master["content_sha256"],
            "route_registry_path": str(registry_path.as_posix()),
            "route_registry_sha256": file_sha256(registry_path),
        }
        tokens.append(token)
    return sorted(tokens, key=lambda row: row["field_token_id"])


def summarize(tokens: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = list(tokens)
    contexts: dict[str, dict[str, int]] = {}
    for row in rows:
        item = contexts.setdefault(row["context"], {"total": 0, "registry_present": 0, "generator_exposed": 0})
        item["total"] += 1
        item["registry_present"] += int(row["registry_present"])
        item["generator_exposed"] += int(row["generator_exposed"])
    return {
        "token_count": len(rows),
        "registry_present_count": sum(row["registry_present"] for row in rows),
        "generator_exposed_count": sum(row["generator_exposed"] for row in rows),
        "information_qualified_count": 0,
        "core_pack_selected_count": 0,
        "contexts": contexts,
        "performance_or_reward_used": False,
        "validation_accessed": False,
        "holdout_accessed": False,
        "forward_2026_accessed": False,
    }
