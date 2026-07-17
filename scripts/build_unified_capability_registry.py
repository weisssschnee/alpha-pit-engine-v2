"""Merge external Phase-2 contracts with executable CN source registries."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Iterable, Mapping

from our_system_phase2.services.fundamental_representations import (
    FUNDAMENTAL_SOURCE_RELEASE,
    PROVIDER_BY_TABLE,
    canonical_representation_specs,
    qualify_source_universe,
)
from our_system_phase2.services.chip_sidecar import CHIP_SIDECAR_VERSION, chip_field_specs
from our_system_phase2.services.tdx_plate_market_sidecar import (
    TDX_PLATE_MARKET_VERSION,
    plate_market_field_specs,
)
from our_system_phase2.services.true1min_plate_aggregation import (
    TRUE1MIN_PLATE_AGGREGATION_VERSION,
    true1min_plate_field_specs,
)
from our_system_phase2.services.unified_capability_registry import (
    REGISTRY_VERSION,
    freeze_registry,
    representation_id,
    source_field_id,
    stable_hash,
)


ACTIVE121_RELEASE = "cn_true1min_development_only_release_v1_20260712_77o"
BROAD_EVENT_RELEASE = "cn_broad_event_discovery_entry_pack_v1_20260714"


def _sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]], columns: Iterable[str] | None = None) -> None:
    materialized = [dict(row) for row in rows]
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(columns or (materialized[0].keys() if materialized else ()))
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(materialized)


def _active_value_kind(name: str, dtype: str) -> tuple[str, str]:
    lower = name.lower()
    if dtype in {"string", "date32[day]", "metadata"}:
        return "metadata", "NOT_APPLICABLE_METADATA"
    if any(token in lower for token in ("ratio", "return", "ret_", "pct", "_pb", "_pe", "_ps", "range")):
        return "ratio", "ASSERTED_FROM_EXACT_ACTIVE121_FIELD"
    if any(token in lower for token in ("amount", "money", "market_cap", "rzye", "rzjme")):
        return "currency", "ASSERTED_FROM_EXACT_ACTIVE121_FIELD"
    if any(token in lower for token in ("volume", "_vol", "hold_num", "shares")):
        return "shares", "ASSERTED_FROM_EXACT_ACTIVE121_FIELD"
    if any(token in lower for token in ("_num", "count", "rank", "age", "bars", "times", "type_code")):
        return "count", "ASSERTED_FROM_EXACT_ACTIVE121_FIELD"
    if any(token in lower for token in ("active", "is_", "flag")):
        return "flag", "ASSERTED_FROM_EXACT_ACTIVE121_FIELD"
    if lower in {"open", "high", "low", "close", "vwap"} or any(
        lower.endswith(token) for token in ("_open", "_high", "_low", "_close", "_vwap")
    ):
        return "price", "ASSERTED_FROM_EXACT_ACTIVE121_FIELD"
    if dtype.startswith(("float", "int")):
        return "numeric_unknown_unit", "SOURCE_UNIT_GLOSSARY_NOT_ASSERTED"
    return "unknown", "SOURCE_UNIT_GLOSSARY_NOT_ASSERTED"


def _active_entity_scope(name: str, external: Mapping[str, Mapping[str, Any]]) -> str:
    if name in external:
        return str(external[name].get("entity_scope") or "STOCK")
    lower = name.lower()
    if lower.startswith(("ctx_sent_", "ctx_zls_", "ctx_market_")):
        return "MARKET"
    return "STOCK"


def _active_routes(field: Mapping[str, Any], entity_scope: str, eligible: bool) -> list[str]:
    if not eligible:
        return []
    family = str(field["family"])
    role = str(field["role"])
    if family == "raw_1min":
        return ["MINUTE_STATIC", "FIRSTN_PATH", "INTRADAY_STATE_TRANSITION", "MARKET_REGIME_CONDITION"]
    if family == "firstN":
        return ["FIRSTN_PATH"]
    if family == "lagged_daily_context":
        if entity_scope == "MARKET" or role in {"state-only", "condition-only"}:
            return ["MARKET_REGIME_CONDITION"]
        return ["SLOW_CROSS_SECTIONAL_LEVEL", "SLOW_TEMPORAL_CHANGE"]
    # Direct event_state fields are source ingredients, not frozen Broad
    # Event entries.  Only the immutable 11-mechanism entry pack receives the
    # BROAD_EVENT_FROZEN_ENTRY route below.
    return []


def _active_temporal_semantics(field: Mapping[str, Any], entity_scope: str) -> str:
    family = str(field["family"])
    if family == "raw_1min":
        return "BAR_VALUE"
    if family == "firstN":
        return "FIRSTN_SESSION_PATH"
    if family == "lagged_daily_context":
        return "PREVIOUS_SESSION_MARKET_STATE" if entity_scope == "MARKET" else "PREVIOUS_SESSION_STOCK_CONTEXT"
    if family == "event_state":
        return "LATCHED_OBSERVATION_OR_EVENT_ATTRIBUTE"
    return "METADATA"


def _active_support_unit(field: Mapping[str, Any], entity_scope: str) -> str:
    if entity_scope == "MARKET":
        return "market-time block"
    family = str(field["family"])
    if family in {"raw_1min", "firstN"}:
        return "stock-minute cross-section"
    if family == "event_state":
        return "event episode"
    return "stock-session cross-section"


def _active_source_glossary(
    fields: Iterable[Mapping[str, Any]],
    external_entries: Mapping[str, Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    glossary: list[dict[str, Any]] = []
    capabilities: list[dict[str, Any]] = []
    for field in fields:
        name = str(field["name"])
        source_id = source_field_id(
            provider="CN_TRUE1MIN_AUGMENTED_RELEASE",
            source_release_version=ACTIVE121_RELEASE,
            source_table="true1min_augmented_panel",
            source_field=name,
        )
        value_kind, unit_status = _active_value_kind(name, str(field["dtype"]))
        entity_scope = _active_entity_scope(name, external_entries)
        blocked = str(field.get("blocked_reason") or "")
        source_qualified = not blocked and str(field["role"]) != "blocked" and unit_status.startswith("ASSERTED")
        routes = _active_routes(field, entity_scope, source_qualified)
        eligible = source_qualified and bool(routes)
        exposure_blocker = (
            blocked
            or ("" if unit_status.startswith("ASSERTED") else unit_status)
            or ("" if routes else "NO_DIRECT_TYPED_ROUTE_USE_FROZEN_EVENT_ENTRY_OR_DERIVED_REPRESENTATION")
        )
        rep_id = representation_id(
            representation_type="direct_active121_field",
            source_field_ids=[source_id],
            parameters={"field": name, "transform": str(field.get("transform") or "identity")},
        )
        glossary.append(
            {
                "source_field_id": source_id,
                "provider": "CN_TRUE1MIN_AUGMENTED_RELEASE",
                "source_release_version": ACTIVE121_RELEASE,
                "source_table": "true1min_augmented_panel",
                "source_field": name,
                "value_kind": value_kind,
                "flow_or_stock": "bar_or_session_state",
                "period_basis": str(field["observable_clock"]),
                "consolidation_scope": "not_applicable",
                "sign_semantics": "source_native",
                "normalizer_candidates": "",
                "applicable_company_types": "all_listed_equities",
                "source_reported_change": any(token in name.lower() for token in ("change", "diff", "ratio", "ret")),
                "coverage": "release_schema_present",
                "unit_certainty": unit_status,
                "semantic_certainty": "ASSERTED_FROM_VERSIONED_ACTIVE121_REGISTRY",
                "semantic_family": str(field["family"]),
                "search_eligible": eligible,
                "qualification_blocker": exposure_blocker,
            }
        )
        capabilities.append(
            {
                "field_id": name,
                "source_field_id": source_id,
                "representation_id": rep_id,
                "source_family": str(field["family"]),
                "source_table": "true1min_augmented_panel",
                "source_field": name,
                "entity_scope": entity_scope,
                "temporal_semantics": _active_temporal_semantics(field, entity_scope),
                "observable_clock": str(field["observable_clock"]),
                "maturity_rule": f"{field['maturity']} {field['maturity_unit']}",
                "pit_status": "PIT_VERSIONED_HISTORY",
                "allowed_routes": routes,
                "search_eligible": eligible,
                "semantic_role": str(field["role"]),
                "support_unit": _active_support_unit(field, entity_scope),
                "field_role": str(field["role"]),
                "blocked_reason": exposure_blocker,
                "unit_status": unit_status,
                "source_lag": int(field.get("source_lag") or 0),
                "source_lag_unit": str(field.get("source_lag_unit") or "bars"),
                "reset_semantics": "SESSION_RESET" if field["family"] in {"raw_1min", "firstN"} else "SOURCE_DEFINED",
                "matched_control_required": False,
                "metadata": {
                    "dtype": field["dtype"],
                    "transform": field.get("transform"),
                    "external_phase2_join": name in external_entries,
                },
            }
        )
    return glossary, capabilities


def _synthetic_state_fields(active_capabilities: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_id = {str(row["field_id"]): row for row in active_capabilities}
    definitions = (
        ("state_intraday_return_sign", ("intraday_ret_from_open",), "Sign($intraday_ret_from_open)"),
        ("state_bar_return_sign", ("ret_1m",), "Sign($ret_1m)"),
        ("state_close_range_location_sign", ("close", "high", "low"), "Sign(Sub(Mul($close,2),Add($high,$low)))"),
    )
    rows: list[dict[str, Any]] = []
    for field_id, source_fields, expression in definitions:
        sources = [by_id[name] for name in source_fields]
        ids = [str(row["source_field_id"]) for row in sources]
        rows.append(
            {
                "field_id": field_id,
                "source_field_id": ids[0],
                "representation_id": representation_id(
                    representation_type="intraday_derived_state",
                    source_field_ids=ids,
                    parameters={"expression": expression},
                ),
                "source_family": "intraday_derived_state",
                "source_table": "true1min_augmented_panel",
                "source_field": "|".join(source_fields),
                "entity_scope": "STOCK",
                "temporal_semantics": "INTRADAY_DERIVED_STATE",
                "observable_clock": "bar_close",
                "maturity_rule": "bar_close",
                "pit_status": "PIT_VERSIONED_HISTORY",
                "allowed_routes": ["INTRADAY_STATE_TRANSITION"],
                "search_eligible": True,
                "semantic_role": "state-only",
                "support_unit": "symbol-state episode",
                "field_role": "state-only",
                "blocked_reason": "",
                "unit_status": "SOURCE_UNIT_GLOSSARY_ASSERTED",
                "source_lag": 0,
                "source_lag_unit": "bars",
                "reset_semantics": "SESSION_RESET",
                "matched_control_required": True,
                "metadata": {"materialization_expression": expression, "source_fields": list(source_fields)},
            }
        )
    return rows


def _sidecar_capabilities() -> list[dict[str, Any]]:
    definitions = (
        ("chip_distribution", CHIP_SIDECAR_VERSION, chip_field_specs(), "stock-session cross-section"),
        ("plate_market_context", TDX_PLATE_MARKET_VERSION, plate_market_field_specs(), "plate-session context"),
        ("true1min_plate_sparse", TRUE1MIN_PLATE_AGGREGATION_VERSION, true1min_plate_field_specs(), "stock-minute cross-section"),
    )
    rows: list[dict[str, Any]] = []
    for family, release, specs, support_unit in definitions:
        for spec in specs:
            field = spec.canonical()
            role = str(field["role"])
            routes: list[str] = []
            if family == "chip_distribution" and role == "interaction-only":
                routes = ["SLOW_CROSS_SECTIONAL_LEVEL", "SLOW_TEMPORAL_CHANGE"]
            elif family == "true1min_plate_sparse" and role == "interaction-only":
                routes = ["MINUTE_STATIC"]
            eligible = bool(routes)
            blocker = "" if eligible else f"ROLE_{role.upper().replace('-', '_')}_NOT_PAYLOAD_ELIGIBLE"
            source_id = source_field_id(
                provider="CN_PIT_SIDECAR",
                source_release_version=release,
                source_table=family,
                source_field=str(field["name"]),
            )
            rows.append(
                {
                    "field_id": field["name"],
                    "source_field_id": source_id,
                    "representation_id": representation_id(
                        representation_type=f"pit_sidecar_{family}",
                        source_field_ids=[source_id],
                        parameters={"transform": field["transform"], "role": role},
                    ),
                    "source_family": family,
                    "source_table": family,
                    "source_field": field["name"],
                    "entity_scope": "STOCK",
                    "temporal_semantics": (
                        "BAR_CLOSE_PIT_PLATE_CONTEXT"
                        if family == "true1min_plate_sparse"
                        else "PREVIOUS_SESSION_PIT_CONTEXT"
                    ),
                    "observable_clock": field["observable_clock"],
                    "maturity_rule": f'{field["maturity"]} {field["maturity_unit"]}',
                    "pit_status": "PIT_SAFE_WITH_REGISTERED_LAG_AND_MEMBERSHIP",
                    "allowed_routes": routes,
                    "search_eligible": eligible,
                    "semantic_role": role,
                    "support_unit": support_unit,
                    "field_role": role,
                    "blocked_reason": blocker,
                    "unit_status": "SIDECAR_FIELD_CONTRACT_ASSERTED",
                    "source_lag": int(field.get("source_lag") or 0),
                    "source_lag_unit": str(field.get("source_lag_unit") or "bars"),
                    "reset_semantics": "SESSION_RESET" if family == "true1min_plate_sparse" else "ASOF_HOLD",
                    "matched_control_required": False,
                    "metadata": {
                        "field_spec": field,
                        "formal_performance_search_allowed": False,
                        "pit_membership_release_required": family.startswith(("plate_", "true1min_plate")),
                        "survivorship_guard_required": family.startswith(("plate_", "true1min_plate")),
                    },
                }
            )
    return rows


def build(
    *,
    external_root: Path,
    fundamental_universe_path: Path,
    fundamental_registry_path: Path,
    fundamental_manifest_path: Path,
    active121_registry_path: Path,
    broad_event_pack_path: Path,
    output_root: Path,
    repo_sha: str,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    external_registry_path = external_root / "CN_UNIFIED_CAPABILITY_REGISTRY_V0_2_20260714.json"
    route_contract_path = external_root / "CN_TYPED_ROUTE_COMPILER_CONTRACT_V1_20260714.json"
    external_registry = json.loads(external_registry_path.read_text(encoding="utf-8"))
    route_contract = json.loads(route_contract_path.read_text(encoding="utf-8"))
    external_entries = {
        str(row["current_release_field"]): row for row in external_registry["field_entries"]
    }

    fundamental_source_rows = _read_csv(fundamental_universe_path)
    fundamental_semantics = json.loads(fundamental_registry_path.read_text(encoding="utf-8"))
    semantic_by_key = {
        (str(row["source_table"]), str(row["source_field"])): row
        for row in fundamental_semantics["fields"]
    }
    manifest = json.loads(fundamental_manifest_path.read_text(encoding="utf-8"))
    schema_hashes = {str(row["source_table"]): str(row["schema_sha256"]) for row in manifest["tables"]}
    source_identity_rows: list[dict[str, Any]] = []
    source_ids: dict[tuple[str, str], str] = {}
    for source in fundamental_source_rows:
        table = str(source["source_table"])
        field = str(source["source_field"])
        provider = PROVIDER_BY_TABLE[table]
        stable_id = source_field_id(
            provider=provider,
            source_release_version=FUNDAMENTAL_SOURCE_RELEASE,
            source_table=table,
            source_field=field,
        )
        source_ids[(table, field)] = stable_id
        source_identity_rows.append(
            {
                **source,
                "legacy_source_field_id": source["source_field_id"],
                "source_field_id": stable_id,
                "provider": provider,
                "source_release_version": FUNDAMENTAL_SOURCE_RELEASE,
                "source_schema_sha256": schema_hashes[table],
                "identity_version": "cn_source_field_identity_v2",
                "identity_payload_sha256": stable_hash(
                    {
                        "provider": provider,
                        "source_release_version": FUNDAMENTAL_SOURCE_RELEASE,
                        "source_table": table,
                        "source_field": field,
                    }
                ),
            }
        )
    qualified_fundamental = qualify_source_universe(source_identity_rows)

    active_registry = json.loads(active121_registry_path.read_text(encoding="utf-8"))
    active_glossary, active_capabilities = _active_source_glossary(active_registry["fields"], external_entries)
    state_capabilities = _synthetic_state_fields(active_capabilities)
    sidecar_capabilities = _sidecar_capabilities()

    representation_specs = canonical_representation_specs(source_ids)
    fundamental_capabilities: list[dict[str, Any]] = []
    qualified_by_key = {
        (str(row["source_table"]), str(row["source_field"])): row for row in qualified_fundamental
    }
    for spec in representation_specs:
        first = spec["source_fields"][0]
        source_row = qualified_by_key[(first["source_table"], first["source_field"])]
        fundamental_capabilities.append(
            {
                "field_id": spec["field_id"],
                "source_field_id": first["source_field_id"],
                "representation_id": spec["representation_id"],
                "source_family": "canonical_fundamental_" + spec["semantic_family"],
                "source_table": first["source_table"],
                "source_field": first["source_field"],
                "entity_scope": spec["entity_scope"],
                "temporal_semantics": spec["temporal_semantics"],
                "observable_clock": spec["observable_clock"],
                "maturity_rule": spec["maturity_rule"],
                "pit_status": spec["pit_status"],
                "allowed_routes": [spec["route_id"]] if spec["search_eligible"] else [],
                "search_eligible": spec["search_eligible"],
                "semantic_role": spec["representation_type"],
                "support_unit": spec["support_unit"],
                "field_role": spec["field_role"],
                "blocked_reason": spec["blocked_reason"],
                "unit_status": spec["unit_status"],
                "source_lag": 1,
                "source_lag_unit": "sessions",
                "reset_semantics": "DISCLOSURE_EPISODE" if spec["route_id"] == "DISCLOSURE_EVENT" else "ASOF_HOLD_WITH_STALENESS",
                "matched_control_required": spec["matched_control_required"],
                "metadata": {"canonical_representation": spec},
            }
        )

    event_pack = json.loads(broad_event_pack_path.read_text(encoding="utf-8"))
    mechanisms = event_pack.get("mechanisms") or event_pack.get("entries") or []
    event_capabilities: list[dict[str, Any]] = []
    for mechanism in mechanisms:
        mechanism_id = str(mechanism["mechanism_id"])
        source = str(mechanism["source"])
        sfid = source_field_id(
            provider="CN_BROAD_EVENT_RECOVERY",
            source_release_version=BROAD_EVENT_RELEASE,
            source_table="frozen_event_episode_registry",
            source_field=source,
        )
        event_capabilities.append(
            {
                "field_id": f"broad_event_{mechanism_id}",
                "source_field_id": sfid,
                "representation_id": representation_id(
                    representation_type="frozen_broad_event_mechanism",
                    source_field_ids=[sfid],
                    parameters={"mechanism_id": mechanism_id, "transform": mechanism.get("transform"), "variant": mechanism.get("variant")},
                ),
                "source_family": "broad_event_frozen_entry",
                "source_table": "frozen_event_episode_registry",
                "source_field": source,
                "entity_scope": "STOCK" if source != "MARKET_ECOLOGY_TRANSITION" else "MARKET",
                "temporal_semantics": "EVENT_EPISODE",
                "observable_clock": "episode_maturity_time",
                "maturity_rule": "registered_episode_maturity",
                "pit_status": "PIT_VERSIONED_HISTORY",
                "allowed_routes": ["BROAD_EVENT_FROZEN_ENTRY"],
                "search_eligible": True,
                "semantic_role": "frozen_event_entry",
                "support_unit": "event episode" if source != "MARKET_ECOLOGY_TRANSITION" else "market-time block",
                "field_role": "condition-only",
                "blocked_reason": "",
                "unit_status": "EVENT_SEMANTIC_CONTRACT_ASSERTED",
                "source_lag": 0,
                "source_lag_unit": "episodes",
                "reset_semantics": "EPISODE_DEFINED",
                "matched_control_required": True,
                "metadata": {"frozen_mechanism": mechanism},
            }
        )

    all_capabilities = (
        active_capabilities
        + state_capabilities
        + fundamental_capabilities
        + event_capabilities
        + sidecar_capabilities
    )
    registry_payload = {
        "registry_version": REGISTRY_VERSION,
        "status": "FROZEN_FOR_CAPABILITY_PREFLIGHT",
        "repo_sha": repo_sha,
        "authority_scope": "single semantic, PIT, identity and route authority; not performance authority",
        "external_contract_baseline": external_registry["evidence_baseline_repo_sha"],
        "external_contract_hashes": {
            path.name: _sha256(path) for path in sorted(external_root.glob("*")) if path.is_file()
        },
        "source_releases": {
            "active121": ACTIVE121_RELEASE,
            "fundamental": FUNDAMENTAL_SOURCE_RELEASE,
            "broad_event": BROAD_EVENT_RELEASE,
            "chip": CHIP_SIDECAR_VERSION,
            "plate_market": TDX_PLATE_MARKET_VERSION,
            "true1min_plate": TRUE1MIN_PLATE_AGGREGATION_VERSION,
        },
        "routes": route_contract["route_contracts"],
        "fields": all_capabilities,
        "field_count": len(all_capabilities),
        "source_identity_count": len(source_identity_rows) + len(active_glossary) + len(event_capabilities),
        "canonical_fundamental_root_count": len(representation_specs),
        "claim_ceilings": [
            "zero budget is NOT_EXECUTED",
            "wired without strict exposure is NOT_EVALUATED",
            "development increment is not validation or OOS",
            "two seeds on one development release prove frozen reproducibility only",
        ],
        "hard_boundaries": {
            "validation": "FORBIDDEN",
            "holdout": "FORBIDDEN",
            "challenge": "FORBIDDEN_IN_THIS_RESULT_DOMAIN",
            "forward_2026": "SEALED",
            "candidate_promotion": "FORBIDDEN",
            "cross_sprint_memory": "FORBIDDEN",
            "zygc_em": "PIT_CONTRACT_UNRESOLVED",
        },
    }
    frozen = freeze_registry(registry_payload, output_root / "unified_capability_registry.json")

    _write_csv(output_root / "source_field_identity_registry.csv", source_identity_rows)
    _write_csv(output_root / "source_unit_applicability_glossary.csv", qualified_fundamental + active_glossary)
    _write_csv(output_root / "canonical_fundamental_representation_registry.csv", representation_specs)
    (output_root / "source_field_identity_registry.json").write_text(
        json.dumps({"identity_version": "cn_source_field_identity_v2", "rows": source_identity_rows}, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_root / "source_unit_applicability_glossary.json").write_text(
        json.dumps({"glossary_version": "cn_source_unit_applicability_glossary_v1", "rows": qualified_fundamental + active_glossary}, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_root / "canonical_fundamental_representation_registry.json").write_text(
        json.dumps({"representation_version": "cn_canonical_fundamental_representation_v1", "rows": representation_specs}, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    join_rows = []
    for name, external in sorted(external_entries.items()):
        local = next((row for row in active_capabilities if row["field_id"] == name), None)
        join_rows.append(
            {
                "provisional_field_id": external["provisional_field_id"],
                "current_release_field": name,
                "join_status": "EXACT_ACTIVE121_JOIN" if local else "UNRESOLVED_ALIAS",
                "source_field_id": local["source_field_id"] if local else "",
                "representation_id": local["representation_id"] if local else "",
                "allowed_routes": "|".join(local["allowed_routes"]) if local else "",
                "blocked_reason": local["blocked_reason"] if local else "UNRESOLVED_ALIAS",
            }
        )
    _write_csv(output_root / "external_contract_source_join.csv", join_rows)

    summary = {
        "status": "CN_UNIFIED_CAPABILITY_REGISTRY_BUILT",
        "repo_sha": repo_sha,
        "registry_hash": frozen["registry_hash"],
        "field_count": len(all_capabilities),
        "route_eligible_counts": {
            route["route_id"]: sum(
                row["search_eligible"] and route["route_id"] in row["allowed_routes"]
                for row in all_capabilities
            )
            for route in route_contract["route_contracts"]
        },
        "fundamental_source_field_count": len(source_identity_rows),
        "fundamental_unit_qualified_count": sum(bool(row["search_eligible"]) for row in qualified_fundamental),
        "canonical_fundamental_root_count": len(representation_specs),
        "canonical_root_limit": 384,
        "external_active121_join_count": sum(row["join_status"] == "EXACT_ACTIVE121_JOIN" for row in join_rows),
        "external_active121_unresolved_count": sum(row["join_status"] != "EXACT_ACTIVE121_JOIN" for row in join_rows),
        "broad_event_mechanism_count": len(event_capabilities),
        "sidecar_field_count": len(sidecar_capabilities),
        "sidecar_search_eligible_count": sum(row["search_eligible"] for row in sidecar_capabilities),
        "zygc_status": "PIT_CONTRACT_UNRESOLVED",
    }
    if len(representation_specs) > 384:
        raise ValueError("canonical fundamental roots exceed frozen v1 limit")
    if summary["external_active121_unresolved_count"]:
        raise ValueError("external active121 aliases failed exact join")
    (output_root / "registry_build_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    supersession = {
        "supersession_version": "cn_unified_capability_contract_supersession_v1",
        "current_repo_sha": repo_sha,
        "task_declared_baseline": "d4319137dc9c86ad6eda250ce8fb15879d76ad7c",
        "effective_baseline": "70f8c3dff4e191631186b8c57d636956b558c2c8",
        "reason": "preserve subsequently pushed immutable Phase-1 field audit merge and release-bound source identity inputs",
        "external_evidence_baseline": external_registry["evidence_baseline_repo_sha"],
        "external_status_superseded": "PROVISIONAL_PENDING_FUNDAMENTAL_SOURCE_UNIVERSE_JOIN",
        "new_status": "SOURCE_UNIVERSE_JOINED_EXECUTABLE_REGISTRY_FROZEN",
        "identity_supersession": "cn_source::<table>::<field> retained as legacy ID; release-bound cn.sf.<hash> is authoritative",
        "preflight_authorization_supersession": "current user instruction authorizes automatic preflight, CANARY and development discovery; challenge/forward/promotion remain forbidden",
        "historical_results_modified": False,
        "broad_event_pack_modified": False,
    }
    (output_root / "CONTRACT_SUPERSESSION.json").write_text(
        json.dumps(supersession, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--external-root", type=Path, required=True)
    parser.add_argument("--fundamental-universe", type=Path, required=True)
    parser.add_argument("--fundamental-registry", type=Path, required=True)
    parser.add_argument("--fundamental-manifest", type=Path, required=True)
    parser.add_argument("--active121-registry", type=Path, required=True)
    parser.add_argument("--broad-event-pack", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--repo-sha", required=True)
    args = parser.parse_args(argv)
    result = build(
        external_root=args.external_root,
        fundamental_universe_path=args.fundamental_universe,
        fundamental_registry_path=args.fundamental_registry,
        fundamental_manifest_path=args.fundamental_manifest,
        active121_registry_path=args.active121_registry,
        broad_event_pack_path=args.broad_event_pack,
        output_root=args.output_root,
        repo_sha=args.repo_sha,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
