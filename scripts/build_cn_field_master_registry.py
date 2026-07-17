"""Build the authoritative CN field inventory without performance access."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable

from our_system_phase2.services.chip_sidecar import chip_field_specs
from our_system_phase2.services.tdx_plate_market_sidecar import plate_market_field_specs
from our_system_phase2.services.true1min_plate_aggregation import true1min_plate_field_specs


REPO = Path(__file__).resolve().parents[1]
OUTPUT = REPO / "runtime/field_registry/cn_field_master_registry_v1"
COLUMNS = (
    "field_uid", "field_name", "record_kind", "data_family", "source_table",
    "source_field", "entity_scope", "semantic_role", "field_role", "dtype",
    "observable_time", "maturity", "pit_status", "materialization_status",
    "search_status", "recommended_routes", "source_artifact", "blocked_reason",
)


def _text(value: Any) -> str:
    if isinstance(value, (list, tuple)):
        return "|".join(map(str, value))
    if value is None:
        return ""
    return str(value)


def _normalized(rows: Iterable[dict[str, Any]]) -> list[dict[str, str]]:
    output = [{key: _text(row.get(key, "")) for key in COLUMNS} for row in rows]
    output.sort(key=lambda row: (row["record_kind"], row["field_uid"]))
    identities = [row["field_uid"] for row in output]
    if len(identities) != len(set(identities)):
        raise ValueError("duplicate master field_uid")
    return output


def build(output: Path = OUTPUT) -> dict[str, Any]:
    source_path = REPO / "reports/cn_field_universe_search_exposure_audit_20260714/fundamental_source_universe.csv"
    capability_path = REPO / "runtime/field_registry/cn_unified_capability_registry_v3_20260717/unified_capability_registry.json"
    active_path = REPO / "runtime/field_registry/nextgen_dark_field_registry_v2.json"
    with source_path.open("r", encoding="utf-8", newline="") as handle:
        fundamental_sources = list(csv.DictReader(handle))
    capability = json.loads(capability_path.read_text(encoding="utf-8"))
    active = json.loads(active_path.read_text(encoding="utf-8"))

    rows: list[dict[str, Any]] = []
    for row in fundamental_sources:
        rows.append({
            "field_uid": f'source::{row["source_field_id"]}',
            "field_name": row["fabric_field_id"],
            "record_kind": "SOURCE_FIELD",
            "data_family": row["source_family"],
            "source_table": row["source_table"],
            "source_field": row["source_field"],
            "entity_scope": row["entity_scope"],
            "semantic_role": row["semantic_role"],
            "field_role": "blocked" if row["blocked_reason"] else "source-only",
            "dtype": row["source_dtype"],
            "observable_time": row["observable_time_policy"],
            "maturity": "next_session_after_disclosure",
            "pit_status": row["pit_status"],
            "materialization_status": row["materialization_status"],
            "search_status": "NOT_DIRECTLY_EXPOSED",
            "recommended_routes": row["recommended_search_routes"],
            "source_artifact": source_path.relative_to(REPO).as_posix(),
            "blocked_reason": row["blocked_reason"],
        })

    for row in capability["fields"]:
        rows.append({
            "field_uid": f'capability::{row["field_id"]}',
            "field_name": row["field_id"],
            "record_kind": "CAPABILITY_FIELD",
            "data_family": row["source_family"],
            "source_table": row["source_table"],
            "source_field": row["source_field"],
            "entity_scope": row["entity_scope"],
            "semantic_role": row["semantic_role"],
            "field_role": row["field_role"],
            "observable_time": row["observable_clock"],
            "maturity": row["maturity_rule"],
            "pit_status": row["pit_status"],
            "materialization_status": "REGISTERED_TYPED_CAPABILITY",
            "search_status": "ROUTE_ELIGIBLE" if row["search_eligible"] else "BLOCKED",
            "recommended_routes": row["allowed_routes"],
            "source_artifact": capability_path.relative_to(REPO).as_posix(),
            "blocked_reason": row["blocked_reason"],
        })

    capability_by_name = {str(row["field_id"]) for row in capability["fields"]}

    sidecars = (
        ("CHIP_SIDECAR_FIELD", "chip_distribution", "STOCK", chip_field_specs(), "G:/BaiduNetdiskDownload/每日筹码及胜率.zip"),
        ("PLATE_MARKET_FIELD", "plate_market_context", "PLATE", plate_market_field_specs(), "G:/BaiduNetdiskDownload/*板块_历史行情数据"),
        ("PLATE_TRUE1MIN_FIELD", "true1min_plate_sparse", "STOCK_PLATE", true1min_plate_field_specs(), "G:/BaiduNetdiskDownload/板块成分_东财"),
    )
    for kind, family, scope, specs, artifact in sidecars:
        for spec in specs:
            row = spec.canonical()
            if row["name"] in capability_by_name:
                continue
            rows.append({
                "field_uid": f'sidecar::{row["name"]}',
                "field_name": row["name"],
                "record_kind": kind,
                "data_family": family,
                "source_table": family,
                "source_field": row["source_fields"],
                "entity_scope": scope,
                "semantic_role": "PIT_CONTEXT",
                "field_role": row["role"],
                "dtype": row["dtype"],
                "observable_time": row["observable_clock"],
                "maturity": f'{row["maturity"]} {row["maturity_unit"]}',
                "pit_status": "PIT_SAFE_WITH_REGISTERED_LAG",
                "materialization_status": "SIDECAR_IMPLEMENTED_NOT_MINUTE_EXPANDED",
                "search_status": "NOT_FORMALLY_SEARCH_ENABLED",
                "recommended_routes": row["role"],
                "source_artifact": artifact,
                "blocked_reason": row["blocked_reason"],
            })

    for name, dtype in (
        ("group_id", "string"), ("group_type", "string"),
        ("effective_from", "datetime64[ns]"), ("effective_to", "datetime64[ns]"),
        ("source_observed_at", "datetime64[ns]"), ("source_observed_to", "datetime64[ns]"),
    ):
        rows.append({
            "field_uid": f"membership::{name}", "field_name": name,
            "record_kind": "PLATE_MEMBERSHIP_METADATA", "data_family": "pit_plate_membership",
            "source_table": "plate_membership_release", "source_field": name,
            "entity_scope": "STOCK_PLATE", "semantic_role": "METADATA_BLOCKED",
            "field_role": "blocked", "dtype": dtype, "observable_time": "source_observed_at",
            "maturity": "max(effective_time,observed_time)", "pit_status": "PIT_SAFE_RELEASE_REQUIRED",
            "materialization_status": "PIT_MEMBERSHIP_SIDECAR_IMPLEMENTED",
            "search_status": "METADATA_NOT_SEARCHABLE", "recommended_routes": "join-only",
            "source_artifact": "G:/BaiduNetdiskDownload/板块成分_东财",
            "blocked_reason": "membership key/clock metadata is not a direct signal",
        })

    normalized = _normalized(rows)
    payload = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    schema_hash = hashlib.sha256(("|".join(COLUMNS)).encode()).hexdigest()
    content_hash = hashlib.sha256(payload).hexdigest()
    output.mkdir(parents=True, exist_ok=True)
    csv_path = output / "cn_field_master_registry_v1.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader(); writer.writerows(normalized)
    counts: dict[str, int] = {}
    for row in normalized:
        counts[row["record_kind"]] = counts.get(row["record_kind"], 0) + 1
    document = {
        "registry_version": "cn_field_master_registry_v1",
        "authority_status": "AUTHORITATIVE_FIELD_UNIVERSE",
        "supersedes_as_total_table": active_path.relative_to(REPO).as_posix(),
        "schema": list(COLUMNS), "schema_sha256": schema_hash,
        "content_sha256": content_hash, "field_record_count": len(normalized),
        "record_kind_counts": counts,
        "performance_or_reward_used": False, "forward_2026_accessed": False,
        "rows": normalized,
    }
    (output / "cn_field_master_registry_v1.json").write_text(
        json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return {key: document[key] for key in ("field_record_count", "record_kind_counts", "schema_sha256", "content_sha256")}


if __name__ == "__main__":
    print(json.dumps(build(), ensure_ascii=False, indent=2, sort_keys=True))
