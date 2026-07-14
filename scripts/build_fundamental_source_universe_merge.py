"""Project the PIT Fundamental registry into a stable cross-audit merge table."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "cn_pit_fundamental_source_universe_merge_v1"
SCHEMA: tuple[tuple[str, str], ...] = (
    ("source_field_id", "string"),
    ("source_table", "string"),
    ("source_field", "string"),
    ("semantic_role", "pipe_delimited_enum"),
    ("entity_scope", "enum"),
    ("report_period_field", "string"),
    ("observable_time_field", "string"),
    ("revision_time_field", "string"),
    ("pit_status", "enum"),
    ("fabric_field_id", "string"),
    ("materialization_status", "enum"),
    ("recommended_search_routes", "pipe_delimited_enum"),
    ("blocked_reason", "string"),
    ("source_family", "string"),
    ("source_dtype", "string"),
    ("observable_time_policy", "enum"),
    ("fabric_typed_routes", "pipe_delimited_route"),
    ("current_121_presence", "enum"),
    ("current_121_field", "string"),
    ("current_generator_exposure", "boolean"),
    ("development_safe_non_null", "nullable_integer"),
    ("development_safe_missing", "nullable_integer"),
)

CLOCK_FIELDS = {
    "balance_sheet_report_em": ("REPORT_DATE", "NOTICE_DATE", "UPDATE_DATE"),
    "profit_sheet_report_em": ("REPORT_DATE", "NOTICE_DATE", "UPDATE_DATE"),
    "cash_flow_sheet_report_em": ("REPORT_DATE", "NOTICE_DATE", "UPDATE_DATE"),
    "main_stock_holder_sina": ("截至日期", "公告日期", ""),
    "zygc_em": ("报告日期", "", ""),
}

SEARCH_ROUTE_BY_ROLE = {
    "FUNDAMENTAL_LEVEL": "SLOW_XS_LEVEL",
    "FUNDAMENTAL_CHANGE": "SLOW_TEMPORAL_CHANGE",
    "DISCLOSURE_EVENT": "DISCLOSURE_EVENT",
    "FUNDAMENTAL_CONDITION": "FUNDAMENTAL_CONDITION",
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def schema_hash() -> str:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "columns": [{"name": name, "type": kind} for name, kind in SCHEMA],
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _materialization_status(field: dict[str, Any]) -> str:
    if field["pit_status"] == "PIT_CONTRACT_UNRESOLVED":
        return "BLOCKED_PIT_CONTRACT_UNRESOLVED"
    if field["semantic_roles"] == "METADATA_BLOCKED":
        return "SOURCE_METADATA_SIDECAR_ONLY"
    if field.get("current_121_presence") == "EXACT_NAME":
        return "CURRENT_121_MATERIALIZED_AND_PIT_SIDECAR_REGISTERED"
    if field.get("current_121_presence") == "SEMANTIC_EQUIVALENT_OTHER_SOURCE":
        return "PIT_SIDECAR_REGISTERED_WITH_121_SEMANTIC_EQUIVALENT"
    return "PIT_SIDECAR_REGISTERED_LAZY_NOT_MINUTE_EXPANDED"


def _recommended_search_routes(field: dict[str, Any]) -> str:
    roles = str(field["semantic_roles"]).split("|")
    if roles == ["METADATA_BLOCKED"]:
        return "BLOCKED"
    return "|".join(SEARCH_ROUTE_BY_ROLE[role] for role in roles if role in SEARCH_ROUTE_BY_ROLE)


def project_field(field: dict[str, Any]) -> dict[str, Any]:
    table = str(field["source_table"])
    if table not in CLOCK_FIELDS:
        raise ValueError(f"missing clock contract for source table: {table}")
    report_period, observable_time, revision_time = CLOCK_FIELDS[table]
    typed_routes = [
        field.get("level_route"),
        field.get("change_route"),
        field.get("event_route"),
        field.get("condition_route"),
    ]
    return {
        "source_field_id": f"cn_source::{table}::{field['source_field']}",
        "source_table": table,
        "source_field": field["source_field"],
        "semantic_role": field["semantic_roles"],
        "entity_scope": field["entity_scope"],
        "report_period_field": report_period,
        "observable_time_field": observable_time,
        "revision_time_field": revision_time,
        "pit_status": field["pit_status"],
        "fabric_field_id": field["field_id"],
        "materialization_status": _materialization_status(field),
        "recommended_search_routes": _recommended_search_routes(field),
        "blocked_reason": field.get("blocker", ""),
        "source_family": field["source_family"],
        "source_dtype": field["source_dtype"],
        "observable_time_policy": field["observable_time_policy"],
        "fabric_typed_routes": "|".join(route for route in typed_routes if route),
        "current_121_presence": field["current_121_presence"],
        "current_121_field": field.get("current_121_field", ""),
        "current_generator_exposure": bool(field["current_generator_exposure"]),
        "development_safe_non_null": field.get("development_safe_non_null"),
        "development_safe_missing": field.get("development_safe_missing"),
    }


def build(*, registry_path: Path, output_path: Path, schema_output_path: Path) -> dict[str, Any]:
    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    fields = registry["fields"]
    rows = sorted(
        (project_field(field) for field in fields),
        key=lambda row: (str(row["source_table"]), str(row["source_field"])),
    )
    identifiers = [row["source_field_id"] for row in rows]
    if len(identifiers) != len(set(identifiers)):
        raise ValueError("source_field_id collision")
    if len(rows) != int(registry["field_count"]):
        raise ValueError("registry field_count mismatch")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=[name for name, _ in SCHEMA], lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

    schema_payload = {
        "schema_version": SCHEMA_VERSION,
        "schema_sha256": schema_hash(),
        "columns": [{"name": name, "type": kind} for name, kind in SCHEMA],
        "row_count": len(rows),
        "source_registry": str(registry_path),
        "source_registry_sha256": _sha256(registry_path),
        "output": str(output_path),
        "output_sha256": _sha256(output_path),
        "generator_exposed_rows": sum(bool(row["current_generator_exposure"]) for row in rows),
        "pit_unresolved_rows": sum(row["pit_status"] == "PIT_CONTRACT_UNRESOLVED" for row in rows),
    }
    schema_output_path.write_text(
        json.dumps(schema_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return schema_payload


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--schema-output", type=Path, required=True)
    args = parser.parse_args()
    print(
        json.dumps(
            build(
                registry_path=args.registry,
                output_path=args.output,
                schema_output_path=args.schema_output,
            ),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
