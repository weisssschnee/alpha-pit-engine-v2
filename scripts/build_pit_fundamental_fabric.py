from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
import time
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from our_system_phase2.services.pit_fundamental_fabric import (
    FABRIC_VERSION,
    FINANCIAL_TABLES,
    conservative_financial_versions,
    conservative_holder_episodes,
    stable_hash,
)


DATASETS: dict[str, dict[str, Any]] = {
    "balance_sheet_report_em": {
        "family": "fundamental_balance_sheet",
        "entity_scope": "STOCK_REPORT_PERIOD",
        "status": "PIT_SAFE_CURRENT_SNAPSHOT_ONLY",
    },
    "profit_sheet_report_em": {
        "family": "fundamental_profit_sheet",
        "entity_scope": "STOCK_REPORT_PERIOD",
        "status": "PIT_SAFE_CURRENT_SNAPSHOT_ONLY",
    },
    "cash_flow_sheet_report_em": {
        "family": "fundamental_cashflow_sheet",
        "entity_scope": "STOCK_REPORT_PERIOD",
        "status": "PIT_SAFE_CURRENT_SNAPSHOT_ONLY",
    },
    "zygc_em": {
        "family": "business_composition",
        "entity_scope": "STOCK_REPORT_PERIOD_COMPONENT",
        "status": "PIT_CONTRACT_UNRESOLVED",
    },
    "main_stock_holder_sina": {
        "family": "major_shareholder_structure",
        "entity_scope": "STOCK_DISCLOSURE_HOLDER",
        "status": "PIT_SAFE_WHEN_ANNOUNCEMENT_PRESENT",
    },
}

SOURCE_METADATA = {
    "source_dataset", "source_family", "source_code6", "source_em", "source_file",
    "ingest_time_utc", "SECURITY_CODE", "SECUCODE", "SECURITY_NAME_ABBR", "ORG_CODE",
    "ORG_TYPE", "REPORT_DATE", "REPORT_TYPE", "REPORT_DATE_NAME", "NOTICE_DATE",
    "UPDATE_DATE", "股票代码", "报告日期", "公告日期", "截至日期", "编号",
}
HOLDER_CONDITION_FIELDS = {"股本性质"}
HOLDER_METADATA_FIELDS = {"股东名称", "股东说明"}
CURRENT_EQUIVALENTS = {
    ("main_stock_holder_sina", "股东总数"): "ctx_holder_holder_num",
    ("main_stock_holder_sina", "平均持股数"): "ctx_holder_avg_hold_num",
}


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    materialized = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not materialized:
        path.write_text("", encoding="utf-8")
        return
    columns: list[str] = []
    for row in materialized:
        for key in row:
            if key not in columns:
                columns.append(key)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(materialized)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _repo_sha(repo: Path) -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    except Exception:
        return "UNAVAILABLE_REMOTE_BUILD"


def _arrow_schema(path: Path) -> pa.Schema:
    return pq.ParquetFile(path).schema_arrow


def _schema_hash(schema: pa.Schema) -> str:
    return hashlib.sha256(schema.serialize().to_pybytes()).hexdigest()


def _table_hash(table: pa.Table) -> str:
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return hashlib.sha256(sink.getvalue().to_pybytes()).hexdigest()


def _roles(table: str, field: pa.Field) -> tuple[list[str], str, str, bool]:
    name = field.name
    if name in SOURCE_METADATA or name.startswith("source_"):
        return ["METADATA_BLOCKED"], "source key/clock/provenance metadata", "metadata.blocked", False
    if table == "zygc_em":
        if pa.types.is_integer(field.type) or pa.types.is_floating(field.type) or pa.types.is_decimal(field.type):
            return ["FUNDAMENTAL_LEVEL", "FUNDAMENTAL_CHANGE", "DISCLOSURE_EVENT"], "business-composition source value; PIT clock unresolved", "pit_unresolved", False
        return ["METADATA_BLOCKED"], "business-composition label or metadata; PIT clock unresolved", "pit_unresolved", False
    if table == "main_stock_holder_sina":
        if name in HOLDER_CONDITION_FIELDS:
            return ["FUNDAMENTAL_CONDITION"], "source-native shareholder capital-nature condition", "fundamental.condition", False
        if name in HOLDER_METADATA_FIELDS:
            return ["METADATA_BLOCKED"], "holder identity/free-text metadata", "metadata.blocked", False
        if pa.types.is_integer(field.type) or pa.types.is_floating(field.type) or pa.types.is_decimal(field.type):
            return ["FUNDAMENTAL_LEVEL", "FUNDAMENTAL_CHANGE", "DISCLOSURE_EVENT"], "source-native holder value at a disclosure episode; aggregation contract required for holder-level rows", "fundamental.holder", True
        return ["METADATA_BLOCKED"], "untyped holder source metadata", "metadata.blocked", False
    if pa.types.is_integer(field.type) or pa.types.is_floating(field.type) or pa.types.is_decimal(field.type):
        if name.upper().endswith("_YOY"):
            return ["FUNDAMENTAL_CHANGE", "DISCLOSURE_EVENT"], "source-reported YoY value in the safely observable current version", "fundamental.change.source_reported_yoy", True
        return ["FUNDAMENTAL_LEVEL", "FUNDAMENTAL_CHANGE", "DISCLOSURE_EVENT"], "source-native financial statement cell; exact identifier retained without inferred relabeling", "fundamental.statement", True
    return ["METADATA_BLOCKED"], "non-numeric statement metadata; no direct search route", "metadata.blocked", False


def _allowed_change_transforms(table: str, field_name: str, roles: list[str]) -> list[str]:
    if "FUNDAMENTAL_CHANGE" not in roles:
        return []
    if field_name.upper().endswith("_YOY"):
        return ["source_reported_yoy"]
    if table == "balance_sheet_report_em":
        return ["delta", "yoy", "slope", "persistence", "acceleration"]
    if table in {"profit_sheet_report_em", "cash_flow_sheet_report_em"}:
        return ["delta", "yoy", "ttm", "slope", "persistence", "acceleration"]
    if table == "main_stock_holder_sina":
        return ["delta_after_episode_aggregation", "yoy_after_episode_aggregation"]
    return []


def _observable_contract() -> dict[str, Any]:
    return {
        "contract_version": "cn_pit_fundamental_observable_time_contract_v1",
        "fabric_version": FABRIC_VERSION,
        "timezone": "Asia/Shanghai",
        "date_only_maturity": "NEXT_DECLARED_CN_TRADING_SESSION_OPEN_09_30_STRICTLY_AFTER_SOURCE_DATE",
        "prohibitions": [
            "REPORT_PERIOD_IS_NOT_OBSERVABLE_TIME",
            "NO_REPORT_DATE_PLUS_GUESSED_LAG",
            "NO_FINAL_REVISION_BACKFILL",
            "NO_CURRENT_SNAPSHOT_BEFORE_LAST_UPDATE",
            "NO_FUTURE_ANNUAL_TTM_BACKSOLVE",
            "NO_VALIDATION_HOLDOUT_OR_2026_VALUE_ACCESS",
        ],
        "datasets": {
            "balance_sheet_report_em": {
                "report_period": "REPORT_DATE",
                "initial_disclosure_date": "NOTICE_DATE",
                "revision_or_last_update_date": "UPDATE_DATE",
                "safe_current_version_source_date": "max(NOTICE_DATE, UPDATE_DATE)",
                "required_non_null": ["REPORT_DATE", "NOTICE_DATE", "UPDATE_DATE"],
                "historical_revision_replay": "UNAVAILABLE_SOURCE_HAS_ONE_CURRENT_SNAPSHOT_ROW_PER_REPORT_PERIOD",
                "event_policy": "initial disclosure pulse only when UPDATE_DATE equals NOTICE_DATE; later snapshot is level-eligible at UPDATE_DATE but not asserted as a market event",
            },
            "profit_sheet_report_em": {"inherits": "balance_sheet_report_em"},
            "cash_flow_sheet_report_em": {"inherits": "balance_sheet_report_em"},
            "main_stock_holder_sina": {
                "report_period": "截至日期",
                "initial_disclosure_date": "公告日期",
                "safe_current_version_source_date": "公告日期",
                "required_non_null": ["截至日期", "公告日期"],
                "missing_announcement_policy": "PIT_CONTRACT_UNRESOLVED_ROW_EXCLUDED",
                "episode_key": ["source_code6", "截至日期", "公告日期"],
            },
            "zygc_em": {
                "report_period": "报告日期",
                "status": "PIT_CONTRACT_UNRESOLVED",
                "reason": "source schema has report period but no independent disclosure/announcement/observed time",
                "report_date_fallback_allowed": False,
            },
        },
        "ttm": {
            "annual": "annual disclosed value",
            "regular_interim": "prior disclosed annual + current disclosed YTD - prior-year comparable disclosed YTD",
            "required": "every component version observable at as-of time",
            "irregular_period": "UNKNOWN",
            "missing_component": "UNKNOWN",
        },
    }


def _revision_policy_markdown() -> str:
    return """# CN PIT Fundamental Revision Policy v1

The local statement archive is a current provider snapshot, not a revision tape. Each
statement/report-period normally has one row. `NOTICE_DATE` records the initial notice
date and `UPDATE_DATE` records the source's last update date, but the superseded values
are absent.

## Safe rule

- Never use `REPORT_DATE` as observable time.
- Require `NOTICE_DATE`, `UPDATE_DATE`, and `REPORT_DATE` for statement rows.
- Treat the current value as eligible only from the next declared trading session after
  `max(NOTICE_DATE, UPDATE_DATE)`.
- If `UPDATE_DATE > NOTICE_DATE`, do not reconstruct or impute the initial value. Before
  the update, the value is unknown. The row may supply a level after the safe time but
  cannot claim a measured revision delta or an initial-disclosure event.
- If any required clock is missing, exclude the row as `PIT_CONTRACT_UNRESOLVED`.
- Date-only announcements mature at the next session open (09:30 Asia/Shanghai), never
  on the same trading date.

## TTM and temporal changes

TTM uses only components whose current versions were already observable at the query
time. Annual rows equal the disclosed annual value. Regular Q1/H1/Q3 values use prior
annual plus current YTD minus the prior-year comparable YTD. Irregular periods or
missing components remain unknown. Generic QoQ/YoY/slope/persistence/acceleration must
operate on the as-of disclosed sequence; they may not read a later version.

## Dataset-specific isolation

`zygc_em` contains only report dates, so its values and event pulses remain
`PIT_CONTRACT_UNRESOLVED`. Holder rows missing `公告日期` are excluded individually;
other holder disclosures can proceed. No unresolved family is replaced by a guessed
lag or a current snapshot.
"""


def _read_split(split_manifest: Path) -> tuple[pd.DatetimeIndex, pd.Timestamp, dict[str, int]]:
    frame = pd.read_csv(split_manifest, usecols=["trade_date", "split"])
    frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="raise")
    counts = frame["split"].astype(str).str.lower().value_counts().to_dict()
    train = frame.loc[frame["split"].astype(str).str.lower().eq("train"), "trade_date"]
    if train.empty:
        raise ValueError("split manifest has no development/train sessions")
    calendar = pd.DatetimeIndex(frame["trade_date"].sort_values().unique())
    maximum = train.max() + pd.Timedelta(hours=15)
    return calendar, maximum, {str(key): int(value) for key, value in counts.items()}


def _scan_dataset(
    root: Path,
    table: str,
    *,
    calendar: pd.DatetimeIndex,
    maximum: pd.Timestamp,
    scan_values: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any], Counter[str], dict[str, dict[str, Any]]]:
    dataset_root = root / table
    files = sorted(dataset_root.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"no parquet files under {dataset_root}")
    schema_hashes: Counter[str] = Counter()
    union_fields: dict[str, dict[str, Any]] = {}
    index_rows: list[dict[str, Any]] = []
    nulls: Counter[str] = Counter()
    nonnulls: Counter[str] = Counter()
    safe_rows = 0
    duplicate_keys = 0
    unique_episodes = 0
    revised_snapshot_rows = 0
    initial_unchanged_rows = 0
    unresolved_rows = 0
    development_digest = hashlib.sha256()
    values_read = 0
    started = time.perf_counter()
    cutoff_date = maximum.date()
    for path in files:
        parquet = pq.ParquetFile(path)
        schema = parquet.schema_arrow
        for field in schema:
            entry = union_fields.setdefault(field.name, {"field": field, "types": Counter()})
            entry["types"][str(field.type)] += 1
        current_schema_hash = _schema_hash(schema)
        schema_hashes[current_schema_hash] += 1
        metadata = parquet.metadata
        row = {
            "source_table": table,
            "relative_path": path.relative_to(root).as_posix(),
            "bytes": int(path.stat().st_size),
            "rows_footer": int(metadata.num_rows),
            "row_groups": int(metadata.num_row_groups),
            "schema_sha256": current_schema_hash,
            "development_safe_rows": 0,
            "development_safe_content_sha256": "NOT_SCANNED",
        }
        if scan_values and table != "zygc_em":
            if table in FINANCIAL_TABLES:
                filters = [("NOTICE_DATE", "<=", cutoff_date), ("UPDATE_DATE", "<=", cutoff_date)]
            else:
                filters = [("公告日期", "<=", cutoff_date)]
            arrow = pq.read_table(path, columns=schema.names, filters=filters, use_threads=False)
            values_read += int(arrow.nbytes)
            frame = arrow.to_pandas()
            if table in FINANCIAL_TABLES:
                prepared = conservative_financial_versions(
                    frame, table=table, sessions=calendar, maximum_observable_time=maximum
                )
                eligible = prepared.loc[prepared["pit_status"].eq("ELIGIBLE_CURRENT_SNAPSHOT_VERSION")]
                revised_snapshot_rows += int(eligible["version_kind"].eq("LATEST_REVISED_SNAPSHOT_ONLY").sum())
                initial_unchanged_rows += int(eligible["version_kind"].eq("INITIAL_UNCHANGED_CURRENT_VERSION").sum())
                unresolved_rows += int(prepared["pit_status"].eq("PIT_CONTRACT_UNRESOLVED").sum())
                duplicate_keys += int(
                    eligible.duplicated(["code", "report_period"], keep=False).sum()
                )
            else:
                prepared = conservative_holder_episodes(
                    frame, sessions=calendar, maximum_observable_time=maximum
                )
                eligible = prepared.loc[prepared["pit_status"].eq("ELIGIBLE_DISCLOSURE_EPISODE")]
                unresolved_rows += int(prepared["pit_status"].eq("PIT_CONTRACT_UNRESOLVED").sum())
                unique_episodes += int(eligible["episode_id"].nunique())
            safe_rows += int(len(eligible))
            safe_arrow = pa.Table.from_pandas(eligible[schema.names], preserve_index=False)
            content_hash = _table_hash(safe_arrow)
            development_digest.update(path.name.encode("utf-8"))
            development_digest.update(content_hash.encode("ascii"))
            row["development_safe_rows"] = int(len(eligible))
            row["development_safe_content_sha256"] = content_hash
            for field in schema.names:
                count = int(eligible[field].notna().sum())
                nonnulls[field] += count
                nulls[field] += int(len(eligible) - count)
        index_rows.append(row)
    elapsed = time.perf_counter() - started
    if scan_values and table != "zygc_em":
        # A column absent from a symbol's physical schema is missing for every
        # eligible row in that file.  Union-schema missingness therefore uses
        # total eligible rows minus non-null cells, not only Arrow null counts
        # from files where the column happened to exist.
        for field_name in union_fields:
            nulls[field_name] = safe_rows - nonnulls[field_name]
    coverage = {
        "source_table": table,
        "source_files": len(files),
        "source_bytes": int(sum(row["bytes"] for row in index_rows)),
        "source_rows_footer": int(sum(row["rows_footer"] for row in index_rows)),
        "schema_field_count": len(union_fields),
        "schema_variants": dict(schema_hashes),
        "development_safe_rows": safe_rows if scan_values and table != "zygc_em" else None,
        "duplicate_financial_version_key_rows": duplicate_keys if table in FINANCIAL_TABLES and scan_values else None,
        "development_unique_disclosure_episodes": unique_episodes if table == "main_stock_holder_sina" and scan_values else None,
        "initial_unchanged_current_version_rows": initial_unchanged_rows if table in FINANCIAL_TABLES and scan_values else None,
        "latest_revised_snapshot_only_rows": revised_snapshot_rows if table in FINANCIAL_TABLES and scan_values else None,
        "pit_unresolved_rows_seen_in_filtered_scan": unresolved_rows if scan_values and table != "zygc_em" else None,
        "values_read_bytes_arrow": values_read,
        "elapsed_seconds": round(elapsed, 6),
        "development_safe_content_sha256": development_digest.hexdigest() if scan_values and table != "zygc_em" else "NOT_SCANNED_PIT_UNRESOLVED",
        "value_scan_status": "PIT_UNRESOLVED_VALUES_NOT_READ" if table == "zygc_em" else ("DEVELOPMENT_ONLY_COMPLETE" if scan_values else "NOT_SCANNED"),
    }
    return (
        index_rows,
        coverage,
        nonnulls | Counter({f"__NULL__{key}": value for key, value in nulls.items()}),
        union_fields,
    )


def build(args: argparse.Namespace) -> dict[str, Any]:
    started_at = datetime.now(timezone.utc)
    started = time.perf_counter()
    repo = args.repo.resolve()
    source_root = args.source_root.resolve()
    runtime_root = args.runtime_root.resolve()
    report_root = args.report_root.resolve()
    runtime_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    calendar, maximum, split_counts = _read_split(args.split_manifest)
    current_registry = json.loads(args.current_registry.read_text(encoding="utf-8"))
    current_names = {row["name"] for row in current_registry["fields"]}
    declared_clock_coverage: dict[str, dict[str, Any]] = {}
    clock_metadata_path = source_root.parent / "metadata" / "pit_availability_table.csv"
    if clock_metadata_path.exists():
        for row in pd.read_csv(clock_metadata_path).to_dict("records"):
            declared_clock_coverage[str(row.get("dataset"))] = {
                "pit_date_columns": row.get("pit_date_columns", ""),
                "primary_available_time": row.get("primary_available_time", ""),
                "pit_present_cells": row.get("pit_present_cells", ""),
                "pit_missing_cells": row.get("pit_missing_cells", ""),
                "evidence_path": str(clock_metadata_path),
                "evidence_kind": "SOURCE_RELEASE_METADATA_ONLY_NO_VALUE_ROWS",
            }

    universe: list[dict[str, Any]] = []
    semantic_fields: list[dict[str, Any]] = []
    gap_rows: list[dict[str, Any]] = []
    file_index: list[dict[str, Any]] = []
    coverage_tables: list[dict[str, Any]] = []
    coverage_fields: list[dict[str, Any]] = []
    source_table_manifests: list[dict[str, Any]] = []

    for table, config in DATASETS.items():
        files = sorted((source_root / table).glob("*.parquet"))
        if not files:
            raise FileNotFoundError(source_root / table)
        index_rows, coverage, counts, union_fields = _scan_dataset(
            source_root,
            table,
            calendar=calendar,
            maximum=maximum,
            scan_values=args.scan_coverage,
        )
        coverage["declared_clock_coverage"] = declared_clock_coverage.get(table, {})
        file_index.extend(index_rows)
        coverage_tables.append(coverage)
        union_schema_contract = {
            name: dict(entry["types"])
            for name, entry in sorted(union_fields.items())
        }
        table_schema_hash = stable_hash(union_schema_contract)
        source_table_manifests.append(
            {
                "source_table": table,
                "family": config["family"],
                "status": config["status"],
                "partition_root": str(source_root / table),
                "file_count": len(files),
                "bytes": coverage["source_bytes"],
                "rows_footer": coverage["source_rows_footer"],
                "schema_sha256": table_schema_hash,
                "development_safe_content_sha256": coverage["development_safe_content_sha256"],
            }
        )
        for field_name, field_entry in sorted(union_fields.items()):
            field = field_entry["field"]
            source_dtypes = dict(field_entry["types"])
            roles, semantic, typed_route, future_usable = _roles(table, field)
            allowed_changes = _allowed_change_transforms(table, field.name, roles)
            current_equivalent = CURRENT_EQUIVALENTS.get((table, field.name), "")
            exact_presence = field.name in current_names
            pit_status = config["status"]
            generator_exposure = False
            blocker = ""
            if pit_status == "PIT_CONTRACT_UNRESOLVED":
                blocker = "NO_INDEPENDENT_DISCLOSURE_OR_OBSERVABLE_TIME"
            elif roles == ["METADATA_BLOCKED"]:
                blocker = "METADATA_NOT_SEARCHABLE"
            elif table == "main_stock_holder_sina" and field.name in {"持股数量", "持股比例"}:
                blocker = "HOLDER_LEVEL_VALUE_REQUIRES_PREDECLARED_STOCK_EPISODE_AGGREGATION"
            elif field.name not in SOURCE_METADATA:
                blocker = "NO_FORMAL_SEARCH_IN_THIS_TASK; SOURCE_UNIT_GLOSSARY_NOT_ASSERTED"
            row = {
                "source_table": table,
                "source_family": config["family"],
                "source_field": field.name,
                "source_dtype": str(field.type),
                "source_dtype_variants": json.dumps(source_dtypes, ensure_ascii=False, sort_keys=True),
                "entity_scope": config["entity_scope"],
                "semantic_roles": "|".join(roles),
                "semantic_description": semantic,
                "semantic_evidence": "SOURCE_SCHEMA_AND_NATIVE_IDENTIFIER",
                "typed_route": typed_route,
                "allowed_change_transforms": "|".join(allowed_changes),
                "observable_time_policy": (
                    "NEXT_SESSION_AFTER_MAX_NOTICE_UPDATE"
                    if table in FINANCIAL_TABLES
                    else "NEXT_SESSION_AFTER_ANNOUNCEMENT"
                    if table == "main_stock_holder_sina"
                    else "PIT_CONTRACT_UNRESOLVED"
                ),
                "pit_status": pit_status,
                "future_route_eligible": bool(future_usable and pit_status != "PIT_CONTRACT_UNRESOLVED"),
                "current_121_presence": "EXACT_NAME" if exact_presence else ("SEMANTIC_EQUIVALENT_OTHER_SOURCE" if current_equivalent else "ABSENT"),
                "current_121_field": field.name if exact_presence else current_equivalent,
                "current_generator_exposure": generator_exposure,
                "recommended_route": "BLOCKED" if roles == ["METADATA_BLOCKED"] else typed_route,
                "blocker": blocker,
                "development_safe_non_null": int(counts.get(field.name, 0)) if args.scan_coverage else None,
                "development_safe_missing": int(counts.get(f"__NULL__{field.name}", 0)) if args.scan_coverage else None,
            }
            universe.append(row)
            semantic_fields.append(
                {
                    "field_id": f"{table}.{field.name}",
                    **row,
                    "typed_routes": roles,
                    "allowed_change_transforms": allowed_changes,
                    "level_route": "pit_fundamental.level" if "FUNDAMENTAL_LEVEL" in roles else None,
                    "change_route": "pit_fundamental.change" if "FUNDAMENTAL_CHANGE" in roles else None,
                    "event_route": "pit_fundamental.disclosure_event" if "DISCLOSURE_EVENT" in roles else None,
                    "condition_route": "pit_fundamental.condition" if "FUNDAMENTAL_CONDITION" in roles else None,
                    "missing_policy": "UNKNOWN_PROPAGATES_NO_ZERO_FILL",
                    "unit_contract": "SOURCE_NATIVE_UNSPECIFIED_NO_CONVERSION",
                }
            )
            gap_rows.append(
                {
                    "source_field": field.name,
                    "source_table": table,
                    "semantic_role": "|".join(roles),
                    "observable_time": row["observable_time_policy"],
                    "current_121_presence": row["current_121_presence"],
                    "current_121_field": row["current_121_field"],
                    "current_generator_exposure": False,
                    "recommended_route": row["recommended_route"],
                    "blocker": blocker,
                }
            )
            coverage_fields.append(
                {
                    "source_table": table,
                    "source_field": field.name,
                    "development_safe_non_null": row["development_safe_non_null"],
                    "development_safe_missing": row["development_safe_missing"],
                    "scan_status": coverage["value_scan_status"],
                }
            )

    observable_contract = _observable_contract()
    semantic_registry = {
        "registry_version": "cn_pit_fundamental_semantic_registry_v1",
        "fabric_version": FABRIC_VERSION,
        "field_count": len(semantic_fields),
        "controlled_roles": [
            "FUNDAMENTAL_LEVEL", "FUNDAMENTAL_CHANGE", "DISCLOSURE_EVENT",
            "FUNDAMENTAL_CONDITION", "METADATA_BLOCKED",
        ],
        "search_status": "NO_FORMAL_PERFORMANCE_SEARCH_GENERATOR_EXPOSURE_FALSE",
        "fields": semantic_fields,
    }
    semantic_registry["registry_hash"] = stable_hash(semantic_registry)
    observable_contract["contract_hash"] = stable_hash(observable_contract)
    sidecar_manifest = {
        "manifest_version": "cn_pit_fundamental_sidecar_manifest_v1",
        "fabric_version": FABRIC_VERSION,
        "source_root": str(source_root),
        "physical_design": "IMMUTABLE_SYMBOL_PARTITIONED_WIDE_PARQUET_PLUS_LAZY_COLUMN_LOAD",
        "minute_panel_expansion": "FORBIDDEN_NOT_MATERIALIZED",
        "development_maximum_observable_time": maximum.isoformat(),
        "split_manifest": str(args.split_manifest),
        "split_manifest_sha256": _sha256(args.split_manifest),
        "semantic_registry_hash": semantic_registry["registry_hash"],
        "observable_contract_hash": observable_contract["contract_hash"],
        "tables": source_table_manifests,
    }
    sidecar_manifest["manifest_hash"] = stable_hash(sidecar_manifest)

    _write_json(runtime_root / "fundamental_source_universe.json", {"fabric_version": FABRIC_VERSION, "field_count": len(universe), "fields": universe})
    _write_csv(runtime_root / "fundamental_source_universe.csv", universe)
    _write_json(runtime_root / "fundamental_semantic_registry.json", semantic_registry)
    _write_json(runtime_root / "fundamental_observable_time_contract.json", observable_contract)
    _write_json(runtime_root / "pit_sidecar_manifest.json", sidecar_manifest)
    _write_csv(runtime_root / "source_file_index.csv", file_index)
    _write_json(runtime_root / "fundamental_121_gap_report.json", {"fabric_version": FABRIC_VERSION, "rows": gap_rows})
    _write_csv(runtime_root / "fundamental_121_gap_report.csv", gap_rows)
    (report_root / "fundamental_revision_policy.md").write_text(_revision_policy_markdown(), encoding="utf-8")
    _write_json(
        report_root / "coverage_missingness_report.json",
        {
            "fabric_version": FABRIC_VERSION,
            "scope": "DEVELOPMENT_TRAIN_ONLY",
            "maximum_observable_time": maximum.isoformat(),
            "tables": coverage_tables,
            "fields": coverage_fields,
        },
    )
    _write_csv(report_root / "coverage_missingness_report.csv", coverage_fields)

    access_entries = [
        {
            "access_id": "fundamental_schema_inventory",
            "data_role": "SOURCE_SCHEMA_ONLY",
            "operation": "PARQUET_FOOTER_SCHEMA_ROWCOUNT",
            "allowed": True,
            "value_rows_read": 0,
        },
        {
            "access_id": "fundamental_development_coverage",
            "data_role": "DEVELOPMENT_TRAIN_ONLY",
            "operation": "PREDICATE_FILTERED_VALUE_COVERAGE_AND_CONTENT_HASH",
            "allowed": bool(args.scan_coverage),
            "maximum_observable_time": maximum.isoformat(),
            "forbidden_evaluation_dataset_rows_read": 0,
            "validation_rows_emitted_or_used": 0,
            "holdout_rows_emitted_or_used": 0,
            "forward_2026_rows_emitted_or_used": 0,
            "zygc_value_rows_read": 0,
            "predicate_applied_before_pandas_materialization": True,
            "physical_source_partition_note": "symbol parquet row groups can span dates; no row after the development observable cutoff is emitted, hashed, cached, or used",
        },
        {
            "access_id": "formal_search",
            "data_role": "NONE",
            "operation": "NOT_EXECUTED",
            "reward_rows_read": 0,
            "candidate_promotions": 0,
        },
    ]
    ledger = report_root / "development_only_access_ledger.jsonl"
    ledger.write_text("".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in access_entries), encoding="utf-8")

    elapsed = time.perf_counter() - started
    resource = {
        "execution_host": args.execution_host,
        "scan_coverage": bool(args.scan_coverage),
        "elapsed_seconds": round(elapsed, 6),
        "source_bytes": int(sum(row["source_bytes"] for row in coverage_tables)),
        "arrow_value_bytes_read_development_filtered": int(sum(row["values_read_bytes_arrow"] for row in coverage_tables)),
        "field_count": len(universe),
        "file_count": len(file_index),
    }
    _write_json(report_root / "resource_performance_report.json", resource)
    run_manifest = {
        "run_id": args.run_id,
        "fabric_version": FABRIC_VERSION,
        "status": "CN_PIT_FUNDAMENTAL_FABRIC_PARTIALLY_COMPLETED",
        "repo_sha": args.repo_sha or _repo_sha(repo),
        "execution_host": args.execution_host,
        "started_at_utc": started_at.isoformat(),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "objective": "Build PIT fundamental data fabric without performance research",
        "inputs": {
            "source_root": str(source_root),
            "split_manifest": str(args.split_manifest),
            "split_manifest_sha256": _sha256(args.split_manifest),
            "current_121_registry": str(args.current_registry),
            "current_121_registry_sha256": _sha256(args.current_registry),
            "frozen_broad_event_pack_sha256": args.frozen_broad_event_pack_sha256,
        },
        "parameters": {
            "maximum_observable_time": maximum.isoformat(),
            "split_counts": split_counts,
            "scan_coverage": bool(args.scan_coverage),
            "date_only_maturity": "NEXT_SESSION_OPEN",
        },
        "boundaries": {
            "performance_search": "NOT_EXECUTED",
            "validation_evaluation_dataset": "NOT_READ",
            "holdout_evaluation_dataset": "NOT_READ",
            "forward_2026_performance_dataset": "SEALED_NOT_READ",
            "fundamental_rows_after_development_cutoff": "NOT_EMITTED_NOT_HASHED_NOT_USED",
            "candidate_promotion": "NONE",
            "broad_event_pack": "FROZEN_UNMODIFIED",
        },
        "outputs": {
            "runtime_root": str(runtime_root),
            "report_root": str(report_root),
            "semantic_registry_hash": semantic_registry["registry_hash"],
            "sidecar_manifest_hash": sidecar_manifest["manifest_hash"],
        },
        "continuation": {
            "resume_key": stable_hash({"run_id": args.run_id, "source": sidecar_manifest["manifest_hash"]}),
            "failed_families_are_isolated": ["zygc_em"],
            "limitations": [
                "zygc_em has no credible disclosure clock and remains PIT_CONTRACT_UNRESOLVED",
                "financial statement source is a current snapshot; superseded historical values are absent",
                "new fields remain outside formal generator exposure",
            ],
        },
    }
    _write_json(report_root / "run_manifest.json", run_manifest)
    artifact_paths = sorted([*runtime_root.glob("*"), *report_root.glob("*")])
    artifact_rows = []
    for path in artifact_paths:
        if path.is_file() and path.name != "artifact_index.json":
            artifact_rows.append(
                {
                    "path": str(path),
                    "bytes": path.stat().st_size,
                    "sha256": _sha256(path),
                }
            )
    _write_json(
        report_root / "artifact_index.json",
        {
            "run_id": args.run_id,
            "fabric_version": FABRIC_VERSION,
            "artifact_count": len(artifact_rows),
            "artifacts": artifact_rows,
        },
    )
    return {
        "status": "CN_PIT_FUNDAMENTAL_FABRIC_PARTIALLY_COMPLETED",
        "field_count": len(universe),
        "file_count": len(file_index),
        "maximum_observable_time": maximum.isoformat(),
        "pit_unresolved_families": ["zygc_em"],
        "runtime_root": str(runtime_root),
        "report_root": str(report_root),
        "resource": resource,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--report-root", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--current-registry", type=Path, required=True)
    parser.add_argument("--run-id", default="cn_pit_fundamental_fabric_v1")
    parser.add_argument("--repo-sha", default="")
    parser.add_argument("--execution-host", default="LOCAL_LIGHTWEIGHT")
    parser.add_argument("--frozen-broad-event-pack-sha256", required=True)
    parser.add_argument("--scan-coverage", action="store_true")
    args = parser.parse_args()
    print(json.dumps(build(args), ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
