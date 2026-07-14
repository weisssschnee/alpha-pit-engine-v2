from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts.build_fundamental_source_universe_merge import SCHEMA, build, schema_hash


REPO = Path(__file__).resolve().parents[1]
REGISTRY = REPO / "runtime/cn_pit_fundamental_fabric_v1/fundamental_semantic_registry.json"
EXTERNAL_MATRIX = REPO / "reports/cn_field_universe_search_exposure_audit_20260714/CN_121_LAGGED_CONTEXT_EXPOSURE_MATRIX_20260714.csv"


def test_merge_projection_has_stable_schema_and_external_join(tmp_path: Path) -> None:
    output = tmp_path / "fundamental_source_universe.csv"
    schema_output = tmp_path / "fundamental_source_universe_schema.json"
    manifest = build(registry_path=REGISTRY, output_path=output, schema_output_path=schema_output)
    rows = list(csv.DictReader(output.open(encoding="utf-8", newline="")))

    assert list(rows[0]) == [name for name, _ in SCHEMA]
    assert len(rows) == 1227
    assert len({row["source_field_id"] for row in rows}) == 1227
    assert manifest["schema_sha256"] == schema_hash()
    assert manifest["generator_exposed_rows"] == 0
    assert manifest["pit_unresolved_rows"] == 17

    statement = next(
        row for row in rows
        if row["source_table"] == "balance_sheet_report_em" and row["source_field"] == "ACCOUNTS_PAYABLE"
    )
    assert statement["report_period_field"] == "REPORT_DATE"
    assert statement["observable_time_field"] == "NOTICE_DATE"
    assert statement["revision_time_field"] == "UPDATE_DATE"
    assert statement["recommended_search_routes"] == "SLOW_XS_LEVEL|SLOW_TEMPORAL_CHANGE|DISCLOSURE_EVENT"

    unresolved = [row for row in rows if row["source_table"] == "zygc_em"]
    assert {row["materialization_status"] for row in unresolved} == {"BLOCKED_PIT_CONTRACT_UNRESOLVED"}
    assert {row["observable_time_field"] for row in unresolved} == {""}

    external_names = {
        row["field_name"]
        for row in csv.DictReader(EXTERNAL_MATRIX.open(encoding="utf-8-sig", newline=""))
    }
    equivalent_fields = {row["current_121_field"] for row in rows if row["current_121_field"]}
    assert equivalent_fields == {"ctx_holder_holder_num", "ctx_holder_avg_hold_num"}
    assert equivalent_fields <= external_names

    persisted = json.loads(schema_output.read_text(encoding="utf-8"))
    assert persisted["schema_sha256"] == schema_hash()
