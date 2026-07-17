from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts.build_cn_field_master_registry import build


def test_master_registry_contains_fundamental_plate_and_chip_families(tmp_path: Path) -> None:
    summary = build(tmp_path)
    document = json.loads((tmp_path / "cn_field_master_registry_v1.json").read_text(encoding="utf-8"))
    with (tmp_path / "cn_field_master_registry_v1.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    names = {row["field_name"] for row in rows}
    assert summary["field_record_count"] == len(rows) == len(document["rows"])
    assert summary["record_kind_counts"]["SOURCE_FIELD"] == 1227
    assert {"chip_cost_p50", "plate_close", "plate_peer_return_mean"} <= names
    assert any(
        row["record_kind"] == "CAPABILITY_FIELD"
        and row["source_table"] in {"balance_sheet_report_em", "profit_sheet_report_em", "cash_flow_sheet_report_em"}
        for row in rows
    )
    assert document["authority_status"] == "AUTHORITATIVE_FIELD_UNIVERSE"
    assert document["performance_or_reward_used"] is False
    assert document["forward_2026_accessed"] is False
