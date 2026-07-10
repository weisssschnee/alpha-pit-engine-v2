"""Export the accepted CNline2 parquet schema as a searchable field registry."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from our_system_phase2.services.typed_primitive_gate import field_category


METADATA_FIELDS = {
    "code",
    "trade_time",
    "date",
    "exec_date",
    "signal_time",
    "dataset_route_id",
    "label_horizon",
}


def _read_contract(path: Path) -> dict[str, dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return {str(row["field_name"]): dict(row) for row in csv.DictReader(handle)}


def _bool_text(value: Any) -> str:
    return "true" if str(value).strip().lower() == "true" else "false"


def _field_policy(name: str, contract: dict[str, str] | None) -> dict[str, str]:
    if name in METADATA_FIELDS:
        return {
            "field_group": "metadata_or_key",
            "search_route": "excluded_from_formula_search",
            "availability_contract": "identity_time_or_provenance_only",
            "direct_formula_allowed": "false",
            "typed_search_allowed": "false",
            "primitive_policy": "not_a_feature",
            "contract_source": "base_true1min_schema",
            "verification": "parquet_schema",
        }
    if name.startswith("m1_first"):
        window = next((value for value in (5, 15, 30) if name.startswith(f"m1_first{value}_")), None)
        return {
            "field_group": "firstn_opening_state",
            "search_route": "firstn_minute_derived",
            "availability_contract": f"non_null_after_first_{window}_observed_bars" if window else "firstn_maturity_guarded",
            "direct_formula_allowed": "true",
            "typed_search_allowed": "true",
            "primitive_policy": "firstn_minute_derived",
            "contract_source": "base_true1min_schema",
            "verification": "sampled_5_15_30_bar_maturity_verified_20260711",
        }
    if name.startswith("ctx_"):
        if contract is None:
            raise RuntimeError(f"missing sidecar contract for {name}")
        return {
            "field_group": "lagged_context",
            "search_route": str(contract.get("route") or "lagged_context"),
            "availability_contract": str(contract.get("availability_contract") or ""),
            "direct_formula_allowed": _bool_text(contract.get("formula_allowed")),
            "typed_search_allowed": "true",
            "primitive_policy": str(contract.get("ordinary_primitive_allowed") or "coverage_guard_required"),
            "contract_source": "phase3cs_sidecar_field_contract",
            "verification": "previous_available_source_date_lt_exec_date",
        }
    if name.startswith("evt_"):
        is_cutoff_key = name.endswith("cutoff_minute")
        availability = (
            str(contract.get("availability_contract") or "")
            if contract is not None
            else "derived_same_day_after_evt_uplimit_cutoff_minute"
        )
        return {
            "field_group": "event_state",
            "search_route": "event_metadata_only" if is_cutoff_key else "typed_event_state",
            "availability_contract": availability,
            "direct_formula_allowed": "false",
            "typed_search_allowed": "false" if is_cutoff_key else "true",
            "primitive_policy": "not_a_feature" if is_cutoff_key else "event_primitive_required",
            "contract_source": "phase3cs_sidecar_field_contract" if contract is not None else "phase3cs_derived_event_lifecycle",
            "verification": "same_day_cutoff_masked",
        }
    return {
        "field_group": "raw_minute",
        "search_route": "continuous_true1min",
        "availability_contract": "at_or_before_current_trade_time",
        "direct_formula_allowed": "true",
        "typed_search_allowed": "true",
        "primitive_policy": "ordinary_numeric_with_typed_gate",
        "contract_source": "base_true1min_schema",
        "verification": "parquet_schema",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--panel", type=Path, required=True)
    parser.add_argument("--sidecar-contract", type=Path, required=True)
    parser.add_argument("--output-csv", type=Path, required=True)
    parser.add_argument("--output-summary", type=Path, required=True)
    args = parser.parse_args(argv)

    schema = pq.read_schema(args.panel.resolve())
    contract = _read_contract(args.sidecar_contract.resolve())
    rows: list[dict[str, Any]] = []
    for index, field in enumerate(schema):
        policy = _field_policy(field.name, contract.get(field.name))
        rows.append(
            {
                "field_ordinal": index + 1,
                "field_name": field.name,
                "parquet_dtype": str(field.type),
                "nullable": str(bool(field.nullable)).lower(),
                "typed_category": field_category(field.name),
                **policy,
            }
        )

    if len(rows) != 121:
        raise RuntimeError(f"accepted field registry must contain 121 columns, found {len(rows)}")
    group_counts = Counter(str(row["field_group"]) for row in rows)
    expected = {
        "metadata_or_key": 7,
        "raw_minute": 12,
        "firstn_opening_state": 30,
        "lagged_context": 59,
        "event_state": 13,
    }
    if dict(group_counts) != expected:
        raise RuntimeError(f"unexpected field groups: {dict(group_counts)}")

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    schema_payload = "\n".join(f"{row['field_name']}:{row['parquet_dtype']}" for row in rows)
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "decision": "CNLINE2_TRUE1MIN_121_FIELD_REGISTRY_ACCEPTED",
        "field_count": len(rows),
        "field_group_counts": expected,
        "schema_sha256": hashlib.sha256(schema_payload.encode("utf-8")).hexdigest(),
        "old_1d_allowed": False,
        "plate_membership_field_count": 0,
        "event_fields_require_typed_route": True,
        "firstn_maturity_sample_verified": True,
    }
    args.output_summary.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
