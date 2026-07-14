#!/usr/bin/env python3
"""Build conservative, non-performance CN fundamental qualifications.

The builder reads only the versioned Fundamental Fabric source inventory and
static qualification contracts.  It reuses the repository's authoritative
source and representation identity functions and its exact source-field unit
assertions.  Unknown fields remain visible in the matrix but fail closed.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from our_system_phase2.services.fundamental_representations import (  # noqa: E402
    FUNDAMENTAL_SOURCE_RELEASE,
    PROVIDER_BY_TABLE,
    canonical_representation_specs,
    qualify_source_field,
)
from our_system_phase2.services.unified_capability_registry import (  # noqa: E402
    SOURCE_ID_VERSION,
    source_field_id,
)


DEFAULT_SOURCE = REPO_ROOT / "runtime/cn_pit_fundamental_fabric_v1/fundamental_source_universe.csv"
DEFAULT_POLICY = REPO_ROOT / "runtime/run_plans/cn_fundamental_field_qualification_policy_v1.json"
DEFAULT_MERGE = REPO_ROOT / "runtime/run_plans/cn_unified_registry_merge_contract_v1.json"
DEFAULT_RUNTIME = REPO_ROOT / "runtime/cn_unified_capability_v1"
DEFAULT_REPORT = REPO_ROOT / "reports/cn_unified_capability_v1"

FORBIDDEN_SOURCE_COLUMNS = {
    "return", "returns", "label", "reward", "ic", "selector_rank",
    "survivor", "validation", "holdout", "forward_2026",
}
FINANCIAL_INSTITUTION_FRAGMENTS = (
    "DEPOSIT", "INTERBANK", "LOAN", "PREMIUM", "INSURANCE",
    "POLICYHOLDER", "REINSURANCE", "UNDERWRITE", "BROKER",
    "CAPITAL_ADEQUACY", "INTEREST_INCOME", "INTEREST_EXPENSE",
)
FAMILY_MAP = {
    "size": "VALUATION_SIZE",
    "profitability": "PROFITABILITY",
    "growth": "GROWTH",
    "leverage": "LEVERAGE",
    "liquidity_solvency": "LIQUIDITY_SOLVENCY",
    "working_capital": "WORKING_CAPITAL",
    "cashflow_quality": "CASH_FLOW_QUALITY",
    "accruals": "ACCRUALS",
    "capital_investment": "CAPEX_INVESTMENT",
    "operating_efficiency": "OPERATING_EFFICIENCY",
    "shareholder_structure": "SHAREHOLDER_STRUCTURE",
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def _coverage(row: Mapping[str, Any]) -> float:
    non_null = int(float(row.get("development_safe_non_null") or 0))
    missing = int(float(row.get("development_safe_missing") or 0))
    total = non_null + missing
    return non_null / total if total else 0.0


def _metadata_blocked(row: Mapping[str, Any]) -> bool:
    return (
        str(row.get("semantic_roles") or "") == "METADATA_BLOCKED"
        or "METADATA_NOT_SEARCHABLE" in str(row.get("blocker") or "")
    )


def _industry_specific(field: str) -> bool:
    upper = str(field).upper()
    return any(fragment in upper for fragment in FINANCIAL_INSTITUTION_FRAGMENTS)


def _heuristic_family(row: Mapping[str, Any], asserted_family: str) -> str:
    if asserted_family in FAMILY_MAP:
        return FAMILY_MAP[asserted_family]
    table = str(row["source_table"])
    field = str(row["source_field"]).upper()
    if table == "zygc_em":
        return "BUSINESS_COMPOSITION"
    if table == "main_stock_holder_sina":
        return "SHAREHOLDER_STRUCTURE"
    if any(token in field for token in ("NOTICE_DATE", "UPDATE_DATE", "REPORT_DATE")):
        return "DISCLOSURE_FRESHNESS"
    if _industry_specific(field):
        return "FINANCIAL_INSTITUTION_SPECIFIC"
    if field.endswith("_YOY") or "GROWTH" in field:
        return "GROWTH"
    if any(token in field for token in ("CASH", "NETCASH")):
        return "CASH_FLOW_QUALITY"
    if any(token in field for token in ("PROFIT", "INCOME", "REVENUE", "EPS", "EBIT")):
        return "PROFITABILITY"
    if any(token in field for token in ("LIAB", "DEBT", "BORROW", "LEVERAGE")):
        return "LEVERAGE"
    if any(token in field for token in ("RECE", "PAYABLE", "INVENTORY", "ADVANCE")):
        return "WORKING_CAPITAL"
    if any(token in field for token in ("CAPEX", "FIXED_ASSET", "CONSTRUCT", "INVEST")):
        return "CAPEX_INVESTMENT"
    if "ASSET" in field or "EQUITY" in field:
        return "VALUATION_SIZE"
    return "OTHER_UNRESOLVED"


def _qualification(
    row: Mapping[str, Any],
    asserted: Mapping[str, Any],
) -> tuple[str, str, str]:
    pit = str(row.get("pit_status") or "")
    if pit == "PIT_CONTRACT_UNRESOLVED":
        return "PIT_UNRESOLVED", "NONE", "credible observable clock unavailable"
    if _metadata_blocked(row):
        return "BLOCKED_METADATA", "NONE", "metadata, key, or source lineage field"
    unit_asserted = str(asserted.get("unit_certainty") or "").startswith("ASSERTED")
    if unit_asserted and bool(asserted.get("search_eligible")):
        if str(row["source_table"]) == "main_stock_holder_sina":
            return "QUALIFIED_ROOT", "ALL_LISTED_EQUITIES", "asserted holder aggregation contract"
        return "DERIVED_ONLY", "|".join(asserted.get("applicable_company_types", "").split("|")), (
            "asserted statement field; raw currency level remains blocked and only canonical "
            "change, normalized ratio, or source-reported YoY representations are permitted"
        )
    if _industry_specific(str(row["source_field"])):
        return "INDUSTRY_SPECIFIC", "FINANCIAL_INSTITUTION_REVIEW_REQUIRED", (
            "native identifier suggests sector scope but unit/applicability contract is unresolved"
        )
    if _coverage(row) < 0.10:
        return "LOW_COVERAGE_ARCHIVE", "NONE", "coverage below 0.10 without asserted applicability"
    return "SEMANTICS_UNRESOLVED", "NONE", (
        "unit, period, sign, or field semantics are not asserted from exact local evidence"
    )


def _load_source(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = list(reader.fieldnames or [])
        rows = list(reader)
    forbidden = sorted(FORBIDDEN_SOURCE_COLUMNS.intersection(name.lower() for name in columns))
    if forbidden:
        raise PermissionError(f"qualification source contains forbidden performance columns: {forbidden}")
    required = {
        "source_table", "source_family", "source_field", "semantic_roles",
        "observable_time_policy", "pit_status", "blocker",
        "development_safe_non_null", "development_safe_missing",
    }
    missing = sorted(required - set(columns))
    if missing:
        raise ValueError(f"fundamental source universe is missing columns: {missing}")
    if len(rows) != 1227:
        raise ValueError(f"unexpected source universe count: {len(rows)}")
    return columns, rows


def build(
    *,
    source: Path,
    policy_path: Path,
    merge_contract_path: Path,
    runtime_root: Path,
    report_root: Path,
) -> dict[str, Any]:
    policy = _read_json(policy_path)
    merge_contract = _read_json(merge_contract_path)
    source_columns, source_rows = _load_source(source)
    states = list(policy["qualification_states"])
    families = list(policy["canonical_families"])
    state_counts: Counter[str] = Counter({state: 0 for state in states})
    family_counts: Counter[str] = Counter({family: 0 for family in families})
    source_ids: dict[tuple[str, str], str] = {}
    matrix: list[dict[str, Any]] = []

    for row in sorted(source_rows, key=lambda item: (item["source_table"], item["source_field"])):
        table = str(row["source_table"])
        field = str(row["source_field"])
        if table not in PROVIDER_BY_TABLE:
            raise ValueError(f"unregistered fundamental provider table: {table}")
        sfid = source_field_id(
            provider=PROVIDER_BY_TABLE[table],
            source_release_version=FUNDAMENTAL_SOURCE_RELEASE,
            source_table=table,
            source_field=field,
        )
        source_ids[(table, field)] = sfid
        asserted = qualify_source_field({**row, "source_field_id": sfid})
        state, applicability, reason = _qualification(row, asserted)
        family = _heuristic_family(row, str(asserted.get("semantic_family") or ""))
        blocked_reason = "" if state in {"QUALIFIED_ROOT", "DERIVED_ONLY"} else reason
        out = {
            **row,
            "source_field_id": sfid,
            "source_identity_version": SOURCE_ID_VERSION,
            "provider": PROVIDER_BY_TABLE[table],
            "source_release_version": FUNDAMENTAL_SOURCE_RELEASE,
            "value_kind": asserted.get("value_kind", "unknown"),
            "flow_or_stock": asserted.get("flow_or_stock", "unknown"),
            "period_basis": asserted.get("period_basis", "unknown"),
            "consolidation_scope": asserted.get("consolidation_scope", "unknown"),
            "sign_semantics": asserted.get("sign_semantics", "unknown"),
            "unit_certainty": asserted.get("unit_certainty", "SOURCE_UNIT_GLOSSARY_NOT_ASSERTED"),
            "semantic_certainty": asserted.get("semantic_certainty", "SOURCE_SEMANTICS_NOT_ASSERTED"),
            "company_type_applicability": applicability,
            "coverage_ratio": f"{_coverage(row):.10f}",
            "source_reported_change": bool(asserted.get("source_reported_change", False)),
            "canonical_family": family,
            "qualification_state": state,
            "qualification_reason": reason,
            "allowed_routes": "",
            "representation_ids": "",
            "blocked_reason": blocked_reason,
            "raw_generator_exposure": False,
            "historical_revision_replay_supported": False,
        }
        matrix.append(out)
        state_counts[state] += 1
        family_counts[family] += 1

    if len(source_ids) != len(source_rows):
        raise ValueError("stable source identity collision or duplicate source key")

    specs = canonical_representation_specs(source_ids)
    matrix_by_id = {row["source_field_id"]: row for row in matrix}
    representation_ids: set[str] = set()
    field_ids: set[str] = set()
    routes_by_source: dict[str, set[str]] = defaultdict(set)
    reps_by_source: dict[str, list[str]] = defaultdict(list)
    annotated_specs: list[dict[str, Any]] = []
    for spec in specs:
        rep_id = str(spec["representation_id"])
        field_id = str(spec["field_id"])
        if rep_id in representation_ids or field_id in field_ids:
            raise ValueError(f"canonical representation identity collision: {field_id}")
        representation_ids.add(rep_id)
        field_ids.add(field_id)
        source_states = [matrix_by_id[sfid]["qualification_state"] for sfid in spec["source_field_ids"]]
        if bool(spec["search_eligible"]) and any(
            state not in {"QUALIFIED_ROOT", "DERIVED_ONLY"} for state in source_states
        ):
            raise PermissionError(f"search representation references unresolved source: {field_id}")
        annotated = {
            **spec,
            "source_qualification_states": source_states,
            "historical_revision_replay_supported": False,
            "performance_used": False,
        }
        annotated_specs.append(annotated)
        for sfid in spec["source_field_ids"]:
            reps_by_source[sfid].append(rep_id)
            if bool(spec["search_eligible"]):
                routes_by_source[sfid].add(str(spec["route_id"]))

    if len(annotated_specs) > int(policy["canonical_representation_contract"]["maximum_roots"]):
        raise ValueError("canonical representation count exceeds frozen limit")
    for row in matrix:
        sfid = str(row["source_field_id"])
        row["allowed_routes"] = "|".join(sorted(routes_by_source[sfid]))
        row["representation_ids"] = "|".join(sorted(reps_by_source[sfid]))
        if row["qualification_state"] not in {"QUALIFIED_ROOT", "DERIVED_ONLY"} and row["allowed_routes"]:
            raise PermissionError(f"fail-closed field received a route: {sfid}")

    source_reported = {
        spec["source_fields"][0]["source_field"][:-4]: spec["representation_id"]
        for spec in annotated_specs
        if spec["representation_type"] == "source_reported_yoy"
    }
    internally_derived = {
        spec["source_fields"][0]["source_field"]: spec["representation_id"]
        for spec in annotated_specs
        if spec["representation_type"] == "internally_derived_yoy"
    }
    shared_yoy_bases = sorted(set(source_reported).intersection(internally_derived))
    if not shared_yoy_bases or any(
        source_reported[name] == internally_derived[name] for name in shared_yoy_bases
    ):
        raise ValueError("source-reported and internally derived YoY identities are not separated")

    runtime_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    matrix_path = runtime_root / "fundamental_field_qualification_matrix.csv"
    extra_columns = [name for name in matrix[0] if name not in source_columns]
    with matrix_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=source_columns + extra_columns,
            extrasaction="raise",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(matrix)

    registry_path = runtime_root / "fundamental_canonical_representation_registry.json"
    registry = {
        "registry_version": "cn_fundamental_canonical_representation_registry_v1",
        "source_identity_version": SOURCE_ID_VERSION,
        "source_release_version": FUNDAMENTAL_SOURCE_RELEASE,
        "source_universe_count": len(matrix),
        "representation_count": len(annotated_specs),
        "search_eligible_representation_count": sum(bool(row["search_eligible"]) for row in annotated_specs),
        "raw_1227_generator_exposure": False,
        "current_snapshot_revision_replay_supported": False,
        "performance_used": False,
        "representations": annotated_specs,
    }
    _write_json(registry_path, registry)

    output_hashes = {
        "qualification_matrix_sha256": _sha256(matrix_path),
        "representation_registry_sha256": _sha256(registry_path),
    }
    summary = {
        "status": "FUNDAMENTAL_FIELD_QUALIFICATION_COMPLETED_NON_PERFORMANCE",
        "source_field_count": len(matrix),
        "stable_source_field_id_count": len(source_ids),
        "qualification_state_counts": dict(state_counts),
        "canonical_family_counts": dict(family_counts),
        "canonical_family_count": sum(count > 0 for count in family_counts.values()),
        "representation_count": len(annotated_specs),
        "search_eligible_representation_count": sum(bool(row["search_eligible"]) for row in annotated_specs),
        "qualified_root_count": state_counts["QUALIFIED_ROOT"],
        "derived_only_count": state_counts["DERIVED_ONLY"],
        "industry_specific_count": state_counts["INDUSTRY_SPECIFIC"],
        "low_coverage_archive_count": state_counts["LOW_COVERAGE_ARCHIVE"],
        "semantics_unresolved_count": state_counts["SEMANTICS_UNRESOLVED"],
        "pit_unresolved_count": state_counts["PIT_UNRESOLVED"],
        "semantics_or_pit_unresolved_count": (
            state_counts["SEMANTICS_UNRESOLVED"] + state_counts["PIT_UNRESOLVED"]
        ),
        "source_reported_vs_internal_yoy_identity_checks": len(shared_yoy_bases),
        "performance_used": False,
        "validation_accessed": False,
        "holdout_accessed": False,
        "forward_2026_accessed": False,
        "candidate_promotion": False,
        "inputs": {
            "source": _relative(source),
            "source_sha256": _sha256(source),
            "policy": _relative(policy_path),
            "policy_sha256": _sha256(policy_path),
            "merge_contract": _relative(merge_contract_path),
            "merge_contract_sha256": _sha256(merge_contract_path),
            "merge_contract_version": merge_contract["contract_version"],
        },
        "outputs": {
            "qualification_matrix": _relative(matrix_path),
            "representation_registry": _relative(registry_path),
            **output_hashes,
        },
        "limitations": [
            "unknown unit, period, sign, and applicability fields remain fail closed",
            "industry-specific native identifiers remain review-only until exact contracts exist",
            "current statement snapshots cannot reproduce superseded historical revisions",
            "zygc_em remains PIT unresolved",
            "no performance search or candidate promotion was executed",
        ],
    }
    summary_path = report_root / "fundamental_qualification_summary.json"
    _write_json(summary_path, summary)
    _write_json(
        report_root / "qualification_access_ledger.json",
        {
            "ledger_version": "cn_fundamental_qualification_access_ledger_v1",
            "inputs": summary["inputs"],
            "performance_inputs": [],
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "reward_reads": 0,
            "selector_reads": 0,
            "candidate_promotion": False,
        },
    )
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--merge-contract", type=Path, default=DEFAULT_MERGE)
    parser.add_argument("--runtime-root", type=Path, default=DEFAULT_RUNTIME)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()
    summary = build(
        source=args.source,
        policy_path=args.policy,
        merge_contract_path=args.merge_contract,
        runtime_root=args.runtime_root,
        report_root=args.report_root,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
