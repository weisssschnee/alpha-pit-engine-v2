"""Resolve compiled Candidate Program leaves into physical sidecar adapters.

The Candidate Program compiler owns semantic dependencies.  A legacy route or
candidate table is therefore not authoritative for a joint program's physical
field coverage.  This module converts the complete compiled-program dependency
union into an auditable, reward-free materialization plan.
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Iterable, Mapping, Sequence

from our_system_phase2.services.unified_capability_registry import (
    CapabilityField,
    UnifiedCapabilityRegistry,
    stable_hash,
)


PLAN_SCHEMA_VERSION = "cn_candidate_program_materialization_plan_v1"
PLAN_STATUS = "PROGRAM_MATERIALIZATION_PLAN_COMPLETE"
INFORMATION_COVERAGE_SCHEMA_VERSION = "cn_program_information_coverage_v1"
INFORMATION_COVERAGE_STATUS = "PROGRAM_INFORMATION_COVERAGE_COMPLETE"

ADAPTER_ALREADY_MATERIALIZED = "PIT_MATERIALIZED_FIELD_SIDECAR"
ADAPTER_STOCK_PRELAGGED_SESSION = "STOCK_SESSION_PRELAGGED_CONTEXT"
ADAPTER_MARKET_PRELAGGED_BROADCAST = "MARKET_SESSION_PRELAGGED_BROADCAST"
ADAPTER_STOCK_SESSION_CLOSE = "STOCK_SESSION_CLOSE_FROM_RAW_BAR"
ADAPTER_MARKET_SESSION_CLOSE_BROADCAST = "MARKET_SESSION_CLOSE_BROADCAST_FROM_RAW_BAR"


def _strings(values: Iterable[Any]) -> tuple[str, ...]:
    return tuple(sorted({str(value) for value in values if str(value)}))


def resolve_program_information_coverage_v1(
    required_field_ids: Iterable[str],
    *,
    information_metrics: Sequence[Mapping[str, Any]],
    authority_path: str,
    authority_file_sha256: str,
) -> dict[str, Any]:
    """Bind required physical leaves to development-only information evidence.

    Registry eligibility answers whether a field is legal.  This gate answers
    the separate physical question: whether the already-qualified development
    source contains at least one finite observation for every required leaf.
    It is intentionally reward-free and must run before sidecar writes or any
    financial evaluation.
    """

    required = _strings(required_field_ids)
    if len(authority_file_sha256) != 64:
        raise ValueError("program information coverage authority hash is invalid")
    metrics_by_field: dict[str, Mapping[str, Any]] = {}
    duplicates: set[str] = set()
    for raw in information_metrics:
        field_id = str(raw.get("field_id") or "")
        if not field_id:
            raise ValueError("information metric lacks field identity")
        if field_id in metrics_by_field:
            duplicates.add(field_id)
        metrics_by_field[field_id] = raw
    if duplicates:
        raise ValueError(f"duplicate information metrics: {sorted(duplicates)}")

    rows: list[dict[str, Any]] = []
    missing: list[str] = []
    unqualified: list[str] = []
    for field_id in required:
        metric = metrics_by_field.get(field_id)
        if metric is None:
            missing.append(field_id)
            rows.append(
                {
                    "field_id": field_id,
                    "metric_present": False,
                    "information_qualified": False,
                    "coverage": None,
                    "finite_count": None,
                    "gate_passed": False,
                    "failure_reasons": ["MISSING_INFORMATION_METRIC"],
                }
            )
            continue
        reasons: list[str] = []
        coverage = float(metric.get("coverage") or 0.0)
        finite_count = int(metric.get("finite_count") or 0)
        information_qualified = bool(metric.get("information_qualified"))
        if coverage <= 0.0:
            reasons.append("NONPOSITIVE_COVERAGE")
        if finite_count <= 0:
            reasons.append("NONPOSITIVE_FINITE_COUNT")
        if not information_qualified:
            reasons.append("INFORMATION_NOT_QUALIFIED")
        if reasons:
            unqualified.append(field_id)
        rows.append(
            {
                "field_id": field_id,
                "metric_present": True,
                "information_qualified": information_qualified,
                "information_status": str(metric.get("information_status") or ""),
                "coverage": coverage,
                "finite_count": finite_count,
                "row_count": int(metric.get("row_count") or 0),
                "gate_passed": not reasons,
                "failure_reasons": reasons,
            }
        )

    payload: dict[str, Any] = {
        "schema_version": INFORMATION_COVERAGE_SCHEMA_VERSION,
        "status": INFORMATION_COVERAGE_STATUS,
        "information_metrics_authority": {
            "path": authority_path,
            "file_sha256": authority_file_sha256,
        },
        "required_field_count": len(required),
        "required_field_ids": list(required),
        "field_metrics": rows,
        "missing_information_metric_field_ids": missing,
        "unqualified_information_field_ids": unqualified,
        "all_required_fields_information_qualified": not missing and not unqualified,
        "financial_evaluation_executed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "historical_2023_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
    }
    payload["information_coverage_sha256"] = stable_hash(payload)
    return payload


def verify_program_information_coverage_v1(report: Mapping[str, Any]) -> None:
    body = dict(report)
    claimed = str(body.pop("information_coverage_sha256", ""))
    if len(claimed) != 64 or stable_hash(body) != claimed:
        raise ValueError("program information coverage self-hash drift")
    if (
        body.get("schema_version") != INFORMATION_COVERAGE_SCHEMA_VERSION
        or body.get("status") != INFORMATION_COVERAGE_STATUS
    ):
        raise ValueError("program information coverage authority drift")
    if not bool(body.get("all_required_fields_information_qualified")):
        raise ValueError("program information coverage did not pass")
    required = list(body.get("required_field_ids") or [])
    metrics = list(body.get("field_metrics") or [])
    if int(body.get("required_field_count") or 0) != len(required):
        raise ValueError("program information coverage field-count drift")
    if required != sorted(set(required)) or [row.get("field_id") for row in metrics] != required:
        raise ValueError("program information coverage field identity drift")
    if any(not bool(row.get("gate_passed")) for row in metrics):
        raise ValueError("program information coverage contains a failed field")
    if body.get("missing_information_metric_field_ids") or body.get(
        "unqualified_information_field_ids"
    ):
        raise ValueError("program information coverage contains unresolved fields")
    if any(
        int(body.get(key) or 0)
        for key in (
            "validation_reads",
            "holdout_reads",
            "historical_2023_reads",
            "forward_b_reads",
            "forward_2026_reads",
        )
    ):
        raise PermissionError("program information coverage records prohibited reads")
    if bool(body.get("financial_evaluation_executed")):
        raise PermissionError("program information coverage performed financial evaluation")


def _compiled_dependencies(
    compiled: Mapping[str, Any],
    *,
    label: str,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    leaves = _strings(compiled.get("physical_leaf_ids") or ())
    component_requirements: set[str] = set()
    clocks = compiled.get("component_clock_requirements")
    if not isinstance(clocks, Mapping):
        raise ValueError(f"{label} lacks component clock requirements")
    for node_id, raw in clocks.items():
        if not isinstance(raw, Mapping):
            raise ValueError(f"{label} component clock is invalid: {node_id}")
        fields = raw.get("field_requirements")
        if not isinstance(fields, Mapping):
            raise ValueError(f"{label} component field requirements are invalid: {node_id}")
        component_requirements.update(str(field_id) for field_id in fields)
    if set(leaves) != component_requirements:
        raise ValueError(
            f"{label} physical leaves and component clocks disagree: "
            f"missing_from_clocks={sorted(set(leaves) - component_requirements)}, "
            f"unexpected_in_clocks={sorted(component_requirements - set(leaves))}"
        )
    external = _strings(compiled.get("external_adapter_requirements") or ())
    return leaves, external


def materialization_adapter_v1(capability: CapabilityField) -> str:
    """Return the exact supported adapter for a missing registered field.

    Unsupported combinations fail closed.  Adapter selection is based on the
    registry's scope and clock semantics, never on a field-name allowlist.
    """

    scope = capability.entity_scope.upper()
    family = capability.source_family
    semantics = capability.temporal_semantics
    clock = capability.observable_clock
    lag = int(capability.source_lag)
    lag_unit = capability.source_lag_unit

    if (
        scope == "STOCK"
        and family == "lagged_daily_context"
        and semantics == "PREVIOUS_SESSION_STOCK_CONTEXT"
        and clock == "previous_session"
        and lag == 1
        and lag_unit == "sessions"
    ):
        return ADAPTER_STOCK_PRELAGGED_SESSION
    if (
        scope == "MARKET"
        and family == "lagged_daily_context"
        and semantics == "PREVIOUS_SESSION_MARKET_STATE"
        and clock == "previous_session"
        and lag == 1
        and lag_unit == "sessions"
    ):
        return ADAPTER_MARKET_PRELAGGED_BROADCAST
    if (
        scope == "STOCK"
        and family == "raw_1min"
        and semantics == "BAR_VALUE"
        and clock == "bar_close"
        and lag == 0
        and lag_unit == "bars"
    ):
        return ADAPTER_STOCK_SESSION_CLOSE
    if (
        scope == "MARKET"
        and family == "raw_1min"
        and semantics == "BAR_VALUE"
        and clock == "bar_close"
        and lag == 0
        and lag_unit == "bars"
    ):
        return ADAPTER_MARKET_SESSION_CLOSE_BROADCAST
    raise ValueError(
        "no registered program materialization adapter for "
        f"{capability.field_id}: scope={scope}, family={family}, "
        f"semantics={semantics}, clock={clock}, lag={lag} {lag_unit}"
    )


def resolve_program_materialization_plan_v1(
    schedule_records: Sequence[Mapping[str, Any]],
    *,
    registry: UnifiedCapabilityRegistry,
    available_fields: Iterable[str],
) -> dict[str, Any]:
    """Resolve the complete primary/control program dependency union.

    The caller must recompile and compare frozen program records before calling
    this function.  This function then verifies that compiler leaves and clock
    requirements agree, classifies every missing physical leaf, and emits a
    deterministic self-hashed plan without touching price, labels or rewards.
    """

    if not schedule_records:
        raise ValueError("program materialization requires a non-empty schedule")
    available = set(_strings(available_fields))
    required: set[str] = set()
    external: set[str] = set()
    program_rows: list[dict[str, Any]] = []
    template_counts: Counter[str] = Counter()

    for index, record in enumerate(schedule_records, start=1):
        template_id = str(record.get("template_id") or "")
        pair_id = str(record.get("pair_id") or "")
        if not template_id or not pair_id:
            raise ValueError(f"schedule record {index} lacks template or pair identity")
        template_counts[template_id] += 1
        member_fields: dict[str, list[str]] = {}
        member_external: dict[str, list[str]] = {}
        for member, key in (("PRIMARY", "primary_compiled"), ("CONTROL", "control_compiled")):
            compiled = record.get(key)
            if not isinstance(compiled, Mapping):
                raise ValueError(f"schedule record {index} lacks {key}")
            leaves, adapters = _compiled_dependencies(
                compiled,
                label=f"schedule record {index} {member}",
            )
            required.update(leaves)
            external.update(adapters)
            member_fields[member] = list(leaves)
            member_external[member] = list(adapters)
        program_rows.append(
            {
                "main_record_ordinal": int(
                    record["main_record_ordinal"]
                    if "main_record_ordinal" in record
                    else index - 1
                ),
                "pair_id": pair_id,
                "template_id": template_id,
                "member_physical_leaf_ids": member_fields,
                "member_external_adapter_requirements": member_external,
            }
        )

    bindings: list[dict[str, Any]] = []
    unsupported: list[dict[str, Any]] = []
    for field_id in sorted(required):
        capability = registry.resolve(field_id)
        if field_id in available:
            adapter = ADAPTER_ALREADY_MATERIALIZED
        else:
            try:
                adapter = materialization_adapter_v1(capability)
            except ValueError as exc:
                unsupported.append({"field_id": field_id, "reason": str(exc)})
                continue
        bindings.append(
            {
                "field_id": field_id,
                "adapter_id": adapter,
                "already_materialized": field_id in available,
                "entity_scope": capability.entity_scope,
                "source_family": capability.source_family,
                "source_table": capability.source_table,
                "source_field": capability.source_field,
                "temporal_semantics": capability.temporal_semantics,
                "observable_clock": capability.observable_clock,
                "maturity_rule": capability.maturity_rule,
                "source_lag": capability.source_lag,
                "source_lag_unit": capability.source_lag_unit,
                "revision_policy": capability.revision_policy,
                "pit_status": capability.pit_status,
            }
        )
    if unsupported:
        raise ValueError(f"unmaterializable compiled-program leaves: {unsupported}")

    materializable = {row["field_id"] for row in bindings}
    if materializable != required:
        raise RuntimeError("compiled-program required/materializable field coverage drift")
    missing = required - available
    planned = {
        row["field_id"] for row in bindings if row["adapter_id"] != ADAPTER_ALREADY_MATERIALIZED
    }
    if planned != missing:
        raise RuntimeError("compiled-program missing/materialization-plan field coverage drift")

    payload: dict[str, Any] = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "status": PLAN_STATUS,
        "registry_hash": registry.registry_hash,
        "schedule_record_count": len(schedule_records),
        "template_counts": dict(sorted(template_counts.items())),
        "required_physical_leaf_ids": sorted(required),
        "required_physical_leaf_count": len(required),
        "available_required_field_ids": sorted(required & available),
        "missing_required_field_ids": sorted(missing),
        "materializable_required_field_ids": sorted(materializable),
        "external_adapter_requirements": sorted(external),
        "field_bindings": bindings,
        "program_records": program_rows,
        "coverage_checks": {
            "compiled_leaves_equal_component_clock_fields": True,
            "required_equals_materializable": True,
            "missing_equals_planned_materialization": True,
            "unsupported_required_fields": [],
            "wrong_scope_fields": [],
            "wrong_clock_fields": [],
        },
        "financial_evaluation_executed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "historical_2023_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
    }
    payload["plan_sha256"] = stable_hash(payload)
    return payload


def verify_program_materialization_plan_v1(plan: Mapping[str, Any]) -> None:
    body = dict(plan)
    claimed = str(body.pop("plan_sha256", ""))
    if len(claimed) != 64 or stable_hash(body) != claimed:
        raise ValueError("program materialization plan self-hash drift")
    if body.get("schema_version") != PLAN_SCHEMA_VERSION or body.get("status") != PLAN_STATUS:
        raise ValueError("program materialization plan authority drift")
    checks = body.get("coverage_checks")
    if not isinstance(checks, Mapping) or not all(
        bool(checks.get(key))
        for key in (
            "compiled_leaves_equal_component_clock_fields",
            "required_equals_materializable",
            "missing_equals_planned_materialization",
        )
    ):
        raise ValueError("program materialization plan coverage did not pass")
    if any(
        checks.get(key)
        for key in ("unsupported_required_fields", "wrong_scope_fields", "wrong_clock_fields")
    ):
        raise ValueError("program materialization plan contains unresolved fields")
    if any(
        int(body.get(key) or 0)
        for key in (
            "validation_reads",
            "holdout_reads",
            "historical_2023_reads",
            "forward_b_reads",
            "forward_2026_reads",
        )
    ):
        raise PermissionError("program materialization plan records prohibited reads")
    if bool(body.get("financial_evaluation_executed")):
        raise PermissionError("program materialization plan performed financial evaluation")
