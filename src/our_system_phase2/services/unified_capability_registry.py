"""Authoritative typed capability registry for unified CN discovery.

The registry is deliberately independent from performance results.  It binds
source identity, representation identity, PIT qualification and legal search
routes so generators do not infer capability from a column name or from field
presence in a panel.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


REGISTRY_VERSION = "cn_unified_capability_registry_v1"
SOURCE_ID_VERSION = "cn_source_field_identity_v2"
REPRESENTATION_ID_VERSION = "cn_canonical_representation_v1"

ROUTE_IDS = (
    "MINUTE_STATIC",
    "FIRSTN_PATH",
    "SLOW_CROSS_SECTIONAL_LEVEL",
    "SLOW_TEMPORAL_CHANGE",
    "DISCLOSURE_EVENT",
    "MARKET_REGIME_CONDITION",
    "INTRADAY_STATE_TRANSITION",
    "BROAD_EVENT_FROZEN_ENTRY",
)


def stable_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def stable_hash(payload: Any) -> str:
    return hashlib.sha256(stable_json(payload).encode("utf-8")).hexdigest()


def source_field_id(
    *,
    provider: str,
    source_release_version: str,
    source_table: str,
    source_field: str,
) -> str:
    """Return a stable ID bound to the physical provider/release identity."""

    payload = {
        "identity_version": SOURCE_ID_VERSION,
        "provider": str(provider),
        "source_release_version": str(source_release_version),
        "source_table": str(source_table),
        "source_field": str(source_field),
    }
    return "cn.sf." + stable_hash(payload)[:32]


def representation_id(
    *,
    representation_type: str,
    source_field_ids: Sequence[str],
    parameters: Mapping[str, Any] | None = None,
) -> str:
    payload = {
        "identity_version": REPRESENTATION_ID_VERSION,
        "representation_type": str(representation_type),
        "source_field_ids": sorted(str(value) for value in source_field_ids),
        "parameters": dict(parameters or {}),
    }
    return "cn.rep." + stable_hash(payload)[:32]


@dataclass(frozen=True, slots=True)
class CapabilityField:
    field_id: str
    source_field_id: str
    representation_id: str
    source_family: str
    source_table: str
    source_field: str
    entity_scope: str
    temporal_semantics: str
    observable_clock: str
    maturity_rule: str
    pit_status: str
    allowed_routes: tuple[str, ...]
    search_eligible: bool
    semantic_role: str
    support_unit: str
    field_role: str = "primary"
    blocked_reason: str = ""
    unit_status: str = "SOURCE_UNIT_GLOSSARY_NOT_ASSERTED"
    source_lag: int = 0
    source_lag_unit: str = "bars"
    reset_semantics: str = ""
    matched_control_required: bool = False
    metadata: Mapping[str, Any] | None = None

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "CapabilityField":
        known = {
            "field_id",
            "source_field_id",
            "representation_id",
            "source_family",
            "source_table",
            "source_field",
            "entity_scope",
            "temporal_semantics",
            "observable_clock",
            "maturity_rule",
            "pit_status",
            "allowed_routes",
            "search_eligible",
            "semantic_role",
            "support_unit",
            "field_role",
            "blocked_reason",
            "unit_status",
            "source_lag",
            "source_lag_unit",
            "reset_semantics",
            "matched_control_required",
            "metadata",
        }
        extra = {key: value for key, value in payload.items() if key not in known}
        metadata = dict(payload.get("metadata") or {})
        metadata.update(extra)
        return cls(
            field_id=str(payload["field_id"]),
            source_field_id=str(payload["source_field_id"]),
            representation_id=str(payload["representation_id"]),
            source_family=str(payload["source_family"]),
            source_table=str(payload["source_table"]),
            source_field=str(payload["source_field"]),
            entity_scope=str(payload["entity_scope"]),
            temporal_semantics=str(payload["temporal_semantics"]),
            observable_clock=str(payload["observable_clock"]),
            maturity_rule=str(payload["maturity_rule"]),
            pit_status=str(payload["pit_status"]),
            allowed_routes=tuple(str(value) for value in payload.get("allowed_routes", ())),
            search_eligible=bool(payload.get("search_eligible", False)),
            semantic_role=str(payload.get("semantic_role", "")),
            support_unit=str(payload.get("support_unit", "")),
            field_role=str(payload.get("field_role", "primary")),
            blocked_reason=str(payload.get("blocked_reason", "")),
            unit_status=str(payload.get("unit_status", "SOURCE_UNIT_GLOSSARY_NOT_ASSERTED")),
            source_lag=int(payload.get("source_lag", 0)),
            source_lag_unit=str(payload.get("source_lag_unit", "bars")),
            reset_semantics=str(payload.get("reset_semantics", "")),
            matched_control_required=bool(payload.get("matched_control_required", False)),
            metadata=metadata,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "field_id": self.field_id,
            "source_field_id": self.source_field_id,
            "representation_id": self.representation_id,
            "source_family": self.source_family,
            "source_table": self.source_table,
            "source_field": self.source_field,
            "entity_scope": self.entity_scope,
            "temporal_semantics": self.temporal_semantics,
            "observable_clock": self.observable_clock,
            "maturity_rule": self.maturity_rule,
            "pit_status": self.pit_status,
            "allowed_routes": list(self.allowed_routes),
            "search_eligible": self.search_eligible,
            "semantic_role": self.semantic_role,
            "support_unit": self.support_unit,
            "field_role": self.field_role,
            "blocked_reason": self.blocked_reason,
            "unit_status": self.unit_status,
            "source_lag": self.source_lag,
            "source_lag_unit": self.source_lag_unit,
            "reset_semantics": self.reset_semantics,
            "matched_control_required": self.matched_control_required,
            "metadata": dict(self.metadata or {}),
        }


class UnifiedCapabilityRegistry:
    """Validated, queryable registry used by generators and the compiler."""

    def __init__(self, payload: Mapping[str, Any]) -> None:
        self.payload = dict(payload)
        self.registry_version = str(payload.get("registry_version", ""))
        self.route_contracts = {
            str(row["route_id"]): dict(row) for row in payload.get("routes", ())
        }
        self.fields = tuple(CapabilityField.from_dict(row) for row in payload.get("fields", ()))
        self._by_field = {row.field_id: row for row in self.fields}
        self._by_representation = {row.representation_id: row for row in self.fields}
        self.validate()

    @classmethod
    def read(cls, path: Path) -> "UnifiedCapabilityRegistry":
        return cls(json.loads(Path(path).read_text(encoding="utf-8")))

    def validate(self) -> None:
        if self.registry_version != REGISTRY_VERSION:
            raise ValueError(f"unsupported unified registry version: {self.registry_version}")
        if set(self.route_contracts) != set(ROUTE_IDS):
            missing = sorted(set(ROUTE_IDS) - set(self.route_contracts))
            extra = sorted(set(self.route_contracts) - set(ROUTE_IDS))
            raise ValueError(f"route registry mismatch: missing={missing}, extra={extra}")
        if len(self._by_field) != len(self.fields):
            raise ValueError("duplicate candidate-visible field_id")
        if len(self._by_representation) != len(self.fields):
            raise ValueError("duplicate representation_id")
        for row in self.fields:
            if not row.source_field_id.startswith("cn.sf."):
                raise ValueError(f"unresolved source identity: {row.field_id}")
            if not row.representation_id.startswith("cn.rep."):
                raise ValueError(f"unresolved representation identity: {row.field_id}")
            unknown = set(row.allowed_routes) - set(ROUTE_IDS)
            if unknown:
                raise ValueError(f"unknown routes for {row.field_id}: {sorted(unknown)}")
            if row.search_eligible and not row.allowed_routes:
                raise ValueError(f"eligible field has no route: {row.field_id}")
            if row.search_eligible and row.pit_status == "PIT_CONTRACT_UNRESOLVED":
                raise ValueError(f"PIT-unresolved field exposed to search: {row.field_id}")
            if row.search_eligible and row.unit_status == "SOURCE_UNIT_GLOSSARY_NOT_ASSERTED":
                raise ValueError(f"unit-unqualified field exposed to search: {row.field_id}")

    @property
    def registry_hash(self) -> str:
        payload = dict(self.payload)
        payload.pop("registry_hash", None)
        return stable_hash(payload)

    def resolve(self, field_id: str) -> CapabilityField:
        try:
            return self._by_field[str(field_id)]
        except KeyError as exc:
            raise KeyError(f"unknown candidate-visible field: {field_id}") from exc

    def resolve_representation(self, identity: str) -> CapabilityField:
        try:
            return self._by_representation[str(identity)]
        except KeyError as exc:
            raise KeyError(f"unknown representation identity: {identity}") from exc

    def fields_for_route(
        self,
        route_id: str,
        *,
        eligible_only: bool = True,
        entity_scopes: Iterable[str] | None = None,
        field_roles: Iterable[str] | None = None,
    ) -> tuple[CapabilityField, ...]:
        if route_id not in self.route_contracts:
            raise KeyError(f"unknown route: {route_id}")
        scopes = set(entity_scopes or ())
        roles = set(field_roles or ())
        rows = [
            row
            for row in self.fields
            if route_id in row.allowed_routes
            and (not eligible_only or row.search_eligible)
            and (not scopes or row.entity_scope in scopes)
            and (not roles or row.field_role in roles)
        ]
        return tuple(sorted(rows, key=lambda row: (row.source_family, row.field_id)))

    def source_lineage(self, field_ids: Iterable[str]) -> list[dict[str, Any]]:
        return [self.resolve(field_id).to_dict() for field_id in sorted(set(field_ids))]


def freeze_registry(payload: Mapping[str, Any], output_path: Path) -> dict[str, Any]:
    frozen = dict(payload)
    frozen["registry_version"] = REGISTRY_VERSION
    frozen.pop("registry_hash", None)
    registry = UnifiedCapabilityRegistry(frozen)
    frozen["registry_hash"] = registry.registry_hash
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(frozen, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return frozen
