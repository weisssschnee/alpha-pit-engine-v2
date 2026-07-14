"""Immutable authorization receipts between proposal and formal evaluation.

Generators remain proposal sources.  This module is the only bridge allowed to
turn a proposal into an evaluator input: it recompiles the typed route against
the frozen capability registry and binds that verdict to the split, data
release and evaluator implementation hashes.
"""

from __future__ import annotations

import ast
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from our_system_phase2.services.fixed_split_authority import FixedSplitAuthority, file_sha256
from our_system_phase2.services.expression_semantics import analyze_expression, parse_expression
from our_system_phase2.services.typed_route_compiler import COMPILER_VERSION, TypedRouteCompiler
from our_system_phase2.services.typed_primitive_gate import expression_fields
from our_system_phase2.services.unified_capability_registry import UnifiedCapabilityRegistry, stable_hash


RECEIPT_SCHEMA_VERSION = "cn_candidate_submission_receipt_v1"
AUTHORIZATION_STATUS = "AUTHORIZED_FOR_FORMAL_EVALUATION"
CONTRACT_LIST_FIELDS = {
    "access_roles",
    "condition_field_ids",
    "declared_field_ids",
    "field_ids",
    "operator_paths",
    "representation_ids",
    "source_field_ids",
}
CONTRACT_BOOL_FIELDS = {
    "exposure_ledger_required",
    "is_matched_control",
    "maturity_contract_registered",
    "requires_intrabar_order",
    "uses_future_revision",
}
CONTRACT_KEYS = (
    "candidate_id",
    "route_id",
    "expression",
    "operator_family",
    "matched_control_id",
    "declared_field_ids",
    "condition_field_ids",
    "is_matched_control",
    "vote_policy",
    "maturity_contract_registered",
    "exposure_ledger_required",
    "access_roles",
    "uses_future_revision",
    "requires_intrabar_order",
    "claimed_state_field_id",
    "state_source_expression",
    "proposal_origin",
    "declared_source_lags",
)


class CandidateReceiptError(RuntimeError):
    """Raised when a candidate lacks a current, immutable authorization."""


LEGACY_ADAPTER_VERSION = "cn_legacy_proposal_to_typed_route_adapter_v1"


def _expression_operators(expression: str) -> set[str]:
    node = parse_expression(analyze_expression(expression).canonical_expression)
    pending = [node]
    operators: set[str] = set()
    while pending:
        current = pending.pop()
        if current.args:
            operators.add(current.token)
            pending.extend(current.args)
    return operators


class LegacyCandidateSubmissionAdapter:
    """Map legacy proposals to typed contracts without granting authority.

    The adapter may infer only static, FirstN and PIT-qualified slow routes.
    Event/state/regime proposals require an explicit typed contract because a
    legacy field name cannot prove episode, maturity or matched-control
    semantics.
    """

    def __init__(self, registry: UnifiedCapabilityRegistry) -> None:
        self.registry = registry

    def adapt_pair(self, candidate: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        row = normalize_candidate_contract(candidate)
        if str(row.get("route_id") or ""):
            raise CandidateReceiptError("legacy adapter cannot rewrite an existing typed route contract")
        expression = str(row.get("expression") or "")
        field_ids = expression_fields(expression)
        if not field_ids:
            raise CandidateReceiptError("legacy proposal has no expression field lineage")
        fields = [self.registry.resolve(field_id) for field_id in field_ids]
        operators = _expression_operators(expression)
        if any(field.source_family == "firstN" for field in fields):
            route_id = "FIRSTN_PATH"
            operator_family = "SignedPath"
        elif all("MINUTE_STATIC" in field.allowed_routes for field in fields):
            route_id = "MINUTE_STATIC"
            operator_family = "Arithmetic" if operators.intersection({"Add", "Sub", "Mul", "Div", "ZScore"}) else "CSRank"
        elif all("SLOW_TEMPORAL_CHANGE" in field.allowed_routes for field in fields) and operators.intersection(
            {"Delta", "Slope", "Acceleration", "Persistence", "MultiScaleRelation", "QoQ", "YoY", "TTMChange"}
        ):
            route_id = "SLOW_TEMPORAL_CHANGE"
            operator_family = next(
                name
                for name in ("Delta", "Slope", "Acceleration", "Persistence", "MultiScaleRelation", "QoQ", "YoY", "TTMChange")
                if name in operators
            )
        elif all("SLOW_CROSS_SECTIONAL_LEVEL" in field.allowed_routes for field in fields):
            route_id = "SLOW_CROSS_SECTIONAL_LEVEL"
            operator_family = "CSRank"
        else:
            raise CandidateReceiptError(
                "legacy proposal cannot be conservatively mapped to a legal typed route; "
                "event/state/regime contracts must be explicit"
            )

        candidate_id = str(row.get("candidate_id") or "")
        if not candidate_id:
            raise CandidateReceiptError("legacy proposal requires candidate_id before adaptation")
        control_id = candidate_id + "__typed_control"
        base = {
            "route_id": route_id,
            "operator_family": operator_family,
            "declared_field_ids": field_ids,
            "condition_field_ids": [],
            "maturity_contract_registered": False,
            "exposure_ledger_required": True,
            "access_roles": ["development"],
            "uses_future_revision": False,
            "requires_intrabar_order": False,
            "proposal_origin": f"legacy_adapter:{LEGACY_ADAPTER_VERSION}",
            "legacy_adapter_version": LEGACY_ADAPTER_VERSION,
        }
        adapted = {
            **row,
            **base,
            "matched_control_id": control_id,
            "is_matched_control": False,
            "vote_policy": "ONE_SUPPORT_UNIT_ONE_VOTE",
        }
        control_field = fields[0].field_id
        control = {
            **base,
            "candidate_id": control_id,
            "expression": f"CSRank(${control_field})",
            "matched_control_id": candidate_id,
            "is_matched_control": True,
            "vote_policy": "CONTROL_NO_SEPARATE_VOTE",
        }
        return adapted, control


def _as_list(value: Any) -> list[str]:
    if value is None or value == "":
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value]
    text = str(value).strip()
    if not text:
        return []
    try:
        parsed = ast.literal_eval(text)
    except (SyntaxError, ValueError):
        parsed = [item.strip() for item in text.split("|") if item.strip()]
    if isinstance(parsed, (list, tuple, set)):
        return [str(item) for item in parsed]
    return [str(parsed)]


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def normalize_candidate_contract(candidate: Mapping[str, Any]) -> dict[str, Any]:
    row = dict(candidate)
    for key in CONTRACT_LIST_FIELDS:
        if key in row:
            row[key] = _as_list(row.get(key))
    for key in CONTRACT_BOOL_FIELDS:
        if key in row:
            row[key] = _as_bool(row.get(key))
    lag_value = row.get("declared_source_lags")
    if isinstance(lag_value, str) and lag_value.strip():
        try:
            parsed_lags = ast.literal_eval(lag_value)
        except (SyntaxError, ValueError) as exc:
            raise CandidateReceiptError("declared_source_lags is not a valid mapping") from exc
        row["declared_source_lags"] = parsed_lags
    return row


def candidate_payload_hash(candidate: Mapping[str, Any]) -> str:
    normalized = normalize_candidate_contract(candidate)
    return stable_hash({key: normalized.get(key, [] if key in CONTRACT_LIST_FIELDS else "") for key in CONTRACT_KEYS})


def _combined_file_hash(paths: Sequence[Path]) -> str:
    return stable_hash(
        [
            {"path": Path(path).name, "sha256": file_sha256(Path(path))}
            for path in sorted((Path(path).resolve() for path in paths), key=lambda item: str(item))
        ]
    )


@dataclass(frozen=True, slots=True)
class ReceiptContext:
    registry_hash: str
    typed_compiler_hash: str
    split_manifest_hash: str
    data_release_hash: str
    evaluator_code_hash: str

    @classmethod
    def build(
        cls,
        *,
        registry: UnifiedCapabilityRegistry,
        split_authority: FixedSplitAuthority,
        data_release_hash: str,
        evaluator_paths: Sequence[Path],
    ) -> "ReceiptContext":
        release_hash = str(data_release_hash).strip().lower()
        if len(release_hash) != 64 or any(char not in "0123456789abcdef" for char in release_hash):
            raise CandidateReceiptError("data_release_hash must be a 64-character lowercase SHA-256")
        compiler_path = Path(__file__).with_name("typed_route_compiler.py")
        return cls(
            registry_hash=registry.registry_hash,
            typed_compiler_hash=file_sha256(compiler_path),
            split_manifest_hash=split_authority.manifest_hash,
            data_release_hash=release_hash,
            evaluator_code_hash=_combined_file_hash(evaluator_paths),
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "unified_registry_hash": self.registry_hash,
            "typed_compiler_hash": self.typed_compiler_hash,
            "split_manifest_hash": self.split_manifest_hash,
            "data_release_hash": self.data_release_hash,
            "evaluator_code_hash": self.evaluator_code_hash,
        }


class CandidateSubmissionAuthority:
    def __init__(self, registry: UnifiedCapabilityRegistry, context: ReceiptContext) -> None:
        self.registry = registry
        self.context = context
        self.compiler = TypedRouteCompiler(registry)

    def authorize(self, candidate: Mapping[str, Any]) -> dict[str, Any]:
        normalized = normalize_candidate_contract(candidate)
        declared_lags = normalized.get("declared_source_lags") or {}
        if not isinstance(declared_lags, Mapping):
            raise CandidateReceiptError("declared_source_lags must be a field_id -> lag mapping")
        for field_id, raw_lag in declared_lags.items():
            field = self.registry.resolve(str(field_id))
            expected = f"{field.source_lag} {field.source_lag_unit}"
            if str(raw_lag).strip() != expected:
                raise CandidateReceiptError(
                    f"source lag drift for {field_id}: declared={raw_lag!r} registry={expected!r}"
                )
        verdict = self.compiler.compile(normalized)
        if not verdict.legal:
            raise CandidateReceiptError(
                f"candidate authorization rejected: candidate={normalized.get('candidate_id')} "
                f"code={verdict.rejection_code} reason={verdict.reason}"
            )
        resolved_fields = [self.registry.resolve(field_id) for field_id in verdict.field_ids]
        proposal_origin = str(normalized.get("proposal_origin") or "unknown_proposal_source")
        proposal_kind = (
            "LEGACY_PROPOSAL_SOURCE"
            if proposal_origin.startswith("legacy_adapter:")
            else "UNIFIED_PROPOSAL_SOURCE"
            if str(normalized.get("generator_version") or "").startswith("cn_unified_")
            else "EXPLICIT_TYPED_PROPOSAL_SOURCE"
        )
        frequency = {
            "MINUTE_STATIC": "1min",
            "FIRSTN_PATH": "intraday_session",
            "SLOW_CROSS_SECTIONAL_LEVEL": "stock_session_asof",
            "SLOW_TEMPORAL_CHANGE": "disclosure_or_session_change",
            "DISCLOSURE_EVENT": "disclosure_episode",
            "MARKET_REGIME_CONDITION": "market_time_block",
            "INTRADAY_STATE_TRANSITION": "intraday_state_episode",
            "BROAD_EVENT_FROZEN_ENTRY": "frozen_event_episode",
        }[verdict.route_id]
        body: dict[str, Any] = {
            "receipt_schema_version": RECEIPT_SCHEMA_VERSION,
            "authorization_status": AUTHORIZATION_STATUS,
            "candidate_id": str(normalized.get("candidate_id") or ""),
            "candidate_payload_hash": candidate_payload_hash(normalized),
            "proposal_source": proposal_origin,
            "generator_origin": str(normalized.get("generator_arm") or proposal_origin),
            "legacy_or_unified_proposal_source": proposal_kind,
            "route_id": verdict.route_id,
            "expression": str(normalized.get("expression") or ""),
            "canonical_expression": verdict.canonical_expression,
            "canonical_identity": verdict.canonical_identity,
            "exact_identity": verdict.exact_identity,
            "field_ids": list(verdict.field_ids),
            "source_field_ids": list(verdict.source_field_ids),
            "representation_ids": list(verdict.representation_ids),
            "operator_paths": list(verdict.operator_paths),
            "primitive_ids": sorted({path.rsplit(":", 1)[-1] for path in verdict.operator_paths}),
            "entity_scope": sorted({field.entity_scope for field in resolved_fields}),
            "frequency": frequency,
            "observable_time_contract": [
                f"{field.field_id}|{field.observable_clock}|{field.maturity_rule}"
                for field in resolved_fields
            ],
            "pit_source_lag_contract": [
                f"{field.field_id}|{field.pit_status}|{field.source_lag} {field.source_lag_unit}"
                for field in resolved_fields
            ],
            "support_unit": verdict.support_unit,
            "matched_control_id": str(normalized.get("matched_control_id") or ""),
            "metadata_fields_rejected": [],
            "compiler_version": COMPILER_VERSION,
            **self.context.to_dict(),
        }
        if not body["candidate_id"]:
            raise CandidateReceiptError("candidate_id is required for an immutable submission receipt")
        body["receipt_hash"] = stable_hash(body)
        body["receipt_id"] = "cn.receipt." + body["receipt_hash"][:32]
        return body

    def _validate_receipt_envelope(self, receipt: Mapping[str, Any]) -> dict[str, Any]:
        supplied = dict(receipt)
        receipt_hash = str(supplied.pop("receipt_hash", ""))
        receipt_id = str(supplied.pop("receipt_id", ""))
        expected_hash = stable_hash(supplied)
        if receipt_hash != expected_hash or receipt_id != "cn.receipt." + expected_hash[:32]:
            raise CandidateReceiptError("candidate receipt hash or immutable receipt_id does not match payload")
        expected_context = self.context.to_dict()
        context_drift = sorted(
            key for key, value in expected_context.items() if supplied.get(key) != value
        )
        if context_drift:
            raise CandidateReceiptError(f"candidate receipt context drift: {context_drift}")
        if supplied.get("authorization_status") != "AUTHORIZED_FOR_FORMAL_EVALUATION":
            raise CandidateReceiptError("candidate receipt is not authorized")
        return dict(receipt)

    def validate(self, candidate: Mapping[str, Any], receipt: Mapping[str, Any]) -> dict[str, Any]:
        self._validate_receipt_envelope(receipt)
        expected = self.authorize(candidate)
        if expected != dict(receipt):
            drift = sorted(key for key in expected if expected.get(key) != receipt.get(key))
            raise CandidateReceiptError(f"candidate receipt authority drift: {drift}")
        return expected

    def authorize_table(self, candidates: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        candidate_rows = [normalize_candidate_contract(row) for row in candidates]
        rows = [self.authorize(row) for row in candidate_rows]
        if len({row["candidate_id"] for row in rows}) != len(rows):
            raise CandidateReceiptError("duplicate candidate_id in receipt table")
        ids = {str(row.get("candidate_id") or "") for row in candidate_rows}
        for row in candidate_rows:
            control_id = str(row.get("matched_control_id") or "")
            if control_id not in ids:
                raise CandidateReceiptError(
                    f"matched control candidate is absent from submission table: "
                    f"candidate={row.get('candidate_id')} control={control_id or '<missing>'}"
                )
        return rows

    def validate_table(
        self,
        candidates: Iterable[Mapping[str, Any]],
        receipts: Iterable[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        by_id = {str(row.get("candidate_id") or ""): dict(row) for row in receipts}
        output: list[dict[str, Any]] = []
        for candidate in candidates:
            candidate_id = str(candidate.get("candidate_id") or "")
            if candidate_id not in by_id:
                raise CandidateReceiptError(f"missing candidate submission receipt: {candidate_id or '<missing>'}")
            validated = self.validate(candidate, by_id[candidate_id])
            control_id = str(validated.get("matched_control_id") or "")
            if control_id not in by_id:
                raise CandidateReceiptError(
                    f"matched control receipt is absent: candidate={candidate_id} control={control_id or '<missing>'}"
                )
            self._validate_receipt_envelope(by_id[control_id])
            output.append(validated)
        return output


def write_receipt_table(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("\n".join(json.dumps(dict(row), ensure_ascii=False, sort_keys=True) for row in rows) + "\n", encoding="utf-8")


def read_receipt_table(path: Path) -> list[dict[str, Any]]:
    target = Path(path)
    if not target.is_file():
        raise CandidateReceiptError(f"candidate receipt table does not exist: {target}")
    if target.suffix.lower() == ".csv":
        with target.open("r", encoding="utf-8-sig", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    rows = [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not rows:
        raise CandidateReceiptError(f"candidate receipt table is empty: {target}")
    return rows
