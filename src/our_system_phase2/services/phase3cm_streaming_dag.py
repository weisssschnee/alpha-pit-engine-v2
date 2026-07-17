"""Deterministic two-level cohort and shared-DAG plan for Phase3CM streaming."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Mapping

from our_system_phase2.services.expression_semantics import ExpressionNode, parse_expression


CROSS_SECTIONAL_MAPPING_OPERATORS = {
    "csrank",
    "rank",
    "zscore",
    "csresidual",
    "safecsresidual",
    "maskedzscore",
    "winsorize",
}
WINDOW_OPERATORS = {
    "acceleration",
    "corr",
    "cov",
    "delay",
    "delta",
    "drawdownpath",
    "eventcount",
    "eventwindow",
    "firsthit",
    "kurt",
    "lasthit",
    "maskedcorr",
    "maskedzscore",
    "mean",
    "med",
    "mom",
    "multiscalerelation",
    "pathshape",
    "persistence",
    "recoverypath",
    "skew",
    "slope",
    "std",
    "validratiogate",
    "windowstatecount",
    "wma",
}


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _as_list(value: Any) -> list[str]:
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("["):
            parsed = json.loads(text)
            return [str(item) for item in parsed]
        return [item for item in text.split("|") if item]
    return [str(item) for item in (value or [])]


def _numeric_atom(node: ExpressionNode) -> int | None:
    if node.args:
        return None
    try:
        value = float(node.token)
    except ValueError:
        return None
    return int(value) if value >= 0 and value.is_integer() else None


def _window_profile(node: ExpressionNode) -> tuple[int, ...]:
    values: set[int] = set()
    if node.token.lower() in WINDOW_OPERATORS:
        for child in node.args[1:]:
            value = _numeric_atom(child)
            if value is not None:
                values.add(value)
    for child in node.args:
        values.update(_window_profile(child))
    return tuple(sorted(values))


def _max_lookback(node: ExpressionNode) -> int:
    values = _window_profile(node)
    return max(values, default=0)


def _contains_mapping(node: ExpressionNode) -> bool:
    return node.token.lower() in CROSS_SECTIONAL_MAPPING_OPERATORS or any(
        _contains_mapping(child) for child in node.args
    )


@dataclass(frozen=True, slots=True)
class ValueCohort:
    cohort_id: str
    backend: str
    raw_field_surface: tuple[str, ...]
    observable_clock_maturity: str
    window_profile: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class MappingSubcohort:
    subcohort_id: str
    value_cohort_id: str
    support_contract: str
    eligibility_contract: str
    mapping_family: str
    portfolio_mode: str


@dataclass(frozen=True, slots=True)
class DAGNode:
    node_id: str
    canonical_expression: str
    operator: str
    input_node_ids: tuple[str, ...]
    raw_fields: tuple[str, ...]
    lookback: int
    output_type: str
    layer: str
    cohort_id: str
    mapping_subcohort_id: str | None
    consumer_count: int
    estimated_bytes: int
    actual_evaluation_count: int = 0
    reuse_count: int = 0


@dataclass(frozen=True, slots=True)
class CandidateRoot:
    candidate_id: str
    pair_id: str
    pair_member_role: str
    value_cohort_id: str
    mapping_subcohort_id: str
    root_node_id: str
    expression: str


@dataclass(frozen=True, slots=True)
class SharedMultiCandidateDAGPlan:
    value_cohorts: tuple[ValueCohort, ...]
    mapping_subcohorts: tuple[MappingSubcohort, ...]
    nodes: tuple[DAGNode, ...]
    candidate_roots: tuple[CandidateRoot, ...]
    plan_hash: str

    @classmethod
    def build(cls, candidates: Iterable[Mapping[str, Any]]) -> "SharedMultiCandidateDAGPlan":
        rows = sorted((dict(row) for row in candidates), key=lambda row: str(row.get("candidate_id") or ""))
        if not rows:
            raise ValueError("at least one candidate is required")

        value_cohort_by_key: dict[tuple[Any, ...], ValueCohort] = {}
        mapping_by_key: dict[tuple[Any, ...], MappingSubcohort] = {}
        raw_nodes: dict[str, dict[str, Any]] = {}
        root_rows: list[CandidateRoot] = []
        consumer_counts: dict[str, int] = {}

        for row in rows:
            candidate_id = str(row.get("candidate_id") or "")
            if not candidate_id:
                raise ValueError("candidate_id is required")
            expression = str(row.get("canonical_expression") or row.get("expression") or "")
            parsed = parse_expression(expression)
            expression = parsed.render()
            backend = "minute_active" if str(row.get("clock_namespace") or "active_bar") == "active_bar" else "session_pit"
            fields = tuple(sorted(set(_as_list(row.get("declared_field_ids") or row.get("field_ids")))))
            maturity = str(
                row.get("maturity_contract")
                or row.get("maturity_rule")
                or row.get("clock_contract")
                or row.get("clock_namespace")
                or "UNSPECIFIED"
            )
            windows = _window_profile(parsed)
            value_key = (backend, fields, maturity, windows)
            value_cohort = value_cohort_by_key.get(value_key)
            if value_cohort is None:
                payload = {
                    "backend": backend,
                    "raw_field_surface": fields,
                    "observable_clock_maturity": maturity,
                    "window_profile": windows,
                }
                value_cohort = ValueCohort("value." + _stable_hash(payload)[:20], *value_key)
                value_cohort_by_key[value_key] = value_cohort

            support = str(row.get("support_unit") or row.get("support_contract") or "UNSPECIFIED")
            eligibility = str(
                row.get("eligibility_contract")
                or row.get("pair_support_alignment_policy")
                or "PRIMARY_CONTROL_FINITE_INTERSECTION_AT_SHARED_COORDINATE"
            )
            mapping_family = str(row.get("outer_mapping") or "cross_sectional")
            portfolio_mode = str(row.get("portfolio_mode") or "long_only_top")
            mapping_key = (value_cohort.cohort_id, support, eligibility, mapping_family, portfolio_mode)
            mapping = mapping_by_key.get(mapping_key)
            if mapping is None:
                payload = {
                    "value_cohort_id": value_cohort.cohort_id,
                    "support_contract": support,
                    "eligibility_contract": eligibility,
                    "mapping_family": mapping_family,
                    "portfolio_mode": portfolio_mode,
                }
                mapping = MappingSubcohort("mapping." + _stable_hash(payload)[:20], *mapping_key)
                mapping_by_key[mapping_key] = mapping

            def add_node(node: ExpressionNode) -> str:
                mapping_layer = _contains_mapping(node)
                child_ids = tuple(add_node(child) for child in node.args)
                layer = "MAPPING" if mapping_layer else "VALUE"
                identity_payload = {
                    "layer": layer,
                    "value_cohort_id": value_cohort.cohort_id,
                    "mapping_subcohort_id": mapping.subcohort_id if mapping_layer else None,
                    "canonical_expression": node.render(),
                }
                node_id = ("mapping_node." if mapping_layer else "value_node.") + _stable_hash(identity_payload)[:24]
                for child_id in child_ids:
                    consumer_counts[child_id] = consumer_counts.get(child_id, 0) + 1
                if node_id not in raw_nodes:
                    raw_fields = tuple(sorted(set(re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", node.render()))))
                    raw_nodes[node_id] = {
                        "node_id": node_id,
                        "canonical_expression": node.render(),
                        "operator": node.token if node.args else ("raw_field" if node.token.startswith("$") else "constant"),
                        "input_node_ids": child_ids,
                        "raw_fields": raw_fields,
                        "lookback": _max_lookback(node),
                        "output_type": "numeric",
                        "layer": layer,
                        "cohort_id": value_cohort.cohort_id,
                        "mapping_subcohort_id": mapping.subcohort_id if mapping_layer else None,
                        "estimated_bytes": 0,
                    }
                return node_id

            root_id = add_node(parsed)
            consumer_counts[root_id] = consumer_counts.get(root_id, 0) + 1
            root_rows.append(
                CandidateRoot(
                    candidate_id=candidate_id,
                    pair_id=str(row.get("pair_id") or ""),
                    pair_member_role=str(row.get("pair_member_role") or ""),
                    value_cohort_id=value_cohort.cohort_id,
                    mapping_subcohort_id=mapping.subcohort_id,
                    root_node_id=root_id,
                    expression=expression,
                )
            )

        nodes = tuple(
            DAGNode(
                **raw_nodes[node_id],
                consumer_count=consumer_counts.get(node_id, 0),
                reuse_count=max(0, consumer_counts.get(node_id, 0) - 1),
            )
            for node_id in sorted(raw_nodes)
        )
        value_cohorts = tuple(sorted(value_cohort_by_key.values(), key=lambda item: item.cohort_id))
        mappings = tuple(sorted(mapping_by_key.values(), key=lambda item: item.subcohort_id))
        roots = tuple(sorted(root_rows, key=lambda item: item.candidate_id))
        payload = {
            "schema_version": "cn_shared_multicandidate_dag_plan_v1",
            "value_cohorts": [asdict(item) for item in value_cohorts],
            "mapping_subcohorts": [asdict(item) for item in mappings],
            "nodes": [asdict(item) for item in nodes],
            "candidate_roots": [asdict(item) for item in roots],
        }
        return cls(value_cohorts, mappings, nodes, roots, _stable_hash(payload))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "cn_shared_multicandidate_dag_plan_v1",
            "plan_hash": self.plan_hash,
            "value_cohorts": [asdict(item) for item in self.value_cohorts],
            "mapping_subcohorts": [asdict(item) for item in self.mapping_subcohorts],
            "nodes": [asdict(item) for item in self.nodes],
            "candidate_roots": [asdict(item) for item in self.candidate_roots],
        }

    def release_node_ids_by_candidate_batch(
        self,
        candidate_batches: Iterable[Iterable[str]],
    ) -> tuple[tuple[str, ...], ...]:
        batches = tuple(tuple(str(value) for value in batch) for batch in candidate_batches)
        if not batches or any(not batch for batch in batches):
            raise ValueError("candidate batches must be non-empty")
        flattened = tuple(value for batch in batches for value in batch)
        if len(flattened) != len(set(flattened)):
            raise ValueError("candidate batches must not contain duplicates")
        root_by_candidate = {root.candidate_id: root for root in self.candidate_roots}
        if set(flattened) != set(root_by_candidate):
            raise ValueError("candidate batches must cover the frozen DAG roots exactly")
        node_by_id = {node.node_id: node for node in self.nodes}
        dependencies: dict[str, set[str]] = {}

        def visit(node_id: str) -> set[str]:
            cached = dependencies.get(node_id)
            if cached is not None:
                return cached
            node = node_by_id[node_id]
            result = {node_id}
            for child_id in node.input_node_ids:
                result.update(visit(child_id))
            dependencies[node_id] = result
            return result

        used_by_batch: list[set[str]] = []
        for batch in batches:
            used: set[str] = set()
            for candidate_id in batch:
                used.update(visit(root_by_candidate[candidate_id].root_node_id))
            used_by_batch.append(used)
        last_use: dict[str, int] = {}
        for batch_index, used in enumerate(used_by_batch):
            for node_id in used:
                last_use[node_id] = batch_index
        return tuple(
            tuple(sorted(node_id for node_id, index in last_use.items() if index == batch_index))
            for batch_index in range(len(batches))
        )
