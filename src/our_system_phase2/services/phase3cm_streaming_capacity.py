"""Deterministic cache-capacity preflight for the Phase3CM shared DAG.

This module predicts the executor's *owned DAG cache* peak without reading a
market-data row.  It deliberately models the same candidate order, recursive
node order, cache ownership, and last-consumer releases as the streaming hot
path.  Temporary arrays and the portfolio signal matrix are outside the DAG
cache contract and remain covered by the separate RSS gates.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any, Iterable

from our_system_phase2.services.phase3cm_streaming_dag import (
    DAGNode,
    SharedMultiCandidateDAGPlan,
)
from our_system_phase2.services.phase3cm_streaming_expression import ROLLING_OPERATORS


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


class BlockRowLimitError(RuntimeError):
    """The materialized block exceeded its frozen cache-capacity bound."""


def enforce_block_row_limit(
    actual_rows: int,
    *,
    max_block_rows: int,
    block_ordinal: int | None = None,
) -> int:
    """Fail before DAG allocation when a real block exceeds the preflight bound."""

    actual = int(actual_rows)
    maximum = int(max_block_rows)
    if actual < 0:
        raise ValueError("actual block rows cannot be negative")
    if maximum <= 0:
        raise ValueError("max_block_rows must be positive")
    if actual > maximum:
        location = (
            f" block_ordinal={int(block_ordinal)}" if block_ordinal is not None else ""
        )
        raise BlockRowLimitError(
            "CN_PHASE3CM_BLOCK_ROW_LIMIT_EXCEEDED:"
            f"{location} actual_rows={actual} max_block_rows={maximum}"
        )
    return actual


@dataclass(frozen=True, slots=True)
class DAGCachePeakPreflight:
    schema_version: str
    status: str
    dag_plan_hash: str
    candidate_order_hash: str
    release_schedule_hash: str
    candidate_count: int
    dag_node_count: int
    runtime_node_count: int
    skipped_parameter_node_count: int
    max_block_rows: int
    owned_array_bytes_per_node: int
    predicted_peak_owned_arrays: int
    predicted_peak_bytes: int
    predicted_peak_entries: int
    dag_block_cache_bytes: int
    dag_block_cache_entries: int
    byte_headroom: int
    entry_headroom: int
    max_safe_block_rows: int | None
    peak_candidate_ordinal: int
    peak_candidate_id: str
    allocated_node_count: int
    released_node_count: int
    final_cache_bytes: int
    final_cache_entries: int
    assumptions: tuple[str, ...]
    preflight_hash: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _runtime_child_ids(
    node: DAGNode,
    *,
    node_by_id: dict[str, DAGNode],
) -> tuple[str, ...]:
    """Return children actually visited by ``StreamingExpressionExecutor``.

    Numeric atoms on rolling/state operators are parameters consumed by the
    operator and are not recursively materialized as cache arrays.
    """

    operator = node.operator.lower()
    if operator not in ROLLING_OPERATORS:
        return node.input_node_ids
    return tuple(
        child_id
        for child_id in node.input_node_ids
        if not (
            node_by_id[child_id].operator == "constant"
            and not node_by_id[child_id].input_node_ids
        )
    )


def predict_dag_cache_peak(
    dag_plan: SharedMultiCandidateDAGPlan,
    *,
    ordered_candidate_ids: Iterable[str],
    max_block_rows: int,
    dag_block_cache_bytes: int,
    dag_block_cache_entries: int = 2048,
) -> DAGCachePeakPreflight:
    """Predict the cache peak and fail-closed status for a frozen run.

    The prediction is exact for the executor's current cache accounting when
    ``max_block_rows`` is an upper bound for every frozen block: every owned
    cache result is a one-dimensional float64 array of that block's row count,
    while raw-field leaves are borrowed and therefore own zero cache bytes.
    """

    candidate_order = tuple(str(value) for value in ordered_candidate_ids)
    if int(max_block_rows) <= 0:
        raise ValueError("max_block_rows must be positive")
    if int(dag_block_cache_bytes) <= 0 or int(dag_block_cache_entries) <= 0:
        raise ValueError("DAG cache caps must be positive")
    if len(candidate_order) != len(set(candidate_order)):
        raise ValueError("candidate order contains duplicates")

    root_by_candidate = {root.candidate_id: root for root in dag_plan.candidate_roots}
    if set(candidate_order) != set(root_by_candidate):
        missing = sorted(set(root_by_candidate) - set(candidate_order))
        extra = sorted(set(candidate_order) - set(root_by_candidate))
        raise ValueError(
            f"candidate order must cover frozen DAG roots exactly: missing={missing}, extra={extra}"
        )
    node_by_id = {node.node_id: node for node in dag_plan.nodes}
    release_schedule = dag_plan.release_node_ids_by_candidate_batch(
        tuple((candidate_id,) for candidate_id in candidate_order)
    )

    bytes_per_owned_node = int(max_block_rows) * 8
    cached: set[str] = set()
    current_bytes = 0
    peak_bytes = 0
    peak_entries = 0
    peak_candidate_ordinal = 0
    peak_candidate_id = candidate_order[0]
    allocated_nodes: set[str] = set()
    released_nodes: set[str] = set()
    runtime_nodes: set[str] = set()
    skipped_parameter_nodes: set[str] = set()

    def visit(
        node_id: str,
        *,
        candidate_ordinal: int,
        candidate_id: str,
        cache_result: bool = True,
    ) -> None:
        nonlocal current_bytes, peak_bytes, peak_entries
        nonlocal peak_candidate_ordinal, peak_candidate_id
        if node_id in cached:
            return
        node = node_by_id[node_id]
        runtime_children = _runtime_child_ids(node, node_by_id=node_by_id)
        skipped_parameter_nodes.update(set(node.input_node_ids) - set(runtime_children))
        for child_id in runtime_children:
            visit(
                child_id,
                candidate_ordinal=candidate_ordinal,
                candidate_id=candidate_id,
            )

        runtime_nodes.add(node_id)
        if not cache_result:
            return
        cached.add(node_id)
        allocated_nodes.add(node_id)
        if node.operator != "raw_field":
            current_bytes += bytes_per_owned_node
        if len(cached) > peak_entries:
            peak_entries = len(cached)
        if current_bytes > peak_bytes:
            peak_bytes = current_bytes
            peak_candidate_ordinal = candidate_ordinal
            peak_candidate_id = candidate_id

    for ordinal, (candidate_id, release_node_ids) in enumerate(
        zip(candidate_order, release_schedule)
    ):
        root_node_id = root_by_candidate[candidate_id].root_node_id
        visit(
            root_node_id,
            candidate_ordinal=ordinal,
            candidate_id=candidate_id,
            cache_result=root_node_id not in release_node_ids,
        )
        for node_id in release_node_ids:
            if node_id not in cached:
                continue
            cached.remove(node_id)
            released_nodes.add(node_id)
            if node_by_id[node_id].operator != "raw_field":
                current_bytes -= bytes_per_owned_node
        if current_bytes < 0:
            raise RuntimeError("predicted DAG cache byte accounting underflow")

    if cached or current_bytes:
        raise RuntimeError(
            "last-consumer schedule did not release the complete runtime DAG cache"
        )

    predicted_peak_owned_arrays = (
        peak_bytes // bytes_per_owned_node if bytes_per_owned_node else 0
    )
    byte_headroom = int(dag_block_cache_bytes) - peak_bytes
    entry_headroom = int(dag_block_cache_entries) - peak_entries
    status = (
        "CN_PHASE3CM_DAG_CACHE_PREFLIGHT_PASS"
        if byte_headroom >= 0 and entry_headroom >= 0
        else "CN_PHASE3CM_DAG_CACHE_PREFLIGHT_FAIL_CLOSED"
    )
    max_safe_block_rows = (
        int(dag_block_cache_bytes) // (predicted_peak_owned_arrays * 8)
        if predicted_peak_owned_arrays
        else None
    )
    assumptions = (
        "max_block_rows_is_an_upper_bound_for_every_frozen_block",
        "cached_numeric_results_are_contiguous_float64_vectors",
        "raw_field_cache_entries_borrow_block_reader_arrays_and_own_zero_bytes",
        "rolling_numeric_parameter_atoms_are_not_materialized",
        "release_occurs_after_each_candidate_last_consumer",
        "last_use_candidate_roots_write_directly_to_the_output_matrix_without_cache_ownership",
        "temporary_arrays_and_signal_matrix_are_governed_by_rss_not_dag_cache_cap",
    )
    body = {
        "schema_version": "cn_phase3cm_dag_cache_peak_preflight_v1",
        "status": status,
        "dag_plan_hash": dag_plan.plan_hash,
        "candidate_order_hash": _stable_hash(candidate_order),
        "release_schedule_hash": _stable_hash(release_schedule),
        "candidate_count": len(candidate_order),
        "dag_node_count": len(dag_plan.nodes),
        "runtime_node_count": len(runtime_nodes),
        "skipped_parameter_node_count": len(skipped_parameter_nodes),
        "max_block_rows": int(max_block_rows),
        "owned_array_bytes_per_node": bytes_per_owned_node,
        "predicted_peak_owned_arrays": predicted_peak_owned_arrays,
        "predicted_peak_bytes": peak_bytes,
        "predicted_peak_entries": peak_entries,
        "dag_block_cache_bytes": int(dag_block_cache_bytes),
        "dag_block_cache_entries": int(dag_block_cache_entries),
        "byte_headroom": byte_headroom,
        "entry_headroom": entry_headroom,
        "max_safe_block_rows": max_safe_block_rows,
        "peak_candidate_ordinal": peak_candidate_ordinal,
        "peak_candidate_id": peak_candidate_id,
        "allocated_node_count": len(allocated_nodes),
        "released_node_count": len(released_nodes),
        "final_cache_bytes": current_bytes,
        "final_cache_entries": len(cached),
        "assumptions": assumptions,
    }
    return DAGCachePeakPreflight(**body, preflight_hash=_stable_hash(body))
