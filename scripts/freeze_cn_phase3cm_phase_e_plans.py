from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.services.phase3cm_streaming_dag import SharedMultiCandidateDAGPlan
from our_system_phase2.services.phase3cm_streaming_capacity import (
    _runtime_child_ids,
    predict_dag_cache_peak,
)
from our_system_phase2.services.phase3cm_streaming_resource_contract import (
    FrozenExecutionPlan,
    balanced_pair_batches,
)
from our_system_phase2.services.phase3cm_streaming_expression import unsupported_streaming_operators


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _runtime_candidate_pairs(
    rows: Sequence[Mapping[str, Any]],
    *,
    pair_count: int,
    clock_namespace: str,
) -> list[dict[str, Any]]:
    """Mirror the evaluator's exact-count Phase E table normalization."""

    pair_order: list[str] = []
    by_pair: dict[str, list[dict[str, Any]]] = {}
    for raw in rows:
        row = dict(raw)
        pair_id = str(row.get("pair_id") or "")
        if not pair_id:
            raise ValueError("frozen candidate row is missing pair_id")
        declared_clock = str(row.get("clock_namespace") or "")
        if declared_clock and declared_clock != str(clock_namespace):
            raise ValueError(f"frozen candidate clock namespace drift: {pair_id}")
        if pair_id not in by_pair:
            pair_order.append(pair_id)
            by_pair[pair_id] = []
        row["clock_namespace"] = str(clock_namespace)
        by_pair[pair_id].append(row)
    if len(pair_order) != int(pair_count):
        raise ValueError("Phase E candidate table pair count does not match binding")
    selected: list[dict[str, Any]] = []
    for pair_id in pair_order:
        members = by_pair[pair_id]
        if len(members) != 2:
            raise ValueError(f"frozen pair does not contain exactly two members: {pair_id}")
        members.sort(
            key=lambda row: 0
            if str(row.get("pair_member_role") or "") == "PRIMARY"
            else 1
        )
        if [str(row.get("pair_member_role") or "") for row in members] != [
            "PRIMARY",
            "CONTROL",
        ]:
            raise ValueError(f"frozen pair roles drift: {pair_id}")
        selected.extend(members)
    return selected


def _bound_candidate_table(
    *,
    binding_path: Path,
    binding: Mapping[str, Any],
    artifact_name: str,
    explicit_path: Path | None,
) -> tuple[Path, list[dict[str, str]]]:
    matching = [
        dict(row)
        for row in binding.get("artifacts") or []
        if Path(str(row.get("path") or "")).name == str(artifact_name)
    ]
    if len(matching) != 1:
        raise ValueError(f"binding must contain exactly one {artifact_name} artifact")
    record = matching[0]
    if not re.fullmatch(r"[0-9a-f]{64}", str(record.get("sha256") or "")):
        raise ValueError(f"binding does not contain an exact {artifact_name} hash")
    path = (
        explicit_path.resolve()
        if explicit_path is not None
        else (binding_path.parent / str(record["path"])).resolve()
    )
    if not path.is_file() or _sha256(path) != str(record["sha256"]):
        raise ValueError(f"candidate table content hash drift: {artifact_name}")
    return path, _read_csv(path)


def _load_plan(path: Path) -> FrozenExecutionPlan:
    return FrozenExecutionPlan.from_dict(json.loads(path.read_text(encoding="utf-8")))


def _phase_e(
    source: FrozenExecutionPlan,
    *,
    heavy_processes: int,
    pair_batches: Sequence[Sequence[str]],
) -> FrozenExecutionPlan:
    if source.phase not in {"D", "E"}:
        raise ValueError("Phase E freeze requires a completed Phase D or E source plan")
    return FrozenExecutionPlan.create(
        phase="E",
        block_size=source.block_size,
        block_boundaries=source.block_boundaries,
        pair_batches=pair_batches,
        heavy_processes=int(heavy_processes),
        compute_threads=source.compute_threads,
        primary_thread_pool=source.primary_thread_pool,
        cache_caps=source.cache_caps,
        checkpoint_every_blocks=source.checkpoint_every_blocks,
        rss_soft_bytes=source.rss_soft_bytes,
        rss_hard_bytes=source.rss_hard_bytes,
        global_rss_hard_bytes=source.global_rss_hard_bytes,
    )


def _dag_cache_greedy_order(
    *,
    pairs: Sequence[Mapping[str, Any]],
    candidate_rows: Sequence[Mapping[str, Any]],
    clock_namespace: str,
    maximum_batch_size: int,
) -> dict[str, Any]:
    """Freeze a deterministic structural order without consulting outcomes.

    The input rows are the same runtime-normalized candidate-table rows consumed
    by the evaluator and DAG-cache preflight.  SharedMultiCandidateDAGPlan owns
    the semantic projection, so unrelated diagnostics cannot affect this order.
    """

    limit = int(maximum_batch_size)
    if limit <= 0:
        raise ValueError("pair batch size must be positive")
    selected_pairs = [
        dict(row)
        for row in pairs
        if str(row.get("clock_namespace") or "") == str(clock_namespace)
    ]
    if not selected_pairs:
        raise ValueError(f"no frozen pairs for clock namespace {clock_namespace}")
    pair_by_id = {str(row.get("pair_id") or ""): row for row in selected_pairs}
    if "" in pair_by_id or len(pair_by_id) != len(selected_pairs):
        raise ValueError(f"duplicate or empty pair identity for {clock_namespace}")

    candidate_input_pair_ids: list[str] = []
    observed_input_pairs: set[str] = set()
    for raw in candidate_rows:
        if str(raw.get("clock_namespace") or "") != str(clock_namespace):
            continue
        pair_id = str(raw.get("pair_id") or "")
        if pair_id and pair_id not in observed_input_pairs:
            candidate_input_pair_ids.append(pair_id)
            observed_input_pairs.add(pair_id)
    if set(candidate_input_pair_ids) != set(pair_by_id):
        raise ValueError(f"candidate-table and binding pair identities differ for {clock_namespace}")

    members_by_pair: dict[str, dict[str, dict[str, Any]]] = {}
    candidate_ids: set[str] = set()
    for raw in candidate_rows:
        if str(raw.get("clock_namespace") or "") != str(clock_namespace):
            continue
        pair_id = str(raw.get("pair_id") or "")
        role = str(raw.get("pair_member_role") or "")
        candidate_id = str(raw.get("candidate_id") or "")
        if pair_id not in pair_by_id or role not in {"PRIMARY", "CONTROL"}:
            raise ValueError(f"invalid frozen member identity for {clock_namespace}: {pair_id}:{role}")
        if not candidate_id or candidate_id in candidate_ids:
            raise ValueError(f"duplicate or empty candidate identity: {candidate_id}")
        candidate_ids.add(candidate_id)
        roles = members_by_pair.setdefault(pair_id, {})
        if role in roles:
            raise ValueError(f"duplicate {role} member for pair {pair_id}")
        roles[role] = dict(raw)

    runtime_rows: list[dict[str, Any]] = []
    pair_member_ids: dict[str, tuple[str, str]] = {}
    for pair_id, pair in pair_by_id.items():
        roles = members_by_pair.get(pair_id) or {}
        if set(roles) != {"PRIMARY", "CONTROL"}:
            raise ValueError(f"pair must contain exactly PRIMARY and CONTROL: {pair_id}")
        primary_id = str(roles["PRIMARY"].get("candidate_id") or "")
        control_id = str(roles["CONTROL"].get("candidate_id") or "")
        if primary_id != str(pair.get("candidate_id") or ""):
            raise ValueError(f"primary candidate identity drift for pair {pair_id}")
        if control_id != str(pair.get("control_candidate_id") or ""):
            raise ValueError(f"control candidate identity drift for pair {pair_id}")
        pair_member_ids[pair_id] = (primary_id, control_id)
        for role in ("PRIMARY", "CONTROL"):
            runtime_rows.append(dict(roles[role]))

    dag = SharedMultiCandidateDAGPlan.build(runtime_rows)
    node_by_id = {node.node_id: node for node in dag.nodes}
    root_by_candidate = {root.candidate_id: root for root in dag.candidate_roots}
    dependencies: dict[str, frozenset[str]] = {}

    def runtime_owned_dependency_closure(node_id: str) -> frozenset[str]:
        cached = dependencies.get(node_id)
        if cached is not None:
            return cached
        node = node_by_id[node_id]
        closure: set[str] = set()
        for child_id in _runtime_child_ids(node, node_by_id=node_by_id):
            closure.update(runtime_owned_dependency_closure(child_id))
        if node.operator != "raw_field":
            closure.add(node_id)
        frozen = frozenset(closure)
        dependencies[node_id] = frozen
        return frozen

    member_owned_nodes: dict[str, frozenset[str]] = {}
    for candidate_id, root in root_by_candidate.items():
        member_owned_nodes[candidate_id] = runtime_owned_dependency_closure(
            root.root_node_id
        )

    structural_nodes: dict[str, frozenset[str]] = {}
    structure_hashes: dict[str, str] = {}
    for pair_id, (primary_id, control_id) in pair_member_ids.items():
        nodes = set(member_owned_nodes[primary_id])
        nodes.update(member_owned_nodes[control_id])
        structural_nodes[pair_id] = frozenset(nodes)
        structure_hashes[pair_id] = _stable_hash(sorted(nodes))

    remaining_uses: dict[str, int] = {}
    for nodes in member_owned_nodes.values():
        for node_id in nodes:
            remaining_uses[node_id] = remaining_uses.get(node_id, 0) + 1
    live_nodes: set[str] = set()
    remaining = set(pair_by_id)
    ordered_pair_ids: list[str] = []
    simulated_peak_owned_nodes = 0

    def simulate_pair(pair_id: str) -> tuple[int, set[str], int]:
        primary_id, control_id = pair_member_ids[pair_id]
        state = set(live_nodes)
        local_remaining = {
            node_id: remaining_uses[node_id]
            for node_id in structural_nodes[pair_id]
        }
        peak = len(state)
        closed: set[str] = set()
        for candidate_id in (primary_id, control_id):
            nodes = member_owned_nodes[candidate_id]
            state.update(nodes)
            peak = max(peak, len(state))
            for node_id in nodes:
                local_remaining[node_id] -= 1
                if local_remaining[node_id] == 0:
                    state.discard(node_id)
                    closed.add(node_id)
        return peak, state, len(closed)

    while remaining:
        simulations = {
            pair_id: simulate_pair(pair_id) for pair_id in remaining
        }
        selected = min(
            remaining,
            key=lambda pair_id: (
                simulations[pair_id][0],
                len(simulations[pair_id][1]),
                -simulations[pair_id][2],
                len(structural_nodes[pair_id]),
                pair_id,
            ),
        )
        materialize_peak, post_release_live, _ = simulations[selected]
        simulated_peak_owned_nodes = max(
            simulated_peak_owned_nodes,
            materialize_peak,
        )
        live_nodes = post_release_live
        for candidate_id in pair_member_ids[selected]:
            for node_id in member_owned_nodes[candidate_id]:
                remaining_uses[node_id] -= 1
        ordered_pair_ids.append(selected)
        remaining.remove(selected)
    if live_nodes or any(remaining_uses.values()):
        raise AssertionError("DAG-cache greedy simulation did not release all owned nodes")

    ordered_pair_ids_tuple = tuple(ordered_pair_ids)
    batches = balanced_pair_batches(ordered_pair_ids_tuple, limit)
    member_order = tuple(
        (pair_id, "PRIMARY", pair_member_ids[pair_id][0], "CONTROL", pair_member_ids[pair_id][1])
        for pair_id in ordered_pair_ids_tuple
    )
    candidate_order = tuple(
        candidate_id
        for pair_id in ordered_pair_ids_tuple
        for candidate_id in pair_member_ids[pair_id]
    )
    exact_preflight = predict_dag_cache_peak(
        dag,
        ordered_candidate_ids=candidate_order,
        max_block_rows=1,
        dag_block_cache_bytes=max(8, 16 * max(1, len(dag.nodes))),
        dag_block_cache_entries=max(1, 2 * len(dag.nodes)),
    )
    if exact_preflight.predicted_peak_owned_arrays != simulated_peak_owned_nodes:
        raise AssertionError(
            "DAG-cache greedy simulation drifts from exact capacity predictor"
        )
    ordering_payload = {
        "strategy": "dag_cache_greedy",
        "objective": "runtime_owned_node_materialize_peak_then_post_release_live",
        "clock_namespace": str(clock_namespace),
        "pair_batch_size_max": limit,
        "pair_batches": batches,
        "pair_member_role_order": ("PRIMARY", "CONTROL"),
        "pair_member_order": member_order,
        "dag_plan_hash": dag.plan_hash,
        "capacity_candidate_order_hash": exact_preflight.candidate_order_hash,
        "simulated_peak_runtime_owned_nodes": simulated_peak_owned_nodes,
        "exact_predicted_peak_owned_arrays": exact_preflight.predicted_peak_owned_arrays,
        "exact_capacity_preflight_hash_at_one_row": exact_preflight.preflight_hash,
        "runtime_owned_node_count": len(remaining_uses),
        "pair_structure_manifest_hash": _stable_hash(
            sorted(structure_hashes.items())
        ),
        "member_runtime_owned_node_manifest_hash": _stable_hash(
            sorted(
                (candidate_id, sorted(node_ids))
                for candidate_id, node_ids in member_owned_nodes.items()
            )
        ),
    }
    result = {
        **ordering_payload,
        "input_pair_order_hash": _stable_hash(
            candidate_input_pair_ids
        ),
        "binding_pair_order_hash": _stable_hash(
            [str(row["pair_id"]) for row in selected_pairs]
        ),
        "execution_pair_order_hash": _stable_hash(ordered_pair_ids_tuple),
        "candidate_member_order_hash": _stable_hash(member_order),
    }
    result["ordering_contract_hash"] = _stable_hash(result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--active-phase-d-plan",
        "--active-source-plan",
        dest="active_source_plan",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--session-phase-d-plan",
        "--session-source-plan",
        dest="session_source_plan",
        type=Path,
        required=True,
    )
    parser.add_argument("--active-candidate-table", type=Path)
    parser.add_argument("--session-candidate-table", type=Path)
    parser.add_argument("--binding", type=Path, required=True)
    parser.add_argument("--repo-sha", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--heavy-processes", type=int, default=2)
    parser.add_argument("--pair-batch-size", type=int, default=4)
    args = parser.parse_args()

    if not re.fullmatch(r"[0-9a-f]{40}", str(args.repo_sha)):
        raise ValueError("repo SHA must be a full lowercase 40-character Git SHA")
    if int(args.heavy_processes) != 2:
        raise ValueError("the final two-backend qualification freezes exactly two heavy processes")
    active_source_path = args.active_source_plan.resolve()
    session_source_path = args.session_source_plan.resolve()
    active_source = _load_plan(active_source_path)
    session_source = _load_plan(session_source_path)
    if active_source.block_boundaries != session_source.block_boundaries:
        raise ValueError("active and session source-plan calendar blocks drift")
    if int(args.pair_batch_size) <= 0:
        raise ValueError("pair batch size must be positive")
    if active_source.compute_threads + session_source.compute_threads > 24:
        raise ValueError("global native compute thread budget exceeds 24")

    binding_path = args.binding.resolve()
    binding = json.loads(binding_path.read_text(encoding="utf-8"))
    binding_body = dict(binding)
    claimed_binding_hash = str(binding_body.pop("binding_hash", ""))
    if not re.fullmatch(r"[0-9a-f]{64}", claimed_binding_hash):
        raise ValueError("frozen input binding hash is not exact")
    if _stable_hash(binding_body) != claimed_binding_hash:
        raise ValueError("frozen input binding hash drift")
    if str(binding.get("status")) != "CN_STREAMING_REPAIR_FROZEN_INPUT_BOUND":
        raise ValueError("frozen input binding status drift")
    if binding.get("sealed_reads") != {"validation": 0, "holdout": 0, "forward_2026": 0}:
        raise ValueError("frozen input binding sealed-read contract drift")

    bound_pairs = list(binding.get("pairs") or [])
    active_bound_pair_count = sum(
        1 for row in bound_pairs if str(row.get("clock_namespace")) == "active_bar"
    )
    session_bound_pair_count = sum(
        1
        for row in bound_pairs
        if str(row.get("clock_namespace")) == "stock_session"
    )
    active_candidate_path, active_raw_rows = _bound_candidate_table(
        binding_path=binding_path,
        binding=binding,
        artifact_name="preflight_active_candidates.csv",
        explicit_path=args.active_candidate_table,
    )
    session_candidate_path, session_raw_rows = _bound_candidate_table(
        binding_path=binding_path,
        binding=binding,
        artifact_name="preflight_session_candidates.csv",
        explicit_path=args.session_candidate_table,
    )
    active_candidates = _runtime_candidate_pairs(
        active_raw_rows,
        pair_count=active_bound_pair_count,
        clock_namespace="active_bar",
    )
    session_candidates = _runtime_candidate_pairs(
        session_raw_rows,
        pair_count=session_bound_pair_count,
        clock_namespace="stock_session",
    )
    unsupported = unsupported_streaming_operators(
        str(member.get("canonical_expression") or member.get("expression") or "")
        for member in (*active_candidates, *session_candidates)
    )
    if unsupported:
        raise ValueError(f"streaming evaluator operator surface incomplete: {list(unsupported)}")
    active_ordering = _dag_cache_greedy_order(
        pairs=bound_pairs,
        candidate_rows=active_candidates,
        clock_namespace="active_bar",
        maximum_batch_size=int(args.pair_batch_size),
    )
    session_ordering = _dag_cache_greedy_order(
        pairs=bound_pairs,
        candidate_rows=session_candidates,
        clock_namespace="stock_session",
        maximum_batch_size=int(args.pair_batch_size),
    )
    for ordering, candidate_path in (
        (active_ordering, active_candidate_path),
        (session_ordering, session_candidate_path),
    ):
        ordering.pop("ordering_contract_hash", None)
        ordering["candidate_table_sha256"] = _sha256(candidate_path)
        ordering["ordering_contract_hash"] = _stable_hash(ordering)
    active_pair_ids = [
        pair_id for batch in active_ordering["pair_batches"] for pair_id in batch
    ]
    session_pair_ids = [
        pair_id for batch in session_ordering["pair_batches"] for pair_id in batch
    ]
    if not active_pair_ids or not session_pair_ids:
        raise ValueError("Phase E binding must contain both active and session pairs")
    if len(active_pair_ids) + len(session_pair_ids) != int(binding.get("pair_count") or -1):
        raise ValueError("Phase E binding pair counts drift")

    active = _phase_e(
        active_source,
        heavy_processes=int(args.heavy_processes),
        pair_batches=active_ordering["pair_batches"],
    )
    session = _phase_e(
        session_source,
        heavy_processes=int(args.heavy_processes),
        pair_batches=session_ordering["pair_batches"],
    )
    output_root = args.output_root.resolve()
    active_path = output_root / "active_bar" / "CN_FROZEN_EXECUTION_PLAN.json"
    session_path = output_root / "stock_session" / "CN_FROZEN_EXECUTION_PLAN.json"
    _write_json(active_path, active.to_dict())
    _write_json(session_path, session.to_dict())
    combined = {
        "schema_version": "cn_phase3cm_phase_e_combined_execution_contract_v1",
        "status": "CN_PHASE3CM_PHASE_E_EXECUTION_PLANS_FROZEN",
        "repo_sha": str(args.repo_sha),
        "input_binding_hash": str(binding["binding_hash"]),
        "input_binding_sha256": _sha256(binding_path),
        "ordering_strategy": "dag_cache_greedy",
        "pair_member_execution_order": ["PRIMARY", "CONTROL"],
        "ordering_contracts": {
            "active_bar": active_ordering,
            "stock_session": session_ordering,
        },
        "candidate_tables": {
            "active_bar": {
                "path": str(active_candidate_path),
                "sha256": _sha256(active_candidate_path),
            },
            "stock_session": {
                "path": str(session_candidate_path),
                "sha256": _sha256(session_candidate_path),
            },
        },
        "heavy_processes": int(args.heavy_processes),
        "compute_threads_per_process": (
            active.compute_threads
            if active.compute_threads == session.compute_threads
            else None
        ),
        "compute_threads_by_backend": {
            "active_bar": active.compute_threads,
            "stock_session": session.compute_threads,
        },
        "global_active_native_compute_threads": active.compute_threads
        + session.compute_threads,
        "primary_thread_pool": active.primary_thread_pool,
        "thread_environment_by_backend": {
            "active_bar": active.thread_environment,
            "stock_session": session.thread_environment,
        },
        "adaptation": "FORBIDDEN",
        "resource_gate_action": "FAIL_CLOSED_NO_PLAN_CHANGE",
        "plans": {
            "active_bar": {
                "path": str(active_path),
                "pair_count": len(active_pair_ids),
                "execution_plan_hash": active.execution_plan_hash,
                "source_execution_plan_phase": active_source.phase,
                "source_execution_plan_path": str(active_source_path),
                "source_execution_plan_hash": active_source.execution_plan_hash,
                "source_execution_plan_sha256": _sha256(active_source_path),
                "source_execution_plan_pair_count": sum(map(len, active_source.pair_batches)),
                "source_phase_d_execution_plan_hash": (
                    active_source.execution_plan_hash if active_source.phase == "D" else None
                ),
                "source_phase_d_pair_count": (
                    sum(map(len, active_source.pair_batches))
                    if active_source.phase == "D"
                    else None
                ),
                "input_pair_order_hash": active_ordering["input_pair_order_hash"],
                "execution_pair_order_hash": active_ordering[
                    "execution_pair_order_hash"
                ],
                "candidate_member_order_hash": active_ordering[
                    "candidate_member_order_hash"
                ],
                "ordering_contract_hash": active_ordering["ordering_contract_hash"],
                "dag_plan_hash": active_ordering["dag_plan_hash"],
                "capacity_candidate_order_hash": active_ordering[
                    "capacity_candidate_order_hash"
                ],
                "simulated_peak_runtime_owned_nodes": active_ordering[
                    "simulated_peak_runtime_owned_nodes"
                ],
                "exact_predicted_peak_owned_arrays": active_ordering[
                    "exact_predicted_peak_owned_arrays"
                ],
                "exact_capacity_preflight_hash_at_one_row": active_ordering[
                    "exact_capacity_preflight_hash_at_one_row"
                ],
                "candidate_table_sha256": _sha256(active_candidate_path),
                "compute_threads": active.compute_threads,
                "sha256": _sha256(active_path),
            },
            "stock_session": {
                "path": str(session_path),
                "pair_count": len(session_pair_ids),
                "execution_plan_hash": session.execution_plan_hash,
                "source_execution_plan_phase": session_source.phase,
                "source_execution_plan_path": str(session_source_path),
                "source_execution_plan_hash": session_source.execution_plan_hash,
                "source_execution_plan_sha256": _sha256(session_source_path),
                "source_execution_plan_pair_count": sum(map(len, session_source.pair_batches)),
                "source_phase_d_execution_plan_hash": (
                    session_source.execution_plan_hash if session_source.phase == "D" else None
                ),
                "source_phase_d_pair_count": (
                    sum(map(len, session_source.pair_batches))
                    if session_source.phase == "D"
                    else None
                ),
                "input_pair_order_hash": session_ordering["input_pair_order_hash"],
                "execution_pair_order_hash": session_ordering[
                    "execution_pair_order_hash"
                ],
                "candidate_member_order_hash": session_ordering[
                    "candidate_member_order_hash"
                ],
                "ordering_contract_hash": session_ordering["ordering_contract_hash"],
                "dag_plan_hash": session_ordering["dag_plan_hash"],
                "capacity_candidate_order_hash": session_ordering[
                    "capacity_candidate_order_hash"
                ],
                "simulated_peak_runtime_owned_nodes": session_ordering[
                    "simulated_peak_runtime_owned_nodes"
                ],
                "exact_predicted_peak_owned_arrays": session_ordering[
                    "exact_predicted_peak_owned_arrays"
                ],
                "exact_capacity_preflight_hash_at_one_row": session_ordering[
                    "exact_capacity_preflight_hash_at_one_row"
                ],
                "candidate_table_sha256": _sha256(session_candidate_path),
                "compute_threads": session.compute_threads,
                "sha256": _sha256(session_path),
            },
        },
        "data_role": "development_train_only",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
    }
    combined_path = output_root / "CN_PHASE_E_COMBINED_EXECUTION_CONTRACT.json"
    _write_json(combined_path, combined)
    print(json.dumps(combined, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
