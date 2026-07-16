from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
import polars as pl

from our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit import (
    _candidate_summary_from_reward_atoms,
)
from our_system_phase2.services.phase3cm_streaming_block_reader import TimeMajorBlockReader
from our_system_phase2.services.phase3cm_streaming_checkpoint import (
    StreamingCheckpointPayload,
    load_checkpoint,
    write_checkpoint,
)
from our_system_phase2.services.phase3cm_streaming_dag import SharedMultiCandidateDAGPlan
from our_system_phase2.services.phase3cm_streaming_expression import StreamingExpressionExecutor
from our_system_phase2.services.phase3cm_streaming_portfolio import BatchedPortfolioKernel
from our_system_phase2.services.phase3cm_streaming_reducer import StreamingPortfolioReducer
from our_system_phase2.services.phase3cm_streaming_resource_contract import (
    FrozenExecutionPlan,
    RSSGate,
    validate_frozen_thread_environment,
)
from our_system_phase2.services.phase3cm_streaming_support import PairSupportAccumulator
from our_system_phase2.services.phase3cm_streaming_telemetry import (
    PhaseTelemetryRecorder,
    _process_snapshot,
    build_phase_event,
)


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_csv(path: Path) -> list[dict[str, str]]:
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    fieldnames = list(rows[0].keys()) if rows else []
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        if fieldnames:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="raise")
            writer.writeheader()
            writer.writerows(rows)
        handle.flush()
    temporary.replace(destination)
    return {
        "path": destination.name,
        "row_count": len(rows),
        "sha256": _sha256(destination),
        "bytes": destination.stat().st_size,
    }


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _verify_binding(binding_path: Path, artifact_root: Path) -> dict[str, Any]:
    binding = json.loads(Path(binding_path).read_text(encoding="utf-8"))
    claimed = str(binding.get("binding_hash") or "")
    body = dict(binding)
    body.pop("binding_hash", None)
    if _stable_hash(body) != claimed:
        raise RuntimeError("CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: binding hash")
    if str(binding.get("status")) != "CN_STREAMING_REPAIR_FROZEN_INPUT_BOUND":
        raise RuntimeError("CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: binding status")
    if str(binding.get("data_role")) != "development":
        raise RuntimeError("CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: data role")
    if binding.get("sealed_reads") != {"validation": 0, "holdout": 0, "forward_2026": 0}:
        raise RuntimeError("CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: sealed reads")
    for record in binding.get("artifacts") or []:
        path = Path(artifact_root) / str(record["path"])
        if not path.is_file() or _sha256(path) != str(record["sha256"]):
            raise RuntimeError(
                f"CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: {record['path']}"
            )
    return binding


def _candidate_pairs(rows: Sequence[Mapping[str, Any]], *, pair_limit: int, clock: str) -> list[dict[str, Any]]:
    pair_order: list[str] = []
    by_pair: dict[str, list[dict[str, Any]]] = {}
    for raw in rows:
        row = dict(raw)
        pair_id = str(row.get("pair_id") or "")
        if pair_id not in by_pair:
            pair_order.append(pair_id)
            by_pair[pair_id] = []
        row["clock_namespace"] = clock
        by_pair[pair_id].append(row)
    selected: list[dict[str, Any]] = []
    for pair_id in pair_order[: int(pair_limit)]:
        members = by_pair[pair_id]
        if len(members) != 2:
            raise RuntimeError(f"frozen pair does not contain exactly two members: {pair_id}")
        members.sort(key=lambda row: 0 if str(row.get("pair_member_role")) == "PRIMARY" else 1)
        if [str(row.get("pair_member_role")) for row in members] != ["PRIMARY", "CONTROL"]:
            raise RuntimeError(f"frozen pair roles drift: {pair_id}")
        selected.extend(members)
    if len(selected) != int(pair_limit) * 2:
        raise RuntimeError("requested pair count exceeds frozen route pack")
    return selected


def _raw_fields(candidates: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    fields = {"close"}
    for row in candidates:
        fields.update(re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", str(row.get("expression") or "")))
    return tuple(sorted(fields))


def _sidecar_files(root: Path) -> tuple[Path, ...]:
    paths = tuple(sorted(path for path in Path(root).glob("shard_*.parquet") if path.is_file()))
    if len(paths) != 16:
        raise RuntimeError(f"qualification requires exactly 16 sidecars: {root} has {len(paths)}")
    return paths


def _symbol_registry(paths: Sequence[Path]) -> tuple[str, ...]:
    lazy = pl.concat(
        [pl.scan_parquet(path, rechunk=False, low_memory=True).select(pl.col("code").cast(pl.String)) for path in paths]
    )
    values = lazy.select(pl.col("code").unique().sort()).collect(engine="streaming")["code"].to_list()
    return tuple(str(value) for value in values)


def _session_dates(paths: Sequence[Path]) -> tuple[str, ...]:
    lazy = pl.concat(
        [
            pl.scan_parquet(path, rechunk=False, low_memory=True).select(
                pl.col("trade_time").dt.date().alias("date")
            )
            for path in paths
        ]
    )
    values = lazy.select(pl.col("date").unique().sort()).collect(engine="streaming")["date"].to_list()
    return tuple(str(value) for value in values)


def _block_boundaries(dates: Sequence[str], block_sessions: int) -> tuple[tuple[str, str], ...]:
    output = []
    for start in range(0, len(dates), int(block_sessions)):
        chunk = dates[start : start + int(block_sessions)]
        begin = pd.Timestamp(chunk[0]).isoformat()
        end = (pd.Timestamp(chunk[-1]) + pd.Timedelta(days=1)).isoformat()
        output.append((begin, end))
    return tuple(output)


def _pair_batches(pair_ids: Sequence[str], batch_pairs: int) -> tuple[tuple[str, ...], ...]:
    return tuple(
        tuple(pair_ids[start : start + int(batch_pairs)])
        for start in range(0, len(pair_ids), int(batch_pairs))
    )


def _load_plan(path: Path) -> FrozenExecutionPlan:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    claimed = str(raw.pop("execution_plan_hash"))
    raw.pop("schema_version", None)
    raw.pop("thread_environment", None)
    plan = FrozenExecutionPlan.create(**raw)
    if plan.execution_plan_hash != claimed:
        raise RuntimeError("frozen execution-plan hash drift")
    return plan


def _write_json(path: Path, payload: Any) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _support_masks(signals: np.ndarray) -> np.ndarray:
    if signals.shape[0] % 2:
        raise ValueError("candidate member count must be even")
    pair_count = signals.shape[0] // 2
    masks = np.empty((pair_count, signals.shape[1]), dtype=np.bool_)
    for pair_index in range(pair_count):
        common = np.isfinite(signals[2 * pair_index]) & np.isfinite(signals[2 * pair_index + 1])
        masks[pair_index] = common
        signals[2 * pair_index, ~common] = np.nan
        signals[2 * pair_index + 1, ~common] = np.nan
    return masks


def _finalize_pairs(
    *,
    candidates: Sequence[Mapping[str, Any]],
    reward_rows: Sequence[Mapping[str, Any]],
    reducer: StreamingPortfolioReducer,
    support: PairSupportAccumulator,
    binding: Mapping[str, Any],
) -> list[dict[str, Any]]:
    reward_by_id = {str(row.get("candidate_id")): dict(row) for row in reward_rows}
    member_binding = {str(row["candidate_id"]): dict(row) for row in binding["candidate_members"]}
    pair_binding = {str(row["pair_id"]): dict(row) for row in binding["pairs"]}
    support_identity = support.identities()
    rows = []
    for pair_index in range(len(candidates) // 2):
        primary = dict(candidates[2 * pair_index])
        control = dict(candidates[2 * pair_index + 1])
        pair_id = str(primary["pair_id"])
        primary_reward = reward_by_id.get(str(primary["candidate_id"]), {})
        control_reward = reward_by_id.get(str(control["candidate_id"]), {})
        primary_value = primary_reward.get("optimizer_reward")
        control_value = control_reward.get("optimizer_reward")
        blockers: list[str] = []
        support_row = support_identity[pair_id]
        if int(support_row["count"]) == 0:
            blockers.append("empty_pair_support")
        primary_behavior = reducer.behavior_identity(2 * pair_index)
        control_behavior = reducer.behavior_identity(2 * pair_index + 1)
        if primary_behavior == control_behavior and not _truthy(primary.get("allow_behavior_equivalence")):
            blockers.append("control_behavior_identity_equals_primary")
        primary_spread = float(np.max(reducer.stats[2 * pair_index, :, -1]))
        control_spread = float(np.max(reducer.stats[2 * pair_index + 1, :, -1]))
        if primary_spread <= 1e-15:
            blockers.append("primary_signal_empty_or_constant")
        if control_spread <= 1e-15:
            blockers.append("control_signal_empty_or_constant")
        if not isinstance(primary_value, (int, float)) or not math.isfinite(float(primary_value)):
            blockers.append("primary_not_evaluated")
        if not isinstance(control_value, (int, float)) or not math.isfinite(float(control_value)):
            blockers.append("control_not_evaluated")
        matched = None if blockers else float(primary_value) - float(control_value)
        bound_pair = pair_binding[pair_id]
        rows.append(
            {
                "pair_id": pair_id,
                "primary_candidate_id": primary["candidate_id"],
                "control_candidate_id": control["candidate_id"],
                "primary_receipt_hash": member_binding[str(primary["candidate_id"])]["receipt_hash"],
                "control_receipt_hash": member_binding[str(control["candidate_id"])]["receipt_hash"],
                "pair_receipt_hash": bound_pair["pair_receipt_hash"],
                "pair_evaluation_status": "PAIR_EVALUATED" if not blockers else "PAIR_EVALUATION_BLOCKED",
                "pair_evaluation_blockers": "|".join(sorted(blockers)),
                "pair_train_reward": matched,
                "primary_train_reward": primary_value,
                "control_train_reward": control_value,
                "pair_support_count": support_row["count"],
                "pair_support_identity": support_row["support_identity"],
                "pair_support_overlap": 1.0 if int(support_row["count"]) else 0.0,
                "primary_behavior_identity": primary_behavior,
                "control_behavior_identity": control_behavior,
                "primary_signal_spread_max": primary_spread,
                "control_signal_spread_max": control_spread,
                "optimizer_reward": matched,
                "optimizer_reward_split": "train",
                "streaming_identity_schema": "block_composable_v1",
            }
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=("active_bar", "stock_session"), required=True)
    parser.add_argument("--phase", choices=("C", "D", "E"), required=True)
    parser.add_argument("--pair-count", type=int, required=True)
    parser.add_argument("--candidate-table", type=Path, required=True)
    parser.add_argument("--binding", type=Path, required=True)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--field-sidecar-root", type=Path, required=True)
    parser.add_argument("--label-sidecar-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--execution-plan", type=Path)
    parser.add_argument("--block-sessions", type=int, default=5)
    parser.add_argument("--pair-batch-size", type=int, default=4)
    parser.add_argument("--compute-threads", type=int, default=16)
    parser.add_argument("--horizons", default="1,5,15,30")
    parser.add_argument("--min-obs", type=int, default=20)
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--top-quantile", type=float, default=0.2)
    parser.add_argument("--portfolio-mode", default="long_only_top")
    parser.add_argument("--checkpoint-every-blocks", type=int, default=1)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument(
        "--stop-after-blocks",
        type=int,
        default=0,
        help="Phase C/D controlled-stop hook used only to prove checkpoint/resume parity.",
    )
    args = parser.parse_args()

    if int(args.stop_after_blocks) < 0:
        raise ValueError("stop-after-blocks cannot be negative")
    if args.phase == "E" and int(args.stop_after_blocks):
        raise RuntimeError("Phase E forbids adaptive or controlled plan interruption")

    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    binding = _verify_binding(args.binding.resolve(), args.artifact_root.resolve())
    candidates = _candidate_pairs(
        _read_csv(args.candidate_table.resolve()),
        pair_limit=int(args.pair_count),
        clock=str(args.backend),
    )
    selected_ids = {str(row["candidate_id"]) for row in candidates}
    bound_ids = {
        str(row["candidate_id"])
        for row in binding["candidate_members"]
        if str(row["clock_namespace"]) == str(args.backend)
    }
    if not selected_ids <= bound_ids:
        raise RuntimeError("CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: selected members")
    pair_ids = tuple(str(candidates[index]["pair_id"]) for index in range(0, len(candidates), 2))
    field_paths = _sidecar_files(args.field_sidecar_root.resolve())
    label_paths = _sidecar_files(args.label_sidecar_root.resolve())
    symbols = _symbol_registry(field_paths)
    dates = _session_dates(field_paths)
    discovered_boundaries = _block_boundaries(dates, int(args.block_sessions))
    batches = _pair_batches(pair_ids, int(args.pair_batch_size))
    if args.phase == "E":
        if args.execution_plan is None:
            raise ValueError("Phase E requires a pre-frozen execution plan")
        plan = _load_plan(args.execution_plan.resolve())
        if plan.phase != "E":
            raise RuntimeError("Phase E execution plan has the wrong phase")
        if plan.block_boundaries != discovered_boundaries or plan.pair_batches != batches:
            raise RuntimeError("Phase E block or pair-batch boundaries drift")
    else:
        plan = FrozenExecutionPlan.create(
            phase=args.phase,
            block_size=int(args.block_sessions),
            block_boundaries=discovered_boundaries,
            pair_batches=batches,
            heavy_processes=1,
            compute_threads=int(args.compute_threads),
            primary_thread_pool="numba",
            cache_caps={
                "dag_block_cache_bytes": 8 * 1024**3,
                "raw_block_arena_bytes": 6 * 1024**3,
                "temporary_array_bytes": 6 * 1024**3,
            },
            checkpoint_every_blocks=int(args.checkpoint_every_blocks),
            rss_soft_bytes=20 * 1024**3,
            rss_hard_bytes=24 * 1024**3,
            global_rss_hard_bytes=60 * 1024**3,
        )
    validate_frozen_thread_environment(plan)
    _write_json(output_root / "CN_FROZEN_EXECUTION_PLAN.json", plan.to_dict())

    dag_started = time.perf_counter()
    dag_plan = SharedMultiCandidateDAGPlan.build(candidates)
    _write_json(output_root / "CN_SHARED_DAG_PLAN.json", dag_plan.to_dict())
    dag_seconds = time.perf_counter() - dag_started
    timing_path = output_root / "CN_PHASE3CM_PHASE_TIMING.jsonl"
    if not args.resume:
        timing_path.unlink(missing_ok=True)
    telemetry = PhaseTelemetryRecorder(
        output_path=timing_path,
        allocated_compute_threads=plan.compute_threads,
    )
    telemetry._append(
        build_phase_event(
            phase="shared_dag_planning",
            wall_seconds=dag_seconds,
            cpu_seconds=dag_seconds,
            allocated_compute_threads=plan.compute_threads,
            compute_heavy=False,
            rss_before_bytes=0,
            rss_after_bytes=_process_snapshot().rss_bytes,
            peak_rss_bytes=_process_snapshot().peak_rss_bytes,
            dag_node_count=len(dag_plan.nodes),
            value_cohort_count=len(dag_plan.value_cohorts),
            mapping_subcohort_count=len(dag_plan.mapping_subcohorts),
        )
    )

    horizons = tuple(int(value) for value in str(args.horizons).split(",") if value)
    reader = TimeMajorBlockReader(
        field_sidecars=field_paths,
        label_sidecars=label_paths,
        raw_fields=_raw_fields(candidates),
        horizons=horizons,
        symbol_registry=symbols,
    )
    expression = StreamingExpressionExecutor(
        code_count=len(symbols),
        compute_threads=plan.compute_threads,
        cache_max_bytes=plan.cache_caps["dag_block_cache_bytes"],
    )
    portfolio = BatchedPortfolioKernel(
        candidate_count=len(candidates),
        code_count=len(symbols),
        horizons=horizons,
        compute_threads=plan.compute_threads,
        min_obs=int(args.min_obs),
        top_quantile=float(args.top_quantile),
        cost_bps=float(args.cost_bps),
        portfolio_mode=str(args.portfolio_mode),
    )
    reducer = StreamingPortfolioReducer(candidates=candidates, horizons=horizons)
    support = PairSupportAccumulator(pair_ids=pair_ids)
    completed_blocks: list[int] = []
    checkpoint_ordinal = 0
    checkpoint_root = output_root / "checkpoints" / str(args.backend)
    total_rows = 0
    expression_audits: list[dict[str, Any]] = []
    portfolio_audits: list[dict[str, Any]] = []
    prior_evaluator_wall_seconds = 0.0

    if args.resume:
        manifest_path = checkpoint_root / "CN_STREAMING_CHECKPOINT_MANIFEST.json"
        if not manifest_path.is_file():
            raise RuntimeError("resume requested without a complete checkpoint manifest")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        payload = load_checkpoint(
            checkpoint_root / str(manifest["latest_complete_checkpoint"]),
            expected_execution_plan_hash=plan.execution_plan_hash,
            expected_input_binding_hash=str(binding["binding_hash"]),
        )
        expression.restore_continuation_payload(
            {
                "rolling": payload.temporal_continuation_payload,
                "state": payload.state_event_continuation_payload["state"],
            }
        )
        support.restore_continuation_payload(payload.state_event_continuation_payload["support"])
        portfolio.restore_continuation_payload(payload.portfolio_continuation_payload)
        reducer.restore_continuation_payload(payload.streaming_reducer_payload)
        completed_blocks = list(payload.completed_blocks)
        checkpoint_ordinal = int(payload.execution_position.get("checkpoint_ordinal") or 0)
        total_rows = int(payload.execution_position.get("rows_processed") or 0)
        expression_audits = [
            dict(row) for row in payload.execution_position.get("expression_audits") or []
        ]
        portfolio_audits = [
            dict(row) for row in payload.execution_position.get("portfolio_audits") or []
        ]
        prior_evaluator_wall_seconds = float(
            payload.execution_position.get("evaluator_wall_seconds") or 0.0
        )

    runtime_position = {"block_ordinal": -1}

    def checkpoint(reason: str) -> None:
        nonlocal checkpoint_ordinal
        checkpoint_ordinal += 1
        continuation = expression.continuation_payload()
        write_checkpoint(
            checkpoint_root,
            StreamingCheckpointPayload(
                execution_plan_hash=plan.execution_plan_hash,
                input_binding_hash=str(binding["binding_hash"]),
                backend_identity=str(args.backend),
                route_cohort=f"{args.backend}:{args.pair_count}pairs",
                execution_position={
                    "block_ordinal": int(runtime_position["block_ordinal"]),
                    "checkpoint_ordinal": checkpoint_ordinal,
                    "reason": reason,
                    "rows_processed": int(total_rows),
                    "evaluator_wall_seconds": prior_evaluator_wall_seconds
                    + (time.perf_counter() - run_started),
                    "expression_audits": list(expression_audits),
                    "portfolio_audits": list(portfolio_audits),
                },
                completed_blocks=list(completed_blocks),
                completed_pair_batches=[_stable_hash(list(batch)) for batch in plan.pair_batches],
                temporal_continuation_payload=continuation["rolling"],
                state_event_continuation_payload={
                    "state": continuation["state"],
                    "support": support.continuation_payload(),
                },
                portfolio_continuation_payload=portfolio.continuation_payload(),
                streaming_reducer_payload=reducer.continuation_payload(),
            ),
            checkpoint_ordinal=checkpoint_ordinal,
        )

    rss_gate = RSSGate(
        soft_bytes=plan.rss_soft_bytes,
        hard_bytes=plan.rss_hard_bytes,
        global_hard_bytes=plan.global_rss_hard_bytes,
        checkpoint=checkpoint,
    )
    run_started = time.perf_counter()
    for block_ordinal, (start, end) in enumerate(plan.block_boundaries):
        if block_ordinal in completed_blocks:
            continue
        runtime_position["block_ordinal"] = block_ordinal
        with telemetry.phase("global_trade_time_barrier", compute_heavy=False) as phase:
            block = reader.read_block(start_time=pd.Timestamp(start), end_time=pd.Timestamp(end))
            phase.add(
                rows_read=block.row_count,
                blocks_processed=1,
                raw_fields_loaded=len(block.raw_fields),
                portfolio_coordinates_processed=block.row_count,
            )
        total_rows += block.row_count
        with telemetry.phase("expression_value_dag", compute_heavy=True) as phase:
            expression.bind_block(
                raw_fields=block.raw_fields,
                code_ids=block.code_ids,
                time_ids=block.time_ids,
            )
            result_by_expression: dict[str, np.ndarray] = {}
            for batch in plan.pair_batches:
                members = [row for row in candidates if str(row["pair_id"]) in set(batch)]
                expressions = [str(row["expression"]) for row in members]
                namespaces = {
                    str(row["expression"]): "|".join(
                        (
                            str(row.get("support_unit") or ""),
                            str(row.get("outer_mapping") or ""),
                            str(args.portfolio_mode),
                        )
                    )
                    for row in members
                }
                result_by_expression.update(
                    expression.evaluate_many(expressions, mapping_namespaces=namespaces)
                )
            signals = np.vstack(
                [result_by_expression[str(row["expression"])] for row in candidates]
            )
            phase.add(
                dag_nodes_evaluated=expression.audit["value_node_evaluations"]
                + expression.audit["mapping_node_evaluations"],
                dag_reuse_count=expression.audit["cache_hits"],
                signals_materialized=len(candidates),
                temporary_rows_allocated=0,
                cache_peak_bytes=expression.audit.get("cache_peak_bytes", 0),
            )
            expression_audits.append(dict(expression.audit))
        with telemetry.phase("pair_common_support", compute_heavy=False) as phase:
            common_masks = _support_masks(signals)
            support.update(
                common_masks=common_masks,
                trade_times_ns=block.trade_times_ns,
                code_ids=block.code_ids,
                source_shards=block.source_shards,
                source_row_identity=block.source_row_identity,
                duplicate_ordinal=block.duplicate_ordinal,
            )
            phase.add(pair_count=len(pair_ids), support_coordinates=int(common_masks.sum()))
        result = portfolio.evaluate_block(
            signals=signals,
            labels=block.labels,
            time_ids=block.time_ids,
            code_ids=block.code_ids,
            day_ids=block.day_ids,
            directions=np.ones(len(candidates), dtype=np.float64),
            day_count=len(block.day_labels),
        )
        snapshot = _process_snapshot()
        for phase_name, prefix in (
            ("cross_sectional_rank_mapping", "mapping"),
            ("turnover_and_cost", "turnover_cost"),
        ):
            wall = float(result.audit[f"{prefix}_wall_seconds"])
            cpu = float(result.audit[f"{prefix}_cpu_seconds"])
            telemetry._append(
                build_phase_event(
                    phase=phase_name,
                    wall_seconds=wall,
                    cpu_seconds=cpu,
                    allocated_compute_threads=plan.compute_threads,
                    compute_heavy=True,
                    rss_before_bytes=snapshot.rss_bytes,
                    rss_after_bytes=snapshot.rss_bytes,
                    peak_rss_bytes=snapshot.peak_rss_bytes,
                    blocks_processed=1,
                    portfolio_coordinates_processed=block.row_count * len(candidates),
                    coordinate_rows_retained=0,
                )
            )
        portfolio_audits.append(dict(result.audit))
        with telemetry.phase("streaming_reducer", compute_heavy=False) as phase:
            reducer.update(result, day_labels=block.day_labels)
            phase.add(blocks_processed=1, coordinate_rows_retained=0, reducer_bytes=reducer.stats.nbytes)
        completed_blocks.append(block_ordinal)
        with telemetry.phase("checkpoint", compute_heavy=False) as phase:
            rss_gate.periodic_checkpoint(
                block_ordinal=block_ordinal + 1,
                cadence=plan.checkpoint_every_blocks,
            )
            phase.add(blocks_processed=1, checkpoint_ordinal=checkpoint_ordinal)
        rss_gate.check(_process_snapshot(), global_rss_bytes=_process_snapshot().rss_bytes)
        if int(args.stop_after_blocks) and len(completed_blocks) >= int(args.stop_after_blocks):
            checkpoint("CONTROLLED_RESUME_PARITY_PAUSE")
            pause_payload = {
                "schema_version": "cn_phase3cm_streaming_pause_receipt_v1",
                "status": "CN_PHASE3CM_STREAMING_BACKEND_PAUSED_RECOVERABLE",
                "backend": args.backend,
                "phase": args.phase,
                "completed_blocks": list(completed_blocks),
                "rows_processed": int(total_rows),
                "evaluator_wall_seconds": prior_evaluator_wall_seconds
                + (time.perf_counter() - run_started),
                "checkpoint_ordinal": int(checkpoint_ordinal),
                "execution_plan_hash": plan.execution_plan_hash,
                "input_binding_hash": binding["binding_hash"],
                "validation_reads": 0,
                "holdout_reads": 0,
                "forward_2026_reads": 0,
                "promotion": "FORBIDDEN",
                "strict_stage_a": "NOT_AUTHORIZED",
            }
            _write_json(output_root / "CN_STREAMING_PAUSE_RECEIPT.json", pause_payload)
            print(json.dumps(pause_payload, sort_keys=True))
            return 0

    with telemetry.phase("candidate_pair_finalization", compute_heavy=False) as phase:
        atom_rows = reducer.reward_atoms()
        reward_rows = []
        split_rows = []
        for candidate_index, candidate in enumerate(candidates):
            candidate_atoms = [
                row for row in atom_rows if str(row.get("candidate_id")) == str(candidate["candidate_id"])
            ]
            per_split, reward = _candidate_summary_from_reward_atoms(
                dict(candidate),
                candidate_atoms,
                horizons,
                seed=20260623 + candidate_index,
                rank_ic_loss_weight=6.0,
                rank_ic_component_cap=0.35,
                regime_stability_weight=0.08,
                regime_component_cap=0.10,
            )
            split_rows.extend(per_split)
            reward_rows.append(reward)
        pair_rows = _finalize_pairs(
            candidates=candidates,
            reward_rows=reward_rows,
            reducer=reducer,
            support=support,
            binding=binding,
        )
        phase.add(candidate_count=len(candidates), pair_count=len(pair_rows))

    reward_atom_artifact = _write_csv(
        output_root / "CN_STREAMING_REWARD_ATOMS.csv",
        atom_rows,
    )

    events = [json.loads(line) for line in timing_path.read_text(encoding="utf-8").splitlines() if line]
    phase_totals: dict[str, dict[str, float]] = {}
    for event in events:
        item = phase_totals.setdefault(str(event["phase"]), {"wall_seconds": 0.0, "cpu_seconds": 0.0})
        item["wall_seconds"] += float(event.get("wall_seconds") or 0.0)
        item["cpu_seconds"] += float(event.get("cpu_seconds") or 0.0)
    compute_total = sum(
        phase_totals.get(name, {}).get("wall_seconds", 0.0)
        for name in ("expression_value_dag", "cross_sectional_rank_mapping", "turnover_and_cost")
    )
    rank_mapping = phase_totals.get("cross_sectional_rank_mapping", {}).get("wall_seconds", 0.0)
    value_dag = phase_totals.get("expression_value_dag", {}).get("wall_seconds", 0.0)
    if compute_total and rank_mapping / compute_total >= 0.60:
        bottleneck = "BATCHED_PORTFOLIO_KERNEL_BOTTLENECK"
    elif compute_total and value_dag / compute_total >= 0.60:
        bottleneck = "VALUE_DAG_BOTTLENECK"
    else:
        bottleneck = "MIXED_COMPUTE_BOTTLENECK"
    compute_events = [event for event in events if bool(event.get("compute_heavy"))]
    parallelism_status = (
        "PARALLELISM_NOT_ENGAGED"
        if any(event.get("parallelism_status") == "PARALLELISM_NOT_ENGAGED" for event in compute_events)
        else "PARALLELISM_ENGAGED"
    )
    evaluator_wall = prior_evaluator_wall_seconds + (time.perf_counter() - run_started)
    required_timing_phases = {
        "global_trade_time_barrier",
        "expression_value_dag",
        "pair_common_support",
        "cross_sectional_rank_mapping",
        "turnover_and_cost",
        "streaming_reducer",
        "checkpoint",
        "candidate_pair_finalization",
    }
    timing_coverage = len(required_timing_phases & set(phase_totals)) / len(required_timing_phases)
    phase_c_gates = {
        "wall_time_lte_3600": evaluator_wall <= 3600.0,
        "peak_rss_lte_24gb": max((int(event.get("peak_rss_bytes") or 0) for event in events), default=0)
        <= 24 * 1024**3,
        "coordinate_rows_retained_zero": reducer.coordinate_rows_retained == 0,
        "phase_timing_coverage_100pct": timing_coverage == 1.0,
        "sealed_reads_zero": True,
        "parallelism_engaged": parallelism_status == "PARALLELISM_ENGAGED",
    }
    result_payload = {
        "schema_version": "cn_phase3cm_streaming_backend_result_v1",
        "status": "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED",
        "backend": args.backend,
        "phase": args.phase,
        "pair_count": len(pair_ids),
        "candidate_count": len(candidates),
        "rows_processed": total_rows,
        "blocks_processed": len(completed_blocks),
        "wall_seconds": evaluator_wall,
        "telemetry_write_wall_seconds": telemetry.telemetry_write_wall_seconds,
        "telemetry_overhead_ratio_upper_bound": telemetry.telemetry_write_wall_seconds
        / max(1e-12, time.perf_counter() - run_started),
        "peak_rss_bytes": max((int(event.get("peak_rss_bytes") or 0) for event in events), default=0),
        "execution_plan_hash": plan.execution_plan_hash,
        "input_binding_hash": binding["binding_hash"],
        "dag_plan_hash": dag_plan.plan_hash,
        "coordinate_rows_retained": reducer.coordinate_rows_retained,
        "parallelism_status": parallelism_status,
        "hot_path_bottleneck": bottleneck,
        "phase_timing_coverage": timing_coverage,
        "phase_c_gates": phase_c_gates,
        "phase_c_gate_status": (
            "SINGLE_PAIR_FULL_COORDINATE_GATE_PASS"
            if args.phase == "C" and all(phase_c_gates.values())
            else "NOT_APPLICABLE"
            if args.phase != "C"
            else "SINGLE_PAIR_FULL_COORDINATE_GATE_FAIL"
        ),
        "phase_totals": phase_totals,
        "support_identities": support.identities(),
        "candidate_rewards": reward_rows,
        "pair_results": pair_rows,
        "reward_atoms": reward_atom_artifact,
        "split_rows": split_rows,
        "expression_audits": expression_audits,
        "portfolio_audits": portfolio_audits,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
    }
    _write_json(output_root / "CN_STREAMING_BACKEND_RESULT.json", result_payload)
    _write_json(
        output_root / "CN_SHARED_DAG_EXECUTION_AUDIT.json",
        {
            "dag_plan_hash": dag_plan.plan_hash,
            "blocks": expression_audits,
            "actual_evaluation_count": sum(
                int(row.get("value_node_evaluations") or 0) + int(row.get("mapping_node_evaluations") or 0)
                for row in expression_audits
            ),
            "reuse_count": sum(int(row.get("cache_hits") or 0) for row in expression_audits),
        },
    )
    _write_json(output_root / "CN_STREAMING_REDUCER_CONTRACT.json", reducer.contract())
    print(
        json.dumps(
            {
                "status": result_payload["status"],
                "backend": args.backend,
                "pairs": len(pair_ids),
                "rows": total_rows,
                "wall_seconds": result_payload["wall_seconds"],
                "peak_rss_bytes": result_payload["peak_rss_bytes"],
                "parallelism_status": parallelism_status,
                "bottleneck": bottleneck,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
