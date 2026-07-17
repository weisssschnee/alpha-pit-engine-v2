from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Mapping

import numpy as np

from our_system_phase2.services.phase3cm_streaming_checkpoint import load_checkpoint


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _compare(left: Any, right: Any, *, path: str = "root") -> list[str]:
    if isinstance(left, np.ndarray) or isinstance(right, np.ndarray):
        left_array = np.asarray(left)
        right_array = np.asarray(right)
        if left_array.shape != right_array.shape or left_array.dtype != right_array.dtype:
            return [f"{path}:array_metadata"]
        if not np.array_equal(left_array, right_array, equal_nan=True):
            return [f"{path}:array_values"]
        return []
    if isinstance(left, Mapping) or isinstance(right, Mapping):
        if not isinstance(left, Mapping) or not isinstance(right, Mapping):
            return [f"{path}:type"]
        if set(left) != set(right):
            return [f"{path}:keys"]
        mismatches: list[str] = []
        for key in sorted(left, key=str):
            mismatches.extend(_compare(left[key], right[key], path=f"{path}.{key}"))
        return mismatches
    if isinstance(left, (list, tuple)) or isinstance(right, (list, tuple)):
        if not isinstance(left, (list, tuple)) or not isinstance(right, (list, tuple)):
            return [f"{path}:type"]
        if len(left) != len(right):
            return [f"{path}:length"]
        mismatches: list[str] = []
        for index, (left_item, right_item) in enumerate(zip(left, right)):
            mismatches.extend(
                _compare(left_item, right_item, path=f"{path}[{index}]")
            )
        return mismatches
    if isinstance(left, float) and isinstance(right, float) and math.isnan(left) and math.isnan(right):
        return []
    return [] if left == right else [f"{path}:value"]


def normalize_portfolio_continuation(payload: Mapping[str, Any]) -> dict[int, dict[str, np.ndarray]]:
    if str(payload.get("schema_version")) != "cn_phase3cm_batched_portfolio_continuation_v1":
        raise ValueError("portfolio continuation schema drift")
    output: dict[int, dict[str, np.ndarray]] = {}
    for batch in payload.get("batches") or []:
        indices = [int(value) for value in batch.get("candidate_indices") or []]
        state = batch["payload"]
        selection = np.asarray(state["selection_epoch"])
        counters = np.asarray(state["epoch_counter"])
        if selection.shape[0] != len(indices) or counters.shape[0] != len(indices):
            raise ValueError("portfolio continuation candidate axis drift")
        for local_index, candidate_index in enumerate(indices):
            if candidate_index in output:
                raise ValueError("portfolio continuation candidate repeated across batches")
            output[candidate_index] = {
                "selection_epoch": selection[local_index],
                "epoch_counter": counters[local_index],
            }
    return output


def _load_probe(root: Path, backend: str) -> dict[str, Any]:
    plan = _read_json(root / "CN_FROZEN_EXECUTION_PLAN.json")
    checkpoint_root = root / "checkpoints" / backend
    manifest = _read_json(checkpoint_root / "CN_STREAMING_CHECKPOINT_MANIFEST.json")
    checkpoint = checkpoint_root / str(manifest["latest_complete_checkpoint"])
    payload = load_checkpoint(
        checkpoint,
        expected_execution_plan_hash=str(plan["execution_plan_hash"]),
        expected_input_binding_hash=str(manifest["record"]["input_binding_hash"]),
    )
    events = [
        json.loads(line)
        for line in (root / "CN_PHASE3CM_PHASE_TIMING.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
        if line
    ]
    phase_totals: dict[str, dict[str, float]] = {}
    for event in events:
        phase = str(event["phase"])
        total = phase_totals.setdefault(phase, {"wall_seconds": 0.0, "cpu_seconds": 0.0})
        total["wall_seconds"] += float(event.get("wall_seconds") or 0.0)
        total["cpu_seconds"] += float(event.get("cpu_seconds") or 0.0)
    return {
        "plan": plan,
        "manifest": manifest,
        "payload": payload,
        "phase_totals": phase_totals,
        "peak_rss_bytes": max((int(row.get("peak_rss_bytes") or 0) for row in events), default=0),
        "parallelism_not_engaged_events": sum(
            1
            for row in events
            if row.get("parallelism_status") == "PARALLELISM_NOT_ENGAGED"
        ),
    }


def compare_probes(reference: Mapping[str, Any], candidate: Mapping[str, Any]) -> dict[str, Any]:
    reference_payload = reference["payload"]
    candidate_payload = candidate["payload"]
    payloads = {
        "temporal": (
            reference_payload.temporal_continuation_payload,
            candidate_payload.temporal_continuation_payload,
        ),
        "state": (
            reference_payload.state_event_continuation_payload["state"],
            candidate_payload.state_event_continuation_payload["state"],
        ),
        "support": (
            reference_payload.state_event_continuation_payload["support"],
            candidate_payload.state_event_continuation_payload["support"],
        ),
        "portfolio": (
            normalize_portfolio_continuation(
                reference_payload.portfolio_continuation_payload
            ),
            normalize_portfolio_continuation(
                candidate_payload.portfolio_continuation_payload
            ),
        ),
        "reducer": (
            reference_payload.streaming_reducer_payload,
            candidate_payload.streaming_reducer_payload,
        ),
    }
    parity = {
        name: {
            "exact": not (mismatches := _compare(left, right, path=name)),
            "mismatches": mismatches[:20],
        }
        for name, (left, right) in payloads.items()
    }
    reference_mapping = float(
        reference["phase_totals"].get("cross_sectional_rank_mapping", {}).get(
            "wall_seconds", 0.0
        )
    )
    candidate_mapping = float(
        candidate["phase_totals"].get("cross_sectional_rank_mapping", {}).get(
            "wall_seconds", 0.0
        )
    )
    reference_compute = sum(
        float(reference["phase_totals"].get(name, {}).get("wall_seconds", 0.0))
        for name in ("expression_value_dag", "cross_sectional_rank_mapping", "turnover_and_cost")
    )
    candidate_compute = sum(
        float(candidate["phase_totals"].get(name, {}).get("wall_seconds", 0.0))
        for name in ("expression_value_dag", "cross_sectional_rank_mapping", "turnover_and_cost")
    )
    all_exact = all(row["exact"] for row in parity.values())
    parallelism_engaged = int(candidate["parallelism_not_engaged_events"]) == 0
    return {
        "status": (
            "CN_PHASE3CM_SCALING_PROBE_PARITY_PASS"
            if all_exact and parallelism_engaged
            else "CN_PHASE3CM_SCALING_PROBE_FAIL_CLOSED"
        ),
        "parity": parity,
        "reference": {
            "compute_threads": reference["plan"]["compute_threads"],
            "pair_batch_size": max(map(len, reference["plan"]["pair_batches"])),
            "mapping_wall_seconds": reference_mapping,
            "compute_wall_seconds": reference_compute,
            "peak_rss_bytes": reference["peak_rss_bytes"],
        },
        "candidate": {
            "compute_threads": candidate["plan"]["compute_threads"],
            "pair_batch_size": max(map(len, candidate["plan"]["pair_batches"])),
            "mapping_wall_seconds": candidate_mapping,
            "compute_wall_seconds": candidate_compute,
            "peak_rss_bytes": candidate["peak_rss_bytes"],
            "parallelism_not_engaged_events": candidate[
                "parallelism_not_engaged_events"
            ],
        },
        "mapping_speedup": reference_mapping / candidate_mapping
        if candidate_mapping
        else None,
        "compute_speedup": reference_compute / candidate_compute
        if candidate_compute
        else None,
        "recommended_for_phase_e_freeze": all_exact
        and parallelism_engaged
        and candidate_compute < reference_compute,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-root", type=Path, required=True)
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--backend", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    reference = _load_probe(args.reference_root, args.backend)
    candidate = _load_probe(args.candidate_root, args.backend)
    result = compare_probes(reference, candidate)
    result["backend"] = args.backend
    result["data_role"] = "development_train_only"
    result["validation_reads"] = 0
    result["holdout_reads"] = 0
    result["forward_2026_reads"] = 0
    _write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "CN_PHASE3CM_SCALING_PROBE_PARITY_PASS" else 3


if __name__ == "__main__":
    raise SystemExit(main())
