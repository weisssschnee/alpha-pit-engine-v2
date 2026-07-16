"""Low-intrusion timers around the receipt-frozen legacy Phase3CM evaluator.

The legacy evaluator file is deliberately not edited: its byte identity is part
of the frozen candidate receipts.  This wrapper replaces selected module
symbols only after import and writes timing evidence from a watchdog before the
controlled Phase-0 stop.
"""

from __future__ import annotations

import argparse
import functools
import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Callable


class PhaseTimer:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.rows: dict[str, dict[str, float | int]] = {}
        self.stacks: dict[int, list[dict[str, Any]]] = {}

    def wrap(self, phase: str, function: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(function)
        def measured(*args: Any, **kwargs: Any) -> Any:
            thread_id = threading.get_ident()
            frame = {
                "phase": phase,
                "wall_start": time.perf_counter(),
                "cpu_start": time.process_time(),
                "child_wall": 0.0,
                "child_cpu": 0.0,
            }
            with self.lock:
                self.stacks.setdefault(thread_id, []).append(frame)
            try:
                return function(*args, **kwargs)
            finally:
                wall = time.perf_counter() - frame["wall_start"]
                cpu = time.process_time() - frame["cpu_start"]
                with self.lock:
                    stack = self.stacks[thread_id]
                    if stack and stack[-1] is frame:
                        stack.pop()
                    exclusive_wall = max(0.0, wall - frame["child_wall"])
                    exclusive_cpu = max(0.0, cpu - frame["child_cpu"])
                    row = self.rows.setdefault(phase, {"calls": 0, "wall_seconds": 0.0, "cpu_seconds": 0.0})
                    row["calls"] = int(row["calls"]) + 1
                    row["wall_seconds"] = float(row["wall_seconds"]) + exclusive_wall
                    row["cpu_seconds"] = float(row["cpu_seconds"]) + exclusive_cpu
                    if stack:
                        stack[-1]["child_wall"] += wall
                        stack[-1]["child_cpu"] += cpu

        return measured

    def snapshot(self) -> dict[str, dict[str, float | int]]:
        now_wall = time.perf_counter()
        now_cpu = time.process_time()
        with self.lock:
            output = {phase: dict(row) for phase, row in self.rows.items()}
            for stack in self.stacks.values():
                if not stack:
                    continue
                active = stack[-1]
                row = output.setdefault(
                    str(active["phase"]),
                    {"calls": 0, "wall_seconds": 0.0, "cpu_seconds": 0.0},
                )
                row["active_at_stop"] = 1
                row["wall_seconds"] = float(row["wall_seconds"]) + max(
                    0.0, now_wall - float(active["wall_start"])
                )
                row["cpu_seconds"] = float(row["cpu_seconds"]) + max(
                    0.0, now_cpu - float(active["cpu_start"])
                )
            return output


def _write_snapshot(path: Path, timer: PhaseTimer, *, status: str) -> None:
    rows = timer.snapshot()
    total_compute = sum(float(row.get("cpu_seconds", 0.0)) for row in rows.values())
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for phase in sorted(rows):
            row = {
                "phase": phase,
                **rows[phase],
                "compute_share": (
                    float(rows[phase].get("cpu_seconds", 0.0)) / total_compute
                    if total_compute > 0
                    else 0.0
                ),
                "status": status,
                "evaluator": "legacy_reference_receipt_frozen",
            }
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--instrumentation-output", type=Path, required=True)
    parser.add_argument("--instrumentation-seconds", type=int, default=60)
    parser.add_argument("legacy_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    legacy_args = list(args.legacy_args)
    if legacy_args and legacy_args[0] == "--":
        legacy_args = legacy_args[1:]

    from our_system_phase2.runtime import phase3cm_train_portfolio_sortino_reward_audit as legacy

    timer = PhaseTimer()
    phase_functions = {
        "panel_discovery": "_discover_panels",
        "panel_read": "_read_windowed_panel",
        "panel_read_positions": "_read_windowed_panel_positions",
        "normalization_orchestration": "_read_train_shard",
        "label_preparation": "_future_returns",
        "global_trade_time_index": "_build_eval_time_index",
        "expression_value_dag": "evaluate_panel_expression",
        "pair_common_support": "_pair_common_finite_mask",
        "cross_sectional_rank": "_rank_by_eval_time_index",
        "portfolio_mapping_turnover_cost": "_candidate_portfolio_rows_from_precomputed_time_groups",
        "streaming_reducer_reference": "_reward_atoms_for_candidate",
        "candidate_finalization": "_candidate_summary_from_reward_atoms",
    }
    for phase, name in phase_functions.items():
        function = getattr(legacy, name, None)
        if function is not None:
            setattr(legacy, name, timer.wrap(phase, function))

    def stop() -> None:
        _write_snapshot(
            args.instrumentation_output,
            timer,
            status="CONTROLLED_STOP_AT_PHASE0_GATE",
        )
        os._exit(124)

    watchdog = threading.Timer(max(10, int(args.instrumentation_seconds)), stop)
    watchdog.daemon = True
    watchdog.start()
    try:
        result = int(legacy.main(legacy_args) or 0)
    finally:
        watchdog.cancel()
        _write_snapshot(args.instrumentation_output, timer, status="LEGACY_EVALUATOR_COMPLETED")
    return result


if __name__ == "__main__":
    raise SystemExit(main())
