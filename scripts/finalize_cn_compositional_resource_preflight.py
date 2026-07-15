"""Close the bounded compositional search when full-coordinate capacity fails.

This command consumes only already-produced development artifacts.  It does not
run a search, select candidates, or read validation, holdout, or forward data.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import subprocess
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd


FINAL_STATUS = "CN_COMPOSITIONAL_SEARCH_COMPUTE_BOTTLENECK"
PREFLIGHT_STATUS = "COMPUTE_OR_MATERIALIZATION_BOTTLENECK"


def _json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def safe_worker_count(
    *,
    total_memory_bytes: int,
    observed_peak_bytes: int,
    logical_processors: int,
    reserve_memory_bytes: int = 20 * 1024**3,
    worker_cap: int = 16,
) -> int:
    usable = max(0, int(total_memory_bytes) - int(reserve_memory_bytes))
    memory_workers = usable // max(1, int(observed_peak_bytes))
    return max(1, min(int(worker_cap), int(logical_processors), int(memory_workers)))


def linear_capacity_projection(
    *,
    one_pair_wall_seconds: float,
    active_pairs: int,
    workers: int,
) -> dict[str, float | int]:
    seconds = float(one_pair_wall_seconds) * int(active_pairs) / max(1, int(workers))
    return {
        "active_pairs": int(active_pairs),
        "workers": int(workers),
        "linear_wall_seconds": round(seconds, 3),
        "linear_wall_hours": round(seconds / 3600.0, 3),
        "linear_wall_days": round(seconds / 86400.0, 3),
    }


def summarize_pair_rows(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    materialized = [dict(row) for row in rows]
    by_route: dict[str, Counter[str]] = defaultdict(Counter)
    matched_clusters: set[str] = set()
    matched_pairs = 0
    for row in materialized:
        route = str(row.get("route_id") or "UNKNOWN")
        status = str(row.get("pair_evaluation_status") or "UNKNOWN")
        by_route[route]["pair_rows"] += 1
        by_route[route]["evaluated"] += int(status == "PAIR_EVALUATED")
        by_route[route]["blocked"] += int(status != "PAIR_EVALUATED")
        decision = str(row.get("pair_train_reward_decision") or "")
        if decision == "PAIR_TRAIN_FEEDBACK_READY":
            matched_pairs += 1
            by_route[route]["diagnostic_matched_positive"] += 1
            identity = str(row.get("primary_behavior_identity") or row.get("pair_id") or "")
            if identity:
                matched_clusters.add(identity)
    return {
        "pair_rows": len(materialized),
        "evaluated_pairs": sum(1 for row in materialized if row.get("pair_evaluation_status") == "PAIR_EVALUATED"),
        "blocked_pairs": sum(1 for row in materialized if row.get("pair_evaluation_status") != "PAIR_EVALUATED"),
        "diagnostic_matched_positive_pairs": matched_pairs,
        "diagnostic_matched_positive_behavior_identities": len(matched_clusters),
        "by_route": {route: dict(sorted(counts.items())) for route, counts in sorted(by_route.items())},
    }


def _git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=repo, text=True).strip()


def _route_failure_markdown(summary: Mapping[str, Any]) -> str:
    lines = [
        "| Route | Preflight pairs | Evaluated | Blocked | Primary attribution |",
        "|---|---:|---:|---:|---|",
    ]
    for route, counts in summary["by_route"].items():
        evaluated = int(counts.get("evaluated", 0))
        total = int(counts.get("pair_rows", 0))
        blocked = int(counts.get("blocked", 0))
        attribution = "COMPUTE_OR_IO_BOTTLENECK" if evaluated else "SUPPORT_OR_MATERIALIZATION_DIAGNOSTIC"
        lines.append(f"| {route} | {total} | {evaluated} | {blocked} | {attribution} |")
    return "\n".join(lines)


def _artifact_record(path: Path, base: Path) -> dict[str, Any]:
    return {
        "path": str(path.relative_to(base)).replace("\\", "/"),
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--report-root", type=Path, required=True)
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--total-memory-bytes", type=int, required=True)
    parser.add_argument("--logical-processors", type=int, required=True)
    args = parser.parse_args()

    repo = args.repo.resolve()
    runtime = args.runtime_root.resolve()
    reports = args.report_root.resolve()
    reports.mkdir(parents=True, exist_ok=True)
    preflight = runtime / "resource_preflight"

    generation = _json(runtime / "CN_GENERATION_EPOCH_SUMMARY.json")
    signal = _json(runtime / "CN_SIGNAL_SKETCH_DIAGNOSTIC.json")
    behavior = _json(runtime / "CN_BEHAVIOR_CLUSTERS.json")
    freeze = _json(runtime / "CN_RESOURCE_PREFLIGHT_FREEZE.json")
    active_summary = _json(preflight / "active_full/phase3cm_train_reward_audit_summary.json")
    session_summary = _json(preflight / "session_full_v2/phase3cm_train_reward_audit_summary.json")
    calibration_abort = _json(preflight / "active_full_coordinate_abort.json")
    active_peak = _json(preflight / "active_peak_monitor.json")
    active_metrics = _json(preflight / "active_resource_metrics.json")
    session_peak = _json(preflight / "session_peak_monitor_v2.json")

    active_rows = _csv(preflight / "active_full/phase3cm_candidate_pair_evaluation.csv")
    session_rows = _csv(preflight / "session_full_v2/phase3cm_candidate_pair_evaluation.csv")
    pair_rows = active_rows + session_rows
    pair_summary = summarize_pair_rows(pair_rows)
    for row in pair_rows:
        row["evaluation_scope"] = "RESOURCE_PREFLIGHT_DIAGNOSTIC_ONLY"
        row["strict_stage_a_executed"] = False
    pd.DataFrame(pair_rows).to_parquet(runtime / "CN_STRICT_PAIR_RESULTS.parquet", index=False)

    waterfall = pd.read_csv(runtime / "CN_ADMISSION_WATERFALL.csv")
    waterfall["diversity_admitted"] = 0
    waterfall["resource_preflight_pairs"] = 0
    waterfall["resource_preflight_evaluated"] = 0
    waterfall["resource_preflight_blocked"] = 0
    waterfall["resource_preflight_diagnostic_matched_positive"] = 0
    for index, row in waterfall.iterrows():
        route = str(row["route_id"])
        behavior_route = dict(behavior.get("route_metrics", {}).get(route, {}))
        preflight_route = dict(pair_summary["by_route"].get(route, {}))
        waterfall.at[index, "behavior_unique"] = int(behavior_route.get("exact_behavior_identity_count", 0))
        waterfall.at[index, "diversity_admitted"] = int(behavior_route.get("admitted_pair_count", 0))
        waterfall.at[index, "resource_preflight_pairs"] = int(preflight_route.get("pair_rows", 0))
        waterfall.at[index, "resource_preflight_evaluated"] = int(preflight_route.get("evaluated", 0))
        waterfall.at[index, "resource_preflight_blocked"] = int(preflight_route.get("blocked", 0))
        waterfall.at[index, "resource_preflight_diagnostic_matched_positive"] = int(
            preflight_route.get("diagnostic_matched_positive", 0)
        )
    waterfall.to_csv(runtime / "CN_ADMISSION_WATERFALL.csv", index=False)

    peak_bytes = int(calibration_abort["peak_working_set_bytes"])
    calibration_wall_seconds = float(calibration_abort["wall_time_lower_bound_seconds"])
    workers = safe_worker_count(
        total_memory_bytes=args.total_memory_bytes,
        observed_peak_bytes=peak_bytes,
        logical_processors=args.logical_processors,
    )
    min_active_pairs = math.ceil(2048 * (2336 / 4096))
    projections = {
        "minimum_2048_pair_run_active_share": linear_capacity_projection(
            one_pair_wall_seconds=calibration_wall_seconds,
            active_pairs=min_active_pairs,
            workers=workers,
        ),
        "stage_a_active_share": linear_capacity_projection(
            one_pair_wall_seconds=calibration_wall_seconds,
            active_pairs=2336,
            workers=workers,
        ),
        "method": "ONE_PAIR_FULL_COORDINATE_RESOURCE_GATE_LOWER_BOUND_LINEAR_PROJECTION",
        "caveat": "The one-pair calibration did not finish before the 120-minute gate, so projected wall times are lower bounds. Shared reads can reduce wall time, but current unbounded full-series expression caching prevents assuming that economy at 2048-pair scale.",
    }

    resource = {
        "status": PREFLIGHT_STATUS,
        "final_search_status": FINAL_STATUS,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "data_role": "development_train_only",
        "validation_accessed": False,
        "holdout_accessed": False,
        "forward_2026_accessed": False,
        "preflight": {
            "frozen_pairs": int(freeze["pair_count"]),
            "frozen_evaluator_calls": int(freeze["evaluator_call_count"]),
            "pair_results": pair_summary,
            "active_sampled": {
                "pairs": int(active_summary["candidate_pair_count"]),
                "evaluated": int(active_summary["pair_evaluated_count"]),
                "blocked": int(active_summary["pair_blocked_count"]),
                "rows": int(active_summary["split_audit"]["row_count"]),
                "sample_trade_times_per_shard": int(active_summary["sample_trade_times_per_shard"]),
                "wall_time_seconds": float(active_metrics["wall_time_seconds"]),
                "peak_working_set_bytes": int(active_peak["peak_working_set_bytes"]),
            },
            "session_full": {
                "pairs": int(session_summary["candidate_pair_count"]),
                "evaluated": int(session_summary["pair_evaluated_count"]),
                "blocked": int(session_summary["pair_blocked_count"]),
                "rows": int(session_summary["split_audit"]["row_count"]),
                "sample_trade_times_per_shard": int(session_summary["sample_trade_times_per_shard"]),
                "wall_time_seconds": float(session_peak["monitor_seconds"]),
                "peak_working_set_bytes": int(session_peak["peak_working_set_bytes"]),
            },
        },
        "full_coordinate_calibration": {
            "status": str(calibration_abort["status"]),
            "reason": str(calibration_abort["reason"]),
            "pairs_requested": int(calibration_abort["candidate_pairs_requested"]),
            "evaluated": 0,
            "rows_completed": None,
            "development_release_rows": int(calibration_abort["development_release_rows"]),
            "sample_trade_times_per_shard": int(calibration_abort["sample_trade_times_per_shard"]),
            "wall_time_lower_bound_seconds": calibration_wall_seconds,
            "peak_working_set_bytes": peak_bytes,
            "read_transfer_bytes": int(calibration_abort["read_transfer_bytes"]),
            "write_transfer_bytes": int(calibration_abort["write_transfer_bytes"]),
            "cpu_seconds": float(calibration_abort["cpu_seconds"]),
            "selected_shards_requested": int(calibration_abort["selected_shards_requested"]),
            "command_line": str(calibration_abort["command_line"]),
            "phase_timing": {
                "materialization_time_seconds": None,
                "evaluator_time_seconds": None,
                "status": "UNAVAILABLE_IN_CURRENT_PHASE3CM_TELEMETRY",
                "remediation": "Add separate panel-read, expression-materialization, portfolio and bootstrap timers before repeating preflight.",
            },
        },
        "host": {
            "total_memory_bytes": int(args.total_memory_bytes),
            "logical_processors": int(args.logical_processors),
            "safe_worker_count": workers,
            "reserve_memory_bytes": 20 * 1024**3,
        },
        "projections": projections,
        "bottleneck": {
            "category": "COMPUTE_OR_IO_BOTTLENECK",
            "primary": "Full-coordinate minute materialization and full-series expression caching do not qualify for the minimum 2048-pair budget.",
            "required_fix": "Bounded streaming multi-candidate DAG materialization with reusable field blocks and bounded expression caches, followed by a repeated full-coordinate representative preflight.",
        },
        "strict_stage_a": "NOT_EXECUTED_COMPUTE_BOTTLENECK",
        "diagnostic_positive_results_are_alpha_claims": False,
    }
    _write_json(runtime / "CN_RESOURCE_PREFLIGHT.json", resource)

    cross_seed = {
        "status": "NOT_EXECUTED_COMPUTE_BOTTLENECK",
        "strict_stage_a_pairs": 0,
        "cross_seed_reproduced_clusters": 0,
        "preflight_results_eligible_for_reproduction_claim": False,
    }
    _write_json(runtime / "CN_CROSS_SEED_REPRODUCTION.json", cross_seed)
    policy_audit = {
        "status": "GENERATION_AND_ADMISSION_OBSERVED_STRICT_COMPARISON_NOT_EXECUTED",
        "generation_policy_exact_unique_counts": generation["policy_exact_unique_counts"],
        "preflight_policy_counts": freeze["policy_counts"],
        "strict_policy_lift": None,
        "search_policy_collapse_claim": "NOT_EVALUATED_AT_STRICT_SCALE",
    }
    _write_json(runtime / "CN_POLICY_BEHAVIOR_AUDIT.json", policy_audit)

    decision = {
        "status": FINAL_STATUS,
        "preflight_status": PREFLIGHT_STATUS,
        "source_sha": _git(repo, "rev-parse", "HEAD"),
        "closure_sha": "4cbb228cedabd93dc1b98bcce17ec11296b68860",
        "proposal_attempts": int(generation["proposal_attempts"]),
        "legal_exact_unique": int(generation["legal_exact_unique_primaries"]),
        "behavior_unique": int(behavior["overall"]["exact_behavior_identity_count"]),
        "behavior_clusters": int(behavior["overall"]["cluster_count"]),
        "strict_stage_a_pairs": 0,
        "strict_evaluator_calls": 0,
        "resource_preflight_pairs": int(freeze["pair_count"]),
        "resource_preflight_evaluator_calls": int(freeze["evaluator_call_count"]),
        "matched_positive_clusters": 0,
        "cross_seed_reproduced_clusters": 0,
        "sealed_reads": {"validation": 0, "holdout": 0, "forward_2026": 0},
        "promotion": "FORBIDDEN",
        "cross_sprint_memory": "FORBIDDEN",
        "why_not_no_alpha": "Strict Stage A was not executed; preflight diagnostic rewards cannot support an Alpha or no-Alpha conclusion.",
    }
    _write_json(reports / "CN_COMPOSITIONAL_SEARCH_DECISION.json", decision)

    route_table = _route_failure_markdown(pair_summary)
    report = f"""# CN Compositional N-line Bounded Search Epoch 1

## Outcome

`{FINAL_STATUS}`

The expression system materially expanded the structural and signal hypothesis space, but the current minute evaluator did not qualify for the minimum 2,048-pair full-coordinate budget. Strict Stage A was therefore not started. This is an engineering capacity conclusion, not a no-Alpha conclusion.

## Delivered research results

- Proposal attempts: {generation['proposal_attempts']:,}; legal exact-unique primaries: {generation['legal_exact_unique_primaries']:,} (natural underfill against the 30,000 target).
- Structural pre-admission: {generation['structural_preadmission_pairs']:,}; diversity admission: 12,000 pairs.
- Development behavior audit: {behavior['overall']['exact_behavior_identity_count']:,} exact behaviors, {behavior['overall']['cluster_count']:,} clusters, N_eff={behavior['overall']['n_eff']:.3f}, top-1={behavior['overall']['top1_share']:.3%}.
- A/B sketch stability and exact-fidelity gates passed for active={signal['clock_summaries']['active_bar']['fidelity_gates']['all_pass']} and session={signal['clock_summaries']['stock_session']['fidelity_gates']['all_pass']}; no validation, holdout, or 2026 data was read.
- Frozen cost preflight: {freeze['pair_count']} pairs / {freeze['evaluator_call_count']} calls; {pair_summary['evaluated_pairs']} pairs evaluated and {pair_summary['blocked_pairs']} support-blocked.

## Capacity result

The sampled active preflight covered all 16 shards but only {active_summary['sample_trade_times_per_shard']} fixed minute coordinates per shard. A separate one-pair calibration disabled sampling and targeted the complete {int(calibration_abort['development_release_rows']):,}-row development release. It did not complete before the {float(calibration_abort['resource_gate_seconds'])/60:.0f}-minute resource gate, consumed {float(calibration_abort['cpu_seconds'])/60:.2f} CPU minutes, read {int(calibration_abort['read_transfer_bytes'])/1024**3:.2f} GiB, wrote no result, and peaked at {peak_bytes/1024**3:.2f} GiB. With a 20 GiB host reserve, the 77o host supports at most {workers} such workers before candidate-batch growth.

The current evaluator keeps full-series expression and reward rows in process memory. Consequently it has no qualified bounded batch size for 2,048 pairs, and a linear lower-bound projection is {projections['minimum_2048_pair_run_active_share']['linear_wall_days']:.2f} days for the active share even before session work and coordination overhead. Running Stage A under the 240-coordinate sample would be cheaper, but would not satisfy the command's full-release strict claim.

## Route preflight

{route_table}

The diagnostic matched-positive rows in this preflight are retained as observations only. They were not selected, promoted, cross-seed reproduced, or written to adaptive memory.

## Required next engineering step

Replace per-worker full-series retention with a bounded streaming DAG evaluator: materialize reusable field blocks once per shard, evaluate multiple candidates in bounded batches, evict expression/subtree series deterministically, write only pair summaries by default, and repeat the same 32-pair full-coordinate preflight. Only after that preflight demonstrates capacity should the frozen 4,096-pair Stage A start.

## Boundaries

`GLOBAL_FORMAL_SEARCH=FORBIDDEN`; `PROMOTION=FORBIDDEN`; `VALIDATION_FEEDBACK=FORBIDDEN`; `HOLDOUT_FEEDBACK=FORBIDDEN`; `FORWARD_2026=SEALED`; `CROSS_SPRINT_MEMORY=FORBIDDEN`.
"""
    (reports / "CN_COMPOSITIONAL_SEARCH_REPORT.md").write_text(report, encoding="utf-8")
    failure = f"""# CN Failure Attribution

Primary attribution: `COMPUTE_OR_IO_BOTTLENECK`.

{route_table}

The route rows above describe only the representative preflight. `NO_GROSS_EDGE`, `CONTROL_NOT_BEATEN`, `CROSS_SEED_INSTABILITY`, and `NO_ALPHA` are not assigned because Strict Stage A did not run. Structural aliasing remains measured separately and is not the stopping cause.
"""
    (reports / "CN_FAILURE_ATTRIBUTION_REPORT.md").write_text(failure, encoding="utf-8")

    required_runtime = [
        "CN_GENERATOR_EXPRESSIVITY_AUDIT.json",
        "CN_TYPED_COMPOSITIONAL_GRAMMAR_V2.json",
        "CN_SKELETON_REGISTRY.json",
        "CN_PROPOSAL_EXPOSURE_LEDGER.parquet",
        "CN_ADMISSION_WATERFALL.csv",
        "CN_PAIR_RECEIPTS.jsonl",
        "CN_STRICT_PAIR_RESULTS.parquet",
        "CN_ROUTE_SKELETON_METRICS.csv",
        "CN_BEHAVIOR_CLUSTERS.json",
        "CN_CROSS_SEED_REPRODUCTION.json",
        "CN_POLICY_BEHAVIOR_AUDIT.json",
        "CN_RESOURCE_PREFLIGHT.json",
    ]
    required_reports = [
        "CN_COMPOSITIONAL_SEARCH_REPORT.md",
        "CN_COMPOSITIONAL_SEARCH_DECISION.json",
        "CN_FAILURE_ATTRIBUTION_REPORT.md",
    ]
    missing = [name for name in required_runtime if not (runtime / name).exists()]
    missing += [name for name in required_reports if not (reports / name).exists()]
    if missing:
        raise RuntimeError(f"missing closure artifacts: {missing}")
    manifest = {
        "status": "CN_COMPOSITIONAL_SEARCH_ARTIFACTS_COMPLETE_COMPUTE_BOTTLENECK",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "repo_sha": _git(repo, "rev-parse", "HEAD"),
        "runtime": [_artifact_record(runtime / name, repo) for name in required_runtime],
        "reports": [_artifact_record(reports / name, repo) for name in required_reports],
        "sealed_reads": {"validation": 0, "holdout": 0, "forward_2026": 0},
    }
    _write_json(runtime / "CN_ARTIFACT_MANIFEST.json", manifest)

    bundle = reports / "CN_COMPOSITIONAL_SEARCH_BUNDLE.zip"
    with zipfile.ZipFile(bundle, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for name in required_runtime + ["CN_ARTIFACT_MANIFEST.json"]:
            archive.write(runtime / name, arcname=f"runtime/{name}")
        for name in required_reports:
            archive.write(reports / name, arcname=f"reports/{name}")
    bundle_record = {"path": str(bundle.relative_to(repo)).replace("\\", "/"), "sha256": _sha256(bundle), "bytes": bundle.stat().st_size}
    _write_json(reports / "CN_COMPOSITIONAL_SEARCH_BUNDLE_MANIFEST.json", bundle_record)
    print(json.dumps({"decision": decision, "resource": resource, "bundle": bundle_record}, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
