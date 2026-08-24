"""Audit the failed Stage-2 BASE_EVENT mature-state exploration collapse and repair."""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_search_core_v2_stage1_v1 as stage1
from our_system_phase2.runtime import cn_program_stage_d_primitive_confirmation_v1 as stage_d_runtime
from our_system_phase2.runtime import cn_search_core_v2_stage2_v1 as runtime
from our_system_phase2.services.program_search_state_jump_adapter_v2 import StateJumpProgramSearchAdapterV2
from our_system_phase2.services.program_search_state_jump_generator_v2 import (
    EVENT_APPLICATION_POLICIES,
    program_region_key,
)
from our_system_phase2.services.unified_capability_registry import stable_hash

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FAILED_STATE_RELATIVE = Path(
    "runtime/run_plans/cn_search_core_v2_stage2_failed_cp1_optimizer_state_452783b_20260825.json"
)
STAGE1_FINAL_RELATIVE = Path(
    "runtime/run_plans/cn_search_core_v2_stage1_final_optimizer_state_47bc94f_20260824.json"
)
FAILED_REPO_SHA = "452783ba8e4d33b989f8f9addcbbcb8f9b7997d5"
FAILED_RUN_ID = "cn_search_core_v2_stage2_20260825_452783b"


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _verify_snapshot(payload: Mapping[str, Any], label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop("snapshot_hash", ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} snapshot self-hash drift")
    return claimed


def _restore(
    snapshot: Mapping[str, Any], *, authority: Mapping[str, Any]
) -> StateJumpProgramSearchAdapterV2:
    return StateJumpProgramSearchAdapterV2.restore(
        snapshot=snapshot,
        adapter=authority["adapter"],
        compiler=authority["compiler"],
        components_by_role=stage1._components_by_role(authority),
        seed=0,
    )


def _ask_probe(
    snapshot: Mapping[str, Any], *, authority: Mapping[str, Any], count: int, label: str
) -> dict[str, Any]:
    state = _restore(snapshot, authority=authority)
    asks = state.ask(
        checkpoint_id=f"STAGE2_COLLAPSE_REPAIR_{label}_{count}",
        count=int(count),
        required_program_template_id="BASE_EVENT",
        eligible_exact_identities=None,
        batch_group_constraint=None,
    )
    operations: Counter[str] = Counter(
        str(
            dict(dict(row.get("acquisition") or {}).get("generator_summary") or {}).get("operation")
            or "UNKNOWN"
        )
        for row in asks
    )
    exacts = [str(row["exact_identity"]) for row in asks]
    if len(exacts) != int(count) or len(set(exacts)) != int(count):
        raise RuntimeError(f"{label} ask-{count} did not return unique coverage")
    if "UNKNOWN" in operations:
        raise RuntimeError(f"{label} operation receipt drift")
    return {
        "requested": int(count),
        "generated": len(exacts),
        "unique_exact_count": len(set(exacts)),
        "operation_counts": dict(sorted(operations.items())),
        "status": "PASS",
    }


def _restricted_zero(records_path: Path) -> tuple[int, dict[str, int]]:
    records = [
        json.loads(line)
        for line in records_path.read_text(encoding="utf-8-sig").splitlines()
        if line.strip()
    ]
    keys = (
        "validation_reads", "holdout_reads", "historical_2023_reads",
        "forward_b_reads", "forward_2026_reads",
    )
    totals = {key: sum(int(row.get(key) or 0) for row in records) for key in keys}
    if any(totals.values()):
        raise RuntimeError(f"failed run restricted-read drift: {totals}")
    return len(records), totals


def audit(args: argparse.Namespace) -> dict[str, Any]:
    repo = PROJECT_ROOT.resolve()
    failed_root = args.failed_output_root.resolve()
    source_auth = stage_d_runtime.verify_authorization(
        args.source_stage_d_authorization.resolve(), repo_root=repo
    )
    provisional = {
        "authorization_payload_sha256": stable_hash(
            {"role": "SEARCH_CORE_V2_STAGE2_EXPLORATION_COLLAPSE_REPAIR_AUDIT"}
        ),
        "campaign_id": runtime.CAMPAIGN_ID,
        "campaign_profile": runtime.CAMPAIGN_PROFILE,
        "source_prior_exact": dict(source_auth["source_prior_exact"]),
    }
    args.executor_workers = runtime.PRIMARY_EXECUTOR_WORKERS
    authority = stage1._load_authority(
        args,
        authorization=provisional,
        repo_sha=str(args.audit_repo_sha),
    )

    failed_state_path = repo / FAILED_STATE_RELATIVE
    stage1_state_path = repo / STAGE1_FINAL_RELATIVE
    if not failed_state_path.is_file() or not stage1_state_path.is_file():
        raise FileNotFoundError("frozen optimizer state missing")
    failed_state = _read(failed_state_path)
    failed_state_hash = _verify_snapshot(failed_state, "failed cp1")
    if failed_state_hash != "b432053e9da2db2a116ca983903cf6c3fea35b3692b9eaecf074ed6fec271226":
        raise RuntimeError("failed cp1 snapshot identity drift")
    if len(failed_state["history"]) != 22 or len(failed_state["generated_exact_identities"]) != 516:
        raise RuntimeError("failed cp1 snapshot cardinality drift")

    # Closed financial prefix must be exactly A12/B12/A12 = 36 records.  The
    # failing second B12 never produced a selected schedule or candidate result.
    closed_metrics: list[dict[str, Any]] = []
    total_records = 0
    restricted_totals = {
        "validation_reads": 0, "holdout_reads": 0, "historical_2023_reads": 0,
        "forward_b_reads": 0, "forward_2026_reads": 0,
    }
    for checkpoint in range(3):
        root = failed_root / f"checkpoint_{checkpoint:04d}"
        metric = _read(root / "checkpoint_metric.json")
        closed_metrics.append(metric)
        count, totals = _restricted_zero(root / "candidate_results.jsonl")
        if count != 12 or int(metric.get("evaluated") or 0) != 12:
            raise RuntimeError("failed Stage-2 closed-prefix cardinality drift")
        total_records += count
        for key, value in totals.items():
            restricted_totals[key] += value
    if total_records != 36:
        raise RuntimeError("failed Stage-2 closed-prefix total drift")
    inflight = failed_root / "checkpoint_0003.inflight"
    if not inflight.is_dir():
        raise RuntimeError("failed Stage-2 cp3 inflight missing")
    inflight_names = sorted(path.name for path in inflight.iterdir())
    if inflight_names != ["optimizer_state_before.json"]:
        raise RuntimeError(f"failed cp3 wrote artifacts past ask boundary: {inflight_names}")
    before = _read(inflight / "optimizer_state_before.json")
    if str(before.get("snapshot_hash") or "") != failed_state_hash:
        raise RuntimeError("failed cp3 did not start from frozen cp1 state")
    if (failed_root / "CN_SEARCH_CORE_V2_STAGE2_COMPLETE.json").exists():
        raise RuntimeError("failed Stage-2 unexpectedly has terminal closure")

    stderr_path = args.stderr_log.resolve()
    stderr_text = stderr_path.read_text(encoding="utf-8-sig", errors="replace")
    if "STATE_JUMP_GENERATOR_EXHAUSTED_ATTEMPTS" not in stderr_text:
        raise RuntimeError("failed Stage-2 stderr fingerprint drift")
    exit_receipt = _read(args.exit_receipt.resolve())
    job_status = _read(args.job_status.resolve())
    if int(exit_receipt.get("exit_code", 0)) != 1 or int(job_status.get("exit_code", 0)) != 1:
        raise RuntimeError("failed Stage-2 exit receipt drift")
    if str(exit_receipt.get("repo_sha") or "") != FAILED_REPO_SHA:
        raise RuntimeError("failed Stage-2 repo SHA drift")
    resource_state = _read(args.resource_state.resolve())
    active_matching = [
        row for row in list(resource_state.get("leases") or ())
        if str(row.get("workload_id") or "") == str(failed_root)
    ]
    if active_matching:
        raise RuntimeError("failed Stage-2 lease was not released")

    # Exact semantic-space census for BASE_EVENT at the failed mature state.
    state = _restore(failed_state, authority=authority)
    generator = state.generator
    seen = set(generator.memory.seen_semantic_hashes) | set(generator._generated_semantic_hashes)
    base_pool = generator.components_by_role["base"]
    event_pool = generator.components_by_role["event"]
    semantics: set[str] = set()
    unseen: set[str] = set()
    non_dead_unseen: set[str] = set()
    for base in base_pool:
        for event in event_pool:
            for event_application in sorted(EVENT_APPLICATION_POLICIES):
                policy = generator._policy_for_template("BASE_EVENT")
                policy["event_application"] = event_application
                program = generator.adapter.compose(
                    "BASE_EVENT",
                    base,
                    event_component=event,
                    combination_policy=policy,
                )
                semantic = program.semantic_program_hash
                semantics.add(semantic)
                if semantic not in seen:
                    unseen.add(semantic)
                    region = program_region_key(
                        template_id="BASE_EVENT",
                        components={"base": base, "event": event},
                        combination_policy=policy,
                    )
                    if not generator.memory.region_dead(region):
                        non_dead_unseen.add(semantic)
    enumerated = len(base_pool) * len(event_pool) * len(EVENT_APPLICATION_POLICIES)
    if (
        enumerated != 1792
        or len(semantics) != 1792
        or len(unseen) < 1500
        or len(non_dead_unseen) != len(unseen)
    ):
        raise RuntimeError("BASE_EVENT semantic-space census contradicts exploration-collapse diagnosis")

    # Learned heuristic reachability without collision rescue.  _jump itself is
    # unchanged by the repair, so this isolates the mature learned proposal surface.
    sample_state = _restore(failed_state, authority=authority)
    sample_generator = sample_state.generator
    sample_seen = set(sample_generator.memory.seen_semantic_hashes) | set(
        sample_generator._generated_semantic_hashes
    )
    counts: Counter[str] = Counter()
    unique_semantics: set[str] = set()
    unique_unseen: set[str] = set()
    for _ in range(10000):
        operation = sample_generator._choose_operation("BASE_EVENT")
        components, policy, _parents, _changed = sample_generator._jump(
            "BASE_EVENT", operation
        )
        region = program_region_key(
            template_id="BASE_EVENT", components=components, combination_policy=policy
        )
        program = sample_generator.adapter.compose(
            "BASE_EVENT",
            components["base"],
            event_component=components["event"],
            combination_policy=policy,
        )
        semantic = program.semantic_program_hash
        unique_semantics.add(semantic)
        if sample_generator.memory.region_dead(region):
            counts["dead"] += 1
        elif semantic in sample_seen:
            counts["seen_duplicate"] += 1
        else:
            counts["unseen_hit"] += 1
            unique_unseen.add(semantic)
    if len(unique_unseen) >= 64 or counts["seen_duplicate"] < 8000:
        raise RuntimeError("mature heuristic no longer demonstrates local coverage collapse")

    repaired_failed_state_probes = {
        str(count): _ask_probe(
            failed_state, authority=authority, count=count, label="failed_cp1"
        )
        for count in (12, 24, 48, 72)
    }

    # The repaired ask-24 must also work on every real mature state observed
    # before Stage-2; this prevents fixing only the single failed snapshot.
    historical_paths: list[tuple[str, Path]] = [
        ("stage1_final", stage1_state_path),
    ]
    historical_paths.extend(
        (
            f"stage15_checkpoint_{checkpoint:04d}",
            args.stage15_run_root.resolve()
            / f"checkpoint_{checkpoint:04d}"
            / "optimizer_state_after.json",
        )
        for checkpoint in (1, 3, 5, 7, 9, 11, 13)
    )
    historical_probes: dict[str, Any] = {}
    for label, path in historical_paths:
        snapshot = _read(path)
        snapshot_hash = _verify_snapshot(snapshot, label)
        historical_probes[label] = {
            "path": str(path),
            "file_sha256": engine._sha256(path),
            "snapshot_payload_sha256": snapshot_hash,
            "history_count": len(snapshot["history"]),
            "generated_exact_count": len(snapshot["generated_exact_identities"]),
            "ask24": _ask_probe(
                snapshot, authority=authority, count=24, label=label
            ),
        }

    payload = {
        "schema_version": "cn_search_core_v2_stage2_exploration_collapse_repair_audit_v1",
        "status": "SEARCH_CORE_V2_STAGE2_EXPLORATION_COLLAPSE_REPAIR_PASS_FRESH_RESTART_REQUIRED",
        "audit_repo_sha": str(args.audit_repo_sha),
        "failed_run": {
            "run_id": FAILED_RUN_ID,
            "output_root": str(failed_root),
            "repo_sha": FAILED_REPO_SHA,
            "closed_financial_record_count": total_records,
            "closed_checkpoint_metrics": closed_metrics,
            "restricted_reads": restricted_totals,
            "failed_checkpoint_ordinal": 3,
            "failed_checkpoint_artifacts": inflight_names,
            "failure_fingerprint": "STATE_JUMP_GENERATOR_EXHAUSTED_ATTEMPTS",
            "job_exit_code": int(job_status["exit_code"]),
            "launcher_exit_code": int(exit_receipt["exit_code"]),
            "lease_released": True,
            "financial_results_reusable": False,
            "fresh_restart_required": True,
        },
        "failed_cp1_optimizer_state": {
            "relative_path": str(FAILED_STATE_RELATIVE).replace("\\", "/"),
            "file_sha256": engine._sha256(failed_state_path),
            "snapshot_payload_sha256": failed_state_hash,
            "history_count": len(failed_state["history"]),
            "generated_exact_count": len(failed_state["generated_exact_identities"]),
        },
        "root_cause": {
            "classification": "MATURE_HEURISTIC_SEMANTIC_COVERAGE_COLLAPSE_NOT_TRUE_SPACE_EXHAUSTION",
            "base_component_count": len(base_pool),
            "event_component_count": len(event_pool),
            "event_policy_count": len(EVENT_APPLICATION_POLICIES),
            "enumerated_combination_count": enumerated,
            "unique_semantic_space_count": len(semantics),
            "unseen_semantic_count": len(unseen),
            "non_dead_unseen_semantic_count": len(non_dead_unseen),
            "learned_heuristic_sample_count": 10000,
            "learned_heuristic_counts": dict(sorted(counts.items())),
            "learned_heuristic_unique_semantic_count": len(unique_semantics),
            "learned_heuristic_unique_unseen_count": len(unique_unseen),
        },
        "repair_contract": {
            "normal_policy": "LEARNED_JUMP_FOR_FIRST_HALF_OF_UNCHANGED_MAXIMUM_ATTEMPTS",
            "collision_rescue": "FULL_POOL_DIVERSIFIED_FRESH_FOR_SECOND_HALF_OF_UNCHANGED_MAXIMUM_ATTEMPTS",
            "maximum_attempts_changed": False,
            "snapshot_schema_changed": False,
            "sealed_feedback_changed": False,
            "stage2_template_batch_size": 24,
        },
        "failed_state_repaired_supply": repaired_failed_state_probes,
        "historical_mature_state_ask24": historical_probes,
        "historical_mature_state_count": len(historical_probes),
        "candidate_evaluation_executed_by_audit": False,
        "failed_financial_records_reused": False,
        "oos_authority": "NONE",
        "automatic_stage2_restart_authorized": False,
    }
    payload["audit_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-stage-d-authorization", type=Path, required=True)
    parser.add_argument("--source-freeze-root", type=Path, required=True)
    parser.add_argument("--prior-exact-freeze", type=Path, required=True)
    parser.add_argument("--execution-contract", type=Path, required=True)
    parser.add_argument("--train-field-root", type=Path, required=True)
    parser.add_argument("--train-price-root", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--node-resource-capacity", type=Path, required=True)
    parser.add_argument("--audit-repo-sha", required=True)
    parser.add_argument("--failed-output-root", type=Path, required=True)
    parser.add_argument("--stage15-run-root", type=Path, required=True)
    parser.add_argument("--job-status", type=Path, required=True)
    parser.add_argument("--exit-receipt", type=Path, required=True)
    parser.add_argument("--resource-state", type=Path, required=True)
    parser.add_argument("--stderr-log", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    audit_sha = str(args.audit_repo_sha).lower()
    if len(audit_sha) != 40 or any(ch not in "0123456789abcdef" for ch in audit_sha):
        parser.error("--audit-repo-sha must be a 40-character hex Git SHA")
    args.audit_repo_sha = audit_sha
    payload = audit(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    engine._write_json(args.output, payload)
    print(
        json.dumps(
            {
                "status": payload["status"],
                "audit_payload_sha256": payload["audit_payload_sha256"],
                "failed_records": payload["failed_run"]["closed_financial_record_count"],
                "unseen_semantics": payload["root_cause"]["unseen_semantic_count"],
                "heuristic_unique_unseen": payload["root_cause"]["learned_heuristic_unique_unseen_count"],
                "historical_ask24_pass": payload["historical_mature_state_count"],
                "output": str(args.output.resolve()),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())