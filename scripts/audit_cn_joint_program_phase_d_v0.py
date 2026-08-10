from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from scripts import run_cn_joint_program_phase_d_v0 as phase_d
from scripts.audit_cn_joint_program_allocator_repair_canary_v0 import (
    arm_metrics_v0,
)
from our_system_phase2.runtime.cn_joint_program_phase_d_v0 import (
    CHECKPOINT_COUNT,
    DECISION_GATES,
    EXPECTED_RECORDS,
    FREE_MEMORY_ENFORCEMENT,
    IMPROVED_TEMPLATES,
    WEAK_TEMPLATES,
)
from our_system_phase2.services.program_allocator_repair_bandit_v0 import (
    ProgramAllocatorRepairBanditV0,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8-sig").splitlines()
        if line.strip()
    ]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _verify_self_hash(payload: Mapping[str, Any], field: str, label: str) -> None:
    body = dict(payload)
    expected = str(body.pop(field, ""))
    if expected != stable_hash(body):
        raise RuntimeError(f"{label} self-hash drift")


def _verify_artifacts(root: Path, manifest: Mapping[str, Any]) -> None:
    resolved_root = root.resolve()
    for artifact in manifest.get("artifacts") or ():
        path = (root / str(artifact["path"])).resolve()
        if not path.is_relative_to(resolved_root):
            raise RuntimeError(f"artifact path escape: {path}")
        if (
            not path.is_file()
            or path.stat().st_size != int(artifact["bytes"])
            or _sha256(path) != str(artifact["sha256"])
        ):
            raise RuntimeError(f"artifact drift: {path}")


def evaluate_decision_gates_v0(
    *,
    revised: Mapping[str, Any],
    uniform: Mapping[str, Any],
    template_metrics: Mapping[str, Mapping[str, Mapping[str, Any]]],
    gates: Mapping[str, Any],
) -> dict[str, Any]:
    metric_names = (
        "productive_rate",
        "all_four_positive_rate",
        "primary_reward_positive_rate",
        "primary_return_positive_rate",
        "three_window_positive_rate",
        "median_return_per_turnover",
        "blocked_rate",
        "matched_return_increment_mean",
        "matched_return_increment_median",
    )
    deltas = {
        name: float(revised[name]) - float(uniform[name]) for name in metric_names
    }
    template_results: dict[str, Any] = {}
    for template_id in IMPROVED_TEMPLATES:
        revised_template = template_metrics[template_id]["REVISED_EXPLOIT"]
        uniform_template = template_metrics[template_id]["UNIFORM_FRESH"]
        all_four_delta = float(revised_template["all_four_positive_rate"]) - float(
            uniform_template["all_four_positive_rate"]
        )
        blocked_delta = float(revised_template["blocked_rate"]) - float(
            uniform_template["blocked_rate"]
        )
        matched_mean_delta = float(
            revised_template["matched_return_increment_mean"]
        ) - float(uniform_template["matched_return_increment_mean"])
        matched_median_delta = float(
            revised_template["matched_return_increment_median"]
        ) - float(uniform_template["matched_return_increment_median"])
        template_results[template_id] = {
            "all_four_positive_rate_delta": all_four_delta,
            "blocked_rate_delta": blocked_delta,
            "matched_return_increment_mean_delta": matched_mean_delta,
            "matched_return_increment_median_delta": matched_median_delta,
            "improved": all_four_delta > 0.0 and blocked_delta <= 0.0,
            "blocker_non_worsening": blocked_delta <= 0.0,
        }
    improved_count = sum(
        bool(result["improved"]) for result in template_results.values()
    )
    checks = {
        "productive_rate_delta": deltas["productive_rate"]
        >= float(gates["productive_rate_delta_vs_uniform_minimum"]),
        "all_four_positive_rate_delta": deltas["all_four_positive_rate"]
        >= float(gates["all_four_positive_rate_delta_vs_uniform_minimum"]),
        "primary_reward_positive_rate_delta": deltas[
            "primary_reward_positive_rate"
        ]
        >= float(gates["primary_reward_positive_rate_delta_vs_uniform_minimum"]),
        "primary_return_positive_rate_delta": deltas[
            "primary_return_positive_rate"
        ]
        >= float(gates["primary_return_positive_rate_delta_vs_uniform_minimum"]),
        "three_window_positive_rate_delta": deltas["three_window_positive_rate"]
        >= float(gates["three_window_positive_rate_delta_vs_uniform_minimum"]),
        "median_return_per_turnover_delta": deltas[
            "median_return_per_turnover"
        ]
        >= float(gates["median_return_per_turnover_delta_vs_uniform_minimum"]),
        "blocked_rate_delta": deltas["blocked_rate"]
        <= float(gates["blocked_rate_delta_vs_uniform_maximum"]),
        "matched_return_increment_mean_delta": deltas[
            "matched_return_increment_mean"
        ]
        >= float(
            gates["matched_return_increment_mean_delta_vs_uniform_minimum"]
        ),
        "matched_return_increment_median_delta": deltas[
            "matched_return_increment_median"
        ]
        >= float(
            gates["matched_return_increment_median_delta_vs_uniform_minimum"]
        ),
        "minimum_improved_revised_templates": improved_count
        >= int(gates["minimum_improved_revised_templates"]),
        "per_template_blocker_non_worsening": all(
            bool(result["blocker_non_worsening"])
            for result in template_results.values()
        ),
    }
    return {
        "deltas": deltas,
        "checks": checks,
        "failed_checks": sorted(key for key, passed in checks.items() if not passed),
        "improved_revised_template_count": improved_count,
        "template_results": template_results,
        "all_gates_pass": all(checks.values()),
    }


def audit(
    root: Path,
    freeze: Path,
    expected_runner_repo_sha: str,
    expected_root_finalizer_repo_sha: str,
    audit_repo_sha: str,
) -> dict[str, Any]:
    phase_d._configure_engine()
    engine = phase_d.engine
    freeze_closure = phase_d.verify_prefinancial_freeze_v0(freeze)
    if str(freeze_closure["repo_sha"]) != expected_runner_repo_sha:
        raise RuntimeError("Phase D freeze repo SHA drift")
    frozen_contract = _read_json(freeze / "phase_c_run_contract.json")
    gates = dict(frozen_contract["decision_gates"])
    if gates != DECISION_GATES:
        raise RuntimeError("Phase D frozen decision gate drift")

    closure_path = root / phase_d.CLOSURE_NAME
    closure = _read_json(closure_path)
    _verify_self_hash(closure, "closure_payload_sha256", "Phase D root closure")
    if (
        str(closure["status"]) != phase_d.STATUS
        or int(closure["record_count"]) != EXPECTED_RECORDS
        or int(closure["checkpoint_count"]) != CHECKPOINT_COUNT
        or str(closure["runner_repo_sha"]) != expected_runner_repo_sha
        or str(closure["root_finalizer_repo_sha"])
        != expected_root_finalizer_repo_sha
        or closure.get("free_memory_enforcement") != FREE_MEMORY_ENFORCEMENT
        or bool(closure.get("fixed_24_gib_hard_gate_applied"))
        or not bool(closure.get("memory_telemetry_recorded"))
        or bool(closure.get("allocator_canary_financial_records_reused"))
        or bool(closure.get("phase_c_financial_records_reused"))
    ):
        raise RuntimeError("Phase D closure contract drift")

    root_manifest = _read_json(root / "ARTIFACT_MANIFEST.json")
    _verify_self_hash(root_manifest, "artifact_manifest_sha256", "root manifest")
    _verify_artifacts(root, root_manifest)
    checkpoint_recovery = bool(closure.get("checkpoint_recovery"))
    if checkpoint_recovery:
        recovery = _read_json(root / "checkpoint_recovery_binding.json")
        _verify_self_hash(recovery, "recovery_binding_sha256", "checkpoint recovery")
        if (
            str(recovery.get("checkpoint_builder_repo_sha"))
            != expected_runner_repo_sha
            or str(recovery.get("checkpoint_recovery_repo_sha"))
            != expected_root_finalizer_repo_sha
            or str(recovery.get("executor_mode"))
            != engine.CHECKPOINT_RECOVERY_EXECUTOR_MODE
            or int(recovery.get("effective_concurrent_workers", 0)) != 1
            or int(recovery.get("max_tasks_per_child", 0)) != 1
            or bool(recovery.get("financial_results_reused"))
            or bool(recovery.get("diagnostic_financial_results_reused"))
            or bool(recovery.get("incomplete_results_reused"))
        ):
            raise RuntimeError("Phase D checkpoint recovery contract drift")
    input_hash = str(closure["phase_c_input_binding_sha256"])
    asks = _read_jsonl(freeze / "phase_c_ask_plan.jsonl")
    if len(asks) != EXPECTED_RECORDS:
        raise RuntimeError("Phase D ask-plan count drift")
    bandit = ProgramAllocatorRepairBanditV0.restore(
        _read_json(freeze / "initial_bandit_state.json")
    )
    behavior_counts: Counter[str] = Counter()
    previous_manifest: Path | None = None
    records: list[dict[str, Any]] = []
    minimum_boundary_free_memory = math.inf
    for index in range(1, CHECKPOINT_COUNT + 1):
        checkpoint_id = f"checkpoint_{index:03d}"
        checkpoint = root / "checkpoints" / checkpoint_id
        schedules = _read_jsonl(checkpoint / "selected_schedule.jsonl")
        decisions = _read_jsonl(checkpoint / "selection_ledger.jsonl")
        expected_ordinals = list(range((index - 1) * 8, index * 8))
        if (
            len(schedules) != 8
            or len(decisions) != 8
            or [int(row["main_record_ordinal"]) for row in schedules]
            != expected_ordinals
        ):
            raise RuntimeError(f"checkpoint schedule shape drift: {checkpoint_id}")
        for schedule in schedules:
            ordinal = int(schedule["main_record_ordinal"])
            ask = asks[ordinal]
            if any(
                str(schedule[key]) != str(ask[key])
                for key in ("template_id", "generation_arm", "ask_record_sha256")
            ):
                raise RuntimeError(f"frozen Phase D ask drift: ordinal={ordinal}")
        for decision in decisions:
            _verify_self_hash(
                decision, "selection_decision_sha256", "selection decision"
            )
        previous_manifest, checkpoint_records, bandit = engine._verify_checkpoint(
            checkpoint,
            checkpoint_id=checkpoint_id,
            previous_manifest=previous_manifest,
            input_hash=input_hash,
            expected_schedules=schedules,
            expected_decisions=decisions,
            bandit=bandit,
            behavior_counts=behavior_counts,
        )
        summary = _read_json(checkpoint / "checkpoint_summary.json")
        _verify_self_hash(summary, "summary_payload_sha256", "checkpoint summary")
        boundary_free = int(summary["minimum_checkpoint_boundary_free_memory_bytes"])
        if boundary_free < 1:
            raise RuntimeError(f"Phase D memory telemetry missing: {checkpoint_id}")
        if any(
            int(summary[key])
            for key in (
                "validation_reads",
                "holdout_reads",
                "historical_2023_reads",
                "forward_b_reads",
                "forward_2026_reads",
            )
        ):
            raise PermissionError(f"sealed read in {checkpoint_id}")
        minimum_boundary_free_memory = min(
            minimum_boundary_free_memory, boundary_free
        )
        records.extend(checkpoint_records)

    if [int(row["main_record_ordinal"]) for row in records] != list(
        range(EXPECTED_RECORDS)
    ):
        raise RuntimeError("Phase D record identity/order drift")
    if any(
        row["generation_arm"] == "REVISED_EXPLOIT"
        and row["template_id"] not in IMPROVED_TEMPLATES
        for row in records
    ):
        raise RuntimeError("Phase D revised exploit escaped qualified templates")
    access = _read_json(root / "access_ledger.json")
    _verify_self_hash(access, "access_ledger_sha256", "access ledger")
    if any(
        int(access[key])
        for key in (
            "validation_reads",
            "holdout_reads",
            "historical_2023_reads",
            "forward_b_reads",
            "forward_2026_reads",
        )
    ):
        raise PermissionError("Phase D root sealed read")
    resource = _read_json(root / "resource_summary.json")
    _verify_self_hash(resource, "resource_summary_payload_sha256", "resource summary")
    if checkpoint_recovery and (
        int(resource.get("effective_workers_per_checkpoint", 0)) != 1
        or str(resource.get("executor_lifecycle"))
        != engine.CHECKPOINT_RECOVERY_EXECUTOR_MODE
    ):
        raise RuntimeError("Phase D checkpoint recovery resource summary drift")

    compared = [
        row for row in records if str(row["template_id"]) in IMPROVED_TEMPLATES
    ]
    compared_by_arm = {
        arm: arm_metrics_v0(
            [row for row in compared if str(row["generation_arm"]) == arm]
        )
        for arm in ("UNIFORM_FRESH", "REVISED_EXPLOIT")
    }
    template_metrics = {
        template_id: {
            arm: arm_metrics_v0(
                [
                    row
                    for row in records
                    if str(row["template_id"]) == template_id
                    and str(row["generation_arm"]) == arm
                ]
            )
            for arm in ("UNIFORM_FRESH", "REVISED_EXPLOIT", "NOVELTY_RESERVE")
        }
        for template_id in IMPROVED_TEMPLATES
    }
    weak_template_metrics = {
        template_id: {
            arm: arm_metrics_v0(
                [
                    row
                    for row in records
                    if str(row["template_id"]) == template_id
                    and str(row["generation_arm"]) == arm
                ]
            )
            for arm in ("UNIFORM_FRESH", "NOVELTY_RESERVE")
        }
        for template_id in WEAK_TEMPLATES
    }
    decision = evaluate_decision_gates_v0(
        revised=compared_by_arm["REVISED_EXPLOIT"],
        uniform=compared_by_arm["UNIFORM_FRESH"],
        template_metrics=template_metrics,
        gates=gates,
    )
    decision_status = (
        "PHASE_D_CONFIRMED_REVISED_ALLOCATOR_CANDIDATE_NO_AUTOMATIC_PROMOTION"
        if decision["all_gates_pass"]
        else "HOLD_REVISED_ALLOCATOR_NO_PROMOTION"
    )
    return {
        "schema_version": "cn_joint_program_phase_d_audit_v0",
        "status": "PASS",
        "decision": decision_status,
        "result_root": str(root),
        "freeze_root": str(freeze),
        "runner_repo_sha": expected_runner_repo_sha,
        "root_finalizer_repo_sha": expected_root_finalizer_repo_sha,
        "audit_repo_sha": audit_repo_sha,
        "checkpoint_recovery": checkpoint_recovery,
        "closure_file_sha256": _sha256(closure_path),
        "closure_payload_sha256": str(closure["closure_payload_sha256"]),
        "checkpoint_count": CHECKPOINT_COUNT,
        "record_count": EXPECTED_RECORDS,
        "generation_arm_counts": dict(
            sorted(Counter(str(row["generation_arm"]) for row in records).items())
        ),
        "minimum_checkpoint_boundary_free_memory_bytes": int(
            minimum_boundary_free_memory
        ),
        "fixed_24_gib_hard_gate_applied": False,
        "memory_telemetry_recorded": True,
        "improved_template_arm_metrics": compared_by_arm,
        "improved_template_metrics": template_metrics,
        "weak_template_diagnostics": weak_template_metrics,
        "frozen_decision_gates": gates,
        "decision_evaluation": decision,
        "bias_scope": {
            "development_only": True,
            "sample_records_per_compared_arm": 96,
            "compared_template_count": len(IMPROVED_TEMPLATES),
            "weak_template_diagnostic_count": len(WEAK_TEMPLATES),
            "oos_claim_allowed": False,
            "automatic_promotion_allowed": False,
            "adaptive_budget_reallocation": False,
            "allocator_canary_financial_records_reused": False,
            "phase_c_financial_records_reused": False,
        },
        "validation_reads": 0,
        "holdout_reads": 0,
        "historical_2023_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
        "sealed_reads": 0,
        "automatic_next_phase_launch": False,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--freeze-root", required=True, type=Path)
    parser.add_argument("--audit-root", required=True, type=Path)
    parser.add_argument("--expected-runner-repo-sha", required=True)
    parser.add_argument("--expected-root-finalizer-repo-sha")
    parser.add_argument("--audit-repo-sha", required=True)
    args = parser.parse_args(argv)
    if len(args.expected_runner_repo_sha) != 40:
        parser.error("expected-runner-repo-sha must be a full Git SHA")
    if len(args.audit_repo_sha) != 40:
        parser.error("audit-repo-sha must be a full Git SHA")
    expected_root_finalizer_repo_sha = (
        args.expected_root_finalizer_repo_sha or args.expected_runner_repo_sha
    )
    if len(expected_root_finalizer_repo_sha) != 40:
        parser.error("expected-root-finalizer-repo-sha must be a full Git SHA")
    audit_root = args.audit_root.resolve()
    if audit_root.exists():
        raise FileExistsError(f"Phase D audit root must be fresh: {audit_root}")
    audit_root.mkdir(parents=True)
    payload = audit(
        args.root.resolve(),
        args.freeze_root.resolve(),
        args.expected_runner_repo_sha,
        expected_root_finalizer_repo_sha,
        args.audit_repo_sha,
    )
    payload["audit_script_sha256"] = _sha256(Path(__file__).resolve())
    payload["audit_payload_sha256"] = stable_hash(payload)
    output = audit_root / "audit.json"
    output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
