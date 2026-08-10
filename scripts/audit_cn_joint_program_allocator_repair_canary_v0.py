from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
from statistics import median
import sys
from typing import Any, Mapping, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from our_system_phase2.runtime.cn_joint_program_allocator_repair_canary_v0 import (
    DECISION_GATES,
    ENHANCED_TEMPLATE_ORDER,
)
from our_system_phase2.services.program_allocator_repair_bandit_v0 import (
    ProgramAllocatorRepairBanditV0,
    allocator_outcome_from_record_v0,
)
from our_system_phase2.services.unified_capability_registry import stable_hash
from scripts import run_cn_joint_program_allocator_repair_canary_v0 as canary


EXPECTED_REPO_SHA_LENGTH = 40
EXPECTED_RECORDS = 256
EXPECTED_CHECKPOINTS = 32
MINIMUM_FREE_MEMORY_BYTES = 24 * 1024**3


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


def _mean(values: Sequence[float]) -> float:
    return float(sum(values) / len(values)) if values else 0.0


def arm_metrics_v0(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rows = [dict(row) for row in records]
    outcomes = [allocator_outcome_from_record_v0(row) for row in rows]
    count = len(rows)
    if count == 0:
        raise ValueError("allocator arm metrics require at least one record")
    complete = sum(outcome["replay_admissible"] > 0.0 for outcome in outcomes)
    blocked = count - complete
    primary_rewards = [
        float((row.get("primary") or {}).get("continuous_book_net_reward") or 0.0)
        for row in rows
    ]
    primary_returns = [
        float((row.get("primary") or {}).get("cumulative_net_return") or 0.0)
        for row in rows
    ]
    turnover_efficiencies = [
        float(outcome["turnover_efficiency"]) for outcome in outcomes
    ]
    matched_rewards = [
        float(outcome["matched_reward_increment"]) for outcome in outcomes
    ]
    matched_returns = [
        float(outcome["matched_return_increment"]) for outcome in outcomes
    ]
    all_four = [
        float(
            outcome["primary_reward_positive"] > 0.0
            and outcome["primary_return_positive"] > 0.0
            and outcome["matched_reward_positive"] > 0.0
            and outcome["matched_return_positive"] > 0.0
        )
        for outcome in outcomes
    ]
    return {
        "record_count": count,
        "replay_complete_count": complete,
        "replay_blocked_count": blocked,
        "blocked_rate": blocked / count,
        "productive_count": sum(bool(row.get("productive")) for row in rows),
        "productive_rate": sum(bool(row.get("productive")) for row in rows) / count,
        "primary_reward_positive_rate": _mean(
            [float(outcome["primary_reward_positive"]) for outcome in outcomes]
        ),
        "primary_return_positive_rate": _mean(
            [float(outcome["primary_return_positive"]) for outcome in outcomes]
        ),
        "three_window_positive_rate": _mean(
            [float(outcome["stable_two_of_three"]) for outcome in outcomes]
        ),
        "all_four_positive_count": int(sum(all_four)),
        "all_four_positive_rate": _mean(all_four),
        "primary_reward_mean": _mean(primary_rewards),
        "primary_reward_median": float(median(primary_rewards)),
        "primary_return_mean": _mean(primary_returns),
        "primary_return_median": float(median(primary_returns)),
        "median_return_per_turnover": float(median(turnover_efficiencies)),
        "matched_reward_increment_mean": _mean(matched_rewards),
        "matched_reward_increment_median": float(median(matched_rewards)),
        "matched_return_increment_mean": _mean(matched_returns),
        "matched_return_increment_median": float(median(matched_returns)),
    }


def evaluate_decision_gates_v0(
    *,
    revised: Mapping[str, Any],
    uniform: Mapping[str, Any],
    template_metrics: Mapping[str, Mapping[str, Mapping[str, Any]]],
    gates: Mapping[str, Any],
) -> dict[str, Any]:
    deltas = {
        "productive_rate": float(revised["productive_rate"])
        - float(uniform["productive_rate"]),
        "all_four_positive_rate": float(revised["all_four_positive_rate"])
        - float(uniform["all_four_positive_rate"]),
        "primary_reward_positive_rate": float(
            revised["primary_reward_positive_rate"]
        )
        - float(uniform["primary_reward_positive_rate"]),
        "primary_return_positive_rate": float(
            revised["primary_return_positive_rate"]
        )
        - float(uniform["primary_return_positive_rate"]),
        "three_window_positive_rate": float(revised["three_window_positive_rate"])
        - float(uniform["three_window_positive_rate"]),
        "median_return_per_turnover": float(revised["median_return_per_turnover"])
        - float(uniform["median_return_per_turnover"]),
        "blocked_rate": float(revised["blocked_rate"])
        - float(uniform["blocked_rate"]),
    }
    template_results: dict[str, Any] = {}
    for template_id in ENHANCED_TEMPLATE_ORDER:
        revised_template = template_metrics[template_id]["REVISED_EXPLOIT"]
        uniform_template = template_metrics[template_id]["UNIFORM_FRESH"]
        all_four_delta = float(revised_template["all_four_positive_rate"]) - float(
            uniform_template["all_four_positive_rate"]
        )
        blocked_delta = float(revised_template["blocked_rate"]) - float(
            uniform_template["blocked_rate"]
        )
        improved = all_four_delta > 0.0 and blocked_delta <= 0.0
        template_results[template_id] = {
            "all_four_positive_rate_delta": all_four_delta,
            "blocked_rate_delta": blocked_delta,
            "improved": improved,
        }
    improved_templates = sum(
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
        "median_return_per_turnover_delta": deltas["median_return_per_turnover"]
        >= float(gates["median_return_per_turnover_delta_vs_uniform_minimum"]),
        "blocked_rate_delta": deltas["blocked_rate"]
        <= float(gates["blocked_rate_delta_vs_uniform_maximum"]),
        "minimum_improved_enhanced_templates": improved_templates
        >= int(gates["minimum_improved_enhanced_templates"]),
    }
    return {
        "deltas": deltas,
        "checks": checks,
        "failed_checks": sorted(key for key, passed in checks.items() if not passed),
        "improved_enhanced_template_count": improved_templates,
        "template_results": template_results,
        "all_gates_pass": all(checks.values()),
    }


def audit(
    root: Path,
    freeze: Path,
    expected_runner_repo_sha: str,
    audit_repo_sha: str,
) -> dict[str, Any]:
    canary._configure_engine()
    engine = canary.engine
    freeze_closure = canary.verify_prefinancial_freeze_v0(freeze)
    if str(freeze_closure["repo_sha"]) != expected_runner_repo_sha:
        raise RuntimeError("allocator-repair freeze repo SHA drift")
    frozen_contract = _read_json(freeze / "phase_c_run_contract.json")
    gates = dict(frozen_contract["decision_gates"])
    if gates != DECISION_GATES:
        raise RuntimeError("allocator-repair frozen decision gate drift")

    closure_path = root / canary.CLOSURE_NAME
    closure = _read_json(closure_path)
    _verify_self_hash(closure, "closure_payload_sha256", "root closure")
    if (
        str(closure["status"]) != canary.STATUS
        or int(closure["record_count"]) != EXPECTED_RECORDS
        or int(closure["checkpoint_count"]) != EXPECTED_CHECKPOINTS
        or str(closure["runner_repo_sha"]) != expected_runner_repo_sha
        or str(closure["root_finalizer_repo_sha"]) != expected_runner_repo_sha
        or bool(closure["phase_c_financial_records_reused"])
        or bool(closure["phase_d_launched"])
    ):
        raise RuntimeError("allocator-repair closure contract drift")

    root_manifest = _read_json(root / "ARTIFACT_MANIFEST.json")
    _verify_self_hash(root_manifest, "artifact_manifest_sha256", "root manifest")
    _verify_artifacts(root, root_manifest)
    input_hash = str(closure["phase_c_input_binding_sha256"])
    asks = _read_jsonl(freeze / "phase_c_ask_plan.jsonl")
    if len(asks) != EXPECTED_RECORDS:
        raise RuntimeError("allocator-repair ask-plan count drift")
    bandit = ProgramAllocatorRepairBanditV0.restore(
        _read_json(freeze / "initial_bandit_state.json")
    )
    behavior_counts: Counter[str] = Counter()
    previous_manifest: Path | None = None
    records: list[dict[str, Any]] = []
    minimum_boundary_free_memory = math.inf
    for index in range(1, EXPECTED_CHECKPOINTS + 1):
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
                raise RuntimeError(f"frozen ask drift: ordinal={ordinal}")
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
        if boundary_free < MINIMUM_FREE_MEMORY_BYTES:
            raise RuntimeError(f"checkpoint memory gate failure: {checkpoint_id}")
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
        raise RuntimeError("allocator-repair record identity/order drift")
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
        raise PermissionError("allocator-repair root sealed read")

    enhanced = [row for row in records if str(row["template_id"]) != "BASE"]
    enhanced_by_arm = {
        arm: arm_metrics_v0(
            [row for row in enhanced if str(row["generation_arm"]) == arm]
        )
        for arm in ("UNIFORM_FRESH", "REVISED_EXPLOIT", "NOVELTY_RESERVE")
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
        for template_id in ENHANCED_TEMPLATE_ORDER
    }
    decision = evaluate_decision_gates_v0(
        revised=enhanced_by_arm["REVISED_EXPLOIT"],
        uniform=enhanced_by_arm["UNIFORM_FRESH"],
        template_metrics=template_metrics,
        gates=gates,
    )
    decision_status = (
        "PHASE_D_ELIGIBLE_NO_LAUNCH"
        if decision["all_gates_pass"]
        else "HOLD_PHASE_D_NO_LAUNCH"
    )
    return {
        "schema_version": "cn_joint_program_allocator_repair_canary_audit_v0",
        "status": "PASS",
        "decision": decision_status,
        "result_root": str(root),
        "freeze_root": str(freeze),
        "runner_repo_sha": expected_runner_repo_sha,
        "audit_repo_sha": audit_repo_sha,
        "closure_file_sha256": _sha256(closure_path),
        "closure_payload_sha256": str(closure["closure_payload_sha256"]),
        "checkpoint_count": EXPECTED_CHECKPOINTS,
        "record_count": EXPECTED_RECORDS,
        "generation_arm_counts": dict(
            sorted(Counter(str(row["generation_arm"]) for row in records).items())
        ),
        "minimum_checkpoint_boundary_free_memory_bytes": int(
            minimum_boundary_free_memory
        ),
        "enhanced_arm_metrics": enhanced_by_arm,
        "enhanced_template_metrics": template_metrics,
        "frozen_decision_gates": gates,
        "decision_evaluation": decision,
        "bias_scope": {
            "development_only": True,
            "sample_records_per_compared_arm": 84,
            "enhanced_template_count": 7,
            "oos_claim_allowed": False,
            "promotion_allowed": False,
            "phase_d_launched": False,
            "adaptive_budget_reallocation": False,
            "phase_c_financial_records_reused": False,
        },
        "validation_reads": 0,
        "holdout_reads": 0,
        "historical_2023_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
        "sealed_reads": 0,
        "phase_d_launched": False,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--freeze-root", required=True, type=Path)
    parser.add_argument("--audit-root", required=True, type=Path)
    parser.add_argument("--expected-runner-repo-sha", required=True)
    parser.add_argument("--audit-repo-sha", required=True)
    args = parser.parse_args(argv)
    if len(args.expected_runner_repo_sha) != EXPECTED_REPO_SHA_LENGTH:
        parser.error("expected-runner-repo-sha must be a full Git SHA")
    if len(args.audit_repo_sha) != EXPECTED_REPO_SHA_LENGTH:
        parser.error("audit-repo-sha must be a full Git SHA")
    audit_root = args.audit_root.resolve()
    if audit_root.exists():
        raise FileExistsError(f"allocator-repair audit root must be fresh: {audit_root}")
    audit_root.mkdir(parents=True)
    payload = audit(
        args.root.resolve(),
        args.freeze_root.resolve(),
        args.expected_runner_repo_sha,
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
