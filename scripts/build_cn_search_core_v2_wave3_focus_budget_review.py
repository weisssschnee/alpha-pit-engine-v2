"""Freeze a development-only focused Wave 3 budget from existing CN yield gates."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

from our_system_phase2.services.unified_capability_registry import stable_hash

STATUS = "SEARCH_CORE_V2_WAVE3_FOCUS_BUDGET_FROZEN_DEVELOPMENT_ONLY"
GATE_PLAN = Path("runtime/run_plans/cn_program_primitive_market_successor_v3_plan.json")
WAVE2_TERMINAL = Path("runtime/run_plans/cn_search_core_v2_production_wave2_complete_57f0abd_20260826.json")
WAVE2_AUDIT = Path("runtime/run_plans/cn_search_core_v2_production_wave2_postrun_audit_20260826.json")
WAVE2_PREFREEZE = Path("runtime/run_plans/cn_search_core_v2_production_wave2_prefreeze_20260825.json")
TEMPLATES = (
    "BASE_EVENT",
    "BASE_MARKET",
    "BASE_MARKET_EVENT",
    "BASE_TEMPORAL",
    "BASE_TEMPORAL_EVENT",
    "BASE_TEMPORAL_MARKET",
    "BASE_TEMPORAL_MARKET_EVENT",
)
ROUNDS = 2
BATCH_SIZE = 24


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def build(repo: Path) -> dict[str, Any]:
    repo = repo.resolve()
    gate_path = repo / GATE_PLAN
    terminal_path = repo / WAVE2_TERMINAL
    audit_path = repo / WAVE2_AUDIT
    prefreeze_path = repo / WAVE2_PREFREEZE
    gate_plan = _read(gate_path)
    terminal = _read(terminal_path)
    audit = _read(audit_path)
    prefreeze = _read(prefreeze_path)
    gate_hash = _verify(gate_plan, "plan_payload_sha256", "Primitive Market Successor V3 plan")
    terminal_hash = _verify(terminal, "closure_payload_sha256", "Production Wave 2 terminal")
    audit_hash = _verify(audit, "audit_payload_sha256", "Production Wave 2 postrun audit")
    prefreeze_hash = _verify(prefreeze, "prefreeze_payload_sha256", "Production Wave 2 prefreeze")
    if (
        gate_plan.get("status") != "PRIMITIVE_MARKET_SUCCESSOR_V3_PLAN_FROZEN_NOT_RUN"
        or terminal.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE2_COMPLETE"
        or audit.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE2_POSTRUN_AUDIT_COMPLETE_DEVELOPMENT_ARCHIVE_READY"
        or prefreeze.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE2_PREFROZEN_BEFORE_FINANCIAL_READ"
        or terminal.get("evaluation_data_role") != "DEVELOPMENT_ONLY"
        or terminal.get("validation_feedback_used") is not False
        or any(int(value) != 0 for value in dict(terminal.get("restricted_reads") or {}).values())
        or audit["project_control_recommendation"].get("automatic_validation_authorized") is not False
    ):
        raise RuntimeError("Wave3 focus source authority drift")

    gate = dict(gate_plan["prospective_gate"])
    productive_min = float(gate["each_focus_template_primitive_productive_rate_min"])
    stable_min = float(gate["each_focus_template_primitive_uplift_stable_2of3_rate_min"])
    overall = dict(terminal["overall_metrics"])
    aggregate_checks = {
        "productive_rate": float(overall["productive_rate"]) >= float(gate["focus_primitive_productive_rate_min"]),
        "stable_rate": float(overall["stable_rate"]) >= float(gate["focus_primitive_uplift_stable_2of3_rate_min"]),
        "behavior_pair_rate": float(overall["distinct_behavior_pair_rate"]) >= float(gate["behavior_pair_rate_min"]),
        "total_productive": int(overall["productive"]) >= int(gate["total_productive_count_min"]),
    }
    if not all(aggregate_checks.values()):
        raise RuntimeError(f"Wave2 aggregate yield does not justify focused continuation: {aggregate_checks}")

    decisions: dict[str, Any] = {}
    active: list[str] = []
    held: list[str] = []
    for template in TEMPLATES:
        metric = dict(terminal["per_template_metrics"][template])
        checks = {
            "productive_rate": float(metric["productive_rate"]) >= productive_min,
            "stable_rate": float(metric["stable_rate"]) >= stable_min,
        }
        keep = all(checks.values())
        (active if keep else held).append(template)
        decisions[template] = {
            "productive_rate": float(metric["productive_rate"]),
            "stable_rate": float(metric["stable_rate"]),
            "distinct_behavior_pair_rate": float(metric["distinct_behavior_pair_rate"]),
            "checks": checks,
            "wave3_role": "ACTIVE_SEARCH" if keep else "HOLD_NO_FINANCIAL_BUDGET",
        }

    expected_active = [
        "BASE_EVENT",
        "BASE_MARKET_EVENT",
        "BASE_TEMPORAL_EVENT",
        "BASE_TEMPORAL_MARKET",
        "BASE_TEMPORAL_MARKET_EVENT",
    ]
    expected_held = ["BASE_MARKET", "BASE_TEMPORAL"]
    if active != expected_active or held != expected_held:
        raise RuntimeError(f"Wave3 focus template geometry drift: active={active} held={held}")

    total = ROUNDS * BATCH_SIZE * len(active)
    pair_contract = dict(prefreeze["pair_native_annotation_contract"])
    if (
        pair_contract.get("application_scope") != "DEVELOPMENT_RESULT_ANNOTATION_AND_ARCHIVE_ONLY"
        or pair_contract.get("optimizer_feedback_write") is not False
        or pair_contract.get("automatic_policy_adoption") is not False
        or pair_contract.get("validation_label_read_at_application") is not False
    ):
        raise RuntimeError("Wave3 pair-native annotation authority drift")

    payload = {
        "schema_version": "cn_search_core_v2_wave3_focus_budget_review_v1",
        "status": STATUS,
        "source_gate": {
            "relative_path": str(GATE_PLAN).replace("\\", "/"),
            "file_sha256": _sha(gate_path),
            "payload_sha256": gate_hash,
            "productive_rate_min": productive_min,
            "stable_rate_min": stable_min,
            "aggregate_gate": {
                "productive_rate_min": float(gate["focus_primitive_productive_rate_min"]),
                "stable_rate_min": float(gate["focus_primitive_uplift_stable_2of3_rate_min"]),
                "behavior_pair_rate_min": float(gate["behavior_pair_rate_min"]),
                "total_productive_count_min": int(gate["total_productive_count_min"]),
            },
        },
        "source_wave2_terminal": {
            "relative_path": str(WAVE2_TERMINAL).replace("\\", "/"),
            "file_sha256": _sha(terminal_path),
            "payload_sha256": terminal_hash,
        },
        "source_wave2_postrun_audit": {
            "relative_path": str(WAVE2_AUDIT).replace("\\", "/"),
            "file_sha256": _sha(audit_path),
            "payload_sha256": audit_hash,
        },
        "source_wave2_prefreeze": {
            "relative_path": str(WAVE2_PREFREEZE).replace("\\", "/"),
            "file_sha256": _sha(prefreeze_path),
            "payload_sha256": prefreeze_hash,
        },
        "aggregate_checks": aggregate_checks,
        "template_decisions": decisions,
        "active_templates": active,
        "held_templates": held,
        "budget": {
            "rounds": ROUNDS,
            "batch_size": BATCH_SIZE,
            "per_active_template_evaluations": ROUNDS * BATCH_SIZE,
            "total_financial_evaluations": total,
            "checkpoint_count": ROUNDS * len(active),
            "reduction_vs_equal_7_template_wave": 1.0 - total / 336.0,
        },
        "pair_native_annotation_contract": pair_contract,
        "evaluation_data_role": "DEVELOPMENT_ONLY",
        "validation_read": False,
        "holdout_read": False,
        "forward_read": False,
        "promotion_authorized": False,
        "automatic_validation_authorized": False,
        "financial_evaluation_executed_by_review": False,
    }
    payload["focus_review_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("runtime/run_plans/cn_search_core_v2_wave3_focus_budget_review_20260826.json"),
    )
    args = parser.parse_args(argv)
    payload = build(args.repo_root)
    output = args.output if args.output.is_absolute() else args.repo_root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "status": payload["status"],
                "active": payload["active_templates"],
                "held": payload["held_templates"],
                "total": payload["budget"]["total_financial_evaluations"],
                "reduction": payload["budget"]["reduction_vs_equal_7_template_wave"],
                "payload": payload["focus_review_payload_sha256"],
                "output": str(output.resolve()),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
