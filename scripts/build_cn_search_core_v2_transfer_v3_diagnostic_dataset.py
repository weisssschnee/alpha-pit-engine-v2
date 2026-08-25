"""Build the consumed-evidence diagnostic dataset for Search Core V2 Transfer V3 research."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

from our_system_phase2.services.unified_capability_registry import stable_hash

STATUS = "SEARCH_CORE_V2_TRANSFER_V3_DIAGNOSTIC_DATASET_FROZEN_CONSUMED_VALIDATION_ONLY"
FEATURES = (
    "dev_matched_return",
    "dev_matched_reward",
    "dev_window_1_increment",
    "dev_window_2_increment",
    "dev_window_3_increment",
)


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def build(repo: Path) -> dict[str, Any]:
    repo = repo.resolve()
    rp = repo / "runtime/run_plans"
    d1_path = rp / "cn_program_optimizer_d1_transfer_v2_recovery_20260817/combined_183_dataset.json"
    old_members_path = rp / "cn_program_optimizer_d1_validation_candidate_members_20260816.jsonl"
    b_members_path = rp / "cn_program_optimizer_d1_transfer_B_validation_freeze_20260817/validation_candidate_members.jsonl"
    wave_dev_path = rp / "cn_search_core_v2_production_wave1_productive_candidates_412d51c_20260825.jsonl"
    wave_val_path = rp / "cn_search_core_v2_production_wave1_report_only_validation_results_0438beb_20260825.jsonl"
    wave_audit_path = rp / "cn_search_core_v2_production_wave1_report_only_validation_postrun_audit_20260825.json"

    d1 = _read(d1_path)
    d1_hash = _verify(d1, "dataset_payload_sha256", "D1 Transfer V2 recovered dataset")
    wave_audit = _read(wave_audit_path)
    wave_audit_hash = _verify(wave_audit, "audit_payload_sha256", "Wave1 validation audit")
    if (
        int(d1.get("candidate_count") or 0) != 183
        or int(d1.get("validation_productive_count") or 0) != 50
        or tuple(d1.get("development_window_ids") or ()) != ("development_1", "development_2", "development_3")
        or wave_audit.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE1_VALIDATION_POSTRUN_AUDIT_COMPLETE_TRANSFER_FAILED_NO_HOLDOUT"
    ):
        raise RuntimeError("Transfer V3 diagnostic source contract drift")

    metadata: dict[str, dict[str, Any]] = {}
    for path in (old_members_path, b_members_path):
        for row in _read_jsonl(path):
            metadata[str(row["exact_identity"])] = {
                "template_id": str(row["template_id"]),
                "selection_kind": str(row["selection_kind"]),
                "source_cohort": str(row["source_cohort"]),
            }
    d1_rows: list[dict[str, Any]] = []
    for row in d1["rows"]:
        exact = str(row["exact_identity"])
        meta = metadata.get(exact)
        if meta is None:
            raise RuntimeError(f"missing D1 template metadata: {exact}")
        d1_rows.append(
            {
                "exact_identity": exact,
                "source_cohort": str(row["source_cohort"]),
                "template_id": str(meta["template_id"]),
                "selection_kind": str(meta["selection_kind"]),
                **{feature: float(row[feature]) for feature in FEATURES},
                "validation_productive": bool(row["validation_productive"]),
                "validation_stable_2of3": None,
                "label_authority": "CONSUMED_HISTORICAL_D1_VALIDATION",
            }
        )

    wave_dev = {str(row["exact_identity"]): row for row in _read_jsonl(wave_dev_path)}
    wave_val = _read_jsonl(wave_val_path)
    if len(wave_dev) != 175 or len(wave_val) != 42:
        raise RuntimeError("Wave1 diagnostic source population drift")
    wave_rows: list[dict[str, Any]] = []
    for result in wave_val:
        exact = str(result["exact_identity"])
        dev = wave_dev[exact]
        credit = dict(dev["uplift"]["program_credit"])
        windows = list(map(float, credit["window_return_increments"]))
        ids = list(dev["admission"]["metrics"]["development_window_ids"])
        if ids != ["development_1", "development_2", "development_3"] or len(windows) != 3:
            raise RuntimeError("Wave1 development window feature drift")
        wave_rows.append(
            {
                "exact_identity": exact,
                "source_cohort": "SEARCH_CORE_V2_WAVE1",
                "template_id": str(result["template_id"]),
                "selection_kind": "SEMANTIC_STATE_JUMP_GENERATOR_V2",
                "dev_matched_return": float(credit["matched_cumulative_net_return_increment"]),
                "dev_matched_reward": float(credit["matched_net_reward_increment"]),
                "dev_window_1_increment": windows[0],
                "dev_window_2_increment": windows[1],
                "dev_window_3_increment": windows[2],
                "validation_productive": bool(result["validation_productive"]),
                "validation_stable_2of3": bool(result["validation_stable_2of3"]),
                "label_authority": "CONSUMED_WAVE1_REPORT_ONLY_VALIDATION",
            }
        )

    rows = d1_rows + wave_rows
    if len(rows) != 225 or len({str(row["exact_identity"]) for row in rows}) != 225:
        raise RuntimeError("Transfer V3 diagnostic dataset identity drift")
    cohort_counts: dict[str, int] = {}
    cohort_positives: dict[str, int] = {}
    for row in rows:
        cohort = str(row["source_cohort"])
        cohort_counts[cohort] = cohort_counts.get(cohort, 0) + 1
        cohort_positives[cohort] = cohort_positives.get(cohort, 0) + int(bool(row["validation_productive"]))

    payload = {
        "schema_version": "cn_search_core_v2_transfer_v3_diagnostic_dataset_v1",
        "status": STATUS,
        "features": list(FEATURES),
        "candidate_count": len(rows),
        "validation_productive_count": sum(bool(row["validation_productive"]) for row in rows),
        "cohort_counts": dict(sorted(cohort_counts.items())),
        "cohort_productive_counts": dict(sorted(cohort_positives.items())),
        "source_d1_dataset_payload_sha256": d1_hash,
        "source_wave1_validation_audit_payload_sha256": wave_audit_hash,
        "rows": rows,
        "research_boundaries": {
            "financial_evaluation_executed": False,
            "consumed_validation_labels_used_for_diagnostic_model_research": True,
            "prospective_claim_authorized": False,
            "same_validation_reuse_for_prospective_claim": False,
            "holdout_read": False,
            "forward_read": False,
            "promotion_authorized": False,
            "automatic_model_adoption_authorized": False,
        },
    }
    payload["dataset_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path, default=Path("runtime/run_plans/cn_search_core_v2_transfer_v3_diagnostic_dataset_20260825.json"))
    args = parser.parse_args(argv)
    payload = build(args.repo_root)
    output = args.output if args.output.is_absolute() else args.repo_root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": payload["status"], "candidate_count": payload["candidate_count"], "productive": payload["validation_productive_count"], "cohorts": payload["cohort_counts"], "payload": payload["dataset_payload_sha256"], "output": str(output.resolve())}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
