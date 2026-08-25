"""Independent post-run audit for Production Wave 1 report-only validation."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.services.unified_capability_registry import stable_hash

SCHEMA = "cn_search_core_v2_production_wave1_report_only_validation_postrun_audit_v1"
STATUS = "SEARCH_CORE_V2_PRODUCTION_WAVE1_VALIDATION_POSTRUN_AUDIT_COMPLETE_TRANSFER_FAILED_NO_HOLDOUT"
RUN_STATUS = "SEARCH_CORE_V2_PRODUCTION_WAVE1_REPORT_ONLY_VALIDATION_COMPLETE"
CANDIDATE_COUNT = 42


def _wilson(successes: int, total: int) -> dict[str, float]:
    if total <= 0:
        return {"lower": 0.0, "upper": 1.0}
    z = 1.959963984540054
    rate = successes / total
    denom = 1.0 + z * z / total
    center = rate + z * z / (2.0 * total)
    radius = z * math.sqrt(rate * (1.0 - rate) / total + z * z / (4.0 * total * total))
    return {"lower": (center - radius) / denom, "upper": (center + radius) / denom}


def _metric(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    n = len(rows)
    admitted = sum(bool(dict(row["admission"])["admitted"]) for row in rows)
    productive = sum(bool(row["validation_productive"]) for row in rows)
    stable_2of3 = sum(bool(row["validation_stable_2of3"]) for row in rows)
    stable_3of3 = sum(bool(row["validation_stable_3of3"]) for row in rows)
    return {
        "evaluated": n,
        "admitted": admitted,
        "productive": productive,
        "stable_2of3": stable_2of3,
        "stable_3of3": stable_3of3,
        "admission_rate": admitted / n if n else 0.0,
        "productive_transfer_rate": productive / n if n else 0.0,
        "stable_2of3_rate": stable_2of3 / n if n else 0.0,
        "stable_3of3_rate": stable_3of3 / n if n else 0.0,
        "productive_wilson_95": _wilson(productive, n),
        "stable_2of3_wilson_95": _wilson(stable_2of3, n),
    }


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8-sig").splitlines() if line.strip()]


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _verify(payload: Mapping[str, Any], field: str, label: str) -> str:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if not claimed or stable_hash(body) != claimed:
        raise RuntimeError(f"{label} self-hash drift")
    return claimed


def _median(values: Sequence[float | None]) -> float | None:
    clean = [float(v) for v in values if v is not None and math.isfinite(float(v))]
    return statistics.median(clean) if clean else None


def _mean(values: Sequence[float | None]) -> float | None:
    clean = [float(v) for v in values if v is not None and math.isfinite(float(v))]
    return statistics.mean(clean) if clean else None


def _rankdata(values: Sequence[float]) -> list[float]:
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i + 1
        while j < len(order) and values[order[j]] == values[order[i]]:
            j += 1
        rank = (i + 1 + j) / 2.0
        for k in range(i, j):
            ranks[order[k]] = rank
        i = j
    return ranks


def _pearson(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) < 2 or len(left) != len(right):
        return None
    lm = statistics.mean(left)
    rm = statistics.mean(right)
    ld = [value - lm for value in left]
    rd = [value - rm for value in right]
    denom = math.sqrt(sum(v * v for v in ld) * sum(v * v for v in rd))
    return sum(a * b for a, b in zip(ld, rd, strict=True)) / denom if denom else None


def _spearman(left: Sequence[float], right: Sequence[float]) -> float | None:
    return _pearson(_rankdata(left), _rankdata(right))


def audit(repo: Path) -> dict[str, Any]:
    repo = repo.resolve()
    rp = repo / "runtime/run_plans"
    closure_path = rp / "cn_search_core_v2_production_wave1_report_only_validation_complete_0438beb_20260825.json"
    results_path = rp / "cn_search_core_v2_production_wave1_report_only_validation_results_0438beb_20260825.jsonl"
    pairs_path = rp / "cn_search_core_v2_production_wave1_report_only_validation_pair_records_0438beb_20260825.jsonl"
    input_path = rp / "cn_search_core_v2_production_wave1_report_only_validation_input_binding_0438beb_20260825.json"
    exit_path = rp / "cn_search_core_v2_production_wave1_report_only_validation_exit_0438beb_20260825.json"
    identity_path = rp / "cn_search_core_v2_production_wave1_report_only_validation_execution_identity_0438beb_20260825.json"
    auth_path = rp / "cn_search_core_v2_production_wave1_report_only_validation_authorization_v1.json"
    prepared_path = rp / "cn_search_core_v2_production_wave1_validation_prepared_binding_20260825.json"
    freeze_path = rp / "cn_search_core_v2_production_wave1_validation_shortlist_freeze_20260825.json"
    members_path = rp / "cn_search_core_v2_production_wave1_validation_shortlist_members_20260825.jsonl"

    for path in (closure_path, results_path, pairs_path, input_path, exit_path, identity_path, auth_path, prepared_path, freeze_path, members_path):
        if not path.is_file():
            raise FileNotFoundError(path)

    closure = _read(closure_path)
    closure_hash = _verify(closure, "closure_payload_sha256", "Wave1 validation closure")
    auth = _read(auth_path)
    auth_hash = _verify(auth, "authorization_payload_sha256", "Wave1 validation authorization")
    prepared = _read(prepared_path)
    prepared_hash = _verify(prepared, "prepared_binding_payload_sha256", "Wave1 validation prepared binding")
    freeze = _read(freeze_path)
    freeze_hash = _verify(freeze, "freeze_payload_sha256", "Wave1 validation shortlist freeze")
    input_binding = _read(input_path)
    input_hash = _verify(input_binding, "input_binding_sha256", "Wave1 validation input binding")
    exit_receipt = _read(exit_path)
    identity = _read(identity_path)
    results = _read_jsonl(results_path)
    pairs = _read_jsonl(pairs_path)
    members = _read_jsonl(members_path)

    if (
        closure.get("status") != RUN_STATUS
        or closure.get("repo_sha") != "0438beb4b8aea662f4ca5f5c046d6e5689f85e8a"
        or int(closure.get("candidate_count") or 0) != CANDIDATE_COUNT
        or closure.get("evaluation_data_role") != "VALIDATION_REPORT_ONLY"
        or closure.get("candidate_evaluation_executed") is not True
        or closure.get("optimizer_feedback_write") != "FORBIDDEN"
        or closure.get("policy_memory_write") != "FORBIDDEN"
        or closure.get("scheduler_write") != "FORBIDDEN"
        or closure.get("archive_write") != "FORBIDDEN"
        or closure.get("threshold_tuning_allowed") is not False
        or closure.get("same_slice_reselection_allowed") is not False
        or closure.get("promotion_authorized") is not False
        or closure.get("automatic_followon_authorized") is not False
        or int(closure.get("holdout_reads") or 0) != 0
        or int(closure.get("forward_b_reads") or 0) != 0
        or int(closure.get("forward_2026_reads") or 0) != 0
        or closure.get("oos_authority") != "VALIDATION_REPORT_ONLY_EVIDENCE_ONLY"
        or str(closure.get("authorization_payload_sha256") or "") != auth_hash
        or str(closure.get("prepared_binding_payload_sha256") or "") != prepared_hash
        or str(closure.get("input_binding_sha256") or "") != input_hash
        or str(closure.get("results_file_sha256") or "") != _sha(results_path)
    ):
        raise RuntimeError("Wave1 validation closure contract drift")

    if (
        prepared.get("candidate_evaluation_executed") is not False
        or prepared.get("candidate_results_generated") is not False
        or int(prepared.get("candidate_count") or 0) != CANDIDATE_COUNT
        or freeze.get("membership_frozen_before_validation") is not True
        or freeze.get("same_slice_reselection_allowed") is not False
        or freeze.get("threshold_tuning_allowed") is not False
        or int(freeze.get("candidate_count") or 0) != CANDIDATE_COUNT
    ):
        raise RuntimeError("Wave1 validation pre-evaluation freeze drift")

    if len(results) != 42 or len(pairs) != 42 or len(members) != 42:
        raise RuntimeError("Wave1 validation result cardinality drift")
    if not all(_verify(row, "result_payload_sha256", "validation result") for row in results):
        raise RuntimeError("unreachable")
    if not all(_verify(row, "record_payload_sha256", "validation pair") for row in pairs):
        raise RuntimeError("unreachable")
    for member in members:
        _verify(member, "member_payload_sha256", "validation shortlist member")
    result_exacts = [str(row["exact_identity"]) for row in results]
    pair_exacts = [str(row["exact_identity"]) for row in pairs]
    member_exacts = [str(row["exact_identity"]) for row in members]
    if (
        result_exacts != member_exacts
        or pair_exacts != member_exacts
        or len(set(result_exacts)) != 42
        or stable_hash(member_exacts) != str(freeze["candidate_exact_identities_sha256"])
        or any(str(result["pair_record_sha256"]) != str(pair["record_payload_sha256"]) for result, pair in zip(results, pairs, strict=True))
    ):
        raise RuntimeError("Wave1 validation identity/order binding drift")

    recomputed_total = _metric(results)
    recomputed_per_template = {
        template: _metric([row for row in results if str(row["template_id"]) == template])
        for template in sorted({str(row["template_id"]) for row in results})
    }
    if recomputed_total != dict(closure["metrics"]["total"]) or recomputed_per_template != dict(closure["metrics"]["per_template"]):
        raise RuntimeError("Wave1 validation metric replay drift")

    if (
        input_binding.get("authorization_payload_sha256") != auth_hash
        or input_binding.get("prepared_binding_payload_sha256") != prepared_hash
        or input_binding.get("shortlist_freeze_payload_sha256") != freeze_hash
        or int(input_binding.get("holdout_reads") or 0) != 0
        or int(input_binding.get("forward_b_reads") or 0) != 0
        or int(input_binding.get("forward_2026_reads") or 0) != 0
        or input_binding.get("threshold_tuning_allowed") is not False
        or input_binding.get("same_slice_reselection_allowed") is not False
    ):
        raise RuntimeError("Wave1 validation input authority drift")

    if (
        int(exit_receipt.get("exit_code", -1)) != 0
        or str(exit_receipt.get("repo_sha") or "") != str(closure["repo_sha"])
        or str(identity.get("repo_sha") or "") != str(closure["repo_sha"])
        or str(identity.get("target_campaign_id") or "") != "cn-search-core-v2-production-wave1-report-only-validation-v1"
        or str(identity.get("campaign_authorization_file_sha256") or "") != _sha(auth_path)
    ):
        raise RuntimeError("Wave1 validation execution identity drift")

    failure_reasons: dict[str, int] = {}
    matched_rows: list[dict[str, Any]] = []
    windows = {name: [] for name in ("validation_1", "validation_2", "validation_3")}
    member_by_exact = {str(row["exact_identity"]): row for row in members}
    for result, pair in zip(results, pairs, strict=True):
        if not bool(result["admission"]["admitted"]):
            for reason in result["admission"].get("failure_reasons") or ():
                failure_reasons[str(reason)] = failure_reasons.get(str(reason), 0) + 1
        primary = pair.get("primary")
        control = pair.get("base_control")
        window_increments: list[float] = []
        if primary is not None and control is not None:
            pm = {str(row["window_id"]): row for row in primary["development_subwindows"]}
            cm = {str(row["window_id"]): row for row in control["development_subwindows"]}
            for name in windows:
                increment = float(pm[name]["cumulative_net_return"]) - float(cm[name]["cumulative_net_return"])
                windows[name].append(increment)
                window_increments.append(increment)
        matched_rows.append(
            {
                "exact_identity": str(result["exact_identity"]),
                "template_id": str(result["template_id"]),
                "development_rank": int(result["development_rank"]),
                "development_increment": float(result["development_credit"]["matched_cumulative_net_return_increment"]),
                "admitted": bool(result["admission"]["admitted"]),
                "productive": bool(result["validation_productive"]),
                "stable_2of3": bool(result["validation_stable_2of3"]),
                "stable_3of3": bool(result["validation_stable_3of3"]),
                "primary_return": None if primary is None else float(primary["cumulative_net_return"]),
                "control_return": None if control is None else float(control["cumulative_net_return"]),
                "matched_return_increment": pair.get("matched_cumulative_return_increment"),
                "matched_reward_increment": pair.get("matched_net_reward_increment"),
                "window_return_increments": window_increments,
            }
        )

    complete = [row for row in matched_rows if row["matched_return_increment"] is not None]
    window_stats = {
        name: {
            "count": len(values),
            "positive_count": sum(float(value) > 0.0 for value in values),
            "positive_rate": sum(float(value) > 0.0 for value in values) / len(values) if values else 0.0,
            "median_increment": _median(values),
            "mean_increment": _mean(values),
        }
        for name, values in windows.items()
    }
    dev_increments = [float(row["development_increment"]) for row in complete]
    val_increments = [float(row["matched_return_increment"]) for row in complete]
    development_ranks = [float(row["development_rank"]) for row in complete]
    primary_returns = [float(row["primary_return"]) for row in complete]
    ranked = sorted(matched_rows, key=lambda row: int(row["development_rank"]))
    halves = {}
    for label, rows in (("top21", ranked[:21]), ("bottom21", ranked[21:])):
        halves[label] = {
            "admitted": sum(bool(row["admitted"]) for row in rows),
            "productive": sum(bool(row["productive"]) for row in rows),
            "stable_2of3": sum(bool(row["stable_2of3"]) for row in rows),
            "median_validation_increment": _median([row["matched_return_increment"] for row in rows]),
        }

    headline = dict(recomputed_total)
    transfer_failed = (
        int(headline["productive"]) == 8
        and int(headline["stable_2of3"]) == 5
        and int(headline["stable_3of3"]) == 0
    )
    if not transfer_failed:
        raise RuntimeError("Wave1 validation classification contract drift")

    survivors_productive = [
        {
            "exact_identity": row["exact_identity"],
            "template_id": row["template_id"],
            "development_rank": row["development_rank"],
            "stable_2of3": row["stable_2of3"],
            "matched_return_increment": row["matched_return_increment"],
            "matched_reward_increment": row["matched_reward_increment"],
            "window_return_increments": row["window_return_increments"],
        }
        for row in matched_rows
        if row["productive"]
    ]

    payload = {
        "schema_version": SCHEMA,
        "status": STATUS,
        "execution": {
            "repo_sha": str(closure["repo_sha"]),
            "closure_file_sha256": _sha(closure_path),
            "closure_payload_sha256": closure_hash,
            "results_file_sha256": _sha(results_path),
            "pair_records_file_sha256": _sha(pairs_path),
            "input_binding_file_sha256": _sha(input_path),
            "input_binding_payload_sha256": input_hash,
            "exit_receipt_file_sha256": _sha(exit_path),
            "execution_identity_file_sha256": _sha(identity_path),
            "exit_code": int(exit_receipt["exit_code"]),
        },
        "frozen_authority": {
            "authorization_payload_sha256": auth_hash,
            "prepared_binding_payload_sha256": prepared_hash,
            "shortlist_freeze_payload_sha256": freeze_hash,
            "candidate_exact_identities_sha256": str(freeze["candidate_exact_identities_sha256"]),
            "candidate_count": 42,
            "membership_frozen_before_validation": True,
            "threshold_tuning_allowed": False,
            "same_slice_reselection_allowed": False,
        },
        "validation_transfer": {
            "metrics": headline,
            "absolute_rejected_count": 42 - int(headline["admitted"]),
            "admitted_but_nonproductive_count": int(headline["admitted"]) - int(headline["productive"]),
            "failure_reason_counts": dict(sorted(failure_reasons.items())),
            "primary_positive_cumulative_return_count": sum(row["primary_return"] is not None and float(row["primary_return"]) > 0 for row in matched_rows),
            "matched_return_increment_positive_count": sum(row["matched_return_increment"] is not None and float(row["matched_return_increment"]) > 0 for row in matched_rows),
            "matched_reward_increment_positive_count": sum(row["matched_reward_increment"] is not None and float(row["matched_reward_increment"]) > 0 for row in matched_rows),
            "matched_both_positive_count": sum(row["matched_return_increment"] is not None and row["matched_reward_increment"] is not None and float(row["matched_return_increment"]) > 0 and float(row["matched_reward_increment"]) > 0 for row in matched_rows),
            "median_primary_return": _median([row["primary_return"] for row in matched_rows]),
            "median_control_return": _median([row["control_return"] for row in matched_rows]),
            "median_matched_return_increment": _median([row["matched_return_increment"] for row in matched_rows]),
            "mean_matched_return_increment": _mean([row["matched_return_increment"] for row in matched_rows]),
            "window_matched_increments": window_stats,
            "per_template": recomputed_per_template,
            "productive_candidates": survivors_productive,
        },
        "development_to_validation_diagnostic": {
            "spearman_development_increment_vs_validation_increment": _spearman(dev_increments, val_increments),
            "spearman_development_rank_vs_validation_increment": _spearman(development_ranks, val_increments),
            "spearman_development_rank_vs_primary_return": _spearman(development_ranks, primary_returns),
            "development_rank_halves": halves,
            "interpretation": "DEVELOPMENT_PRIORITY_HAS_WEAK_OR_NEGATIVE_TRANSFER_SIGNAL_ON_CONSUMED_VALIDATION_SLICE",
        },
        "authority_boundaries": {
            "validation_domain_now_consumed_for_candidate_evaluation": True,
            "same_validation_slice_reuse_for_prospective_claim": False,
            "optimizer_feedback_write": "FORBIDDEN",
            "policy_memory_write": "FORBIDDEN",
            "scheduler_write": "FORBIDDEN",
            "archive_write": "FORBIDDEN",
            "holdout_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
            "promotion_authorized": False,
            "automatic_followon_authorized": False,
            "oos_authority": "NONE_AFTER_REPORT_ONLY_VALIDATION_REVIEW",
        },
        "project_control_recommendation": {
            "search_policy": "RETAIN_BOOTSTRAP_PRIMITIVE_THEN_MATURE_STATE_JUMP_FOR_DEVELOPMENT_SEARCH_ONLY",
            "validation_transfer": "FAILED",
            "validation_shortlist_policy": "FAILED_AS_ALPHA_TRANSFER_SELECTOR",
            "holdout_action": "DO_NOT_READ",
            "next_action": "VALIDATION_TRANSFER_AUTOPSY_DIAGNOSTIC_ONLY_NO_PROSPECTIVE_CLAIM",
        },
    }
    payload["audit_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output", type=Path, default=Path("runtime/run_plans/cn_search_core_v2_production_wave1_report_only_validation_postrun_audit_20260825.json"))
    args = parser.parse_args(argv)
    payload = audit(args.repo_root)
    output = args.output if args.output.is_absolute() else args.repo_root / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": payload["status"], "productive": payload["validation_transfer"]["metrics"]["productive"], "stable_2of3": payload["validation_transfer"]["metrics"]["stable_2of3"], "stable_3of3": payload["validation_transfer"]["metrics"]["stable_3of3"], "audit_payload_sha256": payload["audit_payload_sha256"], "output": str(output.resolve())}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
