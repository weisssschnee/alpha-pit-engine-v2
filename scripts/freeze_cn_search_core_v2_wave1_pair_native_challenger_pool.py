"""Freeze the Wave1 pair-native challenger pool using DEVELOPMENT_ONLY evidence.

This step applies the already-frozen pair-native challenger rule to all 175
Wave1 development-productive candidates.  It does not read validation labels,
holdout, or forward data and does not authorize evaluation or promotion.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping

from our_system_phase2.services.unified_capability_registry import stable_hash

STATUS = "SEARCH_CORE_V2_WAVE1_PAIR_NATIVE_CHALLENGER_POOL_FROZEN_DEVELOPMENT_ONLY_UNSEEN_TEST_REQUIRED"
RULE_ID = "PAIR_NATIVE_PRIMARY_3OF3_CONTROL_2OF3_POSITIVE_V1"


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


def build(repo: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    repo = repo.resolve()
    rp = repo / "runtime/run_plans"
    freeze_path = rp / "cn_search_core_v2_production_wave1_evidence_freeze_20260825.json"
    productive_path = rp / "cn_search_core_v2_production_wave1_productive_candidates_412d51c_20260825.jsonl"
    pair_path = rp / "cn_search_core_v2_wave1_development_pair_windows_productive175_20260825.json"
    challenger_path = rp / "cn_search_core_v2_pair_native_qualification_challenger_v1_20260825.json"

    freeze = _read(freeze_path)
    pair = _read(pair_path)
    challenger = _read(challenger_path)
    freeze_hash = _verify(freeze, "evidence_freeze_payload_sha256", "Wave1 evidence freeze")
    pair_hash = _verify(pair, "evidence_payload_sha256", "Wave1 productive pair evidence")
    challenger_hash = _verify(challenger, "challenger_payload_sha256", "pair-native challenger")
    if freeze.get("status") != "SEARCH_CORE_V2_PRODUCTION_WAVE1_EVIDENCE_FROZEN_DEVELOPMENT_ONLY":
        raise RuntimeError("Wave1 evidence freeze status drift")
    if challenger.get("status") != "PAIR_NATIVE_CONTROL_ROBUST_CHALLENGER_V1_FROZEN_DIAGNOSTIC_ONLY_UNSEEN_TEST_REQUIRED":
        raise RuntimeError("pair-native challenger status drift")
    if challenger.get("rule_id") != RULE_ID:
        raise RuntimeError("pair-native challenger rule id drift")
    rule = dict(challenger["rule_contract"])
    if (
        int(rule["primary_positive_development_window_count_required"]) != 3
        or int(rule["control_positive_development_window_count_minimum"]) != 2
        or int(rule["development_window_count"]) != 3
        or rule.get("uses_validation_features") is not False
        or rule.get("uses_learned_parameters") is not False
    ):
        raise RuntimeError("pair-native challenger rule contract drift")

    productive_authority = dict(freeze["artifacts"]["productive_candidates"])
    if (
        Path(str(productive_authority["relative_path"])).as_posix()
        != "runtime/run_plans/cn_search_core_v2_production_wave1_productive_candidates_412d51c_20260825.jsonl"
        or _sha(productive_path) != str(productive_authority["file_sha256"])
        or int(freeze["productive_candidate_count"]) != 175
    ):
        raise RuntimeError("Wave1 productive archive authority drift")
    productive = _read_jsonl(productive_path)
    pair_rows = list(pair["rows"])
    if (
        len(productive) != 175
        or len(pair_rows) != 175
        or int(pair["candidate_count"]) != 175
        or pair.get("targeted_shortlist") is not True
        or pair["research_boundaries"].get("financial_evaluation_performed") is not False
        or pair["research_boundaries"].get("validation_read") is not False
    ):
        raise RuntimeError("Wave1 productive pair evidence geometry drift")
    productive_by = {str(row["exact_identity"]): row for row in productive}
    pair_by = {str(row["exact_identity"]): row for row in pair_rows}
    if len(productive_by) != 175 or len(pair_by) != 175 or set(productive_by) != set(pair_by):
        raise RuntimeError("Wave1 productive/pair exact-set drift")

    members: list[dict[str, Any]] = []
    for exact in sorted(productive_by):
        dev = productive_by[exact]
        pair_row = pair_by[exact]
        if str(dev["template_id"]) != str(pair_row["template_id"]):
            raise RuntimeError(f"Wave1 challenger template drift: {exact}")
        primary = list(map(float, pair_row["primary_window_returns"]))
        control = list(map(float, pair_row["control_window_returns"]))
        if len(primary) != 3 or len(control) != 3:
            raise RuntimeError(f"Wave1 challenger pair window drift: {exact}")
        primary_positive = sum(value > 0.0 for value in primary)
        control_positive = sum(value > 0.0 for value in control)
        if primary_positive != 3 or control_positive < 2:
            continue
        credit = dict(dict(dev.get("uplift") or {}).get("program_credit") or {})
        member = {
            "schema_version": "cn_search_core_v2_wave1_pair_native_challenger_member_v1",
            "exact_identity": exact,
            "template_id": str(dev["template_id"]),
            "behavior_pair_identity": str(dev.get("behavior_pair_identity") or ""),
            "structural_region_identity": str(dev["structural_region_identity"]),
            "development_stable": bool(dev["stable"]),
            "checkpoint_ordinal": int(dev["checkpoint_ordinal"]),
            "production_round_index": int(dev["production_round_index"]),
            "primary_positive_window_count": primary_positive,
            "control_positive_window_count": control_positive,
            "primary_cumulative_net_return": float(pair_row["primary_cumulative_net_return"]),
            "control_cumulative_net_return": float(pair_row["control_cumulative_net_return"]),
            "matched_cumulative_return_increment": float(pair_row["matched_cumulative_return_increment"]),
            "primary_window_returns": primary,
            "control_window_returns": control,
            "matched_window_return_increments": list(map(float, pair_row["matched_window_return_increments"])),
            "development_matched_net_reward_increment": float(credit["matched_net_reward_increment"]),
            "source_development_result_payload_sha256": str(dev["result_payload_sha256"]),
            "source_pair_record_payload_sha256": str(pair_row["source_record_payload_sha256"]),
            "rule_id": RULE_ID,
            "classification": "FUTURE_UNSEEN_DOMAIN_CHALLENGER_MEMBER_NOT_ALPHA_QUALIFIED",
            "validation_read": False,
            "holdout_read": False,
            "forward_read": False,
            "alpha_qualified": False,
        }
        member["member_payload_sha256"] = stable_hash(member)
        members.append(member)

    if len(members) != 30 or len({row["exact_identity"] for row in members}) != 30:
        raise RuntimeError(f"Wave1 pair-native challenger pool cardinality drift: {len(members)}")
    template_counts = dict(sorted(Counter(str(row["template_id"]) for row in members).items()))
    expected_template_counts = {
        "BASE_EVENT": 5,
        "BASE_MARKET_EVENT": 7,
        "BASE_TEMPORAL_EVENT": 7,
        "BASE_TEMPORAL_MARKET": 8,
        "BASE_TEMPORAL_MARKET_EVENT": 3,
    }
    if template_counts != expected_template_counts:
        raise RuntimeError(f"Wave1 pair-native challenger template geometry drift: {template_counts}")

    payload = {
        "schema_version": "cn_search_core_v2_wave1_pair_native_challenger_pool_freeze_v1",
        "status": STATUS,
        "rule_id": RULE_ID,
        "source_wave1_evidence_freeze_payload_sha256": freeze_hash,
        "source_productive_archive_file_sha256": _sha(productive_path),
        "source_pair_evidence_payload_sha256": pair_hash,
        "source_challenger_payload_sha256": challenger_hash,
        "source_development_productive_count": 175,
        "candidate_count": len(members),
        "coverage": len(members) / 175,
        "candidate_exact_identities": [str(row["exact_identity"]) for row in members],
        "candidate_exact_identities_sha256": stable_hash([str(row["exact_identity"]) for row in members]),
        "candidate_member_payloads_sha256": stable_hash([str(row["member_payload_sha256"]) for row in members]),
        "development_stable_count": sum(bool(row["development_stable"]) for row in members),
        "template_counts": template_counts,
        "missing_templates": ["BASE_MARKET", "BASE_TEMPORAL"],
        "membership_frozen_before_next_unseen_evaluation": True,
        "classification": "DEVELOPMENT_ONLY_FUTURE_UNSEEN_CHALLENGER_NOT_ALPHA_QUALIFIED",
        "authority_boundaries": {
            "validation_read": False,
            "holdout_read": False,
            "forward_read": False,
            "candidate_evaluation_executed": False,
            "same_consumed_wave1_validation_retest_authorized": False,
            "automatic_promotion_authorized": False,
            "current_search_policy_changed": False,
            "current_validation_shortlist_policy_changed": False,
            "next_required_evidence": "DISTINCT_UNSEEN_EVALUATION_DOMAIN",
        },
    }
    payload["freeze_payload_sha256"] = stable_hash(payload)
    return payload, members


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("runtime/run_plans/cn_search_core_v2_wave1_pair_native_challenger_pool_freeze_20260825.json"),
    )
    parser.add_argument(
        "--members-output",
        type=Path,
        default=Path("runtime/run_plans/cn_search_core_v2_wave1_pair_native_challenger_pool_members_20260825.jsonl"),
    )
    args = parser.parse_args(argv)
    payload, members = build(args.repo_root)
    output = args.output if args.output.is_absolute() else args.repo_root / args.output
    members_output = args.members_output if args.members_output.is_absolute() else args.repo_root / args.members_output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    members_output.write_text(
        "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in members),
        encoding="utf-8",
    )
    print(json.dumps({
        "status": payload["status"],
        "count": payload["candidate_count"],
        "coverage": payload["coverage"],
        "templates": payload["template_counts"],
        "payload": payload["freeze_payload_sha256"],
        "output": str(output.resolve()),
        "members_output": str(members_output.resolve()),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
