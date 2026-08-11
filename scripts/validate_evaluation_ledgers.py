from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path
from typing import Any


REQUIRED_ROLES = {"development", "challenge", "sealed", "spent", "forward"}


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _verify_contract(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    claimed = str(payload.get("contract_payload_sha256") or "")
    body = dict(payload)
    body.pop("contract_payload_sha256", None)
    actual = hashlib.sha256(
        json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if not claimed or claimed != actual:
        raise RuntimeError(f"contract self-hash mismatch: {path}")
    return payload


def validate_ledgers(role_registry: Path, access_ledger: Path, burn_ledger: Path) -> dict[str, Any]:
    registry = json.loads(role_registry.read_text(encoding="utf-8-sig"))
    roles = set(registry.get("roles", {}))
    if roles != REQUIRED_ROLES:
        raise RuntimeError(f"evaluation role registry must contain exactly {sorted(REQUIRED_ROLES)}")

    accesses = _read_csv(access_ledger)
    access_ids = [row.get("access_id", "") for row in accesses]
    if not all(access_ids) or len(access_ids) != len(set(access_ids)):
        raise RuntimeError("evaluation access IDs must be non-empty and unique")
    unknown_roles = sorted({row.get("data_role", "") for row in accesses} - REQUIRED_ROLES)
    if unknown_roles:
        raise RuntimeError(f"access ledger contains unknown roles: {unknown_roles}")
    forward_violations = [
        row
        for row in accesses
        if row.get("data_role") == "forward"
        and row.get("event_type") == "access"
        and row.get("granularity") not in {"", "none"}
    ]
    if forward_violations:
        raise RuntimeError("forward performance/data access is forbidden during EVALRESET")

    burns = _read_csv(burn_ledger)
    burn_ids = [row.get("burn_id", "") for row in burns]
    if not all(burn_ids) or len(burn_ids) != len(set(burn_ids)):
        raise RuntimeError("OOS burn IDs must be non-empty and unique")
    spent_assets = {row.get("asset") for row in burns if row.get("current_role") == "spent"}
    required_spent = {
        "global_split_manifest_validation",
        "global_split_manifest_holdout",
        "historical_challenge_2023_b05e2ca0",
    }
    if not required_spent.issubset(spent_assets):
        raise RuntimeError(
            "validation, holdout and the 2023 challenge must have spent burn records"
        )

    boundary = registry.get("feedback_boundary", {})
    if boundary.get("allowed_role") != "development" or boundary.get("allowed_optimizer_split") != "train":
        raise RuntimeError("feedback boundary must be development/train only")
    asset_states = registry.get("asset_states", {})
    spent_forward = asset_states.get("separate_2026_true1min_asset", {})
    if spent_forward.get("current_role") != "spent":
        raise RuntimeError("the opened 2026-01-05..2026-04-10 asset must remain spent")
    historical_challenge = asset_states.get("historical_challenge_2023_b05e2ca0", {})
    if (
        historical_challenge.get("current_role") != "spent"
        or int(historical_challenge.get("performance_rows_read") or 0) <= 0
        or historical_challenge.get("result") != "NEGATIVE"
        or historical_challenge.get("retry") != "FORBIDDEN"
        or historical_challenge.get("search_feedback") != "FORBIDDEN"
        or historical_challenge.get("promotion") != "FORBIDDEN"
        or historical_challenge.get("permanent_deny")
        != "PERMANENT_DENY_ALREADY_SPENT"
    ):
        raise RuntimeError("the 2023 challenge must remain spent and permanently denied")
    forward_b = asset_states.get("forward_b_tdx_lc1_20260413_20260514_f69cc84f", {})
    if forward_b.get("current_role") != "forward" or forward_b.get("performance_rows_read") != 0:
        raise RuntimeError("Forward-B must remain registered and performance-unopened")
    challenge_contract = _verify_contract(role_registry.parent / "cn_historical_challenge_2023_authorization.json")
    forward_b_contract = _verify_contract(role_registry.parent / "cn_forward_b_reservation_20260805.json")
    if challenge_contract.get("performance_rows_read_before_freeze") != 0:
        raise RuntimeError("2023 challenge contract must be frozen before performance access")
    access_started = json.loads(
        (role_registry.parent / "cn_historical_challenge_2023_access_started.json")
        .read_text(encoding="utf-8-sig")
    )
    outcome = json.loads(
        (
            role_registry.parent
            / "cn_fixed10_historical_challenge_2023_outcome_20260806.json"
        ).read_text(encoding="utf-8-sig")
    )
    if (
        access_started.get("data_role_after_transition") != "spent"
        or dict(outcome.get("access_decision") or {}).get(
            "historical_challenge_2023_state"
        )
        != "SPENT"
        or int(dict(outcome.get("provenance") or {}).get("historical_challenge_reads") or 0)
        <= 0
    ):
        raise RuntimeError("2023 spent authority evidence drift")
    if forward_b_contract.get("performance_access_authorized") is not False:
        raise RuntimeError("Forward-B reservation must not authorize performance access")
    return {
        "registry_version": registry.get("registry_version"),
        "role_count": len(roles),
        "access_count": len(accesses),
        "burn_count": len(burns),
        "forward_access_violation_count": 0,
        "validation_holdout_spent_records": 2,
        "spent_forward_2026_registered": True,
        "historical_challenge_2023_registered": True,
        "historical_challenge_2023_permanent_deny": True,
        "forward_b_registered": True,
        "contract_self_hashes_verified": 2,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--role-registry", type=Path, required=True)
    parser.add_argument("--access-ledger", type=Path, required=True)
    parser.add_argument("--burn-ledger", type=Path, required=True)
    args = parser.parse_args(argv)
    summary = validate_ledgers(args.role_registry, args.access_ledger, args.burn_ledger)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
