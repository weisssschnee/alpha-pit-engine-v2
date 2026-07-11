from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


REQUIRED_ROLES = {"development", "challenge", "sealed", "spent", "forward"}


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


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
    required_spent = {"global_split_manifest_validation", "global_split_manifest_holdout"}
    if not required_spent.issubset(spent_assets):
        raise RuntimeError("validation and holdout must both have explicit spent burn records")

    boundary = registry.get("feedback_boundary", {})
    if boundary.get("allowed_role") != "development" or boundary.get("allowed_optimizer_split") != "train":
        raise RuntimeError("feedback boundary must be development/train only")
    return {
        "registry_version": registry.get("registry_version"),
        "role_count": len(roles),
        "access_count": len(accesses),
        "burn_count": len(burns),
        "forward_access_violation_count": 0,
        "validation_holdout_spent_records": 2,
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
