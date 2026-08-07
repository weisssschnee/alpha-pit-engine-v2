"""Independently audit Phase-B information screening and schedule capacity."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from our_system_phase2.runtime.cn_joint_program_phase_b_v0 import (
    TEMPLATE_ORDER,
    verify_phase_b_prefinancial_freeze_v0,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


def _read(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def audit(root: Path, output: Path) -> dict[str, Any]:
    root = root.resolve()
    closure = verify_phase_b_prefinancial_freeze_v0(root)
    screen_path = root / "component_information_coverage_screen.json"
    screen = _read(screen_path)
    contract = _read(root / "phase_b_run_contract.json")
    rejected = list(screen["rejected_components"])
    rejected_fields = sorted(
        {
            str(field_id)
            for row in rejected
            for field_id in row["rejected_field_ids"]
        }
    )
    rejected_routes = sorted({str(row["route_id"]) for row in rejected})
    if (
        len(rejected) != 4
        or rejected_fields != ["ctx_zls_df_num"]
        or rejected_routes != ["MARKET_REGIME_CONDITION"]
    ):
        raise ValueError("Phase B information rejection set drift")
    role_capacity = {
        str(role): int(count)
        for role, count in dict(screen["role_counts_after_screen"]).items()
    }
    if any(count < 8 for count in role_capacity.values()):
        raise ValueError("Phase B information-qualified role capacity underfill")
    quotas = {
        str(template): int(count)
        for template, count in dict(contract["template_quotas"]).items()
    }
    if quotas != {template: 8 for template in TEMPLATE_ORDER}:
        raise ValueError("Phase B information-qualified template quota drift")
    if any(
        int(screen.get(key) or 0)
        for key in (
            "validation_reads",
            "holdout_reads",
            "historical_2023_reads",
            "forward_b_reads",
            "forward_2026_reads",
        )
    ) or bool(screen.get("financial_evaluation_executed")):
        raise PermissionError("Phase B information screen used prohibited data")

    payload: dict[str, Any] = {
        "schema_version": "cn_joint_program_phase_b_information_audit_v1",
        "status": "CN_JOINT_PROGRAM_PHASE_B_INFORMATION_AUDIT_PASS",
        "root": str(root),
        "closure_file_sha256": _sha256(
            root
            / "CN_JOINT_PROGRAM_PHASE_B_PREFINANCIAL_FREEZE_COMPLETE.json"
        ),
        "closure_payload_sha256": str(closure["closure_sha256"]),
        "information_screen_file_sha256": _sha256(screen_path),
        "information_screen_payload_sha256": str(
            screen["information_screen_sha256"]
        ),
        "component_count_before_screen": int(
            screen["component_count_before_screen"]
        ),
        "component_count_after_screen": int(
            screen["component_count_after_screen"]
        ),
        "rejected_component_count": len(rejected),
        "rejected_component_ids": [str(row["component_id"]) for row in rejected],
        "rejected_field_ids": rejected_fields,
        "rejected_route_ids": rejected_routes,
        "role_capacity_after_screen": role_capacity,
        "template_quotas": quotas,
        "semantic_substitution_used": False,
        "adaptive_feedback_used": False,
        "financial_evaluation_executed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "historical_2023_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
    }
    payload["audit_sha256"] = stable_hash(payload)
    output = output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(audit(args.root, args.output), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
