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

from our_system_phase2.runtime.cn_joint_program_phase_c_v0 import (
    BASE_RECORDS,
    ENHANCED_RECORDS_PER_TEMPLATE,
    ENHANCED_TEMPLATE_ORDER,
    EXPECTED_RECORDS,
    FREEZE_CLOSURE_NAME,
    FREEZE_SCHEMA,
    FREEZE_STATUS,
    MIN_BASE_IDENTITIES_PER_TEMPLATE,
    RAW_RESERVOIR_PER_ENHANCED_TEMPLATE,
    SESSION_EXECUTABLE_ROUTES,
    TEMPLATE_ORDER,
    verify_phase_c_component_materialization_plan_v0,
)
from our_system_phase2.services.program_factorized_bandit_v0 import (
    ProgramFactorizedBanditV0,
)
from our_system_phase2.services.unified_capability_registry import stable_hash


AUDIT_STATUS = "CN_JOINT_PROGRAM_PHASE_C_PREFINANCIAL_INDEPENDENT_AUDIT_PASS"


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


def _check_self_hash(
    checks: list[dict[str, Any]],
    payload: Mapping[str, Any],
    key: str,
    label: str,
) -> None:
    body = dict(payload)
    claimed = str(body.pop(key, ""))
    observed = stable_hash(body)
    checks.append(
        {
            "check": label,
            "passed": len(claimed) == 64 and claimed == observed,
            "expected": claimed,
            "observed": observed,
        }
    )


def audit(root: Path, expected_repo_sha: str) -> dict[str, Any]:
    root = root.resolve()
    checks: list[dict[str, Any]] = []
    closure_path = root / FREEZE_CLOSURE_NAME
    closure = _read_json(closure_path)
    _check_self_hash(checks, closure, "closure_sha256", "closure self-hash")
    checks.append(
        {
            "check": "closure authority",
            "passed": (
                closure.get("schema_version") == FREEZE_SCHEMA
                and closure.get("status") == FREEZE_STATUS
                and closure.get("repo_sha") == expected_repo_sha
                and int(closure.get("main_record_count") or 0) == EXPECTED_RECORDS
            ),
        }
    )
    manifest_path = root / str(closure["artifact_manifest"]["relative_path"])
    checks.append(
        {
            "check": "artifact manifest file binding",
            "passed": (
                manifest_path.is_file()
                and _sha256(manifest_path)
                == str(closure["artifact_manifest"]["sha256"])
            ),
        }
    )
    manifest = _read_json(manifest_path)
    _check_self_hash(
        checks, manifest, "artifact_manifest_sha256", "artifact manifest self-hash"
    )
    for artifact in manifest["artifacts"]:
        path = root / str(artifact["relative_path"])
        checks.append(
            {
                "check": f"artifact {artifact['relative_path']}",
                "passed": (
                    path.is_file()
                    and path.stat().st_size == int(artifact["size_bytes"])
                    and _sha256(path) == str(artifact["sha256"])
                ),
            }
        )

    asks = _read_jsonl(root / "phase_c_ask_plan.jsonl")
    for row in asks:
        body = dict(row)
        claimed = str(body.pop("ask_record_sha256", ""))
        checks.append(
            {
                "check": f"ask {row['main_record_ordinal']} self-hash",
                "passed": claimed == stable_hash(body),
            }
        )
    ask_counts = Counter(str(row["template_id"]) for row in asks)
    arm_counts = {
        template_id: Counter(
            str(row["generation_arm"])
            for row in asks
            if row["template_id"] == template_id
        )
        for template_id in TEMPLATE_ORDER
    }
    expected_ask_counts = {
        "BASE": BASE_RECORDS,
        **{
            template_id: ENHANCED_RECORDS_PER_TEMPLATE
            for template_id in ENHANCED_TEMPLATE_ORDER
        },
    }
    checks.append(
        {
            "check": "exact 512 ask/template quotas",
            "passed": len(asks) == EXPECTED_RECORDS
            and dict(ask_counts) == expected_ask_counts,
        }
    )
    checks.append(
        {
            "check": "enhanced arm quotas",
            "passed": all(
                arm_counts[template_id]
                == Counter(
                    {
                        "UNIFORM_FRESH": 28,
                        "FACTORIZED_EXPLOIT": 24,
                        "NOVELTY_RESERVE": 12,
                    }
                )
                for template_id in ENHANCED_TEMPLATE_ORDER
            ),
        }
    )
    checks.append(
        {
            "check": "no early template cancellation",
            "passed": all(
                row.get("early_template_cancellation_allowed") is False
                for row in asks
            ),
        }
    )

    component_pool = _read_jsonl(
        root / "phase_c_session_executable_component_pool.jsonl"
    )
    component_role_counts = Counter(str(row["role"]) for row in component_pool)
    component_ids = [str(row["component_id"]) for row in component_pool]
    checks.append(
        {
            "check": "accepted session-executable component pool only",
            "passed": bool(component_pool)
            and len(component_ids) == len(set(component_ids))
            and all(
                str(row["route_id"]) in SESSION_EXECUTABLE_ROUTES
                for row in component_pool
            )
            and int(component_role_counts["base"])
            >= MIN_BASE_IDENTITIES_PER_TEMPLATE
            and all(
                int(component_role_counts[role]) >= 8
                for role in ("temporal", "market", "event")
            ),
        }
    )

    reservoir = _read_jsonl(root / "phase_c_raw_program_reservoir.jsonl")
    reservoir_counts = Counter(str(row["template_id"]) for row in reservoir)
    expected_reservoir_counts = {
        "BASE": BASE_RECORDS,
        **{
            template_id: RAW_RESERVOIR_PER_ENHANCED_TEMPLATE
            for template_id in ENHANCED_TEMPLATE_ORDER
        },
    }
    checks.append(
        {
            "check": "raw reservoir quotas",
            "passed": dict(reservoir_counts) == expected_reservoir_counts,
        }
    )
    for row in reservoir:
        body = dict(row)
        claimed = str(body.pop("reservoir_record_sha256", ""))
        checks.append(
            {
                "check": (
                    f"reservoir {row['template_id']}/"
                    f"{row['template_reservoir_ordinal']} self-hash"
                ),
                "passed": claimed == stable_hash(body),
            }
        )
    checks.append(
        {
            "check": "reservoir raw-combination uniqueness",
            "passed": all(
                len(
                    {
                        str(row["raw_combination_sha256"])
                        for row in reservoir
                        if row["template_id"] == template_id
                    }
                )
                == expected_count
                for template_id, expected_count in expected_reservoir_counts.items()
                if template_id != "BASE"
            ),
        }
    )
    base_component_counts = Counter(
        str(row["components"]["base"]["component_id"])
        for row in reservoir
        if row["template_id"] == "BASE"
    )
    checks.append(
        {
            "check": "BASE parity repetition is bounded and explicit",
            "passed": len(base_component_counts) >= MIN_BASE_IDENTITIES_PER_TEMPLATE
            and max(base_component_counts.values())
            <= math.ceil(BASE_RECORDS / int(component_role_counts["base"]))
            and all(
                "base_parity_repeat_ordinal" in row
                for row in reservoir
                if row["template_id"] == "BASE"
            ),
        }
    )
    fixtures = _read_jsonl(root / "phase_c_template_compile_fixtures.jsonl")
    checks.append(
        {
            "check": "eight template compile fixtures",
            "passed": [row["template_id"] for row in fixtures]
            == list(TEMPLATE_ORDER),
        }
    )
    for row in fixtures:
        body = dict(row)
        claimed = str(body.pop("compile_fixture_sha256", ""))
        checks.append(
            {
                "check": f"compile fixture {row['template_id']}",
                "passed": claimed == stable_hash(body),
            }
        )

    feedback = _read_jsonl(root / "phase_b_feedback_seed.jsonl")
    for row in feedback:
        body = dict(row)
        claimed = str(body.pop("feedback_seed_record_sha256", ""))
        checks.append(
            {
                "check": f"feedback seed {row['phase_b_main_record_ordinal']}",
                "passed": claimed == stable_hash(body)
                and row.get("validation_feedback_used") is False
                and row.get("cross_campaign_state_imported") is False,
            }
        )
    checks.append(
        {
            "check": "exact Phase B feedback ledger coverage",
            "passed": len(feedback) == 64
            and [int(row["phase_b_main_record_ordinal"]) for row in feedback]
            == list(range(64)),
        }
    )
    bandit = _read_json(root / "initial_bandit_state.json")
    checks.append(
        {
            "check": "bandit exact restore",
            "passed": ProgramFactorizedBanditV0.restore(bandit).snapshot() == bandit,
        }
    )
    materialization = _read_json(root / "phase_c_materialization_plan.json")
    verify_phase_c_component_materialization_plan_v0(materialization)
    checks.append(
        {
            "check": "complete reservoir materialization coverage",
            "passed": not materialization["missing_required_field_ids"]
            and bool(
                materialization["coverage_checks"]["required_equals_materializable"]
            )
            and int(materialization["component_count"]) == len(component_pool),
        }
    )
    contract = _read_json(root / "phase_c_run_contract.json")
    _check_self_hash(checks, contract, "run_contract_sha256", "run contract self-hash")
    checks.append(
        {
            "check": "non-formal authority boundary",
            "passed": (
                int(contract.get("session_executable_component_count") or 0)
                == len(component_pool)
                and dict(contract.get("session_executable_component_role_counts") or {})
                == dict(component_role_counts)
                and list(contract.get("session_executable_routes") or [])
                == sorted(SESSION_EXECUTABLE_ROUTES)
                and
                contract.get("cross_campaign_optimizer_state_import") is False
                and contract.get("route_local_tpe_authority_unchanged") is True
                and contract.get("formal_optimizer_authority_write") is False
                and contract.get("formal_scheduler_authority_write") is False
                and contract.get("promotion_write") is False
                and contract.get("automatic_phase_d_launch") is False
            ),
        }
    )
    access = _read_json(root / "access_ledger.json")
    _check_self_hash(checks, access, "access_ledger_sha256", "access ledger self-hash")
    checks.append(
        {
            "check": "zero financial and sealed reads",
            "passed": access.get("financial_evaluation_executed") is False
            and all(
                int(access.get(key) or 0) == 0
                for key in (
                    "market_price_rows_read",
                    "label_rows_read",
                    "validation_reads",
                    "holdout_reads",
                    "historical_2023_reads",
                    "forward_b_reads",
                    "forward_2026_reads",
                )
            ),
        }
    )

    failures = [row for row in checks if not bool(row.get("passed"))]
    payload = {
        "schema_version": "cn_joint_program_phase_c_prefinancial_audit_v0",
        "status": AUDIT_STATUS if not failures else "FAIL",
        "freeze_root": str(root),
        "freeze_closure_file_sha256": _sha256(closure_path),
        "freeze_closure_payload_sha256": str(closure["closure_sha256"]),
        "checks_executed": len(checks),
        "failure_count": len(failures),
        "failures": failures,
        "main_record_count": len(asks),
        "reservoir_record_count": len(reservoir),
        "initial_bandit_observations": int(bandit["observations"]),
        "financial_evaluation_executed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "historical_2023_reads": 0,
        "forward_b_reads": 0,
        "forward_2026_reads": 0,
    }
    payload["audit_payload_sha256"] = stable_hash(payload)
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase-c-freeze-root", type=Path, required=True)
    parser.add_argument("--expected-repo-sha", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    payload = audit(args.phase_c_freeze_root, str(args.expected_repo_sha))
    if args.output.exists():
        raise FileExistsError(f"Phase C audit output already exists: {args.output}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return 0 if payload["status"] == AUDIT_STATUS else 1


if __name__ == "__main__":
    raise SystemExit(main())
