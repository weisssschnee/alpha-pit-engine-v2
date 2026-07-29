"""Freeze a zero-financial input-binding receipt for the existing A-share replay.

This script does not materialize signals, evaluate candidates, or create a new
authority. It binds a frozen keep-review cohort to the existing development
minute release, reports which execution inputs are already present, and fails
closed on missing universe, fee, suspension, corporate-action, or delisting
evidence.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
from typing import Any, Mapping

import pandas as pd
import pyarrow.parquet as pq

from our_system_phase2.services.a_share_executable_replay import (
    AShareCorporateActionPolicy,
    AShareFeeSchedule,
)


SCHEMA_VERSION = "cn_finalist_input_authority_binding_v1"
MINUTE_PATTERN = (
    "shard_*/phase3aq_wide_true1min/canary/"
    "phase3aq_true_1min_formula_canary.parquet"
)
DIRECT_REQUIRED_COLUMNS = (
    "code",
    "trade_time",
    "open",
    "high",
    "low",
    "close",
    "security_type",
    "exchange",
    "universe_eligible",
    "listing_age_sessions",
    "is_st",
    "is_delisting",
    "suspended",
    "up_limit_price",
    "down_limit_price",
    "corporate_action_cash_per_share",
    "corporate_action_share_multiplier",
    "is_terminal_session",
    "terminal_liquidation_price",
)
FIELD_ALIASES = {
    "is_st": ("is_st", "ctx_hfq_is_st"),
    "suspended": ("suspended", "susp"),
}
DERIVABLE_FIELDS = {
    "security_type": "SUPPORTED_A_SHARE_CODE_NAMESPACE_RESOLVER",
    "exchange": "SUPPORTED_A_SHARE_CODE_NAMESPACE_RESOLVER",
    "up_limit_price": "CN_CONSERVATIVE_LIMIT_LIFECYCLE_V2",
    "down_limit_price": "CN_CONSERVATIVE_LIMIT_LIFECYCLE_V2",
}


def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _payload_sha256(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _verify_self_hash(payload: Mapping[str, Any], field: str) -> None:
    expected = str(payload.get(field) or "")
    candidate = dict(payload)
    candidate.pop(field, None)
    observed = _payload_sha256(candidate)
    if observed != expected:
        raise ValueError(
            f"{field} mismatch: declared={expected} observed={observed}"
        )


def _verify_keep_review(cohort_root: Path) -> dict[str, Any]:
    manifest_path = cohort_root / "keep_review_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    _verify_self_hash(manifest, "manifest_payload_sha256")
    for artifact in manifest.get("artifacts") or []:
        path = cohort_root / str(artifact["path"])
        if not path.is_file():
            raise FileNotFoundError(path)
        if path.stat().st_size != int(artifact["bytes"]):
            raise ValueError(f"cohort artifact size mismatch: {path}")
        if _sha256(path) != str(artifact["sha256"]):
            raise ValueError(f"cohort artifact hash mismatch: {path}")
    pairs = pd.read_parquet(cohort_root / "keep_review_pairs.parquet")
    candidates = pd.read_parquet(
        cohort_root / "keep_review_candidates.parquet"
    )
    if len(pairs) != 64 or len(candidates) != 128:
        raise ValueError(
            f"frozen cohort must remain 64 pairs/128 members: "
            f"{len(pairs)}/{len(candidates)}"
        )
    if pairs["pair_id"].astype(str).nunique() != 64:
        raise ValueError("frozen cohort pair IDs are not unique")
    if candidates["candidate_id"].astype(str).nunique() != 128:
        raise ValueError("frozen cohort candidate IDs are not unique")
    return {
        "root": str(cohort_root),
        "manifest": str(manifest_path),
        "manifest_file_sha256": _sha256(manifest_path),
        "manifest_payload_sha256": manifest["manifest_payload_sha256"],
        "selection_payload_sha256": manifest["selection_payload_sha256"],
        "pair_count": 64,
        "candidate_member_count": 128,
    }


def _release_schema(release_root: Path) -> dict[str, Any]:
    manifest_path = release_root / "development_only_release_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if str(manifest.get("data_role") or "") != "development":
        raise ValueError("minute release is not development-only")
    if bool(manifest.get("forward_2026_present")):
        raise PermissionError("minute release contains forbidden 2026 data")
    panels = sorted(release_root.glob(MINUTE_PATTERN))
    if not panels:
        raise FileNotFoundError(
            f"no minute panels under frozen release: {release_root}"
        )
    schemas = [set(pq.ParquetFile(path).schema_arrow.names) for path in panels]
    common = set.intersection(*schemas)
    union = set.union(*schemas)
    if common != union:
        raise ValueError("minute release shard schemas differ")
    return {
        "root": str(release_root),
        "manifest": str(manifest_path),
        "manifest_file_sha256": _sha256(manifest_path),
        "release_id": manifest.get("release_id"),
        "release_hash": manifest.get("release_hash"),
        "data_role": manifest.get("data_role"),
        "forward_2026_present": False,
        "date_min": manifest.get("allowed_dates", {}).get("min"),
        "date_max": manifest.get("allowed_dates", {}).get("max"),
        "panel_count": len(panels),
        "schema_sha256": manifest.get("schema_sha256"),
        "columns": sorted(common),
    }


def _field_bindings(columns: set[str]) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    blockers: list[str] = []
    for field in DIRECT_REQUIRED_COLUMNS:
        aliases = FIELD_ALIASES.get(field, (field,))
        observed = next((name for name in aliases if name in columns), None)
        if observed:
            status = "BOUND_EXISTING_RELEASE_COLUMN"
            blocker = False
        elif field in DERIVABLE_FIELDS:
            status = "DERIVATION_IMPLEMENTED_NOT_MATERIALIZED"
            blocker = True
        else:
            status = "MISSING_FROM_FROZEN_RELEASE"
            blocker = True
        rows.append(
            {
                "replay_field": field,
                "observed_column": observed,
                "status": status,
                "derivation_authority": DERIVABLE_FIELDS.get(field),
                "execution_ready": not blocker,
            }
        )
        if blocker:
            blockers.append(f"session_field_not_bound:{field}")
    return rows, blockers


def _validate_universe_manifest(
    path: Path | None,
    *,
    date_min: str,
    date_max: str,
) -> tuple[dict[str, Any] | None, list[str]]:
    if path is None:
        return None, ["promotion_grade_universe_manifest_missing"]
    payload = json.loads(path.read_text(encoding="utf-8"))
    required_true = (
        "pit_membership",
        "survivorship_free",
        "delisting_history_included",
    )
    blockers = [
        f"universe_{field}_not_proven"
        for field in required_true
        if not bool(payload.get(field))
    ]
    if str(payload.get("date_min") or "") > date_min:
        blockers.append("universe_date_start_does_not_cover_release")
    if str(payload.get("date_max") or "") < date_max:
        blockers.append("universe_date_end_does_not_cover_release")
    if int(payload.get("security_count") or 0) <= 0:
        blockers.append("universe_security_count_not_positive")
    if not str(payload.get("source_reference") or ""):
        blockers.append("universe_source_reference_missing")
    return {
        "path": str(path),
        "sha256": _sha256(path),
        "payload": payload,
    }, blockers


def _validate_fee_contract(
    path: Path | None,
    *,
    date_min: str,
    date_max: str,
) -> tuple[dict[str, Any] | None, list[str]]:
    if path is None:
        return None, ["actual_account_fee_contract_missing"]
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not bool(payload.get("account_contract_confirmed")):
        return None, ["account_commission_not_confirmed"]
    schedule = AShareFeeSchedule(**dict(payload.get("fee_schedule") or {}))
    schedule.validate()
    blockers: list[str] = []
    if schedule.effective_start > date_min:
        blockers.append("fee_schedule_start_does_not_cover_release")
    if schedule.effective_end < date_max:
        blockers.append("fee_schedule_end_does_not_cover_release")
    return {
        "path": str(path),
        "sha256": _sha256(path),
        "fee_schedule_sha256": schedule.payload_sha256,
        "account_contract_confirmed": True,
    }, blockers


def _repo_sha(repo_root: Path) -> str:
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_root,
        text=True,
    ).strip()


def _resolve_repo_sha(repo_root: Path, declared_sha: str | None) -> str:
    if declared_sha is None:
        return _repo_sha(repo_root)
    normalized = str(declared_sha).strip().lower()
    if not re.fullmatch(r"[0-9a-f]{40}", normalized):
        raise ValueError("--repo-sha must be an exact 40-character Git SHA")
    return normalized


def freeze(
    *,
    repo_root: Path,
    cohort_root: Path,
    release_root: Path,
    output_root: Path,
    universe_manifest: Path | None = None,
    fee_contract: Path | None = None,
    repo_sha: str | None = None,
) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    cohort_root = cohort_root.resolve()
    release_root = release_root.resolve()
    output_root = output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=False)

    cohort = _verify_keep_review(cohort_root)
    release = _release_schema(release_root)
    fields, blockers = _field_bindings(set(release["columns"]))
    universe, universe_blockers = _validate_universe_manifest(
        universe_manifest.resolve() if universe_manifest else None,
        date_min=str(release["date_min"]),
        date_max=str(release["date_max"]),
    )
    fees, fee_blockers = _validate_fee_contract(
        fee_contract.resolve() if fee_contract else None,
        date_min=str(release["date_min"]),
        date_max=str(release["date_max"]),
    )
    blockers.extend(universe_blockers)
    blockers.extend(fee_blockers)

    corporate_policy = AShareCorporateActionPolicy(
        source_reference=(
            "ADR-0009/0010 existing A-share replay; "
            "PIT-aligned event inputs required"
        )
    )
    corporate_policy.validate()
    policy_payload = {
        "schema_version": "a_share_corporate_action_policy_v1",
        **asdict(corporate_policy),
        "payload_sha256": corporate_policy.payload_sha256,
        "financial_result_recomputed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    policy_path = _write_json(
        output_root / "a_share_corporate_action_policy.json",
        policy_payload,
    )

    binding = {
        "schema_version": SCHEMA_VERSION,
        "status": (
            "FINALIST_INPUT_AUTHORITY_READY"
            if not blockers
            else "HOLD_RESEARCH_FINALIST_INPUTS_INCOMPLETE"
        ),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "repo_sha": _resolve_repo_sha(repo_root, repo_sha),
        "existing_authority": "PHASE3DY_A_SHARE_TRADABILITY_REPLAY",
        "new_authority_node_created": False,
        "cohort": cohort,
        "minute_release": {
            key: value for key, value in release.items() if key != "columns"
        },
        "field_bindings": fields,
        "universe_manifest": universe,
        "fee_contract": fees,
        "corporate_action_policy": {
            "path": str(policy_path),
            "sha256": _sha256(policy_path),
            "payload_sha256": corporate_policy.payload_sha256,
        },
        "blockers": sorted(set(blockers)),
        "financial_replay_authorized": False,
        "report_only_validation_authorized": False,
        "promotion_authorized": False,
        "successor_search_authorized": False,
        "financial_result_recomputed": False,
        "optimizer_feedback_writes": 0,
        "scheduler_writes": 0,
        "archive_writes": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    binding["binding_payload_sha256"] = _payload_sha256(binding)
    binding_path = _write_json(
        output_root / "finalist_input_authority_binding.json",
        binding,
    )

    artifacts = [
        {
            "path": path.relative_to(output_root).as_posix(),
            "bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for path in (policy_path, binding_path)
    ]
    manifest = {
        "schema_version": "cn_finalist_input_authority_manifest_v1",
        "status": binding["status"],
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "repo_sha": binding["repo_sha"],
        "cohort_selection_payload_sha256": cohort[
            "selection_payload_sha256"
        ],
        "binding_payload_sha256": binding["binding_payload_sha256"],
        "artifacts": artifacts,
        "input_manifests": [
            {
                "path": cohort["manifest"],
                "sha256": cohort["manifest_file_sha256"],
            },
            {
                "path": release["manifest"],
                "sha256": release["manifest_file_sha256"],
            },
        ],
        "blockers": binding["blockers"],
        "financial_replay_authorized": False,
        "report_only_validation_authorized": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "financial_result_recomputed": False,
    }
    manifest["manifest_payload_sha256"] = _payload_sha256(manifest)
    manifest_path = _write_json(
        output_root / "finalist_input_authority_manifest.json",
        manifest,
    )
    result = {
        "status": manifest["status"],
        "output_root": str(output_root),
        "manifest": str(manifest_path),
        "manifest_file_sha256": _sha256(manifest_path),
        "manifest_payload_sha256": manifest["manifest_payload_sha256"],
        "binding_payload_sha256": binding["binding_payload_sha256"],
        "blocker_count": len(binding["blockers"]),
        "blockers": binding["blockers"],
        "financial_replay_authorized": False,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument("--cohort-root", type=Path, required=True)
    parser.add_argument("--release-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--universe-manifest", type=Path)
    parser.add_argument("--fee-contract", type=Path)
    parser.add_argument("--repo-sha")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    freeze(
        repo_root=args.repo_root,
        cohort_root=args.cohort_root,
        release_root=args.release_root,
        output_root=args.output_root,
        universe_manifest=args.universe_manifest,
        fee_contract=args.fee_contract,
        repo_sha=args.repo_sha,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
