from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd
import psutil

from our_system_phase2.runtime.cn_iterative_search_v1 import (
    _context_and_binding,
    _run_phase3cm,
)
from our_system_phase2.services.a_share_executable_replay import (
    AShareCandidateReplayBlockerError,
    AShareCorporateActionFractionalSharesError,
    AShareCorporateActionPolicy,
    AShareExecutionPolicy,
    AShareFeeSchedule,
    AShareTerminalLiquidationError,
    AShareUniversePolicy,
    run_a_share_long_only_replay,
)
from our_system_phase2.services.a_share_tradability_guard import (
    a_share_tradability_blockers,
    build_a_share_tradability_receipt,
)
from our_system_phase2.services.candidate_result_schema import (
    candidate_result_summary_frame,
)
from our_system_phase2.services.fixed_split_authority import FixedSplitAuthority
from our_system_phase2.services.node_resource_governor import (
    validate_node_resource_lease_receipt,
)
from our_system_phase2.services.real_market_validation import (
    evaluate_panel_expression,
)
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
)


AUTHORIZED_HOST = "DESKTOP-77OPJ6F"
MINIMUM_FREE_MEMORY_BYTES = 24 * 1024**3
EXPECTED_PAIR_COUNT = 24
EXPECTED_MEMBER_COUNT = 48


def _configure_expected_cohort_size(pair_count: int) -> None:
    global EXPECTED_PAIR_COUNT, EXPECTED_MEMBER_COUNT
    if int(pair_count) <= 0:
        raise ValueError("expected pair count must be positive")
    EXPECTED_PAIR_COUNT = int(pair_count)
    EXPECTED_MEMBER_COUNT = int(pair_count) * 2


EXPECTED_ROUTES = frozenset(
    {
        "SLOW_TEMPORAL_CHANGE",
        "SLOW_CROSS_SECTIONAL_LEVEL",
        "FIRSTN_PATH",
    }
)
CANDIDATE_ECONOMIC_REPLAY_BLOCKERS = {
    frozenset({"replay_has_no_executable_fills"}): "NO_EXECUTABLE_FILLS",
}
REPLAY_REQUIRED_SESSION_COLUMNS = (
    "date",
    "code",
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


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )
    temporary.replace(destination)
    return destination


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.write_text(
        "".join(
            json.dumps(
                dict(row),
                ensure_ascii=False,
                sort_keys=True,
                allow_nan=False,
            )
            + "\n"
            for row in rows
        ),
        encoding="utf-8",
    )
    temporary.replace(destination)
    return destination


def _artifact(path: Path, *, root: Path | None = None) -> dict[str, Any]:
    source = Path(path).resolve()
    rendered = (
        str(source.relative_to(Path(root).resolve())).replace("\\", "/")
        if root is not None
        else str(source)
    )
    return {
        "path": rendered,
        "sha256": _sha256(source),
        "bytes": source.stat().st_size,
    }


def _verify_payload_hash(
    payload: Mapping[str, Any],
    *,
    field: str,
    label: str,
) -> None:
    declared = str(payload.get(field) or "")
    body = dict(payload)
    body.pop(field, None)
    observed = _stable_hash(body)
    if declared != observed:
        raise RuntimeError(
            f"{label} canonical payload hash drift: {declared} != {observed}"
        )


def _verify_declared_artifacts(
    manifest: Mapping[str, Any],
    *,
    root: Path,
) -> None:
    for row in manifest.get("artifacts") or ():
        path = Path(str(row["path"]))
        source = path if path.is_absolute() else Path(root) / path
        if not source.is_file():
            raise FileNotFoundError(f"declared artifact is missing: {source}")
        if int(row.get("bytes") or -1) != source.stat().st_size:
            raise RuntimeError(f"declared artifact size drift: {source}")
        if str(row.get("sha256") or "") != _sha256(source):
            raise RuntimeError(f"declared artifact hash drift: {source}")


def _host_and_memory_gate() -> int:
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError(f"financial work is authorized only on {AUTHORIZED_HOST}")
    available = int(psutil.virtual_memory().available)
    if available < MINIMUM_FREE_MEMORY_BYTES:
        raise RuntimeError(
            f"free memory below 24 GiB gate: {available} bytes"
        )
    return available


def _finite(value: Any) -> float | None:
    try:
        output = float(value)
    except (TypeError, ValueError):
        return None
    return output if math.isfinite(output) else None


def _candidate_economic_blocker_code(
    blockers: Sequence[str],
) -> str | None:
    return CANDIDATE_ECONOMIC_REPLAY_BLOCKERS.get(frozenset(blockers))


def _candidate_replay_exception_blocker(
    exc: Exception,
    *,
    candidate: Mapping[str, Any],
    input_data_sha256: str,
) -> dict[str, Any]:
    blocker = {
        "schema_version": "cn_finalist_replay_blocker_v1",
        "candidate_id": str(candidate["candidate_id"]),
        "pair_id": str(candidate["pair_id"]),
        "pair_member_role": str(candidate["pair_member_role"]),
        "route_id": str(candidate["route_id"]),
        "exact_identity": str(candidate["exact_identity"]),
        "input_data_sha256": input_data_sha256,
        "fail_closed": True,
        "economic_claim_authorized": False,
        "promotion_authorized": False,
    }
    if isinstance(exc, AShareCandidateReplayBlockerError):
        blocker.update(exc.blocker_details())
        return blocker
    raise TypeError(f"unsupported candidate replay exception: {type(exc)!r}")


def _normalize_code(value: Any) -> str:
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.zfill(6) if text.isdigit() and len(text) <= 6 else text


def _ordered_candidates(
    candidates: pd.DataFrame,
    pairs: pd.DataFrame,
) -> pd.DataFrame:
    required_candidate_columns = {
        "candidate_id",
        "pair_id",
        "pair_member_role",
        "route_id",
        "expression",
        "exact_identity",
    }
    missing = sorted(required_candidate_columns - set(candidates.columns))
    if missing:
        raise RuntimeError(f"candidate cohort columns missing: {missing}")
    if len(candidates) != EXPECTED_MEMBER_COUNT:
        raise RuntimeError("candidate member count drift")
    if candidates["candidate_id"].astype(str).duplicated().any():
        raise RuntimeError("candidate IDs are duplicated")
    if candidates["exact_identity"].astype(str).duplicated().any():
        raise RuntimeError("candidate exact identities are duplicated")
    if set(candidates["route_id"].astype(str)) - EXPECTED_ROUTES:
        raise RuntimeError("candidate cohort contains an unsupported route")
    if len(pairs) != EXPECTED_PAIR_COUNT:
        raise RuntimeError("pair count drift")
    if pairs["pair_id"].astype(str).duplicated().any():
        raise RuntimeError("pair IDs are duplicated")
    order_column = (
        "keep_review_rank"
        if "keep_review_rank" in pairs.columns
        else "pair_id"
    )
    pair_order = pairs.sort_values(order_column, kind="mergesort")
    by_candidate = candidates.set_index(
        candidates["candidate_id"].astype(str),
        drop=False,
    )
    rows: list[dict[str, Any]] = []
    for pair in pair_order.to_dict(orient="records"):
        pair_id = str(pair["pair_id"])
        primary_id = str(pair["primary_candidate_id"])
        control_id = str(pair["control_candidate_id"])
        for member_id, role in (
            (primary_id, "PRIMARY"),
            (control_id, "CONTROL"),
        ):
            if member_id not in by_candidate.index:
                raise RuntimeError(
                    f"pair {pair_id} member missing: {member_id}"
                )
            row = dict(by_candidate.loc[member_id])
            if str(row["pair_id"]) != pair_id:
                raise RuntimeError(f"candidate pair binding drift: {member_id}")
            if str(row["pair_member_role"]) != role:
                raise RuntimeError(f"candidate role drift: {member_id}")
            rows.append(row)
    ordered = pd.DataFrame(rows)
    if len(ordered) != EXPECTED_MEMBER_COUNT:
        raise RuntimeError("ordered cohort member count drift")
    return ordered.reset_index(drop=True)


def _protected_source_hashes(
    *,
    cohort_root: Path,
    authority_root: Path,
    split_manifest: Path,
    registry: Path,
) -> dict[str, str]:
    authority = _read_json(
        Path(authority_root) / "finalist_input_authority_binding.json"
    )
    session = authority["session_authority"]
    paths = (
        Path(cohort_root) / "keep_review_manifest.json",
        Path(cohort_root) / "keep_review_candidates.parquet",
        Path(cohort_root) / "keep_review_pairs.parquet",
        Path(authority_root) / "finalist_input_authority_manifest.json",
        Path(authority_root) / "finalist_input_authority_binding.json",
        Path(authority_root) / "a_share_corporate_action_policy.json",
        Path(str(session["path"])),
        Path(str(session["session_authority_path"])),
        Path(str(session["universe_manifest_path"])),
        Path(str(session["fee_contract_path"])),
        Path(split_manifest),
        Path(registry),
    )
    return {str(path.resolve()): _sha256(path.resolve()) for path in paths}


def _read_policies(
    authority: Mapping[str, Any],
    *,
    authority_root: Path,
) -> tuple[
    AShareFeeSchedule,
    AShareUniversePolicy,
    AShareExecutionPolicy,
    AShareCorporateActionPolicy,
]:
    fee_contract = _read_json(
        Path(str(authority["fee_contract"]["path"])).resolve()
    )
    universe_manifest = _read_json(
        Path(str(authority["universe_manifest"]["path"])).resolve()
    )
    corporate_payload = _read_json(
        Path(authority_root) / "a_share_corporate_action_policy.json"
    )
    fee = AShareFeeSchedule(**dict(fee_contract["fee_schedule"]))
    universe = AShareUniversePolicy(
        minimum_listing_sessions=60,
        allowed_exchanges=("SSE", "SZSE"),
        security_type="A_SHARE",
        exclude_st=True,
        exclude_delisting=True,
        pit_membership=bool(universe_manifest["pit_membership"]),
        survivorship_free=bool(universe_manifest["survivorship_free"]),
        delisting_history_included=bool(
            universe_manifest["delisting_history_included"]
        ),
        source_reference=str(universe_manifest["source_reference"]),
    )
    execution = AShareExecutionPolicy(
        signal_clock="SESSION_CLOSE_T",
        execution_clock="NEXT_SESSION_OPEN_T_PLUS_1",
        rebalance_frequency="EACH_SESSION",
        long_only=True,
        top_quantile=0.20,
        initial_cash_cny=1_000_000.0,
        default_lot_size=100,
        price_tick=0.01,
    )
    corporate_fields = {
        field: corporate_payload[field]
        for field in AShareCorporateActionPolicy.__dataclass_fields__
    }
    corporate = AShareCorporateActionPolicy(**corporate_fields)
    fee.validate()
    universe.validate()
    execution.validate()
    corporate.validate()
    return fee, universe, execution, corporate


def prepare(
    *,
    cohort_root: Path,
    authority_root: Path,
    split_manifest: Path,
    registry: Path,
    output_root: Path,
    repo_sha: str,
) -> dict[str, Any]:
    _host_and_memory_gate()
    cohort_root = Path(cohort_root).resolve()
    authority_root = Path(authority_root).resolve()
    output_root = Path(output_root).resolve()
    split_manifest = Path(split_manifest).resolve()
    registry = Path(registry).resolve()
    prepared = output_root / "prepared"
    prepared.mkdir(parents=True, exist_ok=True)

    cohort_manifest_path = cohort_root / "keep_review_manifest.json"
    cohort_manifest = _read_json(cohort_manifest_path)
    if str(cohort_manifest.get("status") or "") != (
        "KEEP_REVIEW_COHORT_CLOSED_IMMUTABLE"
    ):
        raise RuntimeError("source finalist cohort is not immutable")
    _verify_payload_hash(
        cohort_manifest,
        field="manifest_payload_sha256",
        label="cohort manifest",
    )
    _verify_declared_artifacts(cohort_manifest, root=cohort_root)

    authority_manifest_path = (
        authority_root / "finalist_input_authority_manifest.json"
    )
    authority_binding_path = (
        authority_root / "finalist_input_authority_binding.json"
    )
    authority_manifest = _read_json(authority_manifest_path)
    authority = _read_json(authority_binding_path)
    if str(authority_manifest.get("status") or "") != (
        "FINALIST_INPUT_AUTHORITY_READY"
    ):
        raise RuntimeError("finalist input authority is not ready")
    if authority_manifest.get("blockers") or authority.get("blockers"):
        raise RuntimeError("finalist input authority has blockers")
    _verify_payload_hash(
        authority_manifest,
        field="manifest_payload_sha256",
        label="input authority manifest",
    )
    _verify_payload_hash(
        authority,
        field="binding_payload_sha256",
        label="input authority binding",
    )
    _verify_declared_artifacts(authority_manifest, root=authority_root)
    selection_hash = str(cohort_manifest["selection_payload_sha256"])
    if selection_hash != str(
        authority_manifest["cohort_selection_payload_sha256"]
    ):
        raise RuntimeError("cohort/input-authority selection hash drift")
    if int(authority.get("validation_reads") or 0) != 0:
        raise RuntimeError("input authority already contains validation reads")
    if int(authority.get("holdout_reads") or 0) != 0:
        raise RuntimeError("input authority contains holdout reads")
    if int(authority.get("forward_2026_reads") or 0) != 0:
        raise RuntimeError("input authority contains forward-2026 reads")

    split = FixedSplitAuthority.read(split_manifest)
    UnifiedCapabilityRegistry.read(registry)
    candidates = pd.read_parquet(
        cohort_root / "keep_review_candidates.parquet"
    )
    pairs = pd.read_parquet(cohort_root / "keep_review_pairs.parquet")
    ordered = _ordered_candidates(candidates, pairs)
    candidate_path = prepared / "finalist_candidates.parquet"
    ordered.to_parquet(candidate_path, index=False)
    candidate_table_path = (
        prepared / "finalist_stock_session_candidates.csv"
    )
    ordered.to_csv(candidate_table_path, index=False)
    pair_path = prepared / "finalist_pairs.parquet"
    pairs.sort_values("keep_review_rank", kind="mergesort").to_parquet(
        pair_path,
        index=False,
    )

    fee, universe, execution, corporate = _read_policies(
        authority,
        authority_root=authority_root,
    )
    protected_hashes = _protected_source_hashes(
        cohort_root=cohort_root,
        authority_root=authority_root,
        split_manifest=split_manifest,
        registry=registry,
    )
    contract = {
        "schema_version": "cn_finalist_replay_then_oos_execution_contract_v1",
        "status": "ACTIVE_FIXED_COHORT_REPLAY_THEN_OOS",
        "repo_sha": str(repo_sha),
        "selection_payload_sha256": selection_hash,
        "pair_count": EXPECTED_PAIR_COUNT,
        "candidate_member_count": EXPECTED_MEMBER_COUNT,
        "route_counts": {
            str(route): int(count // 2)
            for route, count in ordered["route_id"].value_counts().items()
        },
        "sequence": [
            "A_SHARE_EXECUTABLE_TRAIN_REPLAY",
            "UNCHANGED_COHORT_REPORT_ONLY_VALIDATION",
        ],
        "interstage_filtering": "FORBIDDEN",
        "cohort_mutation": "FORBIDDEN",
        "optimizer_feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "research_replay_authorized": True,
        "report_only_oos_authorized": True,
        "economic_claim_authorized": False,
        "fee_schedule": asdict(fee),
        "fee_schedule_sha256": fee.payload_sha256,
        "universe_policy": {
            **asdict(universe),
            "allowed_exchanges": list(universe.allowed_exchanges),
        },
        "universe_policy_sha256": universe.payload_sha256,
        "execution_policy": asdict(execution),
        "execution_policy_sha256": execution.payload_sha256,
        "corporate_action_policy": asdict(corporate),
        "corporate_action_policy_sha256": corporate.payload_sha256,
        "split_manifest": str(split_manifest),
        "split_manifest_sha256": split.manifest_hash,
        "registry": str(registry),
        "registry_sha256": _sha256(registry),
        "session_authority_manifest": str(
            authority["session_authority"]["path"]
        ),
        "session_authority_manifest_sha256": str(
            authority["session_authority"]["sha256"]
        ),
        "session_authority_path": str(
            authority["session_authority"]["session_authority_path"]
        ),
        "universe_manifest": str(authority["universe_manifest"]["path"]),
        "universe_manifest_sha256": str(
            authority["universe_manifest"]["sha256"]
        ),
        "protected_source_hashes": protected_hashes,
    }
    contract["contract_payload_sha256"] = _stable_hash(contract)
    contract_path = _write_json(
        prepared / "replay_then_oos_execution_contract.json",
        contract,
    )
    freeze = {
        "schema_version": "cn_finalist_replay_then_oos_freeze_v1",
        "status": "FROZEN_UNCHANGED_FIXED_COHORT",
        "repo_sha": str(repo_sha),
        "selection_payload_sha256": selection_hash,
        "pair_count": EXPECTED_PAIR_COUNT,
        "candidate_member_count": EXPECTED_MEMBER_COUNT,
        "candidate_ids": ordered["candidate_id"].astype(str).tolist(),
        "pair_ids": pairs.sort_values(
            "keep_review_rank", kind="mergesort"
        )["pair_id"].astype(str).tolist(),
        "candidate_artifact": _artifact(candidate_path, root=output_root),
        "candidate_table_artifact": _artifact(
            candidate_table_path,
            root=output_root,
        ),
        "pair_artifact": _artifact(pair_path, root=output_root),
        "execution_contract_artifact": _artifact(
            contract_path,
            root=output_root,
        ),
        "protected_source_hashes": protected_hashes,
        "interstage_filtering": "FORBIDDEN",
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    freeze["manifest_body_sha256"] = _stable_hash(freeze)
    freeze_path = _write_json(
        prepared / "finalist_replay_then_oos_freeze.json",
        freeze,
    )
    return {
        "status": "FINALIST_REPLAY_THEN_OOS_PREPARED",
        "freeze_path": str(freeze_path),
        "freeze_sha256": _sha256(freeze_path),
        "contract_path": str(contract_path),
        "contract_sha256": _sha256(contract_path),
        "pair_count": EXPECTED_PAIR_COUNT,
        "candidate_member_count": EXPECTED_MEMBER_COUNT,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }


def _load_freeze(path: Path) -> dict[str, Any]:
    freeze = _read_json(path)
    if str(freeze.get("status") or "") != (
        "FROZEN_UNCHANGED_FIXED_COHORT"
    ):
        raise RuntimeError("finalist cohort freeze status drift")
    _verify_payload_hash(
        freeze,
        field="manifest_body_sha256",
        label="finalist cohort freeze",
    )
    if int(freeze.get("pair_count") or 0) != EXPECTED_PAIR_COUNT:
        raise RuntimeError("finalist cohort freeze pair count drift")
    if int(freeze.get("candidate_member_count") or 0) != (
        EXPECTED_MEMBER_COUNT
    ):
        raise RuntimeError("finalist cohort freeze member count drift")
    return freeze


def _resolved_artifact(
    root: Path,
    artifact: Mapping[str, Any],
) -> Path:
    path = (Path(root) / str(artifact["path"])).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"frozen artifact missing: {path}")
    if str(artifact["sha256"]) != _sha256(path):
        raise RuntimeError(f"frozen artifact hash drift: {path}")
    if int(artifact["bytes"]) != path.stat().st_size:
        raise RuntimeError(f"frozen artifact size drift: {path}")
    return path


def _validate_sidecar(
    root: Path,
    *,
    evaluation_role: str,
    split_hash: str,
) -> tuple[dict[str, Any], Path]:
    root = Path(root).resolve()
    manifest_path = (
        root / "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"
    )
    manifest = _read_json(manifest_path)
    required = {
        "status": "TIME_MAJOR_LAYOUT_PARITY_PASS",
        "evaluation_role": evaluation_role,
        "split_manifest_hash": split_hash,
        "holdout_reads": 0,
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
    }
    drift = [
        key
        for key, value in required.items()
        if manifest.get(key) != value
    ]
    if drift:
        raise RuntimeError(
            f"{evaluation_role} field sidecar drift: {','.join(drift)}"
        )
    expected_role = (
        "development_train_only"
        if evaluation_role == "train"
        else f"{evaluation_role}_report_only"
    )
    if str(manifest.get("data_role") or "") != expected_role:
        raise RuntimeError(f"{evaluation_role} field sidecar role drift")
    if evaluation_role == "train":
        if int(manifest.get("validation_reads") or 0) != 0:
            raise RuntimeError("train field sidecar read validation")
        if int(manifest.get("forward_2026_reads") or 0) != 0:
            raise RuntimeError("train field sidecar read forward 2026")
    elif evaluation_role == "validation":
        if int(manifest.get("validation_reads") or 0) <= 0:
            raise RuntimeError("validation field sidecar has no validation reads")
    elif evaluation_role == "holdout":
        if int(manifest.get("validation_reads") or 0) != 0:
            raise RuntimeError("holdout field sidecar read validation")
        if int(manifest.get("holdout_reads") or 0) <= 0:
            raise RuntimeError("holdout field sidecar has no holdout reads")
    elif evaluation_role == "forward_2026":
        if int(manifest.get("validation_reads") or 0) != 0:
            raise RuntimeError("forward field sidecar read validation")
        if int(manifest.get("forward_2026_reads") or 0) <= 0:
            raise RuntimeError("forward field sidecar has no forward 2026 reads")
    elif evaluation_role == "historical_challenge":
        if int(manifest.get("validation_reads") or 0) != 0:
            raise RuntimeError("historical challenge field sidecar read validation")
        if int(manifest.get("historical_challenge_reads") or 0) <= 0:
            raise RuntimeError(
                "historical challenge field sidecar has no historical reads"
            )
    else:
        raise RuntimeError(f"unsupported field sidecar role: {evaluation_role}")
    if evaluation_role != "forward_2026" and int(
        manifest.get("forward_2026_reads") or 0
    ) != 0:
        raise RuntimeError(f"{evaluation_role} field sidecar read forward 2026")
    fields = set(manifest.get("fields") or ())
    if not {"open", "close"}.issubset(fields):
        raise RuntimeError("field sidecar lacks open/close replay prices")
    for shard in manifest.get("shards") or ():
        path = Path(str(shard["output_path"])).resolve()
        if not path.is_file():
            raise FileNotFoundError(f"field sidecar shard missing: {path}")
        if str(shard["output_sha256"]) != _sha256(path):
            raise RuntimeError(f"field sidecar shard hash drift: {path}")
    return manifest, manifest_path


def _load_field_frame(
    root: Path,
    manifest: Mapping[str, Any],
) -> pd.DataFrame:
    frames = [
        pd.read_parquet(Path(str(row["output_path"])).resolve())
        for row in manifest.get("shards") or ()
    ]
    expected_shards = int(manifest.get("source_shard_count") or -1)
    if not frames or len(frames) != expected_shards:
        raise RuntimeError("replay field sidecar shard cardinality drift")
    frame = pd.concat(frames, ignore_index=True, copy=False)
    frame["code"] = frame["code"].map(_normalize_code)
    frame["trade_time"] = pd.to_datetime(
        frame["trade_time"],
        errors="raise",
    )
    frame["date"] = frame["trade_time"].dt.normalize()
    frame = frame.sort_values(
        ["code", "trade_time"],
        kind="mergesort",
    ).reset_index(drop=True)
    if frame.duplicated(["date", "code"]).any():
        raise RuntimeError("field sidecar has duplicate date/code rows")
    return frame


def _materialize_replay_master(
    field_frame: pd.DataFrame,
    session_authority: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.MultiIndex, pd.MultiIndex]:
    authority = session_authority.copy()
    missing = sorted(
        set(REPLAY_REQUIRED_SESSION_COLUMNS) - set(authority.columns)
    )
    if missing:
        raise RuntimeError(
            f"session authority columns missing: {missing}"
        )
    authority["date"] = pd.to_datetime(
        authority["date"],
        errors="raise",
    ).dt.normalize()
    authority["code"] = authority["code"].map(_normalize_code)
    authority = authority.sort_values(
        ["code", "date"],
        kind="mergesort",
    ).reset_index(drop=True)
    if authority.duplicated(["date", "code"]).any():
        raise RuntimeError("session authority has duplicate date/code rows")
    observed_index = pd.MultiIndex.from_frame(
        field_frame[["date", "code"]]
    )
    authority_index = pd.MultiIndex.from_frame(
        authority[["date", "code"]]
    )
    close_values = pd.Series(
        pd.to_numeric(field_frame["close"], errors="coerce").to_numpy(),
        index=observed_index,
    )
    authority["close"] = close_values.reindex(authority_index).to_numpy()
    authority["close"] = authority.groupby(
        "code",
        sort=False,
    )["close"].ffill()
    if "open" in field_frame.columns:
        open_values = pd.Series(
            pd.to_numeric(field_frame["open"], errors="coerce").to_numpy(),
            index=observed_index,
        )
        authority["open"] = open_values.reindex(authority_index).to_numpy()
    elif "open" in authority.columns:
        # Some accepted development layouts persist only close plus feature
        # fields.  Their immutable session authority remains the owner of the
        # next-open execution price, so preserve that value instead of
        # requiring the field sidecar to duplicate it.
        authority["open"] = pd.to_numeric(
            authority["open"], errors="coerce"
        )
    else:
        raise RuntimeError(
            "replay open is absent from both field sidecar and session authority"
        )
    # A suspension has no executable open.  The replay kernel still needs a
    # finite inventory mark before it can block the sell, so use the last
    # observable close rather than carrying an unrelated prior-session open.
    authority["open"] = authority["open"].fillna(authority["close"])
    return authority, observed_index, authority_index


def _jsonable_replay_summary(replay: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in replay.items()
        if key not in {"daily", "fills"}
    }


def _replay_pair_results(
    candidates: pd.DataFrame,
    candidate_rows: Sequence[Mapping[str, Any]],
) -> pd.DataFrame:
    result_by_id = {
        str(row["candidate_id"]): dict(row)
        for row in candidate_rows
    }
    rows: list[dict[str, Any]] = []
    for pair_id, group in candidates.groupby("pair_id", sort=False):
        members = {
            str(row["pair_member_role"]): row
            for row in group.to_dict(orient="records")
        }
        primary = members["PRIMARY"]
        control = members["CONTROL"]
        primary_result = result_by_id[str(primary["candidate_id"])]
        control_result = result_by_id[str(control["candidate_id"])]
        primary_status = str(primary_result["candidate_replay_status"])
        control_status = str(control_result["candidate_replay_status"])
        pair_complete = (
            primary_status == "CANDIDATE_REPLAY_COMPLETE"
            and control_status == "CANDIDATE_REPLAY_COMPLETE"
        )
        primary_reward = _finite(
            primary_result.get("a_share_executable_net_reward")
        )
        control_reward = _finite(
            control_result.get("a_share_executable_net_reward")
        )
        rows.append(
            {
                "pair_id": str(pair_id),
                "route_id": str(primary["route_id"]),
                "primary_candidate_id": str(primary["candidate_id"]),
                "control_candidate_id": str(control["candidate_id"]),
                "primary_candidate_replay_status": primary_status,
                "control_candidate_replay_status": control_status,
                "primary_a_share_executable_net_reward": primary_reward,
                "control_a_share_executable_net_reward": control_reward,
                "a_share_executable_net_increment": (
                    primary_reward - control_reward
                    if pair_complete
                    and primary_reward is not None
                    and control_reward is not None
                    else None
                ),
                "primary_trade_count": primary_result.get("trade_count"),
                "control_trade_count": control_result.get("trade_count"),
                "primary_blocked_buy_count": primary_result.get(
                    "blocked_buy_count"
                ),
                "primary_blocked_sell_count": primary_result.get(
                    "blocked_sell_count"
                ),
                "primary_replay_blocker": primary_result.get("blocker_code"),
                "control_replay_blocker": control_result.get("blocker_code"),
                "a_share_replay_status": (
                    "PAIR_REPLAY_COMPLETE"
                    if pair_complete
                    else "PAIR_REPLAY_BLOCKED"
                ),
                "economic_claim_authorized": False,
                "promotion_authorized": False,
            }
        )
    return pd.DataFrame(rows)


def replay(
    *,
    freeze_path: Path,
    train_field_root: Path,
    output_root: Path,
) -> dict[str, Any]:
    initial_free_memory = _host_and_memory_gate()
    output_root = Path(output_root).resolve()
    freeze_path = Path(freeze_path).resolve()
    root = freeze_path.parents[1]
    freeze = _load_freeze(freeze_path)
    candidate_path = _resolved_artifact(
        root,
        freeze["candidate_artifact"],
    )
    contract_path = _resolved_artifact(
        root,
        freeze["execution_contract_artifact"],
    )
    candidates = pd.read_parquet(candidate_path).where(pd.notna, None)
    if candidates["candidate_id"].astype(str).tolist() != list(
        freeze["candidate_ids"]
    ):
        raise RuntimeError("replay cohort order drift")
    contract = _read_json(contract_path)
    _verify_payload_hash(
        contract,
        field="contract_payload_sha256",
        label="replay/OOS execution contract",
    )
    if str(contract.get("interstage_filtering")) != "FORBIDDEN":
        raise RuntimeError("interstage filtering boundary drift")
    split_hash = str(contract["split_manifest_sha256"])
    field_manifest, field_manifest_path = _validate_sidecar(
        train_field_root,
        evaluation_role="train",
        split_hash=split_hash,
    )
    field_frame = _load_field_frame(train_field_root, field_manifest)
    observed_dates = set(field_frame["date"].dt.date.astype(str))
    split = pd.read_csv(contract["split_manifest"], dtype=str)
    train_dates = set(
        split.loc[split["split"].eq("train"), "trade_date"].astype(str)
    )
    if observed_dates != train_dates:
        raise RuntimeError("train field sidecar calendar drift")

    session_manifest_path = Path(
        str(contract["session_authority_manifest"])
    ).resolve()
    if _sha256(session_manifest_path) != str(
        contract["session_authority_manifest_sha256"]
    ):
        raise RuntimeError("session authority manifest hash drift")
    session_path = Path(str(contract["session_authority_path"])).resolve()
    session_authority = pd.read_parquet(session_path)
    master, observed_index, authority_index = _materialize_replay_master(
        field_frame,
        session_authority,
    )
    if set(master["date"].dt.date.astype(str)) != train_dates:
        raise RuntimeError("session authority train calendar drift")

    fee = AShareFeeSchedule(**dict(contract["fee_schedule"]))
    universe_raw = dict(contract["universe_policy"])
    universe_raw["allowed_exchanges"] = tuple(
        universe_raw["allowed_exchanges"]
    )
    universe = AShareUniversePolicy(**universe_raw)
    execution = AShareExecutionPolicy(**dict(contract["execution_policy"]))
    corporate = AShareCorporateActionPolicy(
        **dict(contract["corporate_action_policy"])
    )
    input_data_sha256 = _stable_hash(
        {
            "freeze_sha256": _sha256(freeze_path),
            "train_field_manifest_sha256": _sha256(field_manifest_path),
            "session_authority_manifest_sha256": _sha256(
                session_manifest_path
            ),
            "session_authority_sha256": _sha256(session_path),
        }
    )
    replay_code_sha256 = _stable_hash(
        {
            "driver": _sha256(Path(__file__).resolve()),
            "kernel": _sha256(
                Path(__file__).resolve().parents[1]
                / "src"
                / "our_system_phase2"
                / "services"
                / "a_share_executable_replay.py"
            ),
            "expression_evaluator": _sha256(
                Path(__file__).resolve().parents[1]
                / "src"
                / "our_system_phase2"
                / "services"
                / "real_market_validation.py"
            ),
        }
    )
    candidate_root = output_root / "candidates"
    candidate_root.mkdir(parents=True, exist_ok=True)
    candidate_rows: list[dict[str, Any]] = []
    receipts: list[dict[str, Any]] = []
    blocked_receipts: list[dict[str, Any]] = []
    for candidate in candidates.to_dict(orient="records"):
        candidate_id = str(candidate["candidate_id"])
        target = candidate_root / f"{candidate_id}.json"
        if target.is_file():
            row = _read_json(target)
            common_drift = (
                str(row.get("candidate_id")) != candidate_id
                or str(row.get("input_data_sha256")) != input_data_sha256
            )
            status = str(row.get("status") or "")
            if status == "CANDIDATE_REPLAY_COMPLETE_IMMUTABLE":
                invalid = bool(
                    a_share_tradability_blockers(
                        row["receipt"],
                        expected_candidate_id=candidate_id,
                        expected_exact_identity=str(
                            candidate["exact_identity"]
                        ),
                    )
                )
                receipts.append(row["receipt"])
            elif status == "CANDIDATE_REPLAY_BLOCKED_IMMUTABLE":
                blocker = dict(row.get("blocker") or {})
                blocker_code = str(blocker.get("blocker_code") or "")
                invalid = (
                    str(blocker.get("candidate_id")) != candidate_id
                    or str(blocker.get("exact_identity"))
                    != str(candidate["exact_identity"])
                    or blocker.get("economic_claim_authorized") is not False
                )
                if blocker_code == "FINAL_SESSION_UNLIQUIDATED_HOLDINGS":
                    invalid = invalid or not blocker.get(
                        "remaining_holdings"
                    )
                elif blocker_code == "CORPORATE_ACTION_FRACTIONAL_SHARES":
                    invalid = invalid or (
                        not str(blocker.get("security_code") or "")
                        or not str(blocker.get("session_date") or "")
                        or int(blocker.get("opening_shares") or 0) <= 0
                        or str(
                            blocker.get(
                                "corporate_action_fractional_share_policy"
                            )
                            or ""
                        )
                        != "FAIL_CLOSED_NON_INTEGER"
                    )
                elif blocker_code == "NO_EXECUTABLE_FILLS":
                    receipt_blockers = a_share_tradability_blockers(
                        row.get("receipt") or {},
                        expected_candidate_id=candidate_id,
                        expected_exact_identity=str(
                            candidate["exact_identity"]
                        ),
                    )
                    invalid = invalid or (
                        _candidate_economic_blocker_code(receipt_blockers)
                        != blocker_code
                    )
                else:
                    invalid = True
                blocked_receipts.append(blocker)
            else:
                invalid = True
            if common_drift or invalid:
                raise RuntimeError(
                    f"completed replay candidate identity drift: {candidate_id}"
                )
            candidate_rows.append(row["summary"])
            continue
        signal = pd.to_numeric(
            evaluate_panel_expression(
                field_frame,
                str(candidate["expression"]),
                cache={},
                data_role="development",
            ),
            errors="coerce",
        )
        signal_by_coordinate = pd.Series(
            signal.to_numpy(),
            index=observed_index,
        )
        replay_frame = master.copy()
        replay_frame["signal"] = signal_by_coordinate.reindex(
            authority_index
        ).to_numpy()
        try:
            result = run_a_share_long_only_replay(
                replay_frame,
                fee_schedule=fee,
                universe_policy=universe,
                execution_policy=execution,
                corporate_action_policy=corporate,
            )
        except AShareCandidateReplayBlockerError as exc:
            blocker = _candidate_replay_exception_blocker(
                exc,
                candidate=candidate,
                input_data_sha256=input_data_sha256,
            )
            summary = {
                "candidate_id": candidate_id,
                "pair_id": str(candidate["pair_id"]),
                "pair_member_role": str(candidate["pair_member_role"]),
                "route_id": str(candidate["route_id"]),
                "exact_identity": str(candidate["exact_identity"]),
                "candidate_replay_status": "CANDIDATE_REPLAY_BLOCKED",
                "a_share_executable_net_reward": None,
                "blocker_code": blocker["blocker_code"],
                "train_read_count": len(replay_frame),
                "economic_claim_authorized": False,
                "promotion_authorized": False,
            }
            for key in (
                "remaining_holdings",
                "security_code",
                "session_date",
                "opening_shares",
                "corporate_action_share_multiplier",
                "adjusted_shares",
                "corporate_action_fractional_share_policy",
            ):
                if key in blocker:
                    summary[key] = blocker[key]
            _write_json(
                target,
                {
                    "schema_version": "cn_finalist_candidate_replay_result_v2",
                    "status": "CANDIDATE_REPLAY_BLOCKED_IMMUTABLE",
                    "candidate_id": candidate_id,
                    "input_data_sha256": input_data_sha256,
                    "summary": summary,
                    "blocker": blocker,
                },
            )
            candidate_rows.append(summary)
            blocked_receipts.append(blocker)
            print(
                json.dumps(
                    {
                        "candidate_id": candidate_id,
                        "status": "CANDIDATE_REPLAY_BLOCKED",
                        "blocker_code": blocker["blocker_code"],
                        **{
                            key: blocker[key]
                            for key in (
                                "remaining_holdings",
                                "security_code",
                                "session_date",
                            )
                            if key in blocker
                        },
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            continue
        summary = {
            "candidate_id": candidate_id,
            "pair_id": str(candidate["pair_id"]),
            "pair_member_role": str(candidate["pair_member_role"]),
            "route_id": str(candidate["route_id"]),
            "exact_identity": str(candidate["exact_identity"]),
            "candidate_replay_status": "CANDIDATE_REPLAY_COMPLETE",
            "train_read_count": len(replay_frame),
            **_jsonable_replay_summary(result),
        }
        receipt = build_a_share_tradability_receipt(
            candidate_id=candidate_id,
            candidate_exact_identity=str(candidate["exact_identity"]),
            replay_code_sha256=replay_code_sha256,
            input_data_sha256=input_data_sha256,
            universe_manifest_sha256=str(
                contract["universe_manifest_sha256"]
            ),
            fee_schedule_sha256=str(result["fee_schedule_sha256"]),
            execution_policy_sha256=str(
                result["execution_policy_sha256"]
            ),
            corporate_action_policy_sha256=str(
                result["corporate_action_policy_sha256"]
            ),
            executable_net_reward=float(
                result["a_share_executable_net_reward"]
            ),
            train_read_count=len(replay_frame),
            trade_count=int(result["trade_count"]),
            fill_count=int(result["fill_count"]),
            blocked_buy_count=int(result["blocked_buy_count"]),
            blocked_sell_count=int(result["blocked_sell_count"]),
            extra={
                "selection_payload_sha256": str(
                    freeze["selection_payload_sha256"]
                ),
                "train_field_manifest_sha256": _sha256(
                    field_manifest_path
                ),
                "session_authority_manifest_sha256": _sha256(
                    session_manifest_path
                ),
                "research_replay_only": True,
                "account_contract_confirmed": False,
            },
        )
        blockers = a_share_tradability_blockers(
            receipt,
            expected_candidate_id=candidate_id,
            expected_exact_identity=str(candidate["exact_identity"]),
        )
        if blockers:
            blocker_code = _candidate_economic_blocker_code(blockers)
            if blocker_code is not None:
                diagnostic_reward = float(
                    result["a_share_executable_net_reward"]
                )
                blocker = {
                    "schema_version": "cn_finalist_replay_blocker_v1",
                    "candidate_id": candidate_id,
                    "pair_id": str(candidate["pair_id"]),
                    "pair_member_role": str(
                        candidate["pair_member_role"]
                    ),
                    "route_id": str(candidate["route_id"]),
                    "exact_identity": str(candidate["exact_identity"]),
                    "blocker_code": blocker_code,
                    "receipt_blockers": list(blockers),
                    "diagnostic_net_reward": diagnostic_reward,
                    "trade_count": int(result["trade_count"]),
                    "fill_count": int(result["fill_count"]),
                    "input_data_sha256": input_data_sha256,
                    "fail_closed": True,
                    "economic_claim_authorized": False,
                    "promotion_authorized": False,
                }
                summary = {
                    **summary,
                    "candidate_replay_status": "CANDIDATE_REPLAY_BLOCKED",
                    "a_share_executable_net_reward": None,
                    "diagnostic_net_reward": diagnostic_reward,
                    "blocker_code": blocker_code,
                    "economic_claim_authorized": False,
                    "promotion_authorized": False,
                }
                _write_json(
                    target,
                    {
                        "schema_version": (
                            "cn_finalist_candidate_replay_result_v2"
                        ),
                        "status": "CANDIDATE_REPLAY_BLOCKED_IMMUTABLE",
                        "candidate_id": candidate_id,
                        "input_data_sha256": input_data_sha256,
                        "summary": summary,
                        "blocker": blocker,
                        "receipt": receipt,
                    },
                )
                candidate_rows.append(summary)
                blocked_receipts.append(blocker)
                print(
                    json.dumps(
                        {
                            "candidate_id": candidate_id,
                            "status": "CANDIDATE_REPLAY_BLOCKED",
                            "blocker_code": blocker_code,
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )
                continue
            raise RuntimeError(
                f"A-share replay receipt blocked for {candidate_id}: "
                + ",".join(blockers)
            )
        _write_json(
            target,
            {
                "schema_version": "cn_finalist_candidate_replay_result_v1",
                "status": "CANDIDATE_REPLAY_COMPLETE_IMMUTABLE",
                "candidate_id": candidate_id,
                "input_data_sha256": input_data_sha256,
                "summary": summary,
                "receipt": receipt,
            },
        )
        candidate_rows.append(summary)
        receipts.append(receipt)
        print(
            json.dumps(
                {
                    "candidate_id": candidate_id,
                    "status": "CANDIDATE_REPLAY_COMPLETE",
                    "reward": summary[
                        "a_share_executable_net_reward"
                    ],
                },
                sort_keys=True,
            ),
            flush=True,
        )
    if len(candidate_rows) != EXPECTED_MEMBER_COUNT:
        raise RuntimeError("replay candidate result count drift")
    if [str(row["candidate_id"]) for row in candidate_rows] != list(
        freeze["candidate_ids"]
    ):
        raise RuntimeError("replay candidate identity/order drift")
    candidate_results_path = output_root / "candidate_replay_results.parquet"
    candidate_result_summary_frame(candidate_rows).to_parquet(
        candidate_results_path,
        index=False,
    )
    pair_results = _replay_pair_results(candidates, candidate_rows)
    pair_results_path = output_root / "pair_replay_results.parquet"
    pair_results.to_parquet(pair_results_path, index=False)
    receipt_path = _write_jsonl(
        output_root / "a_share_tradability_replay_receipts.jsonl",
        receipts,
    )
    blocker_path = _write_jsonl(
        output_root / "a_share_replay_blockers.jsonl",
        blocked_receipts,
    )
    complete_pairs = pair_results["a_share_replay_status"].eq(
        "PAIR_REPLAY_COMPLETE"
    )
    summary = {
        "schema_version": "cn_finalist_replay_summary_v1",
        "status": "A_SHARE_EXECUTABLE_TRAIN_REPLAY_COMPLETE",
        "selection_payload_sha256": freeze["selection_payload_sha256"],
        "pair_count": len(pair_results),
        "candidate_member_count": len(candidate_rows),
        "candidate_replay_complete_count": len(receipts),
        "candidate_replay_blocked_count": len(blocked_receipts),
        "pair_replay_complete_count": int(complete_pairs.sum()),
        "pair_replay_blocked_count": int((~complete_pairs).sum()),
        "primary_positive_reward_count": int(
            (
                pair_results[
                    "primary_a_share_executable_net_reward"
                ]
                > 0
            ).loc[complete_pairs].sum()
        ),
        "positive_executable_increment_count": int(
            (
                pair_results["a_share_executable_net_increment"] > 0
            ).loc[complete_pairs].sum()
        ),
        "primary_reward_median": _finite(
            pair_results[
                "primary_a_share_executable_net_reward"
            ].loc[complete_pairs].median()
        ),
        "executable_increment_median": _finite(
            pair_results.loc[
                complete_pairs,
                "a_share_executable_net_increment",
            ].median()
        ),
        "train_reads": int(
            sum(int(row["train_read_count"]) for row in candidate_rows)
        ),
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "economic_claim_authorized": False,
    }
    summary_path = _write_json(
        output_root / "replay_summary.json",
        summary,
    )
    artifacts = [
        freeze_path,
        contract_path,
        field_manifest_path,
        session_manifest_path,
        candidate_results_path,
        pair_results_path,
        receipt_path,
        blocker_path,
        summary_path,
        *sorted(candidate_root.glob("*.json")),
    ]
    closure = {
        "schema_version": "cn_finalist_replay_closure_v1",
        "status": "A_SHARE_REPLAY_CLOSED_IMMUTABLE",
        "selection_payload_sha256": freeze["selection_payload_sha256"],
        "pair_count": EXPECTED_PAIR_COUNT,
        "candidate_member_count": EXPECTED_MEMBER_COUNT,
        "input_data_sha256": input_data_sha256,
        "initial_free_memory_bytes": initial_free_memory,
        "minimum_free_memory_gate_bytes": MINIMUM_FREE_MEMORY_BYTES,
        "train_reads": summary["train_reads"],
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "economic_claim_authorized": False,
        "artifacts": [_artifact(path) for path in artifacts],
    }
    closure["manifest_body_sha256"] = _stable_hash(closure)
    closure_path = _write_json(
        output_root / "REPLAY_COMPLETE.json",
        closure,
    )
    return {
        **summary,
        "closure_path": str(closure_path),
        "closure_sha256": _sha256(closure_path),
    }


def _validate_label_sidecar(
    root: Path,
    *,
    split_hash: str,
    evaluation_role: str = "validation",
) -> tuple[dict[str, Any], Path]:
    path = Path(root).resolve() / "CN_FORWARD_LABEL_SIDECAR_MANIFEST.json"
    manifest = _read_json(path)
    required = {
        "status": "GLOBAL_SYMBOL_CONTINUITY_LABEL_SIDECARS_READY",
        "evaluation_role": evaluation_role,
        "data_role": f"{evaluation_role}_report_only",
        "split_manifest_hash": split_hash,
        "holdout_reads": 0,
    }
    drift = [
        key
        for key, value in required.items()
        if manifest.get(key) != value
    ]
    if drift:
        raise RuntimeError(
            f"{evaluation_role} label sidecar drift: " + ",".join(drift)
        )
    if evaluation_role == "forward_2026":
        if int(manifest.get("validation_reads") or 0) != 0:
            raise RuntimeError("forward label sidecar read validation")
        if int(manifest.get("forward_2026_reads") or 0) <= 0:
            raise RuntimeError("forward label sidecar has no forward 2026 reads")
    elif evaluation_role == "validation":
        if int(manifest.get("validation_reads") or 0) <= 0:
            raise RuntimeError("validation label sidecar has no validation reads")
    elif evaluation_role == "holdout":
        if int(manifest.get("validation_reads") or 0) != 0:
            raise RuntimeError("holdout label sidecar read validation")
        if int(manifest.get("holdout_reads") or 0) <= 0:
            raise RuntimeError("holdout label sidecar has no holdout reads")
    elif evaluation_role == "historical_challenge":
        if int(manifest.get("validation_reads") or 0) != 0:
            raise RuntimeError("historical challenge label sidecar read validation")
        if int(manifest.get("historical_challenge_reads") or 0) <= 0:
            raise RuntimeError(
                "historical challenge label sidecar has no historical reads"
            )
    else:
        raise RuntimeError(f"unsupported label sidecar role: {evaluation_role}")
    if evaluation_role != "forward_2026" and int(
        manifest.get("forward_2026_reads") or 0
    ) != 0:
        raise RuntimeError(f"{evaluation_role} label sidecar read forward 2026")
    return manifest, path


def _oos_pair_rows(
    candidates: pd.DataFrame,
    result: Mapping[str, Any],
    replay_pairs: pd.DataFrame,
) -> pd.DataFrame:
    rewards = {
        str(row["candidate_id"]): dict(row)
        for row in result.get("candidate_rewards") or ()
    }
    pair_results = {
        str(row["pair_id"]): dict(row)
        for row in result.get("pair_results") or ()
    }
    replay_by_pair = {
        str(row["pair_id"]): dict(row)
        for row in replay_pairs.to_dict(orient="records")
    }
    rows: list[dict[str, Any]] = []
    for pair_id, group in candidates.groupby("pair_id", sort=False):
        members = {
            str(row["pair_member_role"]): row
            for row in group.to_dict(orient="records")
        }
        primary = members["PRIMARY"]
        control = members["CONTROL"]
        pair = pair_results.get(str(pair_id), {})
        primary_reward = rewards.get(
            str(primary["candidate_id"]),
            {},
        )
        primary_metric = _finite(
            primary_reward.get("validation_report_metric")
        )
        pair_metric = _finite(
            pair.get("pair_validation_report_metric")
        )
        validation_score = (
            min(primary_metric, pair_metric)
            if primary_metric is not None and pair_metric is not None
            else None
        )
        replay = replay_by_pair[str(pair_id)]
        status = str(pair.get("pair_evaluation_status") or "")
        rows.append(
            {
                "pair_id": str(pair_id),
                "route_id": str(primary["route_id"]),
                "primary_candidate_id": str(primary["candidate_id"]),
                "control_candidate_id": str(control["candidate_id"]),
                "a_share_replay_status": str(
                    replay["a_share_replay_status"]
                ),
                "primary_a_share_executable_net_reward": _finite(
                    replay["primary_a_share_executable_net_reward"]
                ),
                "control_a_share_executable_net_reward": _finite(
                    replay["control_a_share_executable_net_reward"]
                ),
                "a_share_executable_net_increment": _finite(
                    replay["a_share_executable_net_increment"]
                ),
                "validation_pair_status": status,
                "validation_pair_blockers": str(
                    pair.get("pair_evaluation_blockers") or ""
                ),
                "validation_pair_support_count": int(
                    pair.get("pair_support_count") or 0
                ),
                "primary_validation_report_metric": primary_metric,
                "control_validation_report_metric": _finite(
                    pair.get("control_validation_report_metric")
                ),
                "pair_validation_report_metric": pair_metric,
                "validation_search_score": validation_score,
                "oos_positive_transfer": bool(
                    status == "PAIR_EVALUATED"
                    and validation_score is not None
                    and validation_score > 0
                ),
                "validation_day_sortino": _finite(
                    primary_reward.get("validation_day_sortino")
                ),
                "validation_worst_horizon_day_sortino": _finite(
                    primary_reward.get("train_worst_horizon_day_sortino")
                ),
                "validation_mean_one_way_turnover": _finite(
                    primary_reward.get("train_mean_one_way_turnover")
                ),
                "validation_regime_positive_share": _finite(
                    primary_reward.get("train_regime_positive_share")
                ),
                "validation_regime_worst_day_sortino": _finite(
                    primary_reward.get("train_regime_worst_day_sortino")
                ),
                "interstage_filter_applied": False,
                "economic_claim_authorized": False,
                "promotion_authorized": False,
            }
        )
    return pd.DataFrame(rows)


def oos(
    *,
    freeze_path: Path,
    replay_root: Path,
    validation_field_root: Path,
    validation_label_root: Path,
    output_root: Path,
    threads: int,
) -> dict[str, Any]:
    initial_free_memory = _host_and_memory_gate()
    freeze_path = Path(freeze_path).resolve()
    root = freeze_path.parents[1]
    freeze = _load_freeze(freeze_path)
    candidate_path = _resolved_artifact(
        root,
        freeze["candidate_artifact"],
    )
    contract_path = _resolved_artifact(
        root,
        freeze["execution_contract_artifact"],
    )
    contract = _read_json(contract_path)
    replay_complete_path = (
        Path(replay_root).resolve() / "REPLAY_COMPLETE.json"
    )
    replay_complete = _read_json(replay_complete_path)
    _verify_payload_hash(
        replay_complete,
        field="manifest_body_sha256",
        label="A-share replay closure",
    )
    if str(replay_complete.get("status") or "") != (
        "A_SHARE_REPLAY_CLOSED_IMMUTABLE"
    ):
        raise RuntimeError("A-share replay is not closed immutable")
    if str(replay_complete["selection_payload_sha256"]) != str(
        freeze["selection_payload_sha256"]
    ):
        raise RuntimeError("replay/OOS selection hash drift")
    replay_pairs_path = (
        Path(replay_root).resolve() / "pair_replay_results.parquet"
    )
    replay_pairs = pd.read_parquet(replay_pairs_path)
    if len(replay_pairs) != EXPECTED_PAIR_COUNT:
        raise RuntimeError("replay pair count drift before OOS")

    split_hash = str(contract["split_manifest_sha256"])
    _, field_manifest_path = _validate_sidecar(
        validation_field_root,
        evaluation_role="validation",
        split_hash=split_hash,
    )
    _, label_manifest_path = _validate_label_sidecar(
        validation_label_root,
        split_hash=split_hash,
    )
    candidates = pd.read_parquet(candidate_path).where(pd.notna, None)
    if candidates["candidate_id"].astype(str).tolist() != list(
        freeze["candidate_ids"]
    ):
        raise RuntimeError("OOS cohort differs from replay cohort")
    registry = UnifiedCapabilityRegistry.read(
        Path(str(contract["registry"])).resolve()
    )
    split = FixedSplitAuthority.read(
        Path(str(contract["split_manifest"])).resolve()
    )
    output_root = Path(output_root).resolve()
    evaluation_root = output_root / "evaluation"
    evaluation_root.mkdir(parents=True, exist_ok=True)
    sidecar_binding_hash = _stable_hash(
        {
            "validation_field_manifest": _sha256(field_manifest_path),
            "validation_label_manifest": _sha256(label_manifest_path),
            "selection_payload_sha256": freeze[
                "selection_payload_sha256"
            ],
        }
    )
    binding_path, table_paths = _context_and_binding(
        batch_root=evaluation_root,
        candidates=candidates.to_dict(orient="records"),
        registry=registry,
        split=split,
        data_release_hash=sidecar_binding_hash,
        evaluation_role="validation",
    )
    if set(table_paths) != {"stock_session"}:
        raise RuntimeError("finalist OOS unexpectedly routed off stock_session")
    access_receipts = _run_phase3cm(
        batch_id=f"fixed_{EXPECTED_PAIR_COUNT}_replay_then_oos",
        batch_root=evaluation_root,
        binding_path=binding_path,
        table_paths=table_paths,
        split_manifest=Path(str(contract["split_manifest"])).resolve(),
        field_roots={
            "stock_session": Path(validation_field_root).resolve()
        },
        label_roots={
            "stock_session": Path(validation_label_root).resolve()
        },
        compute_threads={"stock_session": int(threads)},
        evaluation_role="validation",
    )
    result_path = (
        evaluation_root
        / "phase3cm_validation"
        / "stock_session"
        / "CN_STREAMING_BACKEND_RESULT.json"
    )
    result = _read_json(result_path)
    required = {
        "evaluation_role": "validation",
        "validation_usage": "report_only",
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "portfolio_mode": "long_only_top",
    }
    drift = [
        key
        for key, value in required.items()
        if result.get(key) != value
    ]
    if drift:
        raise RuntimeError("OOS backend boundary drift: " + ",".join(drift))
    if int(result.get("validation_reads") or 0) <= 0:
        raise RuntimeError("OOS backend has no validation reads")
    if float(result.get("cost_bps") or 0.0) != 5.0:
        raise RuntimeError("OOS backend cost drift")
    if [int(value) for value in result.get("horizons") or ()] != [
        1,
        5,
        15,
        30,
    ]:
        raise RuntimeError("OOS backend horizon drift")
    pair_rows = _oos_pair_rows(candidates, result, replay_pairs)
    if len(pair_rows) != EXPECTED_PAIR_COUNT:
        raise RuntimeError("OOS pair result count drift")
    pair_path = output_root / "replay_then_oos_pair_results.parquet"
    pair_rows.to_parquet(pair_path, index=False)
    evaluated = pair_rows["validation_pair_status"].eq("PAIR_EVALUATED")
    summary = {
        "schema_version": "cn_finalist_replay_then_oos_report_v1",
        "status": "UNCHANGED_COHORT_REPORT_ONLY_OOS_COMPLETE",
        "selection_payload_sha256": freeze["selection_payload_sha256"],
        "pair_count": EXPECTED_PAIR_COUNT,
        "candidate_member_count": EXPECTED_MEMBER_COUNT,
        "interstage_filter_applied": False,
        "oos_evaluated_pair_count": int(evaluated.sum()),
        "oos_blocked_pair_count": int((~evaluated).sum()),
        "oos_positive_transfer_count": int(
            pair_rows["oos_positive_transfer"].sum()
        ),
        "oos_positive_transfer_share_selected": float(
            pair_rows["oos_positive_transfer"].mean()
        ),
        "validation_score_median": _finite(
            pair_rows["validation_search_score"].median()
        ),
        "validation_score_p10": _finite(
            pair_rows["validation_search_score"].quantile(0.1)
        ),
        "primary_executable_reward_median": _finite(
            pair_rows[
                "primary_a_share_executable_net_reward"
            ].median()
        ),
        "executable_increment_median": _finite(
            pair_rows["a_share_executable_net_increment"].median()
        ),
        "validation_reads": int(result["validation_reads"]),
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "economic_claim_authorized": False,
    }
    summary_path = _write_json(
        output_root / "replay_then_oos_report.json",
        summary,
    )
    access_receipt_paths = [
        evaluation_root
        / "phase3cm_validation"
        / str(row["backend"])
        / "ITERATIVE_ACCESS_RECEIPT.json"
        for row in access_receipts
    ]
    closure = {
        "schema_version": "cn_finalist_oos_closure_v1",
        "status": "REPORT_ONLY_OOS_CLOSED_IMMUTABLE",
        "selection_payload_sha256": freeze["selection_payload_sha256"],
        "pair_count": EXPECTED_PAIR_COUNT,
        "candidate_member_count": EXPECTED_MEMBER_COUNT,
        "replay_closure_sha256": _sha256(replay_complete_path),
        "interstage_filter_applied": False,
        "initial_free_memory_bytes": initial_free_memory,
        "minimum_free_memory_gate_bytes": MINIMUM_FREE_MEMORY_BYTES,
        "validation_reads": summary["validation_reads"],
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "economic_claim_authorized": False,
        "artifacts": [
            _artifact(path)
            for path in (
                freeze_path,
                replay_complete_path,
                field_manifest_path,
                label_manifest_path,
                binding_path,
                result_path,
                pair_path,
                summary_path,
                *access_receipt_paths,
            )
        ],
    }
    closure["manifest_body_sha256"] = _stable_hash(closure)
    closure_path = _write_json(
        output_root / "OOS_COMPLETE.json",
        closure,
    )
    return {
        **summary,
        "closure_path": str(closure_path),
        "closure_sha256": _sha256(closure_path),
    }


def _verify_closure(path: Path, *, expected_status: str) -> dict[str, Any]:
    payload = _read_json(path)
    _verify_payload_hash(
        payload,
        field="manifest_body_sha256",
        label=str(path),
    )
    if str(payload.get("status") or "") != expected_status:
        raise RuntimeError(f"closure status drift: {path}")
    for row in payload.get("artifacts") or ():
        artifact_path = Path(str(row["path"])).resolve()
        if not artifact_path.is_file():
            raise FileNotFoundError(
                f"closure artifact missing: {artifact_path}"
            )
        if int(row["bytes"]) != artifact_path.stat().st_size:
            raise RuntimeError(
                f"closure artifact size drift: {artifact_path}"
            )
        if str(row["sha256"]) != _sha256(artifact_path):
            raise RuntimeError(
                f"closure artifact hash drift: {artifact_path}"
            )
    return payload


def _resolved_train_replay_recomputed(
    execution_contract: dict[str, Any],
    override: bool | None,
) -> bool:
    if override is not None:
        return bool(override)
    return bool(execution_contract.get("train_replay_recomputed", True))


def finalize(
    *,
    freeze_path: Path,
    replay_root: Path,
    oos_root: Path,
    output_root: Path,
    train_replay_recomputed: bool | None = None,
) -> dict[str, Any]:
    _host_and_memory_gate()
    freeze_path = Path(freeze_path).resolve()
    freeze = _load_freeze(freeze_path)
    freeze_root = freeze_path.parents[1]
    contract_path = _resolved_artifact(
        freeze_root, freeze["execution_contract_artifact"]
    )
    execution_contract = _read_json(contract_path)
    _verify_payload_hash(
        execution_contract,
        field="contract_payload_sha256",
        label="replay/OOS execution contract",
    )
    replay_path = Path(replay_root).resolve() / "REPLAY_COMPLETE.json"
    oos_path = Path(oos_root).resolve() / "OOS_COMPLETE.json"
    replay_closure = _verify_closure(
        replay_path,
        expected_status="A_SHARE_REPLAY_CLOSED_IMMUTABLE",
    )
    oos_closure = _verify_closure(
        oos_path,
        expected_status="REPORT_ONLY_OOS_CLOSED_IMMUTABLE",
    )
    for payload in (replay_closure, oos_closure):
        if str(payload["selection_payload_sha256"]) != str(
            freeze["selection_payload_sha256"]
        ):
            raise RuntimeError("closure selection hash drift")
        if int(payload.get("holdout_reads") or 0) != 0:
            raise RuntimeError("closure contains holdout reads")
        if int(payload.get("forward_2026_reads") or 0) != 0:
            raise RuntimeError("closure contains forward-2026 reads")
        for key in ("feedback_write", "scheduler_write", "archive_write"):
            if str(payload.get(key)) != "FORBIDDEN":
                raise RuntimeError(f"closure {key} boundary drift")
        if str(payload.get("promotion")) != "FORBIDDEN":
            raise RuntimeError("closure promotion boundary drift")
    if int(replay_closure.get("train_reads") or 0) <= 0:
        raise RuntimeError("replay closure has no train reads")
    if int(oos_closure.get("validation_reads") or 0) <= 0:
        raise RuntimeError("OOS closure has no validation reads")
    observed_source_hashes = {
        path: _sha256(Path(path))
        for path in freeze["protected_source_hashes"]
    }
    if observed_source_hashes != dict(
        freeze["protected_source_hashes"]
    ):
        raise RuntimeError("protected source artifacts changed")
    report_path = Path(oos_root).resolve() / "replay_then_oos_report.json"
    report = _read_json(report_path)
    output_root = Path(output_root).resolve()
    replay_was_recomputed = _resolved_train_replay_recomputed(
        execution_contract,
        train_replay_recomputed,
    )
    closure = {
        "schema_version": "cn_finalist_replay_then_oos_root_closure_v1",
        "status": "REPLAY_THEN_OOS_COMPLETE_IMMUTABLE_REPORT_ONLY",
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "selection_payload_sha256": freeze["selection_payload_sha256"],
        "pair_count": EXPECTED_PAIR_COUNT,
        "candidate_member_count": EXPECTED_MEMBER_COUNT,
        "sequence_completed": list(execution_contract["sequence"]),
        "train_replay_recomputed": replay_was_recomputed,
        "interstage_filter_applied": False,
        "protected_source_hashes_unchanged": True,
        "replay_closure_sha256": _sha256(replay_path),
        "oos_closure_sha256": _sha256(oos_path),
        "oos_evaluated_pair_count": int(
            report["oos_evaluated_pair_count"]
        ),
        "oos_blocked_pair_count": int(report["oos_blocked_pair_count"]),
        "oos_positive_transfer_count": int(
            report["oos_positive_transfer_count"]
        ),
        "validation_reads": int(oos_closure["validation_reads"]),
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "economic_claim_authorized": False,
        "successor_search_authorized": False,
        "artifacts": [
            _artifact(path)
            for path in (
                freeze_path,
                replay_path,
                oos_path,
                report_path,
                Path(oos_root).resolve()
                / "replay_then_oos_pair_results.parquet",
            )
        ],
    }
    closure["manifest_body_sha256"] = _stable_hash(closure)
    closure_path = _write_json(
        output_root / "REPLAY_THEN_OOS_COMPLETE.json",
        closure,
    )
    return {
        **closure,
        "closure_path": str(closure_path),
        "closure_sha256": _sha256(closure_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare_parser = subparsers.add_parser("prepare")
    prepare_parser.add_argument("--cohort-root", type=Path, required=True)
    prepare_parser.add_argument("--authority-root", type=Path, required=True)
    prepare_parser.add_argument("--split-manifest", type=Path, required=True)
    prepare_parser.add_argument("--registry", type=Path, required=True)
    prepare_parser.add_argument("--output-root", type=Path, required=True)
    prepare_parser.add_argument("--repo-sha", required=True)
    prepare_parser.add_argument(
        "--expected-pair-count", type=int, default=24
    )

    replay_parser = subparsers.add_parser("replay")
    replay_parser.add_argument("--freeze-manifest", type=Path, required=True)
    replay_parser.add_argument("--train-field-root", type=Path, required=True)
    replay_parser.add_argument("--output-root", type=Path, required=True)
    replay_parser.add_argument(
        "--expected-pair-count", type=int, default=24
    )

    oos_parser = subparsers.add_parser("oos")
    oos_parser.add_argument("--freeze-manifest", type=Path, required=True)
    oos_parser.add_argument("--replay-root", type=Path, required=True)
    oos_parser.add_argument(
        "--validation-field-root",
        type=Path,
        required=True,
    )
    oos_parser.add_argument(
        "--validation-label-root",
        type=Path,
        required=True,
    )
    oos_parser.add_argument("--output-root", type=Path, required=True)
    oos_parser.add_argument("--threads", type=int, default=32)
    oos_parser.add_argument(
        "--expected-pair-count", type=int, default=24
    )

    finalize_parser = subparsers.add_parser("finalize")
    finalize_parser.add_argument(
        "--freeze-manifest",
        type=Path,
        required=True,
    )
    finalize_parser.add_argument("--replay-root", type=Path, required=True)
    finalize_parser.add_argument("--oos-root", type=Path, required=True)
    finalize_parser.add_argument("--output-root", type=Path, required=True)
    finalize_parser.add_argument(
        "--expected-pair-count", type=int, default=24
    )
    finalize_parser.add_argument(
        "--train-replay-recomputed",
        choices=("true", "false"),
        default=None,
    )

    args = parser.parse_args()
    if str(os.environ.get("CN_NODE_RESOURCE_LEASE_REQUIRED") or "") == "1":
        receipt_value = str(
            os.environ.get("CN_NODE_RESOURCE_LEASE_RECEIPT") or ""
        ).strip()
        entitlement = int(
            str(os.environ.get("CN_NODE_CPU_ENTITLEMENT") or "0")
        )
        if not receipt_value or entitlement < 1:
            raise RuntimeError("finalist replay node resource lease is incomplete")
        receipt_path = Path(receipt_value)
        validate_node_resource_lease_receipt(
            receipt_path,
            expected_role="VALIDATION",
            expected_cpu_threads=entitlement,
        )
    _configure_expected_cohort_size(args.expected_pair_count)
    if args.command == "prepare":
        result = prepare(
            cohort_root=args.cohort_root,
            authority_root=args.authority_root,
            split_manifest=args.split_manifest,
            registry=args.registry,
            output_root=args.output_root,
            repo_sha=args.repo_sha,
        )
    elif args.command == "replay":
        result = replay(
            freeze_path=args.freeze_manifest,
            train_field_root=args.train_field_root,
            output_root=args.output_root,
        )
    elif args.command == "oos":
        result = oos(
            freeze_path=args.freeze_manifest,
            replay_root=args.replay_root,
            validation_field_root=args.validation_field_root,
            validation_label_root=args.validation_label_root,
            output_root=args.output_root,
            threads=args.threads,
        )
    else:
        result = finalize(
            freeze_path=args.freeze_manifest,
            replay_root=args.replay_root,
            oos_root=args.oos_root,
            output_root=args.output_root,
            train_replay_recomputed=(
                None
                if args.train_replay_recomputed is None
                else args.train_replay_recomputed == "true"
            ),
        )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
