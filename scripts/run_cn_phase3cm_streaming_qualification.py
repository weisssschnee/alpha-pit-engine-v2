from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import time
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd
import polars as pl

from our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit import (
    _candidate_summary_from_reward_atoms,
)
from our_system_phase2.services.phase3cm_streaming_block_reader import TimeMajorBlockReader
from our_system_phase2.services.phase3cm_streaming_checkpoint import (
    StreamingCheckpointPayload,
    load_checkpoint,
    write_checkpoint,
)
from our_system_phase2.services.phase3cm_streaming_capacity import (
    enforce_block_row_limit,
)
from our_system_phase2.services.phase3cm_streaming_dag import SharedMultiCandidateDAGPlan
from our_system_phase2.services.phase3cm_streaming_expression import StreamingExpressionExecutor
from our_system_phase2.services.phase3cm_streaming_portfolio import (
    BatchedPortfolioKernel,
    prepare_portfolio_block,
)
from our_system_phase2.services.portfolio_behavior_archive import (
    PortfolioBehaviorArchive,
    StreamingLabelFreeBehavior,
    pair_behavior_record,
)
from our_system_phase2.services.phase3cm_streaming_reducer import StreamingPortfolioReducer
from our_system_phase2.services.phase3cm_streaming_resource_contract import (
    FrozenExecutionPlan,
    RSSGate,
    balanced_pair_batches,
    validate_frozen_thread_environment,
)
from our_system_phase2.services.phase3cm_streaming_support import PairSupportAccumulator
from our_system_phase2.services.phase3cm_streaming_telemetry import (
    PhaseTelemetryRecorder,
    _process_snapshot,
    aggregate_compute_phase_parallelism,
    build_phase_event,
)

EVALUATION_ROLES = frozenset({"train", "validation", "holdout"})
REPORT_ONLY_EVALUATION_ROLES = frozenset({"validation", "holdout"})


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _candidate_finalization_seed_map(
    candidates: Sequence[Mapping[str, Any]],
) -> dict[str, int]:
    """Preserve the historically qualified table-order seed for each identity."""

    result: dict[str, int] = {}
    for candidate_index, candidate in enumerate(candidates):
        candidate_id = str(candidate.get("candidate_id") or "")
        if not candidate_id or candidate_id in result:
            raise ValueError("candidate finalization seed map requires unique candidate_id")
        result[candidate_id] = 20260623 + candidate_index
    return result


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_csv(path: Path) -> list[dict[str, str]]:
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    fieldnames = list(rows[0].keys()) if rows else []
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        if fieldnames:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="raise")
            writer.writeheader()
            writer.writerows(rows)
        handle.flush()
    temporary.replace(destination)
    return {
        "path": destination.name,
        "row_count": len(rows),
        "sha256": _sha256(destination),
        "bytes": destination.stat().st_size,
    }


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _finite_float(value: Any) -> float | None:
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return None
    return converted if math.isfinite(converted) else None


def _evaluation_calendar(
    split_manifest: Path,
    binding: Mapping[str, Any],
    *,
    evaluation_role: str,
) -> tuple[str, ...]:
    if evaluation_role not in EVALUATION_ROLES:
        raise ValueError(f"unsupported evaluation role: {evaluation_role}")
    path = Path(split_manifest)
    if not path.is_file() or _sha256(path) != str(binding.get("split_manifest_hash") or ""):
        raise RuntimeError("CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: split manifest")
    rows = _read_csv(path)
    dates = tuple(
        str(row["trade_date"])
        for row in rows
        if str(row.get("split") or "").strip().lower() == evaluation_role
    )
    if not dates or len(set(dates)) != len(dates):
        raise RuntimeError(f"frozen {evaluation_role} calendar is empty or duplicated")
    expected_usage = "allowed" if evaluation_role == "train" else "report_only"
    if any(
        str(row.get("optimizer_usage") or "") != expected_usage
        for row in rows
        if row["trade_date"] in dates
    ):
        raise RuntimeError(
            f"frozen {evaluation_role} calendar has wrong optimizer usage"
        )
    return dates


def _train_calendar(split_manifest: Path, binding: Mapping[str, Any]) -> tuple[str, ...]:
    return _evaluation_calendar(split_manifest, binding, evaluation_role="train")


def _session_sample_calendar(
    sample_manifest: Path,
    *,
    binding: Mapping[str, Any],
    evaluation_role: str,
    full_calendar: Sequence[str],
) -> tuple[tuple[str, ...], dict[str, Any]]:
    """Bind a frozen session subset without changing the fixed split authority."""

    if evaluation_role != "train":
        raise RuntimeError(
            "SESSION_SAMPLED_PHASE3CM_IS_DEVELOPMENT_TRAIN_ONLY"
        )
    path = Path(sample_manifest).resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    claimed = str(payload.get("contract_hash") or "")
    unsigned = dict(payload)
    unsigned.pop("contract_hash", None)
    if claimed != _stable_hash(unsigned):
        raise RuntimeError("SESSION_SAMPLE_CONTRACT_HASH_MISMATCH")
    bound = dict(binding.get("session_sample") or {})
    if (
        str(binding.get("evaluation_scope") or "")
        != "development_session_sample"
        or str(bound.get("sha256") or "") != _sha256(path)
    ):
        raise RuntimeError("SESSION_SAMPLE_NOT_BOUND_TO_INPUT")
    expected = {
        "schema_version": "cn_phase3cm_session_sample_contract_v1",
        "authority_id": "MINUTE_STATIC_PHASE3CM_SESSION_SAMPLE_V1",
        "status": "FROZEN_DETERMINISTIC_SESSION_SUBSET",
        "evaluation_role": "train",
        "full_split_manifest_hash": str(
            binding.get("split_manifest_hash") or ""
        ),
        "selection": "sha256(sample_seed,session_id)_calendar_month",
        "sample_fraction": 0.25,
        "uses_return_or_regime_labels": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    drift = [
        key for key, value in expected.items()
        if payload.get(key) != value
    ]
    if drift:
        raise RuntimeError(
            "SESSION_SAMPLE_CONTRACT_DRIFT:" + ",".join(sorted(drift))
        )
    selected = tuple(map(str, payload.get("selected_sessions") or ()))
    if (
        not selected
        or len(selected) != len(set(selected))
        or list(selected) != sorted(selected)
        or int(payload.get("selected_session_count") or -1)
        != len(selected)
    ):
        raise RuntimeError("SESSION_SAMPLE_SELECTION_INVALID")
    full = set(map(str, full_calendar))
    if any(date not in full or date >= "2026-01-01" for date in selected):
        raise RuntimeError("SESSION_SAMPLE_OUTSIDE_TRAIN_AUTHORITY")
    return selected, {
        "path": str(path),
        "sha256": _sha256(path),
        "contract_hash": claimed,
        "selected_session_count": len(selected),
        "full_session_count": len(full_calendar),
        "sample_fraction": float(payload["sample_fraction"]),
        "selection": str(payload["selection"]),
    }


def _verify_binding(
    binding_path: Path,
    artifact_root: Path,
    *,
    evaluation_role: str = "train",
) -> dict[str, Any]:
    binding = json.loads(Path(binding_path).read_text(encoding="utf-8"))
    claimed = str(binding.get("binding_hash") or "")
    body = dict(binding)
    body.pop("binding_hash", None)
    if _stable_hash(body) != claimed:
        raise RuntimeError("CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: binding hash")
    if str(binding.get("status")) != "CN_STREAMING_REPAIR_FROZEN_INPUT_BOUND":
        raise RuntimeError("CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: binding status")
    expected_data_role = (
        "development" if evaluation_role == "train" else f"{evaluation_role}_report_only"
    )
    if str(binding.get("data_role")) != expected_data_role:
        raise RuntimeError("CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: data role")
    expected_sealed = {
        "train": {"validation": 0, "holdout": 0, "forward_2026": 0},
        "validation": {"holdout": 0, "forward_2026": 0},
        "holdout": {"forward_2026": 0},
    }[evaluation_role]
    if binding.get("sealed_reads") != expected_sealed:
        raise RuntimeError("CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: sealed reads")
    for record in binding.get("artifacts") or []:
        path = Path(artifact_root) / str(record["path"])
        if not path.is_file() or _sha256(path) != str(record["sha256"]):
            raise RuntimeError(
                f"CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: {record['path']}"
            )
    return binding


def _verify_split_boundary_purity(
    purity_path: Path,
    *,
    binding: Mapping[str, Any],
) -> dict[str, Any]:
    record = dict(binding.get("split_boundary_purity") or {})
    if not record:
        raise RuntimeError(
            "CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: split purity binding"
        )
    source = Path(purity_path).resolve()
    if not source.is_file() or _sha256(source) != str(record.get("sha256") or ""):
        raise RuntimeError(
            "CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: split purity hash"
        )
    purity = json.loads(source.read_text(encoding="utf-8"))
    if (
        purity.get("status") != "PASS"
        or purity.get("split_manifest_hash") != binding.get("split_manifest_hash")
        or purity.get("enforcement_contract")
        != "FINITE_SIGNAL_AND_LABEL_INTERSECTION_IN_BATCHED_PORTFOLIO_KERNEL"
        or int(binding.get("retained_label_crossing_count") or 0) != 0
        or not purity.get("routes")
        or any(int(row.get("retained_crossing_count") or 0) != 0 for row in purity["routes"])
    ):
        raise RuntimeError(
            "CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: split purity contract"
        )
    return purity


def _candidate_pairs(
    rows: Sequence[Mapping[str, Any]],
    *,
    pair_limit: int,
    clock: str,
    require_exact_count: bool = False,
) -> list[dict[str, Any]]:
    pair_order: list[str] = []
    by_pair: dict[str, list[dict[str, Any]]] = {}
    for raw in rows:
        row = dict(raw)
        pair_id = str(row.get("pair_id") or "")
        if not pair_id:
            raise RuntimeError("frozen candidate row is missing pair_id")
        declared_clock = str(row.get("clock_namespace") or "")
        if declared_clock and declared_clock != clock:
            raise RuntimeError(f"frozen candidate clock namespace drift: {pair_id}")
        if pair_id not in by_pair:
            pair_order.append(pair_id)
            by_pair[pair_id] = []
        row["clock_namespace"] = clock
        by_pair[pair_id].append(row)
    if require_exact_count and len(pair_order) != int(pair_limit):
        raise RuntimeError(
            "Phase E candidate table pair count does not match the frozen plan scope"
        )
    selected: list[dict[str, Any]] = []
    for pair_id in pair_order[: int(pair_limit)]:
        members = by_pair[pair_id]
        if len(members) != 2:
            raise RuntimeError(f"frozen pair does not contain exactly two members: {pair_id}")
        members.sort(key=lambda row: 0 if str(row.get("pair_member_role")) == "PRIMARY" else 1)
        if [str(row.get("pair_member_role")) for row in members] != ["PRIMARY", "CONTROL"]:
            raise RuntimeError(f"frozen pair roles drift: {pair_id}")
        selected.extend(members)
    if len(selected) != int(pair_limit) * 2:
        raise RuntimeError("requested pair count exceeds frozen route pack")
    return selected


def _phase_e_plan_pair_order(
    *,
    pair_batches: Sequence[Sequence[str]],
    candidate_pair_ids: Sequence[str],
    maximum_batch_size: int,
) -> tuple[str, ...]:
    """Validate and return the immutable Phase E pair order.

    Phase E treats the pre-frozen plan as the ordering authority.  Membership
    remains an exact-set gate against the candidate table, so ordering cannot
    add, drop, duplicate, or split matched pairs.
    """

    limit = int(maximum_batch_size)
    if limit <= 0:
        raise ValueError("pair batch size must be positive")
    normalized_batches = tuple(
        tuple(str(pair_id) for pair_id in batch) for batch in pair_batches
    )
    if not normalized_batches or any(not batch for batch in normalized_batches):
        raise RuntimeError("Phase E execution plan contains an empty pair batch")
    if any(len(batch) > limit for batch in normalized_batches):
        raise RuntimeError("Phase E execution plan exceeds the frozen pair batch size")
    ordered = tuple(pair_id for batch in normalized_batches for pair_id in batch)
    if len(ordered) != len(set(ordered)):
        raise RuntimeError("Phase E execution plan contains duplicate pair identities")
    available = tuple(str(pair_id) for pair_id in candidate_pair_ids)
    if len(available) != len(set(available)):
        raise RuntimeError("Phase E candidate table contains duplicate pair identities")
    if set(ordered) != set(available):
        raise RuntimeError(
            "Phase E execution plan and candidate table pair identities differ"
        )
    return ordered


def _order_candidate_pairs(
    candidates: Sequence[Mapping[str, Any]],
    ordered_pair_ids: Sequence[str],
) -> list[dict[str, Any]]:
    """Reorder whole matched pairs while preserving PRIMARY then CONTROL."""

    by_pair: dict[str, list[dict[str, Any]]] = {}
    for raw in candidates:
        pair_id = str(raw.get("pair_id") or "")
        by_pair.setdefault(pair_id, []).append(dict(raw))
    output: list[dict[str, Any]] = []
    for pair_id in ordered_pair_ids:
        members = by_pair.get(str(pair_id)) or []
        members.sort(
            key=lambda row: (
                0 if str(row.get("pair_member_role") or "") == "PRIMARY" else 1,
                str(row.get("candidate_id") or ""),
            )
        )
        if [str(row.get("pair_member_role") or "") for row in members] != [
            "PRIMARY",
            "CONTROL",
        ]:
            raise RuntimeError(
                f"Phase E frozen pair role identity drift: {pair_id}"
            )
        output.extend(members)
    if len(output) != len(candidates):
        raise RuntimeError("Phase E frozen pair ordering changed candidate membership")
    return output


def _json_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    text = str(value or "").strip()
    if not text:
        return []
    decoded = json.loads(text)
    if not isinstance(decoded, list):
        raise RuntimeError("frozen candidate field_ids is not a JSON list")
    return [str(item) for item in decoded]


def _verify_selected_candidate_semantics(
    candidates: Sequence[Mapping[str, Any]],
    *,
    binding: Mapping[str, Any],
    clock: str,
) -> None:
    """Prove that the CSV rows execute the exact members named by the binding."""

    bound_member_rows = list(binding.get("candidate_members") or [])
    bound_members = {
        str(row.get("candidate_id") or ""): dict(row) for row in bound_member_rows
    }
    if "" in bound_members or len(bound_members) != len(bound_member_rows):
        raise RuntimeError(
            "CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: duplicate bound members"
        )
    bound_pair_rows = list(binding.get("pairs") or [])
    bound_pairs = {str(row.get("pair_id") or ""): dict(row) for row in bound_pair_rows}
    if "" in bound_pairs or len(bound_pairs) != len(bound_pair_rows):
        raise RuntimeError(
            "CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: duplicate bound pairs"
        )

    observed: dict[str, dict[str, Any]] = {}
    by_pair: dict[str, dict[str, str]] = {}
    for raw in candidates:
        row = dict(raw)
        candidate_id = str(row.get("candidate_id") or "")
        if not candidate_id or candidate_id in observed:
            raise RuntimeError(
                "CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: duplicate selected members"
            )
        expected = bound_members.get(candidate_id)
        if expected is None or str(expected.get("clock_namespace") or "") != clock:
            raise RuntimeError(
                "CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: selected members"
            )
        for key in (
            "pair_id",
            "pair_member_role",
            "expression",
            "canonical_expression",
            "route_id",
        ):
            if str(row.get(key) or "") != str(expected.get(key) or ""):
                raise RuntimeError(
                    "CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: "
                    f"candidate semantic drift {candidate_id}:{key}"
                )
        if _json_string_list(row.get("field_ids")) != [
            str(value) for value in expected.get("field_ids") or []
        ]:
            raise RuntimeError(
                "CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: "
                f"candidate semantic drift {candidate_id}:field_ids"
            )
        pair_id = str(row["pair_id"])
        role = str(row["pair_member_role"])
        by_pair.setdefault(pair_id, {})[role] = candidate_id
        observed[candidate_id] = row

    for pair_id, roles in by_pair.items():
        pair = bound_pairs.get(pair_id)
        if pair is None or str(pair.get("clock_namespace") or "") != clock:
            raise RuntimeError(
                f"CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: selected pair {pair_id}"
            )
        if roles != {
            "PRIMARY": str(pair.get("candidate_id") or ""),
            "CONTROL": str(pair.get("control_candidate_id") or ""),
        }:
            raise RuntimeError(
                f"CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: pair membership {pair_id}"
            )


def _raw_fields(candidates: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    fields = {"close"}
    for row in candidates:
        fields.update(re.findall(r"\$([A-Za-z_][A-Za-z0-9_]*)", str(row.get("expression") or "")))
    return tuple(sorted(fields))


def _sidecar_files(root: Path) -> tuple[Path, ...]:
    paths = tuple(sorted(path for path in Path(root).glob("shard_*.parquet") if path.is_file()))
    if len(paths) != 16:
        raise RuntimeError(f"qualification requires exactly 16 sidecars: {root} has {len(paths)}")
    return paths


def _symbol_registry(paths: Sequence[Path]) -> tuple[str, ...]:
    lazy = pl.concat(
        [pl.scan_parquet(path, rechunk=False, low_memory=True).select(pl.col("code").cast(pl.String)) for path in paths]
    )
    values = lazy.select(pl.col("code").unique().sort()).collect(engine="streaming")["code"].to_list()
    return tuple(str(value) for value in values)


def _session_dates(paths: Sequence[Path]) -> tuple[str, ...]:
    lazy = pl.concat(
        [
            pl.scan_parquet(path, rechunk=False, low_memory=True).select(
                pl.col("trade_time").dt.date().alias("date")
            )
            for path in paths
        ]
    )
    values = lazy.select(pl.col("date").unique().sort()).collect(engine="streaming")["date"].to_list()
    return tuple(str(value) for value in values)


def _block_boundaries(dates: Sequence[str], block_sessions: int) -> tuple[tuple[str, str], ...]:
    output = []
    for start in range(0, len(dates), int(block_sessions)):
        chunk = dates[start : start + int(block_sessions)]
        begin = pd.Timestamp(chunk[0]).isoformat()
        end = (pd.Timestamp(chunk[-1]) + pd.Timedelta(days=1)).isoformat()
        output.append((begin, end))
    return tuple(output)


def _pair_batches(pair_ids: Sequence[str], batch_pairs: int) -> tuple[tuple[str, ...], ...]:
    return balanced_pair_batches(pair_ids, int(batch_pairs))


def _candidate_direction(candidate: Mapping[str, Any]) -> float:
    value = str(candidate.get("open_direction") or "long_top").strip().lower()
    if value in {"long_top", "top", "positive", "1", "+1"}:
        return 1.0
    if value in {"long_bottom", "bottom", "negative", "-1"}:
        return -1.0
    raise ValueError(f"unsupported frozen candidate direction: {value}")


def _portfolio_continuation_payload(batches: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "cn_phase3cm_batched_portfolio_continuation_v1",
        "batches": [
            {
                "pair_ids": list(batch["pair_ids"]),
                "pair_indices": list(batch["pair_indices"]),
                "candidate_indices": list(batch["candidate_indices"]),
                "payload": batch["kernel"].continuation_payload(),
                "label_free_behavior_payload": (
                    batch["label_free_behavior"].continuation_payload()
                    if batch.get("label_free_behavior") is not None
                    else None
                ),
            }
            for batch in batches
        ],
    }


def _restore_portfolio_continuation_payload(
    batches: Sequence[Mapping[str, Any]],
    payload: Mapping[str, Any],
) -> None:
    if str(payload.get("schema_version")) != "cn_phase3cm_batched_portfolio_continuation_v1":
        raise ValueError("portfolio continuation schema drift")
    rows = list(payload.get("batches") or [])
    if len(rows) != len(batches):
        raise ValueError("portfolio continuation batch count drift")
    for expected, observed in zip(batches, rows):
        if tuple(map(str, observed.get("pair_ids") or ())) != tuple(expected["pair_ids"]):
            raise ValueError("portfolio continuation pair identity drift")
        if tuple(map(int, observed.get("pair_indices") or ())) != tuple(expected["pair_indices"]):
            raise ValueError("portfolio continuation pair index drift")
        if tuple(map(int, observed.get("candidate_indices") or ())) != tuple(
            expected["candidate_indices"]
        ):
            raise ValueError("portfolio continuation candidate index drift")
        expected["kernel"].restore_continuation_payload(observed["payload"])
        behavior_payload = observed.get("label_free_behavior_payload")
        if behavior_payload is not None:
            if expected.get("label_free_behavior") is None:
                raise ValueError("checkpoint contains unexpected label-free behavior state")
            expected["label_free_behavior"].restore_continuation_payload(behavior_payload)


def _load_plan(path: Path) -> FrozenExecutionPlan:
    return FrozenExecutionPlan.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))


def _write_json(path: Path, payload: Any) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _support_masks(signals: np.ndarray) -> np.ndarray:
    if signals.shape[0] % 2:
        raise ValueError("candidate member count must be even")
    pair_count = signals.shape[0] // 2
    masks = np.empty((pair_count, signals.shape[1]), dtype=np.bool_)
    for pair_index in range(pair_count):
        common = np.isfinite(signals[2 * pair_index]) & np.isfinite(signals[2 * pair_index + 1])
        masks[pair_index] = common
        signals[2 * pair_index, ~common] = np.nan
        signals[2 * pair_index + 1, ~common] = np.nan
    return masks


def _finalize_pairs(
    *,
    candidates: Sequence[Mapping[str, Any]],
    reward_rows: Sequence[Mapping[str, Any]],
    reducer: StreamingPortfolioReducer,
    support: PairSupportAccumulator,
    binding: Mapping[str, Any],
    evaluation_role: str = "train",
    label_free_behavior_by_candidate: Mapping[str, Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    reward_by_id = {str(row.get("candidate_id")): dict(row) for row in reward_rows}
    member_binding = {str(row["candidate_id"]): dict(row) for row in binding["candidate_members"]}
    pair_binding = {str(row["pair_id"]): dict(row) for row in binding["pairs"]}
    support_identity = support.identities()
    rows = []
    for pair_index in range(len(candidates) // 2):
        primary = dict(candidates[2 * pair_index])
        control = dict(candidates[2 * pair_index + 1])
        pair_id = str(primary["pair_id"])
        primary_reward = reward_by_id.get(str(primary["candidate_id"]), {})
        control_reward = reward_by_id.get(str(control["candidate_id"]), {})
        primary_value = _finite_float(primary_reward.get("optimizer_reward"))
        control_value = _finite_float(control_reward.get("optimizer_reward"))
        blockers: list[str] = []
        support_row = support_identity[pair_id]
        if int(support_row["count"]) == 0:
            blockers.append("empty_pair_support")
        primary_behavior = reducer.behavior_identity(2 * pair_index)
        control_behavior = reducer.behavior_identity(2 * pair_index + 1)
        if label_free_behavior_by_candidate is not None:
            primary_exact_behavior = str(
                label_free_behavior_by_candidate.get(str(primary["candidate_id"]), {}).get(
                    "portfolio_behavior_signature_id"
                )
                or ""
            )
            control_exact_behavior = str(
                label_free_behavior_by_candidate.get(str(control["candidate_id"]), {}).get(
                    "portfolio_behavior_signature_id"
                )
                or ""
            )
            exact_behavior_duplicate = bool(primary_exact_behavior) and (
                primary_exact_behavior == control_exact_behavior
            )
        else:
            exact_behavior_duplicate = primary_behavior == control_behavior
        if exact_behavior_duplicate and not _truthy(primary.get("allow_behavior_equivalence")):
            blockers.append("control_behavior_identity_equals_primary")
        primary_spread = float(np.max(reducer.stats[2 * pair_index, :, -1]))
        control_spread = float(np.max(reducer.stats[2 * pair_index + 1, :, -1]))
        if primary_spread <= 1e-15:
            blockers.append("primary_signal_empty_or_constant")
        if control_spread <= 1e-15:
            blockers.append("control_signal_empty_or_constant")
        if primary_value is None:
            blockers.append("primary_not_evaluated")
        if control_value is None:
            blockers.append("control_not_evaluated")
        matched = None if blockers else float(primary_value) - float(control_value)
        bound_pair = pair_binding[pair_id]
        metrics = (
            {
                "pair_train_reward": matched,
                "primary_train_reward": primary_value,
                "control_train_reward": control_value,
                "optimizer_reward": matched,
                "optimizer_reward_split": "train",
            }
            if evaluation_role == "train"
            else {
                f"pair_{evaluation_role}_report_metric": matched,
                f"primary_{evaluation_role}_report_metric": primary_value,
                f"control_{evaluation_role}_report_metric": control_value,
                f"{evaluation_role}_usage": "report_only",
            }
        )
        rows.append(
            {
                "pair_id": pair_id,
                "primary_candidate_id": primary["candidate_id"],
                "control_candidate_id": control["candidate_id"],
                "primary_receipt_hash": member_binding[str(primary["candidate_id"])]["receipt_hash"],
                "control_receipt_hash": member_binding[str(control["candidate_id"])]["receipt_hash"],
                "pair_receipt_hash": bound_pair["pair_receipt_hash"],
                "pair_evaluation_status": "PAIR_EVALUATED" if not blockers else "PAIR_EVALUATION_BLOCKED",
                "pair_evaluation_blockers": "|".join(sorted(blockers)),
                "pair_support_count": support_row["count"],
                "pair_support_identity": support_row["support_identity"],
                "pair_support_overlap": 1.0 if int(support_row["count"]) else 0.0,
                "primary_behavior_identity": primary_behavior,
                "control_behavior_identity": control_behavior,
                "primary_signal_spread_max": primary_spread,
                "control_signal_spread_max": control_spread,
                "streaming_identity_schema": "block_composable_v1",
                **metrics,
            }
        )
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=("active_bar", "stock_session"), required=True)
    parser.add_argument(
        "--evaluation-role",
        choices=("train", "validation", "holdout"),
        default="train",
    )
    parser.add_argument("--phase", choices=("C", "D", "E"), required=True)
    parser.add_argument("--pair-count", type=int, required=True)
    parser.add_argument("--candidate-table", type=Path, required=True)
    parser.add_argument("--binding", type=Path, required=True)
    parser.add_argument("--split-boundary-purity", type=Path)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument(
        "--session-sample-manifest",
        type=Path,
        help=(
            "Optional frozen train-session subset. The official split "
            "manifest remains the access authority."
        ),
    )
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--field-sidecar-root", type=Path, required=True)
    parser.add_argument("--label-sidecar-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--iterative-batch-id", default="")
    parser.add_argument("--execution-plan", type=Path)
    parser.add_argument("--block-sessions", type=int, default=5)
    parser.add_argument("--pair-batch-size", type=int, default=4)
    parser.add_argument("--compute-threads", type=int, default=16)
    parser.add_argument("--horizons", default="1,5,15,30")
    parser.add_argument("--min-obs", type=int, default=20)
    parser.add_argument("--cost-bps", type=float, default=5.0)
    parser.add_argument("--top-quantile", type=float, default=0.2)
    parser.add_argument("--portfolio-mode", default="long_only_top")
    parser.add_argument("--checkpoint-every-blocks", type=int, default=1)
    parser.add_argument(
        "--max-block-rows",
        type=int,
        default=0,
        help="Frozen Phase E cache-capacity bound; enforced on every materialized block.",
    )
    parser.add_argument("--capacity-receipt-hash", default="")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument(
        "--stop-after-blocks",
        type=int,
        default=0,
        help="Phase C/D controlled-stop hook used only to prove checkpoint/resume parity.",
    )
    args = parser.parse_args()

    if int(args.stop_after_blocks) < 0:
        raise ValueError("stop-after-blocks cannot be negative")
    if int(args.max_block_rows) < 0:
        raise ValueError("max-block-rows cannot be negative")
    if args.phase == "E" and int(args.stop_after_blocks):
        raise RuntimeError("Phase E forbids adaptive or controlled plan interruption")
    if args.phase == "E":
        if int(args.max_block_rows) <= 0:
            raise RuntimeError("Phase E requires a positive preflight max-block-rows")
        if re.fullmatch(r"[0-9a-f]{64}", str(args.capacity_receipt_hash)) is None:
            raise RuntimeError("Phase E requires an exact capacity receipt hash")
    elif args.capacity_receipt_hash and re.fullmatch(
        r"[0-9a-f]{64}", str(args.capacity_receipt_hash)
    ) is None:
        raise ValueError("capacity-receipt-hash must be lowercase SHA-256")

    output_root = args.output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    binding = _verify_binding(
        args.binding.resolve(),
        args.artifact_root.resolve(),
        evaluation_role=args.evaluation_role,
    )
    if binding.get("split_boundary_purity"):
        if args.split_boundary_purity is None:
            raise RuntimeError(
                "CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: split purity argument"
            )
        split_boundary_purity = _verify_split_boundary_purity(
            args.split_boundary_purity.resolve(), binding=binding
        )
        split_boundary_purity_hash = _sha256(args.split_boundary_purity.resolve())
    else:
        split_boundary_purity = None
        split_boundary_purity_hash = None
    evaluation_dates = _evaluation_calendar(
        args.split_manifest.resolve(),
        binding,
        evaluation_role=args.evaluation_role,
    )
    session_sample = None
    if args.session_sample_manifest is not None:
        evaluation_dates, session_sample = _session_sample_calendar(
            args.session_sample_manifest.resolve(),
            binding=binding,
            evaluation_role=args.evaluation_role,
            full_calendar=evaluation_dates,
        )
    candidates = _candidate_pairs(
        _read_csv(args.candidate_table.resolve()),
        pair_limit=int(args.pair_count),
        clock=str(args.backend),
        require_exact_count=args.phase == "E",
    )
    _verify_selected_candidate_semantics(
        candidates,
        binding=binding,
        clock=str(args.backend),
    )
    finalization_seed_by_candidate_id = _candidate_finalization_seed_map(candidates)
    pair_ids = tuple(str(candidates[index]["pair_id"]) for index in range(0, len(candidates), 2))
    field_paths = _sidecar_files(args.field_sidecar_root.resolve())
    label_paths = _sidecar_files(args.label_sidecar_root.resolve())
    symbols = _symbol_registry(field_paths)
    discovered_boundaries = _block_boundaries(evaluation_dates, int(args.block_sessions))
    if args.phase == "E":
        if args.execution_plan is None:
            raise ValueError("Phase E requires a pre-frozen execution plan")
        plan = _load_plan(args.execution_plan.resolve())
        if plan.phase != "E":
            raise RuntimeError("Phase E execution plan has the wrong phase")
        if plan.block_boundaries != discovered_boundaries:
            raise RuntimeError("Phase E calendar block boundaries drift")
        pair_ids = _phase_e_plan_pair_order(
            pair_batches=plan.pair_batches,
            candidate_pair_ids=pair_ids,
            maximum_batch_size=int(args.pair_batch_size),
        )
        candidates = _order_candidate_pairs(candidates, pair_ids)
        _verify_selected_candidate_semantics(
            candidates,
            binding=binding,
            clock=str(args.backend),
        )
    else:
        batches = _pair_batches(pair_ids, int(args.pair_batch_size))
        high_capacity_host = int(args.compute_threads) >= 30
        plan = FrozenExecutionPlan.create(
            phase=args.phase,
            block_size=int(args.block_sessions),
            block_boundaries=discovered_boundaries,
            pair_batches=batches,
            heavy_processes=1,
            compute_threads=int(args.compute_threads),
            primary_thread_pool="numba",
            cache_caps={
                "dag_block_cache_bytes": 8 * 1024**3,
                "raw_block_arena_bytes": 6 * 1024**3,
                "temporary_array_bytes": 6 * 1024**3,
            },
            checkpoint_every_blocks=int(args.checkpoint_every_blocks),
            rss_soft_bytes=(40 if high_capacity_host else 20) * 1024**3,
            rss_hard_bytes=(48 if high_capacity_host else 24) * 1024**3,
            global_rss_hard_bytes=(72 if high_capacity_host else 60) * 1024**3,
        )
    validate_frozen_thread_environment(plan)
    _write_json(output_root / "CN_FROZEN_EXECUTION_PLAN.json", plan.to_dict())

    dag_started = time.perf_counter()
    dag_plan = SharedMultiCandidateDAGPlan.build(candidates)
    _write_json(output_root / "CN_SHARED_DAG_PLAN.json", dag_plan.to_dict())
    dag_seconds = time.perf_counter() - dag_started
    timing_path = output_root / "CN_PHASE3CM_PHASE_TIMING.jsonl"
    if not args.resume:
        timing_path.unlink(missing_ok=True)
    telemetry = PhaseTelemetryRecorder(
        output_path=timing_path,
        allocated_compute_threads=plan.compute_threads,
    )
    telemetry._append(
        build_phase_event(
            phase="shared_dag_planning",
            wall_seconds=dag_seconds,
            cpu_seconds=dag_seconds,
            allocated_compute_threads=plan.compute_threads,
            compute_heavy=False,
            rss_before_bytes=0,
            rss_after_bytes=_process_snapshot().rss_bytes,
            peak_rss_bytes=_process_snapshot().peak_rss_bytes,
            dag_node_count=len(dag_plan.nodes),
            value_cohort_count=len(dag_plan.value_cohorts),
            mapping_subcohort_count=len(dag_plan.mapping_subcohorts),
        )
    )

    horizons = tuple(int(value) for value in str(args.horizons).split(",") if value)
    reader = TimeMajorBlockReader(
        field_sidecars=field_paths,
        label_sidecars=label_paths,
        raw_fields=_raw_fields(candidates),
        horizons=horizons,
        symbol_registry=symbols,
        eligible_trade_dates=evaluation_dates,
    )
    expression = StreamingExpressionExecutor(
        code_count=len(symbols),
        compute_threads=plan.compute_threads,
        cache_max_bytes=plan.cache_caps["dag_block_cache_bytes"],
    )
    root_by_candidate = {row.candidate_id: row for row in dag_plan.candidate_roots}
    pair_index_by_id = {pair_id: index for index, pair_id in enumerate(pair_ids)}
    candidate_indices_by_pair = {
        pair_id: (2 * pair_index, 2 * pair_index + 1)
        for pair_id, pair_index in pair_index_by_id.items()
    }
    portfolio_batches: list[dict[str, Any]] = []
    for batch_ordinal, batch_pair_ids in enumerate(plan.pair_batches):
        batch_pair_indices = tuple(pair_index_by_id[pair_id] for pair_id in batch_pair_ids)
        batch_candidate_indices = tuple(
            candidate_index
            for pair_id in batch_pair_ids
            for candidate_index in candidate_indices_by_pair[pair_id]
        )
        members = tuple(candidates[index] for index in batch_candidate_indices)
        roots = tuple(root_by_candidate[str(row["candidate_id"])] for row in members)
        portfolio_batches.append(
            {
                "batch_ordinal": batch_ordinal,
                "pair_ids": tuple(batch_pair_ids),
                "pair_indices": batch_pair_indices,
                "candidate_indices": batch_candidate_indices,
                "members": members,
                "value_namespaces": tuple(root.value_cohort_id for root in roots),
                "mapping_namespaces": tuple(root.mapping_subcohort_id for root in roots),
                "directions": np.asarray(
                    [_candidate_direction(row) for row in members],
                    dtype=np.float64,
                ),
                "label_free_behavior": StreamingLabelFreeBehavior(
                    candidate_ids=tuple(str(row["candidate_id"]) for row in members),
                    code_count=len(symbols),
                    coordinate_binding=_stable_hash(
                        {
                            "input_binding_hash": binding["binding_hash"],
                            "execution_plan_hash": plan.execution_plan_hash,
                            "behavior_scope": "full-coordinate-development",
                        }
                    ),
                    scope="full",
                    min_obs=int(args.min_obs),
                    top_quantile=float(args.top_quantile),
                ),
                "kernel": BatchedPortfolioKernel(
                    candidate_count=len(members),
                    code_count=len(symbols),
                    horizons=horizons,
                    compute_threads=plan.compute_threads,
                    min_obs=int(args.min_obs),
                    top_quantile=float(args.top_quantile),
                    cost_bps=float(args.cost_bps),
                    portfolio_mode=str(args.portfolio_mode),
                ),
            }
        )
    release_node_ids = dag_plan.release_node_ids_by_candidate_batch(
        tuple(
            tuple(str(row["candidate_id"]) for row in batch["members"])
            for batch in portfolio_batches
        )
    )
    dag_node_by_id = {node.node_id: node for node in dag_plan.nodes}

    def cache_keys_for_nodes(node_ids: Iterable[str]) -> tuple[str, ...]:
        return tuple(
            expression.cache_key(
                dag_node_by_id[node_id].canonical_expression,
                value_namespace=dag_node_by_id[node_id].cohort_id,
                mapping_namespace=(
                    dag_node_by_id[node_id].mapping_subcohort_id
                    if dag_node_by_id[node_id].layer == "MAPPING"
                    else None
                ),
            )
            for node_id in node_ids
        )

    for portfolio_batch, node_ids in zip(portfolio_batches, release_node_ids):
        portfolio_batch["release_cache_keys"] = cache_keys_for_nodes(node_ids)
    ordered_candidate_ids = tuple(
        str(row["candidate_id"])
        for portfolio_batch in portfolio_batches
        for row in portfolio_batch["members"]
    )
    release_node_ids_by_candidate = dag_plan.release_node_ids_by_candidate_batch(
        tuple((candidate_id,) for candidate_id in ordered_candidate_ids)
    )
    release_keys_by_candidate = {
        candidate_id: cache_keys_for_nodes(node_ids)
        for candidate_id, node_ids in zip(
            ordered_candidate_ids,
            release_node_ids_by_candidate,
        )
    }
    for portfolio_batch in portfolio_batches:
        portfolio_batch["release_cache_keys_after_member"] = tuple(
            release_keys_by_candidate[str(row["candidate_id"])]
            for row in portfolio_batch["members"]
        )
    reducer = StreamingPortfolioReducer(candidates=candidates, horizons=horizons)
    support = PairSupportAccumulator(pair_ids=pair_ids)
    completed_blocks: list[int] = []
    checkpoint_ordinal = 0
    checkpoint_root = output_root / "checkpoints" / str(args.backend)
    total_rows = 0
    expression_audits: list[dict[str, Any]] = []
    portfolio_audits: list[dict[str, Any]] = []
    prior_evaluator_wall_seconds = 0.0
    max_observed_block_rows = 0

    if args.resume:
        manifest_path = checkpoint_root / "CN_STREAMING_CHECKPOINT_MANIFEST.json"
        if not manifest_path.is_file():
            raise RuntimeError("resume requested without a complete checkpoint manifest")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        payload = load_checkpoint(
            checkpoint_root / str(manifest["latest_complete_checkpoint"]),
            expected_execution_plan_hash=plan.execution_plan_hash,
            expected_input_binding_hash=str(binding["binding_hash"]),
        )
        expression.restore_continuation_payload(
            {
                "rolling": payload.temporal_continuation_payload,
                "state": payload.state_event_continuation_payload["state"],
            }
        )
        support.restore_continuation_payload(payload.state_event_continuation_payload["support"])
        _restore_portfolio_continuation_payload(
            portfolio_batches,
            payload.portfolio_continuation_payload,
        )
        reducer.restore_continuation_payload(payload.streaming_reducer_payload)
        completed_blocks = list(payload.completed_blocks)
        checkpoint_ordinal = int(payload.execution_position.get("checkpoint_ordinal") or 0)
        total_rows = int(payload.execution_position.get("rows_processed") or 0)
        expression_audits = [
            dict(row) for row in payload.execution_position.get("expression_audits") or []
        ]
        portfolio_audits = [
            dict(row) for row in payload.execution_position.get("portfolio_audits") or []
        ]
        prior_evaluator_wall_seconds = float(
            payload.execution_position.get("evaluator_wall_seconds") or 0.0
        )
        max_observed_block_rows = int(
            payload.execution_position.get("max_observed_block_rows") or 0
        )

    runtime_position = {"block_ordinal": -1}

    def checkpoint(reason: str) -> None:
        nonlocal checkpoint_ordinal
        checkpoint_ordinal += 1
        continuation = expression.continuation_payload()
        write_checkpoint(
            checkpoint_root,
            StreamingCheckpointPayload(
                execution_plan_hash=plan.execution_plan_hash,
                input_binding_hash=str(binding["binding_hash"]),
                backend_identity=str(args.backend),
                route_cohort=f"{args.backend}:{args.pair_count}pairs",
                execution_position={
                    "block_ordinal": int(runtime_position["block_ordinal"]),
                    "checkpoint_ordinal": checkpoint_ordinal,
                    "reason": reason,
                    "rows_processed": int(total_rows),
                    "max_observed_block_rows": int(max_observed_block_rows),
                    "evaluator_wall_seconds": prior_evaluator_wall_seconds
                    + (time.perf_counter() - run_started),
                    "expression_audits": list(expression_audits),
                    "portfolio_audits": list(portfolio_audits),
                },
                completed_blocks=list(completed_blocks),
                completed_pair_batches=[_stable_hash(list(batch)) for batch in plan.pair_batches],
                temporal_continuation_payload=continuation["rolling"],
                state_event_continuation_payload={
                    "state": continuation["state"],
                    "support": support.continuation_payload(),
                },
                portfolio_continuation_payload=_portfolio_continuation_payload(portfolio_batches),
                streaming_reducer_payload=reducer.continuation_payload(),
            ),
            checkpoint_ordinal=checkpoint_ordinal,
        )

    rss_gate = RSSGate(
        soft_bytes=plan.rss_soft_bytes,
        hard_bytes=plan.rss_hard_bytes,
        global_hard_bytes=plan.global_rss_hard_bytes,
        checkpoint=checkpoint,
    )
    run_started = time.perf_counter()
    for block_ordinal, (start, end) in enumerate(plan.block_boundaries):
        if block_ordinal in completed_blocks:
            continue
        runtime_position["block_ordinal"] = block_ordinal
        with telemetry.phase("global_trade_time_barrier", compute_heavy=False) as phase:
            block = reader.read_block(start_time=pd.Timestamp(start), end_time=pd.Timestamp(end))
            if int(args.max_block_rows) > 0:
                enforce_block_row_limit(
                    block.row_count,
                    max_block_rows=int(args.max_block_rows),
                    block_ordinal=block_ordinal,
                )
            max_observed_block_rows = max(max_observed_block_rows, int(block.row_count))
            phase.add(
                rows_read=block.row_count,
                blocks_processed=1,
                raw_fields_loaded=len(block.raw_fields),
                portfolio_coordinates_processed=block.row_count,
            )
        total_rows += block.row_count
        with telemetry.phase("expression_block_bind", compute_heavy=False) as phase:
            expression.bind_block(
                raw_fields=block.raw_fields,
                code_ids=block.code_ids,
                time_ids=block.time_ids,
            )
            phase.add(
                dag_nodes_evaluated=0,
                dag_reuse_count=0,
                signals_materialized=0,
                temporary_rows_allocated=0,
                cache_peak_bytes=expression.audit.get("cache_peak_bytes", 0),
                block_bind=True,
            )
        with telemetry.phase("portfolio_label_order_prepare", compute_heavy=True) as phase:
            prepared_portfolio_block = prepare_portfolio_block(
                labels=block.labels,
                horizons=horizons,
                time_ids=block.time_ids,
                compute_threads=plan.compute_threads,
            )
            phase.add(
                label_order_rows=block.row_count * len(horizons),
                global_trade_time_count=len(prepared_portfolio_block.starts),
                shared_across_pair_batches=len(portfolio_batches),
            )
        with telemetry.phase("pair_support_coordinate_prepare", compute_heavy=False) as phase:
            support_block_tokens = support.prepare_block_tokens(
                trade_times_ns=block.trade_times_ns,
                code_ids=block.code_ids,
                source_shards=block.source_shards,
                source_row_identity=block.source_row_identity,
                duplicate_ordinal=block.duplicate_ordinal,
            )
            phase.add(
                token_rows=block.row_count,
                shared_across_pair_batches=len(portfolio_batches),
            )
        for portfolio_batch in portfolio_batches:
            batch_ordinal = int(portfolio_batch["batch_ordinal"])
            members = tuple(portfolio_batch["members"])
            candidate_indices = tuple(portfolio_batch["candidate_indices"])
            pair_indices = tuple(portfolio_batch["pair_indices"])
            pair_batch_ids = tuple(portfolio_batch["pair_ids"])
            before = {
                key: int(expression.audit.get(key) or 0)
                for key in (
                    "value_node_evaluations",
                    "mapping_node_evaluations",
                    "cache_hits",
                    "native_kernel_calls",
                    "intra_batch_released_entries",
                    "intra_batch_released_bytes",
                    "root_cache_bypass_count",
                    "root_cache_bypass_bytes",
                    "cache_pressure_evicted_entries",
                    "cache_pressure_evicted_bytes",
                    "cache_pressure_bypass_count",
                    "cache_pressure_bypass_bytes",
                )
            }
            with telemetry.phase("expression_value_dag", compute_heavy=True) as phase:
                signals = expression.evaluate_ordered_into(
                    (str(row["expression"]) for row in members),
                    value_namespaces=portfolio_batch["value_namespaces"],
                    mapping_namespaces=portfolio_batch["mapping_namespaces"],
                    release_keys_after_each=portfolio_batch[
                        "release_cache_keys_after_member"
                    ],
                )
                delta = {
                    key: int(expression.audit.get(key) or 0) - before[key]
                    for key in before
                }
                phase.add(
                    dag_nodes_evaluated=delta["value_node_evaluations"]
                    + delta["mapping_node_evaluations"],
                    dag_reuse_count=delta["cache_hits"],
                    native_kernel_calls=delta["native_kernel_calls"],
                    signals_materialized=len(members),
                    temporary_rows_allocated=block.row_count * len(members),
                    cache_peak_bytes=expression.audit.get("cache_peak_bytes", 0),
                    intra_batch_released_entries=delta[
                        "intra_batch_released_entries"
                    ],
                    intra_batch_released_bytes=delta["intra_batch_released_bytes"],
                    root_cache_bypass_count=delta["root_cache_bypass_count"],
                    root_cache_bypass_bytes=delta["root_cache_bypass_bytes"],
                    cache_pressure_evicted_entries=delta[
                        "cache_pressure_evicted_entries"
                    ],
                    cache_pressure_evicted_bytes=delta[
                        "cache_pressure_evicted_bytes"
                    ],
                    cache_pressure_bypass_count=delta[
                        "cache_pressure_bypass_count"
                    ],
                    cache_pressure_bypass_bytes=delta[
                        "cache_pressure_bypass_bytes"
                    ],
                    pair_batch_ordinal=batch_ordinal,
                    pair_ids=list(pair_batch_ids),
                    candidate_indices=list(candidate_indices),
                )
                expression_audits.append(
                    {
                        **delta,
                        "last_evaluate_wall_seconds": float(
                            expression.audit.get("last_evaluate_wall_seconds") or 0.0
                        ),
                        "last_evaluate_cpu_seconds": float(
                            expression.audit.get("last_evaluate_cpu_seconds") or 0.0
                        ),
                        "last_evaluate_effective_cores": float(
                            expression.audit.get("last_evaluate_effective_cores") or 0.0
                        ),
                        "cache_current_bytes": int(
                            expression.audit.get("cache_current_bytes") or 0
                        ),
                        "cache_peak_bytes": int(expression.audit.get("cache_peak_bytes") or 0),
                        "cache_entry_count": int(
                            expression.audit.get("cache_entry_count") or 0
                        ),
                        "pair_batch_ordinal": batch_ordinal,
                        "pair_ids": list(pair_batch_ids),
                        "candidate_indices": list(candidate_indices),
                    }
                )
            with telemetry.phase("pair_common_support", compute_heavy=False) as phase:
                common_masks = _support_masks(signals)
                support.update(
                    common_masks=common_masks,
                    pair_indices=pair_indices,
                    block_tokens=support_block_tokens,
                )
                phase.add(
                    pair_count=len(pair_batch_ids),
                    support_coordinates=int(common_masks.sum()),
                    pair_batch_ordinal=batch_ordinal,
                )
                del common_masks
            with telemetry.phase("label_free_behavior", compute_heavy=True) as phase:
                portfolio_batch["label_free_behavior"].update_block(
                    signals=signals,
                    time_ids=block.time_ids,
                    code_ids=block.code_ids,
                    trade_times_ns=block.trade_times_ns,
                    directions=portfolio_batch["directions"],
                )
                phase.add(
                    candidate_count=len(members),
                    pair_batch_ordinal=batch_ordinal,
                    portfolio_coordinates_processed=block.row_count * len(members),
                )
            result = portfolio_batch["kernel"].evaluate_block(
                signals=signals,
                labels=block.labels,
                time_ids=block.time_ids,
                code_ids=block.code_ids,
                day_ids=block.day_ids,
                directions=portfolio_batch["directions"],
                day_count=len(block.day_labels),
                prepared_block=prepared_portfolio_block,
            )
            del signals
            snapshot = _process_snapshot()
            for phase_name, prefix in (
                ("cross_sectional_rank_mapping", "mapping"),
                ("turnover_and_cost", "turnover_cost"),
            ):
                wall = float(result.audit[f"{prefix}_wall_seconds"])
                cpu = float(result.audit[f"{prefix}_cpu_seconds"])
                telemetry._append(
                    build_phase_event(
                        phase=phase_name,
                        wall_seconds=wall,
                        cpu_seconds=cpu,
                        allocated_compute_threads=plan.compute_threads,
                        compute_heavy=True,
                        rss_before_bytes=snapshot.rss_bytes,
                        rss_after_bytes=snapshot.rss_bytes,
                        peak_rss_bytes=snapshot.peak_rss_bytes,
                        blocks_processed=1,
                        pair_batch_ordinal=batch_ordinal,
                        pair_ids=list(pair_batch_ids),
                        portfolio_coordinates_processed=block.row_count * len(members),
                        coordinate_rows_retained=0,
                    )
                )
            portfolio_audits.append(
                {
                    **dict(result.audit),
                    "pair_batch_ordinal": batch_ordinal,
                    "pair_ids": list(pair_batch_ids),
                    "candidate_indices": list(candidate_indices),
                }
            )
            with telemetry.phase("streaming_reducer", compute_heavy=False) as phase:
                reducer.update(
                    result,
                    day_labels=block.day_labels,
                    candidate_indices=candidate_indices,
                    complete_block=batch_ordinal == len(portfolio_batches) - 1,
                )
                phase.add(
                    blocks_processed=1 if batch_ordinal == len(portfolio_batches) - 1 else 0,
                    pair_batch_ordinal=batch_ordinal,
                    coordinate_rows_retained=0,
                    reducer_bytes=reducer.stats.nbytes,
                )
            del result
            with telemetry.phase("expression_cache_release", compute_heavy=False) as phase:
                released = expression.release_cache_keys(
                    portfolio_batch["release_cache_keys"]
                )
                phase.add(
                    pair_batch_ordinal=batch_ordinal,
                    cache_released_entries=released["released_entries"],
                    cache_released_bytes=released["released_bytes"],
                    cache_current_bytes=int(expression.audit.get("cache_current_bytes") or 0),
                )
        completed_blocks.append(block_ordinal)
        with telemetry.phase("checkpoint", compute_heavy=False) as phase:
            rss_gate.periodic_checkpoint(
                block_ordinal=block_ordinal + 1,
                cadence=plan.checkpoint_every_blocks,
            )
            phase.add(blocks_processed=1, checkpoint_ordinal=checkpoint_ordinal)
        rss_gate.check(_process_snapshot(), global_rss_bytes=_process_snapshot().rss_bytes)
        if int(args.stop_after_blocks) and len(completed_blocks) >= int(args.stop_after_blocks):
            checkpoint("CONTROLLED_RESUME_PARITY_PAUSE")
            pause_payload = {
                "schema_version": "cn_phase3cm_streaming_pause_receipt_v1",
                "status": "CN_PHASE3CM_STREAMING_BACKEND_PAUSED_RECOVERABLE",
                "backend": args.backend,
                "phase": args.phase,
                "completed_blocks": list(completed_blocks),
                "rows_processed": int(total_rows),
                "evaluator_wall_seconds": prior_evaluator_wall_seconds
                + (time.perf_counter() - run_started),
                "checkpoint_ordinal": int(checkpoint_ordinal),
                "execution_plan_hash": plan.execution_plan_hash,
                "input_binding_hash": binding["binding_hash"],
                "validation_reads": (
                    int(total_rows) if args.evaluation_role == "validation" else 0
                ),
                "holdout_reads": (
                    int(total_rows) if args.evaluation_role == "holdout" else 0
                ),
                "forward_2026_reads": 0,
                "promotion": "FORBIDDEN",
                "strict_stage_a": "NOT_AUTHORIZED",
            }
            _write_json(output_root / "CN_STREAMING_PAUSE_RECEIPT.json", pause_payload)
            print(json.dumps(pause_payload, sort_keys=True))
            return 0

    with telemetry.phase("candidate_pair_finalization", compute_heavy=False) as phase:
        atom_rows = reducer.reward_atoms()
        reward_rows = []
        split_rows = []
        for candidate in candidates:
            candidate_atoms = [
                row for row in atom_rows if str(row.get("candidate_id")) == str(candidate["candidate_id"])
            ]
            per_split, reward = _candidate_summary_from_reward_atoms(
                dict(candidate),
                candidate_atoms,
                horizons,
                seed=finalization_seed_by_candidate_id[
                    str(candidate.get("candidate_id") or "")
                ],
                rank_ic_loss_weight=6.0,
                rank_ic_component_cap=0.35,
                regime_stability_weight=0.08,
                regime_component_cap=0.10,
            )
            split_rows.extend(per_split)
            reward_rows.append(reward)
        behavior_by_candidate: dict[str, dict[str, Any]] = {}
        for portfolio_batch in portfolio_batches:
            for behavior_row in portfolio_batch["label_free_behavior"].rows():
                candidate_id = str(behavior_row.get("candidate_id") or "")
                if candidate_id in behavior_by_candidate:
                    raise RuntimeError(f"duplicate full behavior identity row: {candidate_id}")
                behavior_by_candidate[candidate_id] = behavior_row
        pair_rows = _finalize_pairs(
            candidates=candidates,
            reward_rows=reward_rows,
            reducer=reducer,
            support=support,
            binding=binding,
            evaluation_role=args.evaluation_role,
            label_free_behavior_by_candidate=behavior_by_candidate,
        )
        if args.evaluation_role in REPORT_ONLY_EVALUATION_ROLES:
            for reward in reward_rows:
                reward[f"{args.evaluation_role}_report_metric"] = reward.get(
                    "optimizer_reward"
                )
                reward[f"{args.evaluation_role}_usage"] = "report_only"
                for key in tuple(reward):
                    if key.startswith("optimizer_") or key == "train_reward":
                        reward.pop(key, None)
            for row in split_rows:
                row["split"] = args.evaluation_role
                row["evaluation_role"] = args.evaluation_role
                row[f"{args.evaluation_role}_usage"] = "report_only"
            for atom in atom_rows:
                atom["split"] = args.evaluation_role
                atom["evaluation_role"] = args.evaluation_role
        candidate_by_id = {str(row["candidate_id"]): row for row in candidates}
        behavior_archive = PortfolioBehaviorArchive()
        behavior_by_pair: dict[str, dict[str, Any]] = {}
        for pair_row in pair_rows:
            primary_id = str(pair_row["primary_candidate_id"])
            control_id = str(pair_row["control_candidate_id"])
            primary_candidate = candidate_by_id[primary_id]
            record = pair_behavior_record(
                batch_id=str(args.iterative_batch_id or output_root.name),
                pair_id=str(pair_row["pair_id"]),
                route_id=str(primary_candidate.get("route_id") or ""),
                primary_candidate_id=primary_id,
                control_candidate_id=control_id,
                structural_family_id=str(
                    primary_candidate.get("structural_family_id")
                    or primary_candidate.get("family_id")
                    or ""
                ),
                signal_cluster_id=str(primary_candidate.get("signal_cluster_id") or ""),
                primary_behavior=behavior_by_candidate[primary_id],
                control_behavior=behavior_by_candidate[control_id],
            )
            behavior_archive.add(record)
            behavior_by_pair[str(pair_row["pair_id"])] = record
        for pair_row in pair_rows:
            authoritative = behavior_by_pair[str(pair_row["pair_id"])]
            pair_row.update(
                {
                    "route_id": authoritative["route_id"],
                    "structural_family_id": authoritative["structural_family_id"],
                    "signal_cluster_id": authoritative["signal_cluster_id"],
                    "portfolio_behavior_signature_id": authoritative[
                        "portfolio_behavior_signature_id"
                    ],
                    "portfolio_behavior_family_id": authoritative[
                        "portfolio_behavior_family_id"
                    ],
                    "behavior_status": authoritative["behavior_status"],
                    "v1_behavior_identity_authority": "LABEL_FREE_FULL_COORDINATE",
                    "legacy_primary_behavior_identity_role": "COMPATIBILITY_ONLY",
                    "legacy_control_behavior_identity_role": "COMPATIBILITY_ONLY",
                }
            )
        phase.add(candidate_count=len(candidates), pair_count=len(pair_rows))

    behavior_archive_path = output_root / (
        "CN_PORTFOLIO_BEHAVIOR_FULL.parquet"
        if args.evaluation_role == "train"
        else f"CN_PORTFOLIO_BEHAVIOR_{args.evaluation_role.upper()}_REPORT.parquet"
    )
    behavior_archive.write_parquet(behavior_archive_path)

    reward_atom_artifact = _write_csv(
        output_root / "CN_STREAMING_REWARD_ATOMS.csv",
        atom_rows,
    )

    events = [json.loads(line) for line in timing_path.read_text(encoding="utf-8").splitlines() if line]
    phase_totals: dict[str, dict[str, float]] = {}
    for event in events:
        item = phase_totals.setdefault(str(event["phase"]), {"wall_seconds": 0.0, "cpu_seconds": 0.0})
        item["wall_seconds"] += float(event.get("wall_seconds") or 0.0)
        item["cpu_seconds"] += float(event.get("cpu_seconds") or 0.0)
    compute_total = sum(
        phase_totals.get(name, {}).get("wall_seconds", 0.0)
        for name in ("expression_value_dag", "cross_sectional_rank_mapping", "turnover_and_cost")
    )
    rank_mapping = phase_totals.get("cross_sectional_rank_mapping", {}).get("wall_seconds", 0.0)
    value_dag = phase_totals.get("expression_value_dag", {}).get("wall_seconds", 0.0)
    if compute_total and rank_mapping / compute_total >= 0.60:
        bottleneck = "BATCHED_PORTFOLIO_KERNEL_BOTTLENECK"
    elif compute_total and value_dag / compute_total >= 0.60:
        bottleneck = "VALUE_DAG_BOTTLENECK"
    else:
        bottleneck = "MIXED_COMPUTE_BOTTLENECK"
    compute_phase_parallelism = aggregate_compute_phase_parallelism(events)
    parallelism_status = (
        "PARALLELISM_NOT_ENGAGED"
        if any(
            row.get("parallelism_status") == "PARALLELISM_NOT_ENGAGED"
            for row in compute_phase_parallelism.values()
        )
        else "PARALLELISM_ENGAGED"
    )
    evaluator_wall = prior_evaluator_wall_seconds + (time.perf_counter() - run_started)
    required_timing_phases = {
        "global_trade_time_barrier",
        "portfolio_label_order_prepare",
        "expression_value_dag",
        "pair_common_support",
        "pair_support_coordinate_prepare",
        "label_free_behavior",
        "cross_sectional_rank_mapping",
        "turnover_and_cost",
        "streaming_reducer",
        "checkpoint",
        "candidate_pair_finalization",
    }
    timing_coverage = len(required_timing_phases & set(phase_totals)) / len(required_timing_phases)
    phase_c_gates = {
        "wall_time_lte_3600": evaluator_wall <= 3600.0,
        "peak_rss_lte_plan_hard_limit": max(
            (int(event.get("peak_rss_bytes") or 0) for event in events), default=0
        ) <= plan.rss_hard_bytes,
        "coordinate_rows_retained_zero": reducer.coordinate_rows_retained == 0,
        "phase_timing_coverage_100pct": timing_coverage == 1.0,
        "sealed_reads_zero": True,
        "parallelism_engaged": parallelism_status == "PARALLELISM_ENGAGED",
    }
    result_payload = {
        "schema_version": "cn_phase3cm_streaming_backend_result_v1",
        "status": "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED",
        "backend": args.backend,
        "phase": args.phase,
        "pair_count": len(pair_ids),
        "candidate_count": len(candidates),
        "rows_processed": total_rows,
        "blocks_processed": len(completed_blocks),
        "wall_seconds": evaluator_wall,
        "telemetry_write_wall_seconds": telemetry.telemetry_write_wall_seconds,
        "telemetry_overhead_ratio_upper_bound": telemetry.telemetry_write_wall_seconds
        / max(1e-12, time.perf_counter() - run_started),
        "peak_rss_bytes": max((int(event.get("peak_rss_bytes") or 0) for event in events), default=0),
        "execution_plan_hash": plan.execution_plan_hash,
        "capacity_receipt_hash": str(args.capacity_receipt_hash) or None,
        "max_block_rows_contract": (
            int(args.max_block_rows) if int(args.max_block_rows) > 0 else None
        ),
        "max_observed_block_rows": int(max_observed_block_rows),
        "block_row_guard_status": (
            "BLOCK_ROW_GUARD_PASS"
            if int(args.max_block_rows) > 0
            else "NOT_ENFORCED_NON_PHASE_E"
        ),
        "input_binding_hash": binding["binding_hash"],
        "split_manifest_hash": binding["split_manifest_hash"],
        "split_boundary_purity_hash": split_boundary_purity_hash,
        "split_boundary_purity_status": (
            split_boundary_purity["status"] if split_boundary_purity else "NOT_BOUND"
        ),
        "label_purge_enforcement": (
            split_boundary_purity["enforcement_contract"]
            if split_boundary_purity
            else "LEGACY_BINDING_NOT_APPLICABLE"
        ),
        "evaluation_role": args.evaluation_role,
        "validation_usage": (
            "report_only" if args.evaluation_role == "validation" else "not_accessed"
        ),
        "holdout_usage": (
            "report_only" if args.evaluation_role == "holdout" else "not_accessed"
        ),
        "eligible_date_count": len(evaluation_dates),
        "eligible_train_date_count": (
            len(evaluation_dates) if args.evaluation_role == "train" else 0
        ),
        "evaluation_scope": (
            "development_session_sample"
            if session_sample is not None
            else "full_coordinate_development"
        ),
        "session_sample": session_sample,
        "portfolio_mode": str(args.portfolio_mode),
        "cost_bps": float(args.cost_bps),
        "horizons": list(horizons),
        "top_quantile": float(args.top_quantile),
        "dag_plan_hash": dag_plan.plan_hash,
        "coordinate_rows_retained": reducer.coordinate_rows_retained,
        "parallelism_status": parallelism_status,
        "compute_phase_parallelism": compute_phase_parallelism,
        "hot_path_bottleneck": bottleneck,
        "phase_timing_coverage": timing_coverage,
        "phase_c_gates": phase_c_gates,
        "phase_c_gate_status": (
            "SINGLE_PAIR_FULL_COORDINATE_GATE_PASS"
            if args.phase == "C" and all(phase_c_gates.values())
            else "NOT_APPLICABLE"
            if args.phase != "C"
            else "SINGLE_PAIR_FULL_COORDINATE_GATE_FAIL"
        ),
        "phase_totals": phase_totals,
        "support_identities": support.identities(),
        "candidate_rewards": reward_rows,
        "pair_results": pair_rows,
        (
            "portfolio_behavior_archive"
            if args.evaluation_role == "train"
            else "portfolio_behavior_report"
        ): {
            "path": str(behavior_archive_path),
            "sha256": _sha256(behavior_archive_path),
            "row_count": len(behavior_archive.rows),
            "identity_authority": "LABEL_FREE_FULL_COORDINATE",
            "state_role": (
                "TRAIN_ARCHIVE"
                if args.evaluation_role == "train"
                else f"{args.evaluation_role.upper()}_REPORT_ONLY_NOT_ARCHIVE_STATE"
            ),
        },
        "reward_atoms": reward_atom_artifact,
        "split_rows": split_rows,
        "expression_audits": expression_audits,
        "portfolio_audits": portfolio_audits,
        "validation_reads": (
            int(total_rows) if args.evaluation_role == "validation" else 0
        ),
        "holdout_reads": (
            int(total_rows) if args.evaluation_role == "holdout" else 0
        ),
        "forward_2026_reads": 0,
        "feedback_write": (
            "FORBIDDEN"
            if args.evaluation_role in REPORT_ONLY_EVALUATION_ROLES
            else "TRAIN_ONLY"
        ),
        "scheduler_write": (
            "FORBIDDEN"
            if args.evaluation_role in REPORT_ONLY_EVALUATION_ROLES
            else "CAMPAIGN_LOCAL"
        ),
        "archive_write": (
            "FORBIDDEN"
            if args.evaluation_role in REPORT_ONLY_EVALUATION_ROLES
            else "TRAIN_ONLY"
        ),
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
    }
    _write_json(output_root / "CN_STREAMING_BACKEND_RESULT.json", result_payload)
    _write_json(
        output_root / "CN_SHARED_DAG_EXECUTION_AUDIT.json",
        {
            "dag_plan_hash": dag_plan.plan_hash,
            "blocks": expression_audits,
            "actual_evaluation_count": sum(
                int(row.get("value_node_evaluations") or 0) + int(row.get("mapping_node_evaluations") or 0)
                for row in expression_audits
            ),
            "reuse_count": sum(int(row.get("cache_hits") or 0) for row in expression_audits),
        },
    )
    _write_json(output_root / "CN_STREAMING_REDUCER_CONTRACT.json", reducer.contract())
    print(
        json.dumps(
            {
                "status": result_payload["status"],
                "backend": args.backend,
                "pairs": len(pair_ids),
                "rows": total_rows,
                "wall_seconds": result_payload["wall_seconds"],
                "peak_rss_bytes": result_payload["peak_rss_bytes"],
                "parallelism_status": parallelism_status,
                "bottleneck": bottleneck,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
