"""Freeze the evidence-bound resource contract for the 1,024-pair wave.

This gate deliberately adjudicates resources, not generic scaling telemetry.
The historical 146 execution receipt retains its original fail-closed status;
only the exact readjudication manifest may supersede that status for semantic
parity while leaving the formal evaluator authority unchanged.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import os
import re
import uuid
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "cn_phase3cm_1024_resource_contract_v1"
PASS_STATUS = "CN_PHASE3CM_1024_RESOURCE_CONTRACT_PASS"
FAIL_STATUS = "CN_PHASE3CM_1024_RESOURCE_CONTRACT_FAIL_CLOSED"

TOTAL_PAIR_COUNT = 1024
ACTIVE_PAIR_COUNT = 584
SESSION_PAIR_COUNT = 440
REPLAY_PAIR_COUNT = 146
HISTORICAL_SESSION_PAIR_COUNT = 110

ACTIVE_PROJECTION_MULTIPLIER = 1.25
ACTIVE_REPLAY_MULTIPLE = ACTIVE_PAIR_COUNT / REPLAY_PAIR_COUNT
HOST_HOURS_HARD_MAX = 12.0
HOST_HOUR_ROUNDING_QUANTUM = 0.25
WALL_SECONDS_HARD_MAX = 12 * 3600

HEAVY_PROCESSES = 2
ACTIVE_NATIVE_THREADS = 11
SESSION_NATIVE_THREADS = 2
GLOBAL_NATIVE_THREADS = ACTIVE_NATIVE_THREADS + SESSION_NATIVE_THREADS
GLOBAL_NATIVE_THREADS_HARD_MAX = 24

GIB = 1024**3
WORKER_RSS_SOFT_BYTES = 20 * GIB
WORKER_RSS_HARD_BYTES = 24 * GIB
GLOBAL_RSS_PROJECTED_BYTES = 48 * GIB
GLOBAL_RSS_HARD_BYTES = 60 * GIB
OUTPUT_BYTES_HARD = 64 * GIB

CHECKPOINT_CATEGORIES = ("temporal", "state", "support", "portfolio", "reducer")
ACCESS_FIELDS = ("validation_reads", "holdout_reads", "forward_2026_reads")
SHA256_RE = re.compile(r"[0-9a-f]{64}")
GIT_SHA_RE = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")


class EvidenceError(ValueError):
    pass


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


def _hash_without(payload: Mapping[str, Any], field: str) -> str:
    body = copy.deepcopy(dict(payload))
    body.pop(field, None)
    return _stable_hash(body)


def contract_hash(payload: Mapping[str, Any]) -> str:
    return _hash_without(payload, "contract_hash")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_json(path: Path, label: str) -> dict[str, Any]:
    resolved = Path(path).resolve()
    value = json.loads(resolved.read_text(encoding="utf-8-sig"))
    if not isinstance(value, dict):
        raise EvidenceError(f"{label} must be a JSON object")
    return value


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    destination = Path(path).resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, destination)


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise EvidenceError(message)


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise EvidenceError(f"{label} must be an object")
    return value


def _list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise EvidenceError(f"{label} must be an array")
    return value


def _integer(value: Any, label: str) -> int:
    if type(value) is not int:
        raise EvidenceError(f"{label} must be an integer")
    return value


def _number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EvidenceError(f"{label} must be a finite number")
    result = float(value)
    if not math.isfinite(result):
        raise EvidenceError(f"{label} must be a finite number")
    return result


def _sha_text(value: Any, label: str) -> str:
    text = str(value or "")
    if SHA256_RE.fullmatch(text) is None:
        raise EvidenceError(f"{label} is not an exact SHA-256")
    return text


def _git_sha(value: Any, label: str) -> str:
    text = str(value or "")
    if GIT_SHA_RE.fullmatch(text) is None:
        raise EvidenceError(f"{label} is not an exact Git SHA")
    return text


def _verify_self_hash(
    payload: Mapping[str, Any],
    *,
    field: str,
    label: str,
    required: bool,
) -> None:
    if field not in payload:
        if required:
            raise EvidenceError(f"{label} lacks required {field}")
        return
    claimed = _sha_text(payload[field], f"{label}.{field}")
    if _hash_without(payload, field) != claimed:
        raise EvidenceError(f"{label} self-hash drift in {field}")


def _verify_optional_self_hashes(payload: Mapping[str, Any], label: str) -> None:
    for field in ("freeze_hash", "receipt_hash", "manifest_hash", "contract_hash"):
        if field in payload:
            _verify_self_hash(payload, field=field, label=label, required=False)


def _access(
    payload: Mapping[str, Any],
    label: str,
    *,
    nested: bool,
    require_role: bool,
) -> Mapping[str, Any]:
    evidence = _mapping(payload.get("boundaries"), f"{label}.boundaries") if nested else payload
    for field in ACCESS_FIELDS:
        if type(evidence.get(field)) is not int or evidence.get(field) != 0:
            raise EvidenceError(f"{label} does not prove {field}=0")
    if require_role and evidence.get("data_role") != "development_train_only":
        raise EvidenceError(f"{label} is not development_train_only")
    if evidence.get("promotion") != "FORBIDDEN":
        raise EvidenceError(f"{label} does not prove promotion=FORBIDDEN")
    if evidence.get("strict_stage_a") != "NOT_AUTHORIZED":
        raise EvidenceError(f"{label} does not prove strict_stage_a=NOT_AUTHORIZED")
    return evidence


def _record(
    path: Path,
    payload: Mapping[str, Any],
    *,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "path": str(Path(path).resolve()),
        "sha256": _sha256(path),
        "schema_version": str(payload.get("schema_version") or ""),
        "status": str(payload.get("status") or ""),
    }
    if extra:
        record.update(dict(extra))
    return record


def _freeze_pack(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = _read_json(path, "1024 freeze")
    _require(bool(str(payload.get("schema_version") or "")), "1024 freeze schema_version is missing")
    _require(
        payload.get("status") == "CN_PHASE3CM_1024_PACK_FREEZE_AND_SUBSET_PASS",
        "1024 freeze status is not PASS",
    )
    _require(_integer(payload.get("pair_count"), "1024 freeze.pair_count") == TOTAL_PAIR_COUNT, "1024 freeze pair count is not 1024")
    clocks = _mapping(payload.get("clock_counts"), "1024 freeze.clock_counts")
    _require(
        dict(clocks) == {"active_bar": ACTIVE_PAIR_COUNT, "stock_session": SESSION_PAIR_COUNT},
        "1024 freeze clock counts are not active_bar=584/stock_session=440",
    )
    if "boundaries" in payload:
        _access(payload, "1024 freeze", nested=True, require_role=True)
    _verify_optional_self_hashes(payload, "1024 freeze")
    return payload, _record(path, payload)


def _historical_subset(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = _read_json(path, "historical subset")
    _require(
        payload.get("schema_version")
        == "cn_phase3cm_historical_256_semantic_subset_receipt_v2",
        "historical subset schema drift",
    )
    _require(
        payload.get("status") == "CN_PHASE3CM_HISTORICAL_256_SEMANTIC_SUBSET_PASS",
        "historical subset status is not PASS",
    )
    expected_counts = {
        "historical_pair_count": 256,
        "freeze_pair_count": TOTAL_PAIR_COUNT,
        "historical_candidate_count": 512,
        "freeze_candidate_count": 2048,
    }
    for field, expected in expected_counts.items():
        _require(_integer(payload.get(field), f"historical subset.{field}") == expected, f"historical subset {field} drift")
    for field in (
        "pair_identity_subset_exact",
        "candidate_identity_subset_exact",
        "pair_semantics_exact",
        "candidate_semantics_exact",
    ):
        _require(payload.get(field) is True, f"historical subset does not prove {field}=true")
    for field in (
        "missing_pair_ids",
        "missing_candidate_ids",
        "pair_semantic_mismatch_ids",
        "candidate_semantic_mismatch_ids",
    ):
        _require(_list(payload.get(field), f"historical subset.{field}") == [], f"historical subset {field} is not empty")
    _verify_optional_self_hashes(payload, "historical subset")
    return payload, _record(path, payload)


def _readjudicated_parity(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = _read_json(path, "readjudicated parity")
    _require(
        payload.get("schema_version") == "cn_phase3cm_current_kernel_146_parity_final_v1",
        "readjudicated parity schema drift",
    )
    _require(
        payload.get("status") == "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_PASS",
        "readjudicated parity status is not PASS",
    )
    _require(payload.get("kernel_state") == "PARTIALLY_QUALIFIED", "readjudicated parity kernel state drift")
    _require(payload.get("backend_authority") == "EXPERIMENTAL_BACKEND", "readjudicated parity backend authority drift")
    _require(payload.get("formal_evaluator_authority") == "UNCHANGED", "readjudicated parity changed the formal evaluator")
    identity = _mapping(payload.get("identity_coverage"), "readjudicated parity.identity_coverage")
    expected_counts = {
        "expected_pair_count": REPLAY_PAIR_COUNT,
        "observed_pair_count": REPLAY_PAIR_COUNT,
        "expected_candidate_count": 2 * REPLAY_PAIR_COUNT,
        "observed_candidate_count": 2 * REPLAY_PAIR_COUNT,
    }
    for field, expected in expected_counts.items():
        _require(_integer(identity.get(field), f"readjudicated parity.{field}") == expected, f"readjudicated parity {field} drift")
    _sha_text(identity.get("pair_union_digest"), "readjudicated parity pair union digest")
    _sha_text(identity.get("candidate_union_digest"), "readjudicated parity candidate union digest")
    _require(payload.get("identity_coverage_exact") is True, "readjudicated parity identity coverage is not exact")

    partitions = _list(payload.get("partitions"), "readjudicated parity.partitions")
    _require(len(partitions) == 2, "readjudicated parity must contain two partitions")
    observed_ids: set[str] = set()
    for ordinal, raw in enumerate(partitions):
        row = _mapping(raw, f"readjudicated parity.partitions[{ordinal}]")
        partition_id = str(row.get("partition_id") or "")
        observed_ids.add(partition_id)
        _require(row.get("status") == "EXACT_PARITY_PASS", f"{partition_id} is not exact parity PASS")
        _require(_integer(row.get("pair_count"), f"{partition_id}.pair_count") == 73, f"{partition_id} pair count drift")
        _require(_integer(row.get("candidate_count"), f"{partition_id}.candidate_count") == 146, f"{partition_id} candidate count drift")
        qualification = _mapping(row.get("qualification"), f"{partition_id}.qualification")
        _require(qualification.get("comparable") is True, f"{partition_id} is not comparable")
        _require(qualification.get("semantic_parity_exact") is True, f"{partition_id} semantic parity is not exact")
        checkpoint = _mapping(row.get("checkpoint"), f"{partition_id}.checkpoint")
        continuation = _mapping(checkpoint.get("continuation_parity"), f"{partition_id}.continuation_parity")
        _require(
            set(continuation) == set(CHECKPOINT_CATEGORIES)
            and all(continuation.get(key) is True for key in CHECKPOINT_CATEGORIES),
            f"{partition_id} checkpoint continuation parity drift",
        )
        contract_parity = _mapping(checkpoint.get("contract_parity"), f"{partition_id}.contract_parity")
        _require(bool(contract_parity) and all(value is True for value in contract_parity.values()), f"{partition_id} checkpoint contract parity drift")
    _require(observed_ids == {"active_bar_p0", "active_bar_p1"}, "readjudicated parity partition IDs drift")
    _access(payload, "readjudicated parity", nested=True, require_role=True)
    _require(payload.get("performance_threshold_gate") == "NOT_USED", "readjudicated parity reintroduced a performance threshold")
    _require(payload.get("two_x_speedup_required") is False, "readjudicated parity reintroduced the 2x gate")
    _require(_list(payload.get("errors"), "readjudicated parity.errors") == [], "readjudicated parity contains errors")
    _require(
        payload.get("next_decision") == "FREEZE_1024_RESOURCE_AND_EXECUTION_CONTRACT",
        "readjudicated parity next decision drift",
    )
    _verify_optional_self_hashes(payload, "readjudicated parity")
    return payload, _record(path, payload)


def _readjudication_manifest(
    path: Path,
    *,
    parity_path: Path,
    execution_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = _read_json(path, "146 readjudication manifest")
    _require(
        payload.get("schema_version")
        == "cn_phase3cm_current_kernel_146_readjudication_manifest_v1",
        "146 readjudication manifest schema drift",
    )
    _require(
        payload.get("status")
        == "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_READJUDICATED_PASS",
        "146 readjudication manifest status is not PASS",
    )
    _require(
        payload.get("reason")
        == "CHECKPOINT_EXACTNESS_WAS_CONFLATED_WITH_SCALING_PARALLELISM_DIAGNOSTIC",
        "146 readjudication manifest reason drift",
    )
    repo_sha = _git_sha(payload.get("repo_sha"), "146 readjudication manifest.repo_sha")
    bindings = (
        ("readjudicated_parity_receipt_sha256", parity_path, "readjudicated parity receipt"),
        ("original_execution_receipt_sha256", execution_path, "original execution receipt"),
    )
    for field, bound_path, description in bindings:
        claimed = _sha_text(payload.get(field), f"146 readjudication manifest.{field}")
        _require(
            claimed == _sha256(bound_path),
            f"146 readjudication manifest {description} SHA-256 binding drift",
        )
    # This hash covers the historical readjudication source archive.  The
    # 1,024 launcher has a separate current-repo source closure, validated
    # independently below and exposed as the resource contract repo_sha.
    _sha_text(
        payload.get("source_closure_manifest_sha256"),
        "146 readjudication manifest.source_closure_manifest_sha256",
    )
    _require(payload.get("identity_coverage_exact") is True, "146 readjudication manifest identity coverage is not exact")
    categories = _list(payload.get("checkpoint_categories"), "146 readjudication manifest.checkpoint_categories")
    _require(categories == list(CHECKPOINT_CATEGORIES), "146 readjudication manifest checkpoint categories drift")
    _require(
        isinstance(payload.get("scaling_status_observed_not_gated"), list),
        "146 readjudication manifest does not preserve non-gating scaling telemetry",
    )
    _access(payload, "146 readjudication manifest", nested=True, require_role=True)
    _require(
        payload.get("next_decision") == "FREEZE_1024_RESOURCE_AND_EXECUTION_CONTRACT",
        "146 readjudication manifest next decision drift",
    )
    _verify_optional_self_hashes(payload, "146 readjudication manifest")
    return payload, _record(path, payload, extra={"repo_sha": repo_sha})


def _execution_receipt(path: Path) -> tuple[dict[str, Any], dict[str, Any], float]:
    payload = _read_json(path, "146 execution receipt")
    _require(
        payload.get("schema_version")
        == "cn_phase3cm_current_kernel_146_replay_execution_receipt_v1",
        "146 execution receipt schema drift",
    )
    _require(
        payload.get("status") == "CN_PHASE3CM_CURRENT_KERNEL_146_REPLAY_FAIL_CLOSED",
        "146 execution receipt did not preserve its historical FAIL_CLOSED status",
    )
    _require(
        payload.get("authority_scope") == "PARITY_REPLAY_ONLY_NOT_FORMAL_EVALUATOR",
        "146 execution receipt authority scope drift",
    )
    expected_repo = _git_sha(payload.get("expected_repo_sha"), "146 execution expected_repo_sha")
    observed_repo = _git_sha(payload.get("observed_repo_sha"), "146 execution observed_repo_sha")
    _require(expected_repo == observed_repo, "146 execution repo SHA drift")

    wall_seconds = _number(payload.get("wall_seconds"), "146 execution.wall_seconds")
    wall_hard = _number(payload.get("wall_seconds_hard"), "146 execution.wall_seconds_hard")
    _require(wall_seconds > 0.0 and wall_hard > 0.0 and wall_seconds < wall_hard, "146 execution exceeded its wall hard gate")
    _require(wall_seconds < WALL_SECONDS_HARD_MAX, "146 execution exceeded the frozen 12-hour wall cap")

    global_peak = _integer(payload.get("global_peak_rss_bytes"), "146 execution.global_peak_rss_bytes")
    recorded_global_hard = _integer(payload.get("global_rss_hard_bytes"), "146 execution.global_rss_hard_bytes")
    _require(recorded_global_hard == GLOBAL_RSS_HARD_BYTES, "146 execution global RSS hard cap drift")
    _require(0 <= global_peak < recorded_global_hard, "146 execution exceeded the global RSS hard cap")
    output_hard = _integer(payload.get("output_bytes_hard"), "146 execution.output_bytes_hard")
    _require(output_hard == OUTPUT_BYTES_HARD, "146 execution output hard cap drift")
    for field in ("output_peak_bytes", "output_final_bytes"):
        observed = _integer(payload.get(field), f"146 execution.{field}")
        _require(0 <= observed < output_hard, f"146 execution {field} exceeded the output hard cap")
    _require(payload.get("gate_failure") in {None, ""}, "146 execution recorded a resource gate failure")
    _require(payload.get("launch_failure") in {None, ""}, "146 execution recorded a launch failure")

    partitions = _list(payload.get("partitions"), "146 execution.partitions")
    _require(len(partitions) == HEAVY_PROCESSES, "146 execution did not use exactly two heavy processes")
    partition_ids: set[str] = set()
    for ordinal, raw in enumerate(partitions):
        row = _mapping(raw, f"146 execution.partitions[{ordinal}]")
        partition_id = str(row.get("partition_id") or "")
        partition_ids.add(partition_id)
        _require(row.get("execution_pass") is True, f"{partition_id} execution did not pass")
        _require(row.get("qualification_exact_pass") is True, f"{partition_id} qualification was not exact")
        _require(_integer(row.get("process_exit_code"), f"{partition_id}.process_exit_code") == 0, f"{partition_id} process failed")
        peak = _integer(row.get("peak_process_tree_rss_bytes"), f"{partition_id}.peak_process_tree_rss_bytes")
        _require(0 <= peak < WORKER_RSS_HARD_BYTES, f"{partition_id} exceeded the worker RSS hard cap")
        # checkpoint_exact_pass is intentionally ignored here.  Its historical
        # false value came from a generic scaling diagnostic and is superseded
        # only by the exact readjudication manifest/checkpoint categories.
    _require(partition_ids == {"active_bar_p0", "active_bar_p1"}, "146 execution partition IDs drift")
    _require(payload.get("final_parity_pass") is False, "146 execution no longer preserves the superseded original parity status")
    _access(payload, "146 execution receipt", nested=False, require_role=False)
    _require(payload.get("speedup_threshold_gate") == "NOT_USED", "146 execution reintroduced a speedup threshold")

    source = _mapping(payload.get("source_closure"), "146 execution.source_closure")
    _require(
        source.get("mode") in {"EXPLICIT_SOURCE_CLOSURE_MANIFEST_VERIFIED", "CLEAN_GIT"},
        "146 execution source closure mode drift",
    )
    _git_sha(source.get("repo_sha"), "146 execution source closure repo_sha")
    if source.get("mode") == "EXPLICIT_SOURCE_CLOSURE_MANIFEST_VERIFIED":
        for field in ("manifest_sha256", "manifest_hash", "source_closure_hash"):
            _sha_text(source.get(field), f"146 execution source closure.{field}")
        _require(_integer(source.get("source_count"), "146 execution source_count") > 0, "146 execution source closure is empty")

    record = _record(
        path,
        payload,
        extra={
            "superseded_status": str(payload["status"]),
            "wall_seconds": wall_seconds,
            "global_peak_rss_bytes": global_peak,
            "supersession_basis": "EXACT_READJUDICATION_MANIFEST_SHA_BINDING",
        },
    )
    return payload, record, wall_seconds


def _historical_session(
    execution_path: Path,
    result_path: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], float]:
    result = _read_json(result_path, "historical stock-session result")
    _require(result.get("schema_version") == "cn_phase3cm_streaming_backend_result_v1", "historical stock-session result schema drift")
    _require(result.get("status") == "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED", "historical stock-session result is not complete")
    _require(result.get("backend") == "stock_session", "historical result is not stock_session")
    _require(result.get("phase") == "E", "historical stock-session result is not Phase E")
    _require(_integer(result.get("pair_count"), "historical result.pair_count") == HISTORICAL_SESSION_PAIR_COUNT, "historical stock-session pair count drift")
    _require(_integer(result.get("candidate_count"), "historical result.candidate_count") == 2 * HISTORICAL_SESSION_PAIR_COUNT, "historical stock-session candidate count drift")
    wall_seconds = _number(result.get("wall_seconds"), "historical stock-session result.wall_seconds")
    _require(wall_seconds > 0.0, "historical stock-session wall time is not positive")
    _access(result, "historical stock-session result", nested=False, require_role=False)
    if "peak_rss_bytes" in result:
        peak = _integer(result.get("peak_rss_bytes"), "historical stock-session result.peak_rss_bytes")
        _require(0 <= peak < WORKER_RSS_HARD_BYTES, "historical stock-session result exceeded the worker RSS hard cap")

    execution = _read_json(execution_path, "historical stock-session execution receipt")
    _require(
        execution.get("schema_version") == "cn_phase3cm_phase_e_combined_execution_receipt_v1",
        "historical stock-session execution receipt schema drift",
    )
    _require(
        execution.get("status") == "CN_PHASE3CM_PHASE_E_STRICT_WAVE_FAIL",
        "historical stock-session execution receipt did not preserve its immutable FAIL status",
    )
    _git_sha(execution.get("combined_contract_repo_sha"), "historical stock-session execution repo SHA")
    _require(_integer(execution.get("active_exit_code"), "historical execution.active_exit_code") != 0, "historical execution no longer records the active failure")
    _require(_integer(execution.get("session_exit_code"), "historical execution.session_exit_code") == 0, "historical stock-session process did not exit cleanly")
    expected_counts = {
        "active_pair_count": REPLAY_PAIR_COUNT,
        "session_pair_count": HISTORICAL_SESSION_PAIR_COUNT,
        "total_pair_count": 256,
    }
    for field, expected in expected_counts.items():
        _require(_integer(execution.get(field), f"historical execution.{field}") == expected, f"historical execution {field} drift")
    _require(_integer(execution.get("heavy_processes"), "historical execution.heavy_processes") == HEAVY_PROCESSES, "historical execution heavy process count drift")
    threads = _mapping(execution.get("compute_threads_by_backend"), "historical execution.compute_threads_by_backend")
    _require(dict(threads) == {"active_bar": ACTIVE_NATIVE_THREADS, "stock_session": SESSION_NATIVE_THREADS}, "historical execution route-asymmetric thread shape drift")
    _require(_integer(execution.get("global_active_native_compute_threads"), "historical execution.global_active_native_compute_threads") == GLOBAL_NATIVE_THREADS, "historical execution global native thread count drift")
    _require(GLOBAL_NATIVE_THREADS <= GLOBAL_NATIVE_THREADS_HARD_MAX, "historical execution global native thread hard cap exceeded")
    _access(execution, "historical stock-session execution receipt", nested=False, require_role=False)

    claimed_result = Path(str(execution.get("session_result") or "")).resolve()
    _require(claimed_result == Path(result_path).resolve(), "historical execution points to a different stock-session result")
    if "session_result_sha256" in execution:
        _require(
            _sha_text(execution.get("session_result_sha256"), "historical execution.session_result_sha256")
            == _sha256(result_path),
            "historical execution stock-session result SHA-256 drift",
        )
    if "global_peak_rss_bytes" in execution and "global_hard_rss_bytes" in execution:
        peak = _integer(execution.get("global_peak_rss_bytes"), "historical execution.global_peak_rss_bytes")
        hard = _integer(execution.get("global_hard_rss_bytes"), "historical execution.global_hard_rss_bytes")
        _require(hard <= GLOBAL_RSS_HARD_BYTES and 0 <= peak < hard, "historical execution exceeded its global RSS hard cap")
    if "session_peak_process_tree_rss_bytes" in execution:
        session_peak = _integer(execution.get("session_peak_process_tree_rss_bytes"), "historical execution.session_peak_process_tree_rss_bytes")
        _require(0 <= session_peak < WORKER_RSS_HARD_BYTES, "historical execution stock-session worker exceeded its RSS hard cap")
    _verify_optional_self_hashes(execution, "historical stock-session execution receipt")

    seconds_per_pair = wall_seconds / HISTORICAL_SESSION_PAIR_COUNT
    return (
        execution,
        _record(execution_path, execution),
        result,
        _record(
            result_path,
            result,
            extra={
                "measured_wall_seconds": wall_seconds,
                "conservative_measured_seconds_per_pair": seconds_per_pair,
            },
        ),
        seconds_per_pair,
    )


def _sidecar_closure(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = _read_json(path, "1024 sidecar closure")
    _require(payload.get("schema_version") == "cn_phase3cm_1024_sidecar_closure_v1", "1024 sidecar closure schema drift")
    _require(payload.get("status") == "CN_PHASE3CM_1024_SIDECAR_CLOSURE_PASS", "1024 sidecar closure status is not PASS")
    _verify_self_hash(payload, field="closure_hash", label="1024 sidecar closure", required=True)
    pair_counts = _mapping(payload.get("pair_counts"), "1024 sidecar closure.pair_counts")
    _require(
        dict(pair_counts)
        == {"active_bar": ACTIVE_PAIR_COUNT, "stock_session": SESSION_PAIR_COUNT, "total": TOTAL_PAIR_COUNT},
        "1024 sidecar closure pair counts drift",
    )
    coverage = _mapping(payload.get("coverage_audit"), "1024 sidecar closure.coverage_audit")
    _require(coverage.get("all_required_fields_present") is True, "1024 sidecar closure coverage is incomplete")
    _access(payload, "1024 sidecar closure", nested=False, require_role=True)
    sealed = _mapping(payload.get("sealed_reads"), "1024 sidecar closure.sealed_reads")
    for field in ACCESS_FIELDS:
        _require(type(sealed.get(field)) is int and sealed.get(field) == 0, f"1024 sidecar closure sealed_reads does not prove {field}=0")
    _require(payload.get("formal_evaluator_authority") == "UNCHANGED", "1024 sidecar closure changed the formal evaluator")
    _require(payload.get("streaming_backend_authority") == "EXPERIMENTAL_BACKEND", "1024 sidecar closure backend authority drift")
    return payload, _record(path, payload, extra={"closure_hash": str(payload["closure_hash"])})


def _source_closure(
    path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = _read_json(path, "source closure")
    _require(payload.get("schema_version") == "cn_phase3cm_source_closure_manifest_v1", "source closure schema drift")
    _require(payload.get("status") == "CN_PHASE3CM_SOURCE_CLOSURE_MANIFEST_READY", "source closure status is not ready")
    repo_sha = _git_sha(payload.get("repo_sha"), "source closure.repo_sha")
    _verify_self_hash(payload, field="manifest_hash", label="source closure", required=True)
    sources = _list(payload.get("sources"), "source closure.sources")
    source_paths = _list(payload.get("source_paths"), "source closure.source_paths")
    _require(bool(sources), "source closure is empty")
    _require(len(source_paths) == len(set(map(str, source_paths))), "source closure source_paths contains duplicates")
    observed_paths: list[str] = []
    expected_keys = {"path", "sha256", "git_blob_sha", "git_mode", "normalization"}
    for ordinal, raw in enumerate(sources):
        row = _mapping(raw, f"source closure.sources[{ordinal}]")
        _require(set(row) == expected_keys, f"source closure.sources[{ordinal}] schema drift")
        source_path = str(row.get("path") or "")
        _require(bool(source_path), f"source closure.sources[{ordinal}] path is missing")
        observed_paths.append(source_path)
        _sha_text(row.get("sha256"), f"source closure.sources[{ordinal}].sha256")
        _git_sha(row.get("git_blob_sha"), f"source closure.sources[{ordinal}].git_blob_sha")
        _require(str(row.get("git_mode")) in {"100644", "100755"}, f"source closure.sources[{ordinal}] Git mode drift")
        _require(row.get("normalization") == "TEXT_CRLF_TO_LF", f"source closure.sources[{ordinal}] normalization drift")
    _require(observed_paths == list(map(str, source_paths)), "source closure source path/order drift")
    _require(len(observed_paths) == len(set(observed_paths)), "source closure contains duplicate source records")
    claimed_closure_hash = _sha_text(payload.get("source_closure_hash"), "source closure.source_closure_hash")
    _require(_stable_hash(sources) == claimed_closure_hash, "source closure source hash drift")
    return payload, _record(
        path,
        payload,
        extra={
            "repo_sha": repo_sha,
            "manifest_hash": str(payload["manifest_hash"]),
            "source_closure_hash": claimed_closure_hash,
            "source_count": len(sources),
        },
    )


def freeze_resource_contract(
    *,
    freeze_path: Path,
    historical_subset_path: Path,
    readjudicated_parity_path: Path,
    readjudication_manifest_path: Path,
    execution_receipt_path: Path,
    historical_session_execution_path: Path,
    historical_session_result_path: Path,
    sidecar_closure_path: Path,
    source_closure_path: Path,
    output_path: Path,
    host_hours_hard_max: float = HOST_HOURS_HARD_MAX,
    active_native_threads: int = ACTIVE_NATIVE_THREADS,
    session_native_threads: int = SESSION_NATIVE_THREADS,
) -> dict[str, Any]:
    evidence: dict[str, Any] = {}
    errors: list[str] = []
    active_wall_seconds: float | None = None
    session_seconds_per_pair: float | None = None
    active_projected_seconds: float | None = None
    session_projected_seconds: float | None = None
    host_hours_projected_raw: float | None = None
    host_hours_projected: float | None = None
    repo_sha: str | None = None

    try:
        _require(
            float(host_hours_hard_max) == HOST_HOURS_HARD_MAX,
            "host-hour hard cap must remain frozen at 12.0",
        )
        _require(
            type(active_native_threads) is int
            and type(session_native_threads) is int
            and active_native_threads == ACTIVE_NATIVE_THREADS
            and session_native_threads == SESSION_NATIVE_THREADS,
            "route-asymmetric thread shape must remain active_bar=11/stock_session=2",
        )
        _require(
            active_native_threads + session_native_threads == GLOBAL_NATIVE_THREADS
            and GLOBAL_NATIVE_THREADS <= GLOBAL_NATIVE_THREADS_HARD_MAX,
            "global native thread hard cap exceeded",
        )

        _, evidence["freeze"] = _freeze_pack(freeze_path)
        _, evidence["historical_subset"] = _historical_subset(historical_subset_path)
        _, evidence["readjudicated_parity"] = _readjudicated_parity(readjudicated_parity_path)
        _, evidence["readjudication_manifest"] = _readjudication_manifest(
            readjudication_manifest_path,
            parity_path=readjudicated_parity_path,
            execution_path=execution_receipt_path,
        )
        _, evidence["execution_receipt"], active_wall_seconds = _execution_receipt(
            execution_receipt_path
        )
        (
            _,
            evidence["historical_session_execution"],
            _,
            evidence["historical_session_result"],
            session_seconds_per_pair,
        ) = _historical_session(
            historical_session_execution_path,
            historical_session_result_path,
        )
        _, evidence["sidecar_closure"] = _sidecar_closure(sidecar_closure_path)
        source_closure, evidence["source_closure"] = _source_closure(
            source_closure_path
        )
        repo_sha = str(source_closure["repo_sha"])

        active_projected_seconds = (
            active_wall_seconds
            * ACTIVE_REPLAY_MULTIPLE
            * ACTIVE_PROJECTION_MULTIPLIER
        )
        session_projected_seconds = session_seconds_per_pair * SESSION_PAIR_COUNT
        host_hours_projected_raw = (
            active_projected_seconds + session_projected_seconds
        ) / 3600.0
        host_hours_projected = (
            math.ceil(host_hours_projected_raw / HOST_HOUR_ROUNDING_QUANTUM)
            * HOST_HOUR_ROUNDING_QUANTUM
        )

        _require(
            host_hours_projected <= HOST_HOURS_HARD_MAX,
            "projected host-hour total exceeds the 12.0 hard cap",
        )
        _require(
            active_projected_seconds < WALL_SECONDS_HARD_MAX,
            "active-bar projection exceeds the 12-hour launcher wall hard cap",
        )
        _require(
            session_projected_seconds < WALL_SECONDS_HARD_MAX,
            "stock-session projection exceeds the 12-hour launcher wall hard cap",
        )
        _require(WORKER_RSS_SOFT_BYTES < WORKER_RSS_HARD_BYTES, "worker RSS soft/hard limits are invalid")
        _require(GLOBAL_RSS_PROJECTED_BYTES < GLOBAL_RSS_HARD_BYTES, "global RSS projection exceeds the hard cap")
    except (OSError, json.JSONDecodeError, EvidenceError, TypeError, ValueError) as exc:
        errors.append(str(exc))

    passed = not errors
    contract: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "status": PASS_STATUS if passed else FAIL_STATUS,
        "repo_sha": repo_sha,
        "total_pair_count": TOTAL_PAIR_COUNT,
        "backend_pair_counts": {
            "active_bar": ACTIVE_PAIR_COUNT,
            "stock_session": SESSION_PAIR_COUNT,
        },
        "heavy_processes": HEAVY_PROCESSES,
        "native_compute_threads_by_backend": {
            "active_bar": active_native_threads,
            "stock_session": session_native_threads,
        },
        "global_active_native_compute_threads": active_native_threads
        + session_native_threads,
        "global_native_compute_threads_hard_max": GLOBAL_NATIVE_THREADS_HARD_MAX,
        "worker_rss_projected_bytes": WORKER_RSS_SOFT_BYTES,
        "worker_rss_soft_bytes": WORKER_RSS_SOFT_BYTES,
        "worker_rss_hard_bytes": WORKER_RSS_HARD_BYTES,
        "rss_projected_bytes_by_backend": {
            "active_bar": WORKER_RSS_SOFT_BYTES,
            "stock_session": WORKER_RSS_SOFT_BYTES,
        },
        "rss_soft_bytes_by_backend": {
            "active_bar": WORKER_RSS_SOFT_BYTES,
            "stock_session": WORKER_RSS_SOFT_BYTES,
        },
        "rss_hard_bytes_by_backend": {
            "active_bar": WORKER_RSS_HARD_BYTES,
            "stock_session": WORKER_RSS_HARD_BYTES,
        },
        "global_rss_projected_bytes": GLOBAL_RSS_PROJECTED_BYTES,
        "global_rss_hard_bytes": GLOBAL_RSS_HARD_BYTES,
        "output_bytes_hard": OUTPUT_BYTES_HARD,
        "wall_seconds_hard_max": WALL_SECONDS_HARD_MAX,
        "host_hours_projected_raw": host_hours_projected_raw,
        "host_hours_projected": host_hours_projected,
        "host_hours_hard_max": HOST_HOURS_HARD_MAX,
        "resource_projection": {
            "active_bar": {
                "evidence_pair_count": REPLAY_PAIR_COUNT,
                "target_pair_count": ACTIVE_PAIR_COUNT,
                "measured_146_wall_seconds": active_wall_seconds,
                "replay_multiple": ACTIVE_REPLAY_MULTIPLE,
                "conservatism_multiplier": ACTIVE_PROJECTION_MULTIPLIER,
                "projected_wall_seconds": active_projected_seconds,
            },
            "stock_session": {
                "evidence_pair_count": HISTORICAL_SESSION_PAIR_COUNT,
                "target_pair_count": SESSION_PAIR_COUNT,
                "conservative_measured_seconds_per_pair": session_seconds_per_pair,
                "projected_wall_seconds": session_projected_seconds,
            },
            "accounting": "SUM_OF_CONSERVATIVE_BACKEND_WALL_PROJECTIONS",
            "host_hour_rounding": "CEILING_TO_0.25_HOUR",
        },
        "speedup_threshold": "NONE",
        "data_role": "development_train_only",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
        "formal_evaluator_authority": "UNCHANGED",
        "streaming_backend_authority": "EXPERIMENTAL_BACKEND",
        "kernel_state": "PARTIALLY_QUALIFIED",
        "adaptation": "FORBIDDEN",
        "resource_gate_action": "FAIL_CLOSED_NO_PLAN_CHANGE",
        "adjudication": {
            "readjudication_required": True,
            "execution_fail_status_preserved": True,
            "generic_scaling_status_used_as_gate": False,
            "legacy_checkpoint_exact_pass_used_as_gate": False,
            "two_x_speedup_used_as_gate": False,
        },
        "boundaries": {
            "data_role": "development_train_only",
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "promotion": "FORBIDDEN",
            "strict_stage_a": "NOT_AUTHORIZED",
            "formal_evaluator_authority": "UNCHANGED",
            "streaming_backend_authority": "EXPERIMENTAL_BACKEND",
        },
        "resource_gate_pass": passed,
        "evidence": evidence,
        "errors": errors,
        "next_decision": (
            "AUTHORIZE_FROZEN_1024_ROUTE_ASYMMETRIC_EXECUTION_CONTRACT"
            if passed
            else "STOP_BEFORE_1024_EXECUTION"
        ),
    }
    contract["contract_hash"] = contract_hash(contract)
    _atomic_json(output_path, contract)
    return contract


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--freeze", type=Path, required=True)
    parser.add_argument("--historical-subset", type=Path, required=True)
    parser.add_argument("--readjudicated-parity", type=Path, required=True)
    parser.add_argument("--readjudication-manifest", type=Path, required=True)
    parser.add_argument("--execution-receipt", type=Path, required=True)
    parser.add_argument("--historical-session-execution", type=Path, required=True)
    parser.add_argument("--historical-session-result", type=Path, required=True)
    parser.add_argument("--sidecar-closure", type=Path, required=True)
    parser.add_argument("--source-closure", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--host-hours-hard-max", type=float, default=HOST_HOURS_HARD_MAX)
    parser.add_argument("--active-native-threads", type=int, default=ACTIVE_NATIVE_THREADS)
    parser.add_argument("--session-native-threads", type=int, default=SESSION_NATIVE_THREADS)
    args = parser.parse_args(argv)

    contract = freeze_resource_contract(
        freeze_path=args.freeze,
        historical_subset_path=args.historical_subset,
        readjudicated_parity_path=args.readjudicated_parity,
        readjudication_manifest_path=args.readjudication_manifest,
        execution_receipt_path=args.execution_receipt,
        historical_session_execution_path=args.historical_session_execution,
        historical_session_result_path=args.historical_session_result,
        sidecar_closure_path=args.sidecar_closure,
        source_closure_path=args.source_closure,
        output_path=args.output,
        host_hours_hard_max=args.host_hours_hard_max,
        active_native_threads=args.active_native_threads,
        session_native_threads=args.session_native_threads,
    )
    print(
        json.dumps(
            {
                "status": contract["status"],
                "contract_hash": contract["contract_hash"],
                "host_hours_projected": contract["host_hours_projected"],
                "host_hours_hard_max": contract["host_hours_hard_max"],
                "errors": contract["errors"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0 if contract["status"] == PASS_STATUS else 2


if __name__ == "__main__":
    raise SystemExit(main())
