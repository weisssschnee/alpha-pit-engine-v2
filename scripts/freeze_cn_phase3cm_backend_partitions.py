from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.services.phase3cm_streaming_resource_contract import (
    FrozenExecutionPlan,
    balanced_pair_batches,
)

ACCESS_EVIDENCE_FIELDS = (
    "validation_reads",
    "holdout_reads",
    "forward_2026_reads",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()


def _integer(value: Any, *, default: int) -> int:
    """Parse an integer without treating the valid value zero as missing."""

    return default if value is None or value == "" else int(value)


def _require_zero_access_evidence(
    payload: Mapping[str, Any],
    *,
    context: str,
) -> dict[str, int]:
    """Reject absent, coerced, boolean, or non-zero sealed-access evidence."""

    missing = [name for name in ACCESS_EVIDENCE_FIELDS if name not in payload]
    if missing:
        raise ValueError(
            f"{context} access evidence incomplete: missing {','.join(missing)}"
        )
    evidence: dict[str, int] = {}
    for name in ACCESS_EVIDENCE_FIELDS:
        value = payload[name]
        if isinstance(value, bool) or type(value) is not int:
            raise ValueError(
                f"{context} access evidence must be an explicit integer: {name}"
            )
        if value != 0:
            raise ValueError(f"{context} records forbidden reads: {name}={value}")
        evidence[name] = value
    return evidence


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _source_closure_manifest_record(
    path: Path,
    *,
    expected_repo_sha: str,
) -> dict[str, Any]:
    manifest_path = Path(path).resolve()
    payload = _read_json(manifest_path)
    claimed_hash = str(payload.get("manifest_hash") or "")
    body = dict(payload)
    body.pop("manifest_hash", None)
    if _stable_hash(body) != claimed_hash:
        raise ValueError("source closure manifest self-hash drift")
    if payload.get("schema_version") != "cn_phase3cm_source_closure_manifest_v1":
        raise ValueError("source closure manifest schema drift")
    if payload.get("status") != "CN_PHASE3CM_SOURCE_CLOSURE_MANIFEST_READY":
        raise ValueError("source closure manifest status drift")
    if str(payload.get("repo_sha") or "") != str(expected_repo_sha):
        raise ValueError("source closure manifest repo SHA drift")
    sources = list(payload.get("sources") or [])
    if _stable_hash(sources) != str(payload.get("source_closure_hash") or ""):
        raise ValueError("source closure manifest source hash drift")
    if list(payload.get("source_paths") or []) != [
        str(row.get("path") or "") for row in sources
    ]:
        raise ValueError("source closure manifest path/record drift")
    return {
        "path": str(manifest_path),
        "sha256": _sha256(manifest_path),
        "manifest_hash": claimed_hash,
        "source_closure_hash": str(payload["source_closure_hash"]),
        "repo_sha": str(payload["repo_sha"]),
        "source_count": len(sources),
    }


def _read_candidate_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("candidate table has no header")
        return list(reader.fieldnames), [dict(row) for row in reader]


def _write_candidate_rows(
    path: Path,
    *,
    fieldnames: Sequence[str],
    rows: Sequence[Mapping[str, str]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames), extrasaction="raise")
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def _ordered_pairs(rows: Sequence[Mapping[str, str]]) -> tuple[list[str], dict[str, list[dict[str, str]]]]:
    order: list[str] = []
    by_pair: dict[str, list[dict[str, str]]] = {}
    for raw in rows:
        row = dict(raw)
        pair_id = str(row.get("pair_id") or "")
        if not pair_id:
            raise ValueError("candidate row is missing pair_id")
        if pair_id not in by_pair:
            order.append(pair_id)
            by_pair[pair_id] = []
        by_pair[pair_id].append(row)
    for pair_id, members in by_pair.items():
        roles = sorted(str(row.get("pair_member_role") or "") for row in members)
        if len(members) != 2 or roles != ["CONTROL", "PRIMARY"]:
            raise ValueError(f"pair does not contain one primary and one control: {pair_id}")
    return order, by_pair


def freeze_backend_partitions(
    *,
    candidate_table: Path,
    binding_path: Path,
    source_plan_path: Path,
    output_root: Path,
    repo_sha: str,
    logical_backend: str,
    partition_count: int,
    compute_threads_per_partition: int,
    pair_batch_size: int,
    predecessor_engineering_receipt: Path,
    source_closure_manifest: Path,
    reused_backend_result: Path | None = None,
    reused_backend_execution_receipt: Path | None = None,
) -> dict[str, Any]:
    if not re.fullmatch(r"[0-9a-f]{40}", repo_sha):
        raise ValueError("repo SHA must be a full lowercase 40-character Git SHA")
    if partition_count < 2:
        raise ValueError("partitioned qualification requires at least two heavy processes")
    if partition_count > 2:
        raise ValueError("global qualification contract permits at most two heavy processes")
    if compute_threads_per_partition <= 1:
        raise ValueError("each partition requires a real primary native thread pool")
    if partition_count * compute_threads_per_partition > 24:
        raise ValueError("global native compute thread budget exceeds 24")
    if pair_batch_size <= 0:
        raise ValueError("pair batch size must be positive")
    if output_root.exists():
        raise FileExistsError(f"partition output already exists: {output_root}")

    source_closure_record = _source_closure_manifest_record(
        source_closure_manifest,
        expected_repo_sha=repo_sha,
    )

    binding = _read_json(binding_path)
    if str(binding.get("status")) != "CN_STREAMING_REPAIR_FROZEN_INPUT_BOUND":
        raise ValueError("frozen input binding status drift")
    if str(binding.get("data_role")) != "development":
        raise ValueError("partition qualification is development-only")
    if binding.get("sealed_reads") != {"validation": 0, "holdout": 0, "forward_2026": 0}:
        raise ValueError("sealed-read contract drift")
    claimed_binding_hash = str(binding.get("binding_hash") or "")
    binding_body = dict(binding)
    binding_body.pop("binding_hash", None)
    if _stable_hash(binding_body) != claimed_binding_hash:
        raise ValueError("frozen input binding self-hash drift")

    predecessor = _read_json(predecessor_engineering_receipt)
    if predecessor.get("schema_version") != "cn_phase3cm_r6_successor_engineering_receipt_v1":
        raise ValueError("predecessor engineering receipt schema drift")
    if predecessor.get("status") != "CN_PHASE3CM_R6_SUCCESSOR_ENGINEERING_PASS":
        raise ValueError("predecessor engineering receipt is not PASS")
    if re.fullmatch(r"[0-9a-f]{40}", str(predecessor.get("repo_sha") or "")) is None:
        raise ValueError("predecessor engineering receipt repo SHA drift")
    if predecessor.get("data_role") != "development_train_only":
        raise ValueError("predecessor engineering receipt data role drift")
    predecessor_access = predecessor.get("access")
    if not isinstance(predecessor_access, Mapping):
        raise ValueError("predecessor engineering receipt access evidence incomplete")
    _require_zero_access_evidence(
        predecessor_access,
        context="predecessor engineering receipt",
    )
    if predecessor.get("promotion") != "FORBIDDEN":
        raise ValueError("predecessor engineering receipt promotion drift")
    if int(predecessor.get("pair_count") or 0) != 64 or predecessor.get(
        "backend_pair_counts"
    ) != {"active_bar": 36, "stock_session": 28}:
        raise ValueError("predecessor engineering receipt pair coverage drift")
    research_parity = dict(predecessor.get("research_parity") or {})
    if research_parity.get("status") != "BYTE_IDENTICAL_PAIR_ANALYSIS" or re.fullmatch(
        r"[0-9a-f]{64}", str(research_parity.get("pair_csv_sha256") or "")
    ) is None:
        raise ValueError("predecessor research parity drift")
    if predecessor.get("active_backend", {}).get("parallelism_status") != "PARALLELISM_ENGAGED" or predecessor.get(
        "stock_session_backend", {}
    ).get("parallelism_status") != "PARALLELISM_ENGAGED":
        raise ValueError("predecessor backend resource gate drift")
    supersession = dict(predecessor.get("supersession") or {})
    if (
        supersession.get("original_r6_status")
        != "CN_PHASE3CM_PHASE_E_STRICT_WAVE_FAIL"
        or bool(supersession.get("research_rows_changed"))
        or supersession.get("original_backend_exit_codes")
        != {"active_bar": 0, "stock_session": 0}
    ):
        raise ValueError("predecessor supersession evidence drift")

    source_plan = FrozenExecutionPlan.from_dict(_read_json(source_plan_path))
    if source_plan.phase != "E":
        raise ValueError("partition source must be a frozen Phase E plan")

    fieldnames, candidate_rows = _read_candidate_rows(candidate_table)
    pair_order, by_pair = _ordered_pairs(candidate_rows)
    bound_pairs = {
        str(row["pair_id"]): row
        for row in binding.get("pairs") or []
        if str(row.get("clock_namespace")) == logical_backend
    }
    if set(pair_order) != set(bound_pairs):
        raise ValueError("candidate table does not exactly cover the bound logical backend")
    bound_members = {
        str(row["candidate_id"]): dict(row)
        for row in binding.get("candidate_members") or []
        if str(row.get("clock_namespace")) == logical_backend
    }
    if len(bound_members) != sum(
        1
        for row in binding.get("candidate_members") or []
        if str(row.get("clock_namespace")) == logical_backend
    ):
        raise ValueError("binding contains duplicate candidate member identities")
    observed_members = {
        str(row.get("candidate_id") or ""): dict(row) for row in candidate_rows
    }
    if "" in observed_members or len(observed_members) != len(candidate_rows):
        raise ValueError("candidate table contains empty or duplicate candidate identities")
    if set(observed_members) != set(bound_members):
        raise ValueError("candidate members do not exactly cover the bound logical backend")
    for candidate_id, observed in observed_members.items():
        expected = bound_members[candidate_id]
        for key in ("pair_id", "pair_member_role", "expression", "route_id"):
            if str(observed.get(key) or "") != str(expected.get(key) or ""):
                raise ValueError(
                    f"candidate member semantic drift for {candidate_id}: {key}"
                )
        if "canonical_expression" in observed and str(
            observed.get("canonical_expression") or ""
        ) != str(expected.get("canonical_expression") or ""):
            raise ValueError(
                f"candidate member semantic drift for {candidate_id}: canonical_expression"
            )
        observed_clock = str(observed.get("clock_namespace") or logical_backend)
        if observed_clock != str(expected.get("clock_namespace") or ""):
            raise ValueError(
                f"candidate member semantic drift for {candidate_id}: clock_namespace"
            )
    for pair_id, members in by_pair.items():
        expected_pair = bound_pairs[pair_id]
        member_by_role = {str(row["pair_member_role"]): row for row in members}
        if str(member_by_role["PRIMARY"]["candidate_id"]) != str(
            expected_pair.get("candidate_id") or ""
        ) or str(member_by_role["CONTROL"]["candidate_id"]) != str(
            expected_pair.get("control_candidate_id") or ""
        ):
            raise ValueError(f"candidate pair membership drift for {pair_id}")
    source_pair_ids = {pair_id for batch in source_plan.pair_batches for pair_id in batch}
    if source_pair_ids != set(pair_order):
        raise ValueError("source execution plan pair identities drift from candidate table")

    reused_result_record: dict[str, Any] | None = None
    if reused_backend_result is not None:
        if reused_backend_execution_receipt is None:
            raise ValueError("reused backend result requires its execution receipt")
        reused = _read_json(reused_backend_result)
        reused_backend = str(reused.get("backend") or "")
        if reused_backend == logical_backend or not reused_backend:
            raise ValueError("reused backend result must cover a different logical backend")
        if reused.get("status") != "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED":
            raise ValueError("reused backend result is not complete")
        if reused.get("parallelism_status") != "PARALLELISM_ENGAGED":
            raise ValueError("reused backend result did not pass its resource gate")
        if str(reused.get("input_binding_hash") or "") != str(binding["binding_hash"]):
            raise ValueError("reused backend result binding hash drift")
        reused_access = _require_zero_access_evidence(
            reused,
            context="reused backend result",
        )
        expected_reused_pairs = {
            str(row["pair_id"])
            for row in binding.get("pairs") or []
            if str(row.get("clock_namespace")) == reused_backend
        }
        observed_reused_pairs = {
            str(row["pair_id"]) for row in reused.get("pair_results") or []
        }
        if observed_reused_pairs != expected_reused_pairs:
            raise ValueError("reused backend result does not exactly cover its bound pairs")
        if int(reused.get("pair_count") or -1) != len(expected_reused_pairs):
            raise ValueError("reused backend result pair count drift")
        source_run = _read_json(reused_backend_execution_receipt)
        if source_run.get("schema_version") != "cn_phase3cm_phase_e_combined_execution_receipt_v1":
            raise ValueError("reused backend source receipt schema drift")
        if source_run.get("status") != "CN_PHASE3CM_PHASE_E_STRICT_WAVE_FAIL":
            raise ValueError("reused backend source receipt must preserve its historical FAIL status")
        if str(source_run.get("combined_contract_repo_sha") or "") == "" or not re.fullmatch(
            r"[0-9a-f]{40}", str(source_run.get("combined_contract_repo_sha") or "")
        ):
            raise ValueError("reused backend source receipt lacks an exact repo SHA")
        if str(Path(str(source_run.get("session_result") or "")).resolve()) != str(
            reused_backend_result.resolve()
        ):
            raise ValueError("reused backend source receipt points to a different result")
        if _integer(source_run.get("session_exit_code"), default=-1) != 0:
            raise ValueError("reused backend source process did not exit cleanly")
        if _integer(source_run.get("active_exit_code"), default=0) == 0:
            raise ValueError("reused backend source receipt no longer records the active failure")
        if _integer(source_run.get("session_pair_count"), default=-1) != len(
            expected_reused_pairs
        ):
            raise ValueError("reused backend source receipt session pair count drift")
        if source_run.get("promotion") != "FORBIDDEN" or source_run.get(
            "strict_stage_a"
        ) != "NOT_AUTHORIZED":
            raise ValueError("reused backend source receipt research boundary drift")
        source_run_access = _require_zero_access_evidence(
            source_run,
            context="reused backend source receipt",
        )
        reused_result_record = {
            "logical_backend": reused_backend,
            "pair_count": len(expected_reused_pairs),
            "path": str(reused_backend_result.resolve()),
            "sha256": _sha256(reused_backend_result),
            "status": str(reused["status"]),
            "parallelism_status": str(reused["parallelism_status"]),
            "input_binding_hash": str(reused["input_binding_hash"]),
            "access_evidence_complete": True,
            "access": reused_access,
            "reuse_basis": "IMMUTABLE_COMPLETED_BACKEND_PLUS_R6_BYTE_IDENTICAL_RESEARCH_PARITY",
            "source_execution_receipt": {
                "path": str(reused_backend_execution_receipt.resolve()),
                "sha256": _sha256(reused_backend_execution_receipt),
                "status": str(source_run.get("status") or ""),
                "repo_sha": str(source_run["combined_contract_repo_sha"]),
                "session_exit_code": 0,
                "active_exit_code": _integer(source_run.get("active_exit_code"), default=-1),
                "access_evidence_complete": True,
                "access": source_run_access,
            },
        }

    # Round-robin preserves deterministic source order while balancing route/order
    # effects across the two independent heavy processes.
    partition_pair_ids = [pair_order[index::partition_count] for index in range(partition_count)]
    if any(not pair_ids for pair_ids in partition_pair_ids):
        raise ValueError("each execution partition must contain at least one pair")

    output_root.mkdir(parents=True)
    partition_records: list[dict[str, Any]] = []
    for ordinal, pair_ids in enumerate(partition_pair_ids):
        partition_root = output_root / f"partition_{ordinal:02d}"
        table_path = partition_root / "candidates.csv"
        plan_path = partition_root / "CN_FROZEN_EXECUTION_PLAN.json"
        partition_rows = [member for pair_id in pair_ids for member in by_pair[pair_id]]
        _write_candidate_rows(table_path, fieldnames=fieldnames, rows=partition_rows)
        plan = FrozenExecutionPlan.create(
            phase="E",
            block_size=source_plan.block_size,
            block_boundaries=source_plan.block_boundaries,
            pair_batches=balanced_pair_batches(pair_ids, pair_batch_size),
            heavy_processes=partition_count,
            compute_threads=compute_threads_per_partition,
            primary_thread_pool=source_plan.primary_thread_pool,
            cache_caps=source_plan.cache_caps,
            checkpoint_every_blocks=source_plan.checkpoint_every_blocks,
            rss_soft_bytes=source_plan.rss_soft_bytes,
            rss_hard_bytes=source_plan.rss_hard_bytes,
            global_rss_hard_bytes=source_plan.global_rss_hard_bytes,
        )
        _write_json(plan_path, plan.to_dict())
        partition_records.append(
            {
                "partition_id": f"{logical_backend}_p{ordinal}",
                "logical_backend": logical_backend,
                "pair_count": len(pair_ids),
                "pair_ids": pair_ids,
                "candidate_table": str(table_path.resolve()),
                "candidate_table_sha256": _sha256(table_path),
                "execution_plan": str(plan_path.resolve()),
                "execution_plan_sha256": _sha256(plan_path),
                "execution_plan_hash": plan.execution_plan_hash,
                "compute_threads": compute_threads_per_partition,
            }
        )

    flattened = [pair_id for record in partition_records for pair_id in record["pair_ids"]]
    if len(flattened) != len(set(flattened)) or set(flattened) != set(pair_order):
        raise AssertionError("partition disjointness or completeness failed")
    contract: dict[str, Any] = {
        "schema_version": "cn_phase3cm_backend_partition_contract_v1",
        "status": "CN_PHASE3CM_BACKEND_PARTITIONS_FROZEN",
        "repo_sha": repo_sha,
        "logical_backend": logical_backend,
        "partition_method": "DETERMINISTIC_ROUND_ROBIN_SOURCE_ORDER",
        "partition_count": partition_count,
        "pair_count": len(pair_order),
        "heavy_processes": partition_count,
        "compute_threads_per_partition": compute_threads_per_partition,
        "global_active_native_compute_threads": partition_count * compute_threads_per_partition,
        "candidate_table": {"path": str(candidate_table.resolve()), "sha256": _sha256(candidate_table)},
        "binding": {
            "path": str(binding_path.resolve()),
            "sha256": _sha256(binding_path),
            "binding_hash": str(binding["binding_hash"]),
        },
        "source_execution_plan": {
            "path": str(source_plan_path.resolve()),
            "sha256": _sha256(source_plan_path),
            "execution_plan_hash": source_plan.execution_plan_hash,
        },
        "source_closure_manifest": source_closure_record,
        "predecessor_engineering_receipt": {
            "path": str(predecessor_engineering_receipt.resolve()),
            "sha256": _sha256(predecessor_engineering_receipt),
            "status": str(predecessor["status"]),
            "research_parity": predecessor.get("research_parity"),
        },
        "partitions": partition_records,
        "reused_backend_result": reused_result_record,
        "total_bound_pair_count": len(pair_order)
        + (int(reused_result_record["pair_count"]) if reused_result_record else 0),
        "data_role": "development_train_only",
        "sealed_reads": {"validation": 0, "holdout": 0, "forward_2026": 0},
        "adaptation": "FORBIDDEN",
        "promotion": "FORBIDDEN",
    }
    contract["contract_hash"] = _stable_hash(contract)
    contract_path = output_root / "CN_BACKEND_PARTITION_CONTRACT.json"
    _write_json(contract_path, contract)
    contract["contract_path"] = str(contract_path.resolve())
    contract["contract_sha256"] = _sha256(contract_path)
    return contract


def validate_partition_contract(
    contract_path: Path,
    *,
    expected_repo_sha: str,
    source_closure_manifest: Path,
) -> dict[str, Any]:
    contract = _read_json(contract_path)
    claimed_hash = str(contract.get("contract_hash") or "")
    body = dict(contract)
    body.pop("contract_hash", None)
    if _stable_hash(body) != claimed_hash:
        raise ValueError("partition contract self-hash drift")
    if contract.get("status") != "CN_PHASE3CM_BACKEND_PARTITIONS_FROZEN":
        raise ValueError("partition contract status drift")
    if str(contract.get("repo_sha") or "") != expected_repo_sha:
        raise ValueError("partition contract repo SHA drift")
    if contract.get("sealed_reads") != {
        "validation": 0,
        "holdout": 0,
        "forward_2026": 0,
    }:
        raise ValueError("partition contract sealed-read drift")
    if contract.get("adaptation") != "FORBIDDEN" or contract.get("promotion") != "FORBIDDEN":
        raise ValueError("partition contract research boundary drift")
    partitions = list(contract.get("partitions") or [])
    if len(partitions) != int(contract.get("partition_count") or -1):
        raise ValueError("partition contract count drift")
    if len(partitions) != int(contract.get("heavy_processes") or -1):
        raise ValueError("partition heavy-process count drift")
    if int(contract.get("global_active_native_compute_threads") or 0) > 24:
        raise ValueError("partition thread budget drift")

    source_closure_record = dict(contract.get("source_closure_manifest") or {})
    observed_source_closure = _source_closure_manifest_record(
        source_closure_manifest,
        expected_repo_sha=expected_repo_sha,
    )
    for key in (
        "sha256",
        "manifest_hash",
        "source_closure_hash",
        "repo_sha",
        "source_count",
    ):
        if observed_source_closure[key] != source_closure_record.get(key):
            raise ValueError(f"partition source closure manifest drift: {key}")

    for artifact_key in ("candidate_table", "binding", "source_execution_plan"):
        record = dict(contract.get(artifact_key) or {})
        path = Path(str(record.get("path") or ""))
        if not path.is_file() or _sha256(path) != str(record.get("sha256") or ""):
            raise ValueError(f"partition source artifact drift: {artifact_key}")
    predecessor = dict(contract.get("predecessor_engineering_receipt") or {})
    predecessor_path = Path(str(predecessor.get("path") or ""))
    if not predecessor_path.is_file() or _sha256(predecessor_path) != predecessor.get("sha256"):
        raise ValueError("partition predecessor engineering receipt drift")
    predecessor_payload = _read_json(predecessor_path)
    if predecessor_payload.get("status") != "CN_PHASE3CM_R6_SUCCESSOR_ENGINEERING_PASS":
        raise ValueError("partition predecessor engineering receipt is no longer PASS")

    all_pair_ids: list[str] = []
    for record in partitions:
        table_path = Path(str(record.get("candidate_table") or ""))
        plan_path = Path(str(record.get("execution_plan") or ""))
        if not table_path.is_file() or _sha256(table_path) != record.get("candidate_table_sha256"):
            raise ValueError("partition candidate table drift")
        if not plan_path.is_file() or _sha256(plan_path) != record.get("execution_plan_sha256"):
            raise ValueError("partition execution plan file drift")
        plan = FrozenExecutionPlan.from_dict(_read_json(plan_path))
        if plan.execution_plan_hash != record.get("execution_plan_hash"):
            raise ValueError("partition execution plan identity drift")
        if plan.compute_threads != int(record.get("compute_threads") or 0):
            raise ValueError("partition compute thread drift")
        fieldnames, candidate_rows = _read_candidate_rows(table_path)
        del fieldnames
        observed_order, _ = _ordered_pairs(candidate_rows)
        expected_order = [str(value) for value in record.get("pair_ids") or []]
        if observed_order != expected_order:
            raise ValueError("partition pair order drift")
        plan_order = [pair_id for batch in plan.pair_batches for pair_id in batch]
        if plan_order != expected_order:
            raise ValueError("partition plan pair order drift")
        if int(record.get("pair_count") or -1) != len(expected_order):
            raise ValueError("partition pair count drift")
        all_pair_ids.extend(expected_order)
    if len(all_pair_ids) != len(set(all_pair_ids)):
        raise ValueError("partition pair overlap detected")
    if len(all_pair_ids) != int(contract.get("pair_count") or -1):
        raise ValueError("partition pair completeness drift")

    reused = contract.get("reused_backend_result")
    reused_pair_count = 0
    if reused is not None:
        reused_record = dict(reused)
        reused_path = Path(str(reused_record.get("path") or ""))
        if not reused_path.is_file() or _sha256(reused_path) != reused_record.get("sha256"):
            raise ValueError("reused backend result artifact drift")
        reused_payload = _read_json(reused_path)
        if reused_payload.get("status") != "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED":
            raise ValueError("reused backend result is no longer complete")
        if reused_payload.get("parallelism_status") != "PARALLELISM_ENGAGED":
            raise ValueError("reused backend result resource gate drift")
        reused_access = _require_zero_access_evidence(
            reused_payload,
            context="reused backend result",
        )
        if reused_record.get("access_evidence_complete") is not True or reused_access != dict(
            reused_record.get("access") or {}
        ):
            raise ValueError("reused backend result access evidence drift")
        reused_pair_count = int(reused_payload.get("pair_count") or 0)
        if reused_pair_count != int(reused_record.get("pair_count") or -1):
            raise ValueError("reused backend result pair count drift")
        source_record = dict(reused_record.get("source_execution_receipt") or {})
        source_path = Path(str(source_record.get("path") or ""))
        if not source_path.is_file() or _sha256(source_path) != source_record.get("sha256"):
            raise ValueError("reused backend source execution receipt drift")
        source_payload = _read_json(source_path)
        source_repo_sha = str(source_payload.get("combined_contract_repo_sha") or "")
        if not re.fullmatch(r"[0-9a-f]{40}", source_repo_sha):
            raise ValueError("reused backend source receipt repo SHA drift")
        if source_repo_sha != str(source_record.get("repo_sha") or ""):
            raise ValueError("reused backend source receipt recorded repo SHA drift")
        if str(source_payload.get("status") or "") != str(source_record.get("status") or ""):
            raise ValueError("reused backend source receipt status drift")
        if source_payload.get("schema_version") != "cn_phase3cm_phase_e_combined_execution_receipt_v1":
            raise ValueError("reused backend source receipt schema drift")
        if str(Path(str(source_payload.get("session_result") or "")).resolve()) != str(
            reused_path.resolve()
        ):
            raise ValueError("reused backend source receipt result path drift")
        if _integer(source_payload.get("session_exit_code"), default=-1) != 0:
            raise ValueError("reused backend source receipt session exit drift")
        if _integer(source_payload.get("active_exit_code"), default=0) == 0:
            raise ValueError("reused backend source receipt active failure drift")
        if _integer(source_payload.get("session_pair_count"), default=-1) != reused_pair_count:
            raise ValueError("reused backend source receipt session pair count drift")
        if source_payload.get("promotion") != "FORBIDDEN" or source_payload.get(
            "strict_stage_a"
        ) != "NOT_AUTHORIZED":
            raise ValueError("reused backend source receipt research boundary drift")
        if _integer(source_record.get("session_exit_code"), default=-1) != 0:
            raise ValueError("reused backend recorded session exit drift")
        if _integer(source_payload.get("active_exit_code"), default=-1) != _integer(
            source_record.get("active_exit_code"), default=-1
        ):
            raise ValueError("reused backend source receipt active exit drift")
        source_access = _require_zero_access_evidence(
            source_payload,
            context="reused backend source receipt",
        )
        if source_record.get("access_evidence_complete") is not True or source_access != dict(
            source_record.get("access") or {}
        ):
            raise ValueError("reused backend source receipt access drift")
    if len(all_pair_ids) + reused_pair_count != int(contract.get("total_bound_pair_count") or -1):
        raise ValueError("partition total bound pair count drift")
    return {
        "status": "CN_PHASE3CM_BACKEND_PARTITION_CONTRACT_VALIDATED",
        "contract_hash": claimed_hash,
        "logical_backend": str(contract["logical_backend"]),
        "partition_count": len(partitions),
        "pair_count": len(all_pair_ids),
        "reused_pair_count": reused_pair_count,
        "total_bound_pair_count": len(all_pair_ids) + reused_pair_count,
        "access_evidence_complete": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--validate-contract", type=Path)
    parser.add_argument("--expected-repo-sha")
    parser.add_argument("--source-closure-manifest", type=Path)
    parser.add_argument("--candidate-table", type=Path)
    parser.add_argument("--binding", type=Path)
    parser.add_argument("--source-plan", type=Path)
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--repo-sha")
    parser.add_argument("--logical-backend")
    parser.add_argument("--partition-count", type=int, default=2)
    parser.add_argument("--compute-threads-per-partition", type=int, default=11)
    parser.add_argument("--pair-batch-size", type=int, default=8)
    parser.add_argument("--predecessor-engineering-receipt", type=Path)
    parser.add_argument("--reused-backend-result", type=Path)
    parser.add_argument("--reused-backend-execution-receipt", type=Path)
    args = parser.parse_args()
    if args.validate_contract is not None:
        if not args.expected_repo_sha or args.source_closure_manifest is None:
            parser.error(
                "--expected-repo-sha and --source-closure-manifest are required "
                "with --validate-contract"
            )
        result = validate_partition_contract(
            args.validate_contract.resolve(),
            expected_repo_sha=str(args.expected_repo_sha),
            source_closure_manifest=args.source_closure_manifest.resolve(),
        )
        print(json.dumps(result, sort_keys=True))
        return 0
    required = {
        "candidate-table": args.candidate_table,
        "binding": args.binding,
        "source-plan": args.source_plan,
        "output-root": args.output_root,
        "repo-sha": args.repo_sha,
        "logical-backend": args.logical_backend,
        "predecessor-engineering-receipt": args.predecessor_engineering_receipt,
        "source-closure-manifest": args.source_closure_manifest,
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        parser.error("missing required arguments: " + ", ".join(missing))
    contract = freeze_backend_partitions(
        candidate_table=args.candidate_table.resolve(),
        binding_path=args.binding.resolve(),
        source_plan_path=args.source_plan.resolve(),
        output_root=args.output_root.resolve(),
        repo_sha=str(args.repo_sha),
        logical_backend=str(args.logical_backend),
        partition_count=int(args.partition_count),
        compute_threads_per_partition=int(args.compute_threads_per_partition),
        pair_batch_size=int(args.pair_batch_size),
        predecessor_engineering_receipt=args.predecessor_engineering_receipt.resolve(),
        source_closure_manifest=args.source_closure_manifest.resolve(),
        reused_backend_result=(
            args.reused_backend_result.resolve()
            if args.reused_backend_result is not None
            else None
        ),
        reused_backend_execution_receipt=(
            args.reused_backend_execution_receipt.resolve()
            if args.reused_backend_execution_receipt is not None
            else None
        ),
    )
    print(
        json.dumps(
            {
                "status": contract["status"],
                "pair_count": contract["pair_count"],
                "partition_pair_counts": [row["pair_count"] for row in contract["partitions"]],
                "contract_path": contract["contract_path"],
                "contract_sha256": contract["contract_sha256"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
