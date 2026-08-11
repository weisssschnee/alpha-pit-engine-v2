"""Build a compiled-program-complete development session sidecar.

This is a zero-financial preflight.  It derives required fields from the full
compiled Candidate Program schedule, materializes only missing registered
leaves through scope-aware adapters, proves source-payload parity, and applies
one primary program from every template without replaying returns or rewards.
"""

from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
import psutil


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.augment_cn_phase3cm_session_time_major_sidecar import (
    STABLE_KEY,
    _frame_digest,
    _materialize_lagged_daily_context,
    _materialize_market_session_context,
    _materialize_stock_session_close,
    _sha256,
)
from our_system_phase2.services.candidate_program_execution_v1 import (
    apply_compiled_candidate_program_v1,
)
from our_system_phase2.services.candidate_program_materialization_v1 import (
    ADAPTER_ALREADY_MATERIALIZED,
    ADAPTER_MARKET_PRELAGGED_BROADCAST,
    ADAPTER_STOCK_PRELAGGED_SESSION,
    ADAPTER_STOCK_SESSION_CLOSE,
    resolve_program_information_coverage_v1,
    resolve_program_materialization_plan_v1,
    verify_program_information_coverage_v1,
    verify_program_materialization_plan_v1,
)
from our_system_phase2.services.candidate_program_v1 import (
    CandidateProgramSpecV1,
    ProgramCompilerV1,
)
from our_system_phase2.services.real_market_validation import evaluate_panel_expression
from our_system_phase2.services.unified_capability_registry import (
    UnifiedCapabilityRegistry,
    stable_hash,
)


SOURCE_MANIFEST_NAME = "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"
BAR_SOURCE_MANIFEST_NAME = "development_only_release_manifest.json"
PLAN_NAME = "PROGRAM_MATERIALIZATION_PLAN_V1.json"
INFORMATION_COVERAGE_NAME = "PROGRAM_INFORMATION_COVERAGE_V1.json"
FIXTURE_NAME = "PROGRAM_MATERIALIZATION_TEMPLATE_FIXTURES_V1.json"
ARTIFACT_MANIFEST_NAME = "ARTIFACT_MANIFEST.json"
CLOSURE_NAME = "PROGRAM_MATERIALIZATION_PREFLIGHT_V1.json"
STATUS = "PROGRAM_MATERIALIZATION_PREFLIGHT_COMPLETE"
V2_SCHEMA = "cn_development_time_major_execution_layout_manifest_v2_train_only"
V2_STATUS = "TIME_MAJOR_LAYOUT_PARITY_PASS"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in Path(path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(destination)
    return destination


def _self_hashed(payload: Mapping[str, Any], field: str) -> dict[str, Any]:
    output = dict(payload)
    output[field] = stable_hash(output)
    return output


def _verify_self_hash(payload: Mapping[str, Any], field: str, label: str) -> None:
    body = dict(payload)
    claimed = str(body.pop(field, ""))
    if len(claimed) != 64 or stable_hash(body) != claimed:
        raise ValueError(f"{label} self-hash drift")


def _verify_schedule(
    records: Sequence[Mapping[str, Any]],
    *,
    registry: UnifiedCapabilityRegistry,
    expected_records: int,
    expected_template_quota: int,
) -> None:
    if len(records) != expected_records:
        raise ValueError("program schedule record count drift")
    counts = Counter(str(record.get("template_id") or "") for record in records)
    if not counts or set(counts.values()) != {expected_template_quota}:
        raise ValueError(f"program template quota drift: {dict(counts)}")
    ordinals = [int(record.get("main_record_ordinal") or 0) for record in records]
    if ordinals != list(range(expected_records)):
        raise ValueError("program schedule identity/order drift")
    compiler = ProgramCompilerV1(registry)
    for ordinal, record in enumerate(records, start=1):
        body = dict(record)
        claimed = str(body.pop("schedule_record_sha256", ""))
        if stable_hash(body) != claimed:
            raise ValueError(f"program schedule record self-hash drift: {ordinal}")
        for program_key, compiled_key in (
            ("primary_program", "primary_compiled"),
            ("control_program", "control_compiled"),
        ):
            program = CandidateProgramSpecV1.from_record(dict(record[program_key]))
            compiled = compiler.compile(program)
            if compiled.to_record() != dict(record[compiled_key]):
                raise ValueError(f"program compiler replay drift: {ordinal} {compiled_key}")


def _validate_source_manifest(source_root: Path) -> tuple[dict[str, Any], Path]:
    path = Path(source_root).resolve() / SOURCE_MANIFEST_NAME
    manifest = _read_json(path)
    _verify_self_hash(manifest, "manifest_hash", "source sidecar manifest")
    required = {
        "schema_version": V2_SCHEMA,
        "status": V2_STATUS,
        "data_role": "development_train_only",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    drift = [key for key, value in required.items() if manifest.get(key) != value]
    if drift:
        raise ValueError(f"source sidecar authority drift: {drift}")
    shards = list(manifest.get("shards") or ())
    if len(shards) != int(manifest.get("source_shard_count") or -1) or not shards:
        raise ValueError("source sidecar shard cardinality drift")
    return manifest, path


def _validate_bar_source_manifest(bar_source_root: Path) -> tuple[dict[str, Any], Path]:
    path = Path(bar_source_root).resolve() / BAR_SOURCE_MANIFEST_NAME
    manifest = _read_json(path)
    if manifest.get("forbidden_roles_present") or bool(
        manifest.get("forward_2026_present")
    ):
        raise PermissionError("bar context source contains forbidden or sealed roles")
    return manifest, path


def _worker(
    *,
    shard: Mapping[str, Any],
    output_root: str,
    bar_source_root: str,
    plan: Mapping[str, Any],
) -> dict[str, Any]:
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    os.environ.setdefault("MKL_NUM_THREADS", "1")
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
    shard_index = int(shard["source_shard"])
    source_path = Path(str(shard["output_path"])).resolve()
    if not source_path.is_file() or _sha256(source_path) != str(shard["output_sha256"]):
        raise ValueError(f"source sidecar shard hash drift: {shard_index}")
    frame = pd.read_parquet(source_path)
    if len(frame) != int(shard["rows"]):
        raise ValueError(f"source sidecar shard row drift: {shard_index}")
    if sorted(set(STABLE_KEY) - set(frame.columns)):
        raise ValueError(f"source sidecar shard stable key drift: {shard_index}")
    frame["trade_time"] = pd.to_datetime(frame["trade_time"], errors="raise")
    if frame.duplicated(list(STABLE_KEY)).any():
        raise ValueError(f"source sidecar shard duplicate stable key: {shard_index}")
    original_columns = list(frame.columns)
    original_key_digest = _frame_digest(frame, list(STABLE_KEY))
    original_payload_digest = _frame_digest(frame, original_columns)

    by_adapter: dict[str, list[str]] = {}
    for binding in plan["field_bindings"]:
        adapter = str(binding["adapter_id"])
        if adapter != ADAPTER_ALREADY_MATERIALIZED:
            by_adapter.setdefault(adapter, []).append(str(binding["field_id"]))
    unsupported = sorted(
        set(by_adapter)
        - {
            ADAPTER_STOCK_PRELAGGED_SESSION,
            ADAPTER_MARKET_PRELAGGED_BROADCAST,
            ADAPTER_STOCK_SESSION_CLOSE,
        }
    )
    if unsupported:
        raise ValueError(f"worker lacks planned materialization adapters: {unsupported}")

    adapter_evidence: dict[str, Any] = {}
    if by_adapter.get(ADAPTER_STOCK_PRELAGGED_SESSION):
        frame, evidence = _materialize_lagged_daily_context(
            frame,
            fields=sorted(by_adapter[ADAPTER_STOCK_PRELAGGED_SESSION]),
            bar_source_root=Path(bar_source_root),
            shard_index=shard_index,
        )
        adapter_evidence[ADAPTER_STOCK_PRELAGGED_SESSION] = evidence
    if by_adapter.get(ADAPTER_MARKET_PRELAGGED_BROADCAST):
        frame, evidence = _materialize_market_session_context(
            frame,
            fields=sorted(by_adapter[ADAPTER_MARKET_PRELAGGED_BROADCAST]),
            bar_source_root=Path(bar_source_root),
            shard_index=shard_index,
        )
        adapter_evidence[ADAPTER_MARKET_PRELAGGED_BROADCAST] = evidence
    if by_adapter.get(ADAPTER_STOCK_SESSION_CLOSE):
        frame, evidence = _materialize_stock_session_close(
            frame,
            fields=sorted(by_adapter[ADAPTER_STOCK_SESSION_CLOSE]),
            bar_source_root=Path(bar_source_root),
            shard_index=shard_index,
        )
        adapter_evidence[ADAPTER_STOCK_SESSION_CLOSE] = evidence

    required = set(str(value) for value in plan["required_physical_leaf_ids"])
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"materialized sidecar shard misses program fields: {shard_index} {missing}")
    if len(frame) != int(shard["rows"]):
        raise RuntimeError(f"program materialization row drift: {shard_index}")
    if _frame_digest(frame, list(STABLE_KEY)) != original_key_digest:
        raise RuntimeError(f"program materialization stable-key drift: {shard_index}")
    if _frame_digest(frame, original_columns) != original_payload_digest:
        raise RuntimeError(f"program materialization changed source payload: {shard_index}")

    destination = Path(output_root).resolve() / f"shard_{shard_index:02d}.parquet"
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".tmp.parquet")
    frame.to_parquet(temporary, index=False, compression="zstd")
    temporary.replace(destination)
    coverage = {
        field_id: round(float(frame[field_id].notna().mean()), 10)
        for field_id in sorted(required)
    }
    receipt = _self_hashed(
        {
            "schema_version": "cn_program_materialized_session_shard_v1",
            "status": "PROGRAM_MATERIALIZED_SESSION_SHARD_COMPLETE",
            "shard_index": shard_index,
            "source": {
                "path": str(source_path),
                "sha256": _sha256(source_path),
                "rows": len(frame),
            },
            "output": {
                "path": str(destination),
                "sha256": _sha256(destination),
                "bytes": destination.stat().st_size,
                "rows": len(frame),
            },
            "source_stable_key_digest": original_key_digest,
            "source_payload_digest": original_payload_digest,
            "required_physical_leaf_ids": sorted(required),
            "already_present_required_field_ids": sorted(required & set(original_columns)),
            "materialized_field_ids": sorted(required - set(original_columns)),
            "adapter_evidence": adapter_evidence,
            "coverage": coverage,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
        },
        "receipt_sha256",
    )
    receipt_path = Path(output_root).resolve() / f"shard_{shard_index:02d}.materialization.json"
    _write_json(receipt_path, receipt)
    return {**receipt, "receipt_path": str(receipt_path), "receipt_file_sha256": _sha256(receipt_path)}


def _template_fixtures(
    *,
    records: Sequence[Mapping[str, Any]],
    registry: UnifiedCapabilityRegistry,
    sidecar_paths: Sequence[Path],
) -> dict[str, Any]:
    compiler = ProgramCompilerV1(registry)
    selected: dict[str, Mapping[str, Any]] = {}
    for record in records:
        selected.setdefault(str(record["template_id"]), record)
    fixtures: list[dict[str, Any]] = []
    for template_id in sorted(selected):
        record = selected[template_id]
        program = CandidateProgramSpecV1.from_record(dict(record["primary_program"]))
        compiled = compiler.compile(program)
        frame: pd.DataFrame | None = None
        output: pd.DataFrame | None = None
        joint_eligible = 0
        finite_scores = 0
        fixture_shard = -1
        for fixture_shard, sidecar_path in enumerate(sidecar_paths):
            frame = pd.read_parquet(sidecar_path)
            frame["trade_time"] = pd.to_datetime(
                frame["trade_time"], errors="raise"
            )
            output = apply_compiled_candidate_program_v1(
                frame,
                compiled,
                data_role="development",
                materialized_sidecar_clock_column="trade_time",
                materialized_sidecar_authority="PIT_MATERIALIZED_FIELD_SIDECAR",
            )
            joint_eligible = int(output["program_joint_eligible"].sum())
            finite_scores = int(
                np.isfinite(
                    pd.to_numeric(output["program_stock_score"], errors="coerce")
                ).sum()
            )
            if len(output) == len(frame) and joint_eligible > 0 and finite_scores > 0:
                break
        if (
            frame is None
            or output is None
            or len(output) != len(frame)
            or joint_eligible <= 0
            or finite_scores <= 0
        ):
            raise RuntimeError(f"program materialization fixture has no executable support: {template_id}")
        fixture: dict[str, Any] = {
            "template_id": template_id,
            "pair_id": str(record["pair_id"]),
            "main_record_ordinal": int(record["main_record_ordinal"]),
            "compiled_program_hash": compiled.to_record()["compiled_program_hash"],
            "physical_leaf_ids": list(compiled.physical_leaf_ids),
            "row_count": len(output),
            "joint_eligible_rows": joint_eligible,
            "finite_score_rows": finite_scores,
            "fixture_shard_index": fixture_shard,
            "lag_application_count": 0,
            "materialized_sidecar_lag_reapplied": False,
            "status": "PROGRAM_APPLY_PASS",
        }
        if template_id == "BASE":
            legacy_expression = str(record["legacy_primary_candidate"]["canonical_expression"])
            legacy = pd.to_numeric(
                evaluate_panel_expression(
                    frame,
                    legacy_expression,
                    field_lags={},
                    data_role="development",
                ),
                errors="coerce",
            )
            compiled_score = pd.to_numeric(output["program_stock_score"], errors="coerce")
            support = legacy.notna() & compiled_score.notna()
            if not bool(support.any()) or not np.allclose(
                legacy[support].to_numpy(),
                compiled_score[support].to_numpy(),
                rtol=0.0,
                atol=1e-12,
            ):
                raise RuntimeError("BASE compiled-versus-legacy materialized parity failed")
            fixture["base_compiled_legacy_score_parity"] = True
            fixture["base_parity_rows"] = int(support.sum())
        fixtures.append(fixture)
    payload = _self_hashed(
        {
            "schema_version": "cn_program_materialization_template_fixtures_v1",
            "status": "PROGRAM_MATERIALIZATION_TEMPLATE_FIXTURES_PASS",
            "template_count": len(fixtures),
            "fixtures": fixtures,
            "financial_evaluation_executed": False,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
        },
        "fixture_sha256",
    )
    return payload


def prepare(
    *,
    schedule_path: Path,
    registry_path: Path,
    information_metrics_path: Path,
    source_root: Path,
    bar_source_root: Path,
    output_root: Path,
    workers: int,
    minimum_free_memory_bytes: int,
    expected_records: int,
    expected_template_quota: int,
) -> dict[str, Any]:
    output_root = Path(output_root).resolve()
    if output_root.exists():
        allowed_bootstrap = {
            "deployment_binding.json",
            "program_materialization.stdout.log",
            "program_materialization.stderr.log",
            "resource_leases",
        }
        unexpected = sorted(
            path.name for path in output_root.iterdir() if path.name not in allowed_bootstrap
        )
        if unexpected:
            raise FileExistsError(
                f"program materialization output root is not fresh: {unexpected}"
            )
    else:
        output_root.mkdir(parents=True)
    if psutil.virtual_memory().available < minimum_free_memory_bytes:
        raise MemoryError("program materialization preflight lacks minimum free memory")

    registry = UnifiedCapabilityRegistry.read(registry_path)
    records = _read_jsonl(schedule_path)
    _verify_schedule(
        records,
        registry=registry,
        expected_records=expected_records,
        expected_template_quota=expected_template_quota,
    )
    source_manifest, source_manifest_path = _validate_source_manifest(source_root)
    _, bar_source_manifest_path = _validate_bar_source_manifest(bar_source_root)
    available_fields = set(str(value) for value in source_manifest.get("fields") or ())
    plan = resolve_program_materialization_plan_v1(
        records,
        registry=registry,
        available_fields=available_fields,
    )
    verify_program_materialization_plan_v1(plan)
    plan_path = _write_json(output_root / PLAN_NAME, plan)
    information_metrics_path = Path(information_metrics_path).resolve()
    information_metrics = json.loads(
        information_metrics_path.read_text(encoding="utf-8-sig")
    )
    if not isinstance(information_metrics, list):
        raise ValueError("information metrics authority must be a row list")
    information_coverage = resolve_program_information_coverage_v1(
        plan["required_physical_leaf_ids"],
        information_metrics=information_metrics,
        authority_path=str(information_metrics_path),
        authority_file_sha256=_sha256(information_metrics_path),
    )
    information_coverage_path = _write_json(
        output_root / INFORMATION_COVERAGE_NAME, information_coverage
    )
    verify_program_information_coverage_v1(information_coverage)

    shards = list(source_manifest["shards"])
    receipts: list[dict[str, Any]] = []
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                _worker,
                shard=shard,
                output_root=str(output_root),
                bar_source_root=str(Path(bar_source_root).resolve()),
                plan=plan,
            )
            for shard in shards
        ]
        for future in as_completed(futures):
            receipts.append(future.result())
            if psutil.virtual_memory().available < minimum_free_memory_bytes:
                raise MemoryError("program materialization crossed minimum free-memory gate")
    receipts.sort(key=lambda row: int(row["shard_index"]))
    if [int(row["shard_index"]) for row in receipts] != list(range(len(shards))):
        raise RuntimeError("program materialization receipt shard coverage drift")

    market_digests: dict[str, set[str]] = {}
    for receipt in receipts:
        evidence = dict(receipt.get("adapter_evidence") or {}).get(
            ADAPTER_MARKET_PRELAGGED_BROADCAST
        )
        if evidence:
            fields_key = "|".join(sorted(evidence["fields"]))
            market_digests.setdefault(fields_key, set()).add(str(evidence["session_value_digest"]))
    inconsistent = {key: sorted(values) for key, values in market_digests.items() if len(values) != 1}
    if inconsistent:
        raise RuntimeError(f"market-state broadcast differs across source shards: {inconsistent}")

    required = set(str(value) for value in plan["required_physical_leaf_ids"])
    fields = sorted(available_fields | required)
    manifest_shards: list[dict[str, Any]] = []
    parity: list[dict[str, Any]] = []
    total_rows = 0
    total_bytes = 0
    for source_shard, receipt in zip(shards, receipts, strict=True):
        output = dict(receipt["output"])
        total_rows += int(output["rows"])
        total_bytes += int(output["bytes"])
        manifest_shards.append(
            {
                "schema_version": V2_SCHEMA,
                "sidecar_identity": stable_hash(
                    {
                        "source_sha256": receipt["source"]["sha256"],
                        "output_sha256": output["sha256"],
                        "source_shard": receipt["shard_index"],
                        "split_manifest_hash": source_manifest["split_manifest_hash"],
                    }
                ),
                "source_shard": int(receipt["shard_index"]),
                "source_path": receipt["source"]["path"],
                "source_sha256": receipt["source"]["sha256"],
                "source_bytes": Path(receipt["source"]["path"]).stat().st_size,
                "source_rows": int(output["rows"]),
                "source_total_rows": int(output["rows"]),
                "output_path": output["path"],
                "output_sha256": output["sha256"],
                "output_bytes": int(output["bytes"]),
                "rows": int(output["rows"]),
                "fields": fields,
                "split_manifest_hash": source_manifest["split_manifest_hash"],
                "stable_key": list(STABLE_KEY),
                "source_stable_key_digest": receipt["source_stable_key_digest"],
                "source_payload_digest": receipt["source_payload_digest"],
                "materialization_receipt_path": receipt["receipt_path"],
                "materialization_receipt_sha256": receipt["receipt_file_sha256"],
                "status": "TIME_MAJOR_SHARD_READY",
            }
        )
        parity.append(
            {
                "schema_version": "cn_program_materialized_sidecar_parity_v1",
                "status": "SIDECAR_PARITY_PASS",
                "source_shard": int(receipt["shard_index"]),
                "source_rows": int(output["rows"]),
                "sidecar_rows": int(output["rows"]),
                "stable_key_digest_equal": True,
                "source_payload_columns_equal": True,
                "program_required_fields_present": True,
            }
        )
    if total_rows != int(source_manifest["sidecar_rows"]):
        raise RuntimeError("program materialization root row-count drift")

    manifest = _self_hashed(
        {
            "schema_version": V2_SCHEMA,
            "status": V2_STATUS,
            "data_role": "development_train_only",
            "fields": fields,
            "source_shard_count": len(shards),
            "source_rows": total_rows,
            "source_total_rows": total_rows,
            "sidecar_rows": total_rows,
            "sidecar_bytes": total_bytes,
            "split_manifest_hash": source_manifest["split_manifest_hash"],
            "eligible_train_date_count": source_manifest.get("eligible_train_date_count"),
            "shards": manifest_shards,
            "parity": parity,
            "source_manifest": {
                "path": str(source_manifest_path),
                "file_sha256": _sha256(source_manifest_path),
                "payload_sha256": source_manifest["manifest_hash"],
            },
            "bar_source_manifest": {
                "path": str(bar_source_manifest_path),
                "file_sha256": _sha256(bar_source_manifest_path),
            },
            "program_materialization_plan": {
                "path": str(plan_path),
                "file_sha256": _sha256(plan_path),
                "payload_sha256": plan["plan_sha256"],
            },
            "program_information_coverage": {
                "path": str(information_coverage_path),
                "file_sha256": _sha256(information_coverage_path),
                "payload_sha256": information_coverage[
                    "information_coverage_sha256"
                ],
                "information_metrics_authority_file_sha256": _sha256(
                    information_metrics_path
                ),
            },
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
        },
        "manifest_hash",
    )
    manifest_path = _write_json(output_root / SOURCE_MANIFEST_NAME, manifest)

    fixtures = _template_fixtures(
        records=records,
        registry=registry,
        sidecar_paths=[Path(row["output_path"]) for row in manifest_shards],
    )
    if int(fixtures["template_count"]) != len(Counter(record["template_id"] for record in records)):
        raise RuntimeError("program materialization fixture template coverage drift")
    fixture_path = _write_json(output_root / FIXTURE_NAME, fixtures)

    artifact_paths = [
        plan_path,
        information_coverage_path,
        manifest_path,
        fixture_path,
        *[Path(row["receipt_path"]) for row in receipts],
        *[Path(row["output"]["path"]) for row in receipts],
    ]
    artifacts = [
        {
            "path": str(path),
            "sha256": _sha256(path),
            "bytes": path.stat().st_size,
        }
        for path in artifact_paths
    ]
    artifact_manifest = _self_hashed(
        {
            "schema_version": "cn_program_materialization_artifact_manifest_v1",
            "status": "PROGRAM_MATERIALIZATION_ARTIFACTS_HASH_BOUND",
            "artifact_count": len(artifacts),
            "artifacts": artifacts,
        },
        "manifest_sha256",
    )
    artifact_manifest_path = _write_json(output_root / ARTIFACT_MANIFEST_NAME, artifact_manifest)

    actual_program_covered = required & set(manifest["fields"])
    added = set(plan["missing_required_field_ids"])
    output_added = set(manifest["fields"]) - available_fields
    if required != set(plan["materializable_required_field_ids"]) or required != actual_program_covered:
        raise RuntimeError("required/materializable/sidecar-covered program fields disagree")
    if output_added != added:
        raise RuntimeError("program materialization added unexpected unbound fields")
    closure = _self_hashed(
        {
            "schema_version": "cn_program_materialization_preflight_v1",
            "status": STATUS,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "schedule": {
                "path": str(Path(schedule_path).resolve()),
                "sha256": _sha256(schedule_path),
                "record_count": len(records),
                "template_counts": dict(sorted(Counter(record["template_id"] for record in records).items())),
            },
            "registry": {
                "path": str(Path(registry_path).resolve()),
                "sha256": _sha256(registry_path),
                "registry_hash": registry.registry_hash,
            },
            "source_manifest": manifest["source_manifest"],
            "bar_source_manifest": manifest["bar_source_manifest"],
            "output_manifest": {
                "path": str(manifest_path),
                "file_sha256": _sha256(manifest_path),
                "payload_sha256": manifest["manifest_hash"],
            },
            "materialization_plan": manifest["program_materialization_plan"],
            "information_coverage": manifest["program_information_coverage"],
            "all_required_fields_information_qualified": True,
            "template_fixtures": {
                "path": str(fixture_path),
                "file_sha256": _sha256(fixture_path),
                "payload_sha256": fixtures["fixture_sha256"],
            },
            "artifact_manifest": {
                "path": str(artifact_manifest_path),
                "file_sha256": _sha256(artifact_manifest_path),
                "payload_sha256": artifact_manifest["manifest_sha256"],
                "artifact_count": artifact_manifest["artifact_count"],
            },
            "required_physical_leaf_ids": sorted(required),
            "materializable_required_field_ids": sorted(plan["materializable_required_field_ids"]),
            "program_covered_field_ids": sorted(actual_program_covered),
            "added_field_ids": sorted(added),
            "unexpected_unbound_added_fields": [],
            "wrong_scope_fields": [],
            "wrong_clock_fields": [],
            "lag_applied_exactly_once": True,
            "required_equals_materializable_equals_program_covered": True,
            "financial_evaluation_executed": False,
            "optimizer_feedback_write": False,
            "scheduler_write": False,
            "archive_write": False,
            "promotion_write": False,
            "validation_reads": 0,
            "holdout_reads": 0,
            "historical_2023_reads": 0,
            "forward_b_reads": 0,
            "forward_2026_reads": 0,
        },
        "closure_sha256",
    )
    closure_path = _write_json(output_root / CLOSURE_NAME, closure)
    return {**closure, "closure_path": str(closure_path), "closure_file_sha256": _sha256(closure_path)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schedule", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--information-metrics", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--bar-source-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--minimum-free-memory-bytes", type=int, default=24 * 1024**3)
    parser.add_argument("--expected-records", type=int, default=64)
    parser.add_argument("--expected-template-quota", type=int, default=8)
    args = parser.parse_args()
    result = prepare(
        schedule_path=args.schedule,
        registry_path=args.registry,
        information_metrics_path=args.information_metrics,
        source_root=args.source_root,
        bar_source_root=args.bar_source_root,
        output_root=args.output_root,
        workers=args.workers,
        minimum_free_memory_bytes=args.minimum_free_memory_bytes,
        expected_records=args.expected_records,
        expected_template_quota=args.expected_template_quota,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
