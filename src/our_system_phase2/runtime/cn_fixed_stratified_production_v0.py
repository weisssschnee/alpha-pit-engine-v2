"""Evaluate one fixed eight-stratum V0 candidate cohort on train data only."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import platform
import re
import subprocess
import time
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import psutil

from our_system_phase2.runtime.cn_candidate_representation_v0_preflight import (
    CLOSURE_NAME as PREFLIGHT_CLOSURE_NAME,
    verify_candidate_representation_v0_preflight,
)
from our_system_phase2.runtime.cn_iterative_search_v1 import (
    _context_and_binding,
    _outcome_rows,
)
from our_system_phase2.runtime.cn_large_tpe_search_campaign import (
    _is_productive_candidate,
)
from our_system_phase2.runtime.cn_targeted_search_medium_campaign import (
    _run_phase3cm_monitored,
    _runtime_gate,
    materialized_schema_binding,
)
from our_system_phase2.services.fixed_split_authority import (
    FixedSplitAuthority,
)
from our_system_phase2.services.node_resource_governor import (
    validate_node_resource_lease_receipt,
)
from our_system_phase2.services.split_boundary_label_purity import (
    audit_split_boundary_label_purity,
)
from our_system_phase2.services.unified_capability_registry import (
    ROUTE_IDS,
    UnifiedCapabilityRegistry,
    stable_hash,
)


AUTHORIZED_HOST = "DESKTOP-77OPJ6F"
REPO = Path(__file__).resolve().parents[3]
CLOSURE_NAME = "FIXED_STRATIFIED_PRODUCTION_V0_COMPLETE.json"
MINIMUM_FREE_MEMORY_BYTES = 24 * 1024**3
CACHE_CAP_BYTES = 8 * 1024**3
PAIR_BATCH_SIZE_BY_BACKEND = {"active_bar": 12, "stock_session": 24}
ACTIVE_BAR_ROUTES = {
    "MINUTE_STATIC",
    "FIRSTN_PATH",
    "MARKET_REGIME_CONDITION",
    "INTRADAY_STATE_TRANSITION",
}
PROHIBITED_READ_KEYS = {
    "validation_reads",
    "validation_read_count",
    "holdout_reads",
    "holdout_read_count",
    "forward_2026_reads",
    "forward_2026_read_count",
    "sealed_data_reads",
    "sealed_data_read_count",
}
STATE_WRITE_POLICY = {
    "feedback_write": "TRAIN_ONLY_EVALUATOR_OUTPUT_NOT_CONSUMED",
    "scheduler_write": "FORBIDDEN",
    "archive_write": "RUN_LOCAL_EVIDENCE_ONLY",
    "optimizer_write": "FORBIDDEN",
    "promotion": "FORBIDDEN",
    "cross_sprint_memory": "FORBIDDEN",
}
SUPPLY_METRIC_KEYS = (
    "attempted",
    "primary_legal",
    "control_valid",
    "legal_control_valid",
    "legal_or_control_failed",
    "duplicate",
    "generation_failed",
    "pair_unique",
    "candidate_specs",
    "sampling_kind",
    "template_version",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path: Path, payload: Any) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
        newline="\n",
    )
    temporary.replace(destination)
    return destination


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> Path:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(
                json.dumps(
                    dict(row),
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                )
                + "\n"
            )
    return destination


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    with Path(path).open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def _clock_for_route(route_id: str) -> str:
    return "active_bar" if route_id in ACTIVE_BAR_ROUTES else "stock_session"


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _require_execution_authority(
    *, output_root: Path, compute_threads: int
) -> dict[str, Any]:
    if platform.node().upper() != AUTHORIZED_HOST:
        raise RuntimeError(
            f"train production is authorized only on {AUTHORIZED_HOST}"
        )
    repo_sha = str(os.environ.get("CN_CAMPAIGN_REPO_SHA") or "").lower()
    if re.fullmatch(r"[0-9a-f]{40}", repo_sha) is None:
        raise RuntimeError("fixed-stratified production repo SHA missing")
    observed_sha = subprocess.run(
        ["git", "-C", str(REPO), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip().lower()
    if observed_sha != repo_sha:
        raise RuntimeError("fixed-stratified production repo SHA drift")
    if str(os.environ.get("CN_NODE_RESOURCE_LEASE_REQUIRED") or "") != "1":
        raise RuntimeError("fixed-stratified production lease is required")
    receipt_value = str(
        os.environ.get("CN_NODE_RESOURCE_LEASE_RECEIPT") or ""
    )
    if not receipt_value:
        raise RuntimeError("fixed-stratified production lease receipt missing")
    receipt_path = Path(receipt_value).resolve()
    receipt = validate_node_resource_lease_receipt(
        receipt_path,
        expected_role="VALIDATION",
        expected_cpu_threads=int(compute_threads),
    )
    lease = dict(receipt.get("lease") or {})
    if (
        str(lease.get("profile_id") or "") != "VALIDATION_EXCLUSIVE_32"
        or Path(str(lease.get("workload_id") or "")).resolve()
        != Path(output_root).resolve()
        or int(os.environ.get("CN_NODE_CPU_ENTITLEMENT") or 0)
        != int(compute_threads)
    ):
        raise RuntimeError("fixed-stratified production lease binding drift")
    receipt_snapshot_path = _write_json(
        Path(output_root) / "node_resource_lease_receipt.json", receipt
    )
    return {
        "status": "NODE_RESOURCE_LEASE_BOUND",
        "receipt_path": str(receipt_snapshot_path),
        "source_receipt_path": str(receipt_path),
        "receipt_file_sha256": _sha256(receipt_snapshot_path),
        "receipt_payload_sha256": str(receipt.get("receipt_sha256") or ""),
        "profile_id": str(lease.get("profile_id") or ""),
        "cpu_entitlement_threads": int(lease.get("cpu_threads") or 0),
        "capacity_manifest_sha256": str(
            receipt.get("capacity_manifest_sha256") or ""
        ),
    }


def require_fresh_output_root_v0(output_root: Path) -> None:
    root = Path(output_root).resolve()
    if not root.is_dir() or {
        path.name for path in root.iterdir()
    } != {"deployment_binding.json"}:
        raise FileExistsError("fixed-stratified production root is not fresh")


def _require_sidecar_authority(
    *,
    closure_path: Path,
    split_manifest: Path,
    active_field_root: Path,
    session_field_root: Path,
) -> dict[str, Any]:
    payload = json.loads(Path(closure_path).read_text(encoding="utf-8"))
    if (
        str(payload.get("schema_version") or "")
        != "cn_phase3cm_1024_sidecar_closure_v1"
        or str(payload.get("status") or "")
        != "CN_PHASE3CM_1024_SIDECAR_CLOSURE_PASS"
        or str(payload.get("data_role") or "") != "development_train_only"
    ):
        raise RuntimeError("fixed-stratified sidecar closure authority drift")
    body = dict(payload)
    claimed_hash = str(body.pop("closure_hash", ""))
    if not claimed_hash or claimed_hash != stable_hash(body):
        raise RuntimeError("fixed-stratified sidecar closure self-hash drift")
    if (
        Path(str((payload.get("active_sidecar") or {}).get("root") or ""))
        .resolve()
        != Path(active_field_root).resolve()
        or Path(
            str((payload.get("session_sidecar") or {}).get("root") or "")
        ).resolve()
        != Path(session_field_root).resolve()
    ):
        raise RuntimeError("fixed-stratified sidecar root binding drift")
    split_binding = dict(payload.get("split_manifest") or {})
    if (
        Path(str(split_binding.get("path") or "")).resolve()
        != Path(split_manifest).resolve()
        or str(split_binding.get("sha256") or "")
        != _sha256(Path(split_manifest).resolve())
    ):
        raise RuntimeError("fixed-stratified sidecar split binding drift")
    require_zero_prohibited_reads_v0(payload)
    return payload


def _declared_shard_rows(payload: Any, root: Path) -> dict[Path, dict[str, Any]]:
    rows: dict[Path, dict[str, Any]] = {}
    if isinstance(payload, Mapping):
        path_value = payload.get("output_path") or payload.get("path")
        hash_value = payload.get("output_sha256") or payload.get("sha256")
        if path_value and hash_value:
            candidate = Path(str(path_value)).resolve()
            if candidate.is_relative_to(root) and candidate.name.startswith(
                "shard_"
            ) and candidate.suffix.lower() == ".parquet":
                rows[candidate] = {
                    "declared_sha256": str(hash_value),
                    "declared_bytes": (
                        int(payload.get("output_bytes"))
                        if payload.get("output_bytes") is not None
                        else None
                    ),
                    "declared_rows": (
                        int(payload.get("rows"))
                        if payload.get("rows") is not None
                        else None
                    ),
                }
        for value in payload.values():
            rows.update(_declared_shard_rows(value, root))
    elif isinstance(payload, (list, tuple)):
        for value in payload:
            rows.update(_declared_shard_rows(value, root))
    return rows


def _manifest_inventory(
    *, path: Path, root: Path, expected_file_sha256: str | None = None
) -> tuple[dict[str, Any], dict[Path, dict[str, Any]]]:
    manifest_path = Path(path).resolve()
    root = Path(root).resolve()
    if not manifest_path.is_file() or not manifest_path.is_relative_to(root):
        raise RuntimeError("fixed-stratified data manifest path drift")
    file_sha256 = _sha256(manifest_path)
    if expected_file_sha256 and file_sha256 != expected_file_sha256:
        raise RuntimeError("fixed-stratified data manifest hash drift")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    require_zero_prohibited_reads_v0(payload)
    return (
        {
            "path": str(manifest_path),
            "file_sha256": file_sha256,
            "schema_version": str(payload.get("schema_version") or ""),
            "status": str(payload.get("status") or ""),
        },
        _declared_shard_rows(payload, root),
    )


def build_data_input_inventory_v0(
    *,
    sidecar_authority: Mapping[str, Any],
    split_manifest: Path,
    field_roots: Mapping[str, Path],
    label_roots: Mapping[str, Path],
) -> dict[str, Any]:
    """Bind the exact manifests and declared shard identities consumed."""

    split_sha256 = _sha256(Path(split_manifest).resolve())
    manifest_specs = {
        "active_field": (
            Path(str((sidecar_authority.get("active_sidecar") or {})["manifest_path"])),
            Path(field_roots["active_bar"]),
            str((sidecar_authority.get("active_sidecar") or {})["manifest_sha256"]),
        ),
        "session_field_augmentation": (
            Path(
                str(
                    (sidecar_authority.get("session_sidecar") or {})[
                        "augmentation_manifest_path"
                    ]
                )
            ),
            Path(field_roots["stock_session"]),
            str(
                (sidecar_authority.get("session_sidecar") or {})[
                    "augmentation_manifest_sha256"
                ]
            ),
        ),
        "session_field_v2": (
            Path(
                str(
                    (sidecar_authority.get("session_sidecar") or {})[
                        "v2_manifest_path"
                    ]
                )
            ),
            Path(field_roots["stock_session"]),
            str(
                (sidecar_authority.get("session_sidecar") or {})[
                    "v2_manifest_sha256"
                ]
            ),
        ),
        "active_label": (
            Path(label_roots["active_bar"])
            / "CN_FORWARD_LABEL_SIDECAR_MANIFEST.json",
            Path(label_roots["active_bar"]),
            None,
        ),
        "session_label": (
            Path(label_roots["stock_session"])
            / "CN_FORWARD_LABEL_SIDECAR_MANIFEST.json",
            Path(label_roots["stock_session"]),
            None,
        ),
    }
    manifests: dict[str, Any] = {}
    declarations_by_root: dict[Path, dict[Path, dict[str, Any]]] = {}
    for name, (manifest_path, root, expected_hash) in manifest_specs.items():
        record, declarations = _manifest_inventory(
            path=manifest_path,
            root=root,
            expected_file_sha256=expected_hash,
        )
        manifests[name] = record
        resolved_root = Path(root).resolve()
        declarations_by_root.setdefault(resolved_root, {}).update(declarations)
        if name.endswith("_label"):
            payload = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
            if (
                str(payload.get("data_role") or "")
                != "development_train_only"
                or str(payload.get("split_manifest_hash") or "")
                != split_sha256
                or "READY" not in str(payload.get("status") or "")
            ):
                raise RuntimeError("fixed-stratified label authority drift")
    shard_inventory: dict[str, Any] = {}
    all_roots = {
        "active_field": Path(field_roots["active_bar"]).resolve(),
        "session_field": Path(field_roots["stock_session"]).resolve(),
        "active_label": Path(label_roots["active_bar"]).resolve(),
        "session_label": Path(label_roots["stock_session"]).resolve(),
    }
    for name, root in all_roots.items():
        shards = tuple(sorted(root.glob("shard_*.parquet")))
        declarations = declarations_by_root.get(root, {})
        if not shards or set(shards) != set(declarations):
            raise RuntimeError(
                f"fixed-stratified shard inventory drift: {name}"
            )
        rows = []
        for shard in shards:
            declared = declarations[shard]
            actual_bytes = shard.stat().st_size
            observed_sha256 = _sha256(shard)
            if (
                declared["declared_bytes"] is not None
                and int(declared["declared_bytes"]) != actual_bytes
            ):
                raise RuntimeError(
                    f"fixed-stratified shard size drift: {shard}"
                )
            if re.fullmatch(
                r"[0-9a-f]{64}", str(declared["declared_sha256"])
            ) is None:
                raise RuntimeError(
                    f"fixed-stratified shard hash authority drift: {shard}"
                )
            if observed_sha256 != str(declared["declared_sha256"]):
                raise RuntimeError(
                    f"fixed-stratified shard hash drift: {shard}"
                )
            rows.append(
                {
                    "path": str(shard),
                    "bytes": actual_bytes,
                    "observed_sha256": observed_sha256,
                    **declared,
                }
            )
        shard_inventory[name] = rows
    payload = {
        "schema_version": "cn_fixed_stratified_data_input_inventory_v0",
        "status": "DEVELOPMENT_TRAIN_INPUTS_BOUND",
        "split_manifest_path": str(Path(split_manifest).resolve()),
        "split_manifest_file_sha256": split_sha256,
        "manifests": manifests,
        "shards": shard_inventory,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    payload["inventory_payload_sha256"] = stable_hash(payload)
    return payload


def build_route_production_metrics_v0(
    *,
    route_id: str,
    scheduled_attempts: int,
    unique_pairs: int,
    outcomes: Sequence[Mapping[str, Any]],
    behavior_rows: Sequence[Mapping[str, Any]],
    wall_seconds: float,
    compute_threads: int,
) -> dict[str, Any]:
    """Build observable production rates without inventing a composite score."""

    evaluated = [
        row
        for row in outcomes
        if str(row.get("pair_evaluation_status") or "") == "PAIR_EVALUATED"
    ]
    standalone_positive = sum(
        (_finite(row.get("primary_composite_reward")) or 0.0) > 0.0
        for row in evaluated
    )
    matched_positive = 0
    for row in evaluated:
        matched_increment = _finite(row.get("matched_train_increment"))
        if matched_increment is None:
            matched_increment = _finite(row.get("pair_train_reward"))
        if matched_increment is not None and matched_increment > 0.0:
            matched_positive += 1
    productive = sum(_is_productive_candidate(row) for row in evaluated)
    evaluated_pair_ids = {
        str(row.get("pair_id") or "") for row in evaluated
    }
    behavior_families = {
        str(row.get("portfolio_behavior_family_id") or "")
        for row in behavior_rows
        if str(row.get("pair_id") or "") in evaluated_pair_ids
        and str(row.get("portfolio_behavior_family_id") or "")
    }
    wall_hours = max(float(wall_seconds) / 3600.0, 1e-12)
    return {
        "route_id": route_id,
        "template_id": route_id,
        "backend": _clock_for_route(route_id),
        "scheduled_attempts": int(scheduled_attempts),
        "primary_exact_unique": int(unique_pairs),
        "supply_underfill": int(scheduled_attempts) - int(unique_pairs),
        "production_evidence_state": "TRAIN_ONLY_EVALUATED",
        "pair_evaluated": len(evaluated),
        "evaluator_fill_ratio": len(evaluated) / max(1, int(unique_pairs)),
        "standalone_positive": int(standalone_positive),
        "matched_positive": int(matched_positive),
        "development_productive": int(productive),
        "behavior_family_unique": len(behavior_families),
        "wall_seconds": float(wall_seconds),
        "compute_threads": int(compute_threads),
        "pair_evaluated_per_wall_hour": len(evaluated) / wall_hours,
        "candidate_member_evaluated_per_wall_hour": (
            2 * len(evaluated) / wall_hours
        ),
        "productive_per_entitled_core_hour": (
            productive / max(wall_hours * int(compute_threads), 1e-12)
        ),
    }


def require_zero_prohibited_reads_v0(payload: Any) -> None:
    """Fail closed if any nested evidence records a sealed-role read."""

    if isinstance(payload, Mapping):
        for key, value in payload.items():
            if str(key) in PROHIBITED_READ_KEYS and int(value or 0) != 0:
                raise RuntimeError(
                    f"fixed-stratified production prohibited read: {key}={value}"
                )
            require_zero_prohibited_reads_v0(value)
    elif isinstance(payload, (list, tuple)):
        for value in payload:
            require_zero_prohibited_reads_v0(value)


def _forbid_state_writes(binding_path: Path) -> dict[str, Any]:
    binding = json.loads(Path(binding_path).read_text(encoding="utf-8"))
    binding.update(STATE_WRITE_POLICY)
    binding.pop("binding_hash", None)
    binding["binding_hash"] = stable_hash(binding)
    _write_json(binding_path, binding)
    return binding


def _artifact(path: Path, *, root: Path) -> dict[str, Any]:
    source = Path(path).resolve()
    return {
        "path": source.relative_to(root).as_posix(),
        "sha256": _sha256(source),
        "bytes": source.stat().st_size,
    }


def _read_bound_json(
    root: Path, artifacts: Mapping[str, Mapping[str, Any]], relative_path: str
) -> dict[str, Any]:
    if relative_path not in artifacts:
        raise RuntimeError(
            f"fixed-stratified production artifact undeclared: {relative_path}"
        )
    return json.loads((root / relative_path).read_text(encoding="utf-8-sig"))


def verify_fixed_stratified_production_v0(output_root: Path) -> dict[str, Any]:
    root = Path(output_root).resolve()
    closure_path = root / CLOSURE_NAME
    closure = json.loads(closure_path.read_text(encoding="utf-8"))
    payload = {
        key: value
        for key, value in closure.items()
        if key != "closure_payload_sha256"
    }
    if str(closure.get("closure_payload_sha256") or "") != stable_hash(
        payload
    ):
        raise RuntimeError("fixed-stratified production closure hash mismatch")
    if str(closure.get("status") or "") != (
        "FIXED_STRATIFIED_PRODUCTION_V0_COMPLETE"
    ):
        raise RuntimeError("fixed-stratified production status mismatch")
    artifacts = list(closure.get("artifacts") or ())
    if len(artifacts) != int(closure.get("artifact_count") or -1):
        raise RuntimeError("fixed-stratified production artifact count mismatch")
    artifact_by_path: dict[str, Mapping[str, Any]] = {}
    for artifact in artifacts:
        target = (root / str(artifact.get("path") or "")).resolve()
        if not target.is_relative_to(root) or not target.is_file():
            raise RuntimeError("fixed-stratified production artifact missing")
        if target.stat().st_size != int(artifact.get("bytes") or -1):
            raise RuntimeError("fixed-stratified production artifact size drift")
        if _sha256(target) != str(artifact.get("sha256") or ""):
            raise RuntimeError("fixed-stratified production artifact hash drift")
        relative = target.relative_to(root).as_posix()
        if relative in artifact_by_path:
            raise RuntimeError("fixed-stratified production duplicate artifact")
        artifact_by_path[relative] = artifact
    input_binding = _read_bound_json(
        root, artifact_by_path, "input_binding.json"
    )
    summary = _read_bound_json(
        root, artifact_by_path, "production_summary.json"
    )
    deployment_binding = _read_bound_json(
        root, artifact_by_path, "deployment_binding.json"
    )
    for closure_key, relative_path in (
        ("input_binding", "input_binding.json"),
        ("materialized_schema_binding", "materialized_schema_binding.json"),
        ("split_boundary_purity", "split_boundary_purity.json"),
        ("data_input_inventory", "data_input_inventory.json"),
        ("production_summary", "production_summary.json"),
        ("all_outcomes", "all_outcomes.jsonl"),
        ("all_behavior_rows", "all_behavior_rows.jsonl"),
    ):
        if closure.get(closure_key) != artifact_by_path.get(relative_path):
            raise RuntimeError(
                f"fixed-stratified closure artifact binding drift: {closure_key}"
            )
    purity = _read_bound_json(
        root, artifact_by_path, "split_boundary_purity.json"
    )
    _read_bound_json(root, artifact_by_path, "materialized_schema_binding.json")
    if str(purity.get("status") or "") != "PASS":
        raise RuntimeError("fixed-stratified split purity evidence drift")
    binding_body = dict(input_binding)
    binding_hash = str(binding_body.pop("binding_hash", ""))
    if not binding_hash or binding_hash != stable_hash(binding_body):
        raise RuntimeError("fixed-stratified production input binding drift")
    if (
        str(deployment_binding.get("repo_sha") or "").lower()
        != str(closure.get("repo_sha") or "").lower()
        or str(input_binding.get("repo_sha") or "").lower()
        != str(closure.get("repo_sha") or "").lower()
        or re.fullmatch(
            r"[0-9a-f]{40}", str(closure.get("repo_sha") or "").lower()
        )
        is None
        or str(deployment_binding.get("node_resource_profile") or "")
        != "VALIDATION_EXCLUSIVE_32"
        or int(deployment_binding.get("compute_threads") or 0) != 32
        or int(deployment_binding.get("maximum_wall_seconds") or 0) != 14400
        or float(deployment_binding.get("required_pairs_per_hour") or 0.0)
        != 57.25
        or int(input_binding.get("compute_threads") or 0) != 32
        or int(input_binding.get("maximum_wall_seconds") or 0) != 14400
        or float(
            input_binding.get("required_pair_evaluated_per_wall_hour") or 0.0
        )
        != 57.25
    ):
        raise RuntimeError("fixed-stratified deployment evidence drift")
    node_binding = dict(input_binding.get("node_resource_binding") or {})
    if (
        str(node_binding.get("status") or "") != "NODE_RESOURCE_LEASE_BOUND"
        or str(node_binding.get("profile_id") or "")
        != "VALIDATION_EXCLUSIVE_32"
        or int(node_binding.get("cpu_entitlement_threads") or 0) != 32
    ):
        raise RuntimeError("fixed-stratified resource evidence drift")
    receipt_path = Path(str(node_binding.get("receipt_path") or "")).resolve()
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt_body = dict(receipt)
    receipt_hash = str(receipt_body.pop("receipt_sha256", ""))
    receipt_lease = dict(receipt.get("lease") or {})
    if (
        receipt_hash != stable_hash(receipt_body)
        or receipt_hash
        != str(node_binding.get("receipt_payload_sha256") or "")
        or _sha256(receipt_path)
        != str(node_binding.get("receipt_file_sha256") or "")
        or str(receipt.get("capacity_manifest_sha256") or "")
        != str(node_binding.get("capacity_manifest_sha256") or "")
        or str(receipt_lease.get("role") or "") != "VALIDATION"
        or str(receipt_lease.get("profile_id") or "")
        != "VALIDATION_EXCLUSIVE_32"
        or int(receipt_lease.get("cpu_threads") or 0) != 32
        or Path(str(receipt_lease.get("workload_id") or "")).resolve()
        != root
    ):
        raise RuntimeError("fixed-stratified lease receipt drift")
    preflight_root = Path(str(input_binding.get("preflight_root") or "")).resolve()
    preflight = verify_candidate_representation_v0_preflight(preflight_root)
    if (
        _sha256(preflight_root / PREFLIGHT_CLOSURE_NAME)
        != str(input_binding.get("preflight_closure_file_sha256") or "")
        or str(preflight.get("closure_payload_sha256") or "")
        != str(input_binding.get("preflight_closure_payload_sha256") or "")
    ):
        raise RuntimeError("fixed-stratified preflight binding drift")
    registry_path = Path(str(input_binding.get("registry_path") or "")).resolve()
    registry = UnifiedCapabilityRegistry.read(registry_path)
    if (
        _sha256(registry_path)
        != str(input_binding.get("registry_file_sha256") or "")
        or registry.registry_hash
        != str(input_binding.get("registry_hash") or "")
        or registry.registry_hash
        != str(preflight.get("registry_payload_hash") or "")
    ):
        raise RuntimeError("fixed-stratified registry binding drift")
    split_path = Path(
        str(input_binding.get("split_manifest_path") or "")
    ).resolve()
    if _sha256(split_path) != str(
        input_binding.get("split_manifest_file_sha256") or ""
    ):
        raise RuntimeError("fixed-stratified split binding drift")
    field_roots = dict(input_binding.get("field_roots") or {})
    sidecar_authority = _require_sidecar_authority(
        closure_path=Path(
            str(input_binding.get("sidecar_closure_path") or "")
        ).resolve(),
        split_manifest=split_path,
        active_field_root=Path(str(field_roots.get("active_bar") or "")),
        session_field_root=Path(
            str(field_roots.get("stock_session") or "")
        ),
    )
    label_roots = dict(input_binding.get("label_roots") or {})
    observed_inventory = _read_bound_json(
        root, artifact_by_path, "data_input_inventory.json"
    )
    inventory_body = dict(observed_inventory)
    inventory_hash = str(inventory_body.pop("inventory_payload_sha256", ""))
    if (
        inventory_hash != stable_hash(inventory_body)
        or inventory_hash
        != str(input_binding.get("data_input_inventory_payload_sha256") or "")
        or _sha256(root / "data_input_inventory.json")
        != str(input_binding.get("data_input_inventory_file_sha256") or "")
        or observed_inventory
        != build_data_input_inventory_v0(
            sidecar_authority=sidecar_authority,
            split_manifest=split_path,
            field_roots={key: Path(str(value)) for key, value in field_roots.items()},
            label_roots={key: Path(str(value)) for key, value in label_roots.items()},
        )
    ):
        raise RuntimeError("fixed-stratified data input inventory drift")
    if (
        str(summary.get("status") or "")
        != "TRAIN_ONLY_PRODUCTION_EVIDENCE_READY"
        or not bool(summary.get("throughput_floor_met"))
        or int(summary.get("minimum_free_memory_bytes") or 0)
        < MINIMUM_FREE_MEMORY_BYTES
        or int(closure.get("minimum_free_memory_bytes") or 0)
        != int(summary.get("minimum_free_memory_bytes") or 0)
        or float(closure.get("pair_evaluated_per_wall_hour") or 0.0)
        != float(summary.get("pair_evaluated_per_wall_hour") or 0.0)
        or not bool(closure.get("throughput_floor_met"))
        or float(closure.get("required_pair_evaluated_per_wall_hour") or 0.0)
        != float(summary.get("required_pair_evaluated_per_wall_hour") or 0.0)
    ):
        raise RuntimeError("fixed-stratified production summary gate drift")
    expected_pair_rate = int(summary.get("pair_evaluated") or 0) / max(
        float(summary.get("wall_seconds") or 0.0) / 3600.0, 1e-12
    )
    if expected_pair_rate != float(
        summary.get("pair_evaluated_per_wall_hour") or 0.0
    ):
        raise RuntimeError("fixed-stratified production throughput drift")
    waterfall = list(closure.get("template_waterfall") or ())
    if [str(row.get("route_id") or "") for row in waterfall] != list(
        ROUTE_IDS
    ):
        raise RuntimeError("fixed-stratified production route order drift")
    if any(
        str(row.get("production_evidence_state") or "")
        != "TRAIN_ONLY_EVALUATED"
        or int(row.get("pair_evaluated") or 0)
        > int(row.get("primary_exact_unique") or 0)
        for row in waterfall
    ):
        raise RuntimeError("fixed-stratified production waterfall invalid")
    if sum(int(row.get("pair_evaluated") or 0) for row in waterfall) != int(
        closure.get("pair_evaluated") or 0
    ):
        raise RuntimeError("fixed-stratified production evaluated total drift")
    if sum(
        int(row.get("development_productive") or 0) for row in waterfall
    ) != int(closure.get("development_productive") or 0):
        raise RuntimeError("fixed-stratified production productive total drift")
    if waterfall != list(summary.get("template_waterfall") or ()):
        raise RuntimeError("fixed-stratified production waterfall summary drift")
    supply_rows = json.loads(
        (preflight_root / "template_waterfall_v0.json").read_text(
            encoding="utf-8"
        )
    )
    supply_by_route = {str(row["route_id"]): row for row in supply_rows}
    preflight_candidate_rows = _read_jsonl(
        preflight_root / "compatible_candidate_rows_v0.jsonl"
    )
    concatenated_outcomes: list[dict[str, Any]] = []
    concatenated_behavior: list[dict[str, Any]] = []
    for metrics in waterfall:
        route_id = str(metrics["route_id"])
        route_relative = f"routes/{route_id.lower()}"
        persisted_metrics = _read_bound_json(
            root,
            artifact_by_path,
            f"{route_relative}/route_production_metrics.json",
        )
        if persisted_metrics != metrics:
            raise RuntimeError(
                f"fixed-stratified persisted metric drift: {route_id}"
            )
        outcomes_relative = f"{route_relative}/outcomes.jsonl"
        behavior_relative = f"{route_relative}/behavior_rows.jsonl"
        if (
            outcomes_relative not in artifact_by_path
            or behavior_relative not in artifact_by_path
        ):
            raise RuntimeError(
                f"fixed-stratified route evidence undeclared: {route_id}"
            )
        outcomes = _read_jsonl(root / outcomes_relative)
        behavior_rows = _read_jsonl(root / behavior_relative)
        concatenated_outcomes.extend(outcomes)
        concatenated_behavior.extend(behavior_rows)
        recomputed = build_route_production_metrics_v0(
            route_id=route_id,
            scheduled_attempts=int(metrics["scheduled_attempts"]),
            unique_pairs=int(metrics["primary_exact_unique"]),
            outcomes=outcomes,
            behavior_rows=behavior_rows,
            wall_seconds=float(metrics["wall_seconds"]),
            compute_threads=int(metrics["compute_threads"]),
        )
        for key, value in recomputed.items():
            if metrics.get(key) != value:
                raise RuntimeError(
                    f"fixed-stratified route metric drift: {route_id}:{key}"
                )
        supply = supply_by_route[route_id]
        for key in SUPPLY_METRIC_KEYS:
            if metrics.get(key) != supply.get(key):
                raise RuntimeError(
                    f"fixed-stratified supply metric drift: {route_id}:{key}"
                )
        route_binding = _read_bound_json(
            root,
            artifact_by_path,
            f"{route_relative}/phase3cm_input_binding.json",
        )
        route_binding_body = dict(route_binding)
        route_binding_hash = str(route_binding_body.pop("binding_hash", ""))
        if (
            route_binding_hash != stable_hash(route_binding_body)
            or str(route_binding.get("evaluation_role") or "") != "train"
            or any(
                route_binding.get(key) != value
                for key, value in STATE_WRITE_POLICY.items()
            )
        ):
            raise RuntimeError(
                f"fixed-stratified route binding drift: {route_id}"
            )
        expected_rows = [
            row
            for row in preflight_candidate_rows
            if str(row.get("route_id") or "") == route_id
        ]
        expected_members = [
            {
                "pair_id": str(row["pair_id"]),
                "pair_member_role": str(row["pair_member_role"]),
                "candidate_id": str(row["candidate_id"]),
                "route_id": route_id,
                "clock_namespace": _clock_for_route(route_id),
                "expression": str(row["expression"]),
                "canonical_expression": str(row["canonical_expression"]),
            }
            for row in expected_rows
        ]
        observed_members = [
            {key: row.get(key) for key in expected_members[0]}
            for row in list(route_binding.get("candidate_members") or ())
        ]
        expected_pairs = []
        for index in range(0, len(expected_rows), 2):
            primary = expected_rows[index]
            control = expected_rows[index + 1]
            expected_pairs.append(
                {
                    "pair_id": str(primary["pair_id"]),
                    "candidate_id": str(primary["candidate_id"]),
                    "control_candidate_id": str(control["candidate_id"]),
                    "route_id": route_id,
                    "clock_namespace": _clock_for_route(route_id),
                }
            )
        observed_pairs = [
            {key: row.get(key) for key in expected_pairs[0]}
            for row in list(route_binding.get("pairs") or ())
        ]
        if observed_members != expected_members or observed_pairs != expected_pairs:
            raise RuntimeError(
                f"fixed-stratified frozen cohort drift: {route_id}"
            )
        candidate_receipt_relative = (
            f"{route_relative}/candidate_receipts.jsonl"
        )
        pair_receipt_relative = f"{route_relative}/pair_receipts.jsonl"
        if (
            candidate_receipt_relative not in artifact_by_path
            or pair_receipt_relative not in artifact_by_path
        ):
            raise RuntimeError(
                f"fixed-stratified receipt evidence undeclared: {route_id}"
            )
        candidate_receipts = _read_jsonl(root / candidate_receipt_relative)
        pair_receipts = _read_jsonl(root / pair_receipt_relative)
        if [str(row.get("candidate_id") or "") for row in candidate_receipts] != [
            str(row["candidate_id"]) for row in expected_rows
        ] or [str(row.get("pair_id") or "") for row in pair_receipts] != [
            str(row["pair_id"]) for row in expected_pairs
        ]:
            raise RuntimeError(
                f"fixed-stratified candidate receipt order drift: {route_id}"
            )
        receipt_hash_by_id = {
            str(row["candidate_id"]): str(row.get("receipt_hash") or "")
            for row in candidate_receipts
        }
        pair_receipt_hash_by_id = {
            str(row["pair_id"]): str(row.get("pair_receipt_hash") or "")
            for row in pair_receipts
        }
        if any(
            str(row.get("receipt_hash") or "")
            != receipt_hash_by_id.get(str(row.get("candidate_id") or ""), "")
            for row in list(route_binding.get("candidate_members") or ())
        ) or any(
            str(row.get("pair_receipt_hash") or "")
            != pair_receipt_hash_by_id.get(str(row.get("pair_id") or ""), "")
            for row in list(route_binding.get("pairs") or ())
        ):
            raise RuntimeError(
                f"fixed-stratified receipt hash binding drift: {route_id}"
            )
        backend = str(metrics["backend"])
        result = _read_bound_json(
            root,
            artifact_by_path,
            (
                f"{route_relative}/phase3cm/{backend}/"
                "CN_STREAMING_BACKEND_RESULT.json"
            ),
        )
        require_zero_prohibited_reads_v0(result)
        maximum_cache_bytes = max(
            (
                int(row.get("cache_peak_bytes") or 0)
                for row in list(result.get("expression_audits") or ())
            ),
            default=0,
        )
        if (
            str(result.get("status") or "")
            != "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED"
            or str(result.get("evaluation_role") or "") != "train"
            or int(result.get("pair_count") or 0)
            != int(metrics["primary_exact_unique"])
            or maximum_cache_bytes != int(metrics["maximum_evaluator_cache_bytes"])
            or maximum_cache_bytes > CACHE_CAP_BYTES
        ):
            raise RuntimeError(
                f"fixed-stratified backend evidence drift: {route_id}"
            )
        gate = _read_bound_json(
            root, artifact_by_path, f"{route_relative}/runtime_gate.json"
        )
        backend_gate = dict((gate.get("backends") or {}).get(backend) or {})
        if (
            gate != (summary.get("runtime_gates") or {}).get(route_id)
            or str(gate.get("semantic_integrity_status") or "") != "PASS"
            or str(gate.get("resource_headroom_status") or "") != "PASS"
            or float(metrics.get("observed_process_cpu_seconds") or 0.0)
            != float(backend_gate.get("process_cpu_seconds") or 0.0)
            or float(metrics.get("observed_effective_compute_cores") or 0.0)
            != float(backend_gate.get("effective_compute_cores") or 0.0)
        ):
            raise RuntimeError(
                f"fixed-stratified runtime evidence drift: {route_id}"
            )
    if (
        _read_jsonl(root / "all_outcomes.jsonl") != concatenated_outcomes
        or _read_jsonl(root / "all_behavior_rows.jsonl")
        != concatenated_behavior
    ):
        raise RuntimeError("fixed-stratified aggregate row drift")
    require_zero_prohibited_reads_v0(closure)
    require_zero_prohibited_reads_v0(summary)
    require_zero_prohibited_reads_v0(input_binding)
    for policy_payload in (closure, summary, input_binding):
        if any(
            bool(policy_payload.get(key))
            for key in (
                "shared_tpe_study",
                "shared_tpe_credit",
                "optimizer_feedback_accessed",
                "evaluator_train_feedback_outputs_consumed",
                "scheduler_state_write",
                "cross_campaign_archive_write",
                "dynamic_budget_reallocation_allowed",
                "underfill_spillover_allowed",
                "promotion_authorized",
            )
        ):
            raise RuntimeError("fixed-stratified production adaptation drift")
    return closure


def run(args: argparse.Namespace) -> dict[str, Any]:
    root = Path(args.output_root).resolve()
    require_fresh_output_root_v0(root)
    if (
        int(args.compute_threads) != 32
        or int(args.maximum_wall_seconds) != 14400
        or float(args.required_pairs_per_hour) != 57.25
    ):
        raise RuntimeError("fixed-stratified production qualification drift")
    deployment_binding = json.loads(
        (root / "deployment_binding.json").read_text(encoding="utf-8-sig")
    )
    node_resource_binding = _require_execution_authority(
        output_root=root,
        compute_threads=int(args.compute_threads),
    )
    if (
        str(deployment_binding.get("repo_sha") or "").lower()
        != str(os.environ.get("CN_CAMPAIGN_REPO_SHA") or "").lower()
        or str(deployment_binding.get("node_resource_profile") or "")
        != "VALIDATION_EXCLUSIVE_32"
        or int(deployment_binding.get("compute_threads") or 0)
        != int(args.compute_threads)
        or int(deployment_binding.get("minimum_free_memory_bytes") or 0)
        != MINIMUM_FREE_MEMORY_BYTES
        or int(deployment_binding.get("evaluator_cache_cap_bytes") or 0)
        != CACHE_CAP_BYTES
        or int(deployment_binding.get("maximum_wall_seconds") or 0) != 14400
        or float(deployment_binding.get("required_pairs_per_hour") or 0.0)
        != 57.25
    ):
        raise RuntimeError("fixed-stratified deployment binding drift")
    started = time.monotonic()
    initial_free_memory = int(psutil.virtual_memory().available)
    if initial_free_memory < MINIMUM_FREE_MEMORY_BYTES:
        raise RuntimeError("fixed-stratified production memory gate failed")

    preflight_root = Path(args.preflight_root).resolve()
    preflight = verify_candidate_representation_v0_preflight(preflight_root)
    candidate_rows = _read_jsonl(
        preflight_root / "compatible_candidate_rows_v0.jsonl"
    )
    registry = UnifiedCapabilityRegistry.read(Path(args.registry).resolve())
    if registry.registry_hash != str(
        preflight.get("registry_payload_hash") or ""
    ):
        raise RuntimeError("fixed-stratified preflight registry binding drift")
    split = FixedSplitAuthority.read(Path(args.split_manifest).resolve())
    field_roots = {
        "active_bar": Path(args.active_field_root).resolve(),
        "stock_session": Path(args.session_field_root).resolve(),
    }
    label_roots = {
        "active_bar": Path(args.active_label_root).resolve(),
        "stock_session": Path(args.session_label_root).resolve(),
    }
    sidecar_authority = _require_sidecar_authority(
        closure_path=Path(args.sidecar_closure).resolve(),
        split_manifest=Path(args.split_manifest).resolve(),
        active_field_root=field_roots["active_bar"],
        session_field_root=field_roots["stock_session"],
    )
    data_inventory = build_data_input_inventory_v0(
        sidecar_authority=sidecar_authority,
        split_manifest=Path(args.split_manifest).resolve(),
        field_roots=field_roots,
        label_roots=label_roots,
    )
    data_inventory_path = _write_json(
        root / "data_input_inventory.json", data_inventory
    )
    schema, _ = materialized_schema_binding(
        field_roots=field_roots,
        registry=registry,
    )
    purity = audit_split_boundary_label_purity(
        split=split,
        registry=registry,
        label_roots=label_roots,
    )
    if str(purity.get("status") or "") != "PASS":
        raise RuntimeError("fixed-stratified production split purity failed")
    schema_path = _write_json(root / "materialized_schema_binding.json", schema)
    purity_path = _write_json(root / "split_boundary_purity.json", purity)
    source_binding = {
        "schema_version": "cn_fixed_stratified_production_v0_input_binding",
        "repo_sha": str(os.environ.get("CN_CAMPAIGN_REPO_SHA") or ""),
        "preflight_root": str(preflight_root),
        "preflight_closure_file_sha256": _sha256(
            preflight_root / PREFLIGHT_CLOSURE_NAME
        ),
        "preflight_closure_payload_sha256": preflight[
            "closure_payload_sha256"
        ],
        "registry_path": str(Path(args.registry).resolve()),
        "registry_file_sha256": _sha256(Path(args.registry).resolve()),
        "registry_hash": registry.registry_hash,
        "split_manifest_path": str(Path(args.split_manifest).resolve()),
        "split_manifest_file_sha256": _sha256(
            Path(args.split_manifest).resolve()
        ),
        "split_manifest_hash": split.manifest_hash,
        "sidecar_closure_path": str(Path(args.sidecar_closure).resolve()),
        "sidecar_closure_sha256": _sha256(
            Path(args.sidecar_closure).resolve()
        ),
        "sidecar_closure_hash": str(sidecar_authority["closure_hash"]),
        "data_input_inventory_file_sha256": _sha256(data_inventory_path),
        "data_input_inventory_payload_sha256": data_inventory[
            "inventory_payload_sha256"
        ],
        "node_resource_binding": node_resource_binding,
        "field_roots": {key: str(value) for key, value in field_roots.items()},
        "label_roots": {key: str(value) for key, value in label_roots.items()},
        "candidate_member_count": len(candidate_rows),
        "pair_count": len({str(row["pair_id"]) for row in candidate_rows}),
        "route_order": list(ROUTE_IDS),
        "compute_threads": int(args.compute_threads),
        "maximum_wall_seconds": int(args.maximum_wall_seconds),
        "required_pair_evaluated_per_wall_hour": float(
            args.required_pairs_per_hour
        ),
        "pair_batch_size_by_backend": PAIR_BATCH_SIZE_BY_BACKEND,
        "minimum_free_memory_bytes": MINIMUM_FREE_MEMORY_BYTES,
        "cache_cap_bytes": CACHE_CAP_BYTES,
        "shared_tpe_credit": False,
        "optimizer_feedback_accessed": False,
        "evaluator_train_feedback_outputs_consumed": False,
        "scheduler_state_write": False,
        "cross_campaign_archive_write": False,
        "dynamic_budget_reallocation_allowed": False,
        "underfill_spillover_allowed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
    }
    source_binding["binding_hash"] = stable_hash(source_binding)
    source_binding_path = _write_json(root / "input_binding.json", source_binding)

    supply_waterfall = json.loads(
        (preflight_root / "template_waterfall_v0.json").read_text(
            encoding="utf-8"
        )
    )
    supply_by_route = {
        str(row["route_id"]): row for row in supply_waterfall
    }
    route_metrics: list[dict[str, Any]] = []
    route_gates: dict[str, Any] = {}
    all_outcomes: list[dict[str, Any]] = []
    all_behavior: list[dict[str, Any]] = []
    deadline = time.time() + int(args.maximum_wall_seconds)
    minimum_free_memory = initial_free_memory

    for route_id in ROUTE_IDS:
        route_rows = [
            row for row in candidate_rows if str(row.get("route_id")) == route_id
        ]
        route_pair_count = len({str(row["pair_id"]) for row in route_rows})
        expected_unique = int(supply_by_route[route_id]["primary_exact_unique"])
        if (
            len(route_rows) != expected_unique * 2
            or route_pair_count != expected_unique
        ):
            raise RuntimeError(f"fixed-stratified route supply drift: {route_id}")
        route_root = root / "routes" / route_id.lower()
        route_started = time.monotonic()
        binding_path, table_paths = _context_and_binding(
            batch_root=route_root,
            candidates=route_rows,
            registry=registry,
            split=split,
            data_release_hash=_sha256(Path(args.sidecar_closure).resolve()),
            evaluation_role="train",
        )
        binding = _forbid_state_writes(binding_path)
        require_zero_prohibited_reads_v0(binding)
        backend = _clock_for_route(route_id)
        receipts = _run_phase3cm_monitored(
            checkpoint_id=f"fixed_v0_{route_id.lower()}",
            checkpoint_root=route_root,
            binding_path=binding_path,
            table_paths=table_paths,
            split_manifest=Path(args.split_manifest).resolve(),
            field_roots=field_roots,
            label_roots=label_roots,
            purity_path=purity_path,
            compute_threads={
                "active_bar": int(args.compute_threads),
                "stock_session": int(args.compute_threads),
            },
            deadline_epoch=deadline,
            selected_backends=(backend,),
            pair_batch_sizes=PAIR_BATCH_SIZE_BY_BACKEND,
        )
        require_zero_prohibited_reads_v0(receipts)
        backend_result_path = (
            route_root
            / "phase3cm"
            / backend
            / "CN_STREAMING_BACKEND_RESULT.json"
        )
        backend_result = json.loads(
            backend_result_path.read_text(encoding="utf-8")
        )
        require_zero_prohibited_reads_v0(backend_result)
        if (
            str(backend_result.get("status") or "")
            != "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED"
            or str(backend_result.get("evaluation_role") or "") != "train"
            or int(backend_result.get("pair_count") or 0) != expected_unique
        ):
            raise RuntimeError(
                f"fixed-stratified backend result authority drift: {route_id}"
            )
        maximum_cache_bytes = max(
            (
                int(row.get("cache_peak_bytes") or 0)
                for row in list(backend_result.get("expression_audits") or ())
            ),
            default=0,
        )
        if maximum_cache_bytes > CACHE_CAP_BYTES:
            raise RuntimeError(
                f"fixed-stratified cache cap failed: {route_id}"
            )
        outcomes, behavior_rows = _outcome_rows(route_root)
        route_wall = time.monotonic() - route_started
        metrics = build_route_production_metrics_v0(
            route_id=route_id,
            scheduled_attempts=int(supply_by_route[route_id]["scheduled"]),
            unique_pairs=expected_unique,
            outcomes=outcomes,
            behavior_rows=behavior_rows,
            wall_seconds=route_wall,
            compute_threads=int(args.compute_threads),
        )
        for key in SUPPLY_METRIC_KEYS:
            metrics[key] = supply_by_route[route_id][key]
        gate = _runtime_gate(
            route_root,
            {
                "active_bar": int(args.compute_threads),
                "stock_session": int(args.compute_threads),
            },
            expected_backends=(backend,),
        )
        if str(gate.get("semantic_integrity_status") or "") != "PASS":
            raise RuntimeError(f"fixed-stratified runtime semantic gate: {route_id}")
        if str(gate.get("resource_headroom_status") or "") != "PASS":
            raise RuntimeError(f"fixed-stratified runtime resource gate: {route_id}")
        runtime_samples_path = (
            route_root / "phase3cm" / backend / "runtime_samples.json"
        )
        if runtime_samples_path.is_file():
            samples = json.loads(runtime_samples_path.read_text(encoding="utf-8"))
            sample_free = [
                int(row.get("available_memory_bytes") or 0)
                for row in samples
                if int(row.get("available_memory_bytes") or 0) > 0
            ]
            if sample_free:
                minimum_free_memory = min(minimum_free_memory, min(sample_free))
        minimum_free_memory = min(
            minimum_free_memory, int(psutil.virtual_memory().available)
        )
        if minimum_free_memory < MINIMUM_FREE_MEMORY_BYTES:
            raise RuntimeError("fixed-stratified production memory gate failed")
        metrics["runtime_gate_evidence_status"] = str(
            gate.get("evidence_status") or ""
        )
        metrics["compute_efficiency_status"] = str(
            gate.get("compute_efficiency_status") or ""
        )
        backend_gate = dict((gate.get("backends") or {}).get(backend) or {})
        observed_cpu_seconds = float(
            backend_gate.get("process_cpu_seconds") or 0.0
        )
        metrics["observed_process_cpu_seconds"] = observed_cpu_seconds
        metrics["observed_effective_compute_cores"] = float(
            backend_gate.get("effective_compute_cores") or 0.0
        )
        metrics["productive_per_observed_cpu_hour"] = (
            int(metrics["development_productive"])
            / max(observed_cpu_seconds / 3600.0, 1e-12)
        )
        metrics["maximum_evaluator_cache_bytes"] = maximum_cache_bytes
        metrics["minimum_free_memory_bytes_so_far"] = minimum_free_memory
        metrics["access_receipts"] = receipts
        _write_json(route_root / "route_production_metrics.json", metrics)
        _write_json(route_root / "runtime_gate.json", gate)
        _write_jsonl(route_root / "outcomes.jsonl", outcomes)
        _write_jsonl(route_root / "behavior_rows.jsonl", behavior_rows)
        route_metrics.append(metrics)
        route_gates[route_id] = gate
        all_outcomes.extend(outcomes)
        all_behavior.extend(behavior_rows)

    total_wall = time.monotonic() - started
    pair_evaluated = sum(int(row["pair_evaluated"]) for row in route_metrics)
    productive = sum(int(row["development_productive"]) for row in route_metrics)
    summary = {
        "schema_version": "cn_fixed_stratified_production_v0_summary",
        "status": "TRAIN_ONLY_PRODUCTION_EVIDENCE_READY",
        "template_waterfall": route_metrics,
        "pair_evaluated": pair_evaluated,
        "development_productive": productive,
        "wall_seconds": total_wall,
        "pair_evaluated_per_wall_hour": (
            pair_evaluated / max(total_wall / 3600.0, 1e-12)
        ),
        "required_pair_evaluated_per_wall_hour": float(
            args.required_pairs_per_hour
        ),
        "throughput_floor_met": (
            pair_evaluated / max(total_wall / 3600.0, 1e-12)
            >= float(args.required_pairs_per_hour)
        ),
        "minimum_free_memory_bytes": minimum_free_memory,
        "minimum_required_free_memory_bytes": MINIMUM_FREE_MEMORY_BYTES,
        "runtime_gates": route_gates,
        "compute_efficiency_gate_role": "DIAGNOSTIC_PER_SMALL_STRATUM",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "shared_tpe_credit": False,
        "optimizer_feedback_accessed": False,
        "evaluator_train_feedback_outputs_consumed": False,
        "scheduler_state_write": False,
        "cross_campaign_archive_write": False,
        "dynamic_budget_reallocation_allowed": False,
        "underfill_spillover_allowed": False,
        "promotion_authorized": False,
    }
    require_zero_prohibited_reads_v0(summary)
    summary_path = _write_json(root / "production_summary.json", summary)
    outcomes_path = _write_jsonl(root / "all_outcomes.jsonl", all_outcomes)
    behavior_path = _write_jsonl(root / "all_behavior_rows.jsonl", all_behavior)
    if not bool(summary["throughput_floor_met"]):
        raise RuntimeError("fixed-stratified production throughput gate failed")

    artifact_paths = sorted(
        path
        for path in root.rglob("*")
        if path.is_file()
        and path.name != CLOSURE_NAME
        and not path.name.endswith(".tmp")
    )
    closure: dict[str, Any] = {
        "schema_version": "cn_fixed_stratified_production_v0_closure",
        "status": "FIXED_STRATIFIED_PRODUCTION_V0_COMPLETE",
        "repo_sha": str(os.environ.get("CN_CAMPAIGN_REPO_SHA") or ""),
        "output_root": str(root),
        "input_binding": _artifact(source_binding_path, root=root),
        "materialized_schema_binding": _artifact(schema_path, root=root),
        "split_boundary_purity": _artifact(purity_path, root=root),
        "data_input_inventory": _artifact(data_inventory_path, root=root),
        "production_summary": _artifact(summary_path, root=root),
        "all_outcomes": _artifact(outcomes_path, root=root),
        "all_behavior_rows": _artifact(behavior_path, root=root),
        "template_waterfall": route_metrics,
        "pair_evaluated": pair_evaluated,
        "development_productive": productive,
        "wall_seconds": total_wall,
        "pair_evaluated_per_wall_hour": summary[
            "pair_evaluated_per_wall_hour"
        ],
        "required_pair_evaluated_per_wall_hour": summary[
            "required_pair_evaluated_per_wall_hour"
        ],
        "throughput_floor_met": summary["throughput_floor_met"],
        "minimum_free_memory_bytes": minimum_free_memory,
        "minimum_required_free_memory_bytes": MINIMUM_FREE_MEMORY_BYTES,
        "cache_cap_bytes": CACHE_CAP_BYTES,
        "artifact_count": len(artifact_paths),
        "artifacts": [
            _artifact(path, root=root) for path in artifact_paths
        ],
        "financial_data_role": "DEVELOPMENT_TRAIN_ONLY",
        "validation_read_count": 0,
        "holdout_read_count": 0,
        "forward_2026_read_count": 0,
        "sealed_data_read_count": 0,
        "shared_tpe_study": False,
        "shared_tpe_credit": False,
        "optimizer_feedback_accessed": False,
        "evaluator_train_feedback_outputs_consumed": False,
        "scheduler_state_write": False,
        "cross_campaign_archive_write": False,
        "dynamic_budget_reallocation_allowed": False,
        "underfill_spillover_allowed": False,
        "promotion_authorized": False,
    }
    closure["closure_payload_sha256"] = stable_hash(closure)
    _write_json(root / CLOSURE_NAME, closure)
    return verify_fixed_stratified_production_v0(root)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preflight-root", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--split-manifest", type=Path, required=True)
    parser.add_argument("--sidecar-closure", type=Path, required=True)
    parser.add_argument("--active-field-root", type=Path, required=True)
    parser.add_argument("--active-label-root", type=Path, required=True)
    parser.add_argument("--session-field-root", type=Path, required=True)
    parser.add_argument("--session-label-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--compute-threads", type=int, default=32)
    parser.add_argument("--maximum-wall-seconds", type=int, default=14400)
    parser.add_argument("--required-pairs-per-hour", type=float, default=57.25)
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args(argv)
    if args.verify_only:
        closure = verify_fixed_stratified_production_v0(args.output_root)
    else:
        closure = run(args)
    print(json.dumps(closure, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
