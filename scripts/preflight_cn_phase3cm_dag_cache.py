from __future__ import annotations

import argparse
import csv
import hashlib
import inspect
import json
import subprocess
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.services.phase3cm_streaming_capacity import (
    predict_dag_cache_peak,
)
from our_system_phase2.services.phase3cm_streaming_dag import SharedMultiCandidateDAGPlan
from our_system_phase2.services.phase3cm_streaming_expression import (
    StreamingExpressionExecutor,
)
from our_system_phase2.services.phase3cm_streaming_resource_contract import (
    FrozenExecutionPlan,
)


SOURCE_CLOSURE_PATHS = (
    "scripts/analyze_cn_core_pack_strict_wave.py",
    "scripts/freeze_cn_phase3cm_backend_partitions.py",
    "scripts/invoke_cn_phase3cm_backend_with_exit_receipt.ps1",
    "scripts/cn_phase3cm_process_tree_monitor.ps1",
    "scripts/preflight_cn_phase3cm_dag_cache.py",
    "scripts/run_cn_phase3cm_streaming_qualification.py",
    "scripts/run_cn_phase3cm_phase_e_qualification_77o.ps1",
    "scripts/run_cn_phase3cm_partitioned_backend_77o.ps1",
    "src/our_system_phase2/services/expression_semantics.py",
    "src/our_system_phase2/runtime/phase3cm_train_portfolio_sortino_reward_audit.py",
    "src/our_system_phase2/services/phase3cm_streaming_block_reader.py",
    "src/our_system_phase2/services/phase3cm_streaming_cache.py",
    "src/our_system_phase2/services/phase3cm_streaming_capacity.py",
    "src/our_system_phase2/services/phase3cm_streaming_checkpoint.py",
    "src/our_system_phase2/services/phase3cm_streaming_dag.py",
    "src/our_system_phase2/services/phase3cm_streaming_expression.py",
    "src/our_system_phase2/services/phase3cm_streaming_portfolio.py",
    "src/our_system_phase2/services/phase3cm_streaming_reducer.py",
    "src/our_system_phase2/services/phase3cm_streaming_resource_contract.py",
    "src/our_system_phase2/services/phase3cm_streaming_support.py",
    "src/our_system_phase2/services/phase3cm_streaming_telemetry.py",
)
FIELD_MANIFEST_NAME = "CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"
LABEL_MANIFEST_NAME = "CN_FORWARD_LABEL_SIDECAR_MANIFEST.json"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _canonical_source_bytes(path: Path) -> bytes:
    """Match Git's text clean filter while remaining portable across Windows."""

    return Path(path).read_bytes().replace(b"\r\n", b"\n")


def _canonical_source_sha256(path: Path) -> str:
    return hashlib.sha256(_canonical_source_bytes(path)).hexdigest()


def _git_blob_hash(path: Path, *, object_id_length: int = 40) -> str:
    payload = _canonical_source_bytes(path)
    header = f"blob {len(payload)}\0".encode("ascii")
    if int(object_id_length) == 40:
        digest = hashlib.sha1()
    elif int(object_id_length) == 64:
        digest = hashlib.sha256()
    else:
        raise ValueError("Git object id must be SHA-1 or SHA-256")
    digest.update(header)
    digest.update(payload)
    return digest.hexdigest()


def _git_output(repo_root: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ("git", "-C", str(repo_root), *arguments),
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _git_repository_available(repo_root: Path) -> bool:
    try:
        return _git_output(repo_root, "rev-parse", "--is-inside-work-tree") == "true"
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


def _source_closure_manifest_body_from_git(repo_root: Path) -> dict[str, Any]:
    """Build a portable closure manifest from a clean tracked commit."""

    root = Path(repo_root).resolve()
    repo_sha = _git_output(root, "rev-parse", "HEAD")
    if len(repo_sha) not in (40, 64):
        raise RuntimeError("cannot bind DAG cache preflight to an exact repo SHA")
    for relative in SOURCE_CLOSURE_PATHS:
        _git_output(root, "ls-files", "--error-unmatch", "--", relative)
    drift = _git_output(
        root,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        "--",
        *SOURCE_CLOSURE_PATHS,
    )
    if drift:
        raise RuntimeError(
            "critical qualification source closure is not clean:\n" + drift
        )
    sources: list[dict[str, str]] = []
    for relative in SOURCE_CLOSURE_PATHS:
        path = root / relative
        if not path.is_file():
            raise FileNotFoundError(f"critical source is missing: {path}")
        tree_record = _git_output(root, "ls-tree", "HEAD", "--", relative)
        metadata, separator, observed_path = tree_record.partition("\t")
        parts = metadata.split()
        if (
            not separator
            or observed_path != relative
            or len(parts) != 3
            or parts[1] != "blob"
        ):
            raise RuntimeError(f"cannot resolve exact Git blob metadata: {relative}")
        mode, _, blob_sha = parts
        calculated_blob_sha = _git_blob_hash(
            path,
            object_id_length=len(blob_sha),
        )
        if calculated_blob_sha != blob_sha:
            raise RuntimeError(
                f"working source does not reproduce committed Git blob: {relative}"
            )
        sources.append(
            {
                "path": relative,
                "sha256": _canonical_source_sha256(path),
                "git_blob_sha": blob_sha,
                "git_mode": mode,
                "normalization": "TEXT_CRLF_TO_LF",
            }
        )
    body: dict[str, Any] = {
        "schema_version": "cn_phase3cm_source_closure_manifest_v1",
        "status": "CN_PHASE3CM_SOURCE_CLOSURE_MANIFEST_READY",
        "repo_sha": repo_sha,
        "source_paths": list(SOURCE_CLOSURE_PATHS),
        "sources": sources,
        "source_closure_hash": _stable_hash(sources),
    }
    body["manifest_hash"] = _stable_hash(body)
    return body


def _clean_source_closure(repo_root: Path) -> dict[str, Any]:
    """Compatibility wrapper used by clean-Git callers and focused tests."""

    return _source_closure_manifest_body_from_git(repo_root)


def validate_source_closure_manifest(
    manifest_path: Path,
    *,
    repo_root: Path,
    expected_repo_sha: str,
) -> dict[str, Any]:
    """Verify a transferred manifest and every source byte without trusting Git."""

    path = Path(manifest_path).resolve()
    payload = json.loads(path.read_text(encoding="utf-8"))
    claimed_hash = str(payload.get("manifest_hash") or "")
    body = dict(payload)
    body.pop("manifest_hash", None)
    if _stable_hash(body) != claimed_hash:
        raise RuntimeError("source closure manifest self-hash drift")
    if payload.get("schema_version") != "cn_phase3cm_source_closure_manifest_v1":
        raise RuntimeError("source closure manifest schema drift")
    if payload.get("status") != "CN_PHASE3CM_SOURCE_CLOSURE_MANIFEST_READY":
        raise RuntimeError("source closure manifest status drift")
    if str(payload.get("repo_sha") or "") != str(expected_repo_sha):
        raise RuntimeError("source closure manifest repo SHA drift")
    if payload.get("source_paths") != list(SOURCE_CLOSURE_PATHS):
        raise RuntimeError("source closure manifest path set/order drift")
    sources = list(payload.get("sources") or [])
    if [row.get("path") for row in sources] != list(SOURCE_CLOSURE_PATHS):
        raise RuntimeError("source closure manifest source records drift")
    if len({str(row.get("path") or "") for row in sources}) != len(sources):
        raise RuntimeError("source closure manifest contains duplicate paths")
    if _stable_hash(sources) != str(payload.get("source_closure_hash") or ""):
        raise RuntimeError("source closure manifest source hash drift")
    root = Path(repo_root).resolve()
    expected_record_keys = {
        "path",
        "sha256",
        "git_blob_sha",
        "git_mode",
        "normalization",
    }
    for record in sources:
        if set(record) != expected_record_keys:
            raise RuntimeError("source closure manifest record schema drift")
        relative = str(record["path"])
        source = root / relative
        if not source.is_file():
            raise FileNotFoundError(f"source closure file is missing: {source}")
        if record["normalization"] != "TEXT_CRLF_TO_LF":
            raise RuntimeError(f"source normalization drift: {relative}")
        if _canonical_source_sha256(source) != str(record["sha256"]):
            raise RuntimeError(f"source file SHA-256 drift: {relative}")
        blob_sha = str(record["git_blob_sha"])
        if _git_blob_hash(source, object_id_length=len(blob_sha)) != blob_sha:
            raise RuntimeError(f"source Git blob SHA drift: {relative}")
        if str(record["git_mode"]) not in {"100644", "100755"}:
            raise RuntimeError(f"source Git mode is invalid: {relative}")
    return payload


def _resolve_source_closure(
    repo_root: Path,
    *,
    source_closure_manifest: Path | None,
    expected_repo_sha: str | None,
) -> dict[str, Any]:
    root = Path(repo_root).resolve()
    git_available = _git_repository_available(root)
    if source_closure_manifest is None:
        if not git_available:
            raise RuntimeError(
                "no-Git qualification requires an explicit source closure manifest"
            )
        manifest = _source_closure_manifest_body_from_git(root)
        if expected_repo_sha and manifest["repo_sha"] != expected_repo_sha:
            raise RuntimeError("clean Git source closure repo SHA drift")
        return {
            "repo_sha": manifest["repo_sha"],
            "source_closure_status": "CLEAN_TRACKED_CRITICAL_SOURCE_CLOSURE",
            "source_paths": manifest["source_paths"],
            "sources": manifest["sources"],
            "source_closure_hash": manifest["source_closure_hash"],
            "source_closure_manifest_hash": manifest["manifest_hash"],
            "source_closure_manifest_sha256": None,
        }
    if not expected_repo_sha:
        raise RuntimeError("explicit source closure manifest requires expected repo SHA")
    manifest = validate_source_closure_manifest(
        source_closure_manifest,
        repo_root=root,
        expected_repo_sha=str(expected_repo_sha),
    )
    if git_available:
        live = _source_closure_manifest_body_from_git(root)
        if live != manifest:
            raise RuntimeError("source closure manifest drifts from clean Git commit")
    return {
        "repo_sha": manifest["repo_sha"],
        "source_closure_status": "EXPLICIT_SOURCE_CLOSURE_MANIFEST_VERIFIED",
        "source_paths": manifest["source_paths"],
        "sources": manifest["sources"],
        "source_closure_hash": manifest["source_closure_hash"],
        "source_closure_manifest_hash": manifest["manifest_hash"],
        "source_closure_manifest_sha256": _sha256(source_closure_manifest),
    }


def _sidecar_manifest_evidence(root: Path, *, kind: str) -> dict[str, Any]:
    resolved_root = Path(root).resolve()
    if kind == "field":
        manifest_name = FIELD_MANIFEST_NAME
        expected_status = "TIME_MAJOR_LAYOUT_PARITY_PASS"
        row_keys = ("sidecar_rows", "source_rows")
    elif kind == "label":
        manifest_name = LABEL_MANIFEST_NAME
        expected_status = "GLOBAL_SYMBOL_CONTINUITY_LABEL_SIDECARS_READY"
        row_keys = ("output_rows", "source_rows")
    else:
        raise ValueError(f"unsupported sidecar manifest kind: {kind}")
    path = resolved_root / manifest_name
    if not path.is_file():
        raise FileNotFoundError(f"{kind} sidecar manifest is required: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if str(payload.get("status") or "") != expected_status:
        raise RuntimeError(f"{kind} sidecar manifest is not qualified")
    if str(payload.get("data_role") or "") != "development_train_only":
        raise PermissionError(f"{kind} sidecar manifest is not development/train-only")
    sealed_reads = {
        name: int(payload.get(name) or 0)
        for name in ("validation_reads", "holdout_reads", "forward_2026_reads")
    }
    if any(sealed_reads.values()):
        raise PermissionError(f"{kind} sidecar manifest records forbidden reads")
    shard_count = int(payload.get("source_shard_count") or len(payload.get("shards") or ()))
    if shard_count != 16:
        raise RuntimeError(f"{kind} sidecar manifest must bind exactly 16 shards")
    row_count = next(
        (int(payload[key]) for key in row_keys if int(payload.get(key) or 0) > 0),
        0,
    )
    if row_count <= 0:
        raise RuntimeError(f"{kind} sidecar manifest has no positive row count")
    split_manifest_hash = str(payload.get("split_manifest_hash") or "")
    if len(split_manifest_hash) != 64:
        raise RuntimeError(f"{kind} sidecar split-manifest hash is not exact")
    return {
        "kind": kind,
        "root": str(resolved_root),
        "manifest_path": str(path),
        "manifest_sha256": _sha256(path),
        "manifest_status": expected_status,
        "split_manifest_hash": split_manifest_hash,
        "shard_count": shard_count,
        "row_count": row_count,
        "sealed_reads": sealed_reads,
    }


def _source_contract(
    repo_root: Path,
    *,
    executor_entry_default: int,
    source_closure_manifest: Path | None,
    expected_repo_sha: str | None,
) -> dict[str, Any]:
    closure = _resolve_source_closure(
        repo_root,
        source_closure_manifest=source_closure_manifest,
        expected_repo_sha=expected_repo_sha,
    )
    contract = {
        "schema_version": "cn_phase3cm_dag_cache_preflight_source_contract_v3",
        **closure,
        "executor_cache_max_entries_default": int(executor_entry_default),
    }
    contract["contract_hash"] = _stable_hash(contract)
    return contract


def _read_csv(path: Path) -> list[dict[str, str]]:
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(destination)


def candidates_in_frozen_execution_order(
    rows: Sequence[Mapping[str, Any]],
    *,
    plan: FrozenExecutionPlan,
    backend: str,
) -> tuple[dict[str, Any], ...]:
    """Select PRIMARY/CONTROL members in the plan's exact pair-batch order."""

    pair_order = tuple(pair_id for batch in plan.pair_batches for pair_id in batch)
    if not pair_order or len(pair_order) != len(set(pair_order)):
        raise ValueError("frozen plan pair batches are empty or duplicated")
    pair_ids = set(pair_order)
    by_pair: dict[str, list[dict[str, Any]]] = {}
    for raw in rows:
        row = dict(raw)
        pair_id = str(row.get("pair_id") or "")
        if pair_id in pair_ids:
            by_pair.setdefault(pair_id, []).append(row)

    ordered: list[dict[str, Any]] = []
    for pair_id in pair_order:
        members = by_pair.get(pair_id, [])
        if len(members) != 2:
            raise ValueError(
                f"frozen pair must resolve to exactly two candidate rows: {pair_id}"
            )
        members.sort(
            key=lambda row: 0
            if str(row.get("pair_member_role") or "") == "PRIMARY"
            else 1
        )
        roles = [str(row.get("pair_member_role") or "") for row in members]
        if roles != ["PRIMARY", "CONTROL"]:
            raise ValueError(f"frozen pair role drift: {pair_id} has {roles}")
        for member in members:
            member["clock_namespace"] = str(backend)
            ordered.append(member)
    candidate_ids = tuple(str(row.get("candidate_id") or "") for row in ordered)
    if not all(candidate_ids) or len(candidate_ids) != len(set(candidate_ids)):
        raise ValueError("frozen candidate identities are empty or duplicated")
    return tuple(ordered)


def build_receipt(
    *,
    candidate_table: Path,
    execution_plan: Path,
    backend: str,
    max_block_rows: int,
    cache_max_entries: int,
    field_sidecar_root: Path,
    label_sidecar_root: Path,
    repo_root: Path | None = None,
    source_closure_manifest: Path | None = None,
    expected_repo_sha: str | None = None,
) -> dict[str, Any]:
    resolved_repo_root = (
        Path(repo_root).resolve()
        if repo_root is not None
        else Path(__file__).resolve().parents[1]
    )
    executor_entry_default = inspect.signature(
        StreamingExpressionExecutor.__init__
    ).parameters["cache_max_entries"].default
    if int(cache_max_entries) != int(executor_entry_default):
        raise ValueError(
            "preflight cache entry cap must equal the active executor default: "
            f"{cache_max_entries} != {executor_entry_default}"
        )
    field_evidence = _sidecar_manifest_evidence(field_sidecar_root, kind="field")
    label_evidence = _sidecar_manifest_evidence(label_sidecar_root, kind="label")
    if field_evidence["split_manifest_hash"] != label_evidence["split_manifest_hash"]:
        raise RuntimeError("field and label sidecar split-manifest hashes drift")
    plan_payload = json.loads(Path(execution_plan).read_text(encoding="utf-8"))
    plan = FrozenExecutionPlan.from_dict(plan_payload)
    candidates = candidates_in_frozen_execution_order(
        _read_csv(candidate_table),
        plan=plan,
        backend=backend,
    )
    dag_plan = SharedMultiCandidateDAGPlan.build(candidates)
    preflight = predict_dag_cache_peak(
        dag_plan,
        ordered_candidate_ids=(str(row["candidate_id"]) for row in candidates),
        max_block_rows=int(max_block_rows),
        dag_block_cache_bytes=int(plan.cache_caps["dag_block_cache_bytes"]),
        dag_block_cache_entries=int(cache_max_entries),
    )
    source_contract = _source_contract(
        resolved_repo_root,
        executor_entry_default=int(executor_entry_default),
        source_closure_manifest=source_closure_manifest,
        expected_repo_sha=expected_repo_sha,
    )
    runner_source = resolved_repo_root / "scripts/run_cn_phase3cm_streaming_qualification.py"
    guard_source = Path(inspect.getsourcefile(predict_dag_cache_peak) or "").resolve()
    receipt = {
        **preflight.to_dict(),
        "backend": str(backend),
        "data_role": "development",
        "execution_plan_hash": plan.execution_plan_hash,
        "execution_plan_path": str(Path(execution_plan).resolve()),
        "execution_plan_sha256": _sha256(execution_plan),
        "candidate_table_path": str(Path(candidate_table).resolve()),
        "candidate_table_sha256": _sha256(candidate_table),
        "block_row_evidence": {
            "kind": "RUNTIME_ENFORCED_UPPER_BOUND_WITH_SIDECAR_MANIFESTS",
            "max_block_rows": int(max_block_rows),
            "plan_block_count": len(plan.block_boundaries),
            "plan_block_boundaries_hash": _stable_hash(plan.block_boundaries),
            "enforcement_point": "AFTER_BLOCK_READ_BEFORE_DAG_BIND_OR_ALLOCATION",
            "runtime_guard": {
                "runner_path": "scripts/run_cn_phase3cm_streaming_qualification.py",
                "runner_sha256": _sha256(runner_source),
                "guard_source_path": str(
                    guard_source.relative_to(resolved_repo_root)
                ).replace("\\", "/"),
                "guard_source_sha256": _sha256(guard_source),
            },
            "field_sidecar_manifest": field_evidence,
            "label_sidecar_manifest": label_evidence,
        },
        "preflight_source_contract": source_contract,
        "runtime_cache_entry_contract": {
            "preflight_cache_max_entries": int(cache_max_entries),
            "executor_cache_max_entries_default": int(executor_entry_default),
            "matches_active_executor": True,
        },
        "sealed_reads": {"validation": 0, "holdout": 0, "forward_2026": 0},
        "changes_to_frozen_execution_plan": 0,
    }
    receipt["receipt_hash"] = _stable_hash(receipt)
    return receipt


def validate_receipt_for_launch(
    *,
    receipt_path: Path,
    candidate_table: Path,
    execution_plan: Path,
    backend: str,
    field_sidecar_root: Path,
    label_sidecar_root: Path,
    repo_root: Path,
    expected_repo_sha: str,
    source_closure_manifest: Path | None = None,
) -> dict[str, Any]:
    """Recompute a receipt from current inputs; exact mismatch blocks launch."""

    receipt = json.loads(Path(receipt_path).read_text(encoding="utf-8"))
    claimed_receipt_hash = str(receipt.get("receipt_hash") or "")
    receipt_body = dict(receipt)
    receipt_body.pop("receipt_hash", None)
    if _stable_hash(receipt_body) != claimed_receipt_hash:
        raise RuntimeError("capacity receipt self-hash drift")
    if receipt.get("status") != "CN_PHASE3CM_DAG_CACHE_PREFLIGHT_PASS":
        raise RuntimeError("capacity receipt is not PASS")
    if str(receipt.get("backend") or "") != str(backend):
        raise RuntimeError("capacity receipt backend drift")
    source_contract = dict(receipt.get("preflight_source_contract") or {})
    if str(source_contract.get("repo_sha") or "") != str(expected_repo_sha):
        raise RuntimeError("capacity receipt repo SHA drifts from launch contract")
    block_evidence = dict(receipt.get("block_row_evidence") or {})
    rebuilt = build_receipt(
        candidate_table=Path(candidate_table).resolve(),
        execution_plan=Path(execution_plan).resolve(),
        backend=str(backend),
        max_block_rows=int(block_evidence.get("max_block_rows") or 0),
        cache_max_entries=int(
            (receipt.get("runtime_cache_entry_contract") or {}).get(
                "preflight_cache_max_entries"
            )
            or 0
        ),
        field_sidecar_root=Path(field_sidecar_root).resolve(),
        label_sidecar_root=Path(label_sidecar_root).resolve(),
        repo_root=Path(repo_root).resolve(),
        source_closure_manifest=(
            Path(source_closure_manifest).resolve()
            if source_closure_manifest is not None
            else None
        ),
        expected_repo_sha=str(expected_repo_sha),
    )
    if _stable_hash(rebuilt) != _stable_hash(receipt):
        raise RuntimeError("capacity receipt does not exactly match recomputed launch inputs")
    return {
        "status": "CN_PHASE3CM_DAG_CACHE_RECEIPT_VALIDATED_FOR_LAUNCH",
        "backend": str(backend),
        "receipt_hash": claimed_receipt_hash,
        "execution_plan_hash": str(receipt["execution_plan_hash"]),
        "max_block_rows": int(block_evidence["max_block_rows"]),
        "source_closure_hash": str(source_contract["source_closure_hash"]),
        "source_closure_manifest_hash": str(
            source_contract["source_closure_manifest_hash"]
        ),
        "source_closure_manifest_sha256": source_contract[
            "source_closure_manifest_sha256"
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fail-closed Phase3CM DAG cache peak preflight"
    )
    parser.add_argument("--write-source-closure-manifest", action="store_true")
    parser.add_argument("--validate-receipt", type=Path)
    parser.add_argument("--candidate-table", type=Path)
    parser.add_argument("--execution-plan", type=Path)
    parser.add_argument("--backend", choices=("active_bar", "stock_session"))
    parser.add_argument("--max-block-rows", type=int)
    parser.add_argument("--cache-max-entries", type=int, default=2048)
    parser.add_argument("--field-sidecar-root", type=Path)
    parser.add_argument("--label-sidecar-root", type=Path)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--expected-repo-sha")
    parser.add_argument("--source-closure-manifest", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    if args.write_source_closure_manifest:
        if args.output is None:
            parser.error("--output is required with --write-source-closure-manifest")
        manifest = _source_closure_manifest_body_from_git(args.repo_root.resolve())
        if args.expected_repo_sha and manifest["repo_sha"] != args.expected_repo_sha:
            raise RuntimeError("generated source closure manifest repo SHA drift")
        _write_json(args.output.resolve(), manifest)
        print(
            json.dumps(
                {
                    "status": manifest["status"],
                    "repo_sha": manifest["repo_sha"],
                    "source_count": len(manifest["sources"]),
                    "source_closure_hash": manifest["source_closure_hash"],
                    "manifest_hash": manifest["manifest_hash"],
                    "manifest_sha256": _sha256(args.output.resolve()),
                },
                sort_keys=True,
            )
        )
        return

    common_required = {
        "candidate-table": args.candidate_table,
        "execution-plan": args.execution_plan,
        "backend": args.backend,
        "field-sidecar-root": args.field_sidecar_root,
        "label-sidecar-root": args.label_sidecar_root,
    }
    missing = [name for name, value in common_required.items() if value is None]
    if missing:
        parser.error("missing required arguments: " + ", ".join(missing))

    if args.validate_receipt is not None:
        if not args.expected_repo_sha:
            parser.error("--expected-repo-sha is required with --validate-receipt")
        validation = validate_receipt_for_launch(
            receipt_path=args.validate_receipt.resolve(),
            candidate_table=args.candidate_table.resolve(),
            execution_plan=args.execution_plan.resolve(),
            backend=str(args.backend),
            field_sidecar_root=args.field_sidecar_root.resolve(),
            label_sidecar_root=args.label_sidecar_root.resolve(),
            repo_root=args.repo_root.resolve(),
            expected_repo_sha=str(args.expected_repo_sha),
            source_closure_manifest=(
                args.source_closure_manifest.resolve()
                if args.source_closure_manifest is not None
                else None
            ),
        )
        print(json.dumps(validation, ensure_ascii=False, sort_keys=True))
        return

    if args.max_block_rows is None or args.output is None:
        parser.error("--max-block-rows and --output are required when building a receipt")

    receipt = build_receipt(
        candidate_table=args.candidate_table.resolve(),
        execution_plan=args.execution_plan.resolve(),
        backend=str(args.backend),
        max_block_rows=int(args.max_block_rows),
        cache_max_entries=int(args.cache_max_entries),
        field_sidecar_root=args.field_sidecar_root.resolve(),
        label_sidecar_root=args.label_sidecar_root.resolve(),
        repo_root=args.repo_root.resolve(),
        source_closure_manifest=(
            args.source_closure_manifest.resolve()
            if args.source_closure_manifest is not None
            else None
        ),
        expected_repo_sha=(
            str(args.expected_repo_sha) if args.expected_repo_sha else None
        ),
    )
    _write_json(args.output.resolve(), receipt)
    print(
        json.dumps(
            {
                key: receipt[key]
                for key in (
                    "status",
                    "backend",
                    "candidate_count",
                    "max_block_rows",
                    "predicted_peak_bytes",
                    "dag_block_cache_bytes",
                    "byte_headroom",
                    "predicted_peak_entries",
                    "dag_block_cache_entries",
                    "entry_headroom",
                    "preflight_hash",
                )
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    if receipt["status"] != "CN_PHASE3CM_DAG_CACHE_PREFLIGHT_PASS":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
