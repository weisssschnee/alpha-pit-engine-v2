"""Atomic content-addressed checkpoints with real streaming continuation state."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any

import numpy as np


class CheckpointDriftError(RuntimeError):
    """Raised when a checkpoint cannot be resumed under the frozen plan."""


@dataclass(slots=True)
class StreamingCheckpointPayload:
    execution_plan_hash: str
    input_binding_hash: str
    backend_identity: str
    route_cohort: str
    execution_position: dict[str, Any]
    completed_blocks: list[int]
    completed_pair_batches: list[str]
    temporal_continuation_payload: dict[str, Any]
    state_event_continuation_payload: dict[str, Any]
    portfolio_continuation_payload: dict[str, Any]
    streaming_reducer_payload: dict[str, Any]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _encode(value: Any, arrays: dict[str, np.ndarray], path: str = "root") -> Any:
    if isinstance(value, np.ndarray):
        key = f"array_{len(arrays):06d}"
        arrays[key] = value
        return {"__ndarray__": key, "dtype": str(value.dtype), "shape": list(value.shape)}
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, dict):
        return {str(key): _encode(item, arrays, f"{path}.{key}") for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))}
    if isinstance(value, (list, tuple)):
        return [_encode(item, arrays, f"{path}.{index}") for index, item in enumerate(value)]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"unsupported checkpoint value at {path}: {type(value)!r}")


def _decode(value: Any, arrays: dict[str, np.ndarray]) -> Any:
    if isinstance(value, dict) and "__ndarray__" in value:
        key = str(value["__ndarray__"])
        if key not in arrays:
            raise CheckpointDriftError(f"checkpoint array is missing: {key}")
        array = arrays[key]
        if str(array.dtype) != str(value["dtype"]) or list(array.shape) != list(value["shape"]):
            raise CheckpointDriftError(f"checkpoint array metadata drift: {key}")
        return array
    if isinstance(value, dict):
        return {key: _decode(item, arrays) for key, item in value.items()}
    if isinstance(value, list):
        return [_decode(item, arrays) for item in value]
    return value


def _atomic_json_temporary_path(path: Path) -> Path:
    return path.with_name(f".{uuid.uuid4().hex}.tmp")


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    temporary = _atomic_json_temporary_path(path)
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def write_checkpoint(
    checkpoint_root: Path,
    payload: StreamingCheckpointPayload,
    *,
    checkpoint_ordinal: int,
    retain_complete_checkpoints: int = 2,
) -> dict[str, Any]:
    if int(retain_complete_checkpoints) < 1:
        raise ValueError("retain_complete_checkpoints must be at least 1")
    root = Path(checkpoint_root)
    root.mkdir(parents=True, exist_ok=True)
    arrays: dict[str, np.ndarray] = {}
    # dataclasses.asdict() deep-copies every continuation ndarray before the
    # encoder sees it.  Checkpoints are immutable snapshots at this call site,
    # so bind fields directly and let np.savez consume the authoritative arrays
    # once instead of duplicating the complete reducer/portfolio state.
    payload_fields = {
        field.name: getattr(payload, field.name)
        for field in fields(payload)
    }
    metadata = _encode(
        {
            "schema_version": "cn_phase3cm_streaming_checkpoint_v1",
            **payload_fields,
        },
        arrays,
    )
    arrays["__metadata_utf8__"] = np.frombuffer(
        json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8"),
        dtype=np.uint8,
    )
    temporary = root / f"checkpoint_{int(checkpoint_ordinal):06d}.{uuid.uuid4().hex}.npz.tmp"
    with temporary.open("wb") as handle:
        np.savez(handle, **arrays)
        handle.flush()
        os.fsync(handle.fileno())
    payload_sha = _sha256(temporary)
    final = root / f"checkpoint_{int(checkpoint_ordinal):06d}_{payload_sha[:16]}.npz"
    os.replace(temporary, final)
    record = {
        "checkpoint_ordinal": int(checkpoint_ordinal),
        "checkpoint_path": final,
        "payload_sha256": payload_sha,
        "payload_bytes": final.stat().st_size,
        "execution_plan_hash": payload.execution_plan_hash,
        "input_binding_hash": payload.input_binding_hash,
        "backend_identity": payload.backend_identity,
        "route_cohort": payload.route_cohort,
        "completed_block_count": len(payload.completed_blocks),
        "completed_pair_batch_count": len(payload.completed_pair_batches),
        "status": "COMPLETE_RECOVERABLE_PAYLOAD",
    }
    completed = sorted(
        root.glob("checkpoint_[0-9][0-9][0-9][0-9][0-9][0-9]_*.npz"),
        key=lambda path: path.name,
    )
    stale = completed[: -int(retain_complete_checkpoints)]
    record["retained_complete_checkpoint_count"] = len(completed) - len(stale)
    record["pruned_complete_checkpoint_count"] = len(stale)
    record["retention_limit"] = int(retain_complete_checkpoints)
    manifest_record = {**record, "checkpoint_path": final.name}
    _atomic_json(
        root / "CN_STREAMING_CHECKPOINT_MANIFEST.json",
        {
            "schema_version": "cn_phase3cm_streaming_checkpoint_manifest_v1",
            "latest_complete_checkpoint": final.name,
            "record": manifest_record,
        },
    )
    for stale_path in stale:
        if stale_path != final:
            stale_path.unlink(missing_ok=True)
    return record


def load_checkpoint(
    checkpoint_path: Path,
    *,
    expected_execution_plan_hash: str,
    expected_input_binding_hash: str,
) -> StreamingCheckpointPayload:
    path = Path(checkpoint_path)
    with np.load(path, allow_pickle=False) as archive:
        if "__metadata_utf8__" not in archive.files:
            raise CheckpointDriftError("checkpoint metadata is missing")
        metadata = json.loads(bytes(archive["__metadata_utf8__"].tolist()).decode("utf-8"))
        arrays = {name: np.array(archive[name], copy=True) for name in archive.files if name != "__metadata_utf8__"}
    decoded = _decode(metadata, arrays)
    if decoded.get("schema_version") != "cn_phase3cm_streaming_checkpoint_v1":
        raise CheckpointDriftError("checkpoint schema drift")
    if decoded.get("execution_plan_hash") != expected_execution_plan_hash:
        raise CheckpointDriftError("execution-plan hash drift")
    if decoded.get("input_binding_hash") != expected_input_binding_hash:
        raise CheckpointDriftError("input-binding hash drift")
    decoded.pop("schema_version", None)
    return StreamingCheckpointPayload(**decoded)
