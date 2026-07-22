"""Sequential, report-only validation gate for bounded CN search campaigns."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _snapshot(paths: Iterable[Path]) -> dict[str, str]:
    output: dict[str, str] = {}
    for raw in paths:
        path = Path(raw).resolve()
        if not path.is_file():
            raise FileNotFoundError(path)
        output[str(path)] = _sha256(path)
    return output


def run_automatic_post_train_validation(
    *,
    train_manifest_path: Path,
    validation_output_root: Path,
    protected_train_artifacts: Iterable[Path],
    validation_runner: Callable[[], Mapping[str, Any]],
) -> dict[str, Any]:
    """Run validation after TRAIN_COMPLETE and prove it cannot mutate train state."""

    train_manifest_path = Path(train_manifest_path).resolve()
    train_manifest = json.loads(train_manifest_path.read_text(encoding="utf-8"))
    if str(train_manifest.get("status") or "") != "TRAIN_COMPLETE":
        raise RuntimeError("TRAIN_NOT_COMPLETE: automatic validation is sequential")
    if str(train_manifest.get("promotion") or "FORBIDDEN") != "FORBIDDEN":
        raise RuntimeError("TRAIN_MANIFEST_PROMOTION_BOUNDARY_VIOLATION")
    train_manifest_hash = _sha256(train_manifest_path)
    before = _snapshot(protected_train_artifacts)
    result = dict(validation_runner())
    after = _snapshot(protected_train_artifacts)
    if before != after or _sha256(train_manifest_path) != train_manifest_hash:
        raise RuntimeError("VALIDATION_MUTATED_IMMUTABLE_TRAIN_ARTIFACT")

    required = {
        "status": "VALIDATION_COMPLETE",
        "evaluation_role": "validation",
        "validation_usage": "report_only",
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
    }
    violations = [
        key for key, expected in required.items() if result.get(key) != expected
    ]
    if int(result.get("validation_reads") or 0) <= 0:
        violations.append("validation_reads")
    if violations:
        raise RuntimeError(
            "VALIDATION_BOUNDARY_VIOLATION: " + ",".join(sorted(set(violations)))
        )

    output_root = Path(validation_output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    receipt = {
        "schema_version": "cn_automatic_post_train_validation_receipt_v1",
        "status": "AUTOMATIC_POST_TRAIN_VALIDATION_COMPLETE",
        "train_manifest_path": str(train_manifest_path),
        "train_manifest_sha256": train_manifest_hash,
        "train_artifacts": before,
        "train_artifacts_immutable": True,
        "evaluation_role": "validation",
        "validation_usage": "report_only",
        "validation_feedback": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "automatic_promotion": "FORBIDDEN",
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "validation_result": result,
    }
    receipt_path = output_root / "automatic_post_train_validation_receipt.json"
    receipt_path.write_text(
        json.dumps(receipt, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return receipt
