from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import numpy as np


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--uninterrupted-root", type=Path, required=True)
    parser.add_argument("--resumed-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    uninterrupted_root = args.uninterrupted_root.resolve()
    resumed_root = args.resumed_root.resolve()
    uninterrupted = json.loads((uninterrupted_root / "CN_STREAMING_BACKEND_RESULT.json").read_text(encoding="utf-8"))
    resumed = json.loads((resumed_root / "CN_STREAMING_BACKEND_RESULT.json").read_text(encoding="utf-8"))
    pause = json.loads((resumed_root / "CN_STREAMING_PAUSE_RECEIPT.json").read_text(encoding="utf-8"))
    checkpoint_root = resumed_root / "checkpoints" / str(resumed["backend"])
    checkpoint_manifest = json.loads(
        (checkpoint_root / "CN_STREAMING_CHECKPOINT_MANIFEST.json").read_text(encoding="utf-8")
    )
    checkpoint_files = tuple(sorted(path.name for path in checkpoint_root.glob("checkpoint_*.npz")))
    latest_checkpoint = checkpoint_root / str(checkpoint_manifest["latest_complete_checkpoint"])

    comparable_fields = (
        "backend",
        "pair_count",
        "candidate_count",
        "rows_processed",
        "blocks_processed",
        "input_binding_hash",
        "split_manifest_hash",
        "eligible_train_date_count",
        "dag_plan_hash",
        "coordinate_rows_retained",
        "support_identities",
        "candidate_rewards",
        "pair_results",
        "split_rows",
        "validation_reads",
        "holdout_reads",
        "forward_2026_reads",
        "promotion",
        "strict_stage_a",
    )
    comparisons = {
        field: _stable_hash(uninterrupted.get(field)) == _stable_hash(resumed.get(field))
        for field in comparable_fields
    }
    atom_left = uninterrupted_root / str(uninterrupted["reward_atoms"]["path"])
    atom_right = resumed_root / str(resumed["reward_atoms"]["path"])
    comparisons["reward_atom_sha256"] = _sha256(atom_left) == _sha256(atom_right)
    for artifact in ("CN_SHARED_DAG_PLAN.json", "CN_STREAMING_REDUCER_CONTRACT.json"):
        comparisons[f"artifact:{artifact}"] = _sha256(uninterrupted_root / artifact) == _sha256(
            resumed_root / artifact
        )
    with np.load(latest_checkpoint, allow_pickle=False) as archive:
        checkpoint_payload_files = tuple(sorted(archive.files))
    checkpoint_payload_complete = {
        "metadata_payload_present": "__metadata_utf8__" in checkpoint_payload_files,
        "continuation_arrays_present": any(name.startswith("array_") for name in checkpoint_payload_files),
        "latest_checkpoint_exists": latest_checkpoint.is_file(),
        "retained_checkpoint_file_count_lte_2": len(checkpoint_files) <= 2,
        "pause_completed_blocks_nonempty": bool(pause.get("completed_blocks")),
        "pause_rows_processed_positive": int(pause.get("rows_processed") or 0) > 0,
        "resumed_full_rows": int(resumed["rows_processed"]) == int(uninterrupted["rows_processed"]),
    }
    status = (
        "RESUME_UNINTERRUPTED_PARITY_PASS"
        if all(comparisons.values()) and all(checkpoint_payload_complete.values())
        else "RESUME_UNINTERRUPTED_PARITY_FAIL"
    )
    report = {
        "schema_version": "cn_phase3cm_resume_uninterrupted_parity_v1",
        "status": status,
        "uninterrupted_root": str(uninterrupted_root),
        "resumed_root": str(resumed_root),
        "comparisons": comparisons,
        "checkpoint_payload_complete": checkpoint_payload_complete,
        "checkpoint_files": checkpoint_files,
        "latest_checkpoint": checkpoint_manifest["latest_complete_checkpoint"],
        "latest_checkpoint_files": checkpoint_payload_files,
        "pause_receipt_status": pause.get("status"),
        "pause_completed_blocks": pause.get("completed_blocks"),
        "uninterrupted_reward_atom_sha256": _sha256(atom_left),
        "resumed_reward_atom_sha256": _sha256(atom_right),
        "data_role": "development_train_only",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
    }
    args.output.resolve().parent.mkdir(parents=True, exist_ok=True)
    args.output.resolve().write_text(
        json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"status": status, "comparison_count": len(comparisons)}, sort_keys=True))
    return 0 if status.endswith("PASS") else 2


if __name__ == "__main__":
    raise SystemExit(main())
