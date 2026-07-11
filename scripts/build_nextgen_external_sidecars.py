"""Build external NEXTGEN plate/chip sidecars with a durable run record."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
import traceback

from our_system_phase2.services.atomic_checkpoint import atomic_write_json
from our_system_phase2.services.chip_sidecar import build_chip_sidecar
from our_system_phase2.services.tdx_plate_snapshot_release import build_tdx_plate_release


EXPERIMENT_ID = "20260711_pit_sidecars_001"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _persist_attempt(attempt_path: Path, latest_path: Path, record: dict[str, object]) -> None:
    """Update one attempt while preserving every earlier attempt as a separate file."""

    atomic_write_json(attempt_path, record)
    atomic_write_json(
        latest_path,
        {
            "experiment_id": EXPERIMENT_ID,
            "latest_attempt": str(attempt_path),
            "status": record["status"],
            "updated_at": datetime.now(timezone.utc).isoformat(),
        },
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plate-root", type=Path, required=True)
    parser.add_argument("--chip-archive", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--members-per-shard", type=int, default=128)
    parser.add_argument("--plate-workers", type=int, default=4)
    parser.add_argument("--chip-workers", type=int, default=4)
    args = parser.parse_args(argv)

    run_root = args.output_root / "run_records"
    started_at = datetime.now(timezone.utc)
    attempt_id = started_at.strftime("%Y%m%dT%H%M%S%fZ")
    manifest_path = run_root / "attempts" / f"{EXPERIMENT_ID}_{attempt_id}.json"
    latest_path = run_root / f"{EXPERIMENT_ID}.latest.json"
    command = (
        "python scripts/build_nextgen_external_sidecars.py "
        f'--plate-root "{args.plate_root}" --chip-archive "{args.chip_archive}" '
        f'--output-root "{args.output_root}" --members-per-shard {args.members_per_shard} '
        f"--plate-workers {args.plate_workers} --chip-workers {args.chip_workers}"
    )
    record = {
        "experiment_id": EXPERIMENT_ID,
        "attempt_id": attempt_id,
        "objective": "build non-performance PIT plate membership and lagged chip sidecars for NEXTGEN-DARK",
        "status": "RUNNING",
        "mode": "research",
        "started_at": started_at.isoformat(),
        "inputs": {
            "plate_root": str(args.plate_root),
            "chip_archive": str(args.chip_archive),
            "chip_archive_size": args.chip_archive.stat().st_size,
        },
        "parameters": {
            "cutoff": "2025-12-31",
            "forward_2026_performance_allowed": False,
            "mixed_archive_2026_feature_rows_policy": (
                "scan_date_only_then_filter_without_numeric_value_conversion_or_use"
            ),
            "members_per_shard": args.members_per_shard,
            "plate_workers": args.plate_workers,
            "chip_workers": args.chip_workers,
            "data_role": "development_context_only",
        },
        "commands": [command],
        "estimated_runtime": "plate 3-6 minutes; chip 2-5 minutes",
        "outputs": [],
        "reproducibility": "PENDING",
        "decision": "N/A_NO_PERFORMANCE_EVALUATION",
        "failure": None,
    }
    _persist_attempt(manifest_path, latest_path, record)
    started = time.perf_counter()
    try:
        plate = build_tdx_plate_release(
            args.plate_root,
            args.output_root / "tdx_plate_pit_v1",
            workers=args.plate_workers,
        )
        plate_manifest = Path(plate.manifest_path)
        record["outputs"].append(
            {
                "path": str(plate_manifest),
                "producer": "build_tdx_plate_release",
                "purpose": "partial-history 2025 TDX plate PIT membership",
                "stage": "final",
                "size": plate_manifest.stat().st_size,
                "sha256": _sha256(plate_manifest),
            }
        )
        _persist_attempt(manifest_path, latest_path, record)
        chip = build_chip_sidecar(
            args.chip_archive,
            args.output_root / "chip_pit_v1",
            members_per_shard=args.members_per_shard,
            workers=args.chip_workers,
        )
        chip_manifest = Path(chip.manifest_path)
        record["outputs"].append(
            {
                "path": str(chip_manifest),
                "producer": "build_chip_sidecar",
                "purpose": "sealed-2025 lagged daily chip sidecar",
                "stage": "final",
                "size": chip_manifest.stat().st_size,
                "sha256": _sha256(chip_manifest),
            }
        )
        record.update(
            {
                "status": "COMPLETED",
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "actual_runtime_seconds": round(time.perf_counter() - started, 3),
                "reproducibility": "YES_WITH_PATH_INDEPENDENT_CONTENT_HASHES",
                "continuation": "validate manifests and join smoke; do not start CANARY without authorization",
            }
        )
        _persist_attempt(manifest_path, latest_path, record)
        print(
            json.dumps(
                {
                    "status": "COMPLETED",
                    "run_manifest": str(manifest_path),
                    "latest_pointer": str(latest_path),
                },
                indent=2,
            )
        )
        return 0
    except Exception as exc:
        record.update(
            {
                "status": "FAILED",
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "actual_runtime_seconds": round(time.perf_counter() - started, 3),
                "reproducibility": "PARTIAL",
                "failure": {
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "traceback": traceback.format_exc(),
                },
                "continuation": "fix recorded failure and rerun; completed shards resume idempotently",
            }
        )
        _persist_attempt(manifest_path, latest_path, record)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
