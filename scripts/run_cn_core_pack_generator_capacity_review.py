"""Run the non-performance Core Pack to generator capacity review."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import platform
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from our_system_phase2.services.core_pack_generator_review import (  # noqa: E402
    audit_generator_capacity,
    build_field_root_review,
    build_frozen_discovery_contract,
    summarize_field_review,
)
from our_system_phase2.services.unified_capability_registry import (  # noqa: E402
    ROUTE_IDS,
    UnifiedCapabilityRegistry,
    stable_hash,
)


DEFAULT_MASTER = REPO / "runtime/field_registry/cn_field_master_registry_v1/cn_field_master_registry_v1.json"
DEFAULT_REGISTRY = REPO / "runtime/field_registry/cn_unified_capability_registry_v3_20260717/unified_capability_registry.json"
DEFAULT_INFORMATION = REPO / "runtime/cn_full_field_information_research_v1_20260717"
DEFAULT_OUTPUT = REPO / "runtime/cn_core_pack_generator_capacity_v1_20260718"
DEFAULT_CONTRACT = REPO / "runtime/run_plans/cn_core_pack_development_discovery_v1.json"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _csv_value(value: Any) -> Any:
    if isinstance(value, (list, tuple, set)):
        return "|".join(str(item) for item in value)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return value


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]]) -> str:
    materialized = list(rows)
    if not materialized:
        raise ValueError(f"refusing to write empty CSV: {path}")
    columns = list(materialized[0])
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in materialized:
            writer.writerow({column: _csv_value(row.get(column)) for column in columns})
    return stable_hash(columns)


def _repo_sha(path: Path) -> str:
    try:
        return subprocess.check_output(
            ["git", "-C", str(path), "rev-parse", "HEAD"],
            text=True,
            encoding="utf-8",
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return "UNAVAILABLE_NON_GIT_EXECUTION_ROOT"


def _parse_ints(text: str) -> list[int]:
    return [int(value.strip()) for value in text.split(",") if value.strip()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--master", type=Path, default=DEFAULT_MASTER)
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--information-root", type=Path, default=DEFAULT_INFORMATION)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--contract-output", type=Path, default=DEFAULT_CONTRACT)
    parser.add_argument("--attempts-per-route", type=int, default=32768)
    parser.add_argument("--broad-event-attempts", type=int, default=4096)
    parser.add_argument("--seeds", default="20260718,20260719")
    parser.add_argument("--checkpoints", default="2048,8192,32768")
    parser.add_argument("--expected-hostname", default="")
    parser.add_argument("--source-repo-sha", default="")
    args = parser.parse_args(argv)

    hostname = platform.node()
    if args.expected_hostname and hostname.upper() != args.expected_hostname.upper():
        raise RuntimeError(
            f"execution host mismatch: expected={args.expected_hostname} actual={hostname}"
        )
    started = datetime.now(timezone.utc)
    timer = time.perf_counter()
    core_path = args.information_root / "core_pack.json"
    metrics_path = args.information_root / "capability_information_metrics.json"
    information_manifest = args.information_root / "run_manifest.json"
    input_files = {
        "field_master_registry": args.master,
        "unified_capability_registry": args.registry,
        "information_core_pack": core_path,
        "capability_information_metrics": metrics_path,
        "information_research_manifest": information_manifest,
    }
    required = list(input_files.values())
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"missing review inputs: {missing}")

    source_hashes = {name: _sha256(path) for name, path in input_files.items()}
    master = _json(args.master)
    registry = UnifiedCapabilityRegistry.read(args.registry)
    core_pack = _json(core_path)
    metrics = _json(metrics_path)
    seeds = _parse_ints(args.seeds)
    checkpoints = _parse_ints(args.checkpoints)
    attempts_by_route = {
        route_id: (
            int(args.broad_event_attempts)
            if route_id == "BROAD_EVENT_FROZEN_ENTRY"
            else int(args.attempts_per_route)
        )
        for route_id in ROUTE_IDS
    }
    capacity = audit_generator_capacity(
        registry,
        core_field_ids=core_pack["selected_field_ids"],
        attempts_by_route=attempts_by_route,
        seeds=seeds,
        checkpoints=checkpoints,
    )
    field_rows = build_field_root_review(
        master["rows"],
        registry,
        information_metrics=metrics,
        core_pack=core_pack,
        capacity_review=capacity,
    )
    factor_review = summarize_field_review(field_rows)
    contract = build_frozen_discovery_contract(
        registry,
        core_field_ids=core_pack["selected_field_ids"],
        capacity_review=capacity,
        source_hashes=source_hashes,
    )

    output = args.output
    output.mkdir(parents=True, exist_ok=True)
    _write_json(output / "route_capacity_review.json", capacity)
    _write_json(output / "field_root_review.json", field_rows)
    schema_hash = _write_csv(output / "field_root_review.csv", field_rows)
    _write_json(output / "candidate_factor_review.json", factor_review)
    _write_json(output / "frozen_discovery_contract.json", contract)
    _write_json(args.contract_output, contract)
    access = {
        "status": "DEVELOPMENT_ROLE_NOT_OPENED_STRUCTURAL_ONLY",
        "development_market_data_accessed": False,
        "returns_labels_reward_or_selector_accessed": False,
        "validation_accessed": False,
        "holdout_accessed": False,
        "forward_2026_accessed": False,
        "candidate_promotion": False,
        "cross_epoch_memory": False,
    }
    _write_json(output / "access_ledger.json", access)

    finished = datetime.now(timezone.utc)
    runtime_seconds = time.perf_counter() - timer
    source_repo_sha = args.source_repo_sha or _repo_sha(REPO)
    manifest = {
        "experiment_id": "20260718_cn_core_pack_generator_capacity_001",
        "objective": "QUALIFY_CORE_PACK_ROOT_CONSUMPTION_AND_FREEZE_A_NON_EXECUTABLE_DISCOVERY_SCOPE",
        "status": "CN_CORE_PACK_GENERATOR_CAPACITY_REVIEW_COMPLETED",
        "mode": "STRUCTURAL_RESEARCH_NO_PERFORMANCE",
        "execution_host": hostname,
        "python_executable": sys.executable,
        "source_repo_sha": source_repo_sha,
        "started_at_utc": started.isoformat(),
        "finished_at_utc": finished.isoformat(),
        "runtime_seconds": runtime_seconds,
        "input_hashes": source_hashes,
        "input_files": {name: str(path.as_posix()) for name, path in input_files.items()},
        "parameters": {
            "attempts_by_route": attempts_by_route,
            "seeds": seeds,
            "checkpoints": checkpoints,
        },
        "field_root_review_schema_hash": schema_hash,
        "registry_hash": registry.registry_hash,
        "core_root_count": capacity["core_root_count"],
        "core_root_exposed_count": capacity["core_root_exposed_count"],
        "core_review_outcomes": factor_review["core_review_outcomes"],
        "contract_hash": contract["contract_hash"],
        "execution_authorized": False,
        "reproducible": True,
        "continuation": "AWAIT_SEPARATE_DEVELOPMENT_ONLY_DISCOVERY_AUTHORIZATION",
        "failure": None,
        "cost": {
            "market_data_rows_read": 0,
            "remote_compute_runtime_seconds": runtime_seconds,
            "economic_evaluator_calls": 0,
        },
    }
    _write_json(output / "run_manifest.json", manifest)

    indexed = [
        "route_capacity_review.json",
        "field_root_review.json",
        "field_root_review.csv",
        "candidate_factor_review.json",
        "frozen_discovery_contract.json",
        "access_ledger.json",
        "run_manifest.json",
    ]
    artifact_index = {
        "status": manifest["status"],
        "artifacts": [
            {
                "path": name,
                "sha256": _sha256(output / name),
                "bytes": (output / name).stat().st_size,
                "stage": "FINAL",
            }
            for name in indexed
        ],
    }
    _write_json(output / "artifact_index.json", artifact_index)
    print(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
