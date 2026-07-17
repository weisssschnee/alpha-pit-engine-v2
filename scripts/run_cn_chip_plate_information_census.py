from __future__ import annotations

import argparse
import csv
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from our_system_phase2.services.field_information_census import (
    InformationCensusPolicy,
    field_metrics,
    pairwise_nmi,
    select_core_pack,
)


CHIP_ROLES = {
    "chip_historical_low": "benchmark-only",
    "chip_historical_high": "benchmark-only",
    "chip_cost_p05": "interaction-only",
    "chip_cost_p15": "interaction-only",
    "chip_cost_p50": "interaction-only",
    "chip_cost_p85": "interaction-only",
    "chip_cost_p95": "interaction-only",
    "chip_cost_weighted_mean": "interaction-only",
    "chip_profit_ratio": "condition-only",
}
FIELDS = tuple(CHIP_ROLES)
COORDINATES = ("code", "source_session", "source_observed_at")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = list(rows[0]) if rows else []
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def resolve_shards(manifest_path: Path, manifest: dict[str, Any]) -> list[Path]:
    paths: list[Path] = []
    for row in manifest["shards"]:
        declared = Path(str(row["output_path"]))
        fallback = manifest_path.parent / "shards" / declared.name
        selected = declared if declared.exists() else fallback
        if not selected.exists():
            raise FileNotFoundError(f"missing declared and colocated shard: {declared} / {fallback}")
        paths.append(selected)
    return paths


def collect(manifest_path: Path, *, sample_modulus: int) -> tuple[pd.DataFrame, dict[str, dict[str, Any]], int]:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("reward_or_performance_used") is not False:
        raise ValueError("chip manifest lacks explicit non-performance boundary")
    shards = resolve_shards(manifest_path, manifest)
    counts: dict[str, dict[str, Any]] = {
        field: {
            "row_count": 0,
            "finite_count": 0,
            "minimum": None,
            "maximum": None,
            "temporal_comparison_count": 0,
            "temporal_change_count": 0,
            "session_parts": [],
        }
        for field in FIELDS
    }
    samples: list[pd.DataFrame] = []
    total_rows = 0
    for shard in shards:
        frame = pd.read_parquet(shard, columns=[*COORDINATES, *FIELDS])
        source_session = pd.to_datetime(frame["source_session"], errors="raise")
        observed = pd.to_datetime(frame["source_observed_at"], errors="raise")
        if (source_session >= pd.Timestamp("2026-01-01")).any() or (
            observed >= pd.Timestamp("2026-01-01")
        ).any():
            raise ValueError("FORWARD_2026_SEALED violation in chip sidecar")
        total_rows += len(frame)
        hashes = pd.util.hash_pandas_object(frame[["code", "source_session"]], index=False).to_numpy()
        bucket = (hashes % sample_modulus).astype("int16")
        sampled = frame.loc[bucket < 2, [*COORDINATES, *FIELDS]].copy()
        sampled["sample_bucket"] = bucket[bucket < 2]
        samples.append(sampled)
        for field in FIELDS:
            numeric = pd.to_numeric(frame[field], errors="coerce")
            finite = np.isfinite(numeric.to_numpy(dtype="float64"))
            state = counts[field]
            state["row_count"] += len(frame)
            state["finite_count"] += int(finite.sum())
            if finite.any():
                minimum = float(numeric[finite].min())
                maximum = float(numeric[finite].max())
                state["minimum"] = minimum if state["minimum"] is None else min(state["minimum"], minimum)
                state["maximum"] = maximum if state["maximum"] is None else max(state["maximum"], maximum)
            ordered = frame[["code", "source_session", field]].sort_values(
                ["code", "source_session"], kind="mergesort"
            )
            previous = ordered.groupby("code", sort=False)[field].shift(1)
            comparable = pd.to_numeric(ordered[field], errors="coerce").notna() & pd.to_numeric(
                previous, errors="coerce"
            ).notna()
            state["temporal_comparison_count"] += int(comparable.sum())
            state["temporal_change_count"] += int(
                (ordered.loc[comparable, field].to_numpy() != previous.loc[comparable].to_numpy()).sum()
            )
            grouped = pd.DataFrame(
                {"source_session": source_session, "value": numeric}
            ).dropna().groupby("source_session", sort=False)["value"]
            part = grouped.agg(["count", "sum"])
            squared = pd.DataFrame(
                {"source_session": source_session, "squared": numeric * numeric}
            ).dropna().groupby("source_session", sort=False)["squared"].sum()
            part["sum_sq"] = squared
            state["session_parts"].append(part.reset_index())
    for state in counts.values():
        combined = pd.concat(state.pop("session_parts"), ignore_index=True)
        sessions = combined.groupby("source_session", sort=False)[["count", "sum", "sum_sq"]].sum()
        variance = (sessions["sum_sq"] - sessions["sum"] ** 2 / sessions["count"]).clip(lower=0.0)
        standard_deviation = np.sqrt(variance / (sessions["count"] - 1).clip(lower=1))
        state["cross_sectional_std_mean"] = float(standard_deviation.mean())
    return pd.concat(samples, ignore_index=True), counts, total_rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--chip-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repo-sha", required=True)
    parser.add_argument("--sample-modulus", type=int, default=16)
    args = parser.parse_args()
    started = time.perf_counter()
    policy = InformationCensusPolicy()
    contract = {
        "experiment_id": "20260717_cn_chip_plate_information_census_001",
        "objective": "Measure non-performance information quality and redundancy for CHIP_PLATE_STATE",
        "repo_sha": args.repo_sha,
        "data_roles": ["development_only"],
        "forbidden_inputs": ["return", "label", "reward", "validation", "holdout", "2026_forward"],
        "sample_rule": f"pandas_hash(code,source_session)%{args.sample_modulus} in [0,1]",
        "policy": policy.__dict__ if hasattr(policy, "__dict__") else {
            name: getattr(policy, name) for name in policy.__dataclass_fields__
        },
        "core_pack_status": "EXPLORATORY_NON_PERFORMANCE_ONLY",
        "plate_policy": "FAIL_CLOSED_WHEN_REAL_MATERIALIZATION_IS_ABSENT",
    }
    args.output.mkdir(parents=True, exist_ok=True)
    atomic_json(args.output / "information_census_contract.json", contract)
    sample, counts, row_count = collect(args.chip_manifest, sample_modulus=args.sample_modulus)
    metrics, codes = field_metrics(sample, fields=FIELDS, full_counts=counts, policy=policy)
    pairs = pairwise_nmi(codes, sample_buckets=sample["sample_bucket"].to_numpy())
    core_pack = select_core_pack(metrics, pairs, roles=CHIP_ROLES, policy=policy)
    write_csv(args.output / "field_information_metrics.csv", metrics)
    write_csv(args.output / "pairwise_nmi.csv", pairs)
    atomic_json(args.output / "field_information_metrics.json", metrics)
    atomic_json(args.output / "pairwise_nmi.json", pairs)
    atomic_json(args.output / "core_pack.json", core_pack)
    access_ledger = {
        "development_data_accessed": True,
        "chip_manifest": str(args.chip_manifest),
        "chip_manifest_sha256": sha256(args.chip_manifest),
        "validation_accessed": False,
        "holdout_accessed": False,
        "forward_2026_accessed": False,
        "reward_or_performance_accessed": False,
        "plate_materialization_accessed": False,
        "plate_status": "NOT_EVALUATED_MATERIALIZATION_ABSENT_ON_77O",
    }
    atomic_json(args.output / "access_ledger.json", access_ledger)
    elapsed = time.perf_counter() - started
    outputs = []
    primary_names = {
        "access_ledger.json",
        "core_pack.json",
        "field_information_metrics.csv",
        "field_information_metrics.json",
        "information_census_contract.json",
        "pairwise_nmi.csv",
        "pairwise_nmi.json",
    }
    for path in sorted(args.output / name for name in primary_names):
        outputs.append({"path": path.name, "sha256": sha256(path), "size": path.stat().st_size})
    manifest = {
        "status": "CN_CHIP_PLATE_INFORMATION_CENSUS_PARTIALLY_COMPLETED",
        "experiment_id": contract["experiment_id"],
        "repo_sha": args.repo_sha,
        "chip_row_count": row_count,
        "sample_row_count": len(sample),
        "evaluated_field_count": len(metrics),
        "core_pack_selected_count": len(core_pack["selected_field_ids"]),
        "plate_field_count_evaluated": 0,
        "performance_claim_allowed": False,
        "runtime_seconds": elapsed,
        "reproducibility": "YES",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "outputs": outputs,
    }
    atomic_json(args.output / "run_manifest.json", manifest)
    atomic_json(args.output / "artifact_index.json", {"artifacts": outputs})
    print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
