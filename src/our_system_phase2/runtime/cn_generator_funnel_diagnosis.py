"""Create a reproducible Sprint-1 diagnosis from the formal B1S CANARY artifacts."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import pandas as pd

from our_system_phase2.services.atomic_checkpoint import atomic_write_json
from our_system_phase2.services.generator_funnel_diagnostics import analyze_generator_funnel


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--proposals", type=Path, required=True)
    parser.add_argument("--strict-metrics", type=Path, required=True)
    parser.add_argument("--lane-funnel", type=Path, required=True)
    parser.add_argument("--benchmark-increment", type=Path, required=True)
    parser.add_argument("--admission-stratified", type=Path, required=True)
    parser.add_argument("--admission-global-top-k", type=Path, required=True)
    parser.add_argument("--admission-hybrid", type=Path, required=True)
    parser.add_argument("--adaptive-vs-control", type=Path, required=True)
    parser.add_argument("--bottleneck-diagnosis", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--experiment-id", required=True)
    args = parser.parse_args(argv)
    started = datetime.now(timezone.utc)
    inputs = {
        "proposals": args.proposals.resolve(),
        "strict_metrics": args.strict_metrics.resolve(),
        "lane_funnel": args.lane_funnel.resolve(),
        "benchmark_increment": args.benchmark_increment.resolve(),
        "admission_stratified": args.admission_stratified.resolve(),
        "admission_global_top_k": args.admission_global_top_k.resolve(),
        "admission_hybrid": args.admission_hybrid.resolve(),
        "adaptive_vs_control": args.adaptive_vs_control.resolve(),
        "bottleneck_diagnosis": args.bottleneck_diagnosis.resolve(),
    }
    diagnosis = analyze_generator_funnel(
        pd.read_csv(inputs["proposals"]),
        pd.read_csv(inputs["strict_metrics"]),
        pd.read_csv(inputs["lane_funnel"]),
        json.loads(inputs["benchmark_increment"].read_text(encoding="utf-8")),
        admissions={
            "stratified": pd.read_csv(inputs["admission_stratified"]),
            "global_top_k": pd.read_csv(inputs["admission_global_top_k"]),
            "hybrid": pd.read_csv(inputs["admission_hybrid"]),
        },
        adaptive_vs_control=json.loads(
            inputs["adaptive_vs_control"].read_text(encoding="utf-8")
        ),
        bottleneck_diagnosis=json.loads(
            inputs["bottleneck_diagnosis"].read_text(encoding="utf-8")
        ),
    )
    payload = {
        "experiment_id": args.experiment_id,
        "objective": "identify generator and development-objective defects before Sprint-1 implementation",
        "status": "COMPLETED",
        "mode": "development_only_diagnostic",
        "started_at": started.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "inputs": {
            name: {"path": str(path), "sha256": _sha256(path), "size": path.stat().st_size}
            for name, path in inputs.items()
        },
        "parameters": {
            "data_role": "development_only",
            "cost_adjusted_quality_available": False,
            "runtime_attribution_available": False,
        },
        "commands": ["python app.py cn-generator-funnel-diagnosis -- <frozen args>"],
        "diagnosis": diagnosis,
        "reproducibility": "YES_FOR_FROZEN_FORMAL_CANARY_ARTIFACTS",
        "continuation": "use priority_defects and missing_evidence to implement generators and Pareto objective; do not access sealed data",
        "failure": None,
    }
    atomic_write_json(args.output.resolve(), payload)
    print(json.dumps({"status": payload["status"], "output": str(args.output.resolve())}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
