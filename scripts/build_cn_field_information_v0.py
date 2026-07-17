"""Build the lightweight field-token and route-exposure V0 artifacts."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from our_system_phase2.services.field_information_v0 import (
    apply_information_census,
    compile_tokens,
    summarize,
)
from our_system_phase2.services.unified_capability_registry import UnifiedCapabilityRegistry


REPO = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--master", type=Path, default=REPO / "runtime/field_registry/cn_field_master_registry_v1/cn_field_master_registry_v1.json")
    parser.add_argument("--registry", type=Path, default=REPO / "runtime/field_registry/cn_unified_capability_registry_v3_20260717/unified_capability_registry.json")
    parser.add_argument("--output", type=Path, default=REPO / "runtime/cn_field_information_v0_20260717")
    parser.add_argument("--census-root", type=Path, default=REPO / "runtime/cn_chip_plate_information_census_20260717")
    parser.add_argument("--attempts-per-route", type=int, default=2048)
    args = parser.parse_args()
    master = json.loads(args.master.read_text(encoding="utf-8"))
    registry = UnifiedCapabilityRegistry.read(args.registry)
    tokens = compile_tokens(
        master, registry, master_path=args.master, registry_path=args.registry,
        attempts_per_route=args.attempts_per_route,
    )
    census_metrics = args.census_root / "field_information_metrics.json"
    census_core = args.census_root / "core_pack.json"
    census_manifest = args.census_root / "run_manifest.json"
    if census_metrics.exists() and census_core.exists() and census_manifest.exists():
        tokens = apply_information_census(
            tokens,
            metrics=json.loads(census_metrics.read_text(encoding="utf-8")),
            core_pack=json.loads(census_core.read_text(encoding="utf-8")),
            evidence_path=census_manifest,
        )
    args.output.mkdir(parents=True, exist_ok=True)
    with (args.output / "field_tokens.jsonl").open("w", encoding="utf-8") as handle:
        for row in tokens:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    ledger_columns = (
        "field_token_id", "field_uid", "field_id", "context", "registry_present",
        "route_compatible", "representation_available", "generator_exposed",
        "generator_exposed_routes", "materialization_status", "information_status",
        "information_qualified", "core_pack_selected", "blocker",
    )
    with (args.output / "field_exposure_ledger.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=ledger_columns)
        writer.writeheader()
        for row in tokens:
            writer.writerow({key: "|".join(row[key]) if isinstance(row[key], list) else row[key] for key in ledger_columns})
    summary = summarize(tokens)
    census_applied = census_manifest.exists()
    summary.update({
        "status": (
            "CN_FIELD_INFORMATION_V0_STRUCTURAL_AND_PARTIAL_CENSUS_COMPLETE"
            if census_applied else "CN_FIELD_INFORMATION_V0_STRUCTURAL_COMPLETE"
        ),
        "scope": (
            "TOKEN_EXPOSURE_AND_CHIP_INFORMATION_CENSUS_PLATE_NOT_EVALUATED"
            if census_applied else "TOKEN_AND_EXPOSURE_ONLY_NO_INFORMATION_CENSUS_NO_CORE_PACK"
        ),
        "master_registry": str(args.master),
        "route_registry": str(args.registry),
        "attempts_per_route": args.attempts_per_route,
        "information_census_manifest": str(census_manifest) if census_applied else None,
    })
    (args.output / "run_manifest.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
