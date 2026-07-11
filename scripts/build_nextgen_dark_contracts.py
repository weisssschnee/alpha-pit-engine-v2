"""Export deterministic NEXTGEN-DARK registries without running proposals."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from our_system_phase2.services.benchmark_competitor_harness import default_benchmark_harness
from our_system_phase2.services.event_state_system import event_state_contract
from our_system_phase2.services.hypothesis_lanes import default_nextgen_lane_registry
from our_system_phase2.services.typed_temporal_program import temporal_registry_contract


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args(argv)
    lanes = default_nextgen_lane_registry()
    artifacts = {
        "nextgen_dark_temporal_registry_v1.json": temporal_registry_contract(),
        "nextgen_dark_event_registry_v1.json": event_state_contract(),
        "nextgen_dark_hypothesis_lane_registry_v1.json": lanes.contract(),
        "nextgen_dark_benchmark_registry_v1.json": default_benchmark_harness(lanes).plan(),
    }
    for name, payload in artifacts.items():
        _write(args.output_root / name, payload)
    print(json.dumps({"artifact_count": len(artifacts), "output_root": str(args.output_root)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
