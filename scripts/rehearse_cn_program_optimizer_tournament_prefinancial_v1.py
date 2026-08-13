"""Build and verify Stage 0/1 without app.py or Project Control admission."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from our_system_phase2.services.program_tournament_freeze_v1 import (
    build_stage01_freeze_v1,
    verify_phase_freeze_v1,
    verify_source_binding_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-binding", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--repo-sha", required=True)
    args = parser.parse_args()
    if args.output_root.exists():
        raise FileExistsError(args.output_root)
    source = verify_source_binding_v1(
        args.source_binding, repository_root=ROOT
    )
    build_stage01_freeze_v1(
        output_root=args.output_root,
        source_binding_path=args.source_binding,
        repo_sha=args.repo_sha,
    )
    closure = verify_phase_freeze_v1(args.output_root)
    contract = json.loads(
        (args.output_root / "phase_c_run_contract.json").read_text(
            encoding="utf-8-sig"
        )
    )
    result = {
        "status": "PREFINANCIAL_REHEARSAL_COMPLETE",
        "search_v2_source_freeze_verify": "PASS",
        "tournament_stage01_freeze_verify": "PASS",
        "output_root": str(args.output_root.resolve()),
        "program_space_count": len(source["program_entries"]),
        "program_space_sha256": source["program_space_sha256"],
        "stage01_ask_count": int(closure["main_record_count"]),
        "optimizer_economic_observations": int(
            contract["development_observation_count"]
        ),
        "financial_evaluation_executed": bool(
            closure["financial_evaluation_executed"]
        ),
        "validation_reads": int(closure["validation_reads"]),
        "holdout_reads": int(closure["holdout_reads"]),
        "historical_2023_reads": int(closure["historical_2023_reads"]),
        "forward_b_reads": int(closure["forward_b_reads"]),
        "forward_2026_reads": int(closure["forward_2026_reads"]),
        "project_control_admission": "NOT_REQUESTED",
        "retry": "NOT_RUN",
        "tournament": "NOT_RUN",
    }
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
