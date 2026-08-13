"""Verify the exact existing Search V2 source before Project Control activation."""

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
    verify_source_binding_v1,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-binding", type=Path, required=True)
    args = parser.parse_args()
    verified = verify_source_binding_v1(
        args.source_binding, repository_root=ROOT
    )
    print(
        json.dumps(
            {
                "status": "PASS",
                "search_v2_source_freeze_verify": "PASS",
                "program_space_count": len(verified["program_entries"]),
                "program_space_sha256": verified["program_space_sha256"],
                "financial_reads": 0,
                "project_control_admission": "NOT_REQUESTED",
                "tournament": "NOT_RUN",
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
