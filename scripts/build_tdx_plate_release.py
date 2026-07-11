"""Build the sealed-2025 TDX plate membership PIT release."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from our_system_phase2.services.tdx_plate_snapshot_release import build_tdx_plate_release


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--cutoff", default="2025-12-31")
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args(argv)
    result = build_tdx_plate_release(
        args.snapshot_root, args.output_root, cutoff=args.cutoff, workers=args.workers
    )
    print(
        json.dumps(
            {
                "status": "TDX_PLATE_PIT_RELEASE_BUILT_PARTIAL_HISTORY",
                "membership": str(result.membership_path),
                "manifest": str(result.manifest_path),
                "inventory": str(result.inventory_path),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
