"""Build the sealed-2025 sharded daily chip-distribution sidecar."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from our_system_phase2.services.chip_sidecar import build_chip_sidecar


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--minimum-source-date", default="2023-01-01")
    parser.add_argument("--cutoff", default="2025-12-31")
    parser.add_argument("--members-per-shard", type=int, default=128)
    parser.add_argument("--workers", type=int, default=4)
    args = parser.parse_args(argv)
    result = build_chip_sidecar(
        args.archive,
        args.output_root,
        minimum_source_date=args.minimum_source_date,
        cutoff=args.cutoff,
        members_per_shard=args.members_per_shard,
        workers=args.workers,
    )
    print(
        json.dumps(
            {
                "status": "CHIP_PIT_SIDECAR_BUILT",
                "manifest": str(result.manifest_path),
                "field_registry": str(result.field_registry_path),
                "shard_count": len(result.shard_paths),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
