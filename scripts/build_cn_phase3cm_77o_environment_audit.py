from __future__ import annotations

import argparse
import json
from pathlib import Path

from our_system_phase2.services.phase3cm_environment_audit import build_environment_audit


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--benchmark-bytes", type=int, required=True)
    parser.add_argument("--benchmark-wall-seconds", type=float, required=True)
    args = parser.parse_args()
    audit = build_environment_audit(
        repo=args.repo.resolve(),
        data_root=args.data_root.resolve(),
        device_benchmark={
            "method": "DOTNET_FILESTREAM_SEQUENTIAL_SCAN_8M_BUFFER",
            "bytes_read": args.benchmark_bytes,
            "wall_seconds": args.benchmark_wall_seconds,
            "bytes_per_second": args.benchmark_bytes / args.benchmark_wall_seconds,
            "os_page_cache_purged": False,
            "interpretation": "ACHIEVABLE_SEQUENTIAL_READ_BOUND_NOT_GUARANTEED_APPLICATION_COLD",
        },
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(audit, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "status": "COMPLETE"}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
