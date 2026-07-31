"""Acquire, inspect, or release one shared 77o node resource lease."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from our_system_phase2.services.node_resource_governor import (
    acquire_node_resource_lease,
    inspect_node_resource_state,
    release_node_resource_lease,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    acquire = subparsers.add_parser("acquire")
    acquire.add_argument("--state-root", type=Path, required=True)
    acquire.add_argument("--capacity-manifest", type=Path, required=True)
    acquire.add_argument("--profile", required=True)
    acquire.add_argument("--lease-id", required=True)
    acquire.add_argument("--owner-pid", type=int, required=True)
    acquire.add_argument("--workload-id", required=True)
    acquire.add_argument("--receipt", type=Path, required=True)
    release = subparsers.add_parser("release")
    release.add_argument("--state-root", type=Path, required=True)
    release.add_argument("--lease-id", required=True)
    release.add_argument("--owner-pid", type=int, required=True)
    status = subparsers.add_parser("status")
    status.add_argument("--state-root", type=Path, required=True)
    status.add_argument("--capacity-manifest", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "acquire":
        result = acquire_node_resource_lease(
            state_root=args.state_root,
            capacity_manifest_path=args.capacity_manifest,
            profile_id=args.profile,
            lease_id=args.lease_id,
            owner_pid=args.owner_pid,
            workload_id=args.workload_id,
        )
        args.receipt.parent.mkdir(parents=True, exist_ok=True)
        args.receipt.write_text(
            json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
    elif args.command == "release":
        result = release_node_resource_lease(
            state_root=args.state_root,
            lease_id=args.lease_id,
            owner_pid=args.owner_pid,
        )
    else:
        result = inspect_node_resource_state(
            state_root=args.state_root,
            capacity_manifest_path=args.capacity_manifest,
        )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
